//! Operation: **deep clone** — `try_clone_in` produces an independent copy (distinct
//! child blocks), tearing the clone down never disturbs the original, and the clone
//! itself reclaims cleanly.

use bstack::BStackAllocator;
use bstack_raii::{BStackBlock, BStackDrop, TryCloneIn};

use crate::common::{TempStack, assert_teardown_reclaims};
use crate::fixtures::*;

#[test]
fn block_sink_clone_is_deep_and_independent() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let h = block_sink(&a).unwrap();
    let c = h.try_clone_in(&a).unwrap();

    // Same values …
    assert_eq!(
        c.handle().get_owned(stack).unwrap().get_v(stack).unwrap(),
        10
    );
    // … but a distinct child block (deep copy, not an alias).
    let orig = h.handle().get_owned(stack).unwrap().range().start();
    let clone = c.handle().get_owned(stack).unwrap().range().start();
    assert_ne!(orig, clone);

    // Dropping the clone leaves the original fully intact.
    c.bstack_drop(&a).unwrap();
    assert_eq!(
        h.handle().get_owned(stack).unwrap().get_v(stack).unwrap(),
        10
    );
    h.bstack_drop(&a).unwrap();
}

#[test]
fn block_sink_clone_reclaims() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let src = block_sink(&a).unwrap();
    assert_teardown_reclaims(&a, || src.try_clone_in(&a).unwrap());
    src.bstack_drop(&a).unwrap();
}

#[test]
fn embed_sink_clone_is_independent() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let h = embed_sink(&a).unwrap();
    let c = h.try_clone_in(&a).unwrap();
    // The embedded child's own owned grandchild is deep-copied, not aliased.
    let orig = h
        .handle()
        .get_child()
        .get_owned(stack)
        .unwrap()
        .range()
        .start();
    let clone = c
        .handle()
        .get_child()
        .get_owned(stack)
        .unwrap()
        .range()
        .start();
    assert_ne!(orig, clone);
    c.bstack_drop(&a).unwrap();
    assert_eq!(
        h.handle()
            .get_child()
            .get_owned(stack)
            .unwrap()
            .get_v(stack)
            .unwrap(),
        10
    );
    h.bstack_drop(&a).unwrap();
}

#[test]
fn enum_owned_clone_is_deep() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let e = enum_owned(&a).unwrap();
    let c = e.try_clone_in(&a).unwrap();
    match c.handle().read(&a).unwrap() {
        EnumSinkView::Owned(leaf) => assert_eq!(leaf.get_v(stack).unwrap(), 42),
        _ => panic!("expected Owned"),
    }
    c.bstack_drop(&a).unwrap();
    // Original still readable after the clone is gone.
    match e.handle().read(&a).unwrap() {
        EnumSinkView::Owned(leaf) => assert_eq!(leaf.get_v(stack).unwrap(), 42),
        _ => panic!("expected Owned"),
    }
    e.bstack_drop(&a).unwrap();
}
