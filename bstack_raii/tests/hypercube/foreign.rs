//! Operation: **foreign resolution** (SELF). Cross-file resolution needs a scoped
//! registry (the global is one-shot), so it stays a unit test; here a `SELF` foreign
//! resolves against the local allocator, and `into_local` performs the foreign→ref
//! cast.

use bstack::BStackAllocator;
use bstack_raii::{BStackBlock, BStackDrop};

use crate::common::TempStack;
use crate::fixtures::{Leaf, foreign_self_sink};

#[test]
fn self_foreign_resolves_via_with() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let h = foreign_self_sink(&a).unwrap();
    let got = h
        .handle()
        .get_of(stack)
        .unwrap()
        .with(&a, |t, fs| t.get_v(fs).unwrap())
        .unwrap();
    assert_eq!(got, Some(10));
    h.bstack_drop(&a).unwrap();
}

#[test]
fn self_foreign_into_local_resolves() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let h = foreign_self_sink(&a).unwrap();
    // foreign → ref cast: a SELF pointer resolves to an offset-only `BStackRef`,
    // which reads through a block handle over the same range.
    let r = h
        .handle()
        .get_of(stack)
        .unwrap()
        .into_local()
        .expect("SELF resolvable");
    let leaf = <Leaf as BStackBlock>::from_range(r.into_range());
    assert_eq!(leaf.get_v(stack).unwrap(), 10);
    h.bstack_drop(&a).unwrap();
}
