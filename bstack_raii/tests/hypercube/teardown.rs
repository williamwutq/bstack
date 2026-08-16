//! Operation: **teardown** — `bstack_drop` reclaims the whole tree, no leak. The
//! `assert_teardown_reclaims` oracle (net-zero allocation across two identical
//! cycles) is FirstFit-specific; GhostTree gets a bulk-path smoke test.

use bstack_raii::BStackDrop;

use crate::common::{TempStack, assert_teardown_reclaims};
use crate::fixtures::*;

#[test]
fn block_sink_reclaims() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    assert_teardown_reclaims(&a, || block_sink(&a).unwrap());
}

#[test]
fn mut_sink_reclaims() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    assert_teardown_reclaims(&a, || mut_sink(&a).unwrap());
}

#[test]
fn embed_sink_reclaims() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    assert_teardown_reclaims(&a, || embed_sink(&a).unwrap());
}

#[test]
fn foreign_self_sink_reclaims() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    assert_teardown_reclaims(&a, || foreign_self_sink(&a).unwrap());
}

#[test]
fn enum_owned_reclaims() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    assert_teardown_reclaims(&a, || enum_owned(&a).unwrap());
}

#[test]
fn const_generic_array_reclaims() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    assert_teardown_reclaims(&a, || {
        ConstArrSink::<3>::new(
            &a,
            [
                Leaf::new(&a, 1).unwrap(),
                Leaf::new(&a, 2).unwrap(),
                Leaf::new(&a, 3).unwrap(),
            ],
        )
        .unwrap()
    });
}

#[test]
fn gen_sink_reclaims() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    assert_teardown_reclaims(&a, || {
        GenSink::<Leaf>::new(
            &a,
            1,
            Leaf::new(&a, 5).unwrap(),
            vec![Leaf::new(&a, 6).unwrap()],
        )
        .unwrap()
    });
}

#[test]
fn block_sink_bulk_path_teardown_ok() {
    // GhostTree implements `BStackBulkAllocator`, so building + tearing the sink down
    // drives the *atomic-bulk* `alloc_many` / `free_many` path (FirstFit hits the
    // sequential fallback). Its allocation layout isn't cycle-stable, so the FirstFit
    // len-based leak oracle doesn't apply — this asserts the bulk path neither errors
    // nor double-frees across repeated cycles.
    let tmp = TempStack::new();
    let a = tmp.ghost_allocator();
    for _ in 0..3 {
        block_sink(&a).unwrap().bstack_drop(&a).unwrap();
    }
}
