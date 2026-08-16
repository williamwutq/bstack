//! Operation: **move** — `bstack_move!` frees only the container shell and hands each
//! slot back by value (owned children as their RAII duals, still live). Teardown is
//! defused, so the test frees the transferred children itself.

use bstack::BStackAllocator;
use bstack_raii::{BStackDrop, bstack_move};

use crate::common::TempStack;
use crate::fixtures::*;

#[test]
fn struct_move_transfers_slots() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let h = mut_sink(&a).unwrap();
    let (pod, tuple, owned, strong, arr) = bstack_move!(h, &a).unwrap();

    assert_eq!(pod, 1);
    assert_eq!(tuple, (2, 3));
    // The children come back live (only the shell was freed).
    assert_eq!(owned.handle().get_v(stack).unwrap(), 10);
    assert_eq!(strong.handle().get_v(stack).unwrap(), 20);
    assert_eq!(arr[0].handle().get_v(stack).unwrap(), 30);

    // Move defused teardown, so free the transferred children explicitly.
    owned.bstack_drop(&a).unwrap();
    drop(strong);
    for o in arr {
        o.bstack_drop(&a).unwrap();
    }
}

#[test]
fn enum_move_transfers_payload() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let e = enum_owned(&a).unwrap();
    let data = bstack_move!(e, &a).unwrap();
    match data {
        EnumSinkData::Owned(o) => {
            assert_eq!(o.handle().get_v(stack).unwrap(), 42);
            o.bstack_drop(&a).unwrap();
        }
        _ => panic!("expected Owned"),
    }
}
