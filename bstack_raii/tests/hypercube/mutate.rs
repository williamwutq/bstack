//! Operation: **mutate** — `set_` (pod / tuple / ref), `replace_` (owned / strong,
//! moving the old out), array element/whole `replace_`, and whole-value enum
//! `replace`. Every `replace_` upholds the "old handed back, never stranded"
//! contract; teardown afterwards still reclaims cleanly.

use bstack::BStackAllocator;
use bstack_raii::BStackDrop;

use crate::common::TempStack;
use crate::fixtures::*;

#[test]
fn pod_and_tuple_set() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let h = mut_sink(&a).unwrap();
    h.handle().set_pod(stack, 99).unwrap();
    assert_eq!(h.handle().get_pod(stack).unwrap(), 99);
    h.handle().set_tuple(stack, (8, 9)).unwrap();
    assert_eq!(h.handle().get_tuple(stack).unwrap(), (8, 9));
    h.bstack_drop(&a).unwrap();
}

#[test]
fn owned_replace_moves_old_out() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let h = mut_sink(&a).unwrap();
    let old = h
        .handle()
        .replace_owned(stack, Leaf::new(&a, 77).unwrap())
        .unwrap();
    assert_eq!(old.handle().get_v(stack).unwrap(), 10); // moved out, still live
    old.bstack_drop(&a).unwrap();
    assert_eq!(
        h.handle().get_owned(stack).unwrap().get_v(stack).unwrap(),
        77
    );
    h.bstack_drop(&a).unwrap();
}

#[test]
fn strong_replace_moves_count_out() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let h = mut_sink(&a).unwrap();
    let old = h
        .handle()
        .replace_strong(&a, Shared::new(&a, 88).unwrap())
        .unwrap();
    assert_eq!(old.handle().get_v(stack).unwrap(), 20);
    drop(old); // decrements the old target (1 → 0), frees it
    assert_eq!(
        h.handle().get_strong(stack).unwrap().get_v(stack).unwrap(),
        88
    );
    h.bstack_drop(&a).unwrap();
}

#[test]
fn array_element_and_whole_replace() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let h = mut_sink(&a).unwrap();

    // Element swap by flat index.
    let old = h
        .handle()
        .replace_arr_at(&a, 1, Leaf::new(&a, 310).unwrap())
        .unwrap();
    assert_eq!(old.handle().get_v(stack).unwrap(), 31);
    old.bstack_drop(&a).unwrap();
    assert_eq!(
        h.handle().get_arr(stack).unwrap()[1].get_v(stack).unwrap(),
        310
    );

    // Whole-array swap.
    let old_arr = h
        .handle()
        .replace_arr(
            &a,
            [
                Leaf::new(&a, 1).unwrap(),
                Leaf::new(&a, 2).unwrap(),
                Leaf::new(&a, 3).unwrap(),
            ],
        )
        .unwrap();
    for o in old_arr {
        o.bstack_drop(&a).unwrap();
    }
    assert_eq!(
        h.handle().get_arr(stack).unwrap()[2].get_v(stack).unwrap(),
        3
    );

    h.bstack_drop(&a).unwrap();
}

#[test]
fn enum_whole_value_replace() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let e = MutEnum::new(&a, MutEnumData::Owned(Leaf::new(&a, 5).unwrap())).unwrap();
    // Replace the owned variant with a POD one; the old Owned(Leaf) moves out.
    let old = e.handle().replace(&a, MutEnumData::Num(9)).unwrap();
    match old {
        MutEnumData::Owned(o) => {
            assert_eq!(o.handle().get_v(stack).unwrap(), 5);
            o.bstack_drop(&a).unwrap();
        }
        _ => panic!("expected old Owned"),
    }
    assert!(matches!(e.handle().read(&a).unwrap(), MutEnumView::Num(9)));
    e.bstack_drop(&a).unwrap();
}
