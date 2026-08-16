//! Operations on **`#[bstack_ref]` / `#[bstack_weak]`** cells — which point at
//! *external* targets the test owns. A ref/weak holder owns nothing (targets survive
//! its teardown); a weak upgrades while its strong target is alive. Covers scalar,
//! `Option`, array, and `Vec` ref cells, a scalar + array weak cell, and enum
//! ref/weak variants.

use bstack::BStackAllocator;
use bstack_raii::{BStackBlock, BStackDrop, BStackOwned, BStackRef, TryClone};

use crate::common::TempStack;
use crate::fixtures::{Leaf, RefEnum, RefEnumData, RefEnumView, RefWeakSink, Shared};

#[test]
fn ref_and_weak_cells() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let t1 = Leaf::new(&a, 1).unwrap();
    let t2 = Leaf::new(&a, 2).unwrap();
    let e0 = Leaf::new(&a, 3).unwrap();
    let e1 = Leaf::new(&a, 4).unwrap();
    let rv0 = Leaf::new(&a, 10).unwrap();
    let rv1 = Leaf::new(&a, 11).unwrap();
    let wt = Shared::new(&a, 5).unwrap(); // scalar-weak target
    let wa = Shared::new(&a, 6).unwrap(); // weak-array target

    let mkref = |l: &BStackOwned<Leaf>| unsafe { BStackRef::from_range(l.handle().range()) };
    let h = RefWeakSink::new(
        &a,
        mkref(&t1),
        Some(mkref(&t2)),
        [mkref(&e0), mkref(&e1)],
        vec![mkref(&rv0), mkref(&rv1)],
    )
    .unwrap();
    // Weak cells are not ctor params — wire them after construction.
    h.handle().set_weak(&a, wt.downgrade().unwrap()).unwrap();
    h.handle()
        .set_weak_arr(&a, 0, wa.downgrade().unwrap())
        .unwrap();
    h.handle()
        .set_weak_arr(&a, 1, wa.downgrade().unwrap())
        .unwrap();

    // Reads across every ref shape.
    assert_eq!(h.handle().get_refd(stack).unwrap().get_v(stack).unwrap(), 1);
    assert_eq!(
        h.handle()
            .get_opt_ref(stack)
            .unwrap()
            .unwrap()
            .get_v(stack)
            .unwrap(),
        2
    );
    assert_eq!(
        h.handle().get_arr_ref(stack).unwrap()[1]
            .get_v(stack)
            .unwrap(),
        4
    );
    assert_eq!(
        h.handle()
            .get_ref_vec(&a)
            .unwrap()
            .get(1)
            .unwrap()
            .unwrap()
            .get_v(stack)
            .unwrap(),
        11
    );

    // Weak scalar + array upgrade while their strong targets are alive.
    let up = h.handle().get_weak(&a).unwrap().expect("weak target alive");
    assert_eq!(up.handle().get_v(stack).unwrap(), 5);
    drop(up);
    let warr = h.handle().get_weak_arr(&a).unwrap();
    assert_eq!(warr[0].as_ref().unwrap().handle().get_v(stack).unwrap(), 6);

    // A ref/weak holder owns nothing external: every target survives its teardown.
    h.bstack_drop(&a).unwrap();
    assert_eq!(t1.handle().get_v(stack).unwrap(), 1);
    assert_eq!(rv0.handle().get_v(stack).unwrap(), 10);

    // Cleanup (targets are still solely owned here).
    for l in [t1, t2, e0, e1, rv0, rv1] {
        l.bstack_drop(&a).unwrap();
    }
    drop(wt);
    drop(wa);
}

#[test]
fn enum_ref_variant() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let rt = Leaf::new(&a, 7).unwrap();
    let mkref = |l: &BStackOwned<Leaf>| unsafe { BStackRef::from_range(l.handle().range()) };

    let e = RefEnum::new(&a, RefEnumData::Ref(mkref(&rt))).unwrap();
    match e.handle().read(&a).unwrap() {
        RefEnumView::Ref(l) => assert_eq!(l.get_v(stack).unwrap(), 7),
        _ => panic!("expected Ref"),
    }
    // The ref variant owns nothing: the target survives the enum's teardown.
    e.bstack_drop(&a).unwrap();
    assert_eq!(rt.handle().get_v(stack).unwrap(), 7);
    rt.bstack_drop(&a).unwrap();
}

#[test]
fn enum_weak_variant() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let wt = Shared::new(&a, 9).unwrap();
    let e = RefEnum::new(&a, RefEnumData::Weak(wt.downgrade().unwrap())).unwrap();
    match e.handle().read(&a).unwrap() {
        RefEnumView::Weak(Some(rc)) => assert_eq!(rc.handle().get_v(stack).unwrap(), 9),
        _ => panic!("expected upgradeable Weak"),
    }
    e.bstack_drop(&a).unwrap(); // releases the variant's weak ref
    let _keep = wt.try_clone().unwrap();
    drop(wt);
    drop(_keep);
}
