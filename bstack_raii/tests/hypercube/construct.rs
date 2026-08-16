//! Operation: **construct + read** — every sink builds and round-trips through its
//! accessors. One test per container-form; together they touch every self-owning
//! (kind × shape) cell.

use bstack::BStackAllocator;
use bstack_raii::{BStackDrop, TryClone};

use crate::common::TempStack;
use crate::fixtures::*;

#[test]
fn block_sink_round_trips() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let h = block_sink(&a).unwrap();

    // POD shapes.
    assert_eq!(h.handle().get_pod(stack).unwrap(), 7);
    assert_eq!(h.handle().get_tuple_pod(stack).unwrap(), (1, 2));
    assert_eq!(h.handle().get_arr_pod(stack).unwrap(), [3, 4, 5]);

    // Owned / strong / embed scalars.
    assert_eq!(
        h.handle().get_owned(stack).unwrap().get_v(stack).unwrap(),
        10
    );
    assert_eq!(
        h.handle().get_strong(stack).unwrap().get_v(stack).unwrap(),
        20
    );
    assert_eq!(h.handle().get_emb().get_v(stack).unwrap(), 30);

    // Owned containers.
    let vec = h.handle().get_vec_owned(&a).unwrap();
    assert_eq!(vec.len().unwrap(), 2);
    assert_eq!(vec.get(1).unwrap().unwrap().get_v(stack).unwrap(), 41);
    assert_eq!(
        h.handle().get_arr_owned(stack).unwrap()[0]
            .get_v(stack)
            .unwrap(),
        50
    );
    assert_eq!(
        h.handle().get_nested(stack).unwrap()[1][1]
            .get_v(stack)
            .unwrap(),
        63
    );

    h.bstack_drop(&a).unwrap();
}

#[test]
fn mut_sink_round_trips() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let h = mut_sink(&a).unwrap();
    assert_eq!(h.handle().get_pod(stack).unwrap(), 1);
    assert_eq!(h.handle().get_tuple(stack).unwrap(), (2, 3));
    assert_eq!(
        h.handle().get_owned(stack).unwrap().get_v(stack).unwrap(),
        10
    );
    assert_eq!(
        h.handle().get_strong(stack).unwrap().get_v(stack).unwrap(),
        20
    );
    assert_eq!(
        h.handle().get_arr(stack).unwrap()[2].get_v(stack).unwrap(),
        32
    );
    h.bstack_drop(&a).unwrap();
}

#[test]
fn embed_sink_round_trips() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    // Embedding a whole kitchen-sink block: its inlined child's own fields resolve.
    let h = embed_sink(&a).unwrap();
    assert_eq!(h.handle().get_tag(stack).unwrap(), 9);
    let child = h.handle().get_child();
    assert_eq!(child.get_pod(stack).unwrap(), 7);
    assert_eq!(child.get_owned(stack).unwrap().get_v(stack).unwrap(), 10);
    h.bstack_drop(&a).unwrap();
}

#[test]
fn foreign_self_sink_round_trips() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let h = foreign_self_sink(&a).unwrap();
    // A SELF foreign resolves against the local allocator.
    let got = h
        .handle()
        .get_of(stack)
        .unwrap()
        .with(&a, |t, fs| t.get_v(fs).unwrap())
        .unwrap();
    assert_eq!(got, Some(10));
    let opt = h.handle().get_opt(stack).unwrap().expect("Some");
    assert_eq!(
        opt.with(&a, |t, fs| t.get_v(fs).unwrap()).unwrap(),
        Some(20)
    );
    h.bstack_drop(&a).unwrap();
}

#[test]
fn rc_sink_round_trips() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let rc = rc_sink(&a).unwrap();
    assert_eq!(rc.handle().get_pod(stack).unwrap(), 7);
    assert_eq!(
        rc.handle().get_child(stack).unwrap().get_v(stack).unwrap(),
        10
    );
    drop(rc); // last strong ref → frees data + child

    let rcw = rcweak_sink(&a).unwrap();
    assert_eq!(rcw.handle().get_pod(stack).unwrap(), 7);
    drop(rcw);
}

#[test]
fn enum_sink_variants_round_trip() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let u = EnumSink::new(&a, EnumSinkData::Unit).unwrap();
    assert!(matches!(u.handle().read(&a).unwrap(), EnumSinkView::Unit));
    u.bstack_drop(&a).unwrap();

    let n = EnumSink::new(&a, EnumSinkData::Num(5)).unwrap();
    assert!(matches!(n.handle().read(&a).unwrap(), EnumSinkView::Num(5)));
    n.bstack_drop(&a).unwrap();

    let o = enum_owned(&a).unwrap();
    match o.handle().read(&a).unwrap() {
        EnumSinkView::Owned(leaf) => assert_eq!(leaf.get_v(stack).unwrap(), 42),
        _ => panic!("expected Owned"),
    }
    o.bstack_drop(&a).unwrap();
}

#[test]
fn const_generic_array_round_trips() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let h = ConstArrSink::<3>::new(
        &a,
        [
            Leaf::new(&a, 1).unwrap(),
            Leaf::new(&a, 2).unwrap(),
            Leaf::new(&a, 3).unwrap(),
        ],
    )
    .unwrap();
    assert_eq!(
        h.handle().get_xs(stack).unwrap()[2].get_v(stack).unwrap(),
        3
    );
    h.bstack_drop(&a).unwrap();
}

#[test]
fn rc_enum_round_trips() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    // A shared (`rc`) enum: `new` yields a `BStackRc`, duplicated by a refcount bump.
    let e = RcEnum::new(&a, RcEnumData::Owned(Leaf::new(&a, 5).unwrap())).unwrap();
    match e.handle().read(&a).unwrap() {
        RcEnumView::Owned(leaf) => assert_eq!(leaf.get_v(stack).unwrap(), 5),
        _ => panic!("expected Owned"),
    }
    let e2 = e.try_clone().unwrap();
    drop(e); // still alive via e2
    assert!(matches!(
        e2.handle().read(&a).unwrap(),
        RcEnumView::Owned(_)
    ));
    drop(e2);
}

#[test]
fn gen_sink_round_trips() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let h = GenSink::<Leaf>::new(
        &a,
        1,
        Leaf::new(&a, 5).unwrap(),
        vec![Leaf::new(&a, 6).unwrap()],
    )
    .unwrap();
    assert_eq!(h.handle().get_tag(stack).unwrap(), 1);
    assert_eq!(
        h.handle().get_owned(stack).unwrap().get_v(stack).unwrap(),
        5
    );
    assert_eq!(
        h.handle()
            .get_vec(&a)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .get_v(stack)
            .unwrap(),
        6
    );
    h.bstack_drop(&a).unwrap();
}
