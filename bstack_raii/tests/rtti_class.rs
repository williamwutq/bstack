//! End-to-end test of the `#[bstack_class]` emitter: the macro must produce the
//! ordinary block machinery **and** register an RTTI descriptor that `rtti::sync`
//! persists, with the right shapes / offsets / sizes.
//!
//! This is its own integration-test binary, so its `linkme` slice collects only the
//! types declared here — isolated from the crate's own unit tests.
#![allow(dead_code)] // some fixtures are inspected only via RTTI, never instantiated

use bstack::{BStack, BStackAllocator, FirstFitBStackAllocator};
use bstack_raii::rtti::{self, RttiBody, Shape, Value};
use bstack_raii::{BStackBlock, BStackCast, BStackDrop, BStackOwned, bstack_class};

#[bstack_class]
struct Point {
    x: u32,
    y: u32,
}

#[bstack_class]
struct Line {
    #[bstack_owned]
    a: Point,
    #[bstack_owned]
    b: Point,
    labels: Vec<u8>,
    coords: [u64; 4],
    #[bstack_owned]
    next: Option<Point>,
}

#[bstack_class]
enum Kind2 {
    Empty,
    Pair(u32, u16),
    #[bstack_owned]
    Owns(Point),
}

#[bstack_class]
struct Wrap {
    #[bstack_owned]
    inner: Point,
    n: u32,
}

#[bstack_class]
struct VecArr {
    labels: Vec<u8>,
    coords: [u32; 3],
    #[bstack_owned]
    maybe: Option<Point>,
}

#[bstack_class(rc)]
struct RCell {
    v: u32,
}

#[bstack_class(rc, weak)]
struct WCell {
    v: u32,
}

#[bstack_class]
struct RcHolder {
    #[bstack_strong]
    s: RCell,
}

#[bstack_class]
struct WHolder {
    #[bstack_strong]
    s: WCell,
}

#[bstack_class]
struct WeakHolder {
    tag: u32,
    #[bstack_weak]
    w: WCell,
}

#[bstack_class]
struct Config {
    /// A constant class variable.
    #[bstack_static(7u32)]
    version: u32,
    /// A mutable class variable.
    #[bstack_mut]
    #[bstack_static(0u64)]
    counter: u64,
    /// An ordinary per-instance field.
    id: u32,
}

fn temp_path(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "bstack_raii_rtti_class_{tag}_{}.stack",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[test]
fn bstack_class_emits_block_machinery() {
    // The block half must still work exactly like `#[bstack_block]`.
    let path = temp_path("block");
    let alloc = FirstFitBStackAllocator::new(BStack::open(&path).unwrap()).unwrap();
    let p: BStackOwned<Point> = Point::new(&alloc, 7, 9).unwrap();
    assert_eq!(p.handle().get_x(alloc.stack()).unwrap(), 7);
    assert_eq!(p.handle().get_y(alloc.stack()).unwrap(), 9);
    p.bstack_drop(&alloc).unwrap();
    std::fs::remove_file(&path).ok();
}

#[test]
fn bstack_class_syncs_rtti_schema() {
    let path = temp_path("schema");
    let mut reg = rtti::sync(&path).unwrap();

    // Point: two POD fields, packed at offsets 0 and 4.
    let po = reg.ordinal_of(<Point as BStackCast>::eightcc()).unwrap();
    let pt = reg.load_type(po).unwrap();
    assert_eq!(pt.name, "Point");
    assert!(!pt.rc && !pt.weak);
    // Offsets and size are block-relative: `XOnDisk` leads with the 16-byte
    // `BlockHeader`, so the payload begins at 16 and the full block is 16 + 8.
    assert_eq!(pt.ondisk_size, 24);
    let RttiBody::Struct(pf) = &pt.body else {
        panic!("Point should be a struct body");
    };
    assert_eq!(pf.len(), 2);
    assert_eq!(pf[0].name, "x");
    assert_eq!(pf[0].offset, 16);
    assert_eq!(pf[0].shape, Shape::Pod { width: 4 });
    assert_eq!(pf[1].name, "y");
    assert_eq!(pf[1].offset, 20);
    assert_eq!(pf[1].shape, Shape::Pod { width: 4 });

    // Line: owned children, a vec, an array, and an optional pod.
    let lo = reg.ordinal_of(<Line as BStackCast>::eightcc()).unwrap();
    let lt = reg.load_type(lo).unwrap();
    let RttiBody::Struct(lf) = &lt.body else {
        panic!("Line should be a struct body");
    };
    assert_eq!(lf.len(), 5);
    let point_tag = <Point as BStackCast>::eightcc();
    assert_eq!(lf[0].name, "a");
    assert_eq!(lf[0].shape, Shape::Owned(point_tag));
    assert_eq!(lf[1].shape, Shape::Owned(point_tag));
    assert_eq!(lf[2].shape, Shape::Vec(Box::new(Shape::Pod { width: 1 })));
    assert_eq!(
        lf[3].shape,
        Shape::Array {
            n: 4,
            inner: Box::new(Shape::Pod { width: 8 }),
        }
    );
    assert_eq!(
        lf[4].shape,
        Shape::Option(Box::new(Shape::Owned(point_tag)))
    );

    // Idempotent: re-syncing the same binary's types appends nothing more.
    assert_eq!(reg.sync_compiled().unwrap(), 0);

    drop(reg);
    std::fs::remove_file(&path).ok();
}

#[test]
fn bstack_class_enum_syncs_rtti_schema() {
    let path = temp_path("enum");
    let reg = rtti::sync(&path).unwrap();

    let ord = reg.ordinal_of(<Kind2 as BStackCast>::eightcc()).unwrap();
    let ty = reg.load_type(ord).unwrap();
    assert_eq!(ty.name, "Kind2");
    let RttiBody::Enum(e) = &ty.body else {
        panic!("Kind2 should be an enum body");
    };
    // Discriminants 0/1/2 fit a u8.
    assert_eq!(e.disc_width, 1);
    assert!(e.payload_off > e.disc_off);
    assert_eq!(e.variants.len(), 3);

    // Unit variant.
    assert_eq!(e.variants[0].name, "Empty");
    assert_eq!(e.variants[0].disc_value, 0);
    assert!(e.variants[0].fields.is_empty());

    // POD-aggregate variant: `u32` at 0, `u16` at 4 (packed, payload-relative).
    assert_eq!(e.variants[1].name, "Pair");
    assert_eq!(e.variants[1].disc_value, 1);
    assert_eq!(e.variants[1].fields.len(), 2);
    assert_eq!(e.variants[1].fields[0].offset, 0);
    assert_eq!(e.variants[1].fields[0].shape, Shape::Pod { width: 4 });
    assert_eq!(e.variants[1].fields[1].offset, 4);
    assert_eq!(e.variants[1].fields[1].shape, Shape::Pod { width: 2 });

    // Owned-child variant: one `u64` offset at 0, shaped as Owned(Point).
    assert_eq!(e.variants[2].name, "Owns");
    assert_eq!(e.variants[2].disc_value, 2);
    assert_eq!(e.variants[2].fields.len(), 1);
    assert_eq!(e.variants[2].fields[0].offset, 0);
    assert_eq!(
        e.variants[2].fields[0].shape,
        Shape::Owned(<Point as BStackCast>::eightcc())
    );

    drop(reg);
    std::fs::remove_file(&path).ok();
}

#[test]
fn bstack_class_static_class_variables() {
    let path = temp_path("static");
    let reg = rtti::sync(&path).unwrap();

    let ord = reg.ordinal_of(<Config as BStackCast>::eightcc()).unwrap();
    let ty = reg.load_type(ord).unwrap();
    // Class variables are NOT per-instance: only `id` occupies `XOnDisk`.
    assert_eq!(ty.ondisk_size, 20); // 16-byte header + u32
    let RttiBody::Struct(f) = &ty.body else {
        panic!("Config should be a struct body");
    };
    assert_eq!(f.len(), 3);

    // Const class variable: value bytes are the encoded `7u32`.
    assert_eq!(f[0].name, "version");
    assert_eq!(
        f[0].shape,
        Shape::Class {
            mutable: false,
            inner: Box::new(Shape::Pod { width: 4 }),
            value: vec![7, 0, 0, 0],
        }
    );

    // Mutable class variable: initial value `0u64`.
    assert_eq!(f[1].name, "counter");
    assert_eq!(
        f[1].shape,
        Shape::Class {
            mutable: true,
            inner: Box::new(Shape::Pod { width: 8 }),
            value: vec![0; 8],
        }
    );

    // The one real instance field sits right after the header.
    assert_eq!(f[2].name, "id");
    assert_eq!(f[2].offset, 16);
    assert_eq!(f[2].shape, Shape::Pod { width: 4 });

    drop(reg);
    std::fs::remove_file(&path).ok();
}

/// A `Value::Pod` from raw bytes.
fn pod(bytes: &[u8]) -> Value {
    Value::Pod(bytes.into())
}

/// A fresh data-file allocator (separate from the schema file).
fn data_alloc(tag: &str) -> (FirstFitBStackAllocator, std::path::PathBuf) {
    let path = temp_path(tag);
    let alloc = FirstFitBStackAllocator::new(BStack::open(&path).unwrap()).unwrap();
    (alloc, path)
}

#[test]
fn interpret_reads_pod_struct() {
    let schema = temp_path("read1_schema");
    let reg = rtti::sync(&schema).unwrap();
    let (alloc, dpath) = data_alloc("read1_data");

    let p = Point::new(&alloc, 7, 9).unwrap();
    let off = BStackBlock::range(p.handle()).start();
    let ord = reg.ordinal_of(<Point as BStackCast>::eightcc()).unwrap();

    let Value::Block { tag, fields } = reg.read_value(alloc.stack(), ord, off).unwrap() else {
        panic!("expected a block");
    };
    assert_eq!(tag, <Point as BStackCast>::eightcc());
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].0, "x");
    assert_eq!(fields[0].1, pod(&7u32.to_le_bytes()));
    assert_eq!(fields[1].0, "y");
    assert_eq!(fields[1].1, pod(&9u32.to_le_bytes()));

    p.bstack_drop(&alloc).unwrap();
    drop(reg);
    std::fs::remove_file(&schema).ok();
    std::fs::remove_file(&dpath).ok();
}

#[test]
fn interpret_reads_nested_and_via_pointer() {
    let schema = temp_path("read2_schema");
    let reg = rtti::sync(&schema).unwrap();
    let (alloc, dpath) = data_alloc("read2_data");

    let inner = Point::new(&alloc, 1, 2).unwrap();
    let w = Wrap::new(&alloc, inner, 5).unwrap();
    let off = BStackBlock::range(w.handle()).start();
    let ord = reg.ordinal_of(<Wrap as BStackCast>::eightcc()).unwrap();

    // Direct read: the owned child is *followed* into a nested block.
    let v = reg.read_value(alloc.stack(), ord, off).unwrap();
    let Value::Block { fields, .. } = &v else {
        panic!("expected a block");
    };
    assert_eq!(fields[0].0, "inner");
    let Value::Block {
        fields: inner_fields,
        ..
    } = &fields[0].1
    else {
        panic!("owned child should be a followed block");
    };
    assert_eq!(inner_fields[0].1, pod(&1u32.to_le_bytes()));
    assert_eq!(inner_fields[1].1, pod(&2u32.to_le_bytes()));
    assert_eq!(fields[1].1, pod(&5u32.to_le_bytes()));

    // The same read through a typed pointer (ordinal recovered from the pointer).
    let ptr = rtti::typed_ptr(0, off, ord);
    assert_eq!(&reg.read_ptr(alloc.stack(), ptr).unwrap(), &v);

    w.bstack_drop(&alloc).unwrap();
    drop(reg);
    std::fs::remove_file(&schema).ok();
    std::fs::remove_file(&dpath).ok();
}

#[test]
fn interpret_reads_enum_variant() {
    let schema = temp_path("read3_schema");
    let reg = rtti::sync(&schema).unwrap();
    let (alloc, dpath) = data_alloc("read3_data");

    let k = Kind2::new(&alloc, Kind2Data::Pair(3, 4)).unwrap();
    let off = BStackBlock::range(k.handle()).start();
    let ord = reg.ordinal_of(<Kind2 as BStackCast>::eightcc()).unwrap();

    let Value::Enum {
        variant, fields, ..
    } = reg.read_value(alloc.stack(), ord, off).unwrap()
    else {
        panic!("expected an enum");
    };
    assert_eq!(variant, "Pair");
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].1, pod(&3u32.to_le_bytes()));
    assert_eq!(fields[1].1, pod(&4u16.to_le_bytes()));

    k.bstack_drop(&alloc).unwrap();
    drop(reg);
    std::fs::remove_file(&schema).ok();
    std::fs::remove_file(&dpath).ok();
}

#[test]
fn interpret_reads_vec_array_and_option() {
    let schema = temp_path("read4_schema");
    let reg = rtti::sync(&schema).unwrap();
    let (alloc, dpath) = data_alloc("read4_data");

    let v = VecArr::new(
        &alloc,
        &[10, 20],
        [1, 2, 3],
        Some(Point::new(&alloc, 8, 9).unwrap()),
    )
    .unwrap();
    let off = BStackBlock::range(v.handle()).start();
    let ord = reg.ordinal_of(<VecArr as BStackCast>::eightcc()).unwrap();

    let Value::Block { fields, .. } = reg.read_value(alloc.stack(), ord, off).unwrap() else {
        panic!("expected a block");
    };

    // Vec<u8> → a vector of 1-byte pod leaves.
    assert_eq!(fields[0].0, "labels");
    assert_eq!(fields[0].1, Value::Vec(vec![pod(&[10]), pod(&[20])].into()));

    // [u32; 3] → a fixed array of 4-byte pod leaves.
    assert_eq!(fields[1].0, "coords");
    assert_eq!(
        fields[1].1,
        Value::Array(
            vec![
                pod(&1u32.to_le_bytes()),
                pod(&2u32.to_le_bytes()),
                pod(&3u32.to_le_bytes()),
            ]
            .into()
        )
    );

    // Option<owned Point> present → Some(followed block).
    assert_eq!(fields[2].0, "maybe");
    let Value::Some(boxed) = &fields[2].1 else {
        panic!("expected Some");
    };
    let Value::Block {
        fields: pt_fields, ..
    } = boxed.as_ref()
    else {
        panic!("expected a followed Point block");
    };
    assert_eq!(pt_fields[0].1, pod(&8u32.to_le_bytes()));
    assert_eq!(pt_fields[1].1, pod(&9u32.to_le_bytes()));

    v.bstack_drop(&alloc).unwrap();
    drop(reg);
    std::fs::remove_file(&schema).ok();
    std::fs::remove_file(&dpath).ok();
}

#[test]
fn interpret_teardown_reclaims_owned_tree() {
    let schema = temp_path("td1_schema");
    let reg = rtti::sync(&schema).unwrap();
    let (alloc, dpath) = data_alloc("td1_data");
    let ord = reg.ordinal_of(<Wrap as BStackCast>::eightcc()).unwrap();

    // Build a Wrap owning a Point, return its (detached) root offset.
    let build = || {
        let inner = Point::new(&alloc, 1, 2).unwrap();
        let w = Wrap::new(&alloc, inner, 5).unwrap();
        BStackBlock::range(w.handle()).start()
    };

    // Leak oracle: warm once (allocator high-water settles), snapshot, then do an
    // identical build + RTTI teardown and assert the stack returned to baseline —
    // so teardown reclaimed the root *and* the owned child, with nothing leaked.
    reg.teardown(&alloc, ord, build()).unwrap();
    let base = alloc.stack().len().unwrap();
    reg.teardown(&alloc, ord, build()).unwrap();
    assert_eq!(alloc.stack().len().unwrap(), base, "RTTI teardown leaked");

    drop(reg);
    std::fs::remove_file(&schema).ok();
    std::fs::remove_file(&dpath).ok();
}

#[test]
fn interpret_teardown_strong_rc() {
    let schema = temp_path("tds1_schema");
    let reg = rtti::sync(&schema).unwrap();
    let (alloc, dpath) = data_alloc("tds1_data");
    let ord = reg.ordinal_of(<RcHolder as BStackCast>::eightcc()).unwrap();

    // A holder that is the sole strong owner of an `rc` cell. Teardown must
    // decrement the inline refcount to zero and free the cell (+ the root).
    let build = || {
        let cell = RCell::new(&alloc, 5).unwrap();
        let h = RcHolder::new(&alloc, cell).unwrap();
        BStackBlock::range(h.handle()).start()
    };

    reg.teardown(&alloc, ord, build()).unwrap();
    let base = alloc.stack().len().unwrap();
    reg.teardown(&alloc, ord, build()).unwrap();
    assert_eq!(
        alloc.stack().len().unwrap(),
        base,
        "strong(rc) teardown leaked"
    );

    drop(reg);
    std::fs::remove_file(&schema).ok();
    std::fs::remove_file(&dpath).ok();
}

#[test]
fn interpret_teardown_strong_rc_weak() {
    let schema = temp_path("tds2_schema");
    let reg = rtti::sync(&schema).unwrap();
    let (alloc, dpath) = data_alloc("tds2_data");
    let ord = reg.ordinal_of(<WHolder as BStackCast>::eightcc()).unwrap();

    // Sole strong owner of an `(rc, weak)` cell: teardown decrements `ctrl.strong`
    // to zero (frees the data block), then the phantom weak to zero (frees control).
    let build = || {
        let cell = WCell::new(&alloc, 5).unwrap();
        let h = WHolder::new(&alloc, cell).unwrap();
        BStackBlock::range(h.handle()).start()
    };

    reg.teardown(&alloc, ord, build()).unwrap();
    let base = alloc.stack().len().unwrap();
    reg.teardown(&alloc, ord, build()).unwrap();
    assert_eq!(
        alloc.stack().len().unwrap(),
        base,
        "strong(rc,weak) teardown leaked"
    );

    drop(reg);
    std::fs::remove_file(&schema).ok();
    std::fs::remove_file(&dpath).ok();
}

#[test]
fn interpret_teardown_weak_field() {
    let schema = temp_path("tds3_schema");
    let reg = rtti::sync(&schema).unwrap();
    let (alloc, dpath) = data_alloc("tds3_data");
    let ord = reg
        .ordinal_of(<WeakHolder as BStackCast>::eightcc())
        .unwrap();

    // A holder with a *weak* reference to an externally-owned cell. Tearing the
    // holder down must release only the weak (never the still-strong-alive cell);
    // dropping the cell's strong owner afterward reclaims the cell + control. A full
    // cycle must return to baseline — no leak, no premature free.
    let cycle = || {
        let cell = WCell::new(&alloc, 5).unwrap(); // BStackRc: strong=1, weak=1 (phantom)
        let h = WeakHolder::new(&alloc, 7).unwrap();
        h.handle().set_w(&alloc, cell.downgrade().unwrap()).unwrap(); // weak 1 -> 2
        let off = BStackBlock::range(h.handle()).start();
        // Frees the holder root and decrements the cell's weak (2 -> 1); the cell
        // and its control survive because a strong owner is still live.
        reg.teardown(&alloc, ord, off).unwrap();
        // `cell` (BStackRc) drops here: strong 1 -> 0 frees the data, weak 1 -> 0
        // frees the control.
    };

    cycle();
    let base = alloc.stack().len().unwrap();
    cycle();
    assert_eq!(
        alloc.stack().len().unwrap(),
        base,
        "weak-field teardown leaked"
    );

    drop(reg);
    std::fs::remove_file(&schema).ok();
    std::fs::remove_file(&dpath).ok();
}

#[test]
fn interpret_teardown_reclaims_vec_array_and_option() {
    let schema = temp_path("td2_schema");
    let reg = rtti::sync(&schema).unwrap();
    let (alloc, dpath) = data_alloc("td2_data");
    let ord = reg.ordinal_of(<VecArr as BStackCast>::eightcc()).unwrap();

    // A VecArr owns a data-block-backed Vec and an Option<owned Point>.
    let build = || {
        let v = VecArr::new(
            &alloc,
            &[10, 20, 30],
            [1, 2, 3],
            Some(Point::new(&alloc, 8, 9).unwrap()),
        )
        .unwrap();
        BStackBlock::range(v.handle()).start()
    };

    reg.teardown(&alloc, ord, build()).unwrap();
    let base = alloc.stack().len().unwrap();
    reg.teardown(&alloc, ord, build()).unwrap();
    assert_eq!(alloc.stack().len().unwrap(), base, "RTTI teardown leaked");

    drop(reg);
    std::fs::remove_file(&schema).ok();
    std::fs::remove_file(&dpath).ok();
}

fn read_u64(data: &BStack, off: u64) -> u64 {
    let mut b = [0u8; 8];
    data.get_into(off, &mut b).unwrap();
    u64::from_le_bytes(b)
}

fn field_offset(reg: &rtti::RttiRegistry, ord: u32, name: &str) -> u64 {
    match reg.load_type(ord).unwrap().body {
        RttiBody::Struct(f) => f.iter().find(|x| x.name == name).unwrap().offset as u64,
        _ => panic!("not a struct"),
    }
}

#[test]
fn interpret_set_pod_field() {
    let schema = temp_path("set_schema");
    let reg = rtti::sync(&schema).unwrap();
    let (alloc, dpath) = data_alloc("set_data");
    let ord = reg.ordinal_of(<Point as BStackCast>::eightcc()).unwrap();

    let p = Point::new(&alloc, 7, 9).unwrap();
    let off = BStackBlock::range(p.handle()).start();

    // Overwrite `x`; `y` is untouched.
    reg.set_pod(alloc.stack(), ord, "x", off, &42u32.to_le_bytes())
        .unwrap();
    assert_eq!(p.handle().get_x(alloc.stack()).unwrap(), 42);
    assert_eq!(p.handle().get_y(alloc.stack()).unwrap(), 9);

    // Wrong width and unknown field are rejected.
    assert!(reg.set_pod(alloc.stack(), ord, "x", off, &[1, 2]).is_err());
    assert!(
        reg.set_pod(alloc.stack(), ord, "z", off, &0u32.to_le_bytes())
            .is_err()
    );

    p.bstack_drop(&alloc).unwrap();
    drop(reg);
    std::fs::remove_file(&schema).ok();
    std::fs::remove_file(&dpath).ok();
}

#[test]
fn interpret_clone_equal_and_independent() {
    let schema = temp_path("cl1_schema");
    let reg = rtti::sync(&schema).unwrap();
    let (alloc, dpath) = data_alloc("cl1_data");
    let ord = reg.ordinal_of(<Wrap as BStackCast>::eightcc()).unwrap();

    let inner = Point::new(&alloc, 1, 2).unwrap();
    let w = Wrap::new(&alloc, inner, 5).unwrap();
    let src = BStackBlock::range(w.handle()).start();

    // Clone → a distinct, detached root whose Value tree equals the source's.
    let dst = reg.clone_value(&alloc, ord, src).unwrap();
    assert_ne!(dst, src);
    let src_val = reg.read_value(alloc.stack(), ord, src).unwrap();
    assert_eq!(reg.read_value(alloc.stack(), ord, dst).unwrap(), src_val);

    // The owned child was deep-copied: mutating the source leaves the clone intact.
    reg.set_pod(alloc.stack(), ord, "n", src, &99u32.to_le_bytes())
        .unwrap();
    assert_eq!(reg.read_value(alloc.stack(), ord, dst).unwrap(), src_val);
    assert_ne!(reg.read_value(alloc.stack(), ord, src).unwrap(), src_val);

    // Both trees tear down independently (no shared blocks, no double free).
    reg.teardown(&alloc, ord, src).unwrap();
    reg.teardown(&alloc, ord, dst).unwrap();

    drop(reg);
    std::fs::remove_file(&schema).ok();
    std::fs::remove_file(&dpath).ok();
}

#[test]
fn interpret_clone_then_teardown_reclaims() {
    let schema = temp_path("cl2_schema");
    let reg = rtti::sync(&schema).unwrap();
    let (alloc, dpath) = data_alloc("cl2_data");
    let ord = reg.ordinal_of(<VecArr as BStackCast>::eightcc()).unwrap();

    // A full build → clone → teardown-both cycle must return to baseline: the clone
    // reproduces every owned block (vec data, option child) and both are reclaimed.
    let cycle = || {
        let v = VecArr::new(
            &alloc,
            &[10, 20, 30],
            [1, 2, 3],
            Some(Point::new(&alloc, 8, 9).unwrap()),
        )
        .unwrap();
        let src = BStackBlock::range(v.handle()).start();
        let dst = reg.clone_value(&alloc, ord, src).unwrap();
        reg.teardown(&alloc, ord, src).unwrap();
        reg.teardown(&alloc, ord, dst).unwrap();
    };

    cycle();
    let base = alloc.stack().len().unwrap();
    cycle();
    assert_eq!(alloc.stack().len().unwrap(), base, "clone/teardown leaked");

    drop(reg);
    std::fs::remove_file(&schema).ok();
    std::fs::remove_file(&dpath).ok();
}

#[test]
fn interpret_clone_shares_strong() {
    let schema = temp_path("cl3_schema");
    let reg = rtti::sync(&schema).unwrap();
    let (alloc, dpath) = data_alloc("cl3_data");
    let ord = reg.ordinal_of(<RcHolder as BStackCast>::eightcc()).unwrap();
    let soff = field_offset(&reg, ord, "s");

    // The leak oracle also proves sharing: a *copy* of the rc cell would need two
    // frees, but a shared clone bumps the refcount to 2 and both holders tearing
    // down reclaim the single cell exactly once.
    let cycle = || {
        let cell = RCell::new(&alloc, 5).unwrap();
        let h = RcHolder::new(&alloc, cell).unwrap();
        let src = BStackBlock::range(h.handle()).start();
        let dst = reg.clone_value(&alloc, ord, src).unwrap();
        // The strong field points at the *same* cell in both holders (shared).
        assert_eq!(
            read_u64(alloc.stack(), src + soff),
            read_u64(alloc.stack(), dst + soff),
            "strong clone must share the target, not copy it"
        );
        reg.teardown(&alloc, ord, src).unwrap(); // refcount 2 -> 1 (cell alive)
        reg.teardown(&alloc, ord, dst).unwrap(); // refcount 1 -> 0 (cell freed)
    };

    cycle();
    let base = alloc.stack().len().unwrap();
    cycle();
    assert_eq!(alloc.stack().len().unwrap(), base, "strong clone leaked");

    drop(reg);
    std::fs::remove_file(&schema).ok();
    std::fs::remove_file(&dpath).ok();
}
