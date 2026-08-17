//! End-to-end test of the `#[bstack_class]` emitter: the macro must produce the
//! ordinary block machinery **and** register an RTTI descriptor that `rtti::sync`
//! persists, with the right shapes / offsets / sizes.
//!
//! This is its own integration-test binary, so its `linkme` slice collects only the
//! types declared here — isolated from the crate's own unit tests.
#![allow(dead_code)] // some fixtures are inspected only via RTTI, never instantiated

use bstack::{BStack, BStackAllocator, FirstFitBStackAllocator};
use bstack_raii::rtti::{self, RttiBody, Shape};
use bstack_raii::{BStackCast, BStackDrop, BStackOwned, bstack_class};

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
