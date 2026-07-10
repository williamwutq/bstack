//! Runtime tests against a real `BStack` + `FirstFitBStackAllocator`.
//!
//! These stand in for the (not-yet-written) `#[bstack_block]` macro by defining
//! a block type *by hand* — exactly the shape the macro will generate — and
//! exercising the refcount / two-phase-teardown machinery end to end.

use core::mem::size_of;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

use bstack::{BStack, BStackAllocator, BStackOwnedSliceAllocator, BStackRange, FirstFitBStackAllocator};

use crate::layout::{self, BlockHeader};
use crate::{
    alloc_block, alloc_control, dealloc_range, BStackBlock, BStackCast, BStackDrop, BStackRc,
    BStackRef, BStackWeakable, EightCC, TryClone,
};

// --------------------------------------------------------------------------
// Temp-file harness
// --------------------------------------------------------------------------

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A uniquely named temp `.bstack` file, removed on drop.
struct TempStack {
    path: std::path::PathBuf,
}

impl TempStack {
    fn new() -> Self {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut path = std::env::temp_dir();
        path.push(format!("bstack_raii_test_{}_{n}.bstack", std::process::id()));
        let _ = std::fs::remove_file(&path);
        TempStack { path }
    }

    fn open(&self) -> BStack {
        BStack::open(&self.path).unwrap()
    }

    fn allocator(&self) -> FirstFitBStackAllocator {
        FirstFitBStackAllocator::new(self.open()).unwrap()
    }
}

impl Drop for TempStack {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

// --------------------------------------------------------------------------
// A hand-written `#[bstack_block(rc, weak)]`-shaped type with no children
// --------------------------------------------------------------------------

/// Data block payload: header + `ctrl` back-pointer (an on-disk `u64` ref). No
/// user fields, so teardown has no children to recurse into.
#[repr(C)]
#[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
struct TestOnDisk {
    header: BlockHeader,
    ctrl: u64,
}

/// Control block payload: header + `strong`, `weak`, and `x` forward pointer —
/// at offsets 16 / 24 / 32, matching [`layout`].
#[repr(C)]
#[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
struct TestControl {
    header: BlockHeader,
    strong: u64,
    weak: u64,
    x: u64,
}

#[derive(Clone, Copy)]
struct TestBlock(BStackRange);

impl BStackCast for TestBlock {
    fn eightcc() -> EightCC {
        EightCC::from_name("TESTDATA")
    }
}

impl BStackDrop for TestBlock {
    fn bstack_drop<A: BStackOwnedSliceAllocator>(self, allocator: &A) -> io::Result<()> {
        // No owned children: just free the data block itself.
        unsafe { dealloc_range(allocator, self.0) }
    }
}

impl BStackBlock for TestBlock {
    type OnDisk = TestOnDisk;
    fn from_range(range: BStackRange) -> Self {
        TestBlock(range)
    }
    fn range(&self) -> BStackRange {
        self.0
    }
}

impl BStackWeakable for TestBlock {
    type Control = TestControl;
}

fn ctrl_tag() -> EightCC {
    EightCC::from_name("TESTCTRL")
}

/// Allocate and fully wire an `(rc, weak)` `TestBlock` (data + control),
/// returning both ranges. `strong = 1`, `weak = 1` on return.
fn build_rc_weak(alloc: &FirstFitBStackAllocator) -> (BStackRange, BStackRange) {
    let data = alloc_block(alloc, TestBlock::eightcc(), size_of::<TestOnDisk>() as u64).unwrap();
    let ctrl = alloc_control(alloc, ctrl_tag(), data, size_of::<TestControl>() as u64).unwrap();
    (data, ctrl)
}

/// Wrap the data/control ranges into a strong handle accounting for the initial
/// `strong = 1`.
fn rc_of<'a>(
    alloc: &'a FirstFitBStackAllocator,
    data: BStackRange,
    ctrl: BStackRange,
) -> BStackRc<'a, TestBlock, FirstFitBStackAllocator> {
    unsafe { BStackRc::from_raw(BStackRef::from_range(data), Some(ctrl), alloc) }
}

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

#[test]
fn refcount_ops() {
    let tmp = TempStack::new();
    let stack = tmp.open();
    // A single u64 counter living in the mutable region.
    let off = stack.push(1u64.to_le_bytes()).unwrap();

    assert_eq!(crate::refcount::load(&stack, off).unwrap(), 1);
    assert_eq!(crate::refcount::fetch_add(&stack, off, 5).unwrap(), 1); // returns prev
    assert_eq!(crate::refcount::load(&stack, off).unwrap(), 6);
    assert_eq!(crate::refcount::fetch_sub(&stack, off, 2).unwrap(), 6);
    assert_eq!(crate::refcount::load(&stack, off).unwrap(), 4);
    assert_eq!(crate::refcount::increment_if_nonzero(&stack, off).unwrap(), Some(5));

    // Drive to zero, then confirm zero is terminal for increment_if_nonzero.
    assert_eq!(crate::refcount::fetch_sub(&stack, off, 5).unwrap(), 5);
    assert_eq!(crate::refcount::load(&stack, off).unwrap(), 0);
    assert_eq!(crate::refcount::increment_if_nonzero(&stack, off).unwrap(), None);
    assert_eq!(crate::refcount::load(&stack, off).unwrap(), 0);

    // Underflow is an error, not a wrap.
    assert!(crate::refcount::fetch_sub(&stack, off, 1).is_err());
}

#[test]
fn rc_weak_lifecycle() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let dsize = size_of::<TestOnDisk>() as u64;

    let (data, ctrl) = build_rc_weak(&alloc);

    let strong_off = ctrl.start() + layout::CTRL_STRONG_OFFSET;
    let weak_off = ctrl.start() + layout::CTRL_WEAK_OFFSET;
    let load = |o: u64| crate::refcount::load(alloc.stack(), o).unwrap();

    // Initial state and the wired back/forward pointers.
    assert_eq!(load(strong_off), 1);
    assert_eq!(load(weak_off), 1);
    assert_eq!(load(data.start() + layout::CTRL_BACKPTR_OFFSET), ctrl.start());
    assert_eq!(load(ctrl.start() + layout::CTRL_DATA_OFFSET), data.start());

    let rc = rc_of(&alloc, data, ctrl);

    let rc2 = rc.try_clone().unwrap();
    assert_eq!(load(strong_off), 2);

    let weak = rc.downgrade().unwrap();
    assert_eq!(load(weak_off), 2);

    drop(rc2);
    assert_eq!(load(strong_off), 1);

    // upgrade succeeds while a strong owner is alive.
    let rc3 = weak.upgrade().unwrap().expect("still alive");
    assert_eq!(load(strong_off), 2);
    drop(rc3);
    assert_eq!(load(strong_off), 1);

    // Last strong drop: frees the data block and releases the phantom weak
    // (2 -> 1); the control block survives because a real weak handle remains.
    drop(rc);
    assert_eq!(load(strong_off), 0);
    assert_eq!(load(weak_off), 1);

    // upgrade now fails — zero strong is terminal.
    assert!(weak.upgrade().unwrap().is_none());

    // The data block's slot was actually reclaimed: a fresh same-size alloc
    // reuses its offset (first-fit picks the lowest free slot).
    let reused = alloc_block(&alloc, TestBlock::eightcc(), dsize).unwrap();
    assert_eq!(reused.start(), data.start());
    unsafe { dealloc_range(&alloc, reused).unwrap() };

    // Last weak drop frees the control block.
    drop(weak);
}

/// Many threads hammering `try_clone` + `drop` on a shared strong handle. Each
/// iteration is a balanced +1/-1 on `strong`, and the main handle keeps `strong`
/// >= 1 throughout (so no teardown races). If the on-disk RMW were not atomic
/// under contention, lost updates would leave the final count off.
#[test]
fn concurrent_clone_drop() {
    const THREADS: usize = 8;
    const ITERS: usize = 500;

    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let (data, ctrl) = build_rc_weak(&alloc);
    let strong_off = ctrl.start() + layout::CTRL_STRONG_OFFSET;

    let rc = rc_of(&alloc, data, ctrl);

    std::thread::scope(|s| {
        for _ in 0..THREADS {
            let rc = &rc;
            s.spawn(move || {
                for _ in 0..ITERS {
                    let clone = rc.try_clone().unwrap();
                    drop(clone);
                }
            });
        }
    });

    // Only the main handle survives.
    assert_eq!(crate::refcount::load(alloc.stack(), strong_off).unwrap(), 1);
    // Clean teardown: strong -> 0 frees the data block, the phantom release
    // drives weak (1) -> 0 and frees the control block.
    drop(rc);
}

/// Many threads concurrently `upgrade` (from a shared weak) and `try_clone`
/// (from a shared strong). A live strong owner keeps `strong` >= 1 so every
/// upgrade succeeds; each upgraded/cloned handle is balanced by an immediate
/// drop. Stresses `increment_if_nonzero` against `fetch_add`/`fetch_sub`.
#[test]
fn concurrent_upgrade_downgrade() {
    const THREADS: usize = 8;
    const ITERS: usize = 400;

    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let (data, ctrl) = build_rc_weak(&alloc);
    let strong_off = ctrl.start() + layout::CTRL_STRONG_OFFSET;
    let weak_off = ctrl.start() + layout::CTRL_WEAK_OFFSET;

    let rc = rc_of(&alloc, data, ctrl);
    let weak = rc.downgrade().unwrap(); // weak = 2 (phantom + this handle)

    std::thread::scope(|s| {
        for _ in 0..THREADS {
            let rc = &rc;
            let weak = &weak;
            s.spawn(move || {
                for _ in 0..ITERS {
                    if let Some(upgraded) = weak.upgrade().unwrap() {
                        drop(upgraded);
                    }
                    drop(rc.try_clone().unwrap());
                }
            });
        }
    });

    // Both counts returned to their pre-thread values.
    assert_eq!(crate::refcount::load(alloc.stack(), strong_off).unwrap(), 1);
    assert_eq!(crate::refcount::load(alloc.stack(), weak_off).unwrap(), 2);

    drop(weak); // weak 2 -> 1
    drop(rc); // strong -> 0 frees data; phantom release frees control
}
