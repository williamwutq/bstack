//! Runtime tests against a real `BStack` + `FirstFitBStackAllocator`.
//!
//! These stand in for the (not-yet-written) `#[bstack_block]` macro by defining
//! a block type *by hand* — exactly the shape the macro will generate — and
//! exercising the refcount / two-phase-teardown machinery end to end.

use core::mem::size_of;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

use bstack::{
    BStack, BStackAllocator, BStackOwnedSliceAllocator, BStackRange, FirstFitBStackAllocator,
};

use crate::layout::{self, BlockHeader};
use crate::{
    AutoDrop, BStackBlock, BStackCast, BStackCastAs, BStackCastInto, BStackDrop, BStackOwned,
    BStackRc, BStackRef, BStackShared, BStackWeakable, EightCC, TryClone, alloc_block,
    alloc_control, bstack_block, bstack_cast, bstack_enum, bstack_move, dealloc_range,
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
        path.push(format!(
            "bstack_raii_test_{}_{n}.bstack",
            std::process::id()
        ));
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
    assert_eq!(
        crate::refcount::increment_if_nonzero(&stack, off).unwrap(),
        Some(5)
    );

    // Drive to zero, then confirm zero is terminal for increment_if_nonzero.
    assert_eq!(crate::refcount::fetch_sub(&stack, off, 5).unwrap(), 5);
    assert_eq!(crate::refcount::load(&stack, off).unwrap(), 0);
    assert_eq!(
        crate::refcount::increment_if_nonzero(&stack, off).unwrap(),
        None
    );
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
    assert_eq!(
        load(data.start() + layout::CTRL_BACKPTR_OFFSET),
        ctrl.start()
    );
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
/// iteration is a balanced +1/-1 on `strong`, and the main handle keeps at least
/// one strong reference throughout (so no teardown races). If the on-disk RMW
/// were not atomic under contention, lost updates would leave the final count off.
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

// --------------------------------------------------------------------------
// #[bstack_block] macro — recursive teardown of an owned child
// --------------------------------------------------------------------------

#[bstack_block]
struct MacroLeaf {
    val: u32,
}

#[bstack_block]
struct MacroParent {
    #[bstack_owned]
    child: MacroLeaf,
    tag: u32,
}

#[test]
fn macro_recursive_drop() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let leaf_size = size_of::<<MacroLeaf as BStackBlock>::OnDisk>() as u64;
    let parent_size = size_of::<<MacroParent as BStackBlock>::OnDisk>() as u64;

    let leaf = alloc_block(&alloc, MacroLeaf::eightcc(), leaf_size).unwrap();
    let parent = alloc_block(&alloc, MacroParent::eightcc(), parent_size).unwrap();
    // Wire parent.child -> leaf (the first user field sits right after the header).
    alloc
        .stack()
        .set(
            parent.start() + layout::HEADER_SIZE,
            leaf.start().to_le_bytes(),
        )
        .unwrap();

    // Own the parent; freeing it must recursively free the child, then itself.
    let owned = unsafe { BStackOwned::from_raw(<MacroParent as BStackBlock>::from_range(parent)) };
    owned.bstack_drop(&alloc).unwrap();

    // The child's slot (allocated first, so the lowest offset) is reclaimed —
    // proof the generated `bstack_drop` recursed into the owned child.
    let reused = alloc_block(&alloc, MacroLeaf::eightcc(), leaf_size).unwrap();
    assert_eq!(reused.start(), leaf.start());
    unsafe { dealloc_range(&alloc, reused).unwrap() };
}

// --------------------------------------------------------------------------
// AutoDrop: the RAII guard vs. bare / manual teardown
// --------------------------------------------------------------------------

#[test]
fn autodrop_guard_frees_on_scope_exit() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let size = size_of::<<MacroLeaf as BStackBlock>::OnDisk>() as u64;

    let leaf = alloc_block(&alloc, MacroLeaf::eightcc(), size).unwrap();
    let handle = <MacroLeaf as BStackBlock>::from_range(leaf);

    // Wrapping a bare `BStackDrop` handle in `AutoDrop` makes it free on scope
    // exit — the single, reusable auto-drop mechanism.
    let guard = unsafe { AutoDrop::from_raw(handle, &alloc) };
    drop(guard);

    // The slot is reclaimed: the guard's `Drop` ran the teardown.
    let reused = alloc_block(&alloc, MacroLeaf::eightcc(), size).unwrap();
    assert_eq!(reused.start(), leaf.start());
    unsafe { dealloc_range(&alloc, reused).unwrap() };
}

#[test]
fn bare_handle_frees_only_when_asked() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let size = size_of::<<MacroLeaf as BStackBlock>::OnDisk>() as u64;

    let leaf = alloc_block(&alloc, MacroLeaf::eightcc(), size).unwrap();
    let handle = <MacroLeaf as BStackBlock>::from_range(leaf);

    // A bare handle is `Copy` and owns nothing — holding one triggers no
    // teardown, so the block stays live and the next alloc lands elsewhere.
    let other = alloc_block(&alloc, MacroLeaf::eightcc(), size).unwrap();
    assert_ne!(other.start(), leaf.start());

    // Teardown is explicit: invoke `bstack_drop` directly (the "otherwise" path).
    handle.bstack_drop(&alloc).unwrap();
    let reused = alloc_block(&alloc, MacroLeaf::eightcc(), size).unwrap();
    assert_eq!(reused.start(), leaf.start());
    unsafe { dealloc_range(&alloc, reused).unwrap() };
    unsafe { dealloc_range(&alloc, other).unwrap() };
}

// --------------------------------------------------------------------------
// #[bstack_block(rc, weak)] macro — control block + recursive owned child
// --------------------------------------------------------------------------

#[bstack_block(rc, weak)]
struct MacroShared {
    #[bstack_owned]
    child: MacroLeaf,
}

#[test]
fn macro_rc_weak_with_child() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    let leaf_size = size_of::<<MacroLeaf as BStackBlock>::OnDisk>() as u64;
    let data_size = size_of::<<MacroShared as BStackBlock>::OnDisk>() as u64;
    let ctrl_size = size_of::<<MacroShared as BStackWeakable>::Control>() as u64;

    let leaf = alloc_block(&alloc, MacroLeaf::eightcc(), leaf_size).unwrap();
    let data = alloc_block(&alloc, MacroShared::eightcc(), data_size).unwrap();
    // `child` sits after the header and the injected `ctrl` field (16 + 8).
    alloc
        .stack()
        .set(
            data.start() + layout::HEADER_SIZE + 8,
            leaf.start().to_le_bytes(),
        )
        .unwrap();
    let ctrl = alloc_control(&alloc, ctrl_tag(), data, ctrl_size).unwrap();

    let strong_off = ctrl.start() + layout::CTRL_STRONG_OFFSET;
    let weak_off = ctrl.start() + layout::CTRL_WEAK_OFFSET;
    let load = |o: u64| crate::refcount::load(alloc.stack(), o).unwrap();
    assert_eq!(load(strong_off), 1);
    assert_eq!(load(weak_off), 1);

    let rc = unsafe {
        BStackRc::<MacroShared, _>::from_raw(BStackRef::from_range(data), Some(ctrl), &alloc)
    };
    let rc2 = rc.try_clone().unwrap();
    assert_eq!(load(strong_off), 2);
    let weak = rc.downgrade().unwrap();
    assert_eq!(load(weak_off), 2);

    drop(rc2);
    // Last strong drop: frees the data block AND recursively its owned child,
    // then releases the phantom weak (2 -> 1); control survives.
    drop(rc);
    assert_eq!(load(strong_off), 0);
    assert_eq!(load(weak_off), 1);

    assert!(weak.upgrade().unwrap().is_none());
    drop(weak); // frees the control block

    // With leaf + data + control all freed (and coalesced, since they were
    // allocated consecutively), the lowest slot is reclaimable only if the owned
    // child was actually recursively freed — otherwise leaf's slot would still be
    // live and a fresh alloc would land higher.
    let reused = alloc_block(&alloc, MacroLeaf::eightcc(), leaf_size).unwrap();
    assert_eq!(reused.start(), leaf.start());
    unsafe { dealloc_range(&alloc, reused).unwrap() };
}

// --------------------------------------------------------------------------
// #[bstack_strong] — parent drop dispatches through BStackShared to the child
// --------------------------------------------------------------------------

#[bstack_block(rc, weak)]
struct MacroStrongChild {
    val: u32,
}

#[bstack_block]
struct MacroStrongParent {
    #[bstack_strong]
    s: MacroStrongChild,
}

#[test]
fn macro_strong_child() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    let child_data_size = size_of::<<MacroStrongChild as BStackBlock>::OnDisk>() as u64;
    let child_ctrl_size = size_of::<<MacroStrongChild as BStackWeakable>::Control>() as u64;
    let parent_size = size_of::<<MacroStrongParent as BStackBlock>::OnDisk>() as u64;

    let child = alloc_block(&alloc, MacroStrongChild::eightcc(), child_data_size).unwrap();
    let child_ctrl = alloc_control(&alloc, ctrl_tag(), child, child_ctrl_size).unwrap();
    let strong_off = child_ctrl.start() + layout::CTRL_STRONG_OFFSET;
    // A second, keep-alive strong owner besides the parent's `s` field.
    crate::refcount::fetch_add(alloc.stack(), strong_off, 1).unwrap(); // strong = 2

    let parent = alloc_block(&alloc, MacroStrongParent::eightcc(), parent_size).unwrap();
    // `s` is the first user field, right after the header.
    alloc
        .stack()
        .set(
            parent.start() + layout::HEADER_SIZE,
            child.start().to_le_bytes(),
        )
        .unwrap();

    // Freeing the parent runs its generated teardown, which dispatches through
    // BStackShared::drop_strong_ref to decrement the child's strong count.
    let owned =
        unsafe { BStackOwned::from_raw(<MacroStrongParent as BStackBlock>::from_range(parent)) };
    owned.bstack_drop(&alloc).unwrap();
    assert_eq!(crate::refcount::load(alloc.stack(), strong_off).unwrap(), 1); // child survives

    // Release the keep-alive: strong -> 0 frees the child data + control block.
    MacroStrongChild::drop_strong_ref(unsafe { BStackRef::from_range(child) }, &alloc).unwrap();
    let reused = alloc_block(&alloc, MacroStrongChild::eightcc(), child_data_size).unwrap();
    assert_eq!(reused.start(), child.start());
    unsafe { dealloc_range(&alloc, reused).unwrap() };
}

// --------------------------------------------------------------------------
// Generated `new` constructors + field accessors
// --------------------------------------------------------------------------

#[test]
fn macro_new_and_accessors() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Plain-block constructor: allocates and writes the whole payload.
    let leaf = MacroLeaf::new(&alloc, 42).unwrap();
    assert_eq!(leaf.handle().val(stack).unwrap(), 42);

    // Owned child is consumed by the parent constructor (ownership transferred).
    let parent = MacroParent::new(&alloc, leaf, 7).unwrap();
    assert_eq!(parent.handle().tag(stack).unwrap(), 7);

    // Accessor resolves the owned-ref field to the child handle; reading its own
    // field proves the child pointer was wired correctly.
    let child = parent.handle().child(stack).unwrap();
    assert_eq!(child.val(stack).unwrap(), 42);

    // Freeing the parent recursively frees the child then itself; recursion
    // correctness is covered elsewhere.
    parent.bstack_drop(&alloc).unwrap();
}

#[test]
fn macro_new_rc_weak() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // (rc, weak) constructor allocates the data block AND wires a control block.
    let leaf = MacroLeaf::new(&alloc, 99).unwrap();
    let rc = MacroShared::new(&alloc, leaf).unwrap();

    // Traverse through the shared handle to the owned child and read it.
    assert_eq!(rc.handle().child(stack).unwrap().val(stack).unwrap(), 99);

    // Full shared lifecycle on a constructor-built block.
    let rc2 = rc.try_clone().unwrap();
    let weak = rc.downgrade().unwrap();
    drop(rc2);
    drop(rc);
    assert!(weak.upgrade().unwrap().is_none());
    drop(weak);
}

// --------------------------------------------------------------------------
// #[bstack_weak] field — constructor (null init), setter, upgrade accessor, and
// sound teardown when the target's data is freed first (the cycle case).
// --------------------------------------------------------------------------

#[bstack_block(rc, weak)]
struct WNode {
    #[bstack_weak]
    back: WNode,
    val: u32,
}

#[test]
fn macro_weak_field_cycle() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    // Constructor works for a weak-field block; `back` starts null.
    let a = WNode::new(&alloc, 1).unwrap();
    let b = WNode::new(&alloc, 2).unwrap();
    let a_data = a.handle().range().start(); // lowest allocation

    // Setter wires b.back -> a as a weak reference.
    b.handle().set_back(&alloc, a.downgrade().unwrap()).unwrap();

    // Upgrade accessor resolves the live target.
    let up = b.handle().back(&alloc).unwrap().expect("a is alive");
    assert_eq!(up.handle().val(alloc.stack()).unwrap(), 1);
    drop(up);

    // Drop the strong owner `a` first: its DATA block is freed, but its control
    // block survives because b.back still holds a weak count.
    drop(a);

    // The weak field can no longer upgrade — and reaching this did NOT read a's
    // freed data block, because the field stores a's control offset.
    assert!(b.handle().back(&alloc).unwrap().is_none());

    // Dropping `b` releases b.back's weak on a's control block (freeing it), then
    // frees b. No use-after-free of a's data.
    drop(b);

    // Everything (a data+control, b data+control) is freed and coalesced, so the
    // lowest slot — a's — is reclaimed.
    let reused = alloc_block(
        &alloc,
        WNode::eightcc(),
        size_of::<<WNode as BStackBlock>::OnDisk>() as u64,
    )
    .unwrap();
    assert_eq!(reused.start(), a_data);
    unsafe { dealloc_range(&alloc, reused).unwrap() };
}

// --------------------------------------------------------------------------
// bstack_move! — destructure an owned block into its field handles
// --------------------------------------------------------------------------

#[test]
fn macro_bstack_move() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let leaf = MacroLeaf::new(&alloc, 55).unwrap();
    let leaf_off = leaf.handle().range().start();
    let parent = MacroParent::new(&alloc, leaf, 7).unwrap();

    // Move the fields out: owned child -> BStackOwned<MacroLeaf>, tag -> u32.
    // A bare owned handle carries no allocator, so pass it explicitly.
    let (child, tag) = bstack_move!(parent, &alloc).unwrap();
    assert_eq!(tag, 7);

    // Ownership of the child transferred (same allocation), and it is still live
    // because bstack_move! frees only the parent shell.
    assert_eq!(child.handle().range().start(), leaf_off);
    assert_eq!(child.handle().val(stack).unwrap(), 55);

    // Freeing the moved-out child frees the leaf. With the parent shell already
    // freed, both slots coalesce and the lowest (leaf's) is reclaimed.
    child.bstack_drop(&alloc).unwrap();
    let reused = alloc_block(
        &alloc,
        MacroLeaf::eightcc(),
        size_of::<<MacroLeaf as BStackBlock>::OnDisk>() as u64,
    )
    .unwrap();
    assert_eq!(reused.start(), leaf_off);
    unsafe { dealloc_range(&alloc, reused).unwrap() };
}

// A plain block whose fields reference shared blocks (both `(rc, weak)`).
#[bstack_block]
struct MoveHolder {
    #[bstack_strong]
    s: MacroStrongChild,
    #[bstack_weak]
    w: WNode,
    n: u32,
}

#[test]
fn macro_bstack_move_shared() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let sc = MacroStrongChild::new(&alloc, 88).unwrap(); // BStackRc, strong = 1
    let wt = WNode::new(&alloc, 3).unwrap(); // the weak target

    // The strong field consumes `sc` (transferring its strong count); the weak
    // field is wired after construction.
    let holder = MoveHolder::new(&alloc, sc, 5).unwrap();
    holder
        .handle()
        .set_w(&alloc, wt.downgrade().unwrap())
        .unwrap();

    // Move every field out: strong -> BStackRc, weak -> Option<BStackWeak>, pod.
    let (moved_s, moved_w, n) = bstack_move!(holder, &alloc).unwrap();
    assert_eq!(n, 5);

    // The strong field came back as a live BStackRc.
    assert_eq!(moved_s.handle().val(stack).unwrap(), 88);

    // The weak field came back as Some(weak) and still upgrades (target alive).
    let up = moved_w
        .as_ref()
        .unwrap()
        .upgrade()
        .unwrap()
        .expect("wt alive");
    assert_eq!(up.handle().val(stack).unwrap(), 3);
    drop(up);

    // Clean teardown across the moved-out handles.
    drop(moved_s); // frees the strong child
    drop(moved_w); // releases the weak on wt's control block
    drop(wt); // frees wt (data + control)
}

// --------------------------------------------------------------------------
// EightCC tag generation: readable prefix + non-printable hash tail
// --------------------------------------------------------------------------

#[bstack_block]
struct SomeAbstractThing {
    x: u32,
}

#[bstack_block]
struct ABlock {
    x: u32,
}

#[bstack_block(tag = "OVR")]
struct Overridden {
    x: u32,
}

#[bstack_block(rc, weak)]
struct TagCtrl {
    x: u32,
}

// Same forced prefix, different type names → hash tails must differ.
#[bstack_block(tag = "SAME")]
struct SameA {
    x: u32,
}
#[bstack_block(tag = "SAME")]
struct SameB {
    x: u32,
}

// Overlong override is truncated to 8 bytes (warning silenced).
#[bstack_block(tag = "TOOLONGTAG12", allow(overlong_tag))]
struct Truncated {
    x: u32,
}

#[test]
fn macro_tag_generation() {
    // CamelCase initials, and the tail is the high-bit (non-printable) hash.
    let t = SomeAbstractThing::eightcc().0;
    assert_eq!(&t[0..3], b"SAT");
    assert!(t[3..].iter().all(|&b| b & 0x80 != 0));

    // Two-word initials.
    assert_eq!(&ABlock::eightcc().0[0..2], b"AB");

    // Manual prefix override.
    let o = Overridden::eightcc().0;
    assert_eq!(&o[0..3], b"OVR");
    assert!(o[3..].iter().all(|&b| b & 0x80 != 0));

    // Same prefix, different names → identical prefix, different hash tails.
    let a = SameA::eightcc().0;
    let b = SameB::eightcc().0;
    assert_eq!(&a[0..4], b"SAME");
    assert_eq!(&b[0..4], b"SAME");
    assert_ne!(a[4..], b[4..]);

    // Overlong override truncated to the first 8 bytes.
    assert_eq!(&Truncated::eightcc().0, b"TOOLONGT");
}

#[test]
fn macro_control_tag_is_lowercased() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let rc = TagCtrl::new(&alloc, 1).unwrap();
    let data_off = rc.handle().range().start();

    // data.__bstack_ctrl (offset 16) -> control block offset.
    let mut buf = [0u8; 8];
    stack
        .get_into(data_off + layout::CTRL_BACKPTR_OFFSET, &mut buf)
        .unwrap();
    let ctrl_off = u64::from_le_bytes(buf);

    // Control block's header tag lives at ctrl_off + 8 (after size: u64).
    let mut ctrl_tag = [0u8; 8];
    stack.get_into(ctrl_off + 8, &mut ctrl_tag).unwrap();

    let data_tag = TagCtrl::eightcc().0; // prefix "TC"
    assert_eq!(&data_tag[0..2], b"TC");
    // Control tag = data tag with the prefix lowercased, same hash tail.
    assert_eq!(&ctrl_tag[0..2], b"tc");
    assert_eq!(ctrl_tag[2..], data_tag[2..]);

    drop(rc);
}

// --------------------------------------------------------------------------
// bstack_cast! + cast methods — typed <-> untyped conversion
// --------------------------------------------------------------------------

#[test]
fn macro_cast() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let leaf = MacroLeaf::new(&alloc, 9).unwrap();

    // Borrowed: upcast via the generated `as_slice`, downcast via method + macro.
    let sl = leaf.handle().as_slice(stack);
    assert_eq!(
        sl.cast_as::<MacroLeaf>()
            .unwrap()
            .unwrap()
            .val(stack)
            .unwrap(),
        9
    );
    assert!(sl.cast_as::<MacroParent>().unwrap().is_none()); // wrong tag
    assert!(bstack_cast!(sl as MacroLeaf).unwrap().is_some());
    assert!(bstack_cast!(sl as MacroParent).unwrap().is_none());

    // Owned upcast (macro) — a bare owned handle is wrapped (`auto`) to attach an
    // allocator first — then a wrong-type downcast hands the slice back.
    let slice = bstack_cast!(leaf.auto(&alloc) as BStackOwnedSlice);
    let slice = match slice.cast_into::<MacroParent>().unwrap() {
        Ok(_) => panic!("tag should not match"),
        Err(s) => s,
    };

    // Correct owned downcast (macro) round-trips to the typed (bare) handle.
    let owned = bstack_cast!(slice as BStackOwned<MacroLeaf, _>)
        .unwrap()
        .ok()
        .unwrap();
    assert_eq!(owned.handle().val(stack).unwrap(), 9);
    owned.bstack_drop(&alloc).unwrap(); // frees the leaf
}

// --------------------------------------------------------------------------
// bstack_move! on a BStackRc — try_unwrap-style, solo strong owner only
// --------------------------------------------------------------------------

#[bstack_block(rc)]
struct RcHolder {
    #[bstack_owned]
    leaf: MacroLeaf,
    n: u32,
}

#[bstack_block(rc, weak)]
struct RcwHolder {
    #[bstack_owned]
    leaf: MacroLeaf,
    n: u32,
}

#[test]
fn macro_bstack_move_rc() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let leaf = MacroLeaf::new(&alloc, 5).unwrap();
    let leaf_off = leaf.handle().range().start();
    let rc = RcHolder::new(&alloc, leaf, 7).unwrap(); // BStackRc<RcHolder>, strong = 1

    // A second strong owner blocks the move.
    let clone = rc.try_clone().unwrap(); // strong = 2
    let rc = match bstack_move!(rc).unwrap() {
        Ok(_) => panic!("must not move a shared block"),
        Err(rc) => rc, // handed back, untouched
    };
    drop(clone); // strong = 1 — now the sole owner

    // Sole owner: the move succeeds and transfers the owned child out.
    let (moved_leaf, n) = bstack_move!(rc).unwrap().ok().expect("sole owner");
    assert_eq!(n, 7);
    assert_eq!(moved_leaf.handle().val(stack).unwrap(), 5);

    // Only the RcHolder shell was freed; the child is still live. Freeing it
    // reclaims the last block, so the lowest slot (the leaf's) comes back.
    moved_leaf.bstack_drop(&alloc).unwrap();
    let reused = alloc_block(
        &alloc,
        MacroLeaf::eightcc(),
        size_of::<<MacroLeaf as BStackBlock>::OnDisk>() as u64,
    )
    .unwrap();
    assert_eq!(reused.start(), leaf_off);
    unsafe { dealloc_range(&alloc, reused).unwrap() };
}

#[test]
fn macro_bstack_move_rc_weak() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let leaf = MacroLeaf::new(&alloc, 9).unwrap();
    let rc = RcwHolder::new(&alloc, leaf, 3).unwrap();
    let weak = rc.downgrade().unwrap(); // a weak observer does NOT block the move

    // Sole *strong* owner: move succeeds even with a weak outstanding.
    let (moved_leaf, n) = bstack_move!(rc).unwrap().ok().expect("sole strong owner");
    assert_eq!(n, 3);
    assert_eq!(moved_leaf.handle().val(stack).unwrap(), 9);

    // The data block is gone, so the weak can no longer upgrade.
    assert!(weak.upgrade().unwrap().is_none());

    moved_leaf.bstack_drop(&alloc).unwrap(); // frees the moved-out child
    drop(weak); // frees the now-unreferenced control block
}

// --------------------------------------------------------------------------
// Option<Thing> — nullable reference fields (0 == None)
// --------------------------------------------------------------------------

#[bstack_block]
struct OptHolder {
    #[bstack_owned]
    child: Option<MacroLeaf>,
    n: u32,
}

#[test]
fn macro_option_owned() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Some: constructor takes Option<BStackOwned<_>>, accessor returns Option.
    let leaf = MacroLeaf::new(&alloc, 42).unwrap();
    let leaf_off = leaf.handle().range().start();
    let holder = OptHolder::new(&alloc, Some(leaf), 7).unwrap();
    assert_eq!(holder.handle().n(stack).unwrap(), 7);
    let got = holder.handle().child(stack).unwrap();
    assert_eq!(got.unwrap().val(stack).unwrap(), 42);

    // bstack_move! yields Option<BStackOwned<_>>.
    let (moved_child, n) = bstack_move!(holder, &alloc).unwrap();
    assert_eq!(n, 7);
    assert_eq!(
        moved_child.as_ref().unwrap().handle().val(stack).unwrap(),
        42
    );
    moved_child.unwrap().bstack_drop(&alloc).unwrap(); // frees the leaf

    // The leaf + holder shell are both freed; the lowest slot (leaf's) returns.
    let reused = alloc_block(
        &alloc,
        MacroLeaf::eightcc(),
        size_of::<<MacroLeaf as BStackBlock>::OnDisk>() as u64,
    )
    .unwrap();
    assert_eq!(reused.start(), leaf_off);
    unsafe { dealloc_range(&alloc, reused).unwrap() };

    // None: no child, accessor is None, teardown skips the null field cleanly.
    let empty = OptHolder::new(&alloc, None, 9).unwrap();
    assert_eq!(empty.handle().n(stack).unwrap(), 9);
    assert!(empty.handle().child(stack).unwrap().is_none());
    empty.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// BStackVec — persistent growable POD vector via the descriptor indirection
// --------------------------------------------------------------------------

#[test]
fn bstack_vec_grow_and_free() {
    use crate::BStackVec;

    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    // Build a detached vector from a slice, read it back.
    let mut v = BStackVec::<u8, _>::from_slice(&alloc, b"hello").unwrap();
    assert_eq!(v.len().unwrap(), 5);
    assert_eq!(v.to_vec().unwrap(), b"hello");

    // A detached vector carries its descriptor in memory: as it grows, the
    // descriptor tracks the (reallocating) data block.
    let before = v.descriptor().data_size;
    for &b in b", world!" {
        v.push(b).unwrap();
    }
    assert!(v.descriptor().data_size >= before); // block tracks growth
    assert_eq!(v.to_vec().unwrap(), b"hello, world!");
    assert_eq!(v.len().unwrap(), 13);

    // Free the data block (there is no descriptor block).
    v.bstack_drop().unwrap();

    // Allocator is healthy afterwards: a fresh vector round-trips.
    let v2 = BStackVec::<u8, _>::from_slice(&alloc, b"again").unwrap();
    assert_eq!(v2.to_vec().unwrap(), b"again");
    v2.bstack_drop().unwrap();

    // A larger POD element type also works (unaligned reads).
    let mut nums = BStackVec::<u32, _>::from_slice(&alloc, &[1u32, 2, 3]).unwrap();
    nums.push(4).unwrap();
    assert_eq!(nums.to_vec().unwrap(), vec![1u32, 2, 3, 4]);
    assert_eq!(nums.len().unwrap(), 4);
    nums.bstack_drop().unwrap();
}

// --------------------------------------------------------------------------
// Vec<T> / String fields (POD elements) via BStackVec
// --------------------------------------------------------------------------

#[bstack_block]
struct Record {
    // POD vectors are un-annotated (an annotation would mean block elements).
    name: String,
    tags: Vec<u32>,
    id: u64,
}

#[test]
fn macro_vec_string_fields() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Constructor takes `&str` for String and `&[T]` for Vec<T>.
    let rec = Record::new(&alloc, "hello", &[1u32, 2, 3], 42).unwrap();
    assert_eq!(rec.handle().id(stack).unwrap(), 42);

    // Accessors return BStackVec handles (take the allocator).
    assert_eq!(
        rec.handle().name(&alloc).unwrap().to_vec().unwrap(),
        b"hello"
    );
    assert_eq!(
        rec.handle().tags(&alloc).unwrap().to_vec().unwrap(),
        vec![1u32, 2, 3]
    );

    // Mutate through the handle: the field points at the stable descriptor, so
    // growth (even if the data block moves) is visible on the next read.
    let mut tags = rec.handle().tags(&alloc).unwrap();
    tags.push(4).unwrap();
    assert_eq!(
        rec.handle().tags(&alloc).unwrap().to_vec().unwrap(),
        vec![1u32, 2, 3, 4],
    );

    // Freeing the record frees both vectors (data + descriptor) and the record.
    rec.bstack_drop(&alloc).unwrap();

    // Allocator is healthy: a fresh record round-trips.
    let rec2 = Record::new(&alloc, "again", &[9u32], 1).unwrap();
    assert_eq!(
        rec2.handle().name(&alloc).unwrap().to_vec().unwrap(),
        b"again"
    );
    assert_eq!(
        rec2.handle().tags(&alloc).unwrap().to_vec().unwrap(),
        vec![9u32]
    );
    rec2.bstack_drop(&alloc).unwrap();
}

#[test]
fn macro_vec_bstack_move() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    let rec = Record::new(&alloc, "movable", &[7u32, 8], 5).unwrap();
    // bstack_move! yields the BStackVec handles + the POD.
    let (name, tags, id) = bstack_move!(rec, &alloc).unwrap();
    assert_eq!(id, 5);
    assert_eq!(name.to_vec().unwrap(), b"movable");
    assert_eq!(tags.to_vec().unwrap(), vec![7u32, 8]);
    // The vectors are now independently owned; free them.
    name.bstack_drop().unwrap();
    tags.bstack_drop().unwrap();
}

// --------------------------------------------------------------------------
// #[bstack_owned] Vec<Thing> — a vector of owned block children
// --------------------------------------------------------------------------

#[bstack_block]
struct Tree {
    #[bstack_owned]
    kids: Vec<MacroLeaf>,
    label: u32,
}

#[test]
fn macro_owned_block_vec() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Allocate three owned leaves, then a Tree that owns them.
    let kids = vec![
        MacroLeaf::new(&alloc, 10).unwrap(),
        MacroLeaf::new(&alloc, 20).unwrap(),
        MacroLeaf::new(&alloc, 30).unwrap(),
    ];
    let first_off = kids[0].handle().range().start(); // lowest allocation
    let tree = Tree::new(&alloc, kids, 7).unwrap();
    assert_eq!(tree.handle().label(stack).unwrap(), 7);

    // Accessor resolves to a BStackBlockVec; read the children back.
    let v = tree.handle().kids(&alloc).unwrap();
    assert_eq!(v.len().unwrap(), 3);
    let vals: Vec<u32> = v
        .to_vec()
        .unwrap()
        .iter()
        .map(|k| k.val(stack).unwrap())
        .collect();
    assert_eq!(vals, vec![10, 20, 30]);
    assert_eq!(v.get(1).unwrap().unwrap().val(stack).unwrap(), 20);
    assert!(v.get(3).unwrap().is_none());

    // Freeing the tree recursively frees every owned child, plus the offset
    // array and descriptor. The lowest child slot returns as proof.
    tree.bstack_drop(&alloc).unwrap();
    let reused = alloc_block(
        &alloc,
        MacroLeaf::eightcc(),
        size_of::<<MacroLeaf as BStackBlock>::OnDisk>() as u64,
    )
    .unwrap();
    assert_eq!(reused.start(), first_off);
    unsafe { dealloc_range(&alloc, reused).unwrap() };
}

#[test]
fn macro_owned_block_vec_move() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let kids = vec![
        MacroLeaf::new(&alloc, 1).unwrap(),
        MacroLeaf::new(&alloc, 2).unwrap(),
    ];
    let tree = Tree::new(&alloc, kids, 9).unwrap();

    // bstack_move! transfers the vector out (children stay live); only the Tree
    // shell is freed.
    let (kids_vec, label) = bstack_move!(tree, &alloc).unwrap();
    assert_eq!(label, 9);
    assert_eq!(kids_vec.len().unwrap(), 2);
    assert_eq!(kids_vec.get(0).unwrap().unwrap().val(stack).unwrap(), 1);

    // The moved-out vector is independently owned; free it (children + arrays).
    kids_vec.bstack_drop().unwrap();
}

// --------------------------------------------------------------------------
// #[bstack_strong] / #[bstack_weak] / #[bstack_ref] Vec<Thing> — block-element
// vectors whose annotation states the *elements'* ownership
// --------------------------------------------------------------------------

#[bstack_block]
struct StrongList {
    #[bstack_strong]
    items: Vec<MacroStrongChild>,
    n: u32,
}

/// Read a block's strong count via its data-block `ctrl` back-pointer.
fn strong_of(stack: &BStack, data_off: u64) -> u64 {
    let mut buf = [0u8; 8];
    stack
        .get_into(data_off + layout::CTRL_BACKPTR_OFFSET, &mut buf)
        .unwrap();
    let ctrl = u64::from_le_bytes(buf);
    crate::refcount::load(stack, ctrl + layout::CTRL_STRONG_OFFSET).unwrap()
}

#[test]
fn macro_strong_block_vec() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let a = MacroStrongChild::new(&alloc, 100).unwrap(); // BStackRc, strong = 1
    let b = MacroStrongChild::new(&alloc, 200).unwrap();
    let a_clone = a.try_clone().unwrap(); // a strong = 2
    let a_data = a_clone.handle().range().start();

    // The strong vector consumes each Rc, transferring its strong count.
    let list = StrongList::new(&alloc, vec![a, b], 3).unwrap();
    assert_eq!(strong_of(stack, a_data), 2); // list + a_clone

    let v = list.handle().items(&alloc).unwrap();
    assert_eq!(v.len().unwrap(), 2);
    assert_eq!(v.get(0).unwrap().unwrap().val(stack).unwrap(), 100);

    // Freeing the list releases every element's strong ref: `b` (sole owner) is
    // freed; `a` survives via `a_clone`.
    list.bstack_drop(&alloc).unwrap();
    assert_eq!(strong_of(stack, a_data), 1); // a_clone only

    assert_eq!(a_clone.handle().val(stack).unwrap(), 100);
    drop(a_clone); // a freed now
}

#[bstack_block]
struct WeakList {
    #[bstack_weak]
    watchers: Vec<MacroStrongChild>,
    n: u32,
}

#[test]
fn macro_weak_block_vec() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let a = MacroStrongChild::new(&alloc, 1).unwrap(); // strong owner
    let b = MacroStrongChild::new(&alloc, 2).unwrap();

    // The weak vector consumes each downgraded weak handle.
    let list = WeakList::new(
        &alloc,
        vec![a.downgrade().unwrap(), b.downgrade().unwrap()],
        5,
    )
    .unwrap();

    let v = list.handle().watchers(&alloc).unwrap();
    assert_eq!(v.len().unwrap(), 2);

    // Upgrade element 0 while `a` is alive.
    let up = v.upgrade(0).unwrap().expect("a alive");
    assert_eq!(up.handle().val(stack).unwrap(), 1);
    drop(up);

    // Drop `a`'s data block: element 0 can no longer upgrade (sound — the vector
    // stores control offsets, not freed data offsets).
    drop(a);
    let v = list.handle().watchers(&alloc).unwrap();
    assert!(v.upgrade(0).unwrap().is_none());
    assert!(v.upgrade(1).unwrap().is_some()); // b still alive

    // Teardown releases each weak count (freeing control blocks at zero).
    list.bstack_drop(&alloc).unwrap();
    drop(b);
}

#[bstack_block]
struct RefList {
    #[bstack_ref]
    links: Vec<MacroLeaf>,
    n: u32,
}

#[test]
fn macro_ref_block_vec() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Standalone leaves owned by us; the list only references them.
    let a = MacroLeaf::new(&alloc, 7).unwrap();
    let b = MacroLeaf::new(&alloc, 8).unwrap();
    let refs = vec![
        unsafe { BStackRef::from_range(a.handle().range()) },
        unsafe { BStackRef::from_range(b.handle().range()) },
    ];
    let list = RefList::new(&alloc, refs, 9).unwrap();

    let v = list.handle().links(&alloc).unwrap();
    assert_eq!(v.len().unwrap(), 2);
    assert_eq!(v.get(1).unwrap().unwrap().val(stack).unwrap(), 8);

    // Freeing the list frees only the offset array + descriptor, not the targets.
    list.bstack_drop(&alloc).unwrap();
    assert_eq!(a.handle().val(stack).unwrap(), 7); // still alive
    assert_eq!(b.handle().val(stack).unwrap(), 8);

    a.bstack_drop(&alloc).unwrap();
    b.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// Option<Vec<T>> / Option<String> — nullable vectors via the data_off==0 niche
// --------------------------------------------------------------------------

#[bstack_block]
struct OptVec {
    tags: Option<Vec<u32>>,
    name: Option<String>,
    id: u64,
}

#[test]
fn macro_option_vec() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Some: constructor takes Option<&[T]> / Option<&str>; accessors resolve.
    let a = OptVec::new(&alloc, Some(&[1u32, 2, 3][..]), Some("hi"), 7).unwrap();
    assert_eq!(a.handle().id(stack).unwrap(), 7);
    assert_eq!(
        a.handle()
            .tags(&alloc)
            .unwrap()
            .expect("some")
            .to_vec()
            .unwrap(),
        vec![1u32, 2, 3]
    );
    assert_eq!(
        a.handle()
            .name(&alloc)
            .unwrap()
            .expect("some")
            .to_vec()
            .unwrap(),
        b"hi"
    );

    // bstack_move! yields Option<BStackVec<_>>; free the moved-out vectors.
    let (tags, name, id) = bstack_move!(a, &alloc).unwrap();
    assert_eq!(id, 7);
    tags.unwrap().bstack_drop().unwrap();
    name.unwrap().bstack_drop().unwrap();

    // None: `0` niche — accessors are None, teardown frees nothing extra.
    let b = OptVec::new(&alloc, None, None, 9).unwrap();
    assert_eq!(b.handle().id(stack).unwrap(), 9);
    assert!(b.handle().tags(&alloc).unwrap().is_none());
    assert!(b.handle().name(&alloc).unwrap().is_none());
    b.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// #[bstack_enum] — a tagged-union block (unit / POD / owned / ref variants)
// --------------------------------------------------------------------------

#[bstack_enum]
enum Node {
    Empty,
    Num(u32),
    #[bstack_ref]
    Link(MacroLeaf),
    #[bstack_owned]
    Child(MacroLeaf),
}

#[test]
fn macro_enum_basic() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let leaf_size = size_of::<<MacroLeaf as BStackBlock>::OnDisk>() as u64;

    // Unit variant.
    let e = Node::new(&alloc, NodeData::Empty).unwrap();
    assert!(matches!(e.handle().read(&alloc).unwrap(), NodeView::Empty));
    e.bstack_drop(&alloc).unwrap();

    // POD variant: value stored inline, read back.
    let e = Node::new(&alloc, NodeData::Num(42)).unwrap();
    match e.handle().read(&alloc).unwrap() {
        NodeView::Num(n) => assert_eq!(n, 42),
        _ => panic!("expected Num"),
    }
    e.bstack_drop(&alloc).unwrap();

    // Owned variant: the enum owns the child; dropping it recursively frees it.
    let leaf = MacroLeaf::new(&alloc, 7).unwrap();
    let leaf_off = leaf.handle().range().start();
    let e = Node::new(&alloc, NodeData::Child(leaf)).unwrap();
    match e.handle().read(&alloc).unwrap() {
        NodeView::Child(c) => assert_eq!(c.val(stack).unwrap(), 7),
        _ => panic!("expected Child"),
    }
    e.bstack_drop(&alloc).unwrap();
    let reused = alloc_block(&alloc, MacroLeaf::eightcc(), leaf_size).unwrap();
    assert_eq!(reused.start(), leaf_off); // child slot reclaimed => teardown recursed
    unsafe { dealloc_range(&alloc, reused).unwrap() };

    // Ref variant: references a leaf it does NOT own; dropping the enum leaves it.
    let keep = MacroLeaf::new(&alloc, 9).unwrap();
    let link = unsafe { BStackRef::from_range(keep.handle().range()) };
    let e = Node::new(&alloc, NodeData::Link(link)).unwrap();
    match e.handle().read(&alloc).unwrap() {
        NodeView::Link(l) => assert_eq!(l.val(stack).unwrap(), 9),
        _ => panic!("expected Link"),
    }
    e.bstack_drop(&alloc).unwrap();
    assert_eq!(keep.handle().val(stack).unwrap(), 9); // still alive
    keep.bstack_drop(&alloc).unwrap();
}

// An enum used as an owned field of a struct — enums compose as referenced blocks.
#[bstack_block]
struct EnumHolder {
    #[bstack_owned]
    node: Node,
    tag: u32,
}

#[test]
fn macro_enum_as_field() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let leaf_size = size_of::<<MacroLeaf as BStackBlock>::OnDisk>() as u64;

    let leaf = MacroLeaf::new(&alloc, 5).unwrap();
    let leaf_off = leaf.handle().range().start();
    let node = Node::new(&alloc, NodeData::Child(leaf)).unwrap();
    let holder = EnumHolder::new(&alloc, node, 3).unwrap();
    assert_eq!(holder.handle().tag(stack).unwrap(), 3);

    // Traverse struct -> enum -> owned child.
    let node = holder.handle().node(stack).unwrap();
    match node.read(&alloc).unwrap() {
        NodeView::Child(c) => assert_eq!(c.val(stack).unwrap(), 5),
        _ => panic!("expected Child"),
    }

    // Freeing the struct recursively frees the enum and its owned child.
    holder.bstack_drop(&alloc).unwrap();
    let reused = alloc_block(&alloc, MacroLeaf::eightcc(), leaf_size).unwrap();
    assert_eq!(reused.start(), leaf_off);
    unsafe { dealloc_range(&alloc, reused).unwrap() };
}

// --------------------------------------------------------------------------
// #[bstack_enum(rc)] / (rc, weak) — refcounted / weak-observable enum blocks
// --------------------------------------------------------------------------

#[bstack_enum(rc)]
enum RcNode {
    Empty,
    Val(u32),
    #[bstack_owned]
    Child(MacroLeaf),
}

#[test]
fn macro_enum_rc() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let leaf_size = size_of::<<MacroLeaf as BStackBlock>::OnDisk>() as u64;

    let leaf = MacroLeaf::new(&alloc, 4).unwrap();
    let leaf_off = leaf.handle().range().start();
    let rc = RcNode::new(&alloc, RcNodeData::Child(leaf)).unwrap(); // BStackRc, strong = 1
    let rc2 = rc.try_clone().unwrap(); // strong = 2

    match rc.handle().read(&alloc).unwrap() {
        RcNodeView::Child(c) => assert_eq!(c.val(stack).unwrap(), 4),
        _ => panic!("expected Child"),
    }

    drop(rc); // strong = 1 — still alive
    drop(rc2); // strong = 0 — frees the enum block AND its owned child

    let reused = alloc_block(&alloc, MacroLeaf::eightcc(), leaf_size).unwrap();
    assert_eq!(reused.start(), leaf_off); // child reclaimed => teardown recursed
    unsafe { dealloc_range(&alloc, reused).unwrap() };
}

#[bstack_enum(rc, weak)]
enum RcwNode {
    Nil,
    #[bstack_owned]
    One(MacroLeaf),
}

#[test]
fn macro_enum_rc_weak() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let leaf = MacroLeaf::new(&alloc, 8).unwrap();
    let rc = RcwNode::new(&alloc, RcwNodeData::One(leaf)).unwrap(); // BStackRc, strong = 1
    let weak = rc.downgrade().unwrap();

    match rc.handle().read(&alloc).unwrap() {
        RcwNodeView::One(c) => assert_eq!(c.val(stack).unwrap(), 8),
        _ => panic!("expected One"),
    }

    // Upgrade succeeds while the strong owner is alive.
    let up = weak.upgrade().unwrap().expect("alive");
    assert!(matches!(
        up.handle().read(&alloc).unwrap(),
        RcwNodeView::One(_)
    ));
    drop(up);

    // Last strong drop frees the data block (and its owned child); control
    // survives while a weak handle remains, so upgrade now fails.
    drop(rc);
    assert!(weak.upgrade().unwrap().is_none());
    drop(weak); // frees the control block
}

// --------------------------------------------------------------------------
// #[bstack_strong] / #[bstack_weak] enum variants — a variant holding a shared
// or weak reference (MacroStrongChild is #[bstack_block(rc, weak)]).
// --------------------------------------------------------------------------

#[bstack_enum]
enum Cell {
    Nil,
    #[bstack_strong]
    Shared(MacroStrongChild),
    #[bstack_weak]
    Watch(MacroStrongChild),
}

#[test]
fn macro_enum_strong_weak_variants() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Strong variant: consumes a BStackRc, the enum holds one strong reference.
    let child = MacroStrongChild::new(&alloc, 11).unwrap(); // strong = 1
    let keep = child.try_clone().unwrap(); // strong = 2 (observe after the enum drops)
    let cell = Cell::new(&alloc, CellData::Shared(child)).unwrap(); // consumes child's ref
    match cell.handle().read(&alloc).unwrap() {
        CellView::Shared(c) => assert_eq!(c.val(stack).unwrap(), 11),
        _ => panic!("expected Shared"),
    }
    cell.bstack_drop(&alloc).unwrap(); // releases the enum's strong ref (strong = 1)
    assert_eq!(keep.handle().val(stack).unwrap(), 11); // still alive
    drop(keep); // strong = 0 — freed

    // Weak variant: consumes a BStackWeak; reading upgrades it.
    let owner = MacroStrongChild::new(&alloc, 22).unwrap(); // strong owner
    let cell = Cell::new(&alloc, CellData::Watch(owner.downgrade().unwrap())).unwrap();
    match cell.handle().read(&alloc).unwrap() {
        CellView::Watch(Some(up)) => assert_eq!(up.handle().val(stack).unwrap(), 22),
        _ => panic!("expected a live Watch"),
    }

    // Drop the strong owner: the weak variant can no longer upgrade.
    drop(owner);
    assert!(matches!(
        cell.handle().read(&alloc).unwrap(),
        CellView::Watch(None)
    ));
    cell.bstack_drop(&alloc).unwrap(); // releases the enum's weak ref (frees control)

    // The Nil unit variant still works alongside the shared ones.
    let cell = Cell::new(&alloc, CellData::Nil).unwrap();
    assert!(matches!(cell.handle().read(&alloc).unwrap(), CellView::Nil));
    cell.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// bstack_move! and bstack_cast! on enums
// --------------------------------------------------------------------------

#[test]
fn macro_enum_move() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Owned variant: the child is moved out; the enum shell is freed.
    let leaf = MacroLeaf::new(&alloc, 5).unwrap();
    let node = Node::new(&alloc, NodeData::Child(leaf)).unwrap();
    match bstack_move!(node, &alloc).unwrap() {
        NodeData::Child(owned_leaf) => {
            assert_eq!(owned_leaf.handle().val(stack).unwrap(), 5); // survived the move
            owned_leaf.bstack_drop(&alloc).unwrap();
        }
        _ => panic!("expected Child"),
    }

    // POD / unit variants move by value.
    let node = Node::new(&alloc, NodeData::Num(9)).unwrap();
    assert!(matches!(
        bstack_move!(node, &alloc).unwrap(),
        NodeData::Num(9)
    ));
    let node = Node::new(&alloc, NodeData::Empty).unwrap();
    assert!(matches!(
        bstack_move!(node, &alloc).unwrap(),
        NodeData::Empty
    ));

    // Ref variant: the raw ref is handed out; the target is not owned.
    let keep = MacroLeaf::new(&alloc, 3).unwrap();
    let link = unsafe { BStackRef::from_range(keep.handle().range()) };
    let node = Node::new(&alloc, NodeData::Link(link)).unwrap();
    match bstack_move!(node, &alloc).unwrap() {
        NodeData::Link(r) => {
            assert_eq!(r.into_range().start(), keep.handle().range().start());
        }
        _ => panic!("expected Link"),
    }
    assert_eq!(keep.handle().val(stack).unwrap(), 3); // untouched
    keep.bstack_drop(&alloc).unwrap();
}

#[test]
fn macro_enum_move_shared() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Strong variant: the BStackRc is moved out (transferring the strong ref).
    let child = MacroStrongChild::new(&alloc, 11).unwrap();
    let keep = child.try_clone().unwrap();
    let cell = Cell::new(&alloc, CellData::Shared(child)).unwrap();
    match bstack_move!(cell, &alloc).unwrap() {
        CellData::Shared(rc) => {
            assert_eq!(rc.handle().val(stack).unwrap(), 11);
            drop(rc); // releases the moved-out strong ref
        }
        _ => panic!("expected Shared"),
    }
    assert_eq!(keep.handle().val(stack).unwrap(), 11); // still alive
    drop(keep);

    // Weak variant: the BStackWeak is moved out (transferring the weak ref).
    let owner = MacroStrongChild::new(&alloc, 22).unwrap();
    let cell = Cell::new(&alloc, CellData::Watch(owner.downgrade().unwrap())).unwrap();
    match bstack_move!(cell, &alloc).unwrap() {
        CellData::Watch(w) => {
            assert_eq!(
                w.upgrade()
                    .unwrap()
                    .expect("alive")
                    .handle()
                    .val(stack)
                    .unwrap(),
                22
            );
            drop(w);
        }
        _ => panic!("expected Watch"),
    }
    drop(owner);
}

#[test]
fn macro_enum_cast() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let node = Node::new(&alloc, NodeData::Num(7)).unwrap();

    // Borrowed downcast: slice -> enum handle (tag-checked), like a struct.
    let slice = node.handle().as_slice(stack);
    let n = bstack_cast!(slice as Node).unwrap().expect("tag matches");
    assert!(matches!(n.read(&alloc).unwrap(), NodeView::Num(7)));
    assert!(slice.cast_as::<MacroLeaf>().unwrap().is_none()); // wrong tag

    // Owned upcast then downcast round-trips through BStackOwnedSlice.
    let owned_slice = bstack_cast!(node.auto(&alloc) as BStackOwnedSlice);
    let back = bstack_cast!(owned_slice as BStackOwned<Node, _>)
        .unwrap()
        .ok()
        .unwrap();
    assert!(matches!(
        back.handle().read(&alloc).unwrap(),
        NodeView::Num(7)
    ));
    back.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// #[bstack_enum] discriminant width — repr(..) + inference from values
// --------------------------------------------------------------------------

// repr(u64) (== `repr(aligned)`): an 8-byte discriminant leaves the payload
// 8-aligned. header(16) + disc(8) + payload(8) = 32.
#[bstack_enum(repr(u64))]
enum Aligned {
    X(u32),
    #[bstack_owned]
    Y(MacroLeaf),
}

#[test]
fn macro_enum_repr_aligned() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    assert_eq!(size_of::<<Aligned as BStackBlock>::OnDisk>(), 32);

    let e = Aligned::new(&alloc, AlignedData::X(77)).unwrap();
    match e.handle().read(&alloc).unwrap() {
        AlignedView::X(n) => assert_eq!(n, 77),
        _ => panic!("expected X"),
    }
    e.bstack_drop(&alloc).unwrap();

    let leaf = MacroLeaf::new(&alloc, 3).unwrap();
    let e = Aligned::new(&alloc, AlignedData::Y(leaf)).unwrap();
    match e.handle().read(&alloc).unwrap() {
        AlignedView::Y(c) => assert_eq!(c.val(stack).unwrap(), 3),
        _ => panic!("expected Y"),
    }
    e.bstack_drop(&alloc).unwrap();
}

// Explicit values wider than a byte force a `u16` discriminant (a `u8` literal
// `404` would be a compile error, so compiling here proves inference widened).
#[bstack_enum]
enum Status {
    Ok = 200,
    NotFound = 404,
    Error = 500,
}

// A negative value forces a *signed* discriminant.
#[bstack_enum]
enum Temp {
    Freezing = -40,
    Zero = 0,
    Boiling = 100,
}

#[test]
fn macro_enum_discriminant_inference() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    // header(16) + disc(u16 = 2) + payload(0) = 18. (The value 404 would not fit a
    // `u8` discriminant, so compiling at all proves inference widened to u16.)
    assert_eq!(size_of::<<Status as BStackBlock>::OnDisk>(), 18);

    let e = Status::new(&alloc, StatusData::Ok).unwrap();
    assert!(matches!(e.handle().read(&alloc).unwrap(), StatusView::Ok));
    e.bstack_drop(&alloc).unwrap();
    let e = Status::new(&alloc, StatusData::NotFound).unwrap();
    assert!(matches!(
        e.handle().read(&alloc).unwrap(),
        StatusView::NotFound
    ));
    e.bstack_drop(&alloc).unwrap();
    let e = Status::new(&alloc, StatusData::Error).unwrap();
    assert!(matches!(
        e.handle().read(&alloc).unwrap(),
        StatusView::Error
    ));
    e.bstack_drop(&alloc).unwrap();

    // Signed: header(16) + disc(i8 = 1) + payload(0) = 17.
    assert_eq!(size_of::<<Temp as BStackBlock>::OnDisk>(), 17);
    let e = Temp::new(&alloc, TempData::Freezing).unwrap();
    assert!(matches!(
        e.handle().read(&alloc).unwrap(),
        TempView::Freezing
    ));
    e.bstack_drop(&alloc).unwrap();
    let e = Temp::new(&alloc, TempData::Boiling).unwrap();
    assert!(matches!(
        e.handle().read(&alloc).unwrap(),
        TempView::Boiling
    ));
    e.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// #[bstack_enum] custom tags — tag / ctrl_tag / allow(overlong_tag)
// --------------------------------------------------------------------------

#[bstack_enum(tag = "EN", ctrl_tag = "ec", rc, weak)]
enum TaggedEnum {
    A,
    B(u32),
}

#[bstack_enum(tag = "WAYTOOLONGENUMTAG", allow(overlong_tag))]
enum LongTagEnum {
    A,
}

#[test]
fn macro_enum_tags() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Custom data-tag prefix; overlong override truncated to 8 (allow silences).
    assert_eq!(&TaggedEnum::eightcc().0[0..2], b"EN");
    assert_eq!(&LongTagEnum::eightcc().0, b"WAYTOOLO");

    // ctrl_tag applies to the (rc, weak) control block.
    let rc = TaggedEnum::new(&alloc, TaggedEnumData::A).unwrap();
    let data_off = rc.handle().range().start();
    let mut buf = [0u8; 8];
    stack
        .get_into(data_off + layout::CTRL_BACKPTR_OFFSET, &mut buf)
        .unwrap();
    let ctrl_off = u64::from_le_bytes(buf);
    let mut ctag = [0u8; 8];
    stack.get_into(ctrl_off + 8, &mut ctag).unwrap();
    assert_eq!(&ctag[0..2], b"ec");
    drop(rc);
}

// --------------------------------------------------------------------------
// #[bstack_enum] POD aggregate variants — multi-field tuple + struct variants
// --------------------------------------------------------------------------

#[bstack_enum]
enum Shape {
    Empty,
    Point(i32, i32),         // multi-field tuple (POD)
    Rect { w: u32, h: u32 }, // struct variant (POD)
    Tagged(u8, u16, u8),     // heterogeneous, packed unaligned
}

#[test]
fn macro_enum_pod_aggregate_variants() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    // header(16) + disc(u8 = 1) + payload(max 8: Point/Rect) = 25.
    assert_eq!(size_of::<<Shape as BStackBlock>::OnDisk>(), 25);

    // Multi-field tuple round-trips.
    let e = Shape::new(&alloc, ShapeData::Point(3, -4)).unwrap();
    match e.handle().read(&alloc).unwrap() {
        ShapeView::Point(x, y) => assert_eq!((x, y), (3, -4)),
        _ => panic!("expected Point"),
    }
    e.bstack_drop(&alloc).unwrap();

    // Struct variant round-trips.
    let e = Shape::new(&alloc, ShapeData::Rect { w: 100, h: 200 }).unwrap();
    match e.handle().read(&alloc).unwrap() {
        ShapeView::Rect { w, h } => assert_eq!((w, h), (100, 200)),
        _ => panic!("expected Rect"),
    }
    e.bstack_drop(&alloc).unwrap();

    // Heterogeneous, packed (u8, u16, u8) — read unaligned.
    let e = Shape::new(&alloc, ShapeData::Tagged(1, 258, 255)).unwrap();
    match e.handle().read(&alloc).unwrap() {
        ShapeView::Tagged(a, b, c) => assert_eq!((a, b, c), (1, 258, 255)),
        _ => panic!("expected Tagged"),
    }
    e.bstack_drop(&alloc).unwrap();

    // Unit still round-trips through the same aggregate path.
    let e = Shape::new(&alloc, ShapeData::Empty).unwrap();
    assert!(matches!(e.handle().read(&alloc).unwrap(), ShapeView::Empty));
    e.bstack_drop(&alloc).unwrap();

    // bstack_move! yields the same aggregate variant.
    let e = Shape::new(&alloc, ShapeData::Point(7, 8)).unwrap();
    match bstack_move!(e, &alloc).unwrap() {
        ShapeData::Point(x, y) => assert_eq!((x, y), (7, 8)),
        _ => panic!("expected Point"),
    }
}
