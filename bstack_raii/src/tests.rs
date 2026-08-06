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
    AutoDrop, BStackBTreeMap, BStackBlock, BStackBlockVec, BStackBox, BStackCast, BStackCastAs,
    BStackCastInto, BStackCow, BStackDeque, BStackDrop, BStackHashMap, BStackLinkedList,
    BStackOwned, BStackRc, BStackRef, BStackShared, BStackString, BStackWeakable, EightCC,
    TryClone, TryCloneIn, alloc_block, alloc_control, bstack_block, bstack_cast, bstack_enum,
    bstack_move, dealloc_range,
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

// --------------------------------------------------------------------------
// TryCloneIn — deep clone: owned children copied, shared children re-referenced
// --------------------------------------------------------------------------

#[test]
fn macro_clone_deep_owned() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let leaf = MacroLeaf::new(&alloc, 42).unwrap();
    let parent = MacroParent::new(&alloc, leaf, 7).unwrap();
    let orig_child = parent.handle().child(stack).unwrap();

    // Deep clone -> a fresh, independent BStackOwned copy.
    let clone = parent.try_clone_in(&alloc).unwrap();

    // Same values read back through the clone.
    assert_eq!(clone.handle().tag(stack).unwrap(), 7);
    assert_eq!(clone.handle().child(stack).unwrap().val(stack).unwrap(), 42);

    // Independent storage: both the clone's block and its owned child are new
    // allocations, distinct from the originals (proves the recursion + repoint).
    assert_ne!(
        clone.handle().range().start(),
        parent.handle().range().start()
    );
    assert_ne!(
        clone.handle().child(stack).unwrap().range().start(),
        orig_child.range().start()
    );

    // Freeing the clone frees only the clone's subtree; the original stays intact.
    clone.bstack_drop(&alloc).unwrap();
    assert_eq!(
        parent.handle().child(stack).unwrap().val(stack).unwrap(),
        42
    );
    parent.bstack_drop(&alloc).unwrap();
}

#[test]
fn macro_clone_bumps_shared_refcount() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // A shared child, kept alive by an extra handle, wired into a parent's
    // `#[bstack_strong]` field. After `new` + `try_clone`, strong = 2.
    let rc = MacroStrongChild::new(&alloc, 5).unwrap();
    let rc_keep = rc.try_clone().unwrap();
    let parent = MacroStrongParent::new(&alloc, rc).unwrap();

    // Resolve the child's strong-count offset: parent.s (first user field) ->
    // data block -> ctrl back-pointer -> strong counter.
    let s_data =
        crate::refcount::load(stack, parent.handle().range().start() + layout::HEADER_SIZE)
            .unwrap();
    let ctrl = crate::refcount::load(stack, s_data + layout::CTRL_BACKPTR_OFFSET).unwrap();
    let strong_off = ctrl + layout::CTRL_STRONG_OFFSET;
    assert_eq!(crate::refcount::load(stack, strong_off).unwrap(), 2);

    // Deep-cloning the parent must make the clone's `s` acquire its OWN strong
    // reference (a shared child is re-referenced, not deep-copied): 2 -> 3.
    let clone = parent.try_clone_in(&alloc).unwrap();
    assert_eq!(crate::refcount::load(stack, strong_off).unwrap(), 3);

    // Both parents release their strong ref: 3 -> 1. `rc_keep` still holds one.
    clone.bstack_drop(&alloc).unwrap();
    parent.bstack_drop(&alloc).unwrap();
    assert_eq!(crate::refcount::load(stack, strong_off).unwrap(), 1);
    assert_eq!(rc_keep.handle().val(stack).unwrap(), 5);
    drop(rc_keep);
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
fn macro_vec_field_push_growth_reclaims_old() {
    // A field-resident growth push uses allocate → commit → free: the descriptor
    // moves to a fresh block and the OLD block is reclaimed (not leaked).
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    let rec = Record::new(&alloc, "hi", &[1u32, 2, 3], 0).unwrap();
    let old = rec.handle().tags(&alloc).unwrap().descriptor(); // cap == len == 12 B

    // len 12 + elem 4 > cap 12 → field-resident growth → the reorder path.
    let mut tags = rec.handle().tags(&alloc).unwrap();
    tags.push(4).unwrap();

    let new = rec.handle().tags(&alloc).unwrap().descriptor();
    assert_ne!(new.data_off, old.data_off); // moved to a fresh block
    assert_eq!(
        rec.handle().tags(&alloc).unwrap().to_vec().unwrap(),
        vec![1u32, 2, 3, 4]
    );

    // The old block's slot is reclaimed: a probe of its size reuses its offset.
    let probe = alloc_block(&alloc, MacroLeaf::eightcc(), old.data_size).unwrap();
    assert_eq!(probe.start(), old.data_off);
    unsafe { dealloc_range(&alloc, probe).unwrap() };

    rec.bstack_drop(&alloc).unwrap();
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
// TryCloneIn on vector fields — POD data copied, owned children deep-cloned,
// shared elements re-referenced
// --------------------------------------------------------------------------

#[test]
fn macro_clone_pod_vec() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let rec = Record::new(&alloc, "hello", &[1u32, 2, 3], 42).unwrap();
    let orig_name_off = rec.handle().name(&alloc).unwrap().descriptor().data_off;

    let clone = rec.try_clone_in(&alloc).unwrap();
    assert_eq!(clone.handle().id(stack).unwrap(), 42);
    assert_eq!(
        clone.handle().name(&alloc).unwrap().to_vec().unwrap(),
        b"hello"
    );
    assert_eq!(
        clone.handle().tags(&alloc).unwrap().to_vec().unwrap(),
        vec![1u32, 2, 3]
    );

    // The clone's data blocks are fresh allocations, distinct from the original's.
    let clone_name_off = clone.handle().name(&alloc).unwrap().descriptor().data_off;
    assert_ne!(clone_name_off, orig_name_off);

    // Growing the clone's vector leaves the original untouched (independent data).
    let mut ct = clone.handle().tags(&alloc).unwrap();
    ct.push(99).unwrap();
    assert_eq!(
        clone.handle().tags(&alloc).unwrap().to_vec().unwrap(),
        vec![1u32, 2, 3, 99]
    );
    assert_eq!(
        rec.handle().tags(&alloc).unwrap().to_vec().unwrap(),
        vec![1u32, 2, 3]
    );

    clone.bstack_drop(&alloc).unwrap();
    rec.bstack_drop(&alloc).unwrap();
}

#[test]
fn macro_clone_owned_vec() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let kids = vec![
        MacroLeaf::new(&alloc, 10).unwrap(),
        MacroLeaf::new(&alloc, 20).unwrap(),
    ];
    let tree = Tree::new(&alloc, kids, 7).unwrap();
    let orig_first = tree
        .handle()
        .kids(&alloc)
        .unwrap()
        .get(0)
        .unwrap()
        .unwrap()
        .range()
        .start();

    let clone = tree.try_clone_in(&alloc).unwrap();
    let cv = clone.handle().kids(&alloc).unwrap();
    assert_eq!(cv.len().unwrap(), 2);
    let vals: Vec<u32> = cv
        .to_vec()
        .unwrap()
        .iter()
        .map(|k| k.val(stack).unwrap())
        .collect();
    assert_eq!(vals, vec![10, 20]);

    // Each child is a fresh, independent block (deep clone, not aliased).
    let clone_first = cv.get(0).unwrap().unwrap().range().start();
    assert_ne!(clone_first, orig_first);

    // Freeing the clone frees only the clone's children; the original survives.
    clone.bstack_drop(&alloc).unwrap();
    assert_eq!(
        tree.handle()
            .kids(&alloc)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .val(stack)
            .unwrap(),
        10
    );
    tree.bstack_drop(&alloc).unwrap();
}

#[test]
fn macro_clone_strong_vec() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let a = MacroStrongChild::new(&alloc, 100).unwrap();
    let a_keep = a.try_clone().unwrap();
    let a_data = a_keep.handle().range().start();
    let b = MacroStrongChild::new(&alloc, 200).unwrap();
    let b_keep = b.try_clone().unwrap();
    let b_data = b_keep.handle().range().start();

    let list = StrongList::new(&alloc, vec![a, b], 3).unwrap();
    assert_eq!(strong_of(stack, a_data), 2); // list + a_keep
    assert_eq!(strong_of(stack, b_data), 2);

    // Cloning the list re-references each shared element: strong 2 -> 3.
    let clone = list.try_clone_in(&alloc).unwrap();
    assert_eq!(strong_of(stack, a_data), 3);
    assert_eq!(strong_of(stack, b_data), 3);

    // Freeing both lists releases their references: 3 -> 1. The `keep`s survive.
    clone.bstack_drop(&alloc).unwrap();
    list.bstack_drop(&alloc).unwrap();
    assert_eq!(strong_of(stack, a_data), 1);
    assert_eq!(strong_of(stack, b_data), 1);
    assert_eq!(a_keep.handle().val(stack).unwrap(), 100);
    drop(a_keep);
    drop(b_keep);
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
// TryCloneIn on enums — POD copied, owned variant deep-cloned, ref aliased,
// shared variant re-referenced
// --------------------------------------------------------------------------

#[test]
fn macro_clone_enum() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // POD variant: value byte-copied into an independent block.
    let e = Node::new(&alloc, NodeData::Num(42)).unwrap();
    let c = e.try_clone_in(&alloc).unwrap();
    assert_ne!(c.handle().range().start(), e.handle().range().start());
    match c.handle().read(&alloc).unwrap() {
        NodeView::Num(n) => assert_eq!(n, 42),
        _ => panic!("expected Num"),
    }
    c.bstack_drop(&alloc).unwrap();
    e.bstack_drop(&alloc).unwrap();

    // Owned variant: the child is deep-cloned into a fresh block.
    let leaf = MacroLeaf::new(&alloc, 7).unwrap();
    let e = Node::new(&alloc, NodeData::Child(leaf)).unwrap();
    let orig_child_off = match e.handle().read(&alloc).unwrap() {
        NodeView::Child(ch) => ch.range().start(),
        _ => panic!("expected Child"),
    };
    let c = e.try_clone_in(&alloc).unwrap();
    let clone_child_off = match c.handle().read(&alloc).unwrap() {
        NodeView::Child(ch) => {
            assert_eq!(ch.val(stack).unwrap(), 7);
            ch.range().start()
        }
        _ => panic!("expected Child"),
    };
    assert_ne!(clone_child_off, orig_child_off); // deep clone, not aliased
    c.bstack_drop(&alloc).unwrap();
    match e.handle().read(&alloc).unwrap() {
        NodeView::Child(ch) => assert_eq!(ch.val(stack).unwrap(), 7), // original intact
        _ => panic!("expected Child"),
    }
    e.bstack_drop(&alloc).unwrap();

    // Ref variant: the clone aliases the same target (non-owning).
    let keep = MacroLeaf::new(&alloc, 9).unwrap();
    let link = unsafe { BStackRef::from_range(keep.handle().range()) };
    let e = Node::new(&alloc, NodeData::Link(link)).unwrap();
    let c = e.try_clone_in(&alloc).unwrap();
    match c.handle().read(&alloc).unwrap() {
        NodeView::Link(l) => {
            assert_eq!(l.val(stack).unwrap(), 9);
            assert_eq!(l.range().start(), keep.handle().range().start()); // aliased
        }
        _ => panic!("expected Link"),
    }
    c.bstack_drop(&alloc).unwrap();
    e.bstack_drop(&alloc).unwrap();
    assert_eq!(keep.handle().val(stack).unwrap(), 9); // target untouched
    keep.bstack_drop(&alloc).unwrap();
}

#[test]
fn macro_clone_enum_shared() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let child = MacroStrongChild::new(&alloc, 11).unwrap();
    let keep = child.try_clone().unwrap();
    let data = keep.handle().range().start();
    let cell = Cell::new(&alloc, CellData::Shared(child)).unwrap();
    assert_eq!(strong_of(stack, data), 2); // cell + keep

    // Cloning the enum re-references the strong variant's target: 2 -> 3.
    let clone = cell.try_clone_in(&alloc).unwrap();
    assert_eq!(strong_of(stack, data), 3);

    clone.bstack_drop(&alloc).unwrap();
    cell.bstack_drop(&alloc).unwrap();
    assert_eq!(strong_of(stack, data), 1);
    assert_eq!(keep.handle().val(stack).unwrap(), 11);
    drop(keep);
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

// --------------------------------------------------------------------------
// POD field conveniences: Option<A> via bytemuck::PodInOption, tuple fields
// (`bstack_move!` keeps each tuple as one element), and generic POD wrappers.
// --------------------------------------------------------------------------

#[bstack_block]
struct PodFeat {
    maybe: Option<core::num::NonZeroU32>, // PodInOption niche, stored inline
    wrap: core::num::Wrapping<u32>,       // a generic wrapper that *is* POD
    pair: (u8, u8),                       // POD tuple field
    mixed: (u16, i32),
    n: u64,
}

#[test]
#[allow(clippy::type_complexity)] // the explicit move tuple type is the assertion
fn macro_pod_option_and_tuple_fields() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let f = PodFeat::new(
        &alloc,
        core::num::NonZeroU32::new(7),
        core::num::Wrapping(42),
        (1, 2),
        (300, -5),
        99,
    )
    .unwrap();
    assert_eq!(
        f.handle().maybe(stack).unwrap(),
        core::num::NonZeroU32::new(7)
    );
    assert_eq!(f.handle().wrap(stack).unwrap(), core::num::Wrapping(42u32));
    assert_eq!(f.handle().pair(stack).unwrap(), (1u8, 2u8));
    assert_eq!(f.handle().mixed(stack).unwrap(), (300u16, -5i32));
    assert_eq!(f.handle().n(stack).unwrap(), 99);

    // `bstack_move!` returns each tuple as ONE element (not flattened into
    // `(u8, u8, u16, i32, ..)`), so this exact type annotation must hold.
    let (maybe, wrap, pair, mixed, n): (
        Option<core::num::NonZeroU32>,
        core::num::Wrapping<u32>,
        (u8, u8),
        (u16, i32),
        u64,
    ) = bstack_move!(f, &alloc).unwrap();
    assert_eq!(maybe, core::num::NonZeroU32::new(7));
    assert_eq!(wrap, core::num::Wrapping(42));
    assert_eq!(pair, (1, 2));
    assert_eq!(mixed, (300, -5));
    assert_eq!(n, 99);

    // `Option<A>` None round-trips too (the niche).
    let g = PodFeat::new(&alloc, None, core::num::Wrapping(0), (0, 0), (0, 0), 0).unwrap();
    assert!(g.handle().maybe(stack).unwrap().is_none());
    g.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// Unit struct (header-only block) and tuple struct (POD positional fields)
// --------------------------------------------------------------------------

#[bstack_block]
struct Marker;

#[bstack_block]
struct Rgb(u8, u8, u8);

#[test]
fn macro_unit_and_tuple_structs() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Unit struct: a valid header-only block (just the 16-byte BlockHeader).
    assert_eq!(size_of::<<Marker as BStackBlock>::OnDisk>(), 16);
    let m = Marker::new(&alloc).unwrap();
    let () = bstack_move!(m, &alloc).unwrap(); // moving a unit yields ()

    // Tuple struct: positional constructor, `.field0` / `.field1` / … accessors.
    let c = Rgb::new(&alloc, 10, 20, 30).unwrap();
    assert_eq!(c.handle().field0(stack).unwrap(), 10);
    assert_eq!(c.handle().field1(stack).unwrap(), 20);
    assert_eq!(c.handle().field2(stack).unwrap(), 30);

    // bstack_move! yields the fields in order.
    let (r, g, b) = bstack_move!(c, &alloc).unwrap();
    assert_eq!((r, g, b), (10, 20, 30));
}

// --------------------------------------------------------------------------
// #[embed] — a child block stored inline (its whole on-disk form), in a struct
// and an enum variant. The embedded child keeps its OWN owned children.
// --------------------------------------------------------------------------

#[bstack_block]
struct EmbChild {
    #[bstack_owned]
    leaf: MacroLeaf,
    n: u32,
}

#[bstack_block]
struct EmbHolder {
    #[embed]
    child: EmbChild,
    tag: u32,
}

#[bstack_enum]
enum EmbEnum {
    Empty,
    #[embed]
    Wrap(EmbChild),
}

#[test]
fn macro_embed_struct_and_enum() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let leaf_size = size_of::<<MacroLeaf as BStackBlock>::OnDisk>() as u64;

    // Struct embed: parent -> embedded child -> the child's own owned leaf.
    let leaf = MacroLeaf::new(&alloc, 42).unwrap();
    let leaf_off = leaf.handle().range().start();
    let child = EmbChild::new(&alloc, leaf, 7).unwrap();
    let holder = EmbHolder::new(&alloc, child, 99).unwrap();
    assert_eq!(holder.handle().tag(stack).unwrap(), 99);
    let c = holder.handle().child(); // a handle into the inline region (no I/O)
    assert_eq!(c.n(stack).unwrap(), 7);
    assert_eq!(c.leaf(stack).unwrap().val(stack).unwrap(), 42);

    // Teardown frees the embedded child's owned leaf *in place*, then the holder;
    // the leaf's slot (lowest) is reclaimed — proof the embed recursed.
    holder.bstack_drop(&alloc).unwrap();
    let reused = alloc_block(&alloc, MacroLeaf::eightcc(), leaf_size).unwrap();
    assert_eq!(reused.start(), leaf_off);
    unsafe { dealloc_range(&alloc, reused).unwrap() };

    // bstack_move! re-homes the embedded child to a fresh standalone allocation.
    let leaf = MacroLeaf::new(&alloc, 5).unwrap();
    let child = EmbChild::new(&alloc, leaf, 8).unwrap();
    let holder = EmbHolder::new(&alloc, child, 1).unwrap();
    let (moved, tag) = bstack_move!(holder, &alloc).unwrap();
    assert_eq!(tag, 1);
    assert_eq!(moved.handle().leaf(stack).unwrap().val(stack).unwrap(), 5);
    moved.bstack_drop(&alloc).unwrap();

    // Enum embed: construct, read (a borrowed child handle), move out.
    let leaf = MacroLeaf::new(&alloc, 3).unwrap();
    let child = EmbChild::new(&alloc, leaf, 9).unwrap();
    let e = EmbEnum::new(&alloc, EmbEnumData::Wrap(child)).unwrap();
    match e.handle().read(&alloc).unwrap() {
        EmbEnumView::Wrap(c) => assert_eq!(c.leaf(stack).unwrap().val(stack).unwrap(), 3),
        _ => panic!("expected Wrap"),
    }
    let moved = match bstack_move!(e, &alloc).unwrap() {
        EmbEnumData::Wrap(c) => c,
        _ => panic!("expected Wrap"),
    };
    assert_eq!(moved.handle().n(stack).unwrap(), 9);
    moved.bstack_drop(&alloc).unwrap();
}

#[test]
fn macro_clone_embed() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Struct embed: holder -> inline child -> the child's own owned leaf.
    let leaf = MacroLeaf::new(&alloc, 42).unwrap();
    let child = EmbChild::new(&alloc, leaf, 7).unwrap();
    let holder = EmbHolder::new(&alloc, child, 99).unwrap();
    let orig_leaf_off = holder.handle().child().leaf(stack).unwrap().range().start();

    let clone = holder.try_clone_in(&alloc).unwrap();
    assert_eq!(clone.handle().tag(stack).unwrap(), 99);
    let cc = clone.handle().child();
    assert_eq!(cc.n(stack).unwrap(), 7);
    assert_eq!(cc.leaf(stack).unwrap().val(stack).unwrap(), 42);

    // The embedded child's OWN owned leaf was deep-cloned into a fresh block
    // (the inline region was folded, not just byte-copied with an aliased offset).
    let clone_leaf_off = cc.leaf(stack).unwrap().range().start();
    assert_ne!(clone_leaf_off, orig_leaf_off);

    // Freeing the clone frees only the clone's leaf; the original stays intact.
    clone.bstack_drop(&alloc).unwrap();
    assert_eq!(
        holder
            .handle()
            .child()
            .leaf(stack)
            .unwrap()
            .val(stack)
            .unwrap(),
        42
    );
    holder.bstack_drop(&alloc).unwrap();

    // Enum embed variant: same in-place fold through the payload.
    let leaf = MacroLeaf::new(&alloc, 3).unwrap();
    let child = EmbChild::new(&alloc, leaf, 9).unwrap();
    let e = EmbEnum::new(&alloc, EmbEnumData::Wrap(child)).unwrap();
    let orig_off = match e.handle().read(&alloc).unwrap() {
        EmbEnumView::Wrap(c) => c.leaf(stack).unwrap().range().start(),
        _ => panic!("expected Wrap"),
    };
    let ce = e.try_clone_in(&alloc).unwrap();
    let clone_off = match ce.handle().read(&alloc).unwrap() {
        EmbEnumView::Wrap(c) => {
            assert_eq!(c.leaf(stack).unwrap().val(stack).unwrap(), 3);
            c.leaf(stack).unwrap().range().start()
        }
        _ => panic!("expected Wrap"),
    };
    assert_ne!(clone_off, orig_off); // deep-cloned, not aliased
    ce.bstack_drop(&alloc).unwrap();
    match e.handle().read(&alloc).unwrap() {
        EmbEnumView::Wrap(c) => assert_eq!(c.leaf(stack).unwrap().val(stack).unwrap(), 3),
        _ => panic!("expected Wrap"),
    }
    e.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// WAL: completing / abandoning a crash-left transaction
// --------------------------------------------------------------------------

#[test]
fn wal_finish_rolls_forward_committed() {
    use crate::wal::{finish_at, persist_at};
    use crate::{WalEntry, WalLog, WalStatus};

    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    // A stable anchor slot, plus two "old" slices the transaction was freeing.
    let anchor = alloc.alloc(8).unwrap().as_range().start();
    let v1 = alloc.alloc(64).unwrap().as_range();
    let v2 = alloc.alloc(64).unwrap().as_range();

    // A COMMITTED transaction that had not finished its deallocs.
    let mut log = WalLog::with_capacity(2);
    log.append(WalEntry::dealloc(WalStatus::Pending, v1));
    log.append(WalEntry::dealloc(WalStatus::Pending, v2));
    persist_at(&alloc, anchor, &log, WalStatus::Complete).unwrap();

    // Completing it rolls both deallocs forward.
    assert_eq!(finish_at(&alloc, anchor).unwrap(), 2);

    // Anchor cleared, and v1/v2 reclaimed (a fresh 64-byte alloc reuses a slot).
    let mut buf = [0u8; 8];
    alloc.stack().get_into(anchor, &mut buf).unwrap();
    assert_eq!(u64::from_le_bytes(buf), 0);
    let reused = alloc.alloc(64).unwrap().as_range();
    assert!(reused.start() == v1.start() || reused.start() == v2.start());
}

#[test]
fn wal_finish_abandons_uncommitted() {
    use crate::wal::{finish_at, persist_at};
    use crate::{WalEntry, WalLog, WalStatus};

    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let anchor = alloc.alloc(8).unwrap().as_range().start();
    let v1 = alloc.alloc(64).unwrap().as_range();

    // An UNCOMMITTED transaction: its dealloc must NOT be performed.
    let mut log = WalLog::with_capacity(1);
    log.append(WalEntry::dealloc(WalStatus::Pending, v1));
    persist_at(&alloc, anchor, &log, WalStatus::Pending).unwrap();

    // Abandoned: nothing freed, anchor cleared.
    assert_eq!(finish_at(&alloc, anchor).unwrap(), 0);
    let mut buf = [0u8; 8];
    alloc.stack().get_into(anchor, &mut buf).unwrap();
    assert_eq!(u64::from_le_bytes(buf), 0);
}

// --------------------------------------------------------------------------
// Inline fixed-size arrays [T; N]
// --------------------------------------------------------------------------

#[bstack_block]
struct PodArr {
    xs: [u16; 4],
    tag: u32,
}

#[test]
fn macro_pod_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let p = PodArr::new(&alloc, [1u16, 2, 3, 4], 9).unwrap();
    assert_eq!(p.handle().xs(stack).unwrap(), [1u16, 2, 3, 4]);
    assert_eq!(p.handle().tag(stack).unwrap(), 9);
    p.bstack_drop(&alloc).unwrap();
}

#[bstack_block]
struct ArrHolder {
    #[bstack_owned]
    leaves: [MacroLeaf; 3],
    tag: u32,
}

#[test]
fn macro_owned_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let leaf_size = size_of::<<MacroLeaf as BStackBlock>::OnDisk>() as u64;

    let l0 = MacroLeaf::new(&alloc, 10).unwrap();
    let l1 = MacroLeaf::new(&alloc, 20).unwrap();
    let l2 = MacroLeaf::new(&alloc, 30).unwrap();
    let off0 = l0.handle().range().start();

    let h = ArrHolder::new(&alloc, [l0, l1, l2], 7).unwrap();
    assert_eq!(h.handle().tag(stack).unwrap(), 7);
    let arr = h.handle().leaves(stack).unwrap(); // [MacroLeaf; 3]
    assert_eq!(arr[0].val(stack).unwrap(), 10);
    assert_eq!(arr[1].val(stack).unwrap(), 20);
    assert_eq!(arr[2].val(stack).unwrap(), 30);

    // Teardown frees all three inline children; the lowest slot (l0) is reclaimed.
    h.bstack_drop(&alloc).unwrap();
    let reused = alloc_block(&alloc, MacroLeaf::eightcc(), leaf_size).unwrap();
    assert_eq!(reused.start(), off0);
    unsafe { dealloc_range(&alloc, reused).unwrap() };
}

#[test]
fn macro_owned_array_clone() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let h = ArrHolder::new(
        &alloc,
        [
            MacroLeaf::new(&alloc, 1).unwrap(),
            MacroLeaf::new(&alloc, 2).unwrap(),
            MacroLeaf::new(&alloc, 3).unwrap(),
        ],
        0,
    )
    .unwrap();

    let clone = h.try_clone_in(&alloc).unwrap();
    let carr = clone.handle().leaves(stack).unwrap();
    let oarr = h.handle().leaves(stack).unwrap();
    assert_eq!(carr[1].val(stack).unwrap(), 2);
    // Deep-cloned: each clone element is a fresh block, distinct from the original.
    assert_ne!(carr[0].range().start(), oarr[0].range().start());

    clone.bstack_drop(&alloc).unwrap();
    assert_eq!(h.handle().leaves(stack).unwrap()[2].val(stack).unwrap(), 3);
    h.bstack_drop(&alloc).unwrap();
}

#[bstack_block]
struct RefArrHolder {
    #[bstack_ref]
    refs: [MacroLeaf; 2],
}

#[test]
fn macro_ref_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let l0 = MacroLeaf::new(&alloc, 1).unwrap();
    let l1 = MacroLeaf::new(&alloc, 2).unwrap();
    let r0 = unsafe { BStackRef::from_range(l0.handle().range()) };
    let r1 = unsafe { BStackRef::from_range(l1.handle().range()) };

    let h = RefArrHolder::new(&alloc, [r0, r1]).unwrap();
    let arr = h.handle().refs(stack).unwrap();
    assert_eq!(arr[0].val(stack).unwrap(), 1);
    assert_eq!(arr[1].val(stack).unwrap(), 2);

    // A ref array owns nothing: dropping the holder leaves the targets alive.
    h.bstack_drop(&alloc).unwrap();
    assert_eq!(l0.handle().val(stack).unwrap(), 1);
    l0.bstack_drop(&alloc).unwrap();
    l1.bstack_drop(&alloc).unwrap();
}

#[bstack_block]
struct StrongArrHolder {
    #[bstack_strong]
    shared: [MacroStrongChild; 2],
}

#[test]
fn macro_strong_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let c0 = MacroStrongChild::new(&alloc, 5).unwrap();
    let c1 = MacroStrongChild::new(&alloc, 6).unwrap();
    // A keep-alive so element 0's control block survives the holders' teardown.
    let keep0 = c0.try_clone().unwrap();

    let h = StrongArrHolder::new(&alloc, [c0, c1]).unwrap();
    let arr = h.handle().shared(stack).unwrap();
    assert_eq!(arr[0].val(stack).unwrap(), 5);

    // Cloning the holder re-references each shared child: strong count +1.
    let d0 = arr[0].range().start();
    let ctrl0 = crate::refcount::load(stack, d0 + layout::CTRL_BACKPTR_OFFSET).unwrap();
    let strong0 = ctrl0 + layout::CTRL_STRONG_OFFSET;
    let before = crate::refcount::load(stack, strong0).unwrap(); // keep0 + h = 2
    let clone = h.try_clone_in(&alloc).unwrap();
    assert_eq!(crate::refcount::load(stack, strong0).unwrap(), before + 1);

    // Tear both holders down: element 0's count returns to keep0's alone.
    clone.bstack_drop(&alloc).unwrap();
    h.bstack_drop(&alloc).unwrap();
    assert_eq!(crate::refcount::load(stack, strong0).unwrap(), before - 1);
    assert_eq!(keep0.handle().val(stack).unwrap(), 5);
    drop(keep0);
}

#[test]
fn macro_owned_array_move() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let h = ArrHolder::new(
        &alloc,
        [
            MacroLeaf::new(&alloc, 10).unwrap(),
            MacroLeaf::new(&alloc, 20).unwrap(),
            MacroLeaf::new(&alloc, 30).unwrap(),
        ],
        7,
    )
    .unwrap();
    let (leaves, tag) = bstack_move!(h, &alloc).unwrap();
    assert_eq!(tag, 7);
    assert_eq!(leaves[0].handle().val(stack).unwrap(), 10);
    assert_eq!(leaves[2].handle().val(stack).unwrap(), 30);
    for l in leaves {
        l.bstack_drop(&alloc).unwrap();
    }
}

#[bstack_block]
struct WeakArrHolder {
    #[bstack_weak]
    weaks: [MacroStrongChild; 2],
}

#[test]
fn macro_weak_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let c0 = MacroStrongChild::new(&alloc, 5).unwrap();
    let c1 = MacroStrongChild::new(&alloc, 6).unwrap();

    // Weak arrays start null (not a ctor parameter).
    let h = WeakArrHolder::new(&alloc).unwrap();
    let arr = h.handle().weaks(&alloc).unwrap();
    assert!(arr[0].is_none() && arr[1].is_none());

    // Wire each element via the per-index setter.
    h.handle()
        .set_weaks(&alloc, 0, c0.downgrade().unwrap())
        .unwrap();
    h.handle()
        .set_weaks(&alloc, 1, c1.downgrade().unwrap())
        .unwrap();

    // The accessor upgrades each live element.
    let arr = h.handle().weaks(&alloc).unwrap();
    assert_eq!(arr[0].as_ref().unwrap().handle().val(stack).unwrap(), 5);
    assert_eq!(arr[1].as_ref().unwrap().handle().val(stack).unwrap(), 6);
    drop(arr);

    // Cloning aliases the same control blocks (weak counts bumped).
    let clone = h.try_clone_in(&alloc).unwrap();
    let carr = clone.handle().weaks(&alloc).unwrap();
    assert_eq!(carr[0].as_ref().unwrap().handle().val(stack).unwrap(), 5);
    drop(carr);

    // Both holders' teardown releases the weak refs (no underflow); c0/c1 live.
    clone.bstack_drop(&alloc).unwrap();
    h.bstack_drop(&alloc).unwrap();
    assert_eq!(c0.handle().val(stack).unwrap(), 5);
    assert_eq!(c1.handle().val(stack).unwrap(), 6);
    drop(c0);
    drop(c1);
}

#[bstack_block]
struct OptArrHolder {
    #[bstack_owned]
    leaves: [Option<MacroLeaf>; 3],
}

#[test]
fn macro_owned_option_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let leaf_size = size_of::<<MacroLeaf as BStackBlock>::OnDisk>() as u64;

    // Middle element is None.
    let l0 = MacroLeaf::new(&alloc, 10).unwrap();
    let l2 = MacroLeaf::new(&alloc, 30).unwrap();
    let off0 = l0.handle().range().start();
    let h = OptArrHolder::new(&alloc, [Some(l0), None, Some(l2)]).unwrap();

    let arr = h.handle().leaves(stack).unwrap(); // [Option<MacroLeaf>; 3]
    assert_eq!(arr[0].as_ref().unwrap().val(stack).unwrap(), 10);
    assert!(arr[1].is_none());
    assert_eq!(arr[2].as_ref().unwrap().val(stack).unwrap(), 30);

    // Clone deep-copies the present elements, keeps the hole.
    let clone = h.try_clone_in(&alloc).unwrap();
    let carr = clone.handle().leaves(stack).unwrap();
    assert_eq!(carr[0].as_ref().unwrap().val(stack).unwrap(), 10);
    assert!(carr[1].is_none());
    assert_ne!(
        carr[0].as_ref().unwrap().range().start(),
        arr[0].as_ref().unwrap().range().start()
    );

    // Move yields `[Option<BStackOwned<MacroLeaf>>; 3]`.
    clone.bstack_drop(&alloc).unwrap();
    let (moved,) = bstack_move!(h, &alloc).unwrap();
    assert_eq!(moved[2].as_ref().unwrap().handle().val(stack).unwrap(), 30);
    assert!(moved[1].is_none());
    for o in moved.into_iter().flatten() {
        o.bstack_drop(&alloc).unwrap();
    }
    // The present children were freed by the move re-home + drop; a slot comes back.
    let reused = alloc_block(&alloc, MacroLeaf::eightcc(), leaf_size).unwrap();
    let _ = off0;
    unsafe { dealloc_range(&alloc, reused).unwrap() };
}

#[bstack_block]
struct OptRefArrHolder {
    #[bstack_ref]
    refs: [Option<MacroLeaf>; 2],
}

#[test]
fn macro_ref_option_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let l0 = MacroLeaf::new(&alloc, 1).unwrap();
    let r0 = unsafe { BStackRef::from_range(l0.handle().range()) };

    // Element 1 is a null reference.
    let h = OptRefArrHolder::new(&alloc, [Some(r0), None]).unwrap();
    let arr = h.handle().refs(stack).unwrap(); // [Option<MacroLeaf>; 2]
    assert_eq!(arr[0].as_ref().unwrap().val(stack).unwrap(), 1);
    assert!(arr[1].is_none());

    h.bstack_drop(&alloc).unwrap(); // owns nothing
    assert_eq!(l0.handle().val(stack).unwrap(), 1);
    l0.bstack_drop(&alloc).unwrap();
}

#[bstack_block]
struct PodOptArr {
    xs: [Option<core::num::NonZeroU32>; 3],
}

#[test]
fn macro_pod_option_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let p = PodOptArr::new(
        &alloc,
        [
            core::num::NonZeroU32::new(5),
            None,
            core::num::NonZeroU32::new(9),
        ],
    )
    .unwrap();
    let arr = p.handle().xs(stack).unwrap(); // [Option<NonZeroU32>; 3]
    assert_eq!(arr[0].map(|n| n.get()), Some(5));
    assert!(arr[1].is_none());
    assert_eq!(arr[2].map(|n| n.get()), Some(9));
    p.bstack_drop(&alloc).unwrap();
}

#[bstack_block]
struct EmbArrHolder {
    #[embed]
    kids: [EmbChild; 2],
    tag: u32,
}

#[test]
fn macro_embed_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Two embedded children, each owning its own leaf.
    let k0 = EmbChild::new(&alloc, MacroLeaf::new(&alloc, 10).unwrap(), 1).unwrap();
    let k1 = EmbChild::new(&alloc, MacroLeaf::new(&alloc, 20).unwrap(), 2).unwrap();
    let h = EmbArrHolder::new(&alloc, [k0, k1], 99).unwrap();
    assert_eq!(h.handle().tag(stack).unwrap(), 99);

    // Accessor: `[EmbChild; 2]` handles into the inline slots (pure offset math).
    let kids = h.handle().kids();
    assert_eq!(kids[0].n(stack).unwrap(), 1);
    assert_eq!(kids[0].leaf(stack).unwrap().val(stack).unwrap(), 10);
    assert_eq!(kids[1].leaf(stack).unwrap().val(stack).unwrap(), 20);

    // Clone folds each embedded child inline, deep-cloning its owned leaf.
    let clone = h.try_clone_in(&alloc).unwrap();
    let ckids = clone.handle().kids();
    assert_eq!(ckids[1].leaf(stack).unwrap().val(stack).unwrap(), 20);
    assert_ne!(
        ckids[0].leaf(stack).unwrap().range().start(),
        kids[0].leaf(stack).unwrap().range().start()
    );
    clone.bstack_drop(&alloc).unwrap();
    assert_eq!(
        h.handle().kids()[1]
            .leaf(stack)
            .unwrap()
            .val(stack)
            .unwrap(),
        20
    );

    // Move re-homes each embedded child to a fresh standalone allocation.
    let (moved, tag) = bstack_move!(h, &alloc).unwrap();
    assert_eq!(tag, 99);
    assert_eq!(
        moved[0].handle().leaf(stack).unwrap().val(stack).unwrap(),
        10
    );
    for m in moved {
        m.bstack_drop(&alloc).unwrap();
    }
}

#[bstack_enum]
enum ArrEnum {
    Empty,
    #[bstack_owned]
    Leaves([MacroLeaf; 2]),
}

#[test]
fn macro_enum_owned_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let e = ArrEnum::new(
        &alloc,
        ArrEnumData::Leaves([
            MacroLeaf::new(&alloc, 10).unwrap(),
            MacroLeaf::new(&alloc, 20).unwrap(),
        ]),
    )
    .unwrap();
    match e.handle().read(&alloc).unwrap() {
        ArrEnumView::Leaves(arr) => {
            assert_eq!(arr[0].val(stack).unwrap(), 10);
            assert_eq!(arr[1].val(stack).unwrap(), 20);
        }
        _ => panic!("expected Leaves"),
    }

    // Clone deep-copies each element.
    let clone = e.try_clone_in(&alloc).unwrap();
    match clone.handle().read(&alloc).unwrap() {
        ArrEnumView::Leaves(arr) => assert_eq!(arr[0].val(stack).unwrap(), 10),
        _ => panic!("expected Leaves"),
    }
    clone.bstack_drop(&alloc).unwrap();

    // Move yields `[BStackOwned<MacroLeaf>; 2]`.
    match bstack_move!(e, &alloc).unwrap() {
        ArrEnumData::Leaves(arr) => {
            assert_eq!(arr[1].handle().val(stack).unwrap(), 20);
            for l in arr {
                l.bstack_drop(&alloc).unwrap();
            }
        }
        _ => panic!("expected Leaves"),
    }
}

#[bstack_enum]
enum RefArrEnum {
    Empty,
    #[bstack_ref]
    Refs([MacroLeaf; 2]),
}

#[test]
fn macro_enum_ref_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let l0 = MacroLeaf::new(&alloc, 1).unwrap();
    let l1 = MacroLeaf::new(&alloc, 2).unwrap();
    let e = RefArrEnum::new(
        &alloc,
        RefArrEnumData::Refs([
            unsafe { BStackRef::from_range(l0.handle().range()) },
            unsafe { BStackRef::from_range(l1.handle().range()) },
        ]),
    )
    .unwrap();
    match e.handle().read(&alloc).unwrap() {
        RefArrEnumView::Refs(arr) => {
            assert_eq!(arr[0].val(stack).unwrap(), 1);
            assert_eq!(arr[1].val(stack).unwrap(), 2);
        }
        _ => panic!("expected Refs"),
    }
    e.bstack_drop(&alloc).unwrap(); // owns nothing
    assert_eq!(l0.handle().val(stack).unwrap(), 1);
    l0.bstack_drop(&alloc).unwrap();
    l1.bstack_drop(&alloc).unwrap();
}

#[bstack_enum]
enum StrongArrEnum {
    Empty,
    #[bstack_strong]
    Shared([MacroStrongChild; 2]),
}

#[test]
fn macro_enum_strong_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let c0 = MacroStrongChild::new(&alloc, 5).unwrap();
    let c1 = MacroStrongChild::new(&alloc, 6).unwrap();
    let keep0 = c0.try_clone().unwrap();
    let e = StrongArrEnum::new(&alloc, StrongArrEnumData::Shared([c0, c1])).unwrap();
    match e.handle().read(&alloc).unwrap() {
        StrongArrEnumView::Shared(arr) => assert_eq!(arr[0].val(stack).unwrap(), 5),
        _ => panic!("expected Shared"),
    }
    // Clone re-references each; teardown of both holders returns to keep0's ref.
    let clone = e.try_clone_in(&alloc).unwrap();
    clone.bstack_drop(&alloc).unwrap();
    e.bstack_drop(&alloc).unwrap();
    assert_eq!(keep0.handle().val(stack).unwrap(), 5);
    drop(keep0);
}

#[bstack_enum]
enum WeakArrEnum {
    Empty,
    #[bstack_weak]
    Weaks([MacroStrongChild; 2]),
}

#[test]
fn macro_enum_weak_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let c0 = MacroStrongChild::new(&alloc, 5).unwrap();
    let c1 = MacroStrongChild::new(&alloc, 6).unwrap();
    let e = WeakArrEnum::new(
        &alloc,
        WeakArrEnumData::Weaks([c0.downgrade().unwrap(), c1.downgrade().unwrap()]),
    )
    .unwrap();
    match e.handle().read(&alloc).unwrap() {
        WeakArrEnumView::Weaks(arr) => {
            assert_eq!(arr[0].as_ref().unwrap().handle().val(stack).unwrap(), 5);
            assert_eq!(arr[1].as_ref().unwrap().handle().val(stack).unwrap(), 6);
        }
        _ => panic!("expected Weaks"),
    }
    e.bstack_drop(&alloc).unwrap(); // releases the weak refs
    assert_eq!(c0.handle().val(stack).unwrap(), 5);
    drop(c0);
    drop(c1);
}

#[bstack_enum]
enum PodArrEnum {
    Empty,
    Bytes([u16; 3]),
}

#[test]
fn macro_enum_pod_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let e = PodArrEnum::new(&alloc, PodArrEnumData::Bytes([7, 8, 9])).unwrap();
    match e.handle().read(&alloc).unwrap() {
        PodArrEnumView::Bytes(a) => assert_eq!(a, [7u16, 8, 9]),
        _ => panic!("expected Bytes"),
    }
    e.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// Nested arrays `[[..]; ..]` of handles (structs)
// --------------------------------------------------------------------------

#[bstack_block]
struct OwnedGrid {
    #[bstack_owned]
    grid: [[MacroLeaf; 2]; 2],
    tag: u32,
}

#[test]
fn macro_owned_nested_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let mk = |v| MacroLeaf::new(&alloc, v).unwrap();
    let h = OwnedGrid::new(&alloc, [[mk(1), mk(2)], [mk(3), mk(4)]], 7).unwrap();

    assert_eq!(h.handle().tag(stack).unwrap(), 7);
    let g = h.handle().grid(stack).unwrap(); // [[MacroLeaf; 2]; 2]
    assert_eq!(g[0][0].val(stack).unwrap(), 1);
    assert_eq!(g[0][1].val(stack).unwrap(), 2);
    assert_eq!(g[1][0].val(stack).unwrap(), 3);
    assert_eq!(g[1][1].val(stack).unwrap(), 4);

    // Deep clone: fresh blocks, same values.
    let clone = h.try_clone_in(&alloc).unwrap();
    let cg = clone.handle().grid(stack).unwrap();
    assert_eq!(cg[1][1].val(stack).unwrap(), 4);
    assert_ne!(cg[0][0].range().start(), g[0][0].range().start());
    clone.bstack_drop(&alloc).unwrap();
    assert_eq!(h.handle().grid(stack).unwrap()[1][0].val(stack).unwrap(), 3);

    // Move: nested owning handles.
    let (moved, tag) = bstack_move!(h, &alloc).unwrap();
    assert_eq!(tag, 7);
    assert_eq!(moved[1][1].handle().val(stack).unwrap(), 4);
    for row in moved {
        for m in row {
            m.bstack_drop(&alloc).unwrap();
        }
    }
}

#[bstack_block]
struct RefCube {
    #[bstack_ref]
    cube: [[[MacroLeaf; 2]; 1]; 2],
}

#[test]
fn macro_ref_nested3_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let leaves: Vec<_> = (0..4).map(|v| MacroLeaf::new(&alloc, v).unwrap()).collect();
    let r = |i: usize| unsafe { BStackRef::from_range(leaves[i].handle().range()) };
    let h = RefCube::new(&alloc, [[[r(0), r(1)]], [[r(2), r(3)]]]).unwrap();

    let c = h.handle().cube(stack).unwrap(); // [[[MacroLeaf; 2]; 1]; 2]
    assert_eq!(c[0][0][0].val(stack).unwrap(), 0);
    assert_eq!(c[0][0][1].val(stack).unwrap(), 1);
    assert_eq!(c[1][0][0].val(stack).unwrap(), 2);
    assert_eq!(c[1][0][1].val(stack).unwrap(), 3);

    // A ref cube owns nothing: dropping leaves targets alive.
    h.bstack_drop(&alloc).unwrap();
    for l in leaves {
        assert!(l.handle().val(stack).unwrap() < 4);
        l.bstack_drop(&alloc).unwrap();
    }
}

#[bstack_block]
struct EmbGrid {
    #[embed]
    kids: [[EmbChild; 2]; 1],
    tag: u32,
}

#[test]
fn macro_embed_nested_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let k = |v| EmbChild::new(&alloc, MacroLeaf::new(&alloc, v).unwrap(), v).unwrap();
    let h = EmbGrid::new(&alloc, [[k(10), k(20)]], 5).unwrap();
    assert_eq!(h.handle().tag(stack).unwrap(), 5);

    let g = h.handle().kids(); // [[EmbChild; 2]; 1]
    assert_eq!(g[0][0].leaf(stack).unwrap().val(stack).unwrap(), 10);
    assert_eq!(g[0][1].leaf(stack).unwrap().val(stack).unwrap(), 20);

    let clone = h.try_clone_in(&alloc).unwrap();
    let cg = clone.handle().kids();
    assert_eq!(cg[0][1].leaf(stack).unwrap().val(stack).unwrap(), 20);
    assert_ne!(
        cg[0][0].leaf(stack).unwrap().range().start(),
        g[0][0].leaf(stack).unwrap().range().start()
    );
    clone.bstack_drop(&alloc).unwrap();

    let (moved, tag) = bstack_move!(h, &alloc).unwrap();
    assert_eq!(tag, 5);
    assert_eq!(
        moved[0][0]
            .handle()
            .leaf(stack)
            .unwrap()
            .val(stack)
            .unwrap(),
        10
    );
    for row in moved {
        for m in row {
            m.bstack_drop(&alloc).unwrap();
        }
    }
}

// --------------------------------------------------------------------------
// Enum array variants: Option leaves, #[embed], and nesting
// --------------------------------------------------------------------------

#[bstack_enum]
enum OptArrEnum {
    Empty,
    #[bstack_owned]
    Slots([Option<MacroLeaf>; 3]),
}

#[test]
fn macro_enum_owned_option_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let e = OptArrEnum::new(
        &alloc,
        OptArrEnumData::Slots([
            Some(MacroLeaf::new(&alloc, 10).unwrap()),
            None,
            Some(MacroLeaf::new(&alloc, 30).unwrap()),
        ]),
    )
    .unwrap();
    match e.handle().read(&alloc).unwrap() {
        OptArrEnumView::Slots(arr) => {
            assert_eq!(arr[0].map(|h| h.val(stack).unwrap()), Some(10));
            assert!(arr[1].is_none());
            assert_eq!(arr[2].map(|h| h.val(stack).unwrap()), Some(30));
        }
        _ => panic!("expected Slots"),
    }

    let clone = e.try_clone_in(&alloc).unwrap();
    match clone.handle().read(&alloc).unwrap() {
        OptArrEnumView::Slots(arr) => {
            assert_eq!(arr[2].map(|h| h.val(stack).unwrap()), Some(30));
            assert!(arr[1].is_none());
        }
        _ => panic!("expected Slots"),
    }
    clone.bstack_drop(&alloc).unwrap();

    match bstack_move!(e, &alloc).unwrap() {
        OptArrEnumData::Slots(arr) => {
            assert_eq!(
                arr[0].as_ref().map(|h| h.handle().val(stack).unwrap()),
                Some(10)
            );
            assert!(arr[1].is_none());
            for slot in arr.into_iter().flatten() {
                slot.bstack_drop(&alloc).unwrap();
            }
        }
        _ => panic!("expected Slots"),
    }
}

#[bstack_enum]
enum EmbArrEnum {
    Empty,
    #[embed]
    Kids([EmbChild; 2]),
}

#[test]
fn macro_enum_embed_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let k = |v| EmbChild::new(&alloc, MacroLeaf::new(&alloc, v).unwrap(), v).unwrap();
    let e = EmbArrEnum::new(&alloc, EmbArrEnumData::Kids([k(10), k(20)])).unwrap();

    match e.handle().read(&alloc).unwrap() {
        EmbArrEnumView::Kids(arr) => {
            assert_eq!(arr[0].leaf(stack).unwrap().val(stack).unwrap(), 10);
            assert_eq!(arr[1].leaf(stack).unwrap().val(stack).unwrap(), 20);
        }
        _ => panic!("expected Kids"),
    }

    let clone = e.try_clone_in(&alloc).unwrap();
    match clone.handle().read(&alloc).unwrap() {
        EmbArrEnumView::Kids(arr) => {
            assert_eq!(arr[1].leaf(stack).unwrap().val(stack).unwrap(), 20)
        }
        _ => panic!("expected Kids"),
    }
    clone.bstack_drop(&alloc).unwrap();

    match bstack_move!(e, &alloc).unwrap() {
        EmbArrEnumData::Kids(arr) => {
            assert_eq!(arr[0].handle().leaf(stack).unwrap().val(stack).unwrap(), 10);
            for m in arr {
                m.bstack_drop(&alloc).unwrap();
            }
        }
        _ => panic!("expected Kids"),
    }
}

#[bstack_enum]
enum NestArrEnum {
    Empty,
    #[bstack_owned]
    Grid([[MacroLeaf; 2]; 2]),
}

#[test]
fn macro_enum_owned_nested_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let mk = |v| MacroLeaf::new(&alloc, v).unwrap();
    let e = NestArrEnum::new(
        &alloc,
        NestArrEnumData::Grid([[mk(1), mk(2)], [mk(3), mk(4)]]),
    )
    .unwrap();
    match e.handle().read(&alloc).unwrap() {
        NestArrEnumView::Grid(g) => {
            assert_eq!(g[0][0].val(stack).unwrap(), 1);
            assert_eq!(g[1][1].val(stack).unwrap(), 4);
        }
        _ => panic!("expected Grid"),
    }

    let clone = e.try_clone_in(&alloc).unwrap();
    clone.bstack_drop(&alloc).unwrap();

    match bstack_move!(e, &alloc).unwrap() {
        NestArrEnumData::Grid(g) => {
            assert_eq!(g[1][0].handle().val(stack).unwrap(), 3);
            for row in g {
                for m in row {
                    m.bstack_drop(&alloc).unwrap();
                }
            }
        }
        _ => panic!("expected Grid"),
    }
}

// --------------------------------------------------------------------------
// Inline arrays of vectors `[Vec<T>; N]` (N independent inline VecDescs)
// --------------------------------------------------------------------------

#[bstack_block]
struct PodVecArr {
    rows: [Vec<u32>; 2],
    tag: u32,
}

#[test]
fn macro_pod_vec_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let h = PodVecArr::new(&alloc, [&[1u32, 2][..], &[3, 4, 5][..]], 9).unwrap();
    assert_eq!(h.handle().tag(stack).unwrap(), 9);

    let rows = h.handle().rows(&alloc).unwrap(); // [BStackVec<u32,_>; 2]
    assert_eq!(rows[0].to_vec().unwrap(), vec![1u32, 2]);
    assert_eq!(rows[1].to_vec().unwrap(), vec![3u32, 4, 5]);

    // Each slot is an independent, growable vector.
    let mut rows_mut = h.handle().rows(&alloc).unwrap();
    rows_mut[0].push(99).unwrap();
    assert_eq!(
        h.handle().rows(&alloc).unwrap()[0].to_vec().unwrap(),
        vec![1u32, 2, 99]
    );
    assert_eq!(
        h.handle().rows(&alloc).unwrap()[1].to_vec().unwrap(),
        vec![3u32, 4, 5]
    );

    // Clone deep-copies both data blocks.
    let clone = h.try_clone_in(&alloc).unwrap();
    let crows = clone.handle().rows(&alloc).unwrap();
    assert_eq!(crows[1].to_vec().unwrap(), vec![3u32, 4, 5]);
    clone.bstack_drop(&alloc).unwrap();
    assert_eq!(
        h.handle().rows(&alloc).unwrap()[1].to_vec().unwrap(),
        vec![3u32, 4, 5]
    );

    // Move yields the two vec handles.
    let (moved, tag) = bstack_move!(h, &alloc).unwrap();
    assert_eq!(tag, 9);
    assert_eq!(moved[1].to_vec().unwrap(), vec![3u32, 4, 5]);
    for v in moved {
        v.bstack_drop().unwrap();
    }
}

#[bstack_block]
struct RefVecArr {
    #[bstack_ref]
    lists: [Vec<MacroLeaf>; 2],
}

#[test]
fn macro_ref_vec_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let leaves: Vec<_> = (0..4).map(|v| MacroLeaf::new(&alloc, v).unwrap()).collect();
    let r = |i: usize| unsafe { BStackRef::from_range(leaves[i].handle().range()) };
    let h = RefVecArr::new(&alloc, [vec![r(0), r(1)], vec![r(2), r(3)]]).unwrap();

    let ls = h.handle().lists(&alloc).unwrap(); // [BStackRefVec<MacroLeaf,_>; 2]
    assert_eq!(ls[0].len().unwrap(), 2);
    assert_eq!(ls[0].get(1).unwrap().unwrap().val(stack).unwrap(), 1);
    assert_eq!(ls[1].get(0).unwrap().unwrap().val(stack).unwrap(), 2);

    // Ref vecs own the offset arrays but not the targets.
    h.bstack_drop(&alloc).unwrap();
    for l in leaves {
        assert!(l.handle().val(stack).unwrap() < 4);
        l.bstack_drop(&alloc).unwrap();
    }
}

#[bstack_block]
struct OwnedVecArr {
    #[bstack_owned]
    groups: [Vec<MacroLeaf>; 2],
    tag: u32,
}

#[test]
fn macro_owned_vec_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let g0 = vec![
        MacroLeaf::new(&alloc, 10).unwrap(),
        MacroLeaf::new(&alloc, 11).unwrap(),
    ];
    let g1 = vec![MacroLeaf::new(&alloc, 20).unwrap()];
    let h = OwnedVecArr::new(&alloc, [g0, g1], 7).unwrap();
    assert_eq!(h.handle().tag(stack).unwrap(), 7);

    let gs = h.handle().groups(&alloc).unwrap();
    assert_eq!(gs[0].len().unwrap(), 2);
    assert_eq!(gs[0].get(0).unwrap().unwrap().val(stack).unwrap(), 10);
    assert_eq!(gs[1].get(0).unwrap().unwrap().val(stack).unwrap(), 20);

    // Deep clone: distinct child blocks.
    let clone = h.try_clone_in(&alloc).unwrap();
    let cgs = clone.handle().groups(&alloc).unwrap();
    assert_eq!(cgs[0].get(1).unwrap().unwrap().val(stack).unwrap(), 11);
    assert_ne!(
        cgs[0].get(0).unwrap().unwrap().range().start(),
        gs[0].get(0).unwrap().unwrap().range().start()
    );
    clone.bstack_drop(&alloc).unwrap();
    assert_eq!(
        h.handle().groups(&alloc).unwrap()[1]
            .get(0)
            .unwrap()
            .unwrap()
            .val(stack)
            .unwrap(),
        20
    );

    h.bstack_drop(&alloc).unwrap();
}

#[bstack_block]
struct OptVecArr {
    slots: [Option<Vec<u32>>; 3],
}

#[test]
fn macro_option_vec_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    let h = OptVecArr::new(&alloc, [Some(&[1u32, 2][..]), None, Some(&[9][..])]).unwrap();
    let s = h.handle().slots(&alloc).unwrap(); // [Option<BStackVec<u32,_>>; 3]
    assert_eq!(s[0].as_ref().unwrap().to_vec().unwrap(), vec![1u32, 2]);
    assert!(s[1].is_none());
    assert_eq!(s[2].as_ref().unwrap().to_vec().unwrap(), vec![9u32]);
    h.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// #[bstack_ref] Vec<[T; N]> — a vector of fixed-size arrays of references
// --------------------------------------------------------------------------

#[bstack_block]
struct RefVecOfArr {
    #[bstack_ref]
    rows: Vec<[MacroLeaf; 2]>,
    tag: u32,
}

#[test]
fn macro_ref_vec_of_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let leaves: Vec<_> = (0..4).map(|v| MacroLeaf::new(&alloc, v).unwrap()).collect();
    let r = |i: usize| unsafe { BStackRef::from_range(leaves[i].handle().range()) };
    let h = RefVecOfArr::new(&alloc, vec![[r(0), r(1)], [r(2), r(3)]], 7).unwrap();
    assert_eq!(h.handle().tag(stack).unwrap(), 7);

    let rows = h.handle().rows(&alloc).unwrap(); // Vec<[MacroLeaf; 2]>
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].val(stack).unwrap(), 0);
    assert_eq!(rows[0][1].val(stack).unwrap(), 1);
    assert_eq!(rows[1][0].val(stack).unwrap(), 2);
    assert_eq!(rows[1][1].val(stack).unwrap(), 3);

    // Clone aliases: same target offsets, but a fresh offset-array data block.
    let clone = h.try_clone_in(&alloc).unwrap();
    let crows = clone.handle().rows(&alloc).unwrap();
    assert_eq!(
        crows[1][0].range().start(),
        rows[1][0].range().start() // same target (aliased)
    );
    clone.bstack_drop(&alloc).unwrap();
    // Original + targets still alive after clone teardown.
    assert_eq!(
        h.handle().rows(&alloc).unwrap()[0][1].val(stack).unwrap(),
        1
    );

    // Dropping the holder frees only the offset array, not the targets.
    h.bstack_drop(&alloc).unwrap();
    for l in leaves {
        assert!(l.handle().val(stack).unwrap() < 4);
        l.bstack_drop(&alloc).unwrap();
    }
}

#[bstack_block]
struct OwnedVecOfArr {
    #[bstack_owned]
    rows: Vec<[MacroLeaf; 2]>,
    tag: u32,
}

#[test]
fn macro_owned_vec_of_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let mk = |v| MacroLeaf::new(&alloc, v).unwrap();
    let h = OwnedVecOfArr::new(&alloc, vec![[mk(1), mk(2)], [mk(3), mk(4)]], 7).unwrap();
    assert_eq!(h.handle().tag(stack).unwrap(), 7);

    let rows = h.handle().rows(&alloc).unwrap(); // Vec<[MacroLeaf; 2]>
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].val(stack).unwrap(), 1);
    assert_eq!(rows[0][1].val(stack).unwrap(), 2);
    assert_eq!(rows[1][0].val(stack).unwrap(), 3);
    assert_eq!(rows[1][1].val(stack).unwrap(), 4);

    // Deep clone: distinct child blocks.
    let clone = h.try_clone_in(&alloc).unwrap();
    let crows = clone.handle().rows(&alloc).unwrap();
    assert_eq!(crows[1][1].val(stack).unwrap(), 4);
    assert_ne!(crows[0][0].range().start(), rows[0][0].range().start());
    clone.bstack_drop(&alloc).unwrap();
    assert_eq!(
        h.handle().rows(&alloc).unwrap()[1][0].val(stack).unwrap(),
        3
    );

    h.bstack_drop(&alloc).unwrap();
}

#[bstack_block]
struct StrongVecOfArr {
    #[bstack_strong]
    groups: Vec<[MacroStrongChild; 2]>,
}

#[test]
fn macro_strong_vec_of_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let a = MacroStrongChild::new(&alloc, 10).unwrap();
    let b = MacroStrongChild::new(&alloc, 20).unwrap();
    let a_keep = a.try_clone().unwrap(); // a strong = 2
    let a_data = a_keep.handle().range().start();

    let h = StrongVecOfArr::new(&alloc, vec![[a, b]]).unwrap();
    assert_eq!(strong_of(stack, a_data), 2); // h + a_keep

    let g = h.handle().groups(&alloc).unwrap(); // Vec<[MacroStrongChild; 2]>
    assert_eq!(g[0][0].val(stack).unwrap(), 10);
    assert_eq!(g[0][1].val(stack).unwrap(), 20);

    // Clone bumps every element's strong count.
    let clone = h.try_clone_in(&alloc).unwrap();
    assert_eq!(strong_of(stack, a_data), 3); // h + a_keep + clone
    clone.bstack_drop(&alloc).unwrap();
    assert_eq!(strong_of(stack, a_data), 2);

    h.bstack_drop(&alloc).unwrap();
    assert_eq!(strong_of(stack, a_data), 1); // a_keep only
    drop(a_keep);
}

#[bstack_block]
struct WeakVecOfArr {
    #[bstack_weak]
    groups: Vec<[MacroStrongChild; 2]>,
}

#[test]
fn macro_weak_vec_of_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let a = MacroStrongChild::new(&alloc, 1).unwrap();
    let b = MacroStrongChild::new(&alloc, 2).unwrap();
    let h = WeakVecOfArr::new(
        &alloc,
        vec![[a.downgrade().unwrap(), b.downgrade().unwrap()]],
    )
    .unwrap();

    let g = h.handle().groups(&alloc).unwrap(); // Vec<[Option<BStackRc>; 2]>
    assert_eq!(g[0][0].as_ref().unwrap().handle().val(stack).unwrap(), 1);
    assert!(g[0][1].as_ref().is_some());
    drop(g); // release the upgraded strong refs so `a` can actually be freed

    // Drop `a`'s data: its slot no longer upgrades; `b` still does.
    drop(a);
    let g = h.handle().groups(&alloc).unwrap();
    assert!(g[0][0].is_none());
    assert!(g[0][1].is_some());
    drop(g);

    // Teardown releases each weak count.
    h.bstack_drop(&alloc).unwrap();
    drop(b);
}

// --------------------------------------------------------------------------
// Vec<T> and Vec<[T; N]> in enum variants
// --------------------------------------------------------------------------

#[bstack_enum]
enum OwnedVecEnum {
    Empty,
    #[bstack_owned]
    Items(Vec<MacroLeaf>),
}

#[test]
fn macro_enum_owned_vec() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let items = BStackBlockVec::from_handles(
        &alloc,
        vec![
            MacroLeaf::new(&alloc, 10).unwrap(),
            MacroLeaf::new(&alloc, 20).unwrap(),
        ],
    )
    .unwrap();
    let e = OwnedVecEnum::new(&alloc, OwnedVecEnumData::Items(items)).unwrap();

    match e.handle().read(&alloc).unwrap() {
        OwnedVecEnumView::Items(v) => {
            assert_eq!(v.len().unwrap(), 2);
            assert_eq!(v.get(0).unwrap().unwrap().val(stack).unwrap(), 10);
            assert_eq!(v.get(1).unwrap().unwrap().val(stack).unwrap(), 20);
        }
        _ => panic!("expected Items"),
    }

    // Clone deep-copies the vector + its children.
    let clone = e.try_clone_in(&alloc).unwrap();
    match clone.handle().read(&alloc).unwrap() {
        OwnedVecEnumView::Items(v) => {
            assert_eq!(v.get(1).unwrap().unwrap().val(stack).unwrap(), 20)
        }
        _ => panic!("expected Items"),
    }
    clone.bstack_drop(&alloc).unwrap();

    // Move hands back the vector handle.
    match bstack_move!(e, &alloc).unwrap() {
        OwnedVecEnumData::Items(v) => {
            assert_eq!(v.get(0).unwrap().unwrap().val(stack).unwrap(), 10);
            v.bstack_drop().unwrap();
        }
        _ => panic!("expected Items"),
    }
}

#[bstack_enum]
enum RefVecArrEnum {
    Empty,
    #[bstack_ref]
    Rows(Vec<[MacroLeaf; 2]>),
}

#[test]
fn macro_enum_ref_vec_of_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let leaves: Vec<_> = (0..4).map(|v| MacroLeaf::new(&alloc, v).unwrap()).collect();
    let r = |i: usize| unsafe { BStackRef::from_range(leaves[i].handle().range()) };
    let e = RefVecArrEnum::new(
        &alloc,
        RefVecArrEnumData::Rows(vec![[r(0), r(1)], [r(2), r(3)]]),
    )
    .unwrap();

    match e.handle().read(&alloc).unwrap() {
        RefVecArrEnumView::Rows(v) => {
            // Vec<[MacroLeaf; 2]>
            assert_eq!(v.len(), 2);
            assert_eq!(v[0][0].val(stack).unwrap(), 0);
            assert_eq!(v[1][1].val(stack).unwrap(), 3);
        }
        _ => panic!("expected Rows"),
    }

    // A ref vec of arrays owns nothing: teardown leaves targets alive.
    e.bstack_drop(&alloc).unwrap();
    for l in leaves {
        assert!(l.handle().val(stack).unwrap() < 4);
        l.bstack_drop(&alloc).unwrap();
    }
}

#[bstack_enum]
enum OwnedVecArrEnum {
    Empty,
    #[bstack_owned]
    Grid(Vec<[MacroLeaf; 2]>),
}

#[test]
fn macro_enum_owned_vec_of_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let mk = |v| MacroLeaf::new(&alloc, v).unwrap();
    let e = OwnedVecArrEnum::new(
        &alloc,
        OwnedVecArrEnumData::Grid(vec![[mk(1), mk(2)], [mk(3), mk(4)]]),
    )
    .unwrap();

    match e.handle().read(&alloc).unwrap() {
        OwnedVecArrEnumView::Grid(v) => {
            assert_eq!(v.len(), 2);
            assert_eq!(v[0][0].val(stack).unwrap(), 1);
            assert_eq!(v[1][1].val(stack).unwrap(), 4);
        }
        _ => panic!("expected Grid"),
    }

    let clone = e.try_clone_in(&alloc).unwrap();
    clone.bstack_drop(&alloc).unwrap();

    // Move rebuilds Vec<[BStackOwned<MacroLeaf>; 2]> and frees the offset array.
    match bstack_move!(e, &alloc).unwrap() {
        OwnedVecArrEnumData::Grid(v) => {
            assert_eq!(v[1][0].handle().val(stack).unwrap(), 3);
            for row in v {
                for m in row {
                    m.bstack_drop(&alloc).unwrap();
                }
            }
        }
        _ => panic!("expected Grid"),
    }
}

#[bstack_enum]
enum StrongVecEnum {
    Empty,
    #[bstack_strong]
    Items(Vec<MacroStrongChild>),
}

#[test]
fn macro_enum_strong_vec_rc() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let a = MacroStrongChild::new(&alloc, 10).unwrap();
    let a_keep = a.try_clone().unwrap(); // a strong = 2
    let a_data = a_keep.handle().range().start();

    let items = crate::BStackStrongVec::from_handles(&alloc, vec![a]).unwrap();
    let e = StrongVecEnum::new(&alloc, StrongVecEnumData::Items(items)).unwrap();
    assert_eq!(strong_of(stack, a_data), 2); // e + a_keep

    // Clone bumps the strong count; its teardown restores it.
    let clone = e.try_clone_in(&alloc).unwrap();
    assert_eq!(strong_of(stack, a_data), 3);
    clone.bstack_drop(&alloc).unwrap();
    assert_eq!(strong_of(stack, a_data), 2);

    e.bstack_drop(&alloc).unwrap();
    assert_eq!(strong_of(stack, a_data), 1); // a_keep only
    drop(a_keep);
}

#[bstack_enum]
enum PodVecEnum {
    Empty,
    Nums(Vec<u32>),
    Text(String),
}

#[test]
fn macro_enum_pod_vec() {
    use crate::BStackVec;
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    let nums = BStackVec::<u32, _>::from_slice(&alloc, &[1u32, 2, 3]).unwrap();
    let e = PodVecEnum::new(&alloc, PodVecEnumData::Nums(nums)).unwrap();
    match e.handle().read(&alloc).unwrap() {
        PodVecEnumView::Nums(v) => assert_eq!(v.to_vec().unwrap(), vec![1u32, 2, 3]),
        _ => panic!("expected Nums"),
    }

    // Clone deep-copies the data block.
    let clone = e.try_clone_in(&alloc).unwrap();
    match clone.handle().read(&alloc).unwrap() {
        PodVecEnumView::Nums(v) => assert_eq!(v.to_vec().unwrap(), vec![1u32, 2, 3]),
        _ => panic!("expected Nums"),
    }
    clone.bstack_drop(&alloc).unwrap();

    // Move hands back the BStackVec.
    match bstack_move!(e, &alloc).unwrap() {
        PodVecEnumData::Nums(v) => {
            assert_eq!(v.to_vec().unwrap(), vec![1u32, 2, 3]);
            v.bstack_drop().unwrap();
        }
        _ => panic!("expected Nums"),
    }

    // String variant round-trips as bytes.
    let text = BStackVec::<u8, _>::from_slice(&alloc, b"hello").unwrap();
    let e2 = PodVecEnum::new(&alloc, PodVecEnumData::Text(text)).unwrap();
    match e2.handle().read(&alloc).unwrap() {
        PodVecEnumView::Text(v) => assert_eq!(v.to_vec().unwrap(), b"hello"),
        _ => panic!("expected Text"),
    }
    e2.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// Generic blocks (layout-preserving: type params only in #[bstack_ref] fields)
// --------------------------------------------------------------------------

#[bstack_block]
struct RefBox<T> {
    #[bstack_ref]
    item: T,
    tag: u64,
}

#[test]
fn macro_generic_ref_box() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let leaf = MacroLeaf::new(&alloc, 42).unwrap();
    let r = unsafe { BStackRef::from_range(leaf.handle().range()) };
    let b = RefBox::<MacroLeaf>::new(&alloc, r, 7).unwrap();
    assert_eq!(b.handle().tag(stack).unwrap(), 7);
    assert_eq!(b.handle().item(stack).unwrap().val(stack).unwrap(), 42);

    // Clone aliases the ref (same target block); the box itself is fresh.
    let clone = b.try_clone_in(&alloc).unwrap();
    assert_eq!(
        clone.handle().item(stack).unwrap().range().start(),
        b.handle().item(stack).unwrap().range().start()
    );
    assert_ne!(clone.handle().range().start(), b.handle().range().start());
    clone.bstack_drop(&alloc).unwrap();

    // The box references but does not own the leaf: dropping it leaves it alive.
    b.bstack_drop(&alloc).unwrap();
    assert_eq!(leaf.handle().val(stack).unwrap(), 42);
    leaf.bstack_drop(&alloc).unwrap();
}

#[test]
fn macro_generic_distinct_tags() {
    // Each instantiation gets a distinct discriminant, so `bstack_cast!` can't
    // confuse `RefBox<A>` with `RefBox<B>` (they have the same layout).
    assert_ne!(
        <RefBox<MacroLeaf> as BStackCast>::eightcc(),
        <RefBox<MacroStrongChild> as BStackCast>::eightcc(),
    );
    // …and distinct from an unrelated block, and from the type argument itself.
    assert_ne!(
        <RefBox<MacroLeaf> as BStackCast>::eightcc(),
        <MacroLeaf as BStackCast>::eightcc(),
    );
}

#[test]
fn macro_generic_move_cast() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let leaf = MacroLeaf::new(&alloc, 9).unwrap();
    let r = unsafe { BStackRef::from_range(leaf.handle().range()) };
    let b = RefBox::<MacroLeaf>::new(&alloc, r, 3).unwrap();

    // bstack_cast!: an untyped slice back to the typed generic block (tag checked).
    let sl = b.handle().as_slice(stack);
    let back = bstack_cast!(sl as RefBox<MacroLeaf>)
        .unwrap()
        .expect("same tag");
    assert_eq!(back.item(stack).unwrap().val(stack).unwrap(), 9);

    // bstack_move!: hand out the ref + pod fields, freeing the box shell.
    let (item, tag) = bstack_move!(b, &alloc).unwrap();
    assert_eq!(tag, 3);
    assert_eq!(item.into_range().start(), leaf.handle().range().start());
    leaf.bstack_drop(&alloc).unwrap();
}

#[bstack_block]
struct StrongBox<T> {
    #[bstack_strong]
    item: T,
    tag: u64,
}

#[test]
fn macro_generic_strong_box() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let c = MacroStrongChild::new(&alloc, 10).unwrap();
    let keep = c.try_clone().unwrap(); // strong = 2
    let data = keep.handle().range().start();

    let b = StrongBox::<MacroStrongChild>::new(&alloc, c, 5).unwrap();
    assert_eq!(strong_of(stack, data), 2); // b + keep
    assert_eq!(b.handle().tag(stack).unwrap(), 5);

    // Deep-cloning the box bumps the shared child's strong count.
    let clone = b.try_clone_in(&alloc).unwrap();
    assert_eq!(strong_of(stack, data), 3);
    clone.bstack_drop(&alloc).unwrap();
    assert_eq!(strong_of(stack, data), 2);

    b.bstack_drop(&alloc).unwrap();
    assert_eq!(strong_of(stack, data), 1); // keep only
    drop(keep);
}

#[bstack_block]
struct WeakBox<T> {
    #[bstack_weak]
    item: T,
}

#[test]
fn macro_generic_weak_box() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let c = MacroStrongChild::new(&alloc, 7).unwrap();
    let b = WeakBox::<MacroStrongChild>::new(&alloc).unwrap();
    b.handle().set_item(&alloc, c.downgrade().unwrap()).unwrap();

    let up = b.handle().item(&alloc).unwrap().expect("alive");
    assert_eq!(up.handle().val(stack).unwrap(), 7);
    drop(up);

    drop(c); // sole strong owner gone → can't upgrade
    assert!(b.handle().item(&alloc).unwrap().is_none());
    b.bstack_drop(&alloc).unwrap();
}

#[bstack_block]
struct OwnedBox<T> {
    #[bstack_owned]
    item: T,
    tag: u64,
}

#[test]
fn macro_generic_owned_box() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let leaf = MacroLeaf::new(&alloc, 42).unwrap();
    let leaf_off = leaf.handle().range().start();
    let b = OwnedBox::<MacroLeaf>::new(&alloc, leaf, 7).unwrap();
    assert_eq!(b.handle().tag(stack).unwrap(), 7);
    assert_eq!(b.handle().item(stack).unwrap().val(stack).unwrap(), 42);

    // Deep clone: the owned child is a FRESH block (distinct offset), same value.
    let clone = b.try_clone_in(&alloc).unwrap();
    let citem = clone.handle().item(stack).unwrap();
    assert_eq!(citem.val(stack).unwrap(), 42);
    assert_ne!(
        citem.range().start(),
        b.handle().item(stack).unwrap().range().start()
    );
    clone.bstack_drop(&alloc).unwrap();
    // Original child survives the clone's teardown.
    assert_eq!(b.handle().item(stack).unwrap().val(stack).unwrap(), 42);

    // Dropping the box frees its owned child; the slot is reclaimable.
    b.bstack_drop(&alloc).unwrap();
    let leaf_size = size_of::<<MacroLeaf as BStackBlock>::OnDisk>() as u64;
    let reused = alloc_block(&alloc, MacroLeaf::eightcc(), leaf_size).unwrap();
    assert_eq!(reused.start(), leaf_off);
    unsafe { dealloc_range(&alloc, reused).unwrap() };
}

#[bstack_block]
struct OwnsEnumG<T> {
    #[bstack_owned]
    e: T,
    n: u32,
}

// Generic owned Vec / array compile (deep-clone/teardown reuse the concrete paths).
#[bstack_block]
struct OwnedVecG<T> {
    #[bstack_owned]
    items: Vec<T>,
}
#[bstack_block]
struct OwnedArrG<T> {
    #[bstack_owned]
    items: [T; 2],
}

#[test]
fn macro_generic_owns_enum() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    // ArrEnum::Leaves owns two MacroLeaf children.
    let e = ArrEnum::new(
        &alloc,
        ArrEnumData::Leaves([
            MacroLeaf::new(&alloc, 1).unwrap(),
            MacroLeaf::new(&alloc, 2).unwrap(),
        ]),
    )
    .unwrap();
    let b = OwnsEnumG::<ArrEnum>::new(&alloc, e, 9).unwrap();

    // Deep clone must recurse into the owned enum's OWN owned children — which
    // works only because the enum's clone hook is a `BStackBlock` trait method
    // (reachable through the generic `T` bound), not a generated inherent method.
    let clone = b.try_clone_in(&alloc).unwrap();
    match clone.handle().e(stack).unwrap().read(&alloc).unwrap() {
        ArrEnumView::Leaves(a) => assert_eq!(a[1].val(stack).unwrap(), 2),
        _ => panic!("expected Leaves"),
    }
    clone.bstack_drop(&alloc).unwrap();
    b.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// Generic blocks storing T INLINE: POD (`item: T`, T: Pod) and #[embed]
// (`item: T`, T: BStackBlock) — XOnDisk<T> is generic over the stored param.
// --------------------------------------------------------------------------

#[bstack_block]
struct PodBoxG<T> {
    item: T,
    tag: u64,
}

#[test]
fn macro_generic_pod_box() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let b = PodBoxG::<u32>::new(&alloc, 42u32, 7).unwrap();
    assert_eq!(b.handle().item(stack).unwrap(), 42);
    assert_eq!(b.handle().tag(stack).unwrap(), 7);

    // Clone byte-copies the POD value.
    let clone = b.try_clone_in(&alloc).unwrap();
    assert_eq!(clone.handle().item(stack).unwrap(), 42);
    clone.bstack_drop(&alloc).unwrap();

    // Distinct type args → distinct on-disk layout → distinct tags.
    assert_ne!(
        <PodBoxG<u32> as BStackCast>::eightcc(),
        <PodBoxG<u64> as BStackCast>::eightcc(),
    );

    // Move hands the POD value back.
    let (item, tag) = bstack_move!(b, &alloc).unwrap();
    assert_eq!((item, tag), (42u32, 7u64));
}

#[bstack_block]
struct EmbBoxG<T> {
    #[embed]
    item: T,
    tag: u32,
}

#[test]
fn macro_generic_emb_box() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // EmbChild owns a MacroLeaf; embedding inlines the whole child on disk.
    let child = EmbChild::new(&alloc, MacroLeaf::new(&alloc, 10).unwrap(), 1).unwrap();
    let b = EmbBoxG::<EmbChild>::new(&alloc, child, 99).unwrap();
    assert_eq!(b.handle().tag(stack).unwrap(), 99);
    // Accessor: an EmbChild handle into the inline slot (pure offset math).
    assert_eq!(
        b.handle().item().leaf(stack).unwrap().val(stack).unwrap(),
        10
    );

    // Clone folds the embedded child inline, deep-cloning its owned leaf — via the
    // generic `T`'s `BStackBlock` clone hook (a trait method, not inherent).
    let clone = b.try_clone_in(&alloc).unwrap();
    assert_eq!(
        clone
            .handle()
            .item()
            .leaf(stack)
            .unwrap()
            .val(stack)
            .unwrap(),
        10
    );
    assert_ne!(
        clone.handle().item().leaf(stack).unwrap().range().start(),
        b.handle().item().leaf(stack).unwrap().range().start()
    );
    clone.bstack_drop(&alloc).unwrap();

    // Move re-homes the embedded child to a fresh standalone block.
    let (moved, tag) = bstack_move!(b, &alloc).unwrap();
    assert_eq!(tag, 99);
    assert_eq!(moved.handle().leaf(stack).unwrap().val(stack).unwrap(), 10);
    moved.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// Generic enums (layout-preserving: type params only in reference variants)
// --------------------------------------------------------------------------

#[bstack_enum]
enum BoxEnumG<T> {
    Empty,
    Tag(u32),
    #[bstack_owned]
    Item(T),
}

#[test]
fn macro_generic_enum_owned() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let leaf = MacroLeaf::new(&alloc, 42).unwrap();
    let e = BoxEnumG::<MacroLeaf>::new(&alloc, BoxEnumGData::Item(leaf)).unwrap();
    match e.handle().read(&alloc).unwrap() {
        BoxEnumGView::Item(l) => assert_eq!(l.val(stack).unwrap(), 42),
        _ => panic!("expected Item"),
    }

    // Deep clone recurses into the owned child through T's BStackBlock hooks.
    let clone = e.try_clone_in(&alloc).unwrap();
    match clone.handle().read(&alloc).unwrap() {
        BoxEnumGView::Item(l) => assert_eq!(l.val(stack).unwrap(), 42),
        _ => panic!("expected Item"),
    }
    clone.bstack_drop(&alloc).unwrap();

    // Distinct instantiations → distinct tags.
    assert_ne!(
        <BoxEnumG<MacroLeaf> as BStackCast>::eightcc(),
        <BoxEnumG<MacroStrongChild> as BStackCast>::eightcc(),
    );

    // Move yields the owned child.
    match bstack_move!(e, &alloc).unwrap() {
        BoxEnumGData::Item(owned) => {
            assert_eq!(owned.handle().val(stack).unwrap(), 42);
            owned.bstack_drop(&alloc).unwrap();
        }
        _ => panic!("expected Item"),
    }
}

#[bstack_enum]
enum StrongEnumG<T> {
    Empty,
    #[bstack_strong]
    S(T),
}

#[test]
fn macro_generic_enum_strong() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let c = MacroStrongChild::new(&alloc, 5).unwrap();
    let keep = c.try_clone().unwrap(); // strong = 2
    let data = keep.handle().range().start();

    let e = StrongEnumG::<MacroStrongChild>::new(&alloc, StrongEnumGData::S(c)).unwrap();
    assert_eq!(strong_of(stack, data), 2); // e + keep

    // Clone bumps the strong count; teardown restores it.
    let clone = e.try_clone_in(&alloc).unwrap();
    assert_eq!(strong_of(stack, data), 3);
    clone.bstack_drop(&alloc).unwrap();
    assert_eq!(strong_of(stack, data), 2);
    e.bstack_drop(&alloc).unwrap();
    assert_eq!(strong_of(stack, data), 1);
    drop(keep);
}

// --------------------------------------------------------------------------
// Const generics: `[T; N]` / `[Pod; N]` with a generic `const N: usize`.
// --------------------------------------------------------------------------

#[bstack_block]
struct RefArrN<T, const N: usize> {
    #[bstack_ref]
    arr: [T; N],
    tag: u64,
}

#[bstack_block]
struct OwnArrN<T, const N: usize> {
    #[bstack_owned]
    arr: [T; N],
}

#[bstack_block]
struct PodArrN<const N: usize> {
    xs: [u16; N],
    tag: u32,
}

#[test]
fn macro_generic_const_ref_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let leaves: Vec<_> = (0..3).map(|v| MacroLeaf::new(&alloc, v).unwrap()).collect();
    let r = |i: usize| unsafe { BStackRef::from_range(leaves[i].handle().range()) };
    let b = RefArrN::<MacroLeaf, 3>::new(&alloc, [r(0), r(1), r(2)], 9).unwrap();
    assert_eq!(b.handle().tag(stack).unwrap(), 9);
    let arr = b.handle().arr(stack).unwrap(); // [MacroLeaf; 3]
    assert_eq!(arr[0].val(stack).unwrap(), 0);
    assert_eq!(arr[2].val(stack).unwrap(), 2);

    // Distinct N → distinct on-disk layout → distinct tags.
    assert_ne!(
        <RefArrN<MacroLeaf, 3> as BStackCast>::eightcc(),
        <RefArrN<MacroLeaf, 4> as BStackCast>::eightcc(),
    );

    // A ref array owns nothing: dropping leaves the targets alive.
    b.bstack_drop(&alloc).unwrap();
    for l in leaves {
        assert!(l.handle().val(stack).unwrap() < 3);
        l.bstack_drop(&alloc).unwrap();
    }
}

#[test]
fn macro_generic_const_owned_pod_array() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Owned const array: deep-clone + teardown reuse the concrete paths.
    let mk = |v| MacroLeaf::new(&alloc, v).unwrap();
    let o = OwnArrN::<MacroLeaf, 2>::new(&alloc, [mk(10), mk(20)]).unwrap();
    let a = o.handle().arr(stack).unwrap();
    assert_eq!(a[1].val(stack).unwrap(), 20);
    let clone = o.try_clone_in(&alloc).unwrap();
    assert_ne!(
        clone.handle().arr(stack).unwrap()[0].range().start(),
        a[0].range().start()
    );
    clone.bstack_drop(&alloc).unwrap();
    o.bstack_drop(&alloc).unwrap();

    // POD const array.
    let p = PodArrN::<4>::new(&alloc, [1u16, 2, 3, 4], 7).unwrap();
    assert_eq!(p.handle().xs(stack).unwrap(), [1u16, 2, 3, 4]);
    assert_eq!(p.handle().tag(stack).unwrap(), 7);
    p.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// stdlib: BStackCow<T> — clone-on-write ownership of a block
// --------------------------------------------------------------------------

#[test]
fn stdlib_cow_borrowed_into_owned_deep_copies() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // A block owned elsewhere; the Cow only borrows it.
    let base = MacroLeaf::new(&alloc, 7).unwrap();
    let base_start = base.handle().range().start();
    let cow =
        BStackCow::borrowed(unsafe { BStackRef::<MacroLeaf>::from_range(base.handle().range()) });

    assert!(cow.is_borrowed());
    // Reads go through the borrowed block, at its address.
    assert_eq!(cow.handle().val(stack).unwrap(), 7);
    assert_eq!(cow.range().start(), base_start);

    // into_owned deep-copies: a fresh block at a different address, same value.
    let owned = cow.into_owned(&alloc).unwrap();
    assert_ne!(owned.handle().range().start(), base_start);
    assert_eq!(owned.handle().val(stack).unwrap(), 7);
    owned.bstack_drop(&alloc).unwrap();

    // The borrowed source is untouched.
    assert_eq!(base.handle().val(stack).unwrap(), 7);
    base.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_cow_owned_into_owned_is_free() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let src = MacroLeaf::new(&alloc, 5).unwrap();
    let start = src.handle().range().start();
    let cow = BStackCow::owned(src);
    assert!(cow.is_owned());

    // Already owned: into_owned hands back the *same* block, no copy.
    let owned = cow.into_owned(&alloc).unwrap();
    assert_eq!(owned.handle().range().start(), start);
    assert_eq!(owned.handle().val(stack).unwrap(), 5);
    owned.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_cow_to_mut_copies_then_owns() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let base = MacroLeaf::new(&alloc, 9).unwrap();
    let base_start = base.handle().range().start();
    let mut cow =
        BStackCow::borrowed(unsafe { BStackRef::<MacroLeaf>::from_range(base.handle().range()) });

    // First write forces a private copy and flips to Owned.
    {
        let m = cow.to_mut(&alloc).unwrap();
        assert_ne!(m.handle().range().start(), base_start);
        assert_eq!(m.handle().val(stack).unwrap(), 9);
    }
    assert!(cow.is_owned());

    // A second to_mut is a no-op: still the same owned copy.
    let owned_start = cow.range().start();
    let _ = cow.to_mut(&alloc).unwrap();
    assert_eq!(cow.range().start(), owned_start);

    // Dropping the Cow frees only the copy; the borrowed source survives.
    cow.bstack_drop(&alloc).unwrap();
    assert_eq!(base.handle().val(stack).unwrap(), 9);
    base.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_cow_borrowed_drop_frees_nothing() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let base = MacroLeaf::new(&alloc, 3).unwrap();
    let cow =
        BStackCow::borrowed(unsafe { BStackRef::<MacroLeaf>::from_range(base.handle().range()) });

    // Dropping a borrowed Cow has no claim on the target.
    cow.bstack_drop(&alloc).unwrap();
    assert_eq!(base.handle().val(stack).unwrap(), 3);
    base.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// stdlib: BStackBox<T> — an owned single-value block for Pod T
// --------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Debug, bytemuck::Pod, bytemuck::Zeroable)]
struct Point3 {
    x: i32,
    y: i32,
    z: i32,
}

// A block that owns a box as a child, proving BStackBox composes as a field.
#[bstack_block]
struct BoxHolder {
    #[bstack_owned]
    boxed: BStackBox<u64>,
    tag: u32,
}

#[test]
fn stdlib_box_roundtrip_and_set() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // A bare scalar owned as its own block — no macro struct needed.
    let b = BStackBox::new(&alloc, 42u64).unwrap();
    assert_eq!(b.handle().get(stack).unwrap(), 42);

    // In-place overwrite.
    b.handle().set(&alloc, 99).unwrap();
    assert_eq!(b.handle().get(stack).unwrap(), 99);

    // A plain POD struct payload works too (the point of the Pod bound).
    let p = BStackBox::new(&alloc, Point3 { x: 1, y: 2, z: 3 }).unwrap();
    assert_eq!(p.handle().get(stack).unwrap(), Point3 { x: 1, y: 2, z: 3 });

    b.bstack_drop(&alloc).unwrap();
    p.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_box_clone_is_a_byte_copy() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let b = BStackBox::new(&alloc, 7u32).unwrap();
    let clone = b.try_clone_in(&alloc).unwrap();

    // Fresh, independent block, same value.
    assert_ne!(clone.handle().range().start(), b.handle().range().start());
    assert_eq!(clone.handle().get(stack).unwrap(), 7);

    // Mutating the clone leaves the original untouched.
    clone.handle().set(&alloc, 8).unwrap();
    assert_eq!(b.handle().get(stack).unwrap(), 7);

    b.bstack_drop(&alloc).unwrap();
    clone.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_box_move_yields_the_value() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    let b = BStackBox::new(&alloc, 123u64).unwrap();
    let start = b.handle().range().start();
    let value = bstack_move!(b, &alloc).unwrap();
    assert_eq!(value, 123);

    // The shell was freed: its slot is reused by the next allocation.
    let b2 = BStackBox::new(&alloc, 5u64).unwrap();
    assert_eq!(b2.handle().range().start(), start);
    b2.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_box_distinct_tags_by_size() {
    // Boxes of differently-sized payloads get distinct tags.
    assert_ne!(
        <BStackBox<u32> as BStackCast>::eightcc(),
        <BStackBox<u64> as BStackCast>::eightcc(),
    );
    // Same size => same tag (the generic-POD tag scheme distinguishes by size).
    assert_eq!(
        <BStackBox<u32> as BStackCast>::eightcc(),
        <BStackBox<i32> as BStackCast>::eightcc(),
    );
}

#[test]
fn stdlib_box_composes_as_owned_field() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let inner = BStackBox::new(&alloc, 500u64).unwrap();
    let holder = BoxHolder::new(&alloc, inner, 9).unwrap();
    assert_eq!(
        holder.handle().boxed(stack).unwrap().get(stack).unwrap(),
        500
    );
    assert_eq!(holder.handle().tag(stack).unwrap(), 9);

    // Deep-cloning the parent recurses into the child box (fresh child block).
    let clone = holder.try_clone_in(&alloc).unwrap();
    assert_ne!(
        clone.handle().boxed(stack).unwrap().range().start(),
        holder.handle().boxed(stack).unwrap().range().start(),
    );
    assert_eq!(
        clone.handle().boxed(stack).unwrap().get(stack).unwrap(),
        500
    );

    clone.bstack_drop(&alloc).unwrap();
    holder.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_box_in_cow() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // A borrowed Cow over a box; first write deep-copies the box.
    let base = BStackBox::new(&alloc, 11u64).unwrap();
    let mut cow = BStackCow::borrowed(unsafe {
        BStackRef::<BStackBox<u64>>::from_range(base.handle().range())
    });
    assert_eq!(cow.handle().get(stack).unwrap(), 11);

    let owned = cow.to_mut(&alloc).unwrap();
    owned.handle().set(&alloc, 22).unwrap();
    assert_ne!(cow.range().start(), base.handle().range().start());
    assert_eq!(base.handle().get(stack).unwrap(), 11); // source untouched

    cow.bstack_drop(&alloc).unwrap();
    base.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// stdlib: BStackLinkedList<T> — owned doubly-linked list of block values
// --------------------------------------------------------------------------

fn list_values(list: &BStackLinkedList<MacroLeaf>, stack: &BStack) -> Vec<u32> {
    list.to_vec(stack)
        .unwrap()
        .iter()
        .map(|h| h.val(stack).unwrap())
        .collect()
}

#[test]
fn stdlib_list_push_back_pop_front() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let list = BStackLinkedList::<MacroLeaf>::new(&alloc).unwrap();
    assert!(list.is_empty(stack).unwrap());

    for v in [1u32, 2, 3] {
        list.push_back(&alloc, MacroLeaf::new(&alloc, v).unwrap())
            .unwrap();
    }
    assert_eq!(list.len(stack).unwrap(), 3);
    assert_eq!(list_values(&list, stack), vec![1, 2, 3]);
    assert_eq!(list.front(stack).unwrap().unwrap().val(stack).unwrap(), 1);
    assert_eq!(list.back(stack).unwrap().unwrap().val(stack).unwrap(), 3);

    // FIFO drain from the front.
    let a = list.pop_front(&alloc).unwrap().unwrap();
    assert_eq!(a.handle().val(stack).unwrap(), 1);
    a.bstack_drop(&alloc).unwrap();
    assert_eq!(list.len(stack).unwrap(), 2);
    assert_eq!(list_values(&list, stack), vec![2, 3]);

    list.bstack_drop(&alloc).unwrap(); // frees remaining nodes + values
}

#[test]
fn stdlib_list_both_ends() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let list = BStackLinkedList::<MacroLeaf>::new(&alloc).unwrap();
    list.push_front(&alloc, MacroLeaf::new(&alloc, 2).unwrap())
        .unwrap();
    list.push_front(&alloc, MacroLeaf::new(&alloc, 1).unwrap())
        .unwrap();
    list.push_back(&alloc, MacroLeaf::new(&alloc, 3).unwrap())
        .unwrap();
    assert_eq!(list_values(&list, stack), vec![1, 2, 3]);

    let back = list.pop_back(&alloc).unwrap().unwrap();
    assert_eq!(back.handle().val(stack).unwrap(), 3);
    back.bstack_drop(&alloc).unwrap();

    let front = list.pop_front(&alloc).unwrap().unwrap();
    assert_eq!(front.handle().val(stack).unwrap(), 1);
    front.bstack_drop(&alloc).unwrap();

    assert_eq!(list_values(&list, stack), vec![2]);
    assert!(list.pop_back(&alloc).unwrap().is_some());
    assert!(list.is_empty(stack).unwrap());
    assert!(list.pop_front(&alloc).unwrap().is_none());

    list.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_list_drop_is_recursive() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    // A value type that itself owns a child, to prove teardown recurses through
    // the node's single value ref into the value's own children.
    let leaf = MacroLeaf::new(&alloc, 10).unwrap();
    let leaf_start = leaf.handle().range().start();
    let parent = MacroParent::new(&alloc, leaf, 1).unwrap();

    let list = BStackLinkedList::<MacroParent>::new(&alloc).unwrap();
    list.push_back(&alloc, parent).unwrap();
    list.bstack_drop(&alloc).unwrap();

    // The leaf (a grandchild, freed only via full recursion) slot is reclaimed.
    let reused = MacroLeaf::new(&alloc, 0).unwrap();
    assert_eq!(reused.handle().range().start(), leaf_start);
    reused.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_list_deep_clone_is_independent() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let list = BStackLinkedList::<MacroLeaf>::new(&alloc).unwrap();
    for v in [1u32, 2, 3] {
        list.push_back(&alloc, MacroLeaf::new(&alloc, v).unwrap())
            .unwrap();
    }

    let clone = list.try_clone_in(&alloc).unwrap();
    assert_eq!(list_values(&clone, stack), vec![1, 2, 3]);

    // The clone's values are fresh blocks, not aliases of the source's.
    assert_ne!(
        clone.front(stack).unwrap().unwrap().range().start(),
        list.front(stack).unwrap().unwrap().range().start(),
    );

    // Mutating the clone leaves the original intact.
    let popped = clone.pop_back(&alloc).unwrap().unwrap();
    popped.bstack_drop(&alloc).unwrap();
    assert_eq!(clone.len(stack).unwrap(), 2);
    assert_eq!(list.len(stack).unwrap(), 3);
    assert_eq!(list_values(&list, stack), vec![1, 2, 3]);

    clone.bstack_drop(&alloc).unwrap();
    list.bstack_drop(&alloc).unwrap();
}

/// Many threads hammering one shared list. Phase 1 is concurrent `push_back`
/// only; phase 2 is concurrent `pop_front` only. If the relink/len RMW were not
/// atomic under contention, lost updates would corrupt the chain (a wrong length,
/// a broken `next` walk, or duplicate/missing values). The `inplace_gen`-based
/// [`crate::BStackLinkedList`] mutators need no external lock around them.
#[test]
fn stdlib_list_concurrent_push_pop() {
    // Kept modest: each op is a durable `inplace_gen` commit (an fsync), so the
    // cost is in the op count, not the thread count — the contention that would
    // expose a non-atomic RMW comes from the parallel threads, not from more
    // iterations.
    const THREADS: u32 = 8;
    const ITERS: u32 = 8;

    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let list = BStackLinkedList::<MacroLeaf>::new(&alloc).unwrap();
    let total = (THREADS * ITERS) as u64;

    // Phase 1 — concurrent pushes of distinct values.
    std::thread::scope(|s| {
        for t in 0..THREADS {
            let list = &list;
            let alloc = &alloc;
            s.spawn(move || {
                for i in 0..ITERS {
                    let leaf = MacroLeaf::new(alloc, t * ITERS + i).unwrap();
                    list.push_back(alloc, leaf).unwrap();
                }
            });
        }
    });

    assert_eq!(list.len(alloc.stack()).unwrap(), total);
    // Chain integrity: walking `next` yields exactly the distinct values 0..total.
    let mut seen: Vec<u32> = list
        .to_vec(alloc.stack())
        .unwrap()
        .iter()
        .map(|h| h.val(alloc.stack()).unwrap())
        .collect();
    seen.sort_unstable();
    assert_eq!(seen.len() as u64, total);
    seen.dedup();
    assert_eq!(seen.len() as u64, total, "no lost/duplicated nodes");
    assert_eq!(seen.first().copied(), Some(0));
    assert_eq!(seen.last().copied(), Some(total as u32 - 1));

    // Phase 2 — concurrent pops (exactly `total` across threads, so none miss).
    std::thread::scope(|s| {
        for _ in 0..THREADS {
            let list = &list;
            let alloc = &alloc;
            s.spawn(move || {
                for _ in 0..ITERS {
                    let v = list.pop_front(alloc).unwrap().expect("list non-empty");
                    v.bstack_drop(alloc).unwrap();
                }
            });
        }
    });

    assert_eq!(list.len(alloc.stack()).unwrap(), 0);
    assert!(list.front(alloc.stack()).unwrap().is_none());
    list.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// stdlib: BStackDeque<T> — owned double-ended queue over a contiguous ring
// --------------------------------------------------------------------------

fn deque_values(dq: &BStackDeque<MacroLeaf>, stack: &BStack) -> Vec<u32> {
    dq.to_vec(stack)
        .unwrap()
        .iter()
        .map(|h| h.val(stack).unwrap())
        .collect()
}

#[test]
fn stdlib_deque_push_back_grows() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let dq = BStackDeque::<MacroLeaf>::new(&alloc).unwrap();
    assert!(dq.is_empty(stack).unwrap());

    // Push past the initial capacity to force at least one growth.
    for v in 0..10u32 {
        dq.push_back(&alloc, MacroLeaf::new(&alloc, v).unwrap())
            .unwrap();
    }
    assert_eq!(dq.len(stack).unwrap(), 10);
    assert!(dq.capacity(stack).unwrap() >= 10);
    assert_eq!(deque_values(&dq, stack), (0..10).collect::<Vec<_>>());
    assert_eq!(dq.front(stack).unwrap().unwrap().val(stack).unwrap(), 0);
    assert_eq!(dq.back(stack).unwrap().unwrap().val(stack).unwrap(), 9);

    // FIFO drain from the front.
    for v in 0..10u32 {
        let x = dq.pop_front(&alloc).unwrap().unwrap();
        assert_eq!(x.handle().val(stack).unwrap(), v);
        x.bstack_drop(&alloc).unwrap();
    }
    assert!(dq.is_empty(stack).unwrap());
    assert!(dq.pop_front(&alloc).unwrap().is_none());
    dq.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_deque_wraparound_no_growth() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Fixed capacity 4; exercise circular indexing without any growth.
    let dq = BStackDeque::<MacroLeaf>::with_capacity(&alloc, 4).unwrap();
    for v in [1u32, 2, 3, 4] {
        dq.push_back(&alloc, MacroLeaf::new(&alloc, v).unwrap())
            .unwrap();
    }
    // Drop the front two: head advances into the ring.
    for _ in 0..2 {
        dq.pop_front(&alloc)
            .unwrap()
            .unwrap()
            .bstack_drop(&alloc)
            .unwrap();
    }
    // Two more push_backs wrap around the physical slots 0,1.
    dq.push_back(&alloc, MacroLeaf::new(&alloc, 5).unwrap())
        .unwrap();
    dq.push_back(&alloc, MacroLeaf::new(&alloc, 6).unwrap())
        .unwrap();
    assert_eq!(dq.capacity(stack).unwrap(), 4); // never grew
    assert_eq!(deque_values(&dq, stack), vec![3, 4, 5, 6]);

    dq.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_deque_both_ends() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let dq = BStackDeque::<MacroLeaf>::new(&alloc).unwrap();
    dq.push_back(&alloc, MacroLeaf::new(&alloc, 2).unwrap())
        .unwrap();
    dq.push_front(&alloc, MacroLeaf::new(&alloc, 1).unwrap())
        .unwrap();
    dq.push_back(&alloc, MacroLeaf::new(&alloc, 3).unwrap())
        .unwrap();
    assert_eq!(deque_values(&dq, stack), vec![1, 2, 3]);

    let back = dq.pop_back(&alloc).unwrap().unwrap();
    assert_eq!(back.handle().val(stack).unwrap(), 3);
    back.bstack_drop(&alloc).unwrap();

    let front = dq.pop_front(&alloc).unwrap().unwrap();
    assert_eq!(front.handle().val(stack).unwrap(), 1);
    front.bstack_drop(&alloc).unwrap();

    assert_eq!(deque_values(&dq, stack), vec![2]);
    dq.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_deque_drop_is_recursive() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    let leaf = MacroLeaf::new(&alloc, 10).unwrap();
    let leaf_start = leaf.handle().range().start();
    let parent = MacroParent::new(&alloc, leaf, 1).unwrap();

    let dq = BStackDeque::<MacroParent>::new(&alloc).unwrap();
    dq.push_back(&alloc, parent).unwrap();
    dq.bstack_drop(&alloc).unwrap();

    // The leaf grandchild's slot is reclaimed — full recursion through the ring.
    let reused = MacroLeaf::new(&alloc, 0).unwrap();
    assert_eq!(reused.handle().range().start(), leaf_start);
    reused.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_deque_deep_clone_is_independent() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let dq = BStackDeque::<MacroLeaf>::new(&alloc).unwrap();
    for v in [1u32, 2, 3, 4, 5] {
        dq.push_back(&alloc, MacroLeaf::new(&alloc, v).unwrap())
            .unwrap();
    }

    let clone = dq.try_clone_in(&alloc).unwrap();
    assert_eq!(deque_values(&clone, stack), vec![1, 2, 3, 4, 5]);
    // Clone is compacted to exactly `len` slots.
    assert_eq!(clone.capacity(stack).unwrap(), 5);
    // Fresh element blocks, not aliases.
    assert_ne!(
        clone.front(stack).unwrap().unwrap().range().start(),
        dq.front(stack).unwrap().unwrap().range().start(),
    );

    // Mutating the clone leaves the original intact.
    clone
        .pop_back(&alloc)
        .unwrap()
        .unwrap()
        .bstack_drop(&alloc)
        .unwrap();
    assert_eq!(clone.len(stack).unwrap(), 4);
    assert_eq!(deque_values(&dq, stack), vec![1, 2, 3, 4, 5]);

    clone.bstack_drop(&alloc).unwrap();
    dq.bstack_drop(&alloc).unwrap();
}

/// Many threads hammering one shared deque, including concurrent growth. Phase 1
/// is concurrent `push_back`; phase 2 is concurrent `pop_front`. A non-atomic
/// slot/metadata RMW (or a racy growth) would drop or duplicate elements.
#[test]
fn stdlib_deque_concurrent_push_pop() {
    const THREADS: u32 = 8;
    const ITERS: u32 = 8;

    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let dq = BStackDeque::<MacroLeaf>::new(&alloc).unwrap();
    let total = (THREADS * ITERS) as u64;

    std::thread::scope(|s| {
        for t in 0..THREADS {
            let dq = &dq;
            let alloc = &alloc;
            s.spawn(move || {
                for i in 0..ITERS {
                    let leaf = MacroLeaf::new(alloc, t * ITERS + i).unwrap();
                    dq.push_back(alloc, leaf).unwrap();
                }
            });
        }
    });

    assert_eq!(dq.len(alloc.stack()).unwrap(), total);
    let mut seen = deque_values(&dq, alloc.stack());
    seen.sort_unstable();
    assert_eq!(seen.len() as u64, total);
    seen.dedup();
    assert_eq!(seen.len() as u64, total, "no lost/duplicated elements");

    std::thread::scope(|s| {
        for _ in 0..THREADS {
            let dq = &dq;
            let alloc = &alloc;
            s.spawn(move || {
                for _ in 0..ITERS {
                    let v = dq.pop_front(alloc).unwrap().expect("deque non-empty");
                    v.bstack_drop(alloc).unwrap();
                }
            });
        }
    });

    assert_eq!(dq.len(alloc.stack()).unwrap(), 0);
    dq.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// stdlib: BStackHashMap<K, V> — owned open-addressing hash map
// --------------------------------------------------------------------------

#[test]
fn stdlib_map_insert_get_remove() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let map = BStackHashMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    assert!(map.is_empty(stack).unwrap());
    assert!(map.get(stack, &7).unwrap().is_none());

    // Insert of a new key returns no previous value.
    assert!(
        map.insert(&alloc, 7, MacroLeaf::new(&alloc, 700).unwrap())
            .unwrap()
            .is_none()
    );
    assert!(
        map.insert(&alloc, 9, MacroLeaf::new(&alloc, 900).unwrap())
            .unwrap()
            .is_none()
    );
    assert_eq!(map.len(stack).unwrap(), 2);
    assert_eq!(
        map.get(stack, &7).unwrap().unwrap().val(stack).unwrap(),
        700
    );
    assert_eq!(
        map.get(stack, &9).unwrap().unwrap().val(stack).unwrap(),
        900
    );
    assert!(map.contains_key(stack, &7).unwrap());
    assert!(!map.contains_key(stack, &8).unwrap());

    // Overwrite returns the previous value (owned) and does not change len.
    let old = map
        .insert(&alloc, 7, MacroLeaf::new(&alloc, 701).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(old.handle().val(stack).unwrap(), 700);
    old.bstack_drop(&alloc).unwrap();
    assert_eq!(map.len(stack).unwrap(), 2);
    assert_eq!(
        map.get(stack, &7).unwrap().unwrap().val(stack).unwrap(),
        701
    );

    // Remove returns the value (owned); the key is then absent.
    let removed = map.remove(&alloc, &9).unwrap().unwrap();
    assert_eq!(removed.handle().val(stack).unwrap(), 900);
    removed.bstack_drop(&alloc).unwrap();
    assert!(map.get(stack, &9).unwrap().is_none());
    assert!(map.remove(&alloc, &9).unwrap().is_none());
    assert_eq!(map.len(stack).unwrap(), 1);

    map.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_map_grows_and_keeps_all() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let map = BStackHashMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    // Enough entries to force several rehashes (cap 4 -> ... ).
    for k in 0..100u32 {
        assert!(
            map.insert(&alloc, k, MacroLeaf::new(&alloc, k * 10).unwrap())
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(map.len(stack).unwrap(), 100);
    // Every key survives the rehashes with its value.
    for k in 0..100u32 {
        assert_eq!(
            map.get(stack, &k).unwrap().unwrap().val(stack).unwrap(),
            k * 10
        );
    }

    // Remove the evens; odds remain (exercises tombstones + probing past them).
    for k in (0..100u32).step_by(2) {
        map.remove(&alloc, &k)
            .unwrap()
            .unwrap()
            .bstack_drop(&alloc)
            .unwrap();
    }
    assert_eq!(map.len(stack).unwrap(), 50);
    for k in 0..100u32 {
        let got = map.get(stack, &k).unwrap();
        if k % 2 == 0 {
            assert!(got.is_none());
        } else {
            assert_eq!(got.unwrap().val(stack).unwrap(), k * 10);
        }
    }

    map.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_map_pod_struct_key() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // A composite Pod key (see Point3, defined for the box tests).
    let map = BStackHashMap::<Point3, MacroLeaf>::new(&alloc).unwrap();
    let a = Point3 { x: 1, y: 2, z: 3 };
    let b = Point3 { x: 1, y: 2, z: 4 };
    map.insert(&alloc, a, MacroLeaf::new(&alloc, 11).unwrap())
        .unwrap();
    map.insert(&alloc, b, MacroLeaf::new(&alloc, 22).unwrap())
        .unwrap();
    assert_eq!(map.get(stack, &a).unwrap().unwrap().val(stack).unwrap(), 11);
    assert_eq!(map.get(stack, &b).unwrap().unwrap().val(stack).unwrap(), 22);
    assert!(
        map.get(stack, &Point3 { x: 9, y: 9, z: 9 })
            .unwrap()
            .is_none()
    );

    map.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_map_distinct_tags() {
    assert_ne!(
        <BStackHashMap<u32, MacroLeaf> as BStackCast>::eightcc(),
        <BStackHashMap<u64, MacroLeaf> as BStackCast>::eightcc(),
    );
    assert_ne!(
        <BStackHashMap<u32, MacroLeaf> as BStackCast>::eightcc(),
        <BStackHashMap<u32, MacroStrongChild> as BStackCast>::eightcc(),
    );
}

#[test]
fn stdlib_map_drop_is_recursive() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    let leaf = MacroLeaf::new(&alloc, 10).unwrap();
    let leaf_start = leaf.handle().range().start();
    let parent = MacroParent::new(&alloc, leaf, 1).unwrap();

    let map = BStackHashMap::<u32, MacroParent>::new(&alloc).unwrap();
    map.insert(&alloc, 42, parent).unwrap();
    map.bstack_drop(&alloc).unwrap();

    // The leaf grandchild's slot is reclaimed — full recursion through a value.
    let reused = MacroLeaf::new(&alloc, 0).unwrap();
    assert_eq!(reused.handle().range().start(), leaf_start);
    reused.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_map_deep_clone_is_independent() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let map = BStackHashMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    for k in 0..8u32 {
        map.insert(&alloc, k, MacroLeaf::new(&alloc, k + 100).unwrap())
            .unwrap();
    }

    let clone = map.try_clone_in(&alloc).unwrap();
    for k in 0..8u32 {
        assert_eq!(
            clone.get(stack, &k).unwrap().unwrap().val(stack).unwrap(),
            k + 100
        );
    }
    // Clone's value blocks are fresh, not aliases.
    assert_ne!(
        clone.get(stack, &3).unwrap().unwrap().range().start(),
        map.get(stack, &3).unwrap().unwrap().range().start(),
    );

    // Mutating the clone leaves the original intact.
    clone
        .remove(&alloc, &3)
        .unwrap()
        .unwrap()
        .bstack_drop(&alloc)
        .unwrap();
    assert!(clone.get(stack, &3).unwrap().is_none());
    assert_eq!(
        map.get(stack, &3).unwrap().unwrap().val(stack).unwrap(),
        103
    );

    clone.bstack_drop(&alloc).unwrap();
    map.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// stdlib: BStackBTreeMap<K, V> — owned ordered map (copy-on-write B-tree)
// --------------------------------------------------------------------------

fn tree_pairs(tree: &BStackBTreeMap<u32, MacroLeaf>, stack: &BStack) -> Vec<(u32, u32)> {
    tree.to_vec(stack)
        .unwrap()
        .iter()
        .map(|(k, v)| (*k, v.val(stack).unwrap()))
        .collect()
}

#[test]
fn stdlib_tree_insert_get_ordered() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let tree = BStackBTreeMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    assert!(tree.is_empty(stack).unwrap());
    assert!(tree.get(stack, &5).unwrap().is_none());
    assert!(tree.first(stack).unwrap().is_none());

    // Insert 0..50 in a scrambled (but bijective) order to exercise splits.
    for i in 0..50u32 {
        let k = (i * 17) % 50;
        assert!(
            tree.insert(&alloc, k, MacroLeaf::new(&alloc, k * 10).unwrap())
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(tree.len(stack).unwrap(), 50);

    // Every key present with its value.
    for k in 0..50u32 {
        assert_eq!(
            tree.get(stack, &k).unwrap().unwrap().val(stack).unwrap(),
            k * 10
        );
    }
    assert!(tree.get(stack, &999).unwrap().is_none());

    // Ordered iteration is sorted; first/last are the extremes.
    let pairs = tree_pairs(&tree, stack);
    let expected: Vec<(u32, u32)> = (0..50u32).map(|k| (k, k * 10)).collect();
    assert_eq!(pairs, expected);
    assert_eq!(tree.first(stack).unwrap().unwrap().0, 0);
    assert_eq!(tree.last(stack).unwrap().unwrap().0, 49);

    // Overwrite returns the previous value; len unchanged; order preserved.
    let old = tree
        .insert(&alloc, 25, MacroLeaf::new(&alloc, 9999).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(old.handle().val(stack).unwrap(), 250);
    old.bstack_drop(&alloc).unwrap();
    assert_eq!(tree.len(stack).unwrap(), 50);
    assert_eq!(
        tree.get(stack, &25).unwrap().unwrap().val(stack).unwrap(),
        9999
    );

    tree.bstack_drop(&alloc).unwrap();
}

// A 64-byte Pod key: a B-tree node is 280 + 15*64 = 1240 bytes, past the 1024
// inline `Scratch` buffer, so `get` exercises the heap-spill fallback.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, bytemuck::Pod, bytemuck::Zeroable)]
struct BigKey([u64; 8]);

#[test]
fn stdlib_tree_large_key_spills() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let tree = BStackBTreeMap::<BigKey, MacroLeaf>::new(&alloc).unwrap();
    for i in 0..20u64 {
        let mut k = [0u64; 8];
        k[0] = i;
        tree.insert(&alloc, BigKey(k), MacroLeaf::new(&alloc, i as u32).unwrap())
            .unwrap();
    }
    for i in 0..20u64 {
        let mut k = [0u64; 8];
        k[0] = i;
        assert_eq!(
            tree.get(stack, &BigKey(k))
                .unwrap()
                .unwrap()
                .val(stack)
                .unwrap(),
            i as u32
        );
    }
    tree.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_tree_distinct_tags() {
    assert_ne!(
        <BStackBTreeMap<u32, MacroLeaf> as BStackCast>::eightcc(),
        <BStackBTreeMap<u64, MacroLeaf> as BStackCast>::eightcc(),
    );
    assert_ne!(
        <BStackBTreeMap<u32, MacroLeaf> as BStackCast>::eightcc(),
        <BStackBTreeMap<u32, MacroStrongChild> as BStackCast>::eightcc(),
    );
}

#[test]
fn stdlib_tree_drop_is_recursive() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    let leaf = MacroLeaf::new(&alloc, 10).unwrap();
    let leaf_start = leaf.handle().range().start();
    let parent = MacroParent::new(&alloc, leaf, 1).unwrap();

    let tree = BStackBTreeMap::<u32, MacroParent>::new(&alloc).unwrap();
    tree.insert(&alloc, 42, parent).unwrap();
    tree.bstack_drop(&alloc).unwrap();

    // The leaf grandchild's slot is reclaimed — full recursion through a value.
    let reused = MacroLeaf::new(&alloc, 0).unwrap();
    assert_eq!(reused.handle().range().start(), leaf_start);
    reused.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_tree_deep_clone_is_independent() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let tree = BStackBTreeMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    for k in 0..30u32 {
        tree.insert(&alloc, k, MacroLeaf::new(&alloc, k + 100).unwrap())
            .unwrap();
    }

    let clone = tree.try_clone_in(&alloc).unwrap();
    for k in 0..30u32 {
        assert_eq!(
            clone.get(stack, &k).unwrap().unwrap().val(stack).unwrap(),
            k + 100
        );
    }
    // Fresh value blocks, not aliases.
    assert_ne!(
        clone.get(stack, &10).unwrap().unwrap().range().start(),
        tree.get(stack, &10).unwrap().unwrap().range().start(),
    );

    // Overwriting in the clone leaves the original intact.
    tree.insert(&alloc, 10, MacroLeaf::new(&alloc, 7).unwrap())
        .unwrap()
        .unwrap()
        .bstack_drop(&alloc)
        .unwrap();
    // (`clone` and `tree` share no nodes: the clone deep-copied every node.)
    assert_eq!(
        clone.get(stack, &10).unwrap().unwrap().val(stack).unwrap(),
        110
    );
    assert_eq!(
        tree.get(stack, &10).unwrap().unwrap().val(stack).unwrap(),
        7
    );

    clone.bstack_drop(&alloc).unwrap();
    tree.bstack_drop(&alloc).unwrap();
}

/// Many threads reading one shared tree concurrently (the B-tree is
/// single-writer / multi-reader: no writes race here, only lookups).
#[test]
fn stdlib_tree_concurrent_readers() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    let tree = BStackBTreeMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    for k in 0..64u32 {
        tree.insert(&alloc, k, MacroLeaf::new(&alloc, k * 2).unwrap())
            .unwrap();
    }

    std::thread::scope(|s| {
        for _ in 0..8 {
            let tree = &tree;
            let alloc = &alloc;
            s.spawn(move || {
                for k in 0..64u32 {
                    assert_eq!(
                        tree.get(alloc.stack(), &k)
                            .unwrap()
                            .unwrap()
                            .val(alloc.stack())
                            .unwrap(),
                        k * 2
                    );
                }
            });
        }
    });

    tree.bstack_drop(&alloc).unwrap();
}

/// Many threads inserting distinct keys into one shared map, driving concurrent
/// growth/rehash. A non-atomic probe/write or a racy rehash would drop entries.
#[test]
fn stdlib_map_concurrent_insert() {
    const THREADS: u32 = 8;
    const ITERS: u32 = 8;

    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let map = BStackHashMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    let total = (THREADS * ITERS) as u64;

    std::thread::scope(|s| {
        for t in 0..THREADS {
            let map = &map;
            let alloc = &alloc;
            s.spawn(move || {
                for i in 0..ITERS {
                    let k = t * ITERS + i;
                    map.insert(alloc, k, MacroLeaf::new(alloc, k).unwrap())
                        .unwrap();
                }
            });
        }
    });

    assert_eq!(map.len(alloc.stack()).unwrap(), total);
    for k in 0..(THREADS * ITERS) {
        assert_eq!(
            map.get(alloc.stack(), &k)
                .unwrap()
                .unwrap()
                .val(alloc.stack())
                .unwrap(),
            k
        );
    }

    map.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// stdlib: BStackString — standalone owned UTF-8 string
// --------------------------------------------------------------------------

#[test]
fn stdlib_string_roundtrip_set_push() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let s = BStackString::new(&alloc, "hello").unwrap();
    assert_eq!(s.handle().len(stack).unwrap(), 5);
    assert_eq!(s.handle().to_string(stack).unwrap(), "hello");

    // Replace with something longer, then shorter.
    s.handle().set(&alloc, "hello, world").unwrap();
    assert_eq!(s.handle().to_string(stack).unwrap(), "hello, world");
    s.handle().set(&alloc, "hi").unwrap();
    assert_eq!(s.handle().to_string(stack).unwrap(), "hi");

    // Append.
    s.handle().push_str(&alloc, " there").unwrap();
    assert_eq!(s.handle().to_string(stack).unwrap(), "hi there");

    // Empty string has no bytes block.
    let e = BStackString::new(&alloc, "").unwrap();
    assert!(e.handle().is_empty(stack).unwrap());
    assert_eq!(e.handle().to_string(stack).unwrap(), "");

    s.bstack_drop(&alloc).unwrap();
    e.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_string_unicode() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let text = "héllo — 世界 🦀";
    let s = BStackString::new(&alloc, text).unwrap();
    assert_eq!(s.handle().len(stack).unwrap(), text.len() as u64); // byte length
    assert_eq!(s.handle().to_string(stack).unwrap(), text);
    s.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_string_deep_clone_is_independent() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let s = BStackString::new(&alloc, "original").unwrap();
    let clone = s.try_clone_in(&alloc).unwrap();
    assert_eq!(clone.handle().to_string(stack).unwrap(), "original");

    // Mutating the clone leaves the original intact.
    clone.handle().set(&alloc, "changed").unwrap();
    assert_eq!(clone.handle().to_string(stack).unwrap(), "changed");
    assert_eq!(s.handle().to_string(stack).unwrap(), "original");

    clone.bstack_drop(&alloc).unwrap();
    s.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_string_as_map_value() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // The headline use: strings as owned map values.
    let map = BStackHashMap::<u32, BStackString>::new(&alloc).unwrap();
    map.insert(&alloc, 1, BStackString::new(&alloc, "one").unwrap())
        .unwrap();
    map.insert(&alloc, 2, BStackString::new(&alloc, "two").unwrap())
        .unwrap();
    assert_eq!(
        map.get(stack, &1)
            .unwrap()
            .unwrap()
            .to_string(stack)
            .unwrap(),
        "one"
    );
    assert_eq!(
        map.get(stack, &2)
            .unwrap()
            .unwrap()
            .to_string(stack)
            .unwrap(),
        "two"
    );

    // Overwrite returns the old string (owned), which we free.
    let old = map
        .insert(&alloc, 1, BStackString::new(&alloc, "uno").unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(old.handle().to_string(stack).unwrap(), "one");
    old.bstack_drop(&alloc).unwrap();
    assert_eq!(
        map.get(stack, &1)
            .unwrap()
            .unwrap()
            .to_string(stack)
            .unwrap(),
        "uno"
    );

    // Dropping the map recursively frees every string value (and its bytes block).
    map.bstack_drop(&alloc).unwrap();
}
