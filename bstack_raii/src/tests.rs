//! Runtime tests against a real `BStack` + `FirstFitBStackAllocator`.
//!
//! These stand in for the (not-yet-written) `#[bstack_block]` macro by defining
//! a block type *by hand* — exactly the shape the macro will generate — and
//! exercising the refcount / two-phase-teardown machinery end to end.

use core::mem::size_of;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

use bstack::{
    BStack, BStackAllocator, BStackRange, FirstFitBStackAllocator, GhostTreeBstackAllocator,
};

use crate::layout::{self, BlockHeader};
use crate::{
    AutoDrop, BStackBTreeMap, BStackBTreeSet, BStackBinaryHeap, BStackBlock, BStackBlockVec,
    BStackBox, BStackCast, BStackCastAs, BStackCastInto, BStackCountingBloomFilter, BStackCow,
    BStackDeque, BStackDrop, BStackHashMap, BStackHashSet, BStackLinkedList, BStackOwned,
    BStackRaiiAllocator, BStackRc, BStackRef, BStackShared, BStackString, BStackWeakable, EightCC,
    TryClone, TryCloneIn, alloc_block, bstack_block, bstack_cast, bstack_enum, bstack_move,
    build_control_payload, dealloc_range,
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

    /// A `GhostTree` allocator — the one bstack-provided allocator that both
    /// anchors a WAL and implements `BStackBulkAllocator`, so it exercises the
    /// atomic-bulk override of `alloc_many` / `free_many` (FirstFit hits the
    /// sequential fallback).
    fn ghost_allocator(&self) -> GhostTreeBstackAllocator {
        GhostTreeBstackAllocator::new(self.open()).unwrap()
    }
}

impl Drop for TempStack {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Assert a WAL-backed teardown reclaims a whole structure with **no leak** —
/// including any deeply-nested grandchildren, so it doubles as a recursion check
/// (a non-recursive teardown would leak the grandchild's block).
///
/// `build` constructs the structure fresh each call. We build+tear down once to
/// warm and size the persistent WAL block (which stays allocated by design), then
/// measure the baseline, build+tear down the *identical* structure again, and
/// assert the stack returned exactly to that baseline. Comparing two like cycles
/// makes the constant WAL-block overhead cancel out, so only a real leak shows.
fn assert_teardown_reclaims<T: BStackDrop>(
    alloc: &FirstFitBStackAllocator,
    mut build: impl FnMut() -> T,
) {
    build().bstack_drop(alloc).unwrap();
    let base = alloc.stack().len().unwrap();
    build().bstack_drop(alloc).unwrap();
    assert_eq!(
        alloc.stack().len().unwrap(),
        base,
        "teardown leaked (non-recursive?)"
    );
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
    fn bstack_drop<A: BStackRaiiAllocator>(self, allocator: &A) -> io::Result<()> {
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

/// Allocate and wire the control block for an already-allocated `(rc, weak)`
/// data block, mirroring the atomic path the macro's `RcWeak` constructor
/// uses: the control payload write and the data block's `ctrl` back-pointer
/// write commit together in one [`bstack::BStack::set_batched`], so there is
/// no transient state where one is written and not the other.
fn alloc_control<A: BStackRaiiAllocator>(
    allocator: &A,
    ctrl_tag: EightCC,
    data: BStackRange,
    control_size: u64,
) -> io::Result<BStackRange> {
    let slice = allocator.alloc(control_size)?;
    let ctrl = slice.as_range();
    let payload = build_control_payload(ctrl_tag, data.start(), control_size);
    let backptr_off = data.start() + layout::CTRL_BACKPTR_OFFSET;
    let writes: [(u64, Vec<u8>); 2] = [
        (ctrl.start(), payload),
        (backptr_off, ctrl.start().to_le_bytes().to_vec()),
    ];
    if let Err(e) = allocator.stack().set_batched(writes) {
        let _ = allocator.dealloc(slice);
        return Err(e);
    }
    Ok(ctrl)
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

    // Freeing the owned parent must recursively free the wired child, then itself —
    // proven by the whole structure being reclaimed with no leak.
    assert_teardown_reclaims(&alloc, || {
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
        unsafe { BStackOwned::from_raw(<MacroParent as BStackBlock>::from_range(parent)) }
    });
}

// Same recursive teardown, but on a `GhostTree` allocator (`atomic_bulk() == true`):
// `wal_teardown` frees the whole same-file subtree with one atomic `dealloc_bulk`,
// skipping the WAL. Build + tear down twice and assert the stack returns to baseline
// — a leak (e.g. the child not freed by the bulk path) would show as growth.
#[test]
fn macro_recursive_drop_on_bulk_allocator() {
    let tmp = TempStack::new();
    let alloc = tmp.ghost_allocator();
    let build = || {
        let leaf = MacroLeaf::new(&alloc, 42).unwrap();
        MacroParent::new(&alloc, leaf, 7).unwrap()
    };
    build().bstack_drop(&alloc).unwrap();
    let base = alloc.stack().len().unwrap();
    build().bstack_drop(&alloc).unwrap();
    assert_eq!(
        alloc.stack().len().unwrap(),
        base,
        "bulk teardown leaked (child not freed by dealloc_bulk?)"
    );
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
    // The child's data slot is reclaimed. The parent teardown's persistent WAL
    // block perturbs the free-list order, so the slot may not be handed back first;
    // drain a few same-size allocations to confirm it reappears (all freed slots
    // here are small, so none starves the batch).
    let mut ranges = Vec::new();
    let mut hit = false;
    for _ in 0..8 {
        let r = alloc_block(&alloc, MacroStrongChild::eightcc(), child_data_size).unwrap();
        if r.start() == child.start() {
            hit = true;
        }
        ranges.push(r);
    }
    for r in ranges {
        unsafe { dealloc_range(&alloc, r).unwrap() };
    }
    assert!(hit, "child data slot was not reclaimed on strong -> 0");
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
    assert_eq!(leaf.handle().get_val(stack).unwrap(), 42);

    // Owned child is consumed by the parent constructor (ownership transferred).
    let parent = MacroParent::new(&alloc, leaf, 7).unwrap();
    assert_eq!(parent.handle().get_tag(stack).unwrap(), 7);

    // Accessor resolves the owned-ref field to the child handle; reading its own
    // field proves the child pointer was wired correctly.
    let child = parent.handle().get_child(stack).unwrap();
    assert_eq!(child.get_val(stack).unwrap(), 42);

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
    let orig_child = parent.handle().get_child(stack).unwrap();

    // Deep clone -> a fresh, independent BStackOwned copy.
    let clone = parent.try_clone_in(&alloc).unwrap();

    // Same values read back through the clone.
    assert_eq!(clone.handle().get_tag(stack).unwrap(), 7);
    assert_eq!(
        clone
            .handle()
            .get_child(stack)
            .unwrap()
            .get_val(stack)
            .unwrap(),
        42
    );

    // Independent storage: both the clone's block and its owned child are new
    // allocations, distinct from the originals (proves the recursion + repoint).
    assert_ne!(
        clone.handle().range().start(),
        parent.handle().range().start()
    );
    assert_ne!(
        clone.handle().get_child(stack).unwrap().range().start(),
        orig_child.range().start()
    );

    // Freeing the clone frees only the clone's subtree; the original stays intact.
    clone.bstack_drop(&alloc).unwrap();
    assert_eq!(
        parent
            .handle()
            .get_child(stack)
            .unwrap()
            .get_val(stack)
            .unwrap(),
        42
    );
    parent.bstack_drop(&alloc).unwrap();
}

#[test]
fn macro_deep_clone_on_bulk_allocator() {
    // Exercises the two-pass clone path (measure sizes -> one atomic `alloc_bulk` ->
    // build against real addresses): a bulk allocator (GhostTree) takes `run_clone`'s
    // bulk branch. A parent with an owned child means two home blocks are measured,
    // allocated together, then built — the child's real address must land in the
    // parent payload during the build pass exactly as the single-pass path does.
    let tmp = TempStack::new();
    let alloc = tmp.ghost_allocator();
    let stack = alloc.stack();

    let leaf = MacroLeaf::new(&alloc, 42).unwrap();
    let parent = MacroParent::new(&alloc, leaf, 7).unwrap();
    let orig_child = parent.handle().get_child(stack).unwrap();

    let clone = parent.try_clone_in(&alloc).unwrap();

    // Deep copy read back through the clone.
    assert_eq!(clone.handle().get_tag(stack).unwrap(), 7);
    assert_eq!(
        clone
            .handle()
            .get_child(stack)
            .unwrap()
            .get_val(stack)
            .unwrap(),
        42
    );
    // Independent storage (fresh blocks, distinct from the originals) — proves the
    // build pass repointed the parent at the newly bulk-allocated child.
    assert_ne!(
        clone.handle().range().start(),
        parent.handle().range().start()
    );
    assert_ne!(
        clone.handle().get_child(stack).unwrap().range().start(),
        orig_child.range().start()
    );

    clone.bstack_drop(&alloc).unwrap();
    // Original intact after the clone's subtree is freed.
    assert_eq!(
        parent
            .handle()
            .get_child(stack)
            .unwrap()
            .get_val(stack)
            .unwrap(),
        42
    );
    parent.bstack_drop(&alloc).unwrap();
}

#[test]
fn macro_deep_clone_on_bulk_allocator_no_leak() {
    // The two-pass bulk clone must allocate each block *exactly once* — the measure
    // pass counts, the build pass consumes the pre-allocated pool. A divergence (or a
    // block allocated but not handed out) would over-allocate and leak. Warm the
    // allocator + WAL block once, then assert a clone+drop cycle returns to a steady
    // length.
    let tmp = TempStack::new();
    let alloc = tmp.ghost_allocator();
    let stack = alloc.stack();

    let build = || {
        let leaf = MacroLeaf::new(&alloc, 1).unwrap();
        MacroParent::new(&alloc, leaf, 2).unwrap()
    };

    // Warm: the first clone lazily allocates the persistent WAL block (kept for reuse).
    let p0 = build();
    p0.try_clone_in(&alloc)
        .unwrap()
        .bstack_drop(&alloc)
        .unwrap();
    p0.bstack_drop(&alloc).unwrap();

    let base = stack.len().unwrap();
    let p = build();
    let c = p.try_clone_in(&alloc).unwrap();
    c.bstack_drop(&alloc).unwrap();
    p.bstack_drop(&alloc).unwrap();
    assert_eq!(
        stack.len().unwrap(),
        base,
        "two-pass bulk clone leaked or double-allocated"
    );
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
    assert_eq!(rc_keep.handle().get_val(stack).unwrap(), 5);
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
    assert_eq!(
        rc.handle()
            .get_child(stack)
            .unwrap()
            .get_val(stack)
            .unwrap(),
        99
    );

    // Full shared lifecycle on a constructor-built block.
    let rc2 = rc.try_clone().unwrap();
    let weak = rc.downgrade().unwrap();
    drop(rc2);
    drop(rc);
    assert!(weak.upgrade().unwrap().is_none());
    drop(weak);
}

// The same (rc, weak) constructor/clone/teardown lifecycle, but on a `GhostTree`
// allocator — which implements `BStackBulkAllocator`, so the two-block constructor
// routes through the atomic `alloc_bulk` override of `alloc_many` (and the rollback
// path through `dealloc_bulk`). This is the only test that exercises the bulk
// branch at runtime; every other test uses FirstFit's sequential fallback.
#[test]
fn macro_new_rc_weak_on_bulk_allocator() {
    let tmp = TempStack::new();
    let alloc = tmp.ghost_allocator();
    let stack = alloc.stack();

    // Two-block (data + control) constructor via the bulk `alloc_many` override.
    let leaf = MacroLeaf::new(&alloc, 7).unwrap();
    let rc = MacroShared::new(&alloc, leaf).unwrap();
    assert_eq!(
        rc.handle()
            .get_child(stack)
            .unwrap()
            .get_val(stack)
            .unwrap(),
        7
    );

    // Full shared lifecycle, so the strong/weak release + block frees run too.
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
    let up = b.handle().get_back(&alloc).unwrap().expect("a is alive");
    assert_eq!(up.handle().get_val(alloc.stack()).unwrap(), 1);
    drop(up);

    // Drop the strong owner `a` first: its DATA block is freed, but its control
    // block survives because b.back still holds a weak count.
    drop(a);

    // The weak field can no longer upgrade — and reaching this did NOT read a's
    // freed data block, because the field stores a's control offset.
    assert!(b.handle().get_back(&alloc).unwrap().is_none());

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
    assert_eq!(child.handle().get_val(stack).unwrap(), 55);

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
    assert_eq!(moved_s.handle().get_val(stack).unwrap(), 88);

    // The weak field came back as Some(weak) and still upgrades (target alive).
    let up = moved_w
        .as_ref()
        .unwrap()
        .upgrade()
        .unwrap()
        .expect("wt alive");
    assert_eq!(up.handle().get_val(stack).unwrap(), 3);
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
            .get_val(stack)
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
    assert_eq!(owned.handle().get_val(stack).unwrap(), 9);
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
    assert_eq!(moved_leaf.handle().get_val(stack).unwrap(), 5);

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
    assert_eq!(moved_leaf.handle().get_val(stack).unwrap(), 9);

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
    assert_eq!(holder.handle().get_n(stack).unwrap(), 7);
    let got = holder.handle().get_child(stack).unwrap();
    assert_eq!(got.unwrap().get_val(stack).unwrap(), 42);

    // bstack_move! yields Option<BStackOwned<_>>.
    let (moved_child, n) = bstack_move!(holder, &alloc).unwrap();
    assert_eq!(n, 7);
    assert_eq!(
        moved_child
            .as_ref()
            .unwrap()
            .handle()
            .get_val(stack)
            .unwrap(),
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
    assert_eq!(empty.handle().get_n(stack).unwrap(), 9);
    assert!(empty.handle().get_child(stack).unwrap().is_none());
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
    assert_eq!(rec.handle().get_id(stack).unwrap(), 42);

    // Accessors return BStackVec handles (take the allocator).
    assert_eq!(
        rec.handle().get_name(&alloc).unwrap().to_vec().unwrap(),
        b"hello"
    );
    assert_eq!(
        rec.handle().get_tags(&alloc).unwrap().to_vec().unwrap(),
        vec![1u32, 2, 3]
    );

    // Mutate through the handle: the field points at the stable descriptor, so
    // growth (even if the data block moves) is visible on the next read.
    let mut tags = rec.handle().get_tags(&alloc).unwrap();
    tags.push(4).unwrap();
    assert_eq!(
        rec.handle().get_tags(&alloc).unwrap().to_vec().unwrap(),
        vec![1u32, 2, 3, 4],
    );

    // Freeing the record frees both vectors (data + descriptor) and the record.
    rec.bstack_drop(&alloc).unwrap();

    // Allocator is healthy: a fresh record round-trips.
    let rec2 = Record::new(&alloc, "again", &[9u32], 1).unwrap();
    assert_eq!(
        rec2.handle().get_name(&alloc).unwrap().to_vec().unwrap(),
        b"again"
    );
    assert_eq!(
        rec2.handle().get_tags(&alloc).unwrap().to_vec().unwrap(),
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
    let old = rec.handle().get_tags(&alloc).unwrap().descriptor(); // cap == len == 12 B

    // len 12 + elem 4 > cap 12 → field-resident growth → the reorder path.
    let mut tags = rec.handle().get_tags(&alloc).unwrap();
    tags.push(4).unwrap();

    let new = rec.handle().get_tags(&alloc).unwrap().descriptor();
    assert_ne!(new.data_off, old.data_off); // moved to a fresh block
    assert_eq!(
        rec.handle().get_tags(&alloc).unwrap().to_vec().unwrap(),
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
    let tree = Tree::new(&alloc, kids, 7).unwrap();
    assert_eq!(tree.handle().get_label(stack).unwrap(), 7);

    // Accessor resolves to a BStackBlockVec; read the children back.
    let v = tree.handle().get_kids(&alloc).unwrap();
    assert_eq!(v.len().unwrap(), 3);
    let vals: Vec<u32> = v
        .to_vec()
        .unwrap()
        .iter()
        .map(|k| k.get_val(stack).unwrap())
        .collect();
    assert_eq!(vals, vec![10, 20, 30]);
    assert_eq!(v.get(1).unwrap().unwrap().get_val(stack).unwrap(), 20);
    assert!(v.get(3).unwrap().is_none());

    // Freeing the tree recursively frees every owned child, plus the offset array
    // and descriptor — reclaimed with no leak.
    tree.bstack_drop(&alloc).unwrap();
    assert_teardown_reclaims(&alloc, || {
        let kids = vec![
            MacroLeaf::new(&alloc, 10).unwrap(),
            MacroLeaf::new(&alloc, 20).unwrap(),
            MacroLeaf::new(&alloc, 30).unwrap(),
        ];
        Tree::new(&alloc, kids, 7).unwrap()
    });
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
    assert_eq!(kids_vec.get(0).unwrap().unwrap().get_val(stack).unwrap(), 1);

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
    let orig_name_off = rec.handle().get_name(&alloc).unwrap().descriptor().data_off;

    let clone = rec.try_clone_in(&alloc).unwrap();
    assert_eq!(clone.handle().get_id(stack).unwrap(), 42);
    assert_eq!(
        clone.handle().get_name(&alloc).unwrap().to_vec().unwrap(),
        b"hello"
    );
    assert_eq!(
        clone.handle().get_tags(&alloc).unwrap().to_vec().unwrap(),
        vec![1u32, 2, 3]
    );

    // The clone's data blocks are fresh allocations, distinct from the original's.
    let clone_name_off = clone
        .handle()
        .get_name(&alloc)
        .unwrap()
        .descriptor()
        .data_off;
    assert_ne!(clone_name_off, orig_name_off);

    // Growing the clone's vector leaves the original untouched (independent data).
    let mut ct = clone.handle().get_tags(&alloc).unwrap();
    ct.push(99).unwrap();
    assert_eq!(
        clone.handle().get_tags(&alloc).unwrap().to_vec().unwrap(),
        vec![1u32, 2, 3, 99]
    );
    assert_eq!(
        rec.handle().get_tags(&alloc).unwrap().to_vec().unwrap(),
        vec![1u32, 2, 3]
    );

    clone.bstack_drop(&alloc).unwrap();
    rec.bstack_drop(&alloc).unwrap();
}

#[test]
fn macro_clone_pod_vec_on_bulk_allocator() {
    // Two-pass bulk clone through vec data blocks: `stage_bytevec` routes each string
    // / POD-vec block through `alloc_raw`, so it is measured (size only, image skipped)
    // then built (real address, image written). A `Record` has both a string and a
    // POD `u32` vec, plus its own block — three home blocks bulk-allocated as one.
    let tmp = TempStack::new();
    let alloc = tmp.ghost_allocator();
    let stack = alloc.stack();

    let rec = Record::new(&alloc, "hello", &[1u32, 2, 3], 42).unwrap();
    let orig_name_off = rec.handle().get_name(&alloc).unwrap().descriptor().data_off;

    let clone = rec.try_clone_in(&alloc).unwrap();
    assert_eq!(clone.handle().get_id(stack).unwrap(), 42);
    assert_eq!(
        clone.handle().get_name(&alloc).unwrap().to_vec().unwrap(),
        b"hello"
    );
    assert_eq!(
        clone.handle().get_tags(&alloc).unwrap().to_vec().unwrap(),
        vec![1u32, 2, 3]
    );
    // Fresh, independent data block (built against a real bulk-allocated address).
    let clone_name_off = clone
        .handle()
        .get_name(&alloc)
        .unwrap()
        .descriptor()
        .data_off;
    assert_ne!(clone_name_off, orig_name_off);

    clone.bstack_drop(&alloc).unwrap();
    rec.bstack_drop(&alloc).unwrap();

    // No leak / double-alloc across a warmed clone+drop cycle.
    let base = stack.len().unwrap();
    let r = Record::new(&alloc, "world", &[7u32, 8], 1).unwrap();
    let c = r.try_clone_in(&alloc).unwrap();
    c.bstack_drop(&alloc).unwrap();
    r.bstack_drop(&alloc).unwrap();
    assert_eq!(
        stack.len().unwrap(),
        base,
        "two-pass bulk vec clone leaked or double-allocated"
    );
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
        .get_kids(&alloc)
        .unwrap()
        .get(0)
        .unwrap()
        .unwrap()
        .range()
        .start();

    let clone = tree.try_clone_in(&alloc).unwrap();
    let cv = clone.handle().get_kids(&alloc).unwrap();
    assert_eq!(cv.len().unwrap(), 2);
    let vals: Vec<u32> = cv
        .to_vec()
        .unwrap()
        .iter()
        .map(|k| k.get_val(stack).unwrap())
        .collect();
    assert_eq!(vals, vec![10, 20]);

    // Each child is a fresh, independent block (deep clone, not aliased).
    let clone_first = cv.get(0).unwrap().unwrap().range().start();
    assert_ne!(clone_first, orig_first);

    // Freeing the clone frees only the clone's children; the original survives.
    clone.bstack_drop(&alloc).unwrap();
    assert_eq!(
        tree.handle()
            .get_kids(&alloc)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .get_val(stack)
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
    assert_eq!(a_keep.handle().get_val(stack).unwrap(), 100);
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

    let v = list.handle().get_items(&alloc).unwrap();
    assert_eq!(v.len().unwrap(), 2);
    assert_eq!(v.get(0).unwrap().unwrap().get_val(stack).unwrap(), 100);

    // Freeing the list releases every element's strong ref: `b` (sole owner) is
    // freed; `a` survives via `a_clone`.
    list.bstack_drop(&alloc).unwrap();
    assert_eq!(strong_of(stack, a_data), 1); // a_clone only

    assert_eq!(a_clone.handle().get_val(stack).unwrap(), 100);
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

    let v = list.handle().get_watchers(&alloc).unwrap();
    assert_eq!(v.len().unwrap(), 2);

    // Upgrade element 0 while `a` is alive.
    let up = v.upgrade(0).unwrap().expect("a alive");
    assert_eq!(up.handle().get_val(stack).unwrap(), 1);
    drop(up);

    // Drop `a`'s data block: element 0 can no longer upgrade (sound — the vector
    // stores control offsets, not freed data offsets).
    drop(a);
    let v = list.handle().get_watchers(&alloc).unwrap();
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

    let v = list.handle().get_links(&alloc).unwrap();
    assert_eq!(v.len().unwrap(), 2);
    assert_eq!(v.get(1).unwrap().unwrap().get_val(stack).unwrap(), 8);

    // Freeing the list frees only the offset array + descriptor, not the targets.
    list.bstack_drop(&alloc).unwrap();
    assert_eq!(a.handle().get_val(stack).unwrap(), 7); // still alive
    assert_eq!(b.handle().get_val(stack).unwrap(), 8);

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
    assert_eq!(a.handle().get_id(stack).unwrap(), 7);
    assert_eq!(
        a.handle()
            .get_tags(&alloc)
            .unwrap()
            .expect("some")
            .to_vec()
            .unwrap(),
        vec![1u32, 2, 3]
    );
    assert_eq!(
        a.handle()
            .get_name(&alloc)
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
    assert_eq!(b.handle().get_id(stack).unwrap(), 9);
    assert!(b.handle().get_tags(&alloc).unwrap().is_none());
    assert!(b.handle().get_name(&alloc).unwrap().is_none());
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
        NodeView::Child(c) => assert_eq!(c.get_val(stack).unwrap(), 7),
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
        NodeView::Link(l) => assert_eq!(l.get_val(stack).unwrap(), 9),
        _ => panic!("expected Link"),
    }
    e.bstack_drop(&alloc).unwrap();
    assert_eq!(keep.handle().get_val(stack).unwrap(), 9); // still alive
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

    let leaf = MacroLeaf::new(&alloc, 5).unwrap();
    let node = Node::new(&alloc, NodeData::Child(leaf)).unwrap();
    let holder = EnumHolder::new(&alloc, node, 3).unwrap();
    assert_eq!(holder.handle().get_tag(stack).unwrap(), 3);

    // Traverse struct -> enum -> owned child.
    let node = holder.handle().get_node(stack).unwrap();
    match node.read(&alloc).unwrap() {
        NodeView::Child(c) => assert_eq!(c.get_val(stack).unwrap(), 5),
        _ => panic!("expected Child"),
    }

    // Freeing the struct recursively frees the enum and its owned child —
    // reclaimed with no leak.
    holder.bstack_drop(&alloc).unwrap();
    assert_teardown_reclaims(&alloc, || {
        let leaf = MacroLeaf::new(&alloc, 5).unwrap();
        let node = Node::new(&alloc, NodeData::Child(leaf)).unwrap();
        EnumHolder::new(&alloc, node, 3).unwrap()
    });
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
        RcNodeView::Child(c) => assert_eq!(c.get_val(stack).unwrap(), 4),
        _ => panic!("expected Child"),
    }

    drop(rc); // strong = 1 — still alive
    drop(rc2); // strong = 0 — frees the enum block AND its owned child

    let reused = alloc_block(&alloc, MacroLeaf::eightcc(), leaf_size).unwrap();
    assert_eq!(reused.start(), leaf_off); // child reclaimed => teardown recursed
    unsafe { dealloc_range(&alloc, reused).unwrap() };
}

#[test]
fn macro_enum_rc_val_variant() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    let rc = RcNode::new(&alloc, RcNodeData::Val(7)).unwrap();
    match rc.handle().read(&alloc).unwrap() {
        RcNodeView::Val(v) => assert_eq!(v, 7),
        _ => panic!("expected Val"),
    }
    drop(rc);
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
        RcwNodeView::One(c) => assert_eq!(c.get_val(stack).unwrap(), 8),
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
        CellView::Shared(c) => assert_eq!(c.get_val(stack).unwrap(), 11),
        _ => panic!("expected Shared"),
    }
    cell.bstack_drop(&alloc).unwrap(); // releases the enum's strong ref (strong = 1)
    assert_eq!(keep.handle().get_val(stack).unwrap(), 11); // still alive
    drop(keep); // strong = 0 — freed

    // Weak variant: consumes a BStackWeak; reading upgrades it.
    let owner = MacroStrongChild::new(&alloc, 22).unwrap(); // strong owner
    let cell = Cell::new(&alloc, CellData::Watch(owner.downgrade().unwrap())).unwrap();
    match cell.handle().read(&alloc).unwrap() {
        CellView::Watch(Some(up)) => assert_eq!(up.handle().get_val(stack).unwrap(), 22),
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
            assert_eq!(ch.get_val(stack).unwrap(), 7);
            ch.range().start()
        }
        _ => panic!("expected Child"),
    };
    assert_ne!(clone_child_off, orig_child_off); // deep clone, not aliased
    c.bstack_drop(&alloc).unwrap();
    match e.handle().read(&alloc).unwrap() {
        NodeView::Child(ch) => assert_eq!(ch.get_val(stack).unwrap(), 7), // original intact
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
            assert_eq!(l.get_val(stack).unwrap(), 9);
            assert_eq!(l.range().start(), keep.handle().range().start()); // aliased
        }
        _ => panic!("expected Link"),
    }
    c.bstack_drop(&alloc).unwrap();
    e.bstack_drop(&alloc).unwrap();
    assert_eq!(keep.handle().get_val(stack).unwrap(), 9); // target untouched
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
    assert_eq!(keep.handle().get_val(stack).unwrap(), 11);
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
            assert_eq!(owned_leaf.handle().get_val(stack).unwrap(), 5); // survived the move
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
    assert_eq!(keep.handle().get_val(stack).unwrap(), 3); // untouched
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
            assert_eq!(rc.handle().get_val(stack).unwrap(), 11);
            drop(rc); // releases the moved-out strong ref
        }
        _ => panic!("expected Shared"),
    }
    assert_eq!(keep.handle().get_val(stack).unwrap(), 11); // still alive
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
                    .get_val(stack)
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
        AlignedView::Y(c) => assert_eq!(c.get_val(stack).unwrap(), 3),
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
        f.handle().get_maybe(stack).unwrap(),
        core::num::NonZeroU32::new(7)
    );
    assert_eq!(
        f.handle().get_wrap(stack).unwrap(),
        core::num::Wrapping(42u32)
    );
    assert_eq!(f.handle().get_pair(stack).unwrap(), (1u8, 2u8));
    assert_eq!(f.handle().get_mixed(stack).unwrap(), (300u16, -5i32));
    assert_eq!(f.handle().get_n(stack).unwrap(), 99);

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
    assert!(g.handle().get_maybe(stack).unwrap().is_none());
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
    assert_eq!(c.handle().get_field0(stack).unwrap(), 10);
    assert_eq!(c.handle().get_field1(stack).unwrap(), 20);
    assert_eq!(c.handle().get_field2(stack).unwrap(), 30);

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

    // Struct embed: parent -> embedded child -> the child's own owned leaf.
    let leaf = MacroLeaf::new(&alloc, 42).unwrap();
    let child = EmbChild::new(&alloc, leaf, 7).unwrap();
    let holder = EmbHolder::new(&alloc, child, 99).unwrap();
    assert_eq!(holder.handle().get_tag(stack).unwrap(), 99);
    let c = holder.handle().get_child(); // a handle into the inline region (no I/O)
    assert_eq!(c.get_n(stack).unwrap(), 7);
    assert_eq!(c.get_leaf(stack).unwrap().get_val(stack).unwrap(), 42);

    // Teardown frees the embedded child's owned leaf *in place*, then the holder —
    // reclaimed with no leak (proof the embed recursed).
    holder.bstack_drop(&alloc).unwrap();
    assert_teardown_reclaims(&alloc, || {
        let leaf = MacroLeaf::new(&alloc, 42).unwrap();
        let child = EmbChild::new(&alloc, leaf, 7).unwrap();
        EmbHolder::new(&alloc, child, 99).unwrap()
    });

    // bstack_move! re-homes the embedded child to a fresh standalone allocation.
    let leaf = MacroLeaf::new(&alloc, 5).unwrap();
    let child = EmbChild::new(&alloc, leaf, 8).unwrap();
    let holder = EmbHolder::new(&alloc, child, 1).unwrap();
    let (moved, tag) = bstack_move!(holder, &alloc).unwrap();
    assert_eq!(tag, 1);
    assert_eq!(
        moved
            .handle()
            .get_leaf(stack)
            .unwrap()
            .get_val(stack)
            .unwrap(),
        5
    );
    moved.bstack_drop(&alloc).unwrap();

    // Enum embed: construct, read (a borrowed child handle), move out.
    let leaf = MacroLeaf::new(&alloc, 3).unwrap();
    let child = EmbChild::new(&alloc, leaf, 9).unwrap();
    let e = EmbEnum::new(&alloc, EmbEnumData::Wrap(child)).unwrap();
    match e.handle().read(&alloc).unwrap() {
        EmbEnumView::Wrap(c) => assert_eq!(c.get_leaf(stack).unwrap().get_val(stack).unwrap(), 3),
        _ => panic!("expected Wrap"),
    }
    let moved = match bstack_move!(e, &alloc).unwrap() {
        EmbEnumData::Wrap(c) => c,
        _ => panic!("expected Wrap"),
    };
    assert_eq!(moved.handle().get_n(stack).unwrap(), 9);
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
    let orig_leaf_off = holder
        .handle()
        .get_child()
        .get_leaf(stack)
        .unwrap()
        .range()
        .start();

    let clone = holder.try_clone_in(&alloc).unwrap();
    assert_eq!(clone.handle().get_tag(stack).unwrap(), 99);
    let cc = clone.handle().get_child();
    assert_eq!(cc.get_n(stack).unwrap(), 7);
    assert_eq!(cc.get_leaf(stack).unwrap().get_val(stack).unwrap(), 42);

    // The embedded child's OWN owned leaf was deep-cloned into a fresh block
    // (the inline region was folded, not just byte-copied with an aliased offset).
    let clone_leaf_off = cc.get_leaf(stack).unwrap().range().start();
    assert_ne!(clone_leaf_off, orig_leaf_off);

    // Freeing the clone frees only the clone's leaf; the original stays intact.
    clone.bstack_drop(&alloc).unwrap();
    assert_eq!(
        holder
            .handle()
            .get_child()
            .get_leaf(stack)
            .unwrap()
            .get_val(stack)
            .unwrap(),
        42
    );
    holder.bstack_drop(&alloc).unwrap();

    // Enum embed variant: same in-place fold through the payload.
    let leaf = MacroLeaf::new(&alloc, 3).unwrap();
    let child = EmbChild::new(&alloc, leaf, 9).unwrap();
    let e = EmbEnum::new(&alloc, EmbEnumData::Wrap(child)).unwrap();
    let orig_off = match e.handle().read(&alloc).unwrap() {
        EmbEnumView::Wrap(c) => c.get_leaf(stack).unwrap().range().start(),
        _ => panic!("expected Wrap"),
    };
    let ce = e.try_clone_in(&alloc).unwrap();
    let clone_off = match ce.handle().read(&alloc).unwrap() {
        EmbEnumView::Wrap(c) => {
            assert_eq!(c.get_leaf(stack).unwrap().get_val(stack).unwrap(), 3);
            c.get_leaf(stack).unwrap().range().start()
        }
        _ => panic!("expected Wrap"),
    };
    assert_ne!(clone_off, orig_off); // deep-cloned, not aliased
    ce.bstack_drop(&alloc).unwrap();
    match e.handle().read(&alloc).unwrap() {
        EmbEnumView::Wrap(c) => assert_eq!(c.get_leaf(stack).unwrap().get_val(stack).unwrap(), 3),
        _ => panic!("expected Wrap"),
    }
    e.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// WAL: completing / abandoning a crash-left transaction
// --------------------------------------------------------------------------

#[test]
fn wal_finish_rolls_forward_committed() {
    use crate::wal::{WalEntry, WalLog, WalStatus};
    use crate::wal::{finish, persist_at};

    let tmp = TempStack::new();
    let alloc = tmp.allocator(); // FirstFit: wal_anchor() == Some(8), zeroed on a fresh file
    // Two "old" slices the transaction was freeing.
    let v1 = alloc.alloc(64).unwrap().as_range();
    let v2 = alloc.alloc(64).unwrap().as_range();

    // A COMMITTED transaction that had not finished its deallocs.
    let mut log = WalLog::with_capacity(2);
    log.append(WalEntry::dealloc(WalStatus::Pending, v1));
    log.append(WalEntry::dealloc(WalStatus::Pending, v2));
    persist_at(&alloc, &log, WalStatus::Complete).unwrap();

    // Completing it rolls both deallocs forward.
    assert_eq!(finish(&alloc).unwrap(), 2);

    // The persistent WAL block is now idle: re-completing finds nothing staged.
    // v1/v2 were reclaimed (a fresh 64-byte alloc reuses a freed slot).
    assert_eq!(finish(&alloc).unwrap(), 0);
    let reused = alloc.alloc(64).unwrap().as_range();
    assert!(reused.start() == v1.start() || reused.start() == v2.start());
}

#[test]
fn wal_finish_abandons_uncommitted() {
    use crate::wal::{WalEntry, WalLog, WalStatus};
    use crate::wal::{finish, persist_at};

    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let v1 = alloc.alloc(64).unwrap().as_range();

    // An UNCOMMITTED transaction: its dealloc must NOT be performed.
    let mut log = WalLog::with_capacity(1);
    log.append(WalEntry::dealloc(WalStatus::Pending, v1));
    persist_at(&alloc, &log, WalStatus::Pending).unwrap();

    // Abandoned: the old slice v1 must NOT be freed (it's still live). Reclaiming
    // an abandoned txn frees its *allocs*, and this txn logged only a dealloc.
    assert_eq!(finish(&alloc).unwrap(), 0);
    // Idle after completion: re-running finds nothing staged.
    assert_eq!(finish(&alloc).unwrap(), 0);
}

#[test]
fn wal_anchor_trait_reclaims_via_finish() {
    use crate::wal::{WalEntry, WalLog, WalStatus};
    use crate::wal::{finish, persist_at};

    let tmp = TempStack::new();
    let alloc = tmp.allocator(); // FirstFitBStackAllocator: wal_anchor() == Some(8)
    let orphan = alloc.alloc(64).unwrap().as_range();

    // Persist an abandoned (Pending) txn into the allocator's own anchor slot.
    let mut log = WalLog::with_capacity(1);
    log.append(WalEntry::alloc(WalStatus::Pending, orphan));
    persist_at(&alloc, &log, WalStatus::Pending).unwrap();

    // finish() reclaims the orphan via the allocator's own anchor; the allocator is
    // unharmed by our writes to its reserved slot (a fresh alloc reuses it).
    assert_eq!(finish(&alloc).unwrap(), 1);
    assert_eq!(alloc.alloc(64).unwrap().as_range().start(), orphan.start());
}

#[test]
fn wal_finish_reclaims_abandoned_allocs() {
    use crate::wal::{WalEntry, WalLog, WalStatus};
    use crate::wal::{finish, persist_at};

    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    // Two blocks a crashed op allocated but never linked (orphans).
    let a1 = alloc.alloc(64).unwrap().as_range();
    let a2 = alloc.alloc(64).unwrap().as_range();

    // An UNCOMMITTED (Pending) transaction that had allocated a1/a2.
    let mut log = WalLog::with_capacity(2);
    log.append(WalEntry::alloc(WalStatus::Pending, a1));
    log.append(WalEntry::alloc(WalStatus::Pending, a2));
    persist_at(&alloc, &log, WalStatus::Pending).unwrap();

    // Reclaiming the abandoned txn frees both orphans.
    assert_eq!(finish(&alloc).unwrap(), 2);
    // Reclaimed: a fresh 64-byte alloc reuses one of the freed slots.
    let reused = alloc.alloc(64).unwrap().as_range();
    assert!(reused.start() == a1.start() || reused.start() == a2.start());

    // A *committed* alloc-only txn keeps its allocs (frees nothing); the persistent
    // WAL block is reused for it.
    let keep = alloc.alloc(64).unwrap().as_range();
    let mut log2 = WalLog::with_capacity(1);
    log2.append(WalEntry::alloc(WalStatus::Pending, keep));
    persist_at(&alloc, &log2, WalStatus::Complete).unwrap();
    assert_eq!(finish(&alloc).unwrap(), 0);
}

#[test]
fn wal_clone_descent_orphans_reclaimed_by_finish() {
    // Intention-first clone WAL: `ClonePlan::alloc_raw` logs every allocation to the
    // persistent WAL *during the descent*, before any commit. Model a hard crash
    // mid-descent by dropping the plan without `commit` or `rollback` — `ClonePlan`
    // has no freeing `Drop`, so the two blocks stay allocated and logged `Pending`,
    // exactly as a crashed process would leave them. `finish` on reopen must then
    // reclaim both — the window this closes (before, a mid-descent crash leaked the
    // whole partially-built subtree, since the WAL was only written at commit time).
    use crate::ClonePlan;
    use crate::wal::finish;

    let tmp = TempStack::new();
    let alloc = tmp.allocator(); // FirstFit names a WAL anchor
    let base = alloc.stack().len().unwrap();

    {
        let mut plan = ClonePlan::new();
        let _a = plan.alloc_raw(&alloc, 48).unwrap();
        let _b = plan.alloc_raw(&alloc, 64).unwrap();
        // Drop `plan` here without committing: a crash mid-descent. The held WAL lock
        // releases as the plan drops; the two orphans remain logged `Pending`.
    }
    assert!(
        alloc.stack().len().unwrap() > base,
        "descent allocated its blocks (+ the WAL block)"
    );

    // Recovery abandons the still-`Pending` transaction, freeing exactly the two
    // descent-logged orphans (the persistent WAL block itself stays, idle).
    assert_eq!(
        finish(&alloc).unwrap(),
        2,
        "both mid-descent orphans reclaimed"
    );
    // Idempotent: nothing left to reclaim.
    assert_eq!(finish(&alloc).unwrap(), 0);
}

#[test]
fn wal_finish_reclaims_foreign_orphan_via_registry() {
    // Option-1 cross-file reclamation: the WAL lives on the op's HOME file, but a
    // recorded slice can name a FOREIGN file (`file_id != 0`). Recovery resolves that
    // id through the process-wide registry and frees the orphan on the other side.
    // This is the only test that uses the global registry — `finish`'s recovery path
    // (`free_recorded`) resolves foreign frees through it, exactly as real teardown /
    // clone will.
    use crate::registry;
    use crate::wal::{WalEntry, WalLog, WalStatus};
    use crate::wal::{finish, persist_at};

    // The op's home file (where the WAL is staged) and a separate foreign file.
    let home = TempStack::new();
    let home_alloc = home.allocator();

    let foreign = TempStack::new();
    let foreign_alloc = foreign.allocator();
    // An orphan a crashed cross-file op left behind in the foreign file.
    let orphan = foreign_alloc.alloc(64).unwrap().as_range();

    // Bring up the global registry and attach the foreign file, learning its id.
    // Tolerant of a prior init (only this test touches the singleton).
    let reg_file = TempStack::new();
    let _ = registry::init(&reg_file.path);
    let fid = registry::attach(&foreign.path, foreign_alloc).unwrap();
    assert!(!fid.is_self());

    // A COMMITTED cross-file free staged in the HOME file's WAL: a `Dealloc` tagged
    // with the foreign file's id (the option-1 shape).
    let mut log = WalLog::with_capacity(1);
    log.append(WalEntry::dealloc_in(WalStatus::Pending, fid, orphan));
    persist_at(&home_alloc, &log, WalStatus::Complete).unwrap();

    // Completing the home WAL rolls the foreign free forward *in the foreign file*.
    assert_eq!(finish(&home_alloc).unwrap(), 1);

    // Reclaim confirmed on the foreign side: a fresh 64-byte alloc reuses the slot.
    let reused = registry::with_host(fid, |host| host.alloc(64).unwrap().start()).unwrap();
    assert_eq!(reused, orphan.start());

    // An unresolvable foreign entry (file detached) degrades to a leak, not an error.
    let orphan2 = registry::with_host(fid, |host| host.alloc(64).unwrap().start()).unwrap();
    registry::detach(fid);
    let mut log2 = WalLog::with_capacity(1);
    log2.append(WalEntry::dealloc_in(
        WalStatus::Pending,
        fid,
        BStackRange::new(orphan2, 64),
    ));
    persist_at(&home_alloc, &log2, WalStatus::Complete).unwrap();
    // The detached file can't be freed here — `finish` completes the entry (leaking
    // it) and returns success, counting it as handled rather than erroring.
    assert_eq!(finish(&home_alloc).unwrap(), 1);
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
    assert_eq!(p.handle().get_xs(stack).unwrap(), [1u16, 2, 3, 4]);
    assert_eq!(p.handle().get_tag(stack).unwrap(), 9);
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

    let l0 = MacroLeaf::new(&alloc, 10).unwrap();
    let l1 = MacroLeaf::new(&alloc, 20).unwrap();
    let l2 = MacroLeaf::new(&alloc, 30).unwrap();

    let h = ArrHolder::new(&alloc, [l0, l1, l2], 7).unwrap();
    assert_eq!(h.handle().get_tag(stack).unwrap(), 7);
    let arr = h.handle().get_leaves(stack).unwrap(); // [MacroLeaf; 3]
    assert_eq!(arr[0].get_val(stack).unwrap(), 10);
    assert_eq!(arr[1].get_val(stack).unwrap(), 20);
    assert_eq!(arr[2].get_val(stack).unwrap(), 30);

    // Teardown frees all three inline children — reclaimed with no leak.
    h.bstack_drop(&alloc).unwrap();
    assert_teardown_reclaims(&alloc, || {
        let l0 = MacroLeaf::new(&alloc, 10).unwrap();
        let l1 = MacroLeaf::new(&alloc, 20).unwrap();
        let l2 = MacroLeaf::new(&alloc, 30).unwrap();
        ArrHolder::new(&alloc, [l0, l1, l2], 7).unwrap()
    });
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
    let carr = clone.handle().get_leaves(stack).unwrap();
    let oarr = h.handle().get_leaves(stack).unwrap();
    assert_eq!(carr[1].get_val(stack).unwrap(), 2);
    // Deep-cloned: each clone element is a fresh block, distinct from the original.
    assert_ne!(carr[0].range().start(), oarr[0].range().start());

    clone.bstack_drop(&alloc).unwrap();
    assert_eq!(
        h.handle().get_leaves(stack).unwrap()[2]
            .get_val(stack)
            .unwrap(),
        3
    );
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
    let arr = h.handle().get_refs(stack).unwrap();
    assert_eq!(arr[0].get_val(stack).unwrap(), 1);
    assert_eq!(arr[1].get_val(stack).unwrap(), 2);

    // A ref array owns nothing: dropping the holder leaves the targets alive.
    h.bstack_drop(&alloc).unwrap();
    assert_eq!(l0.handle().get_val(stack).unwrap(), 1);
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
    let arr = h.handle().get_shared(stack).unwrap();
    assert_eq!(arr[0].get_val(stack).unwrap(), 5);

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
    assert_eq!(keep0.handle().get_val(stack).unwrap(), 5);
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
    assert_eq!(leaves[0].handle().get_val(stack).unwrap(), 10);
    assert_eq!(leaves[2].handle().get_val(stack).unwrap(), 30);
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
    let arr = h.handle().get_weaks(&alloc).unwrap();
    assert!(arr[0].is_none() && arr[1].is_none());

    // Wire each element via the per-index setter.
    h.handle()
        .set_weaks(&alloc, 0, c0.downgrade().unwrap())
        .unwrap();
    h.handle()
        .set_weaks(&alloc, 1, c1.downgrade().unwrap())
        .unwrap();

    // The accessor upgrades each live element.
    let arr = h.handle().get_weaks(&alloc).unwrap();
    assert_eq!(arr[0].as_ref().unwrap().handle().get_val(stack).unwrap(), 5);
    assert_eq!(arr[1].as_ref().unwrap().handle().get_val(stack).unwrap(), 6);
    drop(arr);

    // Cloning aliases the same control blocks (weak counts bumped).
    let clone = h.try_clone_in(&alloc).unwrap();
    let carr = clone.handle().get_weaks(&alloc).unwrap();
    assert_eq!(
        carr[0].as_ref().unwrap().handle().get_val(stack).unwrap(),
        5
    );
    drop(carr);

    // Both holders' teardown releases the weak refs (no underflow); c0/c1 live.
    clone.bstack_drop(&alloc).unwrap();
    h.bstack_drop(&alloc).unwrap();
    assert_eq!(c0.handle().get_val(stack).unwrap(), 5);
    assert_eq!(c1.handle().get_val(stack).unwrap(), 6);
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

    let arr = h.handle().get_leaves(stack).unwrap(); // [Option<MacroLeaf>; 3]
    assert_eq!(arr[0].as_ref().unwrap().get_val(stack).unwrap(), 10);
    assert!(arr[1].is_none());
    assert_eq!(arr[2].as_ref().unwrap().get_val(stack).unwrap(), 30);

    // Clone deep-copies the present elements, keeps the hole.
    let clone = h.try_clone_in(&alloc).unwrap();
    let carr = clone.handle().get_leaves(stack).unwrap();
    assert_eq!(carr[0].as_ref().unwrap().get_val(stack).unwrap(), 10);
    assert!(carr[1].is_none());
    assert_ne!(
        carr[0].as_ref().unwrap().range().start(),
        arr[0].as_ref().unwrap().range().start()
    );

    // Move yields `[Option<BStackOwned<MacroLeaf>>; 3]`.
    clone.bstack_drop(&alloc).unwrap();
    let (moved,) = bstack_move!(h, &alloc).unwrap();
    assert_eq!(
        moved[2].as_ref().unwrap().handle().get_val(stack).unwrap(),
        30
    );
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
    let arr = h.handle().get_refs(stack).unwrap(); // [Option<MacroLeaf>; 2]
    assert_eq!(arr[0].as_ref().unwrap().get_val(stack).unwrap(), 1);
    assert!(arr[1].is_none());

    h.bstack_drop(&alloc).unwrap(); // owns nothing
    assert_eq!(l0.handle().get_val(stack).unwrap(), 1);
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
    let arr = p.handle().get_xs(stack).unwrap(); // [Option<NonZeroU32>; 3]
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
    assert_eq!(h.handle().get_tag(stack).unwrap(), 99);

    // Accessor: `[EmbChild; 2]` handles into the inline slots (pure offset math).
    let kids = h.handle().get_kids();
    assert_eq!(kids[0].get_n(stack).unwrap(), 1);
    assert_eq!(kids[0].get_leaf(stack).unwrap().get_val(stack).unwrap(), 10);
    assert_eq!(kids[1].get_leaf(stack).unwrap().get_val(stack).unwrap(), 20);

    // Clone folds each embedded child inline, deep-cloning its owned leaf.
    let clone = h.try_clone_in(&alloc).unwrap();
    let ckids = clone.handle().get_kids();
    assert_eq!(
        ckids[1].get_leaf(stack).unwrap().get_val(stack).unwrap(),
        20
    );
    assert_ne!(
        ckids[0].get_leaf(stack).unwrap().range().start(),
        kids[0].get_leaf(stack).unwrap().range().start()
    );
    clone.bstack_drop(&alloc).unwrap();
    assert_eq!(
        h.handle().get_kids()[1]
            .get_leaf(stack)
            .unwrap()
            .get_val(stack)
            .unwrap(),
        20
    );

    // Move re-homes each embedded child to a fresh standalone allocation.
    let (moved, tag) = bstack_move!(h, &alloc).unwrap();
    assert_eq!(tag, 99);
    assert_eq!(
        moved[0]
            .handle()
            .get_leaf(stack)
            .unwrap()
            .get_val(stack)
            .unwrap(),
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
            assert_eq!(arr[0].get_val(stack).unwrap(), 10);
            assert_eq!(arr[1].get_val(stack).unwrap(), 20);
        }
        _ => panic!("expected Leaves"),
    }

    // Clone deep-copies each element.
    let clone = e.try_clone_in(&alloc).unwrap();
    match clone.handle().read(&alloc).unwrap() {
        ArrEnumView::Leaves(arr) => assert_eq!(arr[0].get_val(stack).unwrap(), 10),
        _ => panic!("expected Leaves"),
    }
    clone.bstack_drop(&alloc).unwrap();

    // Move yields `[BStackOwned<MacroLeaf>; 2]`.
    match bstack_move!(e, &alloc).unwrap() {
        ArrEnumData::Leaves(arr) => {
            assert_eq!(arr[1].handle().get_val(stack).unwrap(), 20);
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
            assert_eq!(arr[0].get_val(stack).unwrap(), 1);
            assert_eq!(arr[1].get_val(stack).unwrap(), 2);
        }
        _ => panic!("expected Refs"),
    }
    e.bstack_drop(&alloc).unwrap(); // owns nothing
    assert_eq!(l0.handle().get_val(stack).unwrap(), 1);
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
        StrongArrEnumView::Shared(arr) => assert_eq!(arr[0].get_val(stack).unwrap(), 5),
        _ => panic!("expected Shared"),
    }
    // Clone re-references each; teardown of both holders returns to keep0's ref.
    let clone = e.try_clone_in(&alloc).unwrap();
    clone.bstack_drop(&alloc).unwrap();
    e.bstack_drop(&alloc).unwrap();
    assert_eq!(keep0.handle().get_val(stack).unwrap(), 5);
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
            assert_eq!(arr[0].as_ref().unwrap().handle().get_val(stack).unwrap(), 5);
            assert_eq!(arr[1].as_ref().unwrap().handle().get_val(stack).unwrap(), 6);
        }
        _ => panic!("expected Weaks"),
    }
    e.bstack_drop(&alloc).unwrap(); // releases the weak refs
    assert_eq!(c0.handle().get_val(stack).unwrap(), 5);
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

    assert_eq!(h.handle().get_tag(stack).unwrap(), 7);
    let g = h.handle().get_grid(stack).unwrap(); // [[MacroLeaf; 2]; 2]
    assert_eq!(g[0][0].get_val(stack).unwrap(), 1);
    assert_eq!(g[0][1].get_val(stack).unwrap(), 2);
    assert_eq!(g[1][0].get_val(stack).unwrap(), 3);
    assert_eq!(g[1][1].get_val(stack).unwrap(), 4);

    // Deep clone: fresh blocks, same values.
    let clone = h.try_clone_in(&alloc).unwrap();
    let cg = clone.handle().get_grid(stack).unwrap();
    assert_eq!(cg[1][1].get_val(stack).unwrap(), 4);
    assert_ne!(cg[0][0].range().start(), g[0][0].range().start());
    clone.bstack_drop(&alloc).unwrap();
    assert_eq!(
        h.handle().get_grid(stack).unwrap()[1][0]
            .get_val(stack)
            .unwrap(),
        3
    );

    // Move: nested owning handles.
    let (moved, tag) = bstack_move!(h, &alloc).unwrap();
    assert_eq!(tag, 7);
    assert_eq!(moved[1][1].handle().get_val(stack).unwrap(), 4);
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

    let c = h.handle().get_cube(stack).unwrap(); // [[[MacroLeaf; 2]; 1]; 2]
    assert_eq!(c[0][0][0].get_val(stack).unwrap(), 0);
    assert_eq!(c[0][0][1].get_val(stack).unwrap(), 1);
    assert_eq!(c[1][0][0].get_val(stack).unwrap(), 2);
    assert_eq!(c[1][0][1].get_val(stack).unwrap(), 3);

    // A ref cube owns nothing: dropping leaves targets alive.
    h.bstack_drop(&alloc).unwrap();
    for l in leaves {
        assert!(l.handle().get_val(stack).unwrap() < 4);
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
    assert_eq!(h.handle().get_tag(stack).unwrap(), 5);

    let g = h.handle().get_kids(); // [[EmbChild; 2]; 1]
    assert_eq!(g[0][0].get_leaf(stack).unwrap().get_val(stack).unwrap(), 10);
    assert_eq!(g[0][1].get_leaf(stack).unwrap().get_val(stack).unwrap(), 20);

    let clone = h.try_clone_in(&alloc).unwrap();
    let cg = clone.handle().get_kids();
    assert_eq!(
        cg[0][1].get_leaf(stack).unwrap().get_val(stack).unwrap(),
        20
    );
    assert_ne!(
        cg[0][0].get_leaf(stack).unwrap().range().start(),
        g[0][0].get_leaf(stack).unwrap().range().start()
    );
    clone.bstack_drop(&alloc).unwrap();

    let (moved, tag) = bstack_move!(h, &alloc).unwrap();
    assert_eq!(tag, 5);
    assert_eq!(
        moved[0][0]
            .handle()
            .get_leaf(stack)
            .unwrap()
            .get_val(stack)
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
            assert_eq!(arr[0].map(|h| h.get_val(stack).unwrap()), Some(10));
            assert!(arr[1].is_none());
            assert_eq!(arr[2].map(|h| h.get_val(stack).unwrap()), Some(30));
        }
        _ => panic!("expected Slots"),
    }

    let clone = e.try_clone_in(&alloc).unwrap();
    match clone.handle().read(&alloc).unwrap() {
        OptArrEnumView::Slots(arr) => {
            assert_eq!(arr[2].map(|h| h.get_val(stack).unwrap()), Some(30));
            assert!(arr[1].is_none());
        }
        _ => panic!("expected Slots"),
    }
    clone.bstack_drop(&alloc).unwrap();

    match bstack_move!(e, &alloc).unwrap() {
        OptArrEnumData::Slots(arr) => {
            assert_eq!(
                arr[0].as_ref().map(|h| h.handle().get_val(stack).unwrap()),
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
            assert_eq!(arr[0].get_leaf(stack).unwrap().get_val(stack).unwrap(), 10);
            assert_eq!(arr[1].get_leaf(stack).unwrap().get_val(stack).unwrap(), 20);
        }
        _ => panic!("expected Kids"),
    }

    let clone = e.try_clone_in(&alloc).unwrap();
    match clone.handle().read(&alloc).unwrap() {
        EmbArrEnumView::Kids(arr) => {
            assert_eq!(arr[1].get_leaf(stack).unwrap().get_val(stack).unwrap(), 20)
        }
        _ => panic!("expected Kids"),
    }
    clone.bstack_drop(&alloc).unwrap();

    match bstack_move!(e, &alloc).unwrap() {
        EmbArrEnumData::Kids(arr) => {
            assert_eq!(
                arr[0]
                    .handle()
                    .get_leaf(stack)
                    .unwrap()
                    .get_val(stack)
                    .unwrap(),
                10
            );
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
            assert_eq!(g[0][0].get_val(stack).unwrap(), 1);
            assert_eq!(g[1][1].get_val(stack).unwrap(), 4);
        }
        _ => panic!("expected Grid"),
    }

    let clone = e.try_clone_in(&alloc).unwrap();
    clone.bstack_drop(&alloc).unwrap();

    match bstack_move!(e, &alloc).unwrap() {
        NestArrEnumData::Grid(g) => {
            assert_eq!(g[1][0].handle().get_val(stack).unwrap(), 3);
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
    assert_eq!(h.handle().get_tag(stack).unwrap(), 9);

    let rows = h.handle().get_rows(&alloc).unwrap(); // [BStackVec<u32,_>; 2]
    assert_eq!(rows[0].to_vec().unwrap(), vec![1u32, 2]);
    assert_eq!(rows[1].to_vec().unwrap(), vec![3u32, 4, 5]);

    // Each slot is an independent, growable vector.
    let mut rows_mut = h.handle().get_rows(&alloc).unwrap();
    rows_mut[0].push(99).unwrap();
    assert_eq!(
        h.handle().get_rows(&alloc).unwrap()[0].to_vec().unwrap(),
        vec![1u32, 2, 99]
    );
    assert_eq!(
        h.handle().get_rows(&alloc).unwrap()[1].to_vec().unwrap(),
        vec![3u32, 4, 5]
    );

    // Clone deep-copies both data blocks.
    let clone = h.try_clone_in(&alloc).unwrap();
    let crows = clone.handle().get_rows(&alloc).unwrap();
    assert_eq!(crows[1].to_vec().unwrap(), vec![3u32, 4, 5]);
    clone.bstack_drop(&alloc).unwrap();
    assert_eq!(
        h.handle().get_rows(&alloc).unwrap()[1].to_vec().unwrap(),
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

    let ls = h.handle().get_lists(&alloc).unwrap(); // [BStackRefVec<MacroLeaf,_>; 2]
    assert_eq!(ls[0].len().unwrap(), 2);
    assert_eq!(ls[0].get(1).unwrap().unwrap().get_val(stack).unwrap(), 1);
    assert_eq!(ls[1].get(0).unwrap().unwrap().get_val(stack).unwrap(), 2);

    // Ref vecs own the offset arrays but not the targets.
    h.bstack_drop(&alloc).unwrap();
    for l in leaves {
        assert!(l.handle().get_val(stack).unwrap() < 4);
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
    assert_eq!(h.handle().get_tag(stack).unwrap(), 7);

    let gs = h.handle().get_groups(&alloc).unwrap();
    assert_eq!(gs[0].len().unwrap(), 2);
    assert_eq!(gs[0].get(0).unwrap().unwrap().get_val(stack).unwrap(), 10);
    assert_eq!(gs[1].get(0).unwrap().unwrap().get_val(stack).unwrap(), 20);

    // Deep clone: distinct child blocks.
    let clone = h.try_clone_in(&alloc).unwrap();
    let cgs = clone.handle().get_groups(&alloc).unwrap();
    assert_eq!(cgs[0].get(1).unwrap().unwrap().get_val(stack).unwrap(), 11);
    assert_ne!(
        cgs[0].get(0).unwrap().unwrap().range().start(),
        gs[0].get(0).unwrap().unwrap().range().start()
    );
    clone.bstack_drop(&alloc).unwrap();
    assert_eq!(
        h.handle().get_groups(&alloc).unwrap()[1]
            .get(0)
            .unwrap()
            .unwrap()
            .get_val(stack)
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
    let s = h.handle().get_slots(&alloc).unwrap(); // [Option<BStackVec<u32,_>>; 3]
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
    assert_eq!(h.handle().get_tag(stack).unwrap(), 7);

    let rows = h.handle().get_rows(&alloc).unwrap(); // Vec<[MacroLeaf; 2]>
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].get_val(stack).unwrap(), 0);
    assert_eq!(rows[0][1].get_val(stack).unwrap(), 1);
    assert_eq!(rows[1][0].get_val(stack).unwrap(), 2);
    assert_eq!(rows[1][1].get_val(stack).unwrap(), 3);

    // Clone aliases: same target offsets, but a fresh offset-array data block.
    let clone = h.try_clone_in(&alloc).unwrap();
    let crows = clone.handle().get_rows(&alloc).unwrap();
    assert_eq!(
        crows[1][0].range().start(),
        rows[1][0].range().start() // same target (aliased)
    );
    clone.bstack_drop(&alloc).unwrap();
    // Original + targets still alive after clone teardown.
    assert_eq!(
        h.handle().get_rows(&alloc).unwrap()[0][1]
            .get_val(stack)
            .unwrap(),
        1
    );

    // Dropping the holder frees only the offset array, not the targets.
    h.bstack_drop(&alloc).unwrap();
    for l in leaves {
        assert!(l.handle().get_val(stack).unwrap() < 4);
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
    assert_eq!(h.handle().get_tag(stack).unwrap(), 7);

    let rows = h.handle().get_rows(&alloc).unwrap(); // Vec<[MacroLeaf; 2]>
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].get_val(stack).unwrap(), 1);
    assert_eq!(rows[0][1].get_val(stack).unwrap(), 2);
    assert_eq!(rows[1][0].get_val(stack).unwrap(), 3);
    assert_eq!(rows[1][1].get_val(stack).unwrap(), 4);

    // Deep clone: distinct child blocks.
    let clone = h.try_clone_in(&alloc).unwrap();
    let crows = clone.handle().get_rows(&alloc).unwrap();
    assert_eq!(crows[1][1].get_val(stack).unwrap(), 4);
    assert_ne!(crows[0][0].range().start(), rows[0][0].range().start());
    clone.bstack_drop(&alloc).unwrap();
    assert_eq!(
        h.handle().get_rows(&alloc).unwrap()[1][0]
            .get_val(stack)
            .unwrap(),
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

    let g = h.handle().get_groups(&alloc).unwrap(); // Vec<[MacroStrongChild; 2]>
    assert_eq!(g[0][0].get_val(stack).unwrap(), 10);
    assert_eq!(g[0][1].get_val(stack).unwrap(), 20);

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

    let g = h.handle().get_groups(&alloc).unwrap(); // Vec<[Option<BStackRc>; 2]>
    assert_eq!(
        g[0][0].as_ref().unwrap().handle().get_val(stack).unwrap(),
        1
    );
    assert!(g[0][1].as_ref().is_some());
    drop(g); // release the upgraded strong refs so `a` can actually be freed

    // Drop `a`'s data: its slot no longer upgrades; `b` still does.
    drop(a);
    let g = h.handle().get_groups(&alloc).unwrap();
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
            assert_eq!(v.get(0).unwrap().unwrap().get_val(stack).unwrap(), 10);
            assert_eq!(v.get(1).unwrap().unwrap().get_val(stack).unwrap(), 20);
        }
        _ => panic!("expected Items"),
    }

    // Clone deep-copies the vector + its children.
    let clone = e.try_clone_in(&alloc).unwrap();
    match clone.handle().read(&alloc).unwrap() {
        OwnedVecEnumView::Items(v) => {
            assert_eq!(v.get(1).unwrap().unwrap().get_val(stack).unwrap(), 20)
        }
        _ => panic!("expected Items"),
    }
    clone.bstack_drop(&alloc).unwrap();

    // Move hands back the vector handle.
    match bstack_move!(e, &alloc).unwrap() {
        OwnedVecEnumData::Items(v) => {
            assert_eq!(v.get(0).unwrap().unwrap().get_val(stack).unwrap(), 10);
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
            assert_eq!(v[0][0].get_val(stack).unwrap(), 0);
            assert_eq!(v[1][1].get_val(stack).unwrap(), 3);
        }
        _ => panic!("expected Rows"),
    }

    // A ref vec of arrays owns nothing: teardown leaves targets alive.
    e.bstack_drop(&alloc).unwrap();
    for l in leaves {
        assert!(l.handle().get_val(stack).unwrap() < 4);
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
            assert_eq!(v[0][0].get_val(stack).unwrap(), 1);
            assert_eq!(v[1][1].get_val(stack).unwrap(), 4);
        }
        _ => panic!("expected Grid"),
    }

    let clone = e.try_clone_in(&alloc).unwrap();
    clone.bstack_drop(&alloc).unwrap();

    // Move rebuilds Vec<[BStackOwned<MacroLeaf>; 2]> and frees the offset array.
    match bstack_move!(e, &alloc).unwrap() {
        OwnedVecArrEnumData::Grid(v) => {
            assert_eq!(v[1][0].handle().get_val(stack).unwrap(), 3);
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
    assert_eq!(b.handle().get_tag(stack).unwrap(), 7);
    assert_eq!(
        b.handle().get_item(stack).unwrap().get_val(stack).unwrap(),
        42
    );

    // Clone aliases the ref (same target block); the box itself is fresh.
    let clone = b.try_clone_in(&alloc).unwrap();
    assert_eq!(
        clone.handle().get_item(stack).unwrap().range().start(),
        b.handle().get_item(stack).unwrap().range().start()
    );
    assert_ne!(clone.handle().range().start(), b.handle().range().start());
    clone.bstack_drop(&alloc).unwrap();

    // The box references but does not own the leaf: dropping it leaves it alive.
    b.bstack_drop(&alloc).unwrap();
    assert_eq!(leaf.handle().get_val(stack).unwrap(), 42);
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
    assert_eq!(back.get_item(stack).unwrap().get_val(stack).unwrap(), 9);

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
    assert_eq!(b.handle().get_tag(stack).unwrap(), 5);

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

    let up = b.handle().get_item(&alloc).unwrap().expect("alive");
    assert_eq!(up.handle().get_val(stack).unwrap(), 7);
    drop(up);

    drop(c); // sole strong owner gone → can't upgrade
    assert!(b.handle().get_item(&alloc).unwrap().is_none());
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
    let b = OwnedBox::<MacroLeaf>::new(&alloc, leaf, 7).unwrap();
    assert_eq!(b.handle().get_tag(stack).unwrap(), 7);
    assert_eq!(
        b.handle().get_item(stack).unwrap().get_val(stack).unwrap(),
        42
    );

    // Deep clone: the owned child is a FRESH block (distinct offset), same value.
    let clone = b.try_clone_in(&alloc).unwrap();
    let citem = clone.handle().get_item(stack).unwrap();
    assert_eq!(citem.get_val(stack).unwrap(), 42);
    assert_ne!(
        citem.range().start(),
        b.handle().get_item(stack).unwrap().range().start()
    );
    clone.bstack_drop(&alloc).unwrap();
    // Original child survives the clone's teardown.
    assert_eq!(
        b.handle().get_item(stack).unwrap().get_val(stack).unwrap(),
        42
    );

    // Dropping the box frees its owned child — reclaimed with no leak.
    b.bstack_drop(&alloc).unwrap();
    assert_teardown_reclaims(&alloc, || {
        let leaf = MacroLeaf::new(&alloc, 42).unwrap();
        OwnedBox::<MacroLeaf>::new(&alloc, leaf, 7).unwrap()
    });
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
    match clone.handle().get_e(stack).unwrap().read(&alloc).unwrap() {
        ArrEnumView::Leaves(a) => assert_eq!(a[1].get_val(stack).unwrap(), 2),
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
    assert_eq!(b.handle().get_item(stack).unwrap(), 42);
    assert_eq!(b.handle().get_tag(stack).unwrap(), 7);

    // Clone byte-copies the POD value.
    let clone = b.try_clone_in(&alloc).unwrap();
    assert_eq!(clone.handle().get_item(stack).unwrap(), 42);
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
    assert_eq!(b.handle().get_tag(stack).unwrap(), 99);
    // Accessor: an EmbChild handle into the inline slot (pure offset math).
    assert_eq!(
        b.handle()
            .get_item()
            .get_leaf(stack)
            .unwrap()
            .get_val(stack)
            .unwrap(),
        10
    );

    // Clone folds the embedded child inline, deep-cloning its owned leaf — via the
    // generic `T`'s `BStackBlock` clone hook (a trait method, not inherent).
    let clone = b.try_clone_in(&alloc).unwrap();
    assert_eq!(
        clone
            .handle()
            .get_item()
            .get_leaf(stack)
            .unwrap()
            .get_val(stack)
            .unwrap(),
        10
    );
    assert_ne!(
        clone
            .handle()
            .get_item()
            .get_leaf(stack)
            .unwrap()
            .range()
            .start(),
        b.handle()
            .get_item()
            .get_leaf(stack)
            .unwrap()
            .range()
            .start()
    );
    clone.bstack_drop(&alloc).unwrap();

    // Move re-homes the embedded child to a fresh standalone block.
    let (moved, tag) = bstack_move!(b, &alloc).unwrap();
    assert_eq!(tag, 99);
    assert_eq!(
        moved
            .handle()
            .get_leaf(stack)
            .unwrap()
            .get_val(stack)
            .unwrap(),
        10
    );
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
        BoxEnumGView::Item(l) => assert_eq!(l.get_val(stack).unwrap(), 42),
        _ => panic!("expected Item"),
    }

    // Deep clone recurses into the owned child through T's BStackBlock hooks.
    let clone = e.try_clone_in(&alloc).unwrap();
    match clone.handle().read(&alloc).unwrap() {
        BoxEnumGView::Item(l) => assert_eq!(l.get_val(stack).unwrap(), 42),
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
            assert_eq!(owned.handle().get_val(stack).unwrap(), 42);
            owned.bstack_drop(&alloc).unwrap();
        }
        _ => panic!("expected Item"),
    }
}

#[test]
fn macro_generic_enum_tag_variant() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    let e = BoxEnumG::<MacroLeaf>::new(&alloc, BoxEnumGData::Tag(99)).unwrap();
    match e.handle().read(&alloc).unwrap() {
        BoxEnumGView::Tag(t) => assert_eq!(t, 99),
        _ => panic!("expected Tag"),
    }
    e.bstack_drop(&alloc).unwrap();
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
    assert_eq!(b.handle().get_tag(stack).unwrap(), 9);
    let arr = b.handle().get_arr(stack).unwrap(); // [MacroLeaf; 3]
    assert_eq!(arr[0].get_val(stack).unwrap(), 0);
    assert_eq!(arr[2].get_val(stack).unwrap(), 2);

    // Distinct N → distinct on-disk layout → distinct tags.
    assert_ne!(
        <RefArrN<MacroLeaf, 3> as BStackCast>::eightcc(),
        <RefArrN<MacroLeaf, 4> as BStackCast>::eightcc(),
    );

    // A ref array owns nothing: dropping leaves the targets alive.
    b.bstack_drop(&alloc).unwrap();
    for l in leaves {
        assert!(l.handle().get_val(stack).unwrap() < 3);
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
    let a = o.handle().get_arr(stack).unwrap();
    assert_eq!(a[1].get_val(stack).unwrap(), 20);
    let clone = o.try_clone_in(&alloc).unwrap();
    assert_ne!(
        clone.handle().get_arr(stack).unwrap()[0].range().start(),
        a[0].range().start()
    );
    clone.bstack_drop(&alloc).unwrap();
    o.bstack_drop(&alloc).unwrap();

    // POD const array.
    let p = PodArrN::<4>::new(&alloc, [1u16, 2, 3, 4], 7).unwrap();
    assert_eq!(p.handle().get_xs(stack).unwrap(), [1u16, 2, 3, 4]);
    assert_eq!(p.handle().get_tag(stack).unwrap(), 7);
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
    assert_eq!(cow.handle().get_val(stack).unwrap(), 7);
    assert_eq!(cow.range().start(), base_start);

    // into_owned deep-copies: a fresh block at a different address, same value.
    let owned = cow.into_owned(&alloc).unwrap();
    assert_ne!(owned.handle().range().start(), base_start);
    assert_eq!(owned.handle().get_val(stack).unwrap(), 7);
    owned.bstack_drop(&alloc).unwrap();

    // The borrowed source is untouched.
    assert_eq!(base.handle().get_val(stack).unwrap(), 7);
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
    assert_eq!(owned.handle().get_val(stack).unwrap(), 5);
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
        assert_eq!(m.handle().get_val(stack).unwrap(), 9);
    }
    assert!(cow.is_owned());

    // A second to_mut is a no-op: still the same owned copy.
    let owned_start = cow.range().start();
    let _ = cow.to_mut(&alloc).unwrap();
    assert_eq!(cow.range().start(), owned_start);

    // Dropping the Cow frees only the copy; the borrowed source survives.
    cow.bstack_drop(&alloc).unwrap();
    assert_eq!(base.handle().get_val(stack).unwrap(), 9);
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
    assert_eq!(base.handle().get_val(stack).unwrap(), 3);
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
        holder
            .handle()
            .get_boxed(stack)
            .unwrap()
            .get(stack)
            .unwrap(),
        500
    );
    assert_eq!(holder.handle().get_tag(stack).unwrap(), 9);

    // Deep-cloning the parent recurses into the child box (fresh child block).
    let clone = holder.try_clone_in(&alloc).unwrap();
    assert_ne!(
        clone.handle().get_boxed(stack).unwrap().range().start(),
        holder.handle().get_boxed(stack).unwrap().range().start(),
    );
    assert_eq!(
        clone.handle().get_boxed(stack).unwrap().get(stack).unwrap(),
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
        .map(|h| h.get_val(stack).unwrap())
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
    assert_eq!(
        list.front(stack).unwrap().unwrap().get_val(stack).unwrap(),
        1
    );
    assert_eq!(
        list.back(stack).unwrap().unwrap().get_val(stack).unwrap(),
        3
    );

    // FIFO drain from the front.
    let a = list.pop_front(&alloc).unwrap().unwrap();
    assert_eq!(a.handle().get_val(stack).unwrap(), 1);
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
    assert_eq!(back.handle().get_val(stack).unwrap(), 3);
    back.bstack_drop(&alloc).unwrap();

    let front = list.pop_front(&alloc).unwrap().unwrap();
    assert_eq!(front.handle().get_val(stack).unwrap(), 1);
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
    // the node's single value ref into the value's own children (a non-recursive
    // teardown would leak the MacroLeaf grandchild).
    assert_teardown_reclaims(&alloc, || {
        let leaf = MacroLeaf::new(&alloc, 10).unwrap();
        let parent = MacroParent::new(&alloc, leaf, 1).unwrap();
        let list = BStackLinkedList::<MacroParent>::new(&alloc).unwrap();
        list.push_back(&alloc, parent).unwrap();
        list
    });
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
        .map(|h| h.get_val(alloc.stack()).unwrap())
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
        .map(|h| h.get_val(stack).unwrap())
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
    assert_eq!(dq.front(stack).unwrap().unwrap().get_val(stack).unwrap(), 0);
    assert_eq!(dq.back(stack).unwrap().unwrap().get_val(stack).unwrap(), 9);

    // FIFO drain from the front.
    for v in 0..10u32 {
        let x = dq.pop_front(&alloc).unwrap().unwrap();
        assert_eq!(x.handle().get_val(stack).unwrap(), v);
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
    assert_eq!(back.handle().get_val(stack).unwrap(), 3);
    back.bstack_drop(&alloc).unwrap();

    let front = dq.pop_front(&alloc).unwrap().unwrap();
    assert_eq!(front.handle().get_val(stack).unwrap(), 1);
    front.bstack_drop(&alloc).unwrap();

    assert_eq!(deque_values(&dq, stack), vec![2]);
    dq.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_deque_drop_is_recursive() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    // Full recursion through the ring must free the MacroLeaf grandchild too.
    assert_teardown_reclaims(&alloc, || {
        let leaf = MacroLeaf::new(&alloc, 10).unwrap();
        let parent = MacroParent::new(&alloc, leaf, 1).unwrap();
        let dq = BStackDeque::<MacroParent>::new(&alloc).unwrap();
        dq.push_back(&alloc, parent).unwrap();
        dq
    });
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
        map.get(stack, &7).unwrap().unwrap().get_val(stack).unwrap(),
        700
    );
    assert_eq!(
        map.get(stack, &9).unwrap().unwrap().get_val(stack).unwrap(),
        900
    );
    assert!(map.contains_key(stack, &7).unwrap());
    assert!(!map.contains_key(stack, &8).unwrap());

    // Overwrite returns the previous value (owned) and does not change len.
    let old = map
        .insert(&alloc, 7, MacroLeaf::new(&alloc, 701).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(old.handle().get_val(stack).unwrap(), 700);
    old.bstack_drop(&alloc).unwrap();
    assert_eq!(map.len(stack).unwrap(), 2);
    assert_eq!(
        map.get(stack, &7).unwrap().unwrap().get_val(stack).unwrap(),
        701
    );

    // Remove returns the value (owned); the key is then absent.
    let removed = map.remove(&alloc, &9).unwrap().unwrap();
    assert_eq!(removed.handle().get_val(stack).unwrap(), 900);
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
            map.get(stack, &k).unwrap().unwrap().get_val(stack).unwrap(),
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
            assert_eq!(got.unwrap().get_val(stack).unwrap(), k * 10);
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
    assert_eq!(
        map.get(stack, &a).unwrap().unwrap().get_val(stack).unwrap(),
        11
    );
    assert_eq!(
        map.get(stack, &b).unwrap().unwrap().get_val(stack).unwrap(),
        22
    );
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

    // Full recursion through a stored value must free the MacroLeaf grandchild.
    assert_teardown_reclaims(&alloc, || {
        let leaf = MacroLeaf::new(&alloc, 10).unwrap();
        let parent = MacroParent::new(&alloc, leaf, 1).unwrap();
        let map = BStackHashMap::<u32, MacroParent>::new(&alloc).unwrap();
        map.insert(&alloc, 42, parent).unwrap();
        map
    });
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
            clone
                .get(stack, &k)
                .unwrap()
                .unwrap()
                .get_val(stack)
                .unwrap(),
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
        map.get(stack, &3).unwrap().unwrap().get_val(stack).unwrap(),
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
        .map(|(k, v)| (*k, v.get_val(stack).unwrap()))
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
            tree.get(stack, &k)
                .unwrap()
                .unwrap()
                .get_val(stack)
                .unwrap(),
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
    assert_eq!(old.handle().get_val(stack).unwrap(), 250);
    old.bstack_drop(&alloc).unwrap();
    assert_eq!(tree.len(stack).unwrap(), 50);
    assert_eq!(
        tree.get(stack, &25)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
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
                .get_val(stack)
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

    // Full recursion through a stored value must free the MacroLeaf grandchild.
    assert_teardown_reclaims(&alloc, || {
        let leaf = MacroLeaf::new(&alloc, 10).unwrap();
        let parent = MacroParent::new(&alloc, leaf, 1).unwrap();
        let tree = BStackBTreeMap::<u32, MacroParent>::new(&alloc).unwrap();
        tree.insert(&alloc, 42, parent).unwrap();
        tree
    });
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
            clone
                .get(stack, &k)
                .unwrap()
                .unwrap()
                .get_val(stack)
                .unwrap(),
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
        clone
            .get(stack, &10)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
        110
    );
    assert_eq!(
        tree.get(stack, &10)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
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
                            .get_val(alloc.stack())
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
                .get_val(alloc.stack())
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

// --------------------------------------------------------------------------
// stdlib: BStackCountingBloomFilter<K> — probabilistic set
// --------------------------------------------------------------------------

#[test]
fn stdlib_bloom_no_false_negatives() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let bloom = BStackCountingBloomFilter::<u32>::with_capacity(&alloc, 1000, 0.001).unwrap();
    assert!(bloom.is_empty(stack).unwrap());
    assert!(!bloom.contains(stack, &7).unwrap()); // fresh: everything absent

    for k in 0..50u32 {
        bloom.insert(&alloc, &k).unwrap();
    }
    assert_eq!(bloom.count(stack).unwrap(), 50);

    // No false negatives: every inserted key reports present.
    for k in 0..50u32 {
        assert!(bloom.contains(stack, &k).unwrap());
    }

    // Disjoint keys are (almost all) absent — allow a few false positives.
    let absent = (1_000..1_050u32)
        .filter(|k| !bloom.contains(stack, k).unwrap())
        .count();
    assert!(
        absent >= 45,
        "too many false positives: {}/50 absent",
        absent
    );

    // A positive FP estimate in (0, 1).
    let fp = bloom.estimated_fp_rate(stack).unwrap();
    assert!(fp > 0.0 && fp < 1.0);

    bloom.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_bloom_remove_and_clear() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // A single inserted key: removing it drives its counters back to zero (no
    // sharing), so `contains` becomes definitively false.
    let bloom = BStackCountingBloomFilter::<u32>::with_capacity(&alloc, 100, 0.01).unwrap();
    bloom.insert(&alloc, &42).unwrap();
    assert!(bloom.contains(stack, &42).unwrap());
    assert_eq!(bloom.count(stack).unwrap(), 1);
    bloom.remove(&alloc, &42).unwrap();
    assert!(!bloom.contains(stack, &42).unwrap());
    assert_eq!(bloom.count(stack).unwrap(), 0);

    // clear() zeroes everything.
    for k in 0..20u32 {
        bloom.insert(&alloc, &k).unwrap();
    }
    assert_eq!(bloom.count(stack).unwrap(), 20);
    bloom.clear(&alloc).unwrap();
    assert_eq!(bloom.count(stack).unwrap(), 0);
    for k in 0..20u32 {
        assert!(!bloom.contains(stack, &k).unwrap());
    }

    bloom.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_bloom_deep_clone_is_independent() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let bloom = BStackCountingBloomFilter::<u32>::with_capacity(&alloc, 100, 0.01).unwrap();
    for k in 0..10u32 {
        bloom.insert(&alloc, &k).unwrap();
    }
    let clone = bloom.try_clone_in(&alloc).unwrap();
    for k in 0..10u32 {
        assert!(clone.contains(stack, &k).unwrap());
    }
    // Clearing the clone leaves the original intact.
    clone.clear(&alloc).unwrap();
    assert_eq!(clone.count(stack).unwrap(), 0);
    assert_eq!(bloom.count(stack).unwrap(), 10);
    assert!(bloom.contains(stack, &5).unwrap());

    clone.bstack_drop(&alloc).unwrap();
    bloom.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_bloom_distinct_tags() {
    assert_ne!(
        <BStackCountingBloomFilter<u32> as BStackCast>::eightcc(),
        <BStackCountingBloomFilter<u64> as BStackCast>::eightcc(),
    );
}

#[test]
fn stdlib_bloom_guards_a_map() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // The headline pattern: a bloom filter in front of a map skips the disk probe
    // for keys that are definitely absent.
    let bloom = BStackCountingBloomFilter::<u32>::with_capacity(&alloc, 1000, 0.001).unwrap();
    let map = BStackHashMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    for k in 0..40u32 {
        map.insert(&alloc, k, MacroLeaf::new(&alloc, k * 3).unwrap())
            .unwrap();
        bloom.insert(&alloc, &k).unwrap();
    }

    let lookup = |k: u32| -> Option<u32> {
        // Fast-reject via the filter before touching the map.
        if !bloom.contains(stack, &k).unwrap() {
            return None;
        }
        map.get(stack, &k)
            .unwrap()
            .map(|v| v.get_val(stack).unwrap())
    };

    for k in 0..40u32 {
        assert_eq!(lookup(k), Some(k * 3));
    }
    // Absent keys: the filter short-circuits (and the map agrees).
    for k in 500..540u32 {
        assert_eq!(lookup(k), None);
    }

    bloom.bstack_drop(&alloc).unwrap();
    map.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// stdlib: BStackHashSet<K> — set with an embedded bloom filter
// --------------------------------------------------------------------------

#[test]
fn stdlib_hashset_basic() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let set = BStackHashSet::<u32>::new(&alloc).unwrap();
    assert!(set.is_empty(stack).unwrap());
    assert!(!set.contains(stack, &5).unwrap());

    // Insert many to force table growth; insert reports newness.
    for k in 0..100u32 {
        assert!(set.insert(&alloc, k).unwrap());
    }
    assert_eq!(set.len(stack).unwrap(), 100);
    // Duplicate insert is a no-op returning false.
    assert!(!set.insert(&alloc, 50).unwrap());
    assert_eq!(set.len(stack).unwrap(), 100);

    // No false negatives: every inserted key is present.
    for k in 0..100u32 {
        assert!(set.contains(stack, &k).unwrap());
    }
    assert!(!set.contains(stack, &10_000).unwrap());

    // Remove and re-check.
    assert!(set.remove(&alloc, &50).unwrap());
    assert!(!set.remove(&alloc, &50).unwrap()); // already gone
    assert!(!set.contains(stack, &50).unwrap());
    assert_eq!(set.len(stack).unwrap(), 99);

    set.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_hashset_deep_clone_is_independent() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let set = BStackHashSet::<u32>::new(&alloc).unwrap();
    for k in 0..20u32 {
        set.insert(&alloc, k).unwrap();
    }
    let clone = set.try_clone_in(&alloc).unwrap();
    for k in 0..20u32 {
        assert!(clone.contains(stack, &k).unwrap());
    }
    // Mutating the clone (incl. its embedded bloom) leaves the original intact.
    clone.remove(&alloc, &5).unwrap();
    assert!(!clone.contains(stack, &5).unwrap());
    assert!(set.contains(stack, &5).unwrap());
    assert_eq!(set.len(stack).unwrap(), 20);
    assert_eq!(clone.len(stack).unwrap(), 19);

    clone.bstack_drop(&alloc).unwrap();
    set.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_hashset_distinct_tags() {
    assert_ne!(
        <BStackHashSet<u32> as BStackCast>::eightcc(),
        <BStackHashSet<u64> as BStackCast>::eightcc(),
    );
}

// --------------------------------------------------------------------------
// stdlib: BStackBTreeSet<K> — ordered set with an embedded bloom filter
// --------------------------------------------------------------------------

#[test]
fn stdlib_btreeset_ordered() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let set = BStackBTreeSet::<u32>::new(&alloc).unwrap();
    assert!(set.first(stack).unwrap().is_none());

    // Scrambled but bijective insertion order to force node splits.
    for i in 0..60u32 {
        let k = (i * 23) % 60;
        assert!(set.insert(&alloc, k).unwrap());
    }
    assert_eq!(set.len(stack).unwrap(), 60);
    assert!(!set.insert(&alloc, 30).unwrap()); // duplicate
    assert_eq!(set.len(stack).unwrap(), 60);

    // No false negatives; ordered iteration is sorted.
    for k in 0..60u32 {
        assert!(set.contains(stack, &k).unwrap());
    }
    assert!(!set.contains(stack, &999).unwrap());
    assert_eq!(set.to_vec(stack).unwrap(), (0..60u32).collect::<Vec<_>>());
    assert_eq!(set.first(stack).unwrap().unwrap(), 0);
    assert_eq!(set.last(stack).unwrap().unwrap(), 59);

    set.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_btreeset_deep_clone_is_independent() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let set = BStackBTreeSet::<u32>::new(&alloc).unwrap();
    for k in 0..30u32 {
        set.insert(&alloc, k).unwrap();
    }
    let clone = set.try_clone_in(&alloc).unwrap();
    for k in 0..30u32 {
        assert!(clone.contains(stack, &k).unwrap());
    }
    // Inserting into the clone leaves the original unchanged.
    assert!(clone.insert(&alloc, 100).unwrap());
    assert!(clone.contains(stack, &100).unwrap());
    assert!(!set.contains(stack, &100).unwrap());
    assert_eq!(set.len(stack).unwrap(), 30);

    clone.bstack_drop(&alloc).unwrap();
    set.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_btreeset_distinct_tags() {
    assert_ne!(
        <BStackBTreeSet<u32> as BStackCast>::eightcc(),
        <BStackBTreeSet<u64> as BStackCast>::eightcc(),
    );
}

#[test]
fn stdlib_tree_remove_rebalances() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let tree = BStackBTreeMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    for k in 0..200u32 {
        tree.insert(&alloc, k, MacroLeaf::new(&alloc, k * 10).unwrap())
            .unwrap();
    }
    assert_eq!(tree.len(stack).unwrap(), 200);

    // Remove-absent returns None.
    assert!(tree.remove(&alloc, &1000).unwrap().is_none());

    // Remove every even key (heavy borrow/merge across a multi-level tree).
    for k in (0..200u32).step_by(2) {
        let v = tree.remove(&alloc, &k).unwrap().unwrap();
        assert_eq!(v.handle().get_val(stack).unwrap(), k * 10);
        v.bstack_drop(&alloc).unwrap();
    }
    assert_eq!(tree.len(stack).unwrap(), 100);

    // Evens gone, odds intact, iteration still sorted.
    for k in 0..200u32 {
        let g = tree.get(stack, &k).unwrap();
        if k % 2 == 0 {
            assert!(g.is_none());
        } else {
            assert_eq!(g.unwrap().get_val(stack).unwrap(), k * 10);
        }
    }
    let keys: Vec<u32> = tree
        .to_vec(stack)
        .unwrap()
        .iter()
        .map(|(k, _)| *k)
        .collect();
    assert_eq!(keys, (0..200u32).filter(|k| k % 2 == 1).collect::<Vec<_>>());

    // Drain the rest → empty (root collapses to 0).
    for k in (1..200u32).step_by(2) {
        tree.remove(&alloc, &k)
            .unwrap()
            .unwrap()
            .bstack_drop(&alloc)
            .unwrap();
    }
    assert_eq!(tree.len(stack).unwrap(), 0);
    assert!(tree.is_empty(stack).unwrap());
    assert!(tree.first(stack).unwrap().is_none());

    // Reinsert after collapse works.
    tree.insert(&alloc, 42, MacroLeaf::new(&alloc, 420).unwrap())
        .unwrap();
    assert_eq!(
        tree.get(stack, &42)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
        420
    );

    tree.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_btreeset_remove_rebalances() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let set = BStackBTreeSet::<u32>::new(&alloc).unwrap();
    for k in 0..60u32 {
        set.insert(&alloc, k).unwrap();
    }
    assert_eq!(set.len(stack).unwrap(), 60);
    assert!(!set.remove(&alloc, &999).unwrap()); // absent

    // Remove every even key (triggers borrow/merge across levels).
    for k in (0..60u32).step_by(2) {
        assert!(set.remove(&alloc, &k).unwrap());
        assert!(!set.remove(&alloc, &k).unwrap()); // already gone
    }
    assert_eq!(set.len(stack).unwrap(), 30);
    for k in 0..60u32 {
        assert_eq!(set.contains(stack, &k).unwrap(), k % 2 == 1);
    }
    assert_eq!(
        set.to_vec(stack).unwrap(),
        (0..60u32).filter(|k| k % 2 == 1).collect::<Vec<_>>()
    );

    // Drain the rest → empty, then reinsert.
    for k in (1..60u32).step_by(2) {
        assert!(set.remove(&alloc, &k).unwrap());
    }
    assert_eq!(set.len(stack).unwrap(), 0);
    assert!(set.first(stack).unwrap().is_none());
    assert!(set.insert(&alloc, 7).unwrap());
    assert!(set.contains(stack, &7).unwrap());

    set.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_string_extra_methods() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let s = BStackString::new(&alloc, "hello").unwrap();
    s.handle().push(&alloc, '!').unwrap();
    assert_eq!(s.handle().to_string(stack).unwrap(), "hello!");
    assert!(s.handle().starts_with(stack, "hell").unwrap());
    assert!(s.handle().ends_with(stack, "o!").unwrap());
    assert!(s.handle().contains(stack, "ell").unwrap());
    assert!(s.handle().eq_str(stack, "hello!").unwrap());
    assert!(!s.handle().eq_str(stack, "nope").unwrap());

    // truncate to a boundary; non-boundary is an error.
    s.handle().truncate(&alloc, 5).unwrap();
    assert_eq!(s.handle().to_string(stack).unwrap(), "hello");
    let u = BStackString::new(&alloc, "héllo").unwrap(); // 'é' is 2 bytes at [1,2]
    assert_eq!(u.handle().char_count(stack).unwrap(), 5);
    assert_eq!(u.handle().len(stack).unwrap(), 6);
    assert!(u.handle().truncate(&alloc, 2).is_err()); // splits 'é'
    u.bstack_drop(&alloc).unwrap();

    // clear empties it.
    s.handle().clear(&alloc).unwrap();
    assert!(s.handle().is_empty(stack).unwrap());
    assert_eq!(s.handle().char_count(stack).unwrap(), 0);

    s.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// stdlib: BStackBinaryHeap<K, V> — priority queue (binary min-heap)
// --------------------------------------------------------------------------

#[test]
fn stdlib_heap_pop_ascending() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let heap = BStackBinaryHeap::<u32, MacroLeaf>::new(&alloc).unwrap();
    assert!(heap.is_empty(stack).unwrap());
    assert!(heap.peek(stack).unwrap().is_none());
    assert!(heap.pop(&alloc).unwrap().is_none());

    // Push 0..50 in a scrambled (bijective) order; forces growth.
    for i in 0..50u32 {
        let k = (i * 17) % 50;
        heap.push(&alloc, k, MacroLeaf::new(&alloc, k * 10).unwrap())
            .unwrap();
    }
    assert_eq!(heap.len(stack).unwrap(), 50);
    assert_eq!(heap.peek(stack).unwrap().unwrap().0, 0); // min on top

    // Pop drains in ascending key order, with the right values.
    for expected in 0..50u32 {
        let (k, v) = heap.pop(&alloc).unwrap().unwrap();
        assert_eq!(k, expected);
        assert_eq!(v.handle().get_val(stack).unwrap(), expected * 10);
        v.bstack_drop(&alloc).unwrap();
    }
    assert!(heap.is_empty(stack).unwrap());
    assert!(heap.pop(&alloc).unwrap().is_none());

    heap.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_heap_duplicate_keys() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    let heap = BStackBinaryHeap::<u32, MacroLeaf>::new(&alloc).unwrap();
    for (k, v) in [(3u32, 30u32), (1, 10), (3, 31), (1, 11), (2, 20)] {
        heap.push(&alloc, k, MacroLeaf::new(&alloc, v).unwrap())
            .unwrap();
    }
    // Keys come out sorted (values within equal keys unspecified).
    let mut keys = Vec::new();
    while let Some((k, v)) = heap.pop(&alloc).unwrap() {
        keys.push(k);
        v.bstack_drop(&alloc).unwrap();
    }
    assert_eq!(keys, vec![1, 1, 2, 3, 3]);

    heap.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_heap_drop_is_recursive() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    // Full recursion through a stored value must free the MacroLeaf grandchild.
    assert_teardown_reclaims(&alloc, || {
        let leaf = MacroLeaf::new(&alloc, 10).unwrap();
        let parent = MacroParent::new(&alloc, leaf, 1).unwrap();
        let heap = BStackBinaryHeap::<u32, MacroParent>::new(&alloc).unwrap();
        heap.push(&alloc, 5, parent).unwrap();
        heap
    });
}

#[test]
fn stdlib_heap_deep_clone_is_independent() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let heap = BStackBinaryHeap::<u32, MacroLeaf>::new(&alloc).unwrap();
    for k in [5u32, 1, 4, 2, 3] {
        heap.push(&alloc, k, MacroLeaf::new(&alloc, k * 10).unwrap())
            .unwrap();
    }
    let clone = heap.try_clone_in(&alloc).unwrap();
    // Fresh value blocks.
    assert_ne!(
        clone.peek(stack).unwrap().unwrap().1.range().start(),
        heap.peek(stack).unwrap().unwrap().1.range().start(),
    );
    // Draining the clone leaves the original intact.
    for expected in 1..=5u32 {
        let (k, v) = clone.pop(&alloc).unwrap().unwrap();
        assert_eq!(k, expected);
        v.bstack_drop(&alloc).unwrap();
    }
    assert_eq!(heap.len(stack).unwrap(), 5);
    assert_eq!(heap.peek(stack).unwrap().unwrap().0, 1);

    clone.bstack_drop(&alloc).unwrap();
    heap.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_heap_distinct_tags() {
    assert_ne!(
        <BStackBinaryHeap<u32, MacroLeaf> as BStackCast>::eightcc(),
        <BStackBinaryHeap<u64, MacroLeaf> as BStackCast>::eightcc(),
    );
}

// --------------------------------------------------------------------------
// stdlib: iterators
// --------------------------------------------------------------------------

#[test]
fn stdlib_deque_iter() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let dq = BStackDeque::<MacroLeaf>::new(&alloc).unwrap();
    for v in 0..10u32 {
        dq.push_back(&alloc, MacroLeaf::new(&alloc, v).unwrap())
            .unwrap();
    }
    let mut got = Vec::new();
    for r in dq.iter(stack).unwrap() {
        got.push(r.unwrap().get_val(stack).unwrap());
    }
    assert_eq!(got, (0..10u32).collect::<Vec<_>>());
    dq.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_list_iter() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let list = BStackLinkedList::<MacroLeaf>::new(&alloc).unwrap();
    for v in 0..6u32 {
        list.push_back(&alloc, MacroLeaf::new(&alloc, v).unwrap())
            .unwrap();
    }
    let mut got = Vec::new();
    for r in list.iter(stack).unwrap() {
        got.push(r.unwrap().get_val(stack).unwrap());
    }
    assert_eq!(got, (0..6u32).collect::<Vec<_>>());
    list.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_map_iter() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let map = BStackHashMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    for k in 0..20u32 {
        map.insert(&alloc, k, MacroLeaf::new(&alloc, k * 10).unwrap())
            .unwrap();
    }
    let mut got = Vec::new();
    for r in map.iter(stack).unwrap() {
        let (k, v) = r.unwrap();
        got.push((k, v.get_val(stack).unwrap()));
    }
    got.sort_unstable(); // unordered iteration
    assert_eq!(got, (0..20u32).map(|k| (k, k * 10)).collect::<Vec<_>>());
    map.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_hashset_iter() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let set = BStackHashSet::<u32>::new(&alloc).unwrap();
    for k in 0..20u32 {
        set.insert(&alloc, k).unwrap();
    }
    let mut got = Vec::new();
    for r in set.iter(stack).unwrap() {
        got.push(r.unwrap());
    }
    got.sort_unstable();
    assert_eq!(got, (0..20u32).collect::<Vec<_>>());
    set.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_tree_iter_and_range() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let tree = BStackBTreeMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    for i in 0..40u32 {
        let k = (i * 13) % 40; // scrambled permutation
        tree.insert(&alloc, k, MacroLeaf::new(&alloc, k * 10).unwrap())
            .unwrap();
    }
    // Full iteration is sorted.
    let mut got = Vec::new();
    for r in tree.iter(stack).unwrap() {
        let (k, v) = r.unwrap();
        got.push((k, v.get_val(stack).unwrap()));
    }
    assert_eq!(got, (0..40u32).map(|k| (k, k * 10)).collect::<Vec<_>>());

    // range(10, 20) is the inclusive sub-slice, still sorted.
    let mut ranged = Vec::new();
    for r in tree.range(stack, 10, 20).unwrap() {
        ranged.push(r.unwrap().0);
    }
    assert_eq!(ranged, (10..=20u32).collect::<Vec<_>>());

    // A range whose lo falls between keys and hi past the end.
    let mut r2 = Vec::new();
    for r in tree.range(stack, 37, 999).unwrap() {
        r2.push(r.unwrap().0);
    }
    assert_eq!(r2, vec![37, 38, 39]);

    tree.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_btreeset_iter_and_range() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let set = BStackBTreeSet::<u32>::new(&alloc).unwrap();
    for i in 0..40u32 {
        set.insert(&alloc, (i * 13) % 40).unwrap();
    }
    let mut got = Vec::new();
    for r in set.iter(stack).unwrap() {
        got.push(r.unwrap());
    }
    assert_eq!(got, (0..40u32).collect::<Vec<_>>());

    let mut ranged = Vec::new();
    for r in set.range(stack, 15, 18).unwrap() {
        ranged.push(r.unwrap());
    }
    assert_eq!(ranged, vec![15, 16, 17, 18]);

    set.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// stdlib: entry API (get_or_insert_with / get_or_insert)
// --------------------------------------------------------------------------

#[test]
fn stdlib_map_entry() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let map = BStackHashMap::<u32, MacroLeaf>::new(&alloc).unwrap();

    // Absent: inserts, f produced the value.
    let (v, inserted) = map
        .get_or_insert_with(&alloc, 7, || MacroLeaf::new(&alloc, 70))
        .unwrap();
    assert!(inserted);
    assert_eq!(v.get_val(stack).unwrap(), 70);

    // Present: single probe, f NOT called, value unchanged.
    map.insert(&alloc, 5, MacroLeaf::new(&alloc, 50).unwrap())
        .unwrap();
    let called = std::cell::Cell::new(false);
    let (v, inserted) = map
        .get_or_insert_with(&alloc, 5, || {
            called.set(true);
            MacroLeaf::new(&alloc, 999)
        })
        .unwrap();
    assert!(!inserted);
    assert!(!called.get(), "f must not run on a hit");
    assert_eq!(v.get_val(stack).unwrap(), 50);

    // Eager get_or_insert frees the unused default on a hit.
    let (v, inserted) = map
        .get_or_insert(&alloc, 5, MacroLeaf::new(&alloc, 111).unwrap())
        .unwrap();
    assert!(!inserted);
    assert_eq!(v.get_val(stack).unwrap(), 50);

    map.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_map_entry_counter() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    // The headline entry pattern: a counter map with in-place value mutation.
    let counts = BStackHashMap::<u32, BStackBox<u64>>::new(&alloc).unwrap();
    for k in [1u32, 1, 2, 1, 2, 1] {
        let (v, inserted) = counts
            .get_or_insert_with(&alloc, k, || BStackBox::new(&alloc, 1u64))
            .unwrap();
        if !inserted {
            let cur = v.get(stack).unwrap();
            v.set(&alloc, cur + 1).unwrap();
        }
    }
    assert_eq!(
        counts.get(stack, &1).unwrap().unwrap().get(stack).unwrap(),
        4
    );
    assert_eq!(
        counts.get(stack, &2).unwrap().unwrap().get(stack).unwrap(),
        2
    );
    counts.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_tree_entry() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let tree = BStackBTreeMap::<u32, MacroLeaf>::new(&alloc).unwrap();

    let (v, inserted) = tree
        .get_or_insert_with(&alloc, 7, || MacroLeaf::new(&alloc, 70))
        .unwrap();
    assert!(inserted);
    assert_eq!(v.get_val(stack).unwrap(), 70);

    let called = std::cell::Cell::new(false);
    let (v, inserted) = tree
        .get_or_insert_with(&alloc, 7, || {
            called.set(true);
            MacroLeaf::new(&alloc, 999)
        })
        .unwrap();
    assert!(!inserted);
    assert!(!called.get());
    assert_eq!(v.get_val(stack).unwrap(), 70);

    tree.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// WAL: clone commit is crash-atomic — a crash mid-commit reclaims the orphans
// (uses bstack's fault injection; requires --features fault-injection + debug)
// --------------------------------------------------------------------------

#[cfg(feature = "fault-injection")]
#[test]
fn wal_clone_reclaims_orphans_on_commit_fault() {
    use bstack::fault::FaultPolicy;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    // Fail the first `inplace_gen` (the clone commit) exactly once; a real crash
    // there would run no rollback, leaving the clone's fresh blocks orphaned.
    struct FailFirstInplaceGen(AtomicBool);
    impl FaultPolicy for FailFirstInplaceGen {
        fn next_fault(&self, op: &'static str, _seq: u64) -> Option<io::Error> {
            if op == "inplace_gen" && !self.0.swap(true, Ordering::SeqCst) {
                Some(io::Error::other("injected clone-commit fault"))
            } else {
                None
            }
        }
    }

    let tmp = TempStack::new();
    let alloc = tmp.allocator(); // FirstFit is a BStackRaiiAllocator
    let stack = alloc.stack();

    // Source owns a child, so each deep clone allocates two blocks (+ a WAL block).
    let leaf = MacroLeaf::new(&alloc, 7).unwrap();
    let src = MacroParent::new(&alloc, leaf, 1).unwrap();

    // Repeatedly crash the clone commit. If the orphans (and WAL block) were
    // leaked, the committed length would climb every iteration; WAL reclamation
    // frees them back to the free list, so growth flattens once it's warm.
    let mut prev: Option<u64> = None;
    for i in 0..30 {
        stack.set_fault_policy(Some(Arc::new(FailFirstInplaceGen(AtomicBool::new(false)))));
        // Automatic WAL: `try_clone_in` on an anchored allocator (FirstFit) logs
        // and reclaims its orphans with no separate opt-in call.
        let r = src.try_clone_in(&alloc);
        stack.set_fault_policy(None);
        assert!(r.is_err(), "injected fault must fail the clone commit");
        let len = stack.len().unwrap();
        if i >= 3 {
            assert_eq!(len, prev.unwrap(), "faulted clone leaked at iter {i}");
        }
        prev = Some(len);
    }

    // Source intact; a real (unfaulted) clone still succeeds, reusing the space.
    let cl = src.try_clone_in(&alloc).unwrap();
    assert_eq!(
        cl.handle()
            .get_child(stack)
            .unwrap()
            .get_val(stack)
            .unwrap(),
        7
    );
    cl.bstack_drop(&alloc).unwrap();
    src.bstack_drop(&alloc).unwrap();
}

#[cfg(feature = "fault-injection")]
#[test]
fn wal_clone_reclaims_bulk_orphans_on_commit_fault() {
    use bstack::fault::FaultPolicy;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    // Same crash as the FirstFit test, but on a BULK allocator (GhostTree): the clone
    // takes the two-pass path, so its blocks are `alloc_bulk`'d and staged `Pending`
    // in the WAL by `allocate` *before* the commit's `inplace_gen`. GhostTree's
    // `alloc_bulk` uses no `inplace_gen`, so failing the first one hits the commit,
    // after the bulk alloc + WAL staging — the WAL must then reclaim the whole bulk.
    struct FailFirstInplaceGen(AtomicBool);
    impl FaultPolicy for FailFirstInplaceGen {
        fn next_fault(&self, op: &'static str, _seq: u64) -> Option<io::Error> {
            if op == "inplace_gen" && !self.0.swap(true, Ordering::SeqCst) {
                Some(io::Error::other("injected bulk clone-commit fault"))
            } else {
                None
            }
        }
    }

    let tmp = TempStack::new();
    let alloc = tmp.ghost_allocator();
    let stack = alloc.stack();

    let leaf = MacroLeaf::new(&alloc, 7).unwrap();
    let src = MacroParent::new(&alloc, leaf, 1).unwrap();

    let mut prev: Option<u64> = None;
    for i in 0..30 {
        stack.set_fault_policy(Some(Arc::new(FailFirstInplaceGen(AtomicBool::new(false)))));
        let r = src.try_clone_in(&alloc);
        stack.set_fault_policy(None);
        assert!(r.is_err(), "injected fault must fail the bulk clone commit");
        let len = stack.len().unwrap();
        if i >= 3 {
            assert_eq!(len, prev.unwrap(), "faulted bulk clone leaked at iter {i}");
        }
        prev = Some(len);
    }

    // A real (unfaulted) clone still succeeds, reusing the reclaimed space.
    let cl = src.try_clone_in(&alloc).unwrap();
    assert_eq!(
        cl.handle()
            .get_child(stack)
            .unwrap()
            .get_val(stack)
            .unwrap(),
        7
    );
    cl.bstack_drop(&alloc).unwrap();
    src.bstack_drop(&alloc).unwrap();
}

#[cfg(feature = "fault-injection")]
#[test]
fn wal_teardown_reclaims_on_free_fault() {
    use bstack::fault::FaultPolicy;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    // The teardown WAL commits by flipping `txn_status` in one `inplace_gen`; the
    // very next `set` is `finish_at_locked`'s first entry-status write — just past the
    // commit point, before any block is actually freed. Failing *that* set models
    // a crash mid-teardown with the transaction already committed (so `finish`
    // must roll every dealloc forward, with no half-freed block).
    struct FailSetAfterCommit {
        committed: AtomicBool,
        fired: AtomicBool,
    }
    impl FaultPolicy for FailSetAfterCommit {
        fn next_fault(&self, op: &'static str, _seq: u64) -> Option<io::Error> {
            if op == "inplace_gen" {
                self.committed.store(true, Ordering::SeqCst);
                return None;
            }
            if op == "set"
                && self.committed.load(Ordering::SeqCst)
                && !self.fired.swap(true, Ordering::SeqCst)
            {
                return Some(io::Error::other("injected teardown free fault"));
            }
            None
        }
    }

    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let build = |a: &_| {
        let list = BStackLinkedList::<MacroLeaf>::new(a).unwrap();
        for v in 0..5u32 {
            list.push_back(a, MacroLeaf::new(a, v).unwrap()).unwrap();
        }
        list
    };

    // Warm the persistent WAL block with one clean WAL-backed teardown, so `peak`
    // already accounts for it (it is allocated once and reused, never per-txn).
    build(&alloc).bstack_drop(&alloc).unwrap();

    let list1 = build(&alloc);
    let peak = stack.len().unwrap();

    // Crash the teardown just after its WAL commits; nothing gets freed inline.
    stack.set_fault_policy(Some(Arc::new(FailSetAfterCommit {
        committed: AtomicBool::new(false),
        fired: AtomicBool::new(false),
    })));
    // Automatic WAL: `bstack_drop` on the owned handle runs the WAL-backed
    // teardown (via `BStackOwned::bstack_drop` → `wal_teardown`) with no opt-in.
    let r = list1.bstack_drop(&alloc);
    stack.set_fault_policy(None);
    assert!(r.is_err(), "the injected fault must interrupt the teardown");

    // `finish` rolls the committed teardown forward, reclaiming the whole subtree.
    assert!(
        crate::wal::finish(&alloc).unwrap() > 0,
        "finish should reclaim the committed teardown's slices"
    );

    // Reclaimed: rebuilding an identical tree reuses the freed space (no leak).
    let list2 = build(&alloc);
    let after = stack.len().unwrap();
    assert!(
        after <= peak,
        "teardown crash leaked: file grew {peak} -> {after}"
    );
    list2.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// Cross-file registry: path<->id persistence + live-host resolution
// --------------------------------------------------------------------------

#[test]
fn registry_paths_persist_and_live_host_round_trips() {
    use crate::registry::{FileId, FileRegistry};
    use std::sync::Arc;

    let reg_file = TempStack::new();
    let foreign = TempStack::new();
    // A path we only *register* (never open) — proves the table stores strings.
    let ghost = std::env::temp_dir().join("bstack_raii_registry_ghost.bstack");

    // --- "run 1": register paths, attach a live host, use it, detach ---
    {
        let reg = FileRegistry::open(&reg_file.path).unwrap();

        let id_a = reg.register_path(&foreign.path).unwrap();
        // Ordinary ids are 1-based (0 is reserved for `SELF`).
        assert_eq!(id_a, FileId::from_u64(1).unwrap());
        assert!(!id_a.is_self());
        // Registration is idempotent (same path -> same id, no new slot).
        assert_eq!(reg.register_path(&foreign.path).unwrap(), id_a);
        // A distinct path gets the next id.
        let id_g = reg.register_path(&ghost).unwrap();
        assert_eq!(id_g.get(), 2);
        assert_eq!(reg.id_of(&foreign.path), Some(id_a));
        assert_eq!(reg.path_of(id_g).as_deref(), Some(ghost.as_path()));
        // `SELF` is never a registry entry and never takes the lock.
        assert!(FileId::SELF.is_self());
        assert!(reg.path_of(FileId::SELF).is_none());
        assert!(reg.with_host(FileId::SELF, |_| ()).is_none());

        // Attach the foreign file's own allocator as its live host (same path ->
        // same id), then read/write/alloc through the type-erased facade.
        let host: Arc<dyn crate::registry::ForeignHost> = Arc::new(foreign.allocator());
        let id = reg.attach(&foreign.path, host).unwrap();
        assert_eq!(id, id_a);
        assert!(reg.is_live(id));

        let block = reg
            .with_host(id, |h| {
                let r = h.alloc(64).unwrap();
                h.stack().set(r.start(), [1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
                let mut buf = [0u8; 8];
                h.stack().get_into(r.start(), &mut buf).unwrap();
                assert_eq!(buf, [1, 2, 3, 4, 5, 6, 7, 8]);
                r
            })
            .expect("host is live");
        // Free it through the host (under the read lock).
        reg.with_host(id, |h| unsafe { h.dealloc(block).unwrap() })
            .expect("host is live");

        reg.detach(id);
        assert!(!reg.is_live(id));
        assert!(reg.with_host(id, |_| ()).is_none());
    }

    // --- "run 2": reopen the same registry file; the path table persisted ---
    {
        let reg = FileRegistry::open(&reg_file.path).unwrap();
        assert_eq!(reg.id_of(&foreign.path).map(FileId::get), Some(1));
        assert_eq!(reg.id_of(&ghost).map(FileId::get), Some(2));
        assert_eq!(
            reg.path_of(FileId::from_u64(1).unwrap()).as_deref(),
            Some(foreign.path.as_path())
        );
        // The live layer is in-memory only: nothing is live after a reopen.
        assert!(!reg.is_live(FileId::from_u64(1).unwrap()));
    }

    let _ = std::fs::remove_file(&ghost);
}

// --------------------------------------------------------------------------
// #[bstack_mut]: generated set_<field> + raw_<field>_slice (POD and ref)
// --------------------------------------------------------------------------

#[bstack_block]
struct MutPod {
    #[bstack_mut]
    n: u64,
    tag: u32, // not mutable — no set_tag generated
}

#[bstack_block]
struct MutRef {
    #[bstack_mut]
    #[bstack_ref]
    target: MacroLeaf,
}

#[test]
fn macro_bstack_mut_pod_set_and_raw_slice() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let b = MutPod::new(&alloc, 10, 7).unwrap();
    assert_eq!(b.handle().get_n(stack).unwrap(), 10);
    assert_eq!(b.handle().get_tag(stack).unwrap(), 7);

    // Generated setter: one atomic overwrite.
    b.handle().set_n(stack, 42).unwrap();
    assert_eq!(b.handle().get_n(stack).unwrap(), 42);
    // `tag` (no #[bstack_mut]) is untouched — and there is no `set_tag` to call.
    assert_eq!(b.handle().get_tag(stack).unwrap(), 7);

    // Raw place: read the field's inline bytes back.
    let slice = unsafe { b.handle().raw_n_slice(stack) };
    assert_eq!(slice.len(), 8);
    let bytes = slice.read().unwrap();
    assert_eq!(u64::from_le_bytes(bytes[..8].try_into().unwrap()), 42);

    // Raw place: a write through it is observed by the typed getter.
    let mut w = unsafe { b.handle().raw_n_slice(stack) };
    w.write(99u64.to_le_bytes()).unwrap();
    assert_eq!(b.handle().get_n(stack).unwrap(), 99);

    b.bstack_drop(&alloc).unwrap();
}

#[test]
fn macro_bstack_mut_ref_repoints() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let a = MacroLeaf::new(&alloc, 111).unwrap();
    let c = MacroLeaf::new(&alloc, 222).unwrap();

    let holder = MutRef::new(&alloc, unsafe { BStackRef::from_range(a.handle().range()) }).unwrap();
    assert_eq!(
        holder
            .handle()
            .get_target(stack)
            .unwrap()
            .get_val(stack)
            .unwrap(),
        111
    );

    // Generated ref setter: repoint to `c` (a ref owns nothing, so nothing frees).
    holder
        .handle()
        .set_target(stack, unsafe { BStackRef::from_range(c.handle().range()) })
        .unwrap();
    assert_eq!(
        holder
            .handle()
            .get_target(stack)
            .unwrap()
            .get_val(stack)
            .unwrap(),
        222
    );

    // Both targets are still independently live (the ref borrowed them).
    holder.bstack_drop(&alloc).unwrap();
    assert_eq!(a.handle().get_val(stack).unwrap(), 111);
    assert_eq!(c.handle().get_val(stack).unwrap(), 222);
    a.bstack_drop(&alloc).unwrap();
    c.bstack_drop(&alloc).unwrap();
}

#[bstack_block]
struct MutOwned {
    #[bstack_mut]
    #[bstack_owned]
    child: MacroLeaf,
}

#[bstack_block]
struct MutStrong {
    #[bstack_mut]
    #[bstack_strong]
    s: MacroStrongChild,
}

#[test]
fn macro_bstack_mut_ref_replace_returns_old() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let a = MacroLeaf::new(&alloc, 111).unwrap();
    let c = MacroLeaf::new(&alloc, 222).unwrap();

    let holder = MutRef::new(&alloc, unsafe { BStackRef::from_range(a.handle().range()) }).unwrap();

    // `replace_` installs `c` and hands the old ref (→ a) back.
    let old = holder
        .handle()
        .replace_target(stack, unsafe { BStackRef::from_range(c.handle().range()) })
        .unwrap();
    assert_eq!(
        holder
            .handle()
            .get_target(stack)
            .unwrap()
            .get_val(stack)
            .unwrap(),
        222
    );
    let old_leaf = <MacroLeaf as BStackBlock>::from_range(old.into_range());
    assert_eq!(old_leaf.get_val(stack).unwrap(), 111);

    holder.bstack_drop(&alloc).unwrap();
    a.bstack_drop(&alloc).unwrap();
    c.bstack_drop(&alloc).unwrap();
}

#[test]
fn macro_bstack_mut_owned_replace_moves_old_out() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let a = MacroLeaf::new(&alloc, 1).unwrap();
    let holder = MutOwned::new(&alloc, a).unwrap();
    assert_eq!(
        holder
            .handle()
            .get_child(stack)
            .unwrap()
            .get_val(stack)
            .unwrap(),
        1
    );

    // Install `b`, move the old child (`a`) out — it is NOT freed.
    let b = MacroLeaf::new(&alloc, 2).unwrap();
    let old = holder.handle().replace_child(stack, b).unwrap();
    assert_eq!(
        holder
            .handle()
            .get_child(stack)
            .unwrap()
            .get_val(stack)
            .unwrap(),
        2
    );
    assert_eq!(old.handle().get_val(stack).unwrap(), 1); // moved-out old is still live

    // The caller owns the old value and frees it explicitly.
    old.bstack_drop(&alloc).unwrap();
    // Tearing down the holder frees the current child (`b`) + the shell.
    holder.bstack_drop(&alloc).unwrap();
}

// A failed `replace_` commit must hand the *consumed* new value back through
// `ReplaceError`, never leak it (the realloc-style hand-back contract). Uses
// bstack's fault injection; requires --features fault-injection + debug.
#[cfg(feature = "fault-injection")]
#[test]
fn macro_bstack_mut_replace_hands_new_value_back_on_commit_fault() {
    use bstack::fault::FaultPolicy;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    // Fail the first `set` (the replace commit) exactly once. `replace_` reads the
    // old offset with `get_into` *before* consuming `value`, so the only `set` in
    // the window is the commit itself.
    struct FailFirstSet(AtomicBool);
    impl FaultPolicy for FailFirstSet {
        fn next_fault(&self, op: &'static str, _seq: u64) -> Option<io::Error> {
            if op == "set" && !self.0.swap(true, Ordering::SeqCst) {
                Some(io::Error::other("injected replace-commit fault"))
            } else {
                None
            }
        }
    }

    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let a = MacroLeaf::new(&alloc, 1).unwrap();
    let holder = MutOwned::new(&alloc, a).unwrap();
    let b = MacroLeaf::new(&alloc, 2).unwrap();
    let b_off = b.handle().range().start();

    stack.set_fault_policy(Some(Arc::new(FailFirstSet(AtomicBool::new(false)))));
    let r = holder.handle().replace_child(stack, b);
    stack.set_fault_policy(None);

    // The commit failed, and the NEW value came back intact — same block, readable,
    // not an orphan.
    let err = match r {
        Ok(_) => panic!("injected fault must fail the replace commit"),
        Err(e) => e,
    };
    let back = err.value.expect("new value must be handed back, not lost");
    assert_eq!(back.handle().range().start(), b_off);
    assert_eq!(back.handle().get_val(stack).unwrap(), 2);

    // The OLD child is untouched — still linked in the field (the swap never
    // committed).
    assert_eq!(
        holder
            .handle()
            .get_child(stack)
            .unwrap()
            .get_val(stack)
            .unwrap(),
        1
    );

    // No leak / no double-free: free the handed-back value, then the holder (which
    // frees the still-linked old child `a`).
    back.bstack_drop(&alloc).unwrap();
    holder.bstack_drop(&alloc).unwrap();
}

// The weak setter consumes a `BStackWeak` (its decrement defused, count moved into
// the field). On a commit fault it must RELEASE that count (balancing drop), not
// orphan it — the `io::Result<()>` analogue of the `replace_` hand-back.
#[cfg(feature = "fault-injection")]
#[test]
fn macro_weak_setter_releases_new_weak_on_commit_fault() {
    use bstack::fault::FaultPolicy;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct FailFirstSet(AtomicBool);
    impl FaultPolicy for FailFirstSet {
        fn next_fault(&self, op: &'static str, _seq: u64) -> Option<io::Error> {
            if op == "set" && !self.0.swap(true, Ordering::SeqCst) {
                Some(io::Error::other("injected weak-setter commit fault"))
            } else {
                None
            }
        }
    }

    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let load = |o: u64| crate::refcount::load(stack, o).unwrap();

    let a = WNode::new(&alloc, 1).unwrap(); // strong = 1, weak = 1
    let b = WNode::new(&alloc, 2).unwrap();
    let a_ctrl = load(a.handle().range().start() + layout::CTRL_BACKPTR_OFFSET);
    let weak_off = a_ctrl + layout::CTRL_WEAK_OFFSET;
    assert_eq!(load(weak_off), 1);

    // `downgrade` bumps a's weak count; the setter would move that count into the
    // field. The `set` fault fires on the commit (the read/decrement use `get`/RMW).
    let w = a.downgrade().unwrap();
    assert_eq!(load(weak_off), 2);

    stack.set_fault_policy(Some(Arc::new(FailFirstSet(AtomicBool::new(false)))));
    let r = b.handle().set_back(&alloc, w);
    stack.set_fault_policy(None);
    assert!(
        r.is_err(),
        "injected fault must fail the weak-setter commit"
    );

    // The consumed weak was released, not orphaned: a's weak count is back to 1 …
    assert_eq!(
        load(weak_off),
        1,
        "weak-setter leaked the consumed weak count on a failed commit"
    );
    // … and the field never committed, so it stays unset.
    assert!(b.handle().get_back(&alloc).unwrap().is_none());

    drop(a); // strong 1->0 frees data; weak 1->0 frees control — nothing leaked
    drop(b);
}

#[test]
fn macro_bstack_mut_strong_replace_moves_count_out() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let a = MacroStrongChild::new(&alloc, 1).unwrap(); // BStackRc, strong = 1
    let holder = MutStrong::new(&alloc, a).unwrap(); // count transferred into the field
    assert_eq!(
        holder
            .handle()
            .get_s(stack)
            .unwrap()
            .get_val(stack)
            .unwrap(),
        1
    );

    // Install `b`; the old strong ref (`a`, still count 1) is handed back as a
    // `BStackRc` — the field's count moves out rather than being decremented here.
    let b = MacroStrongChild::new(&alloc, 2).unwrap();
    let old_a = holder.handle().replace_s(&alloc, b).unwrap();
    assert_eq!(
        holder
            .handle()
            .get_s(stack)
            .unwrap()
            .get_val(stack)
            .unwrap(),
        2
    );
    assert_eq!(old_a.handle().get_val(stack).unwrap(), 1);

    // Dropping the returned rc decrements `a` (1 -> 0) and frees it.
    drop(old_a);
    // Tearing down the holder decrements `b` (1 -> 0), freeing it + the shell.
    holder.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// Foreign<T>: cross-file wide pointer resolved through the registry
// --------------------------------------------------------------------------

#[test]
fn foreign_is_zero_cost_16_bytes_and_detach_semantics() {
    use crate::Foreign;
    use crate::registry::FileId;

    // Zero-cost: the `NonZeroU64` file-id niche in the explicit variant encodes the
    // SELF/explicit discriminant for free, so the in-memory form is exactly the 16-byte
    // on-disk wire size.
    assert_eq!(core::mem::size_of::<Foreign<'static, MacroLeaf>>(), 16);

    // An explicit pointer is registry-resolved and borrow-free: it `detach`es to a
    // `'static` `Foreign` that can be stored / moved anywhere.
    let ext = unsafe { Foreign::<MacroLeaf>::new(FileId::from_u64(3).unwrap(), 64) };
    assert!(!ext.is_self());
    assert!(ext.detach().is_some());

    // A SELF pointer is borrow-bound and cannot `detach` (it is only valid within the
    // scope of the file it was read from).
    let selfp = unsafe { Foreign::<MacroLeaf>::new(FileId::SELF, 64) };
    assert!(selfp.is_self());
    assert!(selfp.detach().is_none());
}

#[test]
fn foreign_resolves_across_files_and_self() {
    use crate::Foreign;
    use crate::registry::{FileId, FileRegistry};
    use std::sync::Arc;

    let reg_file = TempStack::new();
    let foreign_file = TempStack::new();
    let local_file = TempStack::new();

    let reg = FileRegistry::open(&reg_file.path).unwrap();
    let local = local_file.allocator();

    // Place a MacroLeaf in the *foreign* file and remember its offset, then hand
    // that file's allocator to the registry as the live host.
    let foreign_alloc = foreign_file.allocator();
    let leaf = MacroLeaf::new(&foreign_alloc, 77).unwrap();
    let off = leaf.handle().range().start();
    let id = reg
        .attach(&foreign_file.path, Arc::new(foreign_alloc))
        .unwrap();

    // A Foreign pointing at that leaf resolves + reads through the registry.
    let fp = unsafe { Foreign::<MacroLeaf>::new(id, off) };
    assert_eq!(
        fp.with_in(&reg, &local, |t, stack| t.get_val(stack).unwrap())
            .unwrap(),
        Some(77)
    );

    // Detaching the host makes resolution fail (not-attached I/O error), not panic.
    reg.detach(id);
    assert_eq!(
        fp.with_in(&reg, &local, |t, stack| t.get_val(stack).unwrap())
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );

    // SELF resolves against `local` directly — no registry entry needed.
    let lleaf = MacroLeaf::new(&local, 9).unwrap();
    let selfp = unsafe { Foreign::<MacroLeaf>::new(FileId::SELF, lleaf.handle().range().start()) };
    assert!(selfp.is_self());
    assert_eq!(
        selfp
            .with_in(&reg, &local, |t, stack| t.get_val(stack).unwrap())
            .unwrap(),
        Some(9)
    );
    lleaf.bstack_drop(&local).unwrap();
}

#[bstack_block]
struct ForeignHolder {
    tag: u32,
    // A cross-file link to an owned block on the other side. `Foreign` is parsed as
    // a token (like `Option`), so the field type needs no `use` of `Foreign` here.
    #[bstack_owned]
    owned_link: Foreign<MacroLeaf>,
    // Nullable cross-file link (`offset == 0` niche); a *ref* target on the far side.
    #[bstack_ref]
    maybe: Option<Foreign<MacroLeaf>>,
}

#[test]
fn macro_foreign_field() {
    use crate::Foreign;
    use crate::registry::FileRegistry;
    use std::sync::Arc;

    let reg_file = TempStack::new();
    let foreign_file = TempStack::new();
    let local_file = TempStack::new();

    let reg = FileRegistry::open(&reg_file.path).unwrap();
    let local = local_file.allocator();
    let stack = local.stack();

    // Target lives in the foreign file; attach that file as its live host.
    let foreign_alloc = foreign_file.allocator();
    let leaf = MacroLeaf::new(&foreign_alloc, 88).unwrap();
    let off = leaf.handle().range().start();
    let id = reg
        .attach(&foreign_file.path, Arc::new(foreign_alloc))
        .unwrap();

    // POD field + an owned cross-file link + a null optional link.
    let h = ForeignHolder::new(
        &local,
        5,
        unsafe { Foreign::<MacroLeaf>::new(id, off) },
        None,
    )
    .unwrap();
    assert_eq!(h.handle().get_tag(stack).unwrap(), 5);
    assert_eq!(
        h.handle()
            .get_owned_link(stack)
            .unwrap()
            .with_in(&reg, &local, |t, fs| t.get_val(fs).unwrap())
            .unwrap(),
        Some(88)
    );
    assert!(h.handle().get_maybe(stack).unwrap().is_none()); // the `None` niche

    // A present optional link resolves like any other Foreign.
    let h2 = ForeignHolder::new(
        &local,
        5,
        unsafe { Foreign::<MacroLeaf>::new(id, off) },
        Some(unsafe { Foreign::<MacroLeaf>::new(id, off) }),
    )
    .unwrap();
    let m = h2.handle().get_maybe(stack).unwrap().expect("Some link");
    assert_eq!(
        m.with_in(&reg, &local, |t, fs| t.get_val(fs).unwrap())
            .unwrap(),
        Some(88)
    );

    // NB: this test exercises construction / accessors / nullability against a
    // *scoped* registry, and deliberately reuses one target `off` across `h` and
    // `h2`. It does NOT tear the holders down: cross-file teardown resolves the
    // *global* registry (the owning `Foreign` would free the shared target — twice),
    // which is covered by the dedicated `macro_foreign_*_teardown_*` tests. The bare
    // `BStackOwned` holders simply drop as inert handles here.
    let _ = (h, h2);
}

#[test]
fn macro_foreign_field_bstack_move() {
    // `bstack_move!` on a block with `Foreign` fields: it frees only the holder shell
    // and hands each field back by value — a foreign link comes out as a resolvable
    // `Foreign<T>` (or `Option<Foreign<T>>`) still pointing at its far-file target. It
    // does NOT run the owning link's cross-file teardown (move defuses teardown), so the
    // target stays live and ownership transfers to the returned pointer.
    use crate::Foreign;
    use crate::registry::FileRegistry;
    use std::sync::Arc;

    let reg_file = TempStack::new();
    let foreign_file = TempStack::new();
    let local_file = TempStack::new();

    let reg = FileRegistry::open(&reg_file.path).unwrap();
    let local = local_file.allocator();

    let foreign_alloc = foreign_file.allocator();
    let leaf = MacroLeaf::new(&foreign_alloc, 88).unwrap();
    let off = leaf.handle().range().start();
    let id = reg
        .attach(&foreign_file.path, Arc::new(foreign_alloc))
        .unwrap();

    let h = ForeignHolder::new(
        &local,
        5,
        unsafe { Foreign::<MacroLeaf>::new(id, off) },
        Some(unsafe { Foreign::<MacroLeaf>::new(id, off) }),
    )
    .unwrap();

    // Fields come back in declaration order: POD, owned link, optional ref link. The
    // `#[bstack_owned]` field yields a `ForeignOwned` (the RAII dual — it owns the
    // target and carries `bstack_drop`/`into_foreign`); the `#[bstack_ref]` field yields
    // a plain `Foreign` (owns nothing).
    let (tag, owned_link, maybe): (
        u32,
        crate::ForeignOwned<MacroLeaf>,
        Option<Foreign<MacroLeaf>>,
    ) = bstack_move!(h, &local).unwrap();

    assert_eq!(tag, 5);
    // The moved-out owned handle resolves (via its pointer) to the live far target.
    assert_eq!(
        owned_link
            .as_foreign()
            .with_in(&reg, &local, |t, fs| t.get_val(fs).unwrap())
            .unwrap(),
        Some(88)
    );
    // The optional ref link too.
    assert_eq!(
        maybe
            .expect("Some link")
            .with_in(&reg, &local, |t, fs| t.get_val(fs).unwrap())
            .unwrap(),
        Some(88)
    );

    // `into_foreign` relinquishes ownership, handing back a plain `Foreign` that still
    // resolves — the value to re-store into another owning field.
    let relinquished = owned_link.into_foreign();
    assert_eq!(
        relinquished
            .with_in(&reg, &local, |t, fs| t.get_val(fs).unwrap())
            .unwrap(),
        Some(88)
    );

    // The move did not free the target — `leaf` is still the live block on the foreign
    // file (inert handle; the temp foreign file is cleaned up at end of test).
    let _ = leaf;
}

// A home block holding a *strong* cross-file reference. `MacroStrongChild` is
// `#[bstack_block(rc, weak)]`, so it is a shared target; the strong Foreign
// participates in its refcount on the far side.
#[bstack_block]
struct ForeignStrongHolder {
    tag: u32,
    #[bstack_strong]
    link: Foreign<MacroStrongChild>,
}

// A home block holding a *weak* cross-file reference. The stored offset is the
// target's CONTROL block (as an in-file weak field stores).
#[bstack_block]
struct ForeignWeakHolder {
    tag: u32,
    #[bstack_weak]
    link: Foreign<MacroStrongChild>,
}

// A home block owning a *vector* of cross-file pointers.
#[bstack_block]
struct ForeignVecHolder {
    tag: u32,
    #[bstack_owned]
    links: Vec<Foreign<MacroLeaf>>,
}

// A home block owning a fixed-size *array* of cross-file pointers.
#[bstack_block]
struct ForeignArrHolder {
    tag: u32,
    #[bstack_owned]
    links: [Foreign<MacroLeaf>; 3],
}

// A home block holding a vector of *strong* cross-file references.
#[bstack_block]
struct ForeignStrongVecHolder {
    tag: u32,
    #[bstack_strong]
    links: Vec<Foreign<MacroStrongChild>>,
}

// -------- Generic foreign: the target is a struct type parameter --------

#[bstack_block]
struct GenForeign<T> {
    tag: u32,
    #[bstack_owned]
    link: Foreign<T>,
}

#[bstack_block]
struct GenForeignVec<T> {
    #[bstack_owned]
    links: Vec<Foreign<T>>,
}

// Generic foreign target inside a tuple (POD element is concrete).
#[bstack_block]
struct GenForeignTup<T> {
    tag: u32,
    #[bstack_owned]
    pair: (u32, Foreign<T>),
}

// Generic foreign target inside an enum variant.
#[bstack_enum]
enum GenForeignEnum<T> {
    Empty,
    #[bstack_owned]
    Far(Foreign<T>),
}

// -------- Cursed-but-VALID foreign container combinations (must compile) --------

// Per-element-`Option` array of 8 owning pointers.
#[bstack_block]
struct CursedArr8 {
    tag: u32,
    #[bstack_owned]
    slots: [Option<Foreign<MacroLeaf>>; 8],
}

// A *nested* array of strong pointers.
#[bstack_block]
struct CursedNestedArr {
    #[bstack_strong]
    grid: [[Foreign<MacroStrongChild>; 2]; 3],
}

// A single block mixing a nullable owned vector-of-pointers, a ref vector-of-pointers,
// a nullable weak scalar pointer, and a plain owned scalar pointer.
#[bstack_block]
struct CursedMix {
    #[bstack_owned]
    maybe_owned: Option<Vec<Foreign<MacroLeaf>>>,
    #[bstack_ref]
    refs: Vec<Foreign<MacroLeaf>>,
    #[bstack_weak]
    maybe_weak: Option<Foreign<MacroStrongChild>>,
    #[bstack_owned]
    one: Foreign<MacroLeaf>,
    // The deep one: a nullable vector of nullable foreign pointers.
    #[bstack_owned]
    deep: Option<Vec<Option<Foreign<MacroLeaf>>>>,
}

// A vector whose elements are *nullable* foreign pointers.
#[bstack_block]
struct OptForeignVecHolder {
    #[bstack_owned]
    links: Vec<Option<Foreign<MacroLeaf>>>,
}

// An enum with POD variants and an owning cross-file variant.
#[bstack_enum]
enum ForeignEnum {
    Nothing,
    Local(u32),
    #[bstack_owned]
    Far(Foreign<MacroLeaf>),
}

// An enum variant holding a *strong* cross-file reference.
#[bstack_enum]
enum ForeignStrongEnum {
    Empty,
    #[bstack_strong]
    S(Foreign<MacroStrongChild>),
}

// Enum variants holding foreign *containers*.
#[bstack_enum]
enum ForeignContainerEnum {
    Empty,
    #[bstack_owned]
    Many(Vec<Foreign<MacroLeaf>>),
    #[bstack_owned]
    Fixed([Foreign<MacroLeaf>; 2]),
}

// An enum variant holding a tuple that mixes POD and (nullable) foreign elements.
#[bstack_enum]
enum ForeignTupEnum {
    Empty,
    #[bstack_owned]
    Pair((u32, Foreign<MacroLeaf>, Option<Foreign<MacroLeaf>>)),
}

// Tuples that mix POD and (nullable) foreign elements. The annotation names the
// foreign elements' ownership.
#[bstack_block]
struct ForeignTupHolder {
    tag: u32,
    #[bstack_owned]
    pair: (u32, Foreign<MacroLeaf>),
    #[bstack_owned]
    maybe: (u16, Option<Foreign<MacroLeaf>>, u8),
}

#[test]
fn macro_foreign_owned_teardown_reclaims_across_files() {
    // Cross-file teardown dispatch (option 1): tearing down a block with a
    // `#[bstack_owned] Foreign<T>` field frees the target **in the target's own
    // file**, resolved through the process-wide registry (adapter → home WAL →
    // `free_recorded` → foreign host). Uses the global registry, like real code.
    use crate::Foreign;
    use crate::registry;

    let home = TempStack::new();
    let home_alloc = home.allocator();

    let foreign = TempStack::new();
    let foreign_alloc = foreign.allocator();

    // Baseline foreign length, then allocate the owned target in the foreign file.
    let base = foreign_alloc.stack().len().unwrap();
    let leaf = MacroLeaf::new(&foreign_alloc, 88).unwrap();
    let off = leaf.handle().range().start();
    let grown = foreign_alloc.stack().len().unwrap();
    assert!(grown > base, "target should have grown the foreign file");

    // Global registry + attach the foreign file (tolerant of a prior init; several
    // tests share the singleton, each attaching its own file → distinct ids).
    let reg_file = TempStack::new();
    let _ = registry::init(&reg_file.path);
    let fid = registry::attach(&foreign.path, foreign_alloc).unwrap();
    assert!(!fid.is_self());

    // A home block owning the foreign target.
    let h = ForeignHolder::new(
        &home_alloc,
        7,
        unsafe { Foreign::<MacroLeaf>::new(fid, off) },
        None,
    )
    .unwrap();

    // Tearing the home block down frees the target across the file boundary.
    h.bstack_drop(&home_alloc).unwrap();

    // The foreign file shrank back to its pre-target length: the target was
    // reclaimed in its own file (a leak would leave it at `grown`).
    let after = registry::with_host(fid, |host| host.stack().len().unwrap()).unwrap();
    assert!(
        after <= base,
        "foreign owned target was not reclaimed on teardown: {after} > {base}"
    );

    registry::detach(fid);
}

#[test]
fn foreign_owned_move_then_bstack_drop_reclaims_across_files() {
    // The RAII dual in action: `bstack_move!` of a `#[bstack_owned] Foreign` hands back
    // a `ForeignOwned`, and its `bstack_drop` frees the target in its own file
    // (registry-resolved) — the safe replacement for the raw `foreign_drop_*` helpers,
    // and the fix for "moving out an owned foreign leaks / needs unsafe".
    use crate::Foreign;
    use crate::registry;

    let home = TempStack::new();
    let home_alloc = home.allocator();
    let foreign = TempStack::new();
    let foreign_alloc = foreign.allocator();

    let base = foreign_alloc.stack().len().unwrap();
    let leaf = MacroLeaf::new(&foreign_alloc, 88).unwrap();
    let off = leaf.handle().range().start();
    let grown = foreign_alloc.stack().len().unwrap();
    assert!(grown > base, "target should have grown the foreign file");

    let reg_file = TempStack::new();
    let _ = registry::init(&reg_file.path);
    let fid = registry::attach(&foreign.path, foreign_alloc).unwrap();

    let h = ForeignHolder::new(
        &home_alloc,
        7,
        unsafe { Foreign::<MacroLeaf>::new(fid, off) },
        None,
    )
    .unwrap();

    // Move the fields out; the `#[bstack_owned]` link returns as a `ForeignOwned`.
    let (_, owned_link, _maybe): (
        u32,
        crate::ForeignOwned<MacroLeaf>,
        Option<Foreign<MacroLeaf>>,
    ) = bstack_move!(h, &home_alloc).unwrap();

    // Dropping the owning handle reclaims the target in its own file — safe, no leak.
    owned_link.bstack_drop(&home_alloc).unwrap();

    let after = registry::with_host(fid, |host| host.stack().len().unwrap()).unwrap();
    assert!(
        after <= base,
        "ForeignOwned::bstack_drop did not reclaim the target: {after} > {base}"
    );

    registry::detach(fid);
    let _ = leaf;
}

#[test]
fn foreign_owned_into_local_owned_self_resolves_and_frees() {
    // A SELF `ForeignOwned` resolves to a plain `BStackOwned` in the same file, which
    // reads and frees against the local allocator — the owning analogue of
    // `Foreign::into_local`, no registry involved.
    use crate::Foreign;
    use crate::registry::FileId;

    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    // Warm the WAL anchor (a one-time per-file allocation) for a stable baseline.
    {
        let g = MacroLeaf::new(&alloc, 0).unwrap();
        g.bstack_drop(&alloc).unwrap();
    }
    let base = alloc.stack().len().unwrap();

    // Allocate the target and hand its ownership to the holder via a SELF foreign
    // (`into_inner` relinquishes the `BStackOwned`, so there is a single owner).
    let leaf = MacroLeaf::new(&alloc, 88).unwrap().into_inner();
    let off = leaf.range().start();
    let h = ForeignHolder::new(
        &alloc,
        3,
        unsafe { Foreign::<MacroLeaf>::new(FileId::SELF, off) },
        None,
    )
    .unwrap();

    // Move out → `ForeignOwned` (SELF) → a plain `BStackOwned` in this file.
    let (_, owned, _): (
        u32,
        crate::ForeignOwned<MacroLeaf>,
        Option<Foreign<MacroLeaf>>,
    ) = bstack_move!(h, &alloc).unwrap();
    assert!(owned.is_self());
    let local = owned.into_local();

    // It reads against the local stack and frees against the local allocator.
    assert_eq!(local.handle().get_val(alloc.stack()).unwrap(), 88);
    local.bstack_drop(&alloc).unwrap();

    assert!(
        alloc.stack().len().unwrap() <= base,
        "into_local + bstack_drop must reclaim the target"
    );
}

#[test]
fn foreign_rc_into_local_rc_self_resolves_and_frees() {
    // A SELF `ForeignRc` resolves (via `strong_parts`) to a live `BStackRc` bound to the
    // local allocator; dropping it drives strong 1 -> 0 and frees the shared block.
    use crate::Foreign;
    use crate::registry::FileId;

    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    {
        let g = MacroLeaf::new(&alloc, 0).unwrap();
        g.bstack_drop(&alloc).unwrap();
    }
    let base = alloc.stack().len().unwrap();

    let data_size = size_of::<<MacroStrongChild as BStackBlock>::OnDisk>() as u64;
    let ctrl_size = size_of::<<MacroStrongChild as BStackWeakable>::Control>() as u64;
    let data = alloc_block(&alloc, MacroStrongChild::eightcc(), data_size).unwrap();
    let ctrl = alloc_control(&alloc, ctrl_tag(), data, ctrl_size).unwrap();
    let data_off = data.start();
    let strong_off = ctrl.start() + layout::CTRL_STRONG_OFFSET;
    assert_eq!(crate::refcount::load(alloc.stack(), strong_off).unwrap(), 1);

    // The holder adopts the initial strong = 1 via a SELF foreign.
    let h = ForeignStrongHolder::new(&alloc, 1, unsafe {
        Foreign::<MacroStrongChild>::new(FileId::SELF, data_off)
    })
    .unwrap();

    let (_, rc): (u32, crate::ForeignRc<MacroStrongChild>) = bstack_move!(h, &alloc).unwrap();
    assert!(rc.is_self());
    // Resolve to a live in-file `BStackRc`; dropping it (it auto-frees) drives
    // strong 1 -> 0 and reclaims data + control.
    let local = rc.into_local(&alloc).unwrap();
    drop(local);

    assert!(
        alloc.stack().len().unwrap() <= base,
        "into_local + drop must reclaim the shared target"
    );
}

#[test]
fn foreign_weak_into_local_weak_self_decrements() {
    // A SELF `ForeignWeak` resolves to a live `BStackWeak`; dropping it (auto-free)
    // decrements the target's weak count in the local file.
    use crate::Foreign;
    use crate::registry::FileId;

    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    let data_size = size_of::<<MacroStrongChild as BStackBlock>::OnDisk>() as u64;
    let ctrl_size = size_of::<<MacroStrongChild as BStackWeakable>::Control>() as u64;
    let data = alloc_block(&alloc, MacroStrongChild::eightcc(), data_size).unwrap();
    let ctrl = alloc_control(&alloc, ctrl_tag(), data, ctrl_size).unwrap();
    let ctrl_off = ctrl.start();
    let weak_off = ctrl_off + layout::CTRL_WEAK_OFFSET;
    // `alloc_control` leaves weak = 1 (the strong owners' phantom); add one for the
    // holder we are about to create (construction does not bump).
    crate::refcount::fetch_add(alloc.stack(), weak_off, 1).unwrap();
    let load = |o: u64| crate::refcount::load(alloc.stack(), o).unwrap();
    assert_eq!(load(weak_off), 2);

    // A weak foreign holder points at the CONTROL block.
    let h = ForeignWeakHolder::new(&alloc, 1, unsafe {
        Foreign::<MacroStrongChild>::new(FileId::SELF, ctrl_off)
    })
    .unwrap();

    let (_, weak): (u32, crate::ForeignWeak<MacroStrongChild>) = bstack_move!(h, &alloc).unwrap();
    assert!(weak.is_self());
    // Resolve to a live in-file `BStackWeak`; dropping it decrements weak 2 -> 1.
    let local = weak.into_local(&alloc);
    drop(local);
    assert_eq!(
        load(weak_off),
        1,
        "into_local + drop must decrement the weak count"
    );
}

#[test]
fn macro_foreign_strong_teardown_frees_at_zero_across_files() {
    // Cross-file RC teardown: a `#[bstack_strong] Foreign<T>` decrements the target's
    // strong count *in the target's own file* (via the foreign host's stack + the
    // atomic refcount primitives), and frees data + control when it hits zero.
    use crate::Foreign;
    use crate::registry;

    let home = TempStack::new();
    let home_alloc = home.allocator();

    let foreign = TempStack::new();
    let foreign_alloc = foreign.allocator();

    let data_size = size_of::<<MacroStrongChild as BStackBlock>::OnDisk>() as u64;
    let ctrl_size = size_of::<<MacroStrongChild as BStackWeakable>::Control>() as u64;

    // A shared target in the foreign file: strong = 1, weak = 1, back-pointer wired.
    let base = foreign_alloc.stack().len().unwrap();
    let data = alloc_block(&foreign_alloc, MacroStrongChild::eightcc(), data_size).unwrap();
    let ctrl = alloc_control(&foreign_alloc, ctrl_tag(), data, ctrl_size).unwrap();
    let data_off = data.start();
    let strong_off = ctrl.start() + layout::CTRL_STRONG_OFFSET;
    assert_eq!(
        crate::refcount::load(foreign_alloc.stack(), strong_off).unwrap(),
        1
    );
    let grown = foreign_alloc.stack().len().unwrap();
    assert!(grown > base);

    let reg_file = TempStack::new();
    let _ = registry::init(&reg_file.path);
    let fid = registry::attach(&foreign.path, foreign_alloc).unwrap();

    // The home block is the sole strong owner (count is 1) across the file boundary.
    let h = ForeignStrongHolder::new(&home_alloc, 1, unsafe {
        Foreign::<MacroStrongChild>::new(fid, data_off)
    })
    .unwrap();

    // Teardown drives the far-side strong count 1 -> 0, freeing data + control there.
    h.bstack_drop(&home_alloc).unwrap();

    let after = registry::with_host(fid, |host| host.stack().len().unwrap()).unwrap();
    assert!(
        after <= base,
        "foreign strong target not freed at zero: {after} > {base}"
    );

    registry::detach(fid);
}

#[test]
fn macro_foreign_concurrent_ab_ba_teardown() {
    // The AB-BA stress: objects on file A own targets on file B, objects on B own
    // targets on A, and BOTH directions are torn down concurrently. This is the
    // cross-file analogue of the same-file concurrent-teardown race that the WAL
    // mutex was introduced to fix. It exercises the whole cross-file teardown
    // locking story — each teardown's WAL transaction on its *home* file (per-file
    // mutex) + the registry read lock + plain `dealloc`s into the *other* file — for
    // deadlock, double-free, and FirstFit free-list corruption. Completion (no hang)
    // ⇒ no deadlock; full reclamation each round ⇒ no leak / no double-free.
    use crate::Foreign;
    use crate::registry;
    use std::sync::Arc;
    use std::thread;

    let fa = TempStack::new();
    let fb = TempStack::new();
    // One allocator per file, shared as an `Arc` between direct home teardowns
    // (`&**arc`) and the registry's cross-file resolution — the SAME instance on both
    // sides, so there is no illegal double-open of a file.
    let arc_a = Arc::new(fa.allocator());
    let arc_b = Arc::new(fb.allocator());

    let reg_file = TempStack::new();
    let _ = registry::init(&reg_file.path);
    let reg = registry::get().unwrap();
    let fid_a = reg.attach(&fa.path, arc_a.clone()).unwrap();
    let fid_b = reg.attach(&fb.path, arc_b.clone()).unwrap();

    // Warm + size both persistent WAL blocks (one holder each way), so the reclaimed
    // baseline already accounts for them; then record it.
    {
        let bl = MacroLeaf::new(&*arc_b, 1).unwrap();
        ForeignHolder::new(
            &*arc_a,
            0,
            unsafe { Foreign::<MacroLeaf>::new(fid_b, bl.handle().range().start()) },
            None,
        )
        .unwrap()
        .bstack_drop(&*arc_a)
        .unwrap();
        let al = MacroLeaf::new(&*arc_a, 1).unwrap();
        ForeignHolder::new(
            &*arc_b,
            0,
            unsafe { Foreign::<MacroLeaf>::new(fid_a, al.handle().range().start()) },
            None,
        )
        .unwrap()
        .bstack_drop(&*arc_b)
        .unwrap();
    }
    let base_a = arc_a.stack().len().unwrap();
    let base_b = arc_b.stack().len().unwrap();

    const N: usize = 48;
    const ROUNDS: usize = 3;
    const THREADS: usize = 4;
    for _ in 0..ROUNDS {
        // N A-holders (each owns a DISTINCT leaf on B) + N B-holders (each a distinct
        // leaf on A). Distinct objects ⇒ disjoint free sets across the two directions.
        let mut a_holders = Vec::with_capacity(N);
        let mut b_holders = Vec::with_capacity(N);
        for i in 0..N as u32 {
            let bl = MacroLeaf::new(&*arc_b, i).unwrap();
            a_holders.push(
                ForeignHolder::new(
                    &*arc_a,
                    i,
                    unsafe { Foreign::<MacroLeaf>::new(fid_b, bl.handle().range().start()) },
                    None,
                )
                .unwrap()
                .into_inner(),
            );
            let al = MacroLeaf::new(&*arc_a, i).unwrap();
            b_holders.push(
                ForeignHolder::new(
                    &*arc_b,
                    i,
                    unsafe { Foreign::<MacroLeaf>::new(fid_a, al.handle().range().start()) },
                    None,
                )
                .unwrap()
                .into_inner(),
            );
        }

        // Tear both directions down at once: A-holder threads free into B while
        // B-holder threads free into A — the AB-BA contention.
        let chunk = N.div_ceil(THREADS);
        thread::scope(|s| {
            let arc_a = &arc_a;
            let arc_b = &arc_b;
            for part in a_holders.chunks(chunk) {
                let part = part.to_vec();
                s.spawn(move || {
                    for h in part {
                        h.bstack_drop(&**arc_a).unwrap();
                    }
                });
            }
            for part in b_holders.chunks(chunk) {
                let part = part.to_vec();
                s.spawn(move || {
                    for h in part {
                        h.bstack_drop(&**arc_b).unwrap();
                    }
                });
            }
        });

        // Both files returned exactly to baseline: every holder shell AND every
        // cross-file target was reclaimed, with no leak and no corruption.
        assert_eq!(
            arc_a.stack().len().unwrap(),
            base_a,
            "file A not fully reclaimed after concurrent AB-BA teardown"
        );
        assert_eq!(
            arc_b.stack().len().unwrap(),
            base_b,
            "file B not fully reclaimed after concurrent AB-BA teardown"
        );
    }

    reg.detach(fid_a);
    reg.detach(fid_b);
}

#[test]
fn macro_foreign_owned_clone_deep_copies_across_files() {
    // Cross-file deep clone: cloning a block with a `#[bstack_owned] Foreign<T>` field
    // deep-copies the target INTO ITS OWN FILE (a fresh block, the pointer repointed),
    // so the clone is independent — tearing both down frees both copies, no double-free.
    use crate::registry;
    use crate::{Foreign, TryCloneIn};
    use std::sync::Arc;

    let home = TempStack::new();
    let home_alloc = home.allocator();
    let hstack = home_alloc.stack();

    let foreign = TempStack::new();
    // Keep the foreign allocator as an Arc so we can both build typed leaves on it
    // (`&*arc_b`) and attach it for cross-file resolution (same instance).
    let arc_b = Arc::new(foreign.allocator());

    let reg_file = TempStack::new();
    let _ = registry::init(&reg_file.path);
    let reg = registry::get().unwrap();
    let fid = reg.attach(&foreign.path, arc_b.clone()).unwrap();

    // Warm the owned-clone path once (this creates B's persistent WAL block, since the
    // cross-file deep copy runs a WAL-backed clone on B); then record the baseline.
    {
        let l0 = MacroLeaf::new(&*arc_b, 0).unwrap();
        let h0 = ForeignHolder::new(
            &home_alloc,
            0,
            unsafe { Foreign::<MacroLeaf>::new(fid, l0.handle().range().start()) },
            None,
        )
        .unwrap();
        let c0 = h0.handle().try_clone_in(&home_alloc).unwrap();
        h0.bstack_drop(&home_alloc).unwrap();
        c0.bstack_drop(&home_alloc).unwrap();
    }
    let base_b = arc_b.stack().len().unwrap();

    // The real target + home holder owning it.
    let leaf = MacroLeaf::new(&*arc_b, 42).unwrap();
    let off = leaf.handle().range().start();
    let h = ForeignHolder::new(
        &home_alloc,
        7,
        unsafe { Foreign::<MacroLeaf>::new(fid, off) },
        None,
    )
    .unwrap();

    // Deep clone the home holder: its owned foreign target is copied on B.
    let c = h.handle().try_clone_in(&home_alloc).unwrap();

    // The clone points at a DIFFERENT block on B (a fresh copy, not an alias)…
    let clone_link = c.handle().get_owned_link(hstack).unwrap();
    assert_eq!(clone_link.file_id(), fid);
    assert_ne!(
        clone_link.offset(),
        off,
        "owned clone must be a fresh copy, not an alias"
    );
    // …carrying the same value (a genuine deep copy).
    assert_eq!(
        clone_link
            .with(&home_alloc, |t, fs| t.get_val(fs).unwrap())
            .unwrap()
            .unwrap(),
        42
    );

    // Independence: tearing both holders down frees BOTH leaves on B (no double-free,
    // no leak) — the file returns exactly to the warmed baseline.
    h.bstack_drop(&home_alloc).unwrap();
    c.bstack_drop(&home_alloc).unwrap();
    assert_eq!(
        arc_b.stack().len().unwrap(),
        base_b,
        "clone+teardown leaked or double-freed on the foreign file"
    );

    reg.detach(fid);
}

#[test]
fn macro_foreign_owned_clone_on_bulk_home_copies_once() {
    // The foreign guard under the two-pass clone: when the HOME allocator is bulk
    // (GhostTree), cloning a `#[bstack_owned] Foreign<T>` runs the measure->build
    // descent twice. The cross-file deep-copy is eager and must be BUILD-ONLY — if
    // the measure pass also ran it, the foreign file would get TWO copies (a leak).
    // Assert the foreign file returns exactly to baseline, proving it was copied once.
    use crate::registry;
    use crate::{Foreign, TryCloneIn};
    use std::sync::Arc;

    let home = TempStack::new();
    let home_alloc = home.ghost_allocator(); // bulk => two-pass clone
    let hstack = home_alloc.stack();

    let foreign = TempStack::new();
    let arc_b = Arc::new(foreign.allocator());

    let reg_file = TempStack::new();
    let _ = registry::init(&reg_file.path);
    let reg = registry::get().unwrap();
    let fid = reg.attach(&foreign.path, arc_b.clone()).unwrap();

    // Warm B's WAL block via one owned-clone cycle, then record B's baseline length.
    {
        let l0 = MacroLeaf::new(&*arc_b, 0).unwrap();
        let h0 = ForeignHolder::new(
            &home_alloc,
            0,
            unsafe { Foreign::<MacroLeaf>::new(fid, l0.handle().range().start()) },
            None,
        )
        .unwrap();
        h0.handle()
            .try_clone_in(&home_alloc)
            .unwrap()
            .bstack_drop(&home_alloc)
            .unwrap();
        h0.bstack_drop(&home_alloc).unwrap();
    }
    let base_b = arc_b.stack().len().unwrap();

    let leaf = MacroLeaf::new(&*arc_b, 42).unwrap();
    let off = leaf.handle().range().start();
    let h = ForeignHolder::new(
        &home_alloc,
        7,
        unsafe { Foreign::<MacroLeaf>::new(fid, off) },
        None,
    )
    .unwrap();

    // Two-pass clone on the bulk home allocator.
    let c = h.handle().try_clone_in(&home_alloc).unwrap();

    // A single fresh copy on B, carrying the value.
    let clone_link = c.handle().get_owned_link(hstack).unwrap();
    assert_eq!(clone_link.file_id(), fid);
    assert_ne!(
        clone_link.offset(),
        off,
        "must be a fresh copy, not an alias"
    );
    assert_eq!(
        clone_link
            .with(&home_alloc, |t, fs| t.get_val(fs).unwrap())
            .unwrap()
            .unwrap(),
        42
    );

    h.bstack_drop(&home_alloc).unwrap();
    c.bstack_drop(&home_alloc).unwrap();
    assert_eq!(
        arc_b.stack().len().unwrap(),
        base_b,
        "measure pass double-cloned the foreign target (guard missing/broken)"
    );

    reg.detach(fid);
}

#[test]
fn macro_foreign_strong_clone_bumps_count_across_files() {
    // Cross-file strong clone: cloning a `#[bstack_strong] Foreign<T>` shares the same
    // target and bumps its strong count on the far side; both clones releasing it
    // (teardown) drive it back to zero and free it.
    use crate::registry;
    use crate::{Foreign, TryCloneIn};
    use std::sync::Arc;

    let home = TempStack::new();
    let home_alloc = home.allocator();

    let foreign = TempStack::new();
    let arc_b = Arc::new(foreign.allocator());

    let data_size = size_of::<<MacroStrongChild as BStackBlock>::OnDisk>() as u64;
    let ctrl_size = size_of::<<MacroStrongChild as BStackWeakable>::Control>() as u64;

    let base = arc_b.stack().len().unwrap();
    let data = alloc_block(&*arc_b, MacroStrongChild::eightcc(), data_size).unwrap();
    let ctrl = alloc_control(&*arc_b, ctrl_tag(), data, ctrl_size).unwrap();
    let data_off = data.start();
    let strong_off = ctrl.start() + layout::CTRL_STRONG_OFFSET;

    let reg_file = TempStack::new();
    let _ = registry::init(&reg_file.path);
    let reg = registry::get().unwrap();
    let fid = reg.attach(&foreign.path, arc_b.clone()).unwrap();

    let load = |o: u64| crate::refcount::load(arc_b.stack(), o).unwrap();
    assert_eq!(load(strong_off), 1);

    // One strong owner across the boundary; cloning it makes two.
    let h = ForeignStrongHolder::new(&home_alloc, 1, unsafe {
        Foreign::<MacroStrongChild>::new(fid, data_off)
    })
    .unwrap();
    let c = h.handle().try_clone_in(&home_alloc).unwrap();
    assert_eq!(
        load(strong_off),
        2,
        "strong clone should bump the far count"
    );

    // Both owners releasing drives the count to zero and frees the target.
    h.bstack_drop(&home_alloc).unwrap();
    assert_eq!(load(strong_off), 1);
    c.bstack_drop(&home_alloc).unwrap();
    assert_eq!(
        arc_b.stack().len().unwrap(),
        base,
        "target should be reclaimed once both strong owners drop"
    );

    reg.detach(fid);
}

#[test]
fn macro_foreign_weak_clone_bumps_count_across_files() {
    // Cross-file weak clone: cloning a `#[bstack_weak] Foreign<T>` shares the same
    // control block and bumps its weak count on the far side; teardown decrements it.
    use crate::registry;
    use crate::{Foreign, TryCloneIn};
    use std::sync::Arc;

    let home = TempStack::new();
    let home_alloc = home.allocator();

    let foreign = TempStack::new();
    let arc_b = Arc::new(foreign.allocator());

    let data_size = size_of::<<MacroStrongChild as BStackBlock>::OnDisk>() as u64;
    let ctrl_size = size_of::<<MacroStrongChild as BStackWeakable>::Control>() as u64;
    let data = alloc_block(&*arc_b, MacroStrongChild::eightcc(), data_size).unwrap();
    let ctrl = alloc_control(&*arc_b, ctrl_tag(), data, ctrl_size).unwrap();
    let ctrl_off = ctrl.start();
    let weak_off = ctrl_off + layout::CTRL_WEAK_OFFSET;
    // alloc_control leaves strong=1, weak=1 (the phantom the strong owners hold). Add
    // one weak for the holder we are about to create (construction does not bump).
    crate::refcount::fetch_add(arc_b.stack(), weak_off, 1).unwrap();

    let reg_file = TempStack::new();
    let _ = registry::init(&reg_file.path);
    let reg = registry::get().unwrap();
    let fid = reg.attach(&foreign.path, arc_b.clone()).unwrap();

    let load = |o: u64| crate::refcount::load(arc_b.stack(), o).unwrap();
    assert_eq!(load(weak_off), 2); // phantom + holder

    // A weak foreign holder points at the CONTROL block; cloning it bumps weak.
    let h = ForeignWeakHolder::new(&home_alloc, 1, unsafe {
        Foreign::<MacroStrongChild>::new(fid, ctrl_off)
    })
    .unwrap();
    let c = h.handle().try_clone_in(&home_alloc).unwrap();
    assert_eq!(
        load(weak_off),
        3,
        "weak clone should bump the far weak count"
    );

    // Both weak owners releasing brings it back down (the phantom keeps it alive).
    h.bstack_drop(&home_alloc).unwrap();
    c.bstack_drop(&home_alloc).unwrap();
    assert_eq!(
        load(weak_off),
        1,
        "weak teardown should decrement the far count"
    );

    reg.detach(fid);
}

#[test]
fn macro_foreign_concurrent_ab_ba_clone() {
    // AB-BA stress for CLONE. Unlike teardown (whose cross-file frees are plain
    // deallocs), a cross-file owned clone runs a WAL-backed `try_clone_in` on the
    // TARGET file — so it takes the *target's* WAL mutex, then its *home* file's WAL
    // mutex on commit. This drives clones both ways concurrently to confirm those two
    // acquisitions never cycle (they're sequential, not nested) and that every deep
    // copy is independent (no double-free / leak on the ensuing teardown).
    use crate::registry;
    use crate::{Foreign, TryCloneIn};
    use std::sync::Arc;
    use std::thread;

    let fa = TempStack::new();
    let fb = TempStack::new();
    let arc_a = Arc::new(fa.allocator());
    let arc_b = Arc::new(fb.allocator());

    let reg_file = TempStack::new();
    let _ = registry::init(&reg_file.path);
    let reg = registry::get().unwrap();
    let fid_a = reg.attach(&fa.path, arc_a.clone()).unwrap();
    let fid_b = reg.attach(&fb.path, arc_b.clone()).unwrap();

    // Warm both files' WAL blocks (each is a clone home AND a cross-file clone target),
    // then baseline.
    for (ha, hb, tgt) in [(&arc_a, &arc_b, fid_b), (&arc_b, &arc_a, fid_a)] {
        let l = MacroLeaf::new(&**hb, 0).unwrap();
        let h = ForeignHolder::new(
            &**ha,
            0,
            unsafe { Foreign::<MacroLeaf>::new(tgt, l.handle().range().start()) },
            None,
        )
        .unwrap();
        h.handle()
            .try_clone_in(&**ha)
            .unwrap()
            .bstack_drop(&**ha)
            .unwrap();
        h.bstack_drop(&**ha).unwrap();
    }
    let base_a = arc_a.stack().len().unwrap();
    let base_b = arc_b.stack().len().unwrap();

    const N: usize = 32;
    const THREADS: usize = 4;
    let mut a_orig = Vec::with_capacity(N);
    let mut b_orig = Vec::with_capacity(N);
    for i in 0..N as u32 {
        let bl = MacroLeaf::new(&*arc_b, i).unwrap();
        a_orig.push(
            ForeignHolder::new(
                &*arc_a,
                i,
                unsafe { Foreign::<MacroLeaf>::new(fid_b, bl.handle().range().start()) },
                None,
            )
            .unwrap()
            .into_inner(),
        );
        let al = MacroLeaf::new(&*arc_a, i).unwrap();
        b_orig.push(
            ForeignHolder::new(
                &*arc_b,
                i,
                unsafe { Foreign::<MacroLeaf>::new(fid_a, al.handle().range().start()) },
                None,
            )
            .unwrap()
            .into_inner(),
        );
    }

    // Clone both directions at once: A-holder clones deep-copy into B (taking B's WAL
    // mutex) while B-holder clones deep-copy into A.
    let chunk = N.div_ceil(THREADS);
    let (a_clones, b_clones) = thread::scope(|s| {
        let arc_a = &arc_a;
        let arc_b = &arc_b;
        let mut ja = Vec::new();
        let mut jb = Vec::new();
        for part in a_orig.chunks(chunk) {
            let part = part.to_vec();
            ja.push(s.spawn(move || {
                part.iter()
                    .map(|h| h.try_clone_in(&**arc_a).unwrap().into_inner())
                    .collect::<Vec<_>>()
            }));
        }
        for part in b_orig.chunks(chunk) {
            let part = part.to_vec();
            jb.push(s.spawn(move || {
                part.iter()
                    .map(|h| h.try_clone_in(&**arc_b).unwrap().into_inner())
                    .collect::<Vec<_>>()
            }));
        }
        let a_clones: Vec<_> = ja.into_iter().flat_map(|j| j.join().unwrap()).collect();
        let b_clones: Vec<_> = jb.into_iter().flat_map(|j| j.join().unwrap()).collect();
        (a_clones, b_clones)
    });
    assert_eq!(a_clones.len(), N);
    assert_eq!(b_clones.len(), N);

    // Tear down originals + clones. Every original leaf AND every independent deep copy
    // is reclaimed ⇒ both files return exactly to baseline (no leak, no double-free);
    // completion ⇒ no deadlock across the two WAL mutexes.
    for h in a_orig.into_iter().chain(a_clones) {
        h.bstack_drop(&*arc_a).unwrap();
    }
    for h in b_orig.into_iter().chain(b_clones) {
        h.bstack_drop(&*arc_b).unwrap();
    }
    assert_eq!(
        arc_a.stack().len().unwrap(),
        base_a,
        "file A not fully reclaimed after concurrent AB-BA clone"
    );
    assert_eq!(
        arc_b.stack().len().unwrap(),
        base_b,
        "file B not fully reclaimed after concurrent AB-BA clone"
    );

    reg.detach(fid_a);
    reg.detach(fid_b);
}

#[test]
fn macro_foreign_vec_owned_across_files() {
    // `#[bstack_owned] Vec<Foreign<T>>`: each element owns a cross-file target.
    // Construction/access map to `Foreign<T>`; clone deep-copies EVERY element on the
    // far side; teardown frees every element there.
    use crate::registry;
    use crate::{Foreign, TryCloneIn};
    use std::sync::Arc;

    let home = TempStack::new();
    let home_alloc = home.allocator();

    let foreign = TempStack::new();
    let arc_b = Arc::new(foreign.allocator());

    let reg_file = TempStack::new();
    let _ = registry::init(&reg_file.path);
    let reg = registry::get().unwrap();
    let fid = reg.attach(&foreign.path, arc_b.clone()).unwrap();

    // Warm the owned-vec clone path (creates B's WAL block), then baseline.
    {
        let l = MacroLeaf::new(&*arc_b, 0).unwrap();
        let h = ForeignVecHolder::new(
            &home_alloc,
            0,
            vec![unsafe { Foreign::<MacroLeaf>::new(fid, l.handle().range().start()) }],
        )
        .unwrap();
        let c = h.handle().try_clone_in(&home_alloc).unwrap();
        c.bstack_drop(&home_alloc).unwrap();
        h.bstack_drop(&home_alloc).unwrap();
    }
    let base = arc_b.stack().len().unwrap();

    // N owned foreign targets on B.
    const N: u32 = 5;
    let mut links = Vec::new();
    for i in 0..N {
        let l = MacroLeaf::new(&*arc_b, 100 + i).unwrap();
        links.push(unsafe { Foreign::<MacroLeaf>::new(fid, l.handle().range().start()) });
    }
    let h = ForeignVecHolder::new(&home_alloc, 7, links).unwrap();

    // Accessor yields N `Foreign`s resolving to the right values.
    let got = h.handle().get_links(&home_alloc).unwrap();
    assert_eq!(got.len(), N as usize);
    for (i, f) in got.iter().enumerate() {
        assert_eq!(
            f.with(&home_alloc, |t, fs| t.get_val(fs).unwrap())
                .unwrap()
                .unwrap(),
            100 + i as u32
        );
    }

    // Deep clone: every element is copied to a fresh block on B (different offsets),
    // same values.
    let c = h.handle().try_clone_in(&home_alloc).unwrap();
    let clinks = c.handle().get_links(&home_alloc).unwrap();
    assert_eq!(clinks.len(), N as usize);
    for (o, n) in got.iter().zip(clinks.iter()) {
        assert_ne!(o.offset(), n.offset(), "each element must be a fresh copy");
        assert_eq!(
            n.with(&home_alloc, |t, fs| t.get_val(fs).unwrap())
                .unwrap()
                .unwrap(),
            o.with(&home_alloc, |t, fs| t.get_val(fs).unwrap())
                .unwrap()
                .unwrap()
        );
    }

    // Tearing both down frees all 2N targets on B → back to baseline.
    h.bstack_drop(&home_alloc).unwrap();
    c.bstack_drop(&home_alloc).unwrap();
    assert_eq!(
        arc_b.stack().len().unwrap(),
        base,
        "foreign-vec clone/teardown leaked or double-freed on B"
    );

    reg.detach(fid);
}

#[test]
fn macro_foreign_array_owned_across_files() {
    // `#[bstack_owned] [Foreign<T>; N]`: an inline fixed array of owning cross-file
    // pointers. Same per-element teardown / clone as the vector, but stored inline.
    use crate::registry;
    use crate::{Foreign, TryCloneIn};
    use std::sync::Arc;

    let home = TempStack::new();
    let home_alloc = home.allocator();
    let hstack = home_alloc.stack();

    let foreign = TempStack::new();
    let arc_b = Arc::new(foreign.allocator());

    let reg_file = TempStack::new();
    let _ = registry::init(&reg_file.path);
    let reg = registry::get().unwrap();
    let fid = reg.attach(&foreign.path, arc_b.clone()).unwrap();

    let mk = |v: u32| {
        let l = MacroLeaf::new(&*arc_b, v).unwrap();
        unsafe { Foreign::<MacroLeaf>::new(fid, l.handle().range().start()) }
    };

    // Warm the owned-array clone path (creates B's WAL block), baseline.
    {
        let h = ForeignArrHolder::new(&home_alloc, 0, [mk(0), mk(0), mk(0)]).unwrap();
        let c = h.handle().try_clone_in(&home_alloc).unwrap();
        c.bstack_drop(&home_alloc).unwrap();
        h.bstack_drop(&home_alloc).unwrap();
    }
    let base = arc_b.stack().len().unwrap();

    let h = ForeignArrHolder::new(&home_alloc, 7, [mk(10), mk(20), mk(30)]).unwrap();
    let got = h.handle().get_links(hstack).unwrap();
    let vals: Vec<u32> = got
        .iter()
        .map(|f| {
            f.with(&home_alloc, |t, fs| t.get_val(fs).unwrap())
                .unwrap()
                .unwrap()
        })
        .collect();
    assert_eq!(vals, vec![10, 20, 30]);

    // Deep clone: every slot copied to a fresh block on B, same values.
    let c = h.handle().try_clone_in(&home_alloc).unwrap();
    let clinks = c.handle().get_links(hstack).unwrap();
    for (o, n) in got.iter().zip(clinks.iter()) {
        assert_ne!(o.offset(), n.offset(), "each slot must be a fresh copy");
    }
    let cvals: Vec<u32> = clinks
        .iter()
        .map(|f| {
            f.with(&home_alloc, |t, fs| t.get_val(fs).unwrap())
                .unwrap()
                .unwrap()
        })
        .collect();
    assert_eq!(cvals, vec![10, 20, 30]);

    h.bstack_drop(&home_alloc).unwrap();
    c.bstack_drop(&home_alloc).unwrap();
    assert_eq!(
        arc_b.stack().len().unwrap(),
        base,
        "foreign-array clone/teardown leaked or double-freed on B"
    );

    reg.detach(fid);
}

#[test]
fn macro_foreign_strong_vec_across_files() {
    // `#[bstack_strong] Vec<Foreign<T>>`: cloning bumps EVERY element's strong count
    // on the far side; teardown decrements each. (Counts checked directly.)
    use crate::registry;
    use crate::{Foreign, TryCloneIn};
    use std::sync::Arc;

    let home = TempStack::new();
    let home_alloc = home.allocator();

    let foreign = TempStack::new();
    let arc_b = Arc::new(foreign.allocator());

    let ds = size_of::<<MacroStrongChild as BStackBlock>::OnDisk>() as u64;
    let cs = size_of::<<MacroStrongChild as BStackWeakable>::Control>() as u64;

    let reg_file = TempStack::new();
    let _ = registry::init(&reg_file.path);
    let reg = registry::get().unwrap();
    let fid = reg.attach(&foreign.path, arc_b.clone()).unwrap();

    // 3 shared targets on B, strong = 1 each.
    let mut links = Vec::new();
    let mut strong_offs = Vec::new();
    for _ in 0..3 {
        let d = alloc_block(&*arc_b, MacroStrongChild::eightcc(), ds).unwrap();
        let c = alloc_control(&*arc_b, ctrl_tag(), d, cs).unwrap();
        strong_offs.push(c.start() + layout::CTRL_STRONG_OFFSET);
        links.push(unsafe { Foreign::<MacroStrongChild>::new(fid, d.start()) });
    }
    let load = |o: u64| crate::refcount::load(arc_b.stack(), o).unwrap();
    for &o in &strong_offs {
        assert_eq!(load(o), 1);
    }

    let h = ForeignStrongVecHolder::new(&home_alloc, 1, links).unwrap();
    let c = h.handle().try_clone_in(&home_alloc).unwrap();
    for &o in &strong_offs {
        assert_eq!(load(o), 2, "each strong vec element should bump on clone");
    }

    // h releases one ref per element; the clone still holds the other.
    h.bstack_drop(&home_alloc).unwrap();
    for &o in &strong_offs {
        assert_eq!(
            load(o),
            1,
            "each element should drop to 1 after one owner tears down"
        );
    }
    // The clone releasing drives each to zero and frees the targets (no panic).
    c.bstack_drop(&home_alloc).unwrap();

    reg.detach(fid);
}

#[test]
fn macro_foreign_generic_across_files() {
    // A `Foreign<T>` over a struct type parameter `T`: the macro derives `T:
    // BStackBlock (+ TryCloneIn for owned)` on the generated impls, so a generic block
    // deep-clones / tears down its foreign target exactly like a concrete one.
    use crate::registry;
    use crate::{Foreign, TryCloneIn};
    use std::sync::Arc;

    let home = TempStack::new();
    let home_alloc = home.allocator();
    let hstack = home_alloc.stack();

    let foreign = TempStack::new();
    let arc_b = Arc::new(foreign.allocator());
    let reg_file = TempStack::new();
    let _ = registry::init(&reg_file.path);
    let reg = registry::get().unwrap();
    let fid = reg.attach(&foreign.path, arc_b.clone()).unwrap();

    // Warm B's WAL block via one owned-clone cycle, then baseline.
    {
        let l = MacroLeaf::new(&*arc_b, 0).unwrap();
        let h = GenForeign::<MacroLeaf>::new(&home_alloc, 0, unsafe {
            Foreign::new(fid, l.handle().range().start())
        })
        .unwrap();
        let c = h.handle().try_clone_in(&home_alloc).unwrap();
        c.bstack_drop(&home_alloc).unwrap();
        h.bstack_drop(&home_alloc).unwrap();
    }
    let base = arc_b.stack().len().unwrap();

    let l = MacroLeaf::new(&*arc_b, 55).unwrap();
    let off = l.handle().range().start();
    let h =
        GenForeign::<MacroLeaf>::new(&home_alloc, 7, unsafe { Foreign::new(fid, off) }).unwrap();

    // Access resolves the generic foreign target.
    let link = h.handle().get_link(hstack).unwrap();
    assert_eq!(
        link.with(&home_alloc, |t, fs| t.get_val(fs).unwrap())
            .unwrap()
            .unwrap(),
        55
    );

    // Deep clone copies the target on B (fresh offset, same value).
    let c = h.handle().try_clone_in(&home_alloc).unwrap();
    let clink = c.handle().get_link(hstack).unwrap();
    assert_ne!(clink.offset(), off);
    assert_eq!(
        clink
            .with(&home_alloc, |t, fs| t.get_val(fs).unwrap())
            .unwrap()
            .unwrap(),
        55
    );

    // Teardown both → both leaves reclaimed → baseline.
    h.bstack_drop(&home_alloc).unwrap();
    c.bstack_drop(&home_alloc).unwrap();
    assert_eq!(arc_b.stack().len().unwrap(), base);

    // The generic vector form compiles + tears down (empty ⇒ self-contained).
    let gv = GenForeignVec::<MacroLeaf>::new(&home_alloc, vec![]).unwrap();
    assert!(gv.handle().get_links(&home_alloc).unwrap().is_empty());
    gv.bstack_drop(&home_alloc).unwrap();

    reg.detach(fid);
}

#[test]
fn macro_foreign_cursed_valid_combos_compile_and_run() {
    // The cursed-but-valid combinations above must compile; here we also construct /
    // access / clone / tear them down. Everything is null / empty so no registry is
    // needed (teardown & clone skip offset-0 elements and empty vectors).
    use crate::Foreign;
    use crate::TryCloneIn;
    use crate::registry::FileId;

    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // [Option<Foreign>; 8] all None.
    let a = CursedArr8::new(&alloc, 9, [None, None, None, None, None, None, None, None]).unwrap();
    assert_eq!(a.handle().get_tag(stack).unwrap(), 9);
    let slots = a.handle().get_slots(stack).unwrap();
    assert_eq!(slots.len(), 8);
    assert!(slots.iter().all(::core::option::Option::is_none));
    a.handle()
        .try_clone_in(&alloc)
        .unwrap()
        .bstack_drop(&alloc)
        .unwrap();
    a.bstack_drop(&alloc).unwrap();

    // [[Foreign; 2]; 3] all null (offset-0) strong pointers.
    let null = unsafe { Foreign::<MacroStrongChild>::new(FileId::SELF, 0) };
    let n = CursedNestedArr::new(&alloc, [[null, null], [null, null], [null, null]]).unwrap();
    let grid = n.handle().get_grid(stack).unwrap();
    assert_eq!(grid.len(), 3);
    assert_eq!(grid[0].len(), 2);
    n.handle()
        .try_clone_in(&alloc)
        .unwrap()
        .bstack_drop(&alloc)
        .unwrap();
    n.bstack_drop(&alloc).unwrap();

    // The grand mix: null / empty everywhere.
    let m = CursedMix::new(
        &alloc,
        None,
        vec![],
        None,
        unsafe { Foreign::<MacroLeaf>::new(FileId::SELF, 0) },
        None,
    )
    .unwrap();
    assert!(m.handle().get_maybe_owned(&alloc).unwrap().is_none());
    assert!(m.handle().get_refs(&alloc).unwrap().is_empty());
    assert!(m.handle().get_maybe_weak(stack).unwrap().is_none());
    assert!(m.handle().get_deep(&alloc).unwrap().is_none());
    m.handle()
        .try_clone_in(&alloc)
        .unwrap()
        .bstack_drop(&alloc)
        .unwrap();
    m.bstack_drop(&alloc).unwrap();
}

#[test]
fn macro_foreign_vec_of_option_roundtrips() {
    // `Vec<Option<Foreign<T>>>`: a null element (offset 0) reads back as `None`; a
    // present one resolves. Teardown / clone skip the `None`s.
    use crate::registry;
    use crate::{Foreign, TryCloneIn};
    use std::sync::Arc;

    let home = TempStack::new();
    let home_alloc = home.allocator();
    let hstack = home_alloc.stack();

    let foreign = TempStack::new();
    let arc_b = Arc::new(foreign.allocator());
    let reg_file = TempStack::new();
    let _ = registry::init(&reg_file.path);
    let reg = registry::get().unwrap();
    let fid = reg.attach(&foreign.path, arc_b.clone()).unwrap();

    let f = |v: u32| {
        let l = MacroLeaf::new(&*arc_b, v).unwrap();
        Some(unsafe { Foreign::<MacroLeaf>::new(fid, l.handle().range().start()) })
    };
    // [Some, None, Some].
    let h = OptForeignVecHolder::new(&home_alloc, vec![f(11), None, f(22)]).unwrap();
    let got = h.handle().get_links(&home_alloc).unwrap();
    assert_eq!(got.len(), 3);
    assert!(got[1].is_none());
    assert_eq!(
        got[0]
            .unwrap()
            .with(&home_alloc, |t, fs| t.get_val(fs).unwrap())
            .unwrap()
            .unwrap(),
        11
    );
    assert_eq!(
        got[2]
            .unwrap()
            .with(&home_alloc, |t, fs| t.get_val(fs).unwrap())
            .unwrap()
            .unwrap(),
        22
    );

    // Clone: the two present elements are deep-copied, the `None` stays `None`.
    let c = h.handle().try_clone_in(&home_alloc).unwrap();
    let cgot = c.handle().get_links(&home_alloc).unwrap();
    assert!(cgot[1].is_none());
    assert_ne!(cgot[0].unwrap().offset(), got[0].unwrap().offset());

    h.bstack_drop(&home_alloc).unwrap();
    c.bstack_drop(&home_alloc).unwrap();
    let _ = hstack;
    reg.detach(fid);
}

#[test]
fn macro_foreign_in_enum_across_files() {
    // A `#[bstack_owned] V(Foreign<T>)` enum variant: constructed, read, deep-cloned,
    // and torn down cross-file — alongside plain POD variants.
    use crate::registry;
    use crate::{Foreign, TryCloneIn};
    use std::sync::Arc;

    let home = TempStack::new();
    let home_alloc = home.allocator();
    let foreign = TempStack::new();
    let arc_b = Arc::new(foreign.allocator());
    let reg_file = TempStack::new();
    let _ = registry::init(&reg_file.path);
    let reg = registry::get().unwrap();
    let fid = reg.attach(&foreign.path, arc_b.clone()).unwrap();

    let mk = |v: u32| {
        let l = MacroLeaf::new(&*arc_b, v).unwrap();
        unsafe { Foreign::<MacroLeaf>::new(fid, l.handle().range().start()) }
    };

    // A plain POD variant still works.
    let n = ForeignEnum::new(&home_alloc, ForeignEnumData::Local(42)).unwrap();
    match n.handle().read(&home_alloc).unwrap() {
        ForeignEnumView::Local(x) => assert_eq!(x, 42),
        _ => panic!("wrong variant"),
    }
    n.bstack_drop(&home_alloc).unwrap();

    // Warm B's WAL block via the foreign variant, baseline.
    {
        let e = ForeignEnum::new(&home_alloc, ForeignEnumData::Far(mk(0))).unwrap();
        let c = e.handle().try_clone_in(&home_alloc).unwrap();
        c.bstack_drop(&home_alloc).unwrap();
        e.bstack_drop(&home_alloc).unwrap();
    }
    let base = arc_b.stack().len().unwrap();

    let e = ForeignEnum::new(&home_alloc, ForeignEnumData::Far(mk(77))).unwrap();
    let off = match e.handle().read(&home_alloc).unwrap() {
        ForeignEnumView::Far(f) => {
            assert_eq!(
                f.with(&home_alloc, |t, fs| t.get_val(fs).unwrap())
                    .unwrap()
                    .unwrap(),
                77
            );
            f.offset()
        }
        _ => panic!("wrong variant"),
    };

    // Deep clone copies the target on B (fresh offset, same value).
    let c = e.handle().try_clone_in(&home_alloc).unwrap();
    match c.handle().read(&home_alloc).unwrap() {
        ForeignEnumView::Far(f) => {
            assert_ne!(f.offset(), off);
            assert_eq!(
                f.with(&home_alloc, |t, fs| t.get_val(fs).unwrap())
                    .unwrap()
                    .unwrap(),
                77
            );
        }
        _ => panic!("wrong variant"),
    }

    // Teardown both → both leaves reclaimed → baseline.
    e.bstack_drop(&home_alloc).unwrap();
    c.bstack_drop(&home_alloc).unwrap();
    assert_eq!(
        arc_b.stack().len().unwrap(),
        base,
        "foreign enum variant leaked or double-freed on B"
    );
    reg.detach(fid);
}

#[test]
fn macro_foreign_generic_tuple_and_enum() {
    // Generic foreign target inside a tuple field AND inside an enum variant.
    use crate::registry;
    use crate::{Foreign, TryCloneIn};
    use std::sync::Arc;

    let home = TempStack::new();
    let home_alloc = home.allocator();
    let hstack = home_alloc.stack();
    let foreign = TempStack::new();
    let arc_b = Arc::new(foreign.allocator());
    let reg_file = TempStack::new();
    let _ = registry::init(&reg_file.path);
    let reg = registry::get().unwrap();
    let fid = reg.attach(&foreign.path, arc_b.clone()).unwrap();
    let mk = |v: u32| {
        let l = MacroLeaf::new(&*arc_b, v).unwrap();
        unsafe { Foreign::<MacroLeaf>::new(fid, l.handle().range().start()) }
    };

    // Generic foreign tuple.
    let t = GenForeignTup::<MacroLeaf>::new(&home_alloc, 1, (9, mk(11))).unwrap();
    let pair = t.handle().get_pair(hstack).unwrap();
    assert_eq!(pair.0, 9);
    assert_eq!(
        pair.1
            .with(&home_alloc, |x, fs| x.get_val(fs).unwrap())
            .unwrap()
            .unwrap(),
        11
    );
    let tc = t.handle().try_clone_in(&home_alloc).unwrap();
    assert_ne!(
        tc.handle().get_pair(hstack).unwrap().1.offset(),
        pair.1.offset()
    );
    t.bstack_drop(&home_alloc).unwrap();
    tc.bstack_drop(&home_alloc).unwrap();

    // Generic foreign enum variant.
    let e = GenForeignEnum::<MacroLeaf>::new(&home_alloc, GenForeignEnumData::Far(mk(22))).unwrap();
    let off = match e.handle().read(&home_alloc).unwrap() {
        GenForeignEnumView::Far(f) => {
            assert_eq!(
                f.with(&home_alloc, |x, fs| x.get_val(fs).unwrap())
                    .unwrap()
                    .unwrap(),
                22
            );
            f.offset()
        }
        _ => panic!("wrong variant"),
    };
    let ec = e.handle().try_clone_in(&home_alloc).unwrap();
    match ec.handle().read(&home_alloc).unwrap() {
        GenForeignEnumView::Far(f) => assert_ne!(f.offset(), off),
        _ => panic!("wrong variant"),
    }
    e.bstack_drop(&home_alloc).unwrap();
    ec.bstack_drop(&home_alloc).unwrap();
    reg.detach(fid);
}

#[test]
fn macro_foreign_strong_enum_variant() {
    // A `#[bstack_strong] V(Foreign<T>)` enum variant: cloning bumps the far strong
    // count, teardown decrements it.
    use crate::registry;
    use crate::{Foreign, TryCloneIn};
    use std::sync::Arc;

    let home = TempStack::new();
    let home_alloc = home.allocator();
    let foreign = TempStack::new();
    let arc_b = Arc::new(foreign.allocator());
    let ds = size_of::<<MacroStrongChild as BStackBlock>::OnDisk>() as u64;
    let cs = size_of::<<MacroStrongChild as BStackWeakable>::Control>() as u64;
    let reg_file = TempStack::new();
    let _ = registry::init(&reg_file.path);
    let reg = registry::get().unwrap();
    let fid = reg.attach(&foreign.path, arc_b.clone()).unwrap();

    let d = alloc_block(&*arc_b, MacroStrongChild::eightcc(), ds).unwrap();
    let ctrl = alloc_control(&*arc_b, ctrl_tag(), d, cs).unwrap();
    let strong_off = ctrl.start() + layout::CTRL_STRONG_OFFSET;
    let load = |o: u64| crate::refcount::load(arc_b.stack(), o).unwrap();
    assert_eq!(load(strong_off), 1);

    let e = ForeignStrongEnum::new(
        &home_alloc,
        ForeignStrongEnumData::S(unsafe { Foreign::<MacroStrongChild>::new(fid, d.start()) }),
    )
    .unwrap();
    let cl = e.handle().try_clone_in(&home_alloc).unwrap();
    assert_eq!(
        load(strong_off),
        2,
        "strong enum variant should bump on clone"
    );
    e.bstack_drop(&home_alloc).unwrap();
    assert_eq!(load(strong_off), 1);
    cl.bstack_drop(&home_alloc).unwrap();
    reg.detach(fid);
}

#[test]
fn macro_foreign_tuple_in_enum_variant() {
    // A `#[bstack_owned] V((A, Foreign<T>, Option<Foreign<T>>))` variant: POD packed
    // inline, foreign elements resolve / deep-clone / tear down cross-file.
    use crate::registry;
    use crate::{Foreign, TryCloneIn};
    use std::sync::Arc;

    let home = TempStack::new();
    let home_alloc = home.allocator();
    let foreign = TempStack::new();
    let arc_b = Arc::new(foreign.allocator());
    let reg_file = TempStack::new();
    let _ = registry::init(&reg_file.path);
    let reg = registry::get().unwrap();
    let fid = reg.attach(&foreign.path, arc_b.clone()).unwrap();
    let mk = |v: u32| {
        let l = MacroLeaf::new(&*arc_b, v).unwrap();
        unsafe { Foreign::<MacroLeaf>::new(fid, l.handle().range().start()) }
    };

    // Warm, baseline.
    {
        let e = ForeignTupEnum::new(
            &home_alloc,
            ForeignTupEnumData::Pair((0, mk(0), Some(mk(0)))),
        )
        .unwrap();
        e.handle()
            .try_clone_in(&home_alloc)
            .unwrap()
            .bstack_drop(&home_alloc)
            .unwrap();
        e.bstack_drop(&home_alloc).unwrap();
    }
    let base = arc_b.stack().len().unwrap();

    let e = ForeignTupEnum::new(
        &home_alloc,
        ForeignTupEnumData::Pair((100, mk(11), Some(mk(22)))),
    )
    .unwrap();
    let (off1, off2) = match e.handle().read(&home_alloc).unwrap() {
        ForeignTupEnumView::Pair((a, f1, f2)) => {
            assert_eq!(a, 100);
            assert_eq!(
                f1.with(&home_alloc, |x, fs| x.get_val(fs).unwrap())
                    .unwrap()
                    .unwrap(),
                11
            );
            let f2 = f2.expect("Some");
            assert_eq!(
                f2.with(&home_alloc, |x, fs| x.get_val(fs).unwrap())
                    .unwrap()
                    .unwrap(),
                22
            );
            (f1.offset(), f2.offset())
        }
        _ => panic!("wrong variant"),
    };

    // Deep clone copies both foreign elements (fresh offsets).
    let c = e.handle().try_clone_in(&home_alloc).unwrap();
    match c.handle().read(&home_alloc).unwrap() {
        ForeignTupEnumView::Pair((_, f1, f2)) => {
            assert_ne!(f1.offset(), off1);
            assert_ne!(f2.expect("Some").offset(), off2);
        }
        _ => panic!("wrong variant"),
    }

    e.bstack_drop(&home_alloc).unwrap();
    c.bstack_drop(&home_alloc).unwrap();
    assert_eq!(
        arc_b.stack().len().unwrap(),
        base,
        "foreign tuple-in-enum variant leaked"
    );
    reg.detach(fid);
}

#[test]
fn macro_foreign_enum_container_variants() {
    // Enum variants holding foreign containers: `V(Vec<Foreign<T>>)` and
    // `V([Foreign<T>; N])` — constructed, read, deep-cloned, torn down cross-file.
    use crate::registry;
    use crate::{Foreign, TryCloneIn};
    use std::sync::Arc;

    let home = TempStack::new();
    let home_alloc = home.allocator();
    let foreign = TempStack::new();
    let arc_b = Arc::new(foreign.allocator());
    let reg_file = TempStack::new();
    let _ = registry::init(&reg_file.path);
    let reg = registry::get().unwrap();
    let fid = reg.attach(&foreign.path, arc_b.clone()).unwrap();
    let mk = |v: u32| {
        let l = MacroLeaf::new(&*arc_b, v).unwrap();
        unsafe { Foreign::<MacroLeaf>::new(fid, l.handle().range().start()) }
    };

    // Warm both variants' clone paths (B's WAL block), baseline.
    {
        let e = ForeignContainerEnum::new(&home_alloc, ForeignContainerEnumData::Many(vec![mk(0)]))
            .unwrap();
        e.handle()
            .try_clone_in(&home_alloc)
            .unwrap()
            .bstack_drop(&home_alloc)
            .unwrap();
        e.bstack_drop(&home_alloc).unwrap();
        let e =
            ForeignContainerEnum::new(&home_alloc, ForeignContainerEnumData::Fixed([mk(0), mk(0)]))
                .unwrap();
        e.handle()
            .try_clone_in(&home_alloc)
            .unwrap()
            .bstack_drop(&home_alloc)
            .unwrap();
        e.bstack_drop(&home_alloc).unwrap();
    }
    let base = arc_b.stack().len().unwrap();

    // Vec variant.
    let e = ForeignContainerEnum::new(
        &home_alloc,
        ForeignContainerEnumData::Many(vec![mk(1), mk(2), mk(3)]),
    )
    .unwrap();
    match e.handle().read(&home_alloc).unwrap() {
        ForeignContainerEnumView::Many(v) => {
            assert_eq!(v.len(), 3);
            assert_eq!(
                v[1].with(&home_alloc, |x, fs| x.get_val(fs).unwrap())
                    .unwrap()
                    .unwrap(),
                2
            );
        }
        _ => panic!("wrong variant"),
    }
    let c = e.handle().try_clone_in(&home_alloc).unwrap();
    e.bstack_drop(&home_alloc).unwrap();
    c.bstack_drop(&home_alloc).unwrap();

    // Array variant.
    let e = ForeignContainerEnum::new(&home_alloc, ForeignContainerEnumData::Fixed([mk(7), mk(8)]))
        .unwrap();
    match e.handle().read(&home_alloc).unwrap() {
        ForeignContainerEnumView::Fixed(a) => {
            assert_eq!(
                a[0].with(&home_alloc, |x, fs| x.get_val(fs).unwrap())
                    .unwrap()
                    .unwrap(),
                7
            );
            assert_eq!(
                a[1].with(&home_alloc, |x, fs| x.get_val(fs).unwrap())
                    .unwrap()
                    .unwrap(),
                8
            );
        }
        _ => panic!("wrong variant"),
    }
    let c = e.handle().try_clone_in(&home_alloc).unwrap();
    e.bstack_drop(&home_alloc).unwrap();
    c.bstack_drop(&home_alloc).unwrap();

    assert_eq!(
        arc_b.stack().len().unwrap(),
        base,
        "enum foreign container variant leaked"
    );
    reg.detach(fid);
}

#[test]
fn macro_foreign_in_tuple_across_files() {
    // A tuple field mixing POD and (nullable) foreign elements: the POD parts store
    // inline, the foreign parts resolve / deep-clone / tear down cross-file.
    use crate::registry;
    use crate::{Foreign, TryCloneIn};
    use std::sync::Arc;

    let home = TempStack::new();
    let home_alloc = home.allocator();
    let hstack = home_alloc.stack();
    let foreign = TempStack::new();
    let arc_b = Arc::new(foreign.allocator());
    let reg_file = TempStack::new();
    let _ = registry::init(&reg_file.path);
    let reg = registry::get().unwrap();
    let fid = reg.attach(&foreign.path, arc_b.clone()).unwrap();

    let mk = |v: u32| {
        let l = MacroLeaf::new(&*arc_b, v).unwrap();
        unsafe { Foreign::<MacroLeaf>::new(fid, l.handle().range().start()) }
    };

    // Warm B's WAL block, baseline.
    {
        let h = ForeignTupHolder::new(&home_alloc, 0, (0, mk(0)), (0, Some(mk(0)), 0)).unwrap();
        let c = h.handle().try_clone_in(&home_alloc).unwrap();
        c.bstack_drop(&home_alloc).unwrap();
        h.bstack_drop(&home_alloc).unwrap();
    }
    let base = arc_b.stack().len().unwrap();

    let h = ForeignTupHolder::new(&home_alloc, 5, (100, mk(11)), (7, Some(mk(22)), 9)).unwrap();

    // POD parts preserved; foreign parts resolve.
    let pair = h.handle().get_pair(hstack).unwrap();
    assert_eq!(pair.0, 100);
    assert_eq!(
        pair.1
            .with(&home_alloc, |t, fs| t.get_val(fs).unwrap())
            .unwrap()
            .unwrap(),
        11
    );
    let maybe = h.handle().get_maybe(hstack).unwrap();
    assert_eq!((maybe.0, maybe.2), (7, 9));
    assert_eq!(
        maybe
            .1
            .unwrap()
            .with(&home_alloc, |t, fs| t.get_val(fs).unwrap())
            .unwrap()
            .unwrap(),
        22
    );

    // Deep clone copies both foreign elements (fresh offsets, same values).
    let c = h.handle().try_clone_in(&home_alloc).unwrap();
    let cpair = c.handle().get_pair(hstack).unwrap();
    assert_ne!(cpair.1.offset(), pair.1.offset());
    assert_eq!(
        cpair
            .1
            .with(&home_alloc, |t, fs| t.get_val(fs).unwrap())
            .unwrap()
            .unwrap(),
        11
    );

    h.bstack_drop(&home_alloc).unwrap();
    c.bstack_drop(&home_alloc).unwrap();
    assert_eq!(
        arc_b.stack().len().unwrap(),
        base,
        "foreign-in-tuple leaked"
    );
    reg.detach(fid);
}

#[test]
fn macro_foreign_owned_clone_errors_when_target_file_detached() {
    // Cloning an owning `Foreign<T>` whose target file is not attached must ERROR (not
    // silently alias — that would create a second owner and later double-free).
    use crate::registry;
    use crate::{Foreign, TryCloneIn};
    use std::sync::Arc;

    let home = TempStack::new();
    let home_alloc = home.allocator();

    let foreign = TempStack::new();
    let arc_b = Arc::new(foreign.allocator());
    let leaf = MacroLeaf::new(&*arc_b, 5).unwrap();
    let off = leaf.handle().range().start();

    let reg_file = TempStack::new();
    let _ = registry::init(&reg_file.path);
    let reg = registry::get().unwrap();
    let fid = reg.attach(&foreign.path, arc_b.clone()).unwrap();

    let h = ForeignHolder::new(
        &home_alloc,
        1,
        unsafe { Foreign::<MacroLeaf>::new(fid, off) },
        None,
    )
    .unwrap();

    // Detach the target file → the deep clone cannot copy the target → error.
    reg.detach(fid);
    assert!(
        h.handle().try_clone_in(&home_alloc).is_err(),
        "cloning an owned Foreign with a detached target file must error, not alias"
    );
}

#[test]
fn foreign_reverse_map_and_bstack_cast() {
    use crate::registry::{FileId, FileRegistry};
    use crate::{BStackRef, Foreign};
    use bstack::BStackSlice;
    use std::sync::Arc;

    let reg_file = TempStack::new();
    let foreign_file = TempStack::new();
    let local_file = TempStack::new();

    let reg = FileRegistry::open(&reg_file.path).unwrap();

    let foreign_alloc = foreign_file.allocator();
    let leaf = MacroLeaf::new(&foreign_alloc, 3).unwrap();
    let off = leaf.handle().range().start();
    let id = reg
        .attach(&foreign_file.path, Arc::new(foreign_alloc))
        .unwrap();

    // Reverse map: a live host's stack resolves back to its FileId.
    assert_eq!(
        reg.with_host(id, |host| reg.id_of_host(host.stack())),
        Some(Some(id))
    );

    // foreign -> normal (`bstack_cast!(foreign as BStackRef<T>)`): a SELF pointer is
    // always resolvable-in-place; a foreign id that is not live-in-the-GLOBAL-registry
    // is `None`. Use a high id no test ever attaches, so this holds regardless of what
    // other (global-registry) tests are doing concurrently.
    let selfp = unsafe { Foreign::<MacroLeaf>::new(FileId::SELF, off) };
    let r: Option<BStackRef<MacroLeaf>> = bstack_cast!(selfp as BStackRef<MacroLeaf>);
    assert!(r.is_some());
    let dead = FileId::from_u64(60_000).unwrap();
    assert!(
        unsafe { Foreign::<MacroLeaf>::new(dead, off) }
            .into_local()
            .is_none()
    );

    // normal -> foreign (`bstack_cast!(slice as Foreign<T>)`): the local file is never
    // attached to the GLOBAL registry, so its stack has no id → `None`, but the macro
    // arm type-checks.
    let la = local_file.allocator();
    let s = la.alloc(16).unwrap().as_range();
    let slice = unsafe { BStackSlice::from_raw_range(la.stack(), s) };
    let f: Option<Foreign<MacroLeaf>> = bstack_cast!(slice as Foreign<MacroLeaf>);
    assert!(f.is_none());

    // Detach prunes the reverse-map entry.
    reg.detach(id);
    assert!(!reg.is_live(id));
    assert_eq!(reg.id_of_host(la.stack()), None);
}

// --------------------------------------------------------------------------
// Stdlib collections composed into `#[bstack_block]` / `#[bstack_enum]` types.
// Collections are `BStackBlock + TryCloneIn` and override the `__bstack_*`
// hooks, so they should compose anywhere a scalar block can; these tests verify
// the compositions actually build, read back, deep-clone (independent copies),
// and tear down (leak-free) — on both the sequential (FirstFit) and the
// bulk/WAL (GhostTree) allocators, plus cross-file `Foreign<Collection>`
// clone/teardown.
// --------------------------------------------------------------------------

fn deque_offsets(dq: &BStackDeque<MacroLeaf>, stack: &BStack) -> Vec<u64> {
    dq.to_vec(stack)
        .unwrap()
        .into_iter()
        .map(|h| h.range().start())
        .collect()
}

// A block whose fields ARE stdlib collections: an owned deque and a nullable
// owned map. Each lowers to a `u64` offset on disk, exactly like a scalar owned
// child — the collection's own `__bstack_*` hooks do the recursive work.
#[bstack_block]
struct CollectionFields {
    tag: u32,
    #[bstack_owned]
    dq: BStackDeque<MacroLeaf>,
    #[bstack_owned]
    maybe_map: Option<BStackHashMap<u32, MacroLeaf>>,
}

fn build_collection_fields<A: BStackRaiiAllocator>(alloc: &A) -> BStackOwned<CollectionFields> {
    let dq = BStackDeque::<MacroLeaf>::new(alloc).unwrap();
    for v in [10u32, 20, 30] {
        dq.push_back(alloc, MacroLeaf::new(alloc, v).unwrap())
            .unwrap();
    }
    let map = BStackHashMap::<u32, MacroLeaf>::new(alloc).unwrap();
    map.insert(alloc, 1, MacroLeaf::new(alloc, 100).unwrap())
        .unwrap();
    map.insert(alloc, 2, MacroLeaf::new(alloc, 200).unwrap())
        .unwrap();
    CollectionFields::new(alloc, 7, dq, Some(map)).unwrap()
}

#[test]
fn stdlib_in_block_fields_build_read_clone_teardown() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let h = build_collection_fields(&alloc);
    assert_eq!(h.handle().get_tag(stack).unwrap(), 7);

    // Read the collections back through the generated accessors.
    let got_dq = h.handle().get_dq(stack).unwrap();
    assert_eq!(deque_values(&got_dq, stack), vec![10, 20, 30]);
    let got_map = h.handle().get_maybe_map(stack).unwrap().expect("Some map");
    assert_eq!(got_map.len(stack).unwrap(), 2);
    assert_eq!(
        got_map
            .get(stack, &1)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
        100
    );
    assert_eq!(
        got_map
            .get(stack, &2)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
        200
    );

    // A `None` map round-trips through the offset-0 niche.
    let dq0 = BStackDeque::<MacroLeaf>::new(&alloc).unwrap();
    let h_none = CollectionFields::new(&alloc, 0, dq0, None).unwrap();
    assert!(h_none.handle().get_maybe_map(stack).unwrap().is_none());
    h_none.bstack_drop(&alloc).unwrap();

    // Deep clone: the clone carries independent copies of BOTH collections.
    let c = h.handle().try_clone_in(&alloc).unwrap();
    let cdq = c.handle().get_dq(stack).unwrap();
    let cmap = c.handle().get_maybe_map(stack).unwrap().unwrap();
    assert_ne!(cdq.range().start(), got_dq.range().start());
    assert_ne!(c.handle().range().start(), h.handle().range().start());
    assert_eq!(deque_values(&cdq, stack), vec![10, 20, 30]);
    // Every deque element is a fresh block, not an alias (the deep clone
    // recursed into the collection's ring, not just the handle).
    assert_eq!(deque_offsets(&got_dq, stack).len(), 3);
    assert!(
        deque_offsets(&got_dq, stack)
            .iter()
            .zip(deque_offsets(&cdq, stack).iter())
            .all(|(a, b)| a != b),
        "clone must deep-copy the deque's elements"
    );
    assert_eq!(
        cmap.get(stack, &1)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
        100
    );
    assert_eq!(
        cmap.get(stack, &2)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
        200
    );

    // Freeing the clone frees only the clone's subtree; the original is intact.
    c.bstack_drop(&alloc).unwrap();
    assert_eq!(deque_values(&got_dq, stack), vec![10, 20, 30]);
    assert_eq!(got_map.len(stack).unwrap(), 2);
    h.bstack_drop(&alloc).unwrap();

    // Teardown must reclaim the whole structure — deque handle, ring, elements,
    // map handle, bucket blocks, and value blocks — with no leak. Build + tear
    // twice and assert the stack returns exactly to baseline.
    assert_teardown_reclaims(&alloc, || build_collection_fields(&alloc));
}

#[test]
fn stdlib_in_block_fields_on_bulk_allocator() {
    // The same composition on GhostTree (`atomic_bulk() == true`): teardown frees
    // the whole multi-block subtree with one atomic `dealloc_bulk`, and the
    // two-pass (measure -> build) clone must measure every block the collections
    // own — ring, bucket arrays, elements — so the clone is exact-sized.
    let tmp = TempStack::new();
    let alloc = tmp.ghost_allocator();
    let stack = alloc.stack();

    let h = build_collection_fields(&alloc);
    let c = h.handle().try_clone_in(&alloc).unwrap();

    assert_eq!(
        deque_values(&c.handle().get_dq(stack).unwrap(), stack),
        vec![10, 20, 30]
    );
    assert_eq!(
        c.handle()
            .get_maybe_map(stack)
            .unwrap()
            .unwrap()
            .get(stack, &2)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
        200
    );

    // Warm, baseline, then build + tear the identical structure twice.
    build_collection_fields(&alloc).bstack_drop(&alloc).unwrap();
    let base = alloc.stack().len().unwrap();
    build_collection_fields(&alloc).bstack_drop(&alloc).unwrap();
    assert_eq!(
        alloc.stack().len().unwrap(),
        base,
        "bulk teardown leaked a collection's block"
    );

    h.bstack_drop(&alloc).unwrap();
    c.bstack_drop(&alloc).unwrap();
}

// A block whose Vec / array ELEMENTS are collections — the container's own
// machinery (`BStackBlockVec`, the fixed-array path) recurses per element.
#[bstack_block]
struct CollectionContainer {
    tag: u32,
    #[bstack_owned]
    deques: Vec<BStackDeque<MacroLeaf>>,
    #[bstack_owned]
    fixed: [BStackDeque<MacroLeaf>; 2],
}

#[test]
fn stdlib_collections_in_vec_and_array_elements() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let make_dq = |vals: &[u32]| {
        let dq = BStackDeque::<MacroLeaf>::new(&alloc).unwrap();
        for v in vals {
            dq.push_back(&alloc, MacroLeaf::new(&alloc, *v).unwrap())
                .unwrap();
        }
        dq
    };

    let h = CollectionContainer::new(
        &alloc,
        7,
        vec![make_dq(&[1, 2]), make_dq(&[3, 4, 5])],
        [make_dq(&[6]), make_dq(&[7, 8])],
    )
    .unwrap();

    // Read the Vec<..> and [..; 2] back through the accessors.
    let got_vec = h.handle().get_deques(&alloc).unwrap().to_vec().unwrap();
    assert_eq!(got_vec.len(), 2);
    assert_eq!(deque_values(&got_vec[0], stack), vec![1, 2]);
    assert_eq!(deque_values(&got_vec[1], stack), vec![3, 4, 5]);
    let got_arr = h.handle().get_fixed(stack).unwrap();
    assert_eq!(deque_values(&got_arr[0], stack), vec![6]);
    assert_eq!(deque_values(&got_arr[1], stack), vec![7, 8]);

    // Deep clone recurses through each element of each container.
    let c = h.handle().try_clone_in(&alloc).unwrap();
    let cvec = c.handle().get_deques(&alloc).unwrap().to_vec().unwrap();
    let carr = c.handle().get_fixed(stack).unwrap();
    for i in 0..2 {
        assert_ne!(
            cvec[i].range().start(),
            got_vec[i].range().start(),
            "cloned Vec element must be a fresh block"
        );
        assert_ne!(
            carr[i].range().start(),
            got_arr[i].range().start(),
            "cloned array element must be a fresh block"
        );
    }
    assert_eq!(deque_values(&cvec[0], stack), vec![1, 2]);
    assert_eq!(deque_values(&cvec[1], stack), vec![3, 4, 5]);
    assert_eq!(deque_values(&carr[0], stack), vec![6]);
    assert_eq!(deque_values(&carr[1], stack), vec![7, 8]);

    // Independent teardown + full reclamation (a leak here means a nested
    // element was never freed).
    c.bstack_drop(&alloc).unwrap();
    assert_eq!(deque_values(&got_vec[0], stack), vec![1, 2]);
    assert_eq!(deque_values(&got_arr[1], stack), vec![7, 8]);
    assert_teardown_reclaims(&alloc, || {
        CollectionContainer::new(
            &alloc,
            0,
            vec![make_dq(&[1, 2]), make_dq(&[3])],
            [make_dq(&[4]), make_dq(&[5, 6])],
        )
        .unwrap()
    });
}

// A collection as an OWNED ENUM VARIANT payload — the enum's per-variant
// teardown / clone dispatch calls the collection's `__bstack_*` hooks.
#[bstack_enum]
enum CollectionEnum {
    Empty,
    #[bstack_owned]
    Deq(BStackDeque<MacroLeaf>),
    #[bstack_owned]
    Map(BStackHashMap<u32, MacroLeaf>),
}

#[test]
fn stdlib_collection_in_enum_variant() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Build a `Deq` variant, read the deque back through the view, clone it
    // (independent), and tear it down — reclaiming deque + ring + elements.
    let dq = BStackDeque::<MacroLeaf>::new(&alloc).unwrap();
    dq.push_back(&alloc, MacroLeaf::new(&alloc, 1).unwrap())
        .unwrap();
    dq.push_back(&alloc, MacroLeaf::new(&alloc, 2).unwrap())
        .unwrap();
    let e = CollectionEnum::new(&alloc, CollectionEnumData::Deq(dq)).unwrap();
    let got = match e.handle().read(&alloc).unwrap() {
        CollectionEnumView::Deq(d) => d,
        _ => panic!("expected Deq variant"),
    };
    assert_eq!(deque_values(&got, stack), vec![1, 2]);

    let c = e.handle().try_clone_in(&alloc).unwrap();
    let cgot = match c.handle().read(&alloc).unwrap() {
        CollectionEnumView::Deq(d) => d,
        _ => panic!("expected Deq variant"),
    };
    assert_ne!(cgot.range().start(), got.range().start());
    assert_eq!(deque_values(&cgot, stack), vec![1, 2]);
    assert!(
        deque_offsets(&got, stack)
            .iter()
            .zip(deque_offsets(&cgot, stack).iter())
            .all(|(a, b)| a != b),
        "cloned enum deque must deep-copy its elements"
    );

    // `bstack_move!` hands the owned collection back as a `BStackOwned`.
    let mv = bstack_move!(e, &alloc).unwrap();
    match mv {
        CollectionEnumData::Deq(dq) => {
            assert_eq!(deque_values(&dq, stack), vec![1, 2]);
            dq.bstack_drop(&alloc).unwrap();
        }
        _ => panic!("expected Deq variant from bstack_move"),
    }
    c.bstack_drop(&alloc).unwrap();

    // A `Map` variant round-trips, reads back, clones independently, and
    // reclaims too.
    let map = BStackHashMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    map.insert(&alloc, 1, MacroLeaf::new(&alloc, 10).unwrap())
        .unwrap();
    map.insert(&alloc, 2, MacroLeaf::new(&alloc, 20).unwrap())
        .unwrap();
    let em = CollectionEnum::new(&alloc, CollectionEnumData::Map(map)).unwrap();
    let mgot = match em.handle().read(&alloc).unwrap() {
        CollectionEnumView::Map(m) => m,
        _ => panic!("expected Map variant"),
    };
    assert_eq!(mgot.len(stack).unwrap(), 2);
    assert_eq!(
        mgot.get(stack, &2)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
        20
    );
    let mclone = em.handle().try_clone_in(&alloc).unwrap();
    let mc = match mclone.handle().read(&alloc).unwrap() {
        CollectionEnumView::Map(m) => m,
        _ => panic!("expected Map variant from clone"),
    };
    assert_ne!(mc.range().start(), mgot.range().start());
    assert_eq!(mc.len(stack).unwrap(), 2);
    em.bstack_drop(&alloc).unwrap();
    mclone.bstack_drop(&alloc).unwrap();

    assert_teardown_reclaims(&alloc, || {
        let map = BStackHashMap::<u32, MacroLeaf>::new(&alloc).unwrap();
        map.insert(&alloc, 1, MacroLeaf::new(&alloc, 10).unwrap())
            .unwrap();
        CollectionEnum::new(&alloc, CollectionEnumData::Map(map)).unwrap()
    });

    // The `Empty` variant still works alongside the collection variants.
    let empty = CollectionEnum::new(&alloc, CollectionEnumData::Empty).unwrap();
    assert!(matches!(
        empty.handle().read(&alloc).unwrap(),
        CollectionEnumView::Empty
    ));
    empty.bstack_drop(&alloc).unwrap();
}

// A `Foreign<Collection>` target: the home block owns a deque living in ANOTHER
// file. Cross-file deep clone must copy the whole collection into its own file,
// and teardown must free it there recursively.
#[bstack_block]
struct ForeignCollectionHolder {
    tag: u32,
    #[bstack_owned]
    dq: Foreign<BStackDeque<MacroLeaf>>,
}

#[test]
fn stdlib_foreign_collection_target_clone_and_teardown() {
    use crate::Foreign;
    use crate::registry;
    use std::sync::Arc;

    let home = TempStack::new();
    let home_alloc = home.allocator();
    let hstack = home_alloc.stack();

    let foreign = TempStack::new();
    let arc_b = Arc::new(foreign.allocator());

    let reg_file = TempStack::new();
    let _ = registry::init(&reg_file.path);
    let reg = registry::get().unwrap();
    let fid = reg.attach(&foreign.path, arc_b.clone()).unwrap();

    // Build a deque in the FOREIGN file: handle + ring + two element blocks.
    let dq = BStackDeque::<MacroLeaf>::new(&*arc_b).unwrap();
    dq.push_back(&*arc_b, MacroLeaf::new(&*arc_b, 1).unwrap())
        .unwrap();
    dq.push_back(&*arc_b, MacroLeaf::new(&*arc_b, 2).unwrap())
        .unwrap();
    let dq_off = dq.handle().range().start();

    // Warm the cross-file owned-clone path (creates B's persistent WAL block),
    // then record the foreign baseline.
    {
        let warm_dq = BStackDeque::<MacroLeaf>::new(&*arc_b).unwrap();
        warm_dq
            .push_back(&*arc_b, MacroLeaf::new(&*arc_b, 99).unwrap())
            .unwrap();
        let h0 = ForeignCollectionHolder::new(&home_alloc, 0, unsafe {
            Foreign::<BStackDeque<MacroLeaf>>::new(fid, warm_dq.handle().range().start())
        })
        .unwrap();
        let c0 = h0.handle().try_clone_in(&home_alloc).unwrap();
        h0.bstack_drop(&home_alloc).unwrap();
        c0.bstack_drop(&home_alloc).unwrap();
    }
    let base_b = arc_b.stack().len().unwrap();

    // A home block owning the foreign deque.
    let h = ForeignCollectionHolder::new(&home_alloc, 7, unsafe {
        Foreign::<BStackDeque<MacroLeaf>>::new(fid, dq_off)
    })
    .unwrap();

    // Deep clone: the whole deque (handle + ring + elements) is copied into the
    // foreign file; the clone points at a DIFFERENT block there.
    let c = h.handle().try_clone_in(&home_alloc).unwrap();
    let clone_link = c.handle().get_dq(hstack).unwrap();
    assert_eq!(clone_link.file_id(), fid);
    assert_ne!(
        clone_link.offset(),
        dq_off,
        "owned clone must be a fresh copy"
    );
    let clone_vals = clone_link
        .with(&home_alloc, |d, fs| {
            Ok::<_, std::io::Error>(deque_values(&d, fs))
        })
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(clone_vals, vec![1, 2]);

    // The clone's elements are distinct blocks in the foreign file too.
    let orig_offsets = dq.to_vec(arc_b.stack()).unwrap();
    let cloned_offsets = clone_link
        .with(&home_alloc, |d, fs| {
            Ok::<_, std::io::Error>(d.to_vec(fs).unwrap())
        })
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        orig_offsets
            .iter()
            .zip(cloned_offsets.iter())
            .all(|(a, b)| a.range().start() != b.range().start()),
        "cross-file clone must deep-copy the deque's elements"
    );

    // Tearing both holders down frees BOTH full deques on the foreign file (no
    // double-free, no leak) — the file returns exactly to the warmed baseline.
    h.bstack_drop(&home_alloc).unwrap();
    c.bstack_drop(&home_alloc).unwrap();
    assert_eq!(
        arc_b.stack().len().unwrap(),
        base_b,
        "foreign collection clone+teardown leaked or double-freed"
    );

    reg.detach(fid);
}

// --------------------------------------------------------------------------
// Generic struct owning a generic collection. `struct S<T> {
// #[bstack_owned] d: BStackDeque<T> }`: a `T` used INSIDE a generic collection
// field needs the macro to propagate `T: BStackBlock` (+ `TryCloneIn` for
// owned) to its generated impls. These tests verify a concrete instantiation
// builds / reads / deep-clones / tears down exactly like the non-generic
// composition.
// --------------------------------------------------------------------------

#[bstack_block]
struct GenericCollectionHolder<T> {
    tag: u32,
    #[bstack_owned]
    dq: BStackDeque<T>,
    #[bstack_owned]
    map: Option<BStackHashMap<u32, T>>,
}

#[bstack_block]
struct GenericCollectionVec<T> {
    #[bstack_owned]
    deques: Vec<BStackDeque<T>>,
}

#[test]
fn generic_collection_bound_inference_and_composition() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Concrete instantiation with `T = MacroLeaf`.
    let dq = BStackDeque::<MacroLeaf>::new(&alloc).unwrap();
    for v in [1u32, 2, 3] {
        dq.push_back(&alloc, MacroLeaf::new(&alloc, v).unwrap())
            .unwrap();
    }
    let map = BStackHashMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    map.insert(&alloc, 1, MacroLeaf::new(&alloc, 10).unwrap())
        .unwrap();
    let h = GenericCollectionHolder::<MacroLeaf>::new(&alloc, 7, dq, Some(map)).unwrap();

    // Accessors resolve through the generic collection field types.
    assert_eq!(h.handle().get_tag(stack).unwrap(), 7);
    let got_dq = h.handle().get_dq(stack).unwrap();
    assert_eq!(deque_values(&got_dq, stack), vec![1, 2, 3]);
    let got_map = h.handle().get_map(stack).unwrap().expect("Some");
    assert_eq!(got_map.len(stack).unwrap(), 1);
    assert_eq!(
        got_map
            .get(stack, &1)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
        10
    );

    // Deep clone recurses through the generic collection fields.
    let c = h.handle().try_clone_in(&alloc).unwrap();
    assert_ne!(c.handle().range().start(), h.handle().range().start());
    let cdq = c.handle().get_dq(stack).unwrap();
    assert_ne!(cdq.range().start(), got_dq.range().start());
    assert_eq!(deque_values(&cdq, stack), vec![1, 2, 3]);
    assert_eq!(
        c.handle()
            .get_map(stack)
            .unwrap()
            .unwrap()
            .get(stack, &1)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
        10
    );

    // Independent teardown of both, then leak-check the whole composition.
    c.bstack_drop(&alloc).unwrap();
    assert_eq!(deque_values(&got_dq, stack), vec![1, 2, 3]);
    h.bstack_drop(&alloc).unwrap();
    assert_teardown_reclaims(&alloc, || {
        let dq = BStackDeque::<MacroLeaf>::new(&alloc).unwrap();
        dq.push_back(&alloc, MacroLeaf::new(&alloc, 5).unwrap())
            .unwrap();
        let map = BStackHashMap::<u32, MacroLeaf>::new(&alloc).unwrap();
        map.insert(&alloc, 1, MacroLeaf::new(&alloc, 50).unwrap())
            .unwrap();
        GenericCollectionHolder::<MacroLeaf>::new(&alloc, 0, dq, Some(map)).unwrap()
    });

    // A second instantiation `T = MacroLeaf` with different leaf values works,
    // and the Vec-of-generic-collection form compiles + tears down too.
    let vh = GenericCollectionVec::<MacroLeaf>::new(
        &alloc,
        vec![
            BStackDeque::<MacroLeaf>::new(&alloc).unwrap(),
            BStackDeque::<MacroLeaf>::new(&alloc).unwrap(),
        ],
    )
    .unwrap();
    assert_eq!(vh.handle().get_deques(&alloc).unwrap().len().unwrap(), 2);
    vh.bstack_drop(&alloc).unwrap();
    assert_teardown_reclaims(&alloc, || {
        GenericCollectionVec::<MacroLeaf>::new(
            &alloc,
            vec![BStackDeque::<MacroLeaf>::new(&alloc).unwrap()],
        )
        .unwrap()
    });
}

// --------------------------------------------------------------------------
// `#[embed]` of a collection: only the collection's fixed descriptor is folded
// inline (the ring / element blocks stay out-of-line). Teardown and move both
// reason purely from the descriptor's absolute offsets, so they should work;
// deep clone folds the child's `OnDisk` in place via
// `__bstack_clone_children_inplace` — which the hand-written collection impls
// do NOT override (they override only `__bstack_drop_children` /
// `__bstack_clone_into`), so the clone may alias the source's out-of-line ring
// and elements instead of deep-copying them.
// --------------------------------------------------------------------------

#[bstack_block]
struct EmbCollectionHolder {
    #[embed]
    dq: BStackDeque<MacroLeaf>,
    tag: u32,
}

fn build_emb_collection_holder(
    alloc: &FirstFitBStackAllocator,
) -> BStackOwned<EmbCollectionHolder> {
    let dq = BStackDeque::<MacroLeaf>::new(alloc).unwrap();
    dq.push_back(alloc, MacroLeaf::new(alloc, 1).unwrap())
        .unwrap();
    dq.push_back(alloc, MacroLeaf::new(alloc, 2).unwrap())
        .unwrap();
    EmbCollectionHolder::new(alloc, dq, 7).unwrap()
}

#[test]
fn embed_collection_build_read_teardown() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Build: the deque descriptor is folded inline; ring + elements stay out
    // of line and keep their absolute offsets.
    let h = build_emb_collection_holder(&alloc);
    assert_eq!(h.handle().get_tag(stack).unwrap(), 7);

    // Read: the accessor yields a deque handle into the inline region.
    let got = h.handle().get_dq();
    assert_eq!(deque_values(&got, stack), vec![1, 2]);

    // Teardown: frees the ring + both elements *in place* via
    // `__bstack_drop_children`, then the holder shell — no leak.
    h.bstack_drop(&alloc).unwrap();
    assert_teardown_reclaims(&alloc, || build_emb_collection_holder(&alloc));
}

#[test]
fn embed_collection_move_rehomes() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Warm the persistent WAL anchor with one full cycle, then baseline (a
    // WAL-backed teardown's anchor is a one-time per-file allocation).
    {
        let (m, _) = bstack_move!(build_emb_collection_holder(&alloc), &alloc).unwrap();
        m.bstack_drop(&alloc).unwrap();
    }
    let base = alloc.stack().len().unwrap();

    // bstack_move! re-homes the descriptor to a fresh standalone deque block;
    // the parent shell is freed, so nothing is double-freed later.
    let (moved, tag) = bstack_move!(build_emb_collection_holder(&alloc), &alloc).unwrap();
    assert_eq!(tag, 7);
    assert_eq!(deque_values(&moved, stack), vec![1, 2]);
    // The re-homed deque owns its ring + elements; dropping it reclaims all of
    // them (build -> move -> drop the moved child must return to baseline).
    moved.bstack_drop(&alloc).unwrap();
    assert_eq!(
        alloc.stack().len().unwrap(),
        base,
        "move re-homed the embedded deque but leaked its ring or elements"
    );
}

#[test]
fn embed_collection_clone_is_independent() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let h = build_emb_collection_holder(&alloc);
    let got = h.handle().get_dq();

    // A deep clone of the holder must carry an INDEPENDENT copy of the embedded
    // deque — a fresh ring and fresh element blocks, not aliases of the
    // original's out-of-line storage.
    let c = h.handle().try_clone_in(&alloc).unwrap();
    let cdq = c.handle().get_dq();
    assert_ne!(
        c.handle().range().start(),
        h.handle().range().start(),
        "cloned holder must be a fresh block"
    );
    assert_ne!(
        cdq.range().start(),
        got.range().start(),
        "embedded deque must not alias the source descriptor"
    );
    assert_eq!(deque_values(&cdq, stack), vec![1, 2]);
    assert!(
        deque_offsets(&got, stack)
            .iter()
            .zip(deque_offsets(&cdq, stack).iter())
            .all(|(a, b)| a != b),
        "embedded collection clone must deep-copy its ring elements"
    );

    // Tearing the clone down must not touch the original's ring / elements.
    c.bstack_drop(&alloc).unwrap();
    assert_eq!(deque_values(&got, stack), vec![1, 2]);
    h.bstack_drop(&alloc).unwrap();
}
