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
    BStackBlock, BStackCast, BStackCastAs, BStackCastInto, BStackDrop, BStackOwned, BStackRc,
    BStackRef, BStackShared, BStackWeakable, EightCC, TryClone, alloc_block, alloc_control,
    bstack_block, bstack_cast, bstack_move, dealloc_range,
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

    // Own the parent; dropping it must recursively free the child, then itself.
    let owned =
        unsafe { BStackOwned::from_raw(<MacroParent as BStackBlock>::from_range(parent), &alloc) };
    drop(owned);

    // The child's slot (allocated first, so the lowest offset) is reclaimed —
    // proof the generated `bstack_drop` recursed into the owned child.
    let reused = alloc_block(&alloc, MacroLeaf::eightcc(), leaf_size).unwrap();
    assert_eq!(reused.start(), leaf.start());
    unsafe { dealloc_range(&alloc, reused).unwrap() };
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

    // Dropping the parent runs its generated teardown, which dispatches through
    // BStackShared::drop_strong_ref to decrement the child's strong count.
    let owned = unsafe {
        BStackOwned::from_raw(
            <MacroStrongParent as BStackBlock>::from_range(parent),
            &alloc,
        )
    };
    drop(owned);
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

    // Dropping the parent recursively frees the child then itself (no panic /
    // error swallowed by Drop); recursion correctness is covered elsewhere.
    drop(parent);
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
    let (child, tag) = bstack_move!(parent).unwrap();
    assert_eq!(tag, 7);

    // Ownership of the child transferred (same allocation), and it is still live
    // because bstack_move! frees only the parent shell.
    assert_eq!(child.handle().range().start(), leaf_off);
    assert_eq!(child.handle().val(stack).unwrap(), 55);

    // Dropping the moved-out child frees the leaf. With the parent shell already
    // freed, both slots coalesce and the lowest (leaf's) is reclaimed.
    drop(child);
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
    let (moved_s, moved_w, n) = bstack_move!(holder).unwrap();
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
#[bstack_block(tag = "TOOLONGTAG12", allow_long_tag)]
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

    // Owned upcast (macro), then a wrong-type downcast hands the slice back.
    let slice = bstack_cast!(leaf as BStackOwnedSlice);
    let slice = match slice.cast_into::<MacroParent>().unwrap() {
        Ok(_) => panic!("tag should not match"),
        Err(s) => s,
    };

    // Correct owned downcast (macro) round-trips to the typed handle.
    let owned = bstack_cast!(slice as BStackOwned<MacroLeaf, _>)
        .unwrap()
        .ok()
        .unwrap();
    assert_eq!(owned.handle().val(stack).unwrap(), 9);
    drop(owned); // frees the leaf
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

    // Only the RcHolder shell was freed; the child is still live. Dropping it
    // reclaims the last block, so the lowest slot (the leaf's) comes back.
    drop(moved_leaf);
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

    drop(moved_leaf); // frees the moved-out child
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
    let (moved_child, n) = bstack_move!(holder).unwrap();
    assert_eq!(n, 7);
    assert_eq!(
        moved_child.as_ref().unwrap().handle().val(stack).unwrap(),
        42
    );
    drop(moved_child); // frees the leaf

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
    drop(empty);
}
