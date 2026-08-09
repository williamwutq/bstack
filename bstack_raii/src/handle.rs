//! Without-allocator inner handles: small `Copy` types constructed transiently
//! during teardown, each encapsulating one field annotation's destruction logic.
//!
//! Keeping the per-annotation logic here (rather than in generated block code)
//! means `#[bstack_block]` can emit a flat, uniform sequence of
//! `.bstack_drop(allocator)?` calls. The with-allocator wrappers
//! ([`crate::BStackOwned`], [`crate::BStackRc`], [`crate::BStackWeak`]) hold one
//! of these plus an allocator reference.
//!
//! All the layout facts these teardowns need are constants in [`crate::layout`]
//! (the injected refcount / control fields sit at fixed offsets after the
//! header, per RAII.md) plus the `OnDisk` / `Control` sizes from
//! [`BStackBlock`] / [`BStackWeakable`]. No per-type layout members are
//! required.

use core::mem::size_of;
use std::io;

use bstack::{BStackOwnedSliceAllocator, BStackRange};
use crate::wal::BStackWalAnchor;

use crate::block::{BStackBlock, BStackWeakable};
use crate::layout;
use crate::refcount;
use crate::reference::BStackRef;
use crate::teardown::{BStackDrop, dealloc_range};

/// `#[bstack_owned]`: an exclusively-owned child.
#[derive(Clone, Copy)]
pub struct OwnedRef<T>(pub BStackRef<T>);

/// `#[bstack_strong]` on a plain `(rc)` `T`: holds just the data ref; teardown
/// decrements the inline refcount and frees at zero.
///
/// The macro only emits this for children whose type is `#[bstack_block(rc)]`,
/// so the inline `refcount` at [`layout::RC_REFCOUNT_OFFSET`] is guaranteed
/// present; the type system does not otherwise enforce it.
#[derive(Clone, Copy)]
pub struct StrongRef<T>(pub BStackRef<T>);

/// `#[bstack_strong]` on an `(rc, weak)` `T`: holds the data ref and the control
/// ref; teardown runs the two-phase strong-then-weak release.
#[derive(Clone, Copy)]
pub struct StrongWeakRef<T: BStackWeakable>(pub BStackRef<T>, pub BStackRef<T::Control>);

/// `#[bstack_weak]` on an `(rc, weak)` `T`: holds only the control ref; teardown
/// decrements `ctrl.weak` and frees the control block at zero. The data block is
/// never touched.
#[derive(Clone, Copy)]
pub struct WeakRef<T: BStackWeakable>(pub BStackRef<T::Control>);

/// Read a data block's `ctrl` back-pointer (a `u64` offset at
/// [`layout::CTRL_BACKPTR_OFFSET`]) and resolve it to a typed control ref,
/// recovering the control block's length from `size_of::<T::Control>()`.
fn read_ctrl_ref<T: BStackWeakable, A: BStackWalAnchor>(
    data_ref: BStackRef<T>,
    allocator: &A,
) -> io::Result<BStackRef<T::Control>> {
    let pos = data_ref.into_range().start() + layout::CTRL_BACKPTR_OFFSET;
    let mut bytes = [0u8; 8];
    allocator.stack().get_into(pos, &mut bytes)?;
    let ctrl_offset = u64::from_le_bytes(bytes);
    let ctrl_range = BStackRange::new(ctrl_offset, size_of::<T::Control>() as u64);
    Ok(unsafe { BStackRef::from_range(ctrl_range) })
}

impl<T: BStackBlock> BStackDrop for OwnedRef<T> {
    fn bstack_drop<A: BStackWalAnchor>(self, allocator: &A) -> io::Result<()> {
        // An owned child is freed by running the block's own recursive teardown,
        // which frees its children (post-order) and then deallocs the block.
        T::from_range(self.0.into_range()).bstack_drop(allocator)
    }
}

impl<T: BStackBlock> BStackDrop for StrongRef<T> {
    fn bstack_drop<A: BStackWalAnchor>(self, allocator: &A) -> io::Result<()> {
        let data_range = self.0.into_range();
        let off = data_range.start() + layout::RC_REFCOUNT_OFFSET;
        // Decrement the inline refcount; only the last owner frees the block.
        if refcount::fetch_sub(allocator.stack(), off, 1)? == 1 {
            T::from_range(data_range).bstack_drop(allocator)?;
        }
        Ok(())
    }
}

impl<T: BStackWeakable> StrongWeakRef<T> {
    /// Resolve the control ref from the data block's `ctrl` back-pointer with a
    /// single read, then pair it with the data ref.
    pub fn from_disk<A: BStackWalAnchor>(
        data_ref: BStackRef<T>,
        allocator: &A,
    ) -> io::Result<Self> {
        let ctrl = read_ctrl_ref(data_ref, allocator)?;
        Ok(StrongWeakRef(data_ref, ctrl))
    }
}

/// The two-phase strong release for an `(rc, weak)` block, given raw data and
/// control ranges. Requires only `T: BStackBlock` (for the data block's own
/// recursive teardown), so it is shared by both [`StrongWeakRef::bstack_drop`]
/// and [`crate::BStackRc`]'s `Drop` — the latter carries `T: BStackBlock` and so
/// cannot construct a `StrongWeakRef<T>` (which needs `BStackWeakable`) itself.
pub(crate) fn strong_release_ctrl<T: BStackBlock, A: BStackWalAnchor>(
    allocator: &A,
    data_range: BStackRange,
    ctrl_range: BStackRange,
) -> io::Result<()> {
    let stack = allocator.stack();
    let strong_off = ctrl_range.start() + layout::CTRL_STRONG_OFFSET;
    // Phase 1: last strong owner frees the data block (children + shell), then
    // releases the phantom weak the strong owners collectively held.
    if refcount::fetch_sub(stack, strong_off, 1)? == 1 {
        T::from_range(data_range).bstack_drop(allocator)?;
        let weak_off = ctrl_range.start() + layout::CTRL_WEAK_OFFSET;
        // Phase 2 (early): if no real weak handles remain, the phantom release
        // drives weak to zero and the control block is freed here.
        if refcount::fetch_sub(stack, weak_off, 1)? == 1 {
            unsafe { dealloc_range(allocator, ctrl_range)? };
        }
    }
    Ok(())
}

impl<T: BStackWeakable> BStackDrop for StrongWeakRef<T> {
    fn bstack_drop<A: BStackWalAnchor>(self, allocator: &A) -> io::Result<()> {
        strong_release_ctrl::<T, A>(allocator, self.0.into_range(), self.1.into_range())
    }
}

impl<T: BStackWeakable> WeakRef<T> {
    /// Resolve the control ref from the data block's `ctrl` back-pointer.
    pub fn from_disk<A: BStackWalAnchor>(
        data_ref: BStackRef<T>,
        allocator: &A,
    ) -> io::Result<Self> {
        Ok(WeakRef(read_ctrl_ref(data_ref, allocator)?))
    }
}

impl<T: BStackWeakable> BStackDrop for WeakRef<T> {
    fn bstack_drop<A: BStackWalAnchor>(self, allocator: &A) -> io::Result<()> {
        let ctrl_range = self.0.into_range();
        let weak_off = ctrl_range.start() + layout::CTRL_WEAK_OFFSET;
        // Decrement ctrl.weak; free the control block when the last weak handle
        // (or the phantom) drops it to zero. The data block is never touched.
        if refcount::fetch_sub(allocator.stack(), weak_off, 1)? == 1 {
            unsafe { dealloc_range(allocator, ctrl_range)? };
        }
        Ok(())
    }
}
