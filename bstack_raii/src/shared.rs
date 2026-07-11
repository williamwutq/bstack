//! [`BStackRc`] + [`BStackWeak`]: the with-allocator shared handles.

use core::mem::size_of;
use std::io;

use bstack::{BStackOwnedSliceAllocator, BStackRange};

use crate::block::{BStackBlock, BStackWeakable};
use crate::clone::TryClone;
use crate::handle::{StrongRef, WeakRef, strong_release_ctrl};
use crate::layout;
use crate::refcount;
use crate::reference::BStackRef;
use crate::teardown::BStackDrop;

/// A shared, refcounted, allocator-bound handle.
///
/// Serves **both** block kinds. `ctrl` distinguishes them at runtime:
/// * `None` — a plain `#[bstack_block(rc)]` block, whose refcount lives inline
///   in the data block at [`layout::RC_REFCOUNT_OFFSET`].
/// * `Some(range)` — an `#[bstack_block(rc, weak)]` block, whose `strong`/`weak`
///   counters live in a separate control block at `range`.
///
/// Carrying this as a runtime `Option` (rather than a type-level split via an
/// associated `Strong` handle) keeps `BStackRc<'a, T, A>`'s public signature
/// fixed at three parameters; a zero-cost representation can replace it later
/// without breaking callers. Freeing at zero reuses [`StrongRef`] (the `None`
/// path) or [`strong_release_ctrl`] (the `Some` path) — the latter needs only
/// `T: BStackBlock`, so `BStackRc` need not bound `T: BStackWeakable`.
///
/// **Invariant:** for a `T: BStackWeakable` block, `ctrl` is always `Some` — such
/// blocks are only ever constructed through the control-block paths
/// ([`BStackWeak::upgrade`], `bstack_move!`). `downgrade` relies on this.
pub struct BStackRc<'a, T: BStackBlock, A: BStackOwnedSliceAllocator> {
    data: BStackRef<T>,
    ctrl: Option<BStackRange>,
    allocator: &'a A,
}

impl<'a, T: BStackBlock, A: BStackOwnedSliceAllocator> BStackRc<'a, T, A> {
    /// Reconstruct a shared handle from its raw parts.
    ///
    /// # Safety
    /// The refs must describe a live `(rc)` / `(rc, weak)` block owned by
    /// `allocator`, and this handle must account for a strong count the caller
    /// has already established (e.g. the allocation's initial `strong = 1`, or a
    /// count bumped by `upgrade`). `ctrl` must be `Some` iff the block is
    /// `(rc, weak)`.
    pub unsafe fn from_raw(
        data: BStackRef<T>,
        ctrl: Option<BStackRange>,
        allocator: &'a A,
    ) -> Self {
        Self {
            data,
            ctrl,
            allocator,
        }
    }

    /// The underlying typed handle, e.g. to call generated field accessors:
    /// `rc.handle().field(stack)`. Cheap: it just re-wraps the data ref and does
    /// not touch the refcount.
    pub fn handle(&self) -> T {
        <T as BStackBlock>::from_range(self.data.into_range())
    }

    /// Consume the handle into its raw parts **without** decrementing the strong
    /// count — the count is transferred to the caller (e.g. into a parent's
    /// `#[bstack_strong]` field). `ctrl` is `Some` for `(rc, weak)` blocks.
    pub fn into_raw(self) -> (BStackRef<T>, Option<BStackRange>) {
        let me = core::mem::ManuallyDrop::new(self);
        (me.data, me.ctrl)
    }

    /// Byte offset of the strong counter for this handle's block kind.
    fn strong_offset(&self) -> u64 {
        match self.ctrl {
            None => self.data.into_range().start() + layout::RC_REFCOUNT_OFFSET,
            Some(ctrl) => ctrl.start() + layout::CTRL_STRONG_OFFSET,
        }
    }
}

impl<'a, T: BStackBlock, A: BStackOwnedSliceAllocator> TryClone for BStackRc<'a, T, A> {
    fn try_clone(&self) -> io::Result<Self> {
        refcount::fetch_add(self.allocator.stack(), self.strong_offset(), 1)?;
        Ok(Self {
            data: self.data,
            ctrl: self.ctrl,
            allocator: self.allocator,
        })
    }
}

impl<'a, T: BStackWeakable, A: BStackOwnedSliceAllocator> BStackRc<'a, T, A> {
    /// Create a weak handle to the same block by incrementing `ctrl.weak`.
    ///
    /// Available only for `(rc, weak)` blocks (`T: BStackWeakable`), so a plain
    /// `(rc)` block's `BStackRc` has no `downgrade` at all — a compile error, not
    /// a runtime hazard.
    pub fn downgrade(&self) -> io::Result<BStackWeak<'a, T, A>> {
        // Invariant: a weakable block's `BStackRc` always carries a control ref.
        let ctrl_range = self
            .ctrl
            .expect("BStackRc<T: BStackWeakable> always has a control block");
        let weak_off = ctrl_range.start() + layout::CTRL_WEAK_OFFSET;
        refcount::fetch_add(self.allocator.stack(), weak_off, 1)?;
        let ctrl = unsafe { BStackRef::<T::Control>::from_range(ctrl_range) };
        Ok(BStackWeak {
            ctrl,
            allocator: self.allocator,
        })
    }
}

impl<'a, T: BStackBlock, A: BStackOwnedSliceAllocator> Drop for BStackRc<'a, T, A> {
    fn drop(&mut self) {
        // Errors are swallowed, matching the contract of Rust's `Drop`.
        let _ = match self.ctrl {
            None => StrongRef(self.data).bstack_drop(self.allocator),
            Some(ctrl) => strong_release_ctrl::<T, A>(self.allocator, self.data.into_range(), ctrl),
        };
    }
}

/// A non-owning weak handle to an `(rc, weak)` block's control block.
///
/// Obtained from [`BStackRc::downgrade`] or [`TryClone::try_clone`]. It keeps the
/// control block alive (so [`upgrade`](BStackWeak::upgrade) can check liveness)
/// but never pins the data block.
pub struct BStackWeak<'a, T: BStackWeakable, A: BStackOwnedSliceAllocator> {
    ctrl: BStackRef<T::Control>,
    allocator: &'a A,
}

impl<'a, T: BStackWeakable, A: BStackOwnedSliceAllocator> BStackWeak<'a, T, A> {
    /// Reconstruct a weak handle from its raw control ref.
    ///
    /// # Safety
    /// `ctrl` must describe a live control block owned by `allocator`, and this
    /// handle must account for a weak count the caller has already established.
    pub unsafe fn from_raw(ctrl: BStackRef<T::Control>, allocator: &'a A) -> Self {
        Self { ctrl, allocator }
    }

    /// Consume the handle into its raw control ref **without** decrementing the
    /// weak count — the count is transferred to the caller.
    pub fn into_raw(self) -> BStackRef<T::Control> {
        let me = core::mem::ManuallyDrop::new(self);
        me.ctrl
    }

    /// Attempt to promote to a strong handle. Succeeds iff `ctrl.strong` is
    /// currently non-zero (CAS-increment-if-nonzero), reading `ctrl.x` to recover
    /// the data ref. Returns `None` if the data block is already gone.
    pub fn upgrade(&self) -> io::Result<Option<BStackRc<'a, T, A>>> {
        let stack = self.allocator.stack();
        let ctrl_range = self.ctrl.into_range();
        let strong_off = ctrl_range.start() + layout::CTRL_STRONG_OFFSET;
        if refcount::increment_if_nonzero(stack, strong_off)?.is_none() {
            return Ok(None);
        }
        // Strong is now claimed; recover the data ref from the forward pointer.
        let data_pos = ctrl_range.start() + layout::CTRL_DATA_OFFSET;
        let mut bytes = [0u8; 8];
        stack.get_into(data_pos, &mut bytes)?;
        let data_range = BStackRange::new(u64::from_le_bytes(bytes), size_of::<T::OnDisk>() as u64);
        let data = unsafe { BStackRef::<T>::from_range(data_range) };
        Ok(Some(BStackRc {
            data,
            ctrl: Some(ctrl_range),
            allocator: self.allocator,
        }))
    }
}

impl<'a, T: BStackWeakable, A: BStackOwnedSliceAllocator> TryClone for BStackWeak<'a, T, A> {
    fn try_clone(&self) -> io::Result<Self> {
        let weak_off = self.ctrl.into_range().start() + layout::CTRL_WEAK_OFFSET;
        refcount::fetch_add(self.allocator.stack(), weak_off, 1)?;
        Ok(Self {
            ctrl: self.ctrl,
            allocator: self.allocator,
        })
    }
}

impl<'a, T: BStackWeakable, A: BStackOwnedSliceAllocator> Drop for BStackWeak<'a, T, A> {
    fn drop(&mut self) {
        // Decrement ctrl.weak; free the control block at zero. Errors swallowed.
        let _ = WeakRef::<T>(self.ctrl).bstack_drop(self.allocator);
    }
}
