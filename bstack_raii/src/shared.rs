//! [`BStackRc`] + [`BStackWeak`]: the with-allocator shared handles.

use std::io;

use bstack::{BStackOwnedSliceAllocator, BStackRange};

use crate::block::{BStackBlock, BStackWeakable};
use crate::clone::TryClone;
use crate::reference::BStackRef;

/// A shared, refcounted, allocator-bound handle.
///
/// Serves **both** block kinds. `ctrl` distinguishes them at runtime:
/// * `None` — a plain `#[bstack_block(rc)]` block, whose refcount lives inline
///   in the data block.
/// * `Some(range)` — an `#[bstack_block(rc, weak)]` block, whose `strong`/`weak`
///   counters live in a separate control block at `range`.
///
/// Carrying this as a runtime `Option` (rather than a type-level split via an
/// associated `Strong` handle) keeps `BStackRc<'a, T, A>`'s public signature
/// fixed at three parameters. If a zero-cost, branch-free representation is
/// wanted later, it can be introduced behind this same signature without
/// breaking callers. Freeing at zero reconstructs the appropriate inner handle
/// ([`crate::StrongRef`] / [`crate::StrongWeakRef`]) — note the `Some` path only
/// needs `T: BStackBlock` (it drives the raw control-block counters directly),
/// so `BStackRc` need not bound `T: BStackWeakable`.
pub struct BStackRc<'a, T: BStackBlock, A: BStackOwnedSliceAllocator> {
    data: BStackRef<T>,
    ctrl: Option<BStackRange>,
    allocator: &'a A,
}

impl<'a, T: BStackBlock, A: BStackOwnedSliceAllocator> TryClone for BStackRc<'a, T, A> {
    fn try_clone(&self) -> io::Result<Self> {
        // Increment the strong count (inline for `None`, `ctrl.strong` for
        // `Some`), then return a new handle over the same refs + allocator.
        todo!("atomically increment strong count, then clone the handle")
    }
}

impl<'a, T: BStackWeakable, A: BStackOwnedSliceAllocator> BStackRc<'a, T, A> {
    /// Create a weak handle to the same block by incrementing `ctrl.weak`.
    ///
    /// Available only for `(rc, weak)` blocks (`T: BStackWeakable`), so a plain
    /// `(rc)` block's `BStackRc` has no `downgrade` at all — a compile error, not
    /// a runtime hazard.
    pub fn downgrade(&self) -> io::Result<BStackWeak<'a, T, A>> {
        todo!("increment ctrl.weak; wrap the control ref into a BStackWeak")
    }
}

impl<'a, T: BStackBlock, A: BStackOwnedSliceAllocator> Drop for BStackRc<'a, T, A> {
    fn drop(&mut self) {
        // `None`: StrongRef(self.data).bstack_drop(..).
        // `Some(ctrl)`: raw two-phase release driving ctrl.strong/ctrl.weak,
        // using T::bstack_drop for the data block (no T: BStackWeakable needed).
        todo!("decrement strong count; free at zero per block kind")
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
    /// Attempt to promote to a strong handle. Succeeds iff `ctrl.strong` is
    /// currently non-zero (CAS-increment-if-nonzero), reading `ctrl.x` to
    /// recover the data ref. Returns `None` if the data block is already gone.
    pub fn upgrade(&self) -> io::Result<Option<BStackRc<'a, T, A>>> {
        todo!("CAS-increment ctrl.strong if nonzero; on success build a BStackRc")
    }
}

impl<'a, T: BStackWeakable, A: BStackOwnedSliceAllocator> TryClone for BStackWeak<'a, T, A> {
    fn try_clone(&self) -> io::Result<Self> {
        todo!("increment ctrl.weak, then clone the handle")
    }
}

impl<'a, T: BStackWeakable, A: BStackOwnedSliceAllocator> Drop for BStackWeak<'a, T, A> {
    fn drop(&mut self) {
        // WeakRef(self.ctrl).bstack_drop(self.allocator): decrement ctrl.weak,
        // free the control block at zero.
        todo!("decrement ctrl.weak; free control block at zero")
    }
}
