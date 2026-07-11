//! [`AutoDrop`]: the generic RAII guard bridging [`BStackDrop`] to Rust `Drop`.
//!
//! On-disk teardown ([`BStackDrop::bstack_drop`]) is fallible and needs an
//! allocator, so it can't live directly in a `Drop` impl. `AutoDrop<T>` pairs a
//! `BStackDrop` handle with its allocator and runs the teardown on scope exit
//! (swallowing the error, matching the contract of `Drop`). It is the *one* place
//! that calls `bstack_drop` from a `Drop` impl — every allocator-bound handle
//! that wants automatic cleanup is either an `AutoDrop` (as [`BStackOwned`] is)
//! or embeds one, rather than hand-writing its own `Drop`.
//!
//! Without wrapping in `AutoDrop`, a bare `BStackDrop` handle frees nothing on
//! its own: its `bstack_drop` is invoked explicitly, or runs as a child of some
//! parent block's recursive teardown.

use core::mem::ManuallyDrop;
use core::ops::Deref;
use std::io;

use bstack::BStackOwnedSliceAllocator;

use crate::block::{BStackMove, BStackMoveExpr};
use crate::teardown::BStackDrop;

/// A guard that runs [`BStackDrop::bstack_drop`] on its inner handle when it goes
/// out of scope, bridging fallible on-disk teardown to Rust's `Drop`.
///
/// It is a newtype over `(ManuallyDrop<T>, &'a A)`. `bstack_move!` and the raw
/// accessors defuse it via [`into_raw_parts`](Self::into_raw_parts) so no
/// parallel destruction path exists.
pub struct AutoDrop<'a, T: BStackDrop, A: BStackOwnedSliceAllocator> {
    inner: ManuallyDrop<T>,
    allocator: &'a A,
}

/// An owned, allocator-bound handle to a block whose `Drop` recursively frees it
/// on disk. Just an [`AutoDrop`] over the block type itself (whose
/// [`BStackDrop`] is the recursive free), so a fresh block hands back an
/// auto-freeing handle with no bespoke `Drop`.
pub type BStackOwned<'a, T, A> = AutoDrop<'a, T, A>;

impl<'a, T: BStackDrop, A: BStackOwnedSliceAllocator> AutoDrop<'a, T, A> {
    /// Pair an inner handle with its allocator into an auto-dropping guard.
    ///
    /// # Safety
    /// The caller asserts `inner` describes a live allocation owned by
    /// `allocator` and that no other handle will also free it.
    pub unsafe fn from_raw(inner: T, allocator: &'a A) -> Self {
        Self {
            inner: ManuallyDrop::new(inner),
            allocator,
        }
    }

    /// Split into the raw inner handle and allocator **without** running the
    /// disk-level `Drop`. The caller takes over responsibility for the
    /// allocation (e.g. `bstack_move!`, which frees only the parent shell).
    pub fn into_raw_parts(self) -> (T, &'a A) {
        // Wrapping `self` in ManuallyDrop defuses our own `Drop`, so
        // `bstack_drop` is not called; then move the inner `T` out.
        let mut me = ManuallyDrop::new(self);
        let inner = unsafe { ManuallyDrop::take(&mut me.inner) };
        (inner, me.allocator)
    }

    /// The allocator this handle is bound to.
    pub fn allocator(&self) -> &'a A {
        self.allocator
    }

    /// Borrow the underlying handle, e.g. to call generated field accessors:
    /// `owned.handle().field(stack)`.
    pub fn handle(&self) -> &T {
        &self.inner
    }
}

impl<'a, T: BStackDrop, A: BStackOwnedSliceAllocator> Deref for AutoDrop<'a, T, A> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<'a, T: BStackDrop, A: BStackOwnedSliceAllocator> Drop for AutoDrop<'a, T, A> {
    fn drop(&mut self) {
        let inner = unsafe { ManuallyDrop::take(&mut self.inner) };
        // Errors are swallowed, matching the contract of Rust's `Drop`.
        let _ = inner.bstack_drop(self.allocator);
    }
}

impl<'a, T: BStackMove, A: BStackOwnedSliceAllocator> BStackMoveExpr for AutoDrop<'a, T, A> {
    // A unique owner: the destructure is always valid.
    type Output = io::Result<T::Fields<'a, A>>;
    fn bstack_move(self) -> Self::Output {
        T::bstack_move(self)
    }
}
