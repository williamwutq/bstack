//! [`BStackOwned`]: the with-allocator unique handle.
//!
//! A newtype over `(ManuallyDrop<T>, &'a A)`. Rust's `Drop` takes the inner `T`
//! out and calls [`BStackDrop::bstack_drop`]; errors are swallowed, matching the
//! contract of `Drop`. `bstack_move!` consumes it via [`BStackOwned::into_raw_parts`],
//! which defuses this `Drop` so no parallel destruction path exists.

use core::mem::ManuallyDrop;
use std::io;

use bstack::BStackOwnedSliceAllocator;

use crate::block::{BStackMove, BStackMoveExpr};
use crate::teardown::BStackDrop;

/// An owned, allocator-bound handle to a block whose `Drop` recursively frees it
/// on disk via [`BStackDrop`].
pub struct BStackOwned<'a, T: BStackDrop, A: BStackOwnedSliceAllocator> {
    inner: ManuallyDrop<T>,
    allocator: &'a A,
}

impl<'a, T: BStackDrop, A: BStackOwnedSliceAllocator> BStackOwned<'a, T, A> {
    /// Wrap an inner handle and allocator into an owned handle.
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
    /// disk-level `Drop`. This is the destructuring entry point `bstack_move!`
    /// uses; the caller takes over responsibility for the allocation.
    pub fn into_raw_parts(self) -> (T, &'a A) {
        // Wrapping `self` in ManuallyDrop prevents our own `Drop` from running,
        // so `bstack_drop` is not called; then move the inner `T` out.
        let mut me = ManuallyDrop::new(self);
        let inner = unsafe { ManuallyDrop::take(&mut me.inner) };
        (inner, me.allocator)
    }

    /// The allocator this handle is bound to.
    pub fn allocator(&self) -> &'a A {
        self.allocator
    }

    /// Borrow the underlying typed handle, e.g. to call generated field
    /// accessors: `owned.handle().field(stack)`.
    pub fn handle(&self) -> &T {
        &self.inner
    }
}

impl<'a, T: BStackDrop, A: BStackOwnedSliceAllocator> Drop for BStackOwned<'a, T, A> {
    fn drop(&mut self) {
        let inner = unsafe { ManuallyDrop::take(&mut self.inner) };
        let _ = inner.bstack_drop(self.allocator);
    }
}

impl<'a, T: BStackMove, A: BStackOwnedSliceAllocator> BStackMoveExpr for BStackOwned<'a, T, A> {
    // A unique owner: the destructure is always valid.
    type Output = io::Result<T::Fields<'a, A>>;
    fn bstack_move(self) -> Self::Output {
        T::bstack_move(self)
    }
}
