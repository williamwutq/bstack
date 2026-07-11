//! Disk-level recursive destruction, fully decoupled from Rust's `Drop`.
//!
//! [`BStackDrop`] is implemented by every `#[bstack_block]` type (frees the
//! block and recurses into its owned children) and by the small child-handle
//! types in [`crate::handle`]. It takes `self` (a *without-allocator* handle)
//! plus an explicit allocator, so it is generic over all handle-like types.

use core::mem::ManuallyDrop;
use core::ops::Deref;
use std::io;

use bstack::{BStackOwnedSlice, BStackOwnedSliceAllocator, BStackRange};

/// Recursively free a block and all of its owned children.
///
/// Because a bare [`BStackRange`] carries no allocator, freeing is done by
/// reconstructing a [`BStackOwnedSlice`] and handing it to the allocator's
/// `dealloc` — see [`dealloc_range`]. There is deliberately no `dealloc_range`
/// method on the allocator trait itself.
///
/// The allocator is bound to [`BStackOwnedSliceAllocator`] rather than the bare
/// `BStackAllocator`: that supertrait pins `Allocated<'a> = BStackOwnedSlice<'a,
/// A>` (so a reconstructed owned slice is the accepted `dealloc` handle) and
/// `Error = io::Error` (so the layer speaks [`io::Result`]).
pub trait BStackDrop: Sized {
    fn bstack_drop<A: BStackOwnedSliceAllocator>(self, allocator: &A) -> io::Result<()>;
}

/// Free a raw block range by reconstructing an owned slice and delegating to the
/// allocator. The central sink the generated `bstack_drop` code funnels through,
/// since ranges carry no allocator of their own.
///
/// # Safety
/// `range` must be a live allocation owned by `allocator` that no other live
/// handle will also free.
pub unsafe fn dealloc_range<A: BStackOwnedSliceAllocator>(
    allocator: &A,
    range: BStackRange,
) -> io::Result<()> {
    let owned: BStackOwnedSlice<'_, A> =
        unsafe { BStackOwnedSlice::from_raw_range(allocator, range) };
    allocator.dealloc(owned).map_err(|e| e.source)
}

/// A guard that runs [`BStackDrop::bstack_drop`] on its inner handle when it goes
/// out of scope, bridging fallible on-disk teardown to Rust's `Drop`.
///
/// It is the *one* place that calls `bstack_drop` from a `Drop` impl: every
/// allocator-bound handle that wants automatic cleanup is (or embeds) an
/// `AutoDrop`, rather than hand-writing its own `Drop`. A bare [`BStackDrop`]
/// handle that is not wrapped frees nothing on its own — its `bstack_drop` is
/// invoked explicitly, or runs as a child of a parent block's recursive
/// teardown.
///
/// It is a newtype over `(ManuallyDrop<T>, &'a A)`; `bstack_move!` and the raw
/// accessors defuse it via [`into_raw_parts`](Self::into_raw_parts) so no
/// parallel destruction path exists.
pub struct AutoDrop<'a, T: BStackDrop, A: BStackOwnedSliceAllocator> {
    inner: ManuallyDrop<T>,
    allocator: &'a A,
}

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
