//! Disk-level recursive destruction, fully decoupled from Rust's `Drop`.
//!
//! [`BStackDrop`] is implemented by every `#[bstack_block]` type (frees the
//! block and recurses into its owned children) and by the small child-handle
//! types in [`crate::handle`]. It takes `self` (a *without-allocator* handle)
//! plus an explicit allocator, so it is generic over all handle-like types.

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
