//! Typed ↔ untyped handle conversion — the runtime behind `bstack_cast!`.
//!
//! Upcasts (typed → untyped) are infallible. Downcasts (untyped → typed) check
//! the block's [`EightCC`] tag against the target type's and are fallible. The
//! borrowed upcast (`X` → [`BStackSlice`]) is a generated `X::as_slice(stack)`
//! method, since a bare handle carries no stack.

use std::io;

use bstack::{BStackOwnedSlice, BStackOwnedSliceAllocator, BStackSlice};

use crate::block::{BStackBlock, BStackCast};
use crate::layout::EightCC;
use crate::owned::BStackOwned;

/// Byte offset of the `tag` within a [`crate::BlockHeader`] (`size: u64` first).
const TAG_OFFSET: u64 = 8;

impl<'a, T: BStackBlock, A: BStackOwnedSliceAllocator> BStackOwned<'a, T, A> {
    /// Upcast to the untyped owned slice, discarding type info (infallible).
    ///
    /// Consumes the handle without running its disk-level `Drop`; the returned
    /// slice owns the allocation.
    pub fn into_slice(self) -> BStackOwnedSlice<'a, A> {
        let (inner, allocator) = self.into_raw_parts();
        unsafe { BStackOwnedSlice::from_raw_range(allocator, inner.range()) }
    }
}

/// Downcast an owned slice to a typed owned handle by checking the block tag.
pub trait BStackCastInto<'a, A: BStackOwnedSliceAllocator>: Sized {
    /// `Ok(Ok(owned))` on a tag match; `Ok(Err(self))` on mismatch (ownership is
    /// handed back so the caller can try another type); `Err` on an I/O failure
    /// reading the header.
    fn cast_into<T: BStackBlock>(self) -> io::Result<Result<BStackOwned<'a, T, A>, Self>>;
}

impl<'a, A: BStackOwnedSliceAllocator> BStackCastInto<'a, A> for BStackOwnedSlice<'a, A> {
    fn cast_into<T: BStackBlock>(self) -> io::Result<Result<BStackOwned<'a, T, A>, Self>> {
        let mut tag = [0u8; 8];
        self.read_range_into(TAG_OFFSET, &mut tag)?;
        if EightCC(tag) != T::eightcc() {
            return Ok(Err(self));
        }
        let allocator = self.allocator();
        let range = self.as_range();
        Ok(Ok(unsafe {
            BStackOwned::from_raw(T::from_range(range), allocator)
        }))
    }
}

/// Downcast a borrowed slice to a typed handle by checking the block tag.
pub trait BStackCastAs<'a> {
    /// `Some(handle)` on a tag match, `None` on mismatch, `Err` on an I/O
    /// failure reading the header.
    fn cast_as<T: BStackBlock>(&self) -> io::Result<Option<T>>;
}

impl<'a> BStackCastAs<'a> for BStackSlice<'a> {
    fn cast_as<T: BStackBlock>(&self) -> io::Result<Option<T>> {
        let mut tag = [0u8; 8];
        self.read_range_into(TAG_OFFSET, &mut tag)?;
        if EightCC(tag) != T::eightcc() {
            return Ok(None);
        }
        Ok(Some(T::from_range(self.as_range())))
    }
}
