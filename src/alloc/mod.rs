//! Allocator abstraction for [`BStack`]-backed region management.
//!
//! # Overview
//!
//! This module provides three region handle types and a pair of allocator traits:
//!
//! * [`BStackRange`] — a raw `(offset, len)` coordinate pair with no backing
//!   reference.  `Copy`, serializable, suitable for on-disk storage.  No I/O.
//!
//! * [`BStackOwnedSlice<'a, A>`](BStackOwnedSlice) — the **ownership handle**
//!   for one allocation.  Returned by [`alloc`](BStackAllocator::alloc) and
//!   consumed by [`realloc`](BStackAllocator::realloc) /
//!   [`dealloc`](BStackAllocator::dealloc).  Non-`Copy`, non-`Clone`.
//!   No direct I/O; use [`as_slice`](BStackOwnedSlice::as_slice) or
//!   [`as_slice_mut`](BStackOwnedSlice::as_slice_mut) to get a view.
//!
//! * [`BStackSlice<'a>`](BStackSlice) — a **borrowed I/O view**.  Does not
//!   carry an allocator; carries `&'a BStack` directly.  Non-`Copy`, `Clone`.
//!   Exposes `read*(&self)` and (with `set`) `write*(&mut self)`.  Subsliceable.
//!
//! * [`BStackSliceReader`] — a cursor-based reader ([`io::Read`] + [`io::Seek`]).
//!
//! * [`BStackSliceWriter`] — a cursor-based writer ([`io::Write`] + [`io::Seek`],
//!   `set` feature).
//!
//! * [`BStackAllocator`] — allocator trait.  `alloc`/`realloc`/`dealloc`
//!   take and return `Self::Allocated<'a>`, which must implement
//!   `Into<BStackOwnedSlice<'a, Self>>`.  [`into_stack`](BStackAllocator::into_stack)
//!   consumes the allocator; outstanding owned slices statically prevent this.
//!
//! * [`BStackBulkAllocator`] — extension trait for atomic bulk
//!   [`alloc_bulk`](BStackBulkAllocator::alloc_bulk) /
//!   [`dealloc_bulk`](BStackBulkAllocator::dealloc_bulk).
//!
//! * [`BStackOwnedSliceAllocator`] — convenience supertrait:
//!   `BStackAllocator<Error = io::Error, Allocated<'a> = BStackOwnedSlice<'a, Self>>`.
//!
//! * [`BStackByteVec`] — growable `u8` vector backed by a [`BStack`] allocation
//!   (`alloc` + `set`).  16-byte header stores `len`/`cap` for crash recovery.
//!
//! * Standard allocator implementations: [`LinearBStackAllocator`], [`FirstFitBStackAllocator`],
//!   [`GhostTreeBstackAllocator`], [`SlabBStackAllocator`], and [`CheckedSlabBStackAllocator`].
//!
//! # Standard Allocators
//!
//! * [`LinearBStackAllocator`] — bump allocator that always appends to the tail.
//!   `Send` without `atomic`; `Send + Sync` with `atomic`.
//!
//! * [`FirstFitBStackAllocator`] — persistent first-fit free-list allocator
//!   (`alloc` + `set`).  Adjacent free blocks coalesce on dealloc.
//!   `Send` without `atomic`; `Send + Sync` with `atomic`.
//!
//! * [`GhostTreeBstackAllocator`] — pure-AVL general-purpose allocator
//!   (`alloc` feature).  Zero per-block overhead.
//!   `Send` in all configurations; `Send + Sync` with `atomic`.
//!
//! * [`SlabBStackAllocator`] — fixed-block slab allocator (`alloc` + `set`).
//!   O(1) alloc/dealloc.  *Experimental.*
//!
//! * [`CheckedSlabBStackAllocator`] — crash-recoverable slab variant (`alloc` + `set`).
//!   8-byte per-block header tracks state; double-frees caught.  *Experimental.*
//!
//! # Region handle design
//!
//! The three handle types cleanly separate concerns:
//!
//! | Type                                         | Carries      | Copy | I/O      | Alloc ops |
//! |----------------------------------------------|--------------|------|----------|-----------|
//! | [`BStackRange`]                              | nothing      | yes  | no       | no        |
//! | [`BStackOwnedSlice<'a,A>`](BStackOwnedSlice) | `&'a A`      | no   | via view | yes       |
//! | [`BStackSlice<'a>`](BStackSlice)             | `&'a BStack` | no   | yes      | no        |
//!
//! `BStackOwnedSlice` is non-`Copy` and non-`Clone`: an allocation has exactly
//! one owner.  Consuming it for `realloc` or `dealloc` is a compile error if
//! any view (`BStackSlice`) derived from it is still live — views are tied to
//! the handle's borrow by `as_slice<'s>(&'s self) -> BStackSlice<'s>`.
//!
//! `BStackSlice` is non-`Copy` so that `write*(&mut self)` provides genuine
//! single-writer exclusivity within safe code: a slice cannot be silently
//! duplicated out of a `&mut` borrow.  It is `Clone` for explicit second views.
//!
//! # Feature flags
//!
//! The `alloc` Cargo feature enables this module, including all allocator traits,
//! handle types, and [`LinearBStackAllocator`] / [`GhostTreeBstackAllocator`]:
//!
//! ```toml
//! bstack = { version = "0.1", features = ["alloc"] }
//! ```
//!
//! [`BStackSliceWriter`], [`FirstFitBStackAllocator`], [`SlabBStackAllocator`],
//! [`CheckedSlabBStackAllocator`], and [`BStackByteVec`] additionally require `set`:
//!
//! ```toml
//! bstack = { version = "0.1", features = ["alloc", "set"] }
//! ```
//!
//! # Crash consistency
//!
//! Every individual [`BStack`] operation performs a durable sync before returning.
//! At the allocator level, operations spanning multiple [`BStack`] calls are not
//! automatically atomic.  Each allocator documents which operations are
//! single-call (crash-safe by inheritance) and which are multi-call (requiring
//! explicit recovery design, typically write-ahead ordering).
//!
//! # Trait implementations
//!
//! All three handle types implement `PartialEq`/`Eq` and `PartialOrd`/`Ord` on
//! `(offset, len)`, and `Hash` consistently.  `BStackRange` also implements
//! `From<[u8; 16]>` and `From<BStackRange> for [u8; 16]` for serialization.
//!
//! `BStackSliceReader` and `BStackSliceWriter` implement `PartialEq`/`PartialOrd`
//! by absolute payload position.  Both cross-compare with each other and with a
//! bare `BStackSlice` (slice comparison ignores cursor).

use crate::BStack;
use std::fmt;
use std::io;

pub mod slice;
#[cfg(feature = "set")]
pub use slice::BStackSliceWriter;
pub use slice::{BStackOwnedSlice, BStackRange, BStackSlice, BStackSliceReader};

/// A trait for types that own a [`BStack`] and manage contiguous byte regions
/// within its payload.
///
/// # Ownership model
///
/// An implementor takes ownership of a [`BStack`].  [`BStackOwnedSlice`] handles
/// produced by [`alloc`](Self::alloc) borrow the allocator for lifetime `'_`,
/// which prevents the allocator from being consumed by
/// [`into_stack`](Self::into_stack) while any slice is alive.  The canonical
/// pattern:
///
/// ```rust,ignore
/// struct MyAllocator { stack: BStack }
///
/// impl BStackAllocator for MyAllocator {
///     // Or some richer type that implements Debug + Display
///     type Error = io::Error;
///     
///     // Or some richer type that implements Into<BStackOwnedSlice<'a, Self>>
///     type Allocated<'a> = BStackOwnedSlice<'a, Self>;
///
///     fn stack(&self) -> &BStack { &self.stack }
///     fn into_stack(self) -> BStack { self.stack }
///     fn alloc(&self, len: u64) -> io::Result<BStackOwnedSlice<'_, Self>> { ... }
///     fn realloc<'a>(&'a self, handle: BStackOwnedSlice<'a, Self>, new_len: u64)
///         -> io::Result<BStackOwnedSlice<'a, Self>> { ... }
/// }
/// ```
///
/// # Crash consistency
///
/// Implementors **must** document the crash-consistency class of each
/// operation they provide. As a rule of thumb: if every method maps to a
/// single [`BStack`] call it is crash-safe by inheritance; if any method
/// issues two or more calls it requires an explicit recovery design.
///
/// # See also
///
/// [`BStackBulkAllocator`] — extension trait that adds atomic bulk
/// [`alloc_bulk`](BStackBulkAllocator::alloc_bulk) and
/// [`dealloc_bulk`](BStackBulkAllocator::dealloc_bulk) methods for
/// allocators that can batch multiple operations into a single I/O call.
pub trait BStackAllocator: Sized {
    /// The error type returned by [`alloc`](Self::alloc),
    /// [`realloc`](Self::realloc), [`dealloc`](Self::dealloc),
    /// [`alloc_bulk`](BStackBulkAllocator::alloc_bulk), and
    /// [`dealloc_bulk`](BStackBulkAllocator::dealloc_bulk).
    ///
    /// Must implement [`fmt::Debug`] and [`fmt::Display`] so that errors can be
    /// printed and propagated with `?`.
    ///
    /// All allocators provided by this library set `Error = `[`io::Error`].
    /// Third-party implementations may use a richer type, but are encouraged
    /// to follow the same convention for interoperability.
    type Error: fmt::Debug + fmt::Display;

    /// The handle type returned by [`alloc`](Self::alloc) and
    /// [`realloc`](Self::realloc), and accepted by [`realloc`](Self::realloc)
    /// and [`dealloc`](Self::dealloc).
    ///
    /// Must implement `Into<BStackOwnedSlice<'a, Self>>` so that generic code
    /// can extract the underlying owned slice.  Custom allocators may embed
    /// additional metadata in a newtype; the `Into` conversion discards the
    /// metadata and yields the raw allocation handle.
    ///
    /// All allocators provided by this library set
    /// `type Allocated<'a> = BStackOwnedSlice<'a, Self>`.
    type Allocated<'a>: Into<BStackOwnedSlice<'a, Self>>
    where
        Self: 'a;

    /// Return a shared reference to the underlying [`BStack`].
    ///
    /// Note: `Bstack` does not require mutability for any of its operations,
    /// and directly mutating the stack without the knowledge of the allocator
    /// risks violating invariants.  Therefore, use this method with caution
    /// and prefer methods on [`BStackSlice`] that delegate to the stack internally.
    fn stack(&self) -> &BStack;

    /// Consume the allocator and return the underlying [`BStack`].
    ///
    /// This method takes `self` by value, so it can only be called once all
    /// [`BStackSlice`] handles have been dropped — the borrow checker enforces
    /// this because slices borrow `&'a Self`.
    fn into_stack(self) -> BStack;

    /// Allocate `len` zero-initialised bytes.
    ///
    /// Returns a [`BStackSlice`] handle covering the newly allocated region.
    /// The region is durably synced before returning.  `len = 0` is valid.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` on failure.
    fn alloc(&self, len: u64) -> Result<Self::Allocated<'_>, Self::Error>;

    /// Resize the region described by `handle` to `new_len` bytes.
    ///
    /// Returns a (possibly different) handle covering the resized region.
    /// The lifetime `'a` ties the returned handle to the same borrow as the
    /// input handle and the allocator.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` on failure, including when the implementation does
    /// not support reallocation.
    fn realloc<'a>(
        &'a self,
        handle: Self::Allocated<'a>,
        new_len: u64,
    ) -> Result<Self::Allocated<'a>, Self::Error>;

    /// Release the region described by `handle`.
    ///
    /// The default implementation is a **no-op**.  Simple bump allocators
    /// accept this default; allocators with free-list tracking should override
    /// it.
    ///
    /// `handle` is consumed by value; the owned handle ceases to exist and
    /// no outstanding views (`BStackSlice`) tied to it can remain live (the
    /// borrow checker enforces this).
    ///
    /// # Errors
    ///
    /// The default never errors.  Overriding implementations may return
    /// `Self::Error` from underlying operations.
    fn dealloc(&self, _handle: Self::Allocated<'_>) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Return the current logical length of the backing stack payload.
    ///
    /// Delegates to [`BStack::len`].
    fn len(&self) -> io::Result<u64> {
        self.stack().len()
    }

    /// Return `true` if the backing stack is empty.
    ///
    /// Delegates to [`BStack::is_empty`].
    fn is_empty(&self) -> io::Result<bool> {
        self.stack().is_empty()
    }
}

/// Extension trait for allocators that support batching multiple allocations
/// and deallocations in a single operation.
///
/// Both methods must be **atomic**: on success every requested item is
/// allocated or deallocated; on failure the backing store is left unchanged —
/// no partial allocation or deallocation is permitted, unless a crash occurs in
/// the middle of the underlying operation, in which case the backing store may be
/// partially updated but must remain internally consistent and recoverable by the
/// allocator's crash recovery procedure. Implementors should also reduce I/O
/// overhead relative to repeated single-item calls, for example by issuing a reduced
/// [`BStack::extend`] or [`BStack::discard`] call.
///
/// Implementations should not simply loop over single-item `alloc` or `dealloc` calls,
/// as this would not provide the intended atomicity guarantees. Even if protected
/// under some crash safety and rollback mechanism, such an implementation is still not
/// recommended due to its misleading semantics and potential performance pitfalls.
pub trait BStackBulkAllocator: BStackAllocator {
    /// Allocate slices with the given lengths in a single atomic operation.
    ///
    /// Returns a `Vec` whose `i`-th entry covers exactly `lengths[i]` bytes.
    /// The order of slices in the result matches the order of `lengths`.  An
    /// empty `lengths` slice is a valid no-op and returns an empty `Vec`.
    ///
    /// # Atomicity
    ///
    /// Either all slices are allocated and returned, or the backing store is
    /// left completely unchanged and an error is returned. During a crash in
    /// the middle of the underlying operation, the backing store may be partially
    /// updated but must remain internally consistent and recoverable by the
    /// allocator's crash recovery procedure.
    ///
    /// # Errors
    ///
    /// Propagates any [`io::Error`] from the underlying operation.
    fn alloc_bulk(
        &self,
        lengths: impl AsRef<[u64]>,
    ) -> Result<Vec<Self::Allocated<'_>>, Self::Error>;

    /// Deallocate multiple handles in a single atomic operation.
    ///
    /// Handles may be supplied in any order.  An empty iterator is a valid
    /// no-op.  Handles are consumed by the iterator, consistent with the
    /// single-ownership requirement.
    ///
    /// # Atomicity
    ///
    /// Either all eligible handles are reclaimed and the backing store is
    /// updated, or the backing store is left completely unchanged and an error
    /// is returned. During a crash in the middle of the underlying operation,
    /// the backing store may be partially updated but must remain internally
    /// consistent and recoverable by the allocator's crash recovery procedure.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` on failure.
    fn dealloc_bulk<'a>(
        &'a self,
        handles: impl IntoIterator<Item = Self::Allocated<'a>>,
    ) -> Result<(), Self::Error>;
}

/// Convenience supertrait for the common case of a [`BStackAllocator`] whose
/// handle type is [`BStackOwnedSlice`] and whose error type is [`io::Error`].
///
/// Requires `'static` because the `for<'a>` higher-ranked bound implies the
/// allocator must outlive any borrow of its own slices.  All allocators
/// provided by this library own their data and satisfy this bound automatically.
///
/// Generic code that does not need custom handle or error types can use
/// `A: BStackOwnedSliceAllocator` as a compact replacement for the three-part bound:
///
/// ```rust,ignore
/// // Verbose form:
/// A: 'static + BStackAllocator<Error = io::Error>,
/// for<'a> A: BStackAllocator<Allocated<'a> = BStackOwnedSlice<'a, A>>,
///
/// // Compact form:
/// A: BStackOwnedSliceAllocator,
/// ```
pub trait BStackOwnedSliceAllocator:
    'static
    + BStackAllocator<Error = io::Error>
    + for<'a> BStackAllocator<Allocated<'a> = BStackOwnedSlice<'a, Self>>
{
}

impl<A: 'static> BStackOwnedSliceAllocator for A
where
    A: BStackAllocator<Error = io::Error>,
    for<'a> A: BStackAllocator<Allocated<'a> = BStackOwnedSlice<'a, A>>,
{
}

// Macros
#[allow(unused)]
macro_rules! read_buf {
    ($buf:expr, $off:expr => $ty:ty) => {{
        let start = $off as usize;
        let end = start + core::mem::size_of::<$ty>();
        $buf[start..end].try_into().unwrap()
    }};
    ($buf:expr, $off:expr => $num:literal) => {{
        let start = $off as usize;
        let end = start + $num;
        $buf[start..end].try_into().unwrap()
    }};
}

#[allow(unused)]
macro_rules! write_buf {
    ($val:expr => $buf:expr, $off:expr) => {{
        let bytes = $val.to_le_bytes();
        let start = $off as usize;
        let end = start + bytes.len();
        $buf[start..end].copy_from_slice(&bytes);
    }};
}

// Read a little-endian value of type `$ty` from `$buf` at offset `$off`.
#[allow(unused)]
macro_rules! read_buf_le {
    ($buf:expr, $off:expr => $ty:ty) => {
        <$ty>::from_le_bytes(read_buf!($buf, $off => $ty))
    };
}

#[allow(unused)]
macro_rules! read_bstack {
    ($stack:expr, $off:expr => $ty:ty) => {{
        let mut buf = [0u8; core::mem::size_of::<$ty>()];
        $stack.get_into($off, &mut buf)?;
        buf
    }};
    ($stack:expr, $off:expr => $num:literal) => {{
        let mut buf = [0u8; $num];
        $stack.get_into($off, &mut buf)?;
        buf
    }};
}

#[cfg(feature = "set")]
pub mod checked_slab;
#[cfg(feature = "set")]
pub mod first_fit;
#[cfg(feature = "set")]
pub mod ghost_tree;
#[cfg(feature = "guarded")]
pub mod guarded;
pub mod linear;
#[cfg(feature = "set")]
pub mod slab;
#[cfg(feature = "set")]
pub mod vec;

#[cfg(feature = "set")]
pub use checked_slab::CheckedSlabBStackAllocator;
#[cfg(feature = "set")]
pub use first_fit::FirstFitBStackAllocator;
#[cfg(feature = "set")]
pub use ghost_tree::GhostTreeBstackAllocator;
#[cfg(all(feature = "guarded", feature = "atomic"))]
pub use guarded::{BStackAtomicGuardedSlice, BStackAtomicGuardedSliceSubview};
#[cfg(feature = "guarded")]
pub use guarded::{BStackGuardedSlice, BStackGuardedSliceSubview};
pub use linear::LinearBStackAllocator;
#[cfg(feature = "set")]
pub use slab::SlabBStackAllocator;
#[cfg(feature = "set")]
pub use vec::{BStackByteVec, BStackByteVecIter};
