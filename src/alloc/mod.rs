//! Allocator abstraction for [`BStack`]-backed region management.
//!
//! # Overview
//!
//! This module provides the following public items:
//!
//! * [`BStackSlice`] — a lifetime-coupled handle to a contiguous region of a
//!   [`BStack`] payload.  It is a lightweight `Copy` value (one reference plus
//!   two `u64`s) that exposes [`read`](BStackSlice::read),
//!   [`read_into`](BStackSlice::read_into),
//!   [`read_range`](BStackSlice::read_range),
//!   [`read_range_into`](BStackSlice::read_range_into), and (with the `set` feature)
//!   [`write`](BStackSlice::write) and [`zero`](BStackSlice::zero).
//!
//! * [`BStackAllocator`] — a trait for types that own a [`BStack`] and manage
//!   regions within it.  It standardises [`alloc`](BStackAllocator::alloc),
//!   [`realloc`](BStackAllocator::realloc), [`dealloc`](BStackAllocator::dealloc),
//!   and [`into_stack`](BStackAllocator::into_stack).
//!
//! * [`BStackBulkAllocator`] — extension trait for [`BStackAllocator`] that
//!   adds atomic bulk [`alloc_bulk`](BStackBulkAllocator::alloc_bulk) and
//!   [`dealloc_bulk`](BStackBulkAllocator::dealloc_bulk) methods.  Both are
//!   required with no default implementation: on error the backing store must
//!   be left completely unchanged.
//!
//! * [`LinearBStackAllocator`] — the reference bump allocator that always
//!   appends to the tail.  Every operation maps to a single [`BStack`] call
//!   and is crash-safe by inheritance.  `dealloc` of a non-tail slice is a
//!   no-op; space is only reclaimed when the tail slice is freed.  `Send`
//!   without the `atomic` feature (not `Sync`); `Send + Sync` with `atomic`.
//!
//! * [`FirstFitBStackAllocator`] — a persistent first-fit free-list allocator
//!   (requires both `alloc` **and** `set` features).  Freed regions are tracked
//!   on disk in a doubly-linked intrusive free list and reused for future
//!   allocations, so on-disk size does not grow without bound.  Adjacent free
//!   blocks are coalesced automatically on `dealloc`.  A `recovery_needed` flag
//!   enables automatic free-list reconstruction after a crash.  `Send` without
//!   the `atomic` feature (not `Sync`); `Send + Sync` with `atomic`, where an
//!   internal `Mutex` serializes free-list mutation and stack extension.
//!
//! * [`GhostTreeBstackAllocator`] — a pure-AVL general-purpose allocator
//!   (requires `alloc` feature).  Free blocks store their AVL node inline at
//!   offset 0 within the block — live allocations carry **zero** overhead
//!   (no headers, no footers).  The tree is keyed on `(size, address)` for a
//!   strict total order.  All memory is kept zeroed: the BStack zeroes on
//!   extension, and the allocator zeroes on free.  `Send` in all
//!   configurations; `Send + Sync` with the `atomic` feature, where an
//!   internal `Mutex` serialises AVL tree mutations.
//!
//! * [`SlabBStackAllocator`] — a fixed-block slab allocator (requires both
//!   `alloc` **and** `set` features).  All blocks are exactly `block_size`
//!   bytes with no per-block overhead; freed blocks form an intrusive
//!   singly-linked free list.  O(1) alloc and dealloc.  `Send` but not
//!   `Sync`.  *Experimental.*
//!
//! * [`CheckedSlabBStackAllocator`] — a crash-recoverable variant of
//!   [`SlabBStackAllocator`] (requires both `alloc` **and** `set` features).
//!   Each block carries an 8-byte overhead prefix encoding its state: zero
//!   when free (next-free offset in `data[0..8]`), high bit set when in use
//!   (block count in the low bits).  Leaked blocks are recoverable by a linear
//!   scan; double-frees are caught before the free list can be corrupted.
//!   Constructor takes `data_size` (usable bytes per block, ≥ 8); `block_size`
//!   on disk is `data_size + 8`.  [`open`](CheckedSlabBStackAllocator::open)
//!   calls [`recover`](CheckedSlabBStackAllocator::recover) automatically to
//!   reclaim leaked blocks and repair orphaned tails from unclean shutdowns.
//!   `Send` but not `Sync`.  *Experimental.*
//!
//! * [`BStackByteVec`] — a growable byte (`u8`) vector backed by a
//!   [`BStack`] allocation (requires both `alloc` **and** `set`).  Mirrors the
//!   core [`Vec<u8>`] API: `push`, `pop`, `get`, `read_bytes`, `as_slice`,
//!   `truncate`, `reserve`, `resize`, `iter`, and more.  The 16-byte block
//!   header stores `len` and `capacity` as little-endian `u64`s so the state
//!   is recoverable after a crash.  `push` reallocates at `max(cap × 2, 4)`;
//!   `pop` zeros the vacated slot; `truncate` zeros removed slots in a single
//!   [`zero_range`](BStackSlice::zero_range) call.
//!
//! # Lifetime model
//!
//! `BStackSlice<'a, A>` borrows the **allocator** `A` for `'a`, not the
//! underlying [`BStack`] directly.  Tying the lifetime to the allocator has
//! two important consequences:
//!
//! 1. **`into_stack` is statically gated.** [`BStackAllocator::into_stack`]
//!    consumes the allocator by value.  Because outstanding slices borrow
//!    `&'a A`, the borrow checker prevents moving the allocator out while any
//!    slice is still in scope.
//!
//! 2. **The dependency is honest.** A slice's validity depends on the
//!    allocator — not just on the file being open.  Tying `'a` to `&'a BStack`
//!    only prevents the file from closing; the stack could still be freely
//!    resized through interior mutability, silently invalidating the handle.
//!    Tying `'a` to the allocator makes the dependency explicit.
//!
//! # Feature flags
//!
//! The `alloc` Cargo feature enables this entire module, including
//! [`BStackAllocator`], [`BStackBulkAllocator`], [`BStackSlice`],
//! [`BStackSliceReader`], [`LinearBStackAllocator`], and [`GhostTreeBstackAllocator`]:
//!
//! ```toml
//! bstack = { version = "0.1", features = ["alloc"] }
//! ```
//!
//! In-place slice writes ([`BStackSliceWriter`]), [`FirstFitBStackAllocator`],
//! [`SlabBStackAllocator`], [`CheckedSlabBStackAllocator`], and [`BStackByteVec`]
//! additionally require `set`:
//!
//! ```toml
//! bstack = { version = "0.1", features = ["alloc", "set"] }
//! ```
//!
//! # Realloc and dealloc: slice origin requirement
//!
//! [`BStackAllocator::realloc`] and [`BStackAllocator::dealloc`] are only
//! guaranteed to work correctly when the supplied [`BStackSlice`] was returned
//! directly by [`BStackAllocator::alloc`] or by a previous call to
//! [`BStackAllocator::realloc`] on the **same allocator instance**.
//!
//! Passing an *arbitrary* sub-slice — obtained through
//! [`BStackSlice::subslice`], [`BStackSlice::subslice_range`], or a manually
//! constructed [`BStackSlice::new`] — is **not supported** and may silently
//! corrupt the allocator's internal state (e.g. corrupting block headers,
//! writing free-list pointers into live data, or double-freeing memory).
//!
//! If you need to store a slice handle across a session boundary (e.g. after
//! closing and reopening the file), serialise the `(start, len)` fields as raw
//! `u64` values and reconstruct the full slice via [`BStackSlice::new`] only
//! for I/O calls such as [`BStackSlice::read`] or [`BStackSlice::write`] — not
//! for passing back to `realloc` or `dealloc`.  Only the original handle
//! returned by the allocator carries the correct block-level metadata implied
//! by its offset and length.
//!
//! [`BStack`] only grows and shrinks at the tail.  Resizing the **last**
//! (tail) allocation is O(1).  Resizing a **non-tail** allocation cannot be
//! done in place.  Implementors of [`BStackAllocator`], if supported, must
//! copy the data to a new allocation and update the metadata accordingly,
//! and must return an error if they do not support this operation.
//!
//! # Crash consistency
//!
//! Every individual [`BStack`] operation — [`extend`](BStack::extend),
//! [`discard`](BStack::discard), [`set`](BStack::set), etc. — performs a
//! durable sync before returning and is individually crash-safe: a process
//! crash mid-operation leaves the file in the last fully committed state.
//!
//! At the *allocator* level, operations that require more than one [`BStack`]
//! call are **not** automatically atomic.  A crash between two calls leaves
//! the file in an intermediate state that the allocator must be prepared to
//! recover from on the next [`BStack::open`].
//!
//! Implementors must document which of the following two categories each of
//! their operations falls into:
//!
//! **Single-call (crash-safe by inheritance):** Any operation that maps
//! directly to one [`BStack`] call inherits the crash safety of that underlying
//! call.
//!
//! **Multi-call (requires explicit recovery design):** Operations that issue
//! two or more [`BStack`] calls — such as a copy-and-move `realloc` that
//! pushes new data, updates a metadata region, and then marks the old region
//! free — must be designed so that a crash at any step leaves the file in a
//! state that the allocator can detect and recover from on re-open.  The usual
//! technique is to write new data before updating the pointer/metadata that
//! makes it visible (write-ahead), so that a partial update is either fully
//! applied or fully invisible after recovery.
//!
//! Note that writing into an allocation via [`BStackSlice::write`] is a
//! separate operation from [`BStackAllocator::alloc`].  A crash between the
//! two leaves the allocated region filled with zeros (the initial state from
//! [`BStack::extend`]).  This is typically fine — the data simply hasn't been
//! written yet — but callers that need write-then-allocate atomicity must
//! arrange it themselves.
//!
//! # Trait implementations
//!
//! ## `BStackSlice`
//!
//! | Trait | Semantics |
//! |-------|-----------|
//! | `PartialEq` / `Eq` | Compares `(offset, len)`. The allocator reference is **not** compared — callers that need allocator identity must check it separately. |
//! | `Hash` | Hashes `(offset, len)`, consistent with `PartialEq`. |
//! | `PartialOrd` / `Ord` | Ordered by `offset`, then by `len`. Reflects document order within a payload. |
//! | `From<BStackSlice> for [u8; 16]` | Serialises to `[offset_le8 ‖ len_le8]`. Reconstruct with [`BStackSlice::from_bytes`]. |
//!
//! ## `BStackSliceReader` and `BStackSliceWriter`
//!
//! | Trait | Semantics |
//! |-------|-----------|
//! | `PartialEq` / `Eq` | Equal when both the underlying slice (`offset` + `len`) and the cursor position match. |
//! | `Hash` | Hashes `(slice, cursor)`, consistent with `PartialEq`. |
//! | `PartialOrd` / `Ord` | Ordered by **absolute payload position** `slice.start() + cursor`, then by `slice.len()`. |
//!
//! Reader and writer are also **cross-comparable**: `PartialEq` and `PartialOrd` are defined between
//! `BStackSliceReader` and `BStackSliceWriter` using the same `(abs_pos, len)` key (requires the `set`
//! feature), so the two cursor types can be mixed freely in sorted collections.
//!
//! Additionally, both reader and writer implement `PartialEq` against a bare `BStackSlice`, returning
//! `true` when the cursor's underlying slice equals the slice (cursor position is ignored for this
//! comparison).

use crate::BStack;
use std::convert::TryInto;
use std::fmt;
use std::io;

pub mod slice;
#[cfg(feature = "set")]
pub use slice::BStackSliceWriter;
pub use slice::{BStackSlice, BStackSliceReader};

/// A trait for types that own a [`BStack`] and manage contiguous byte regions
/// within its payload.
///
/// # Ownership model
///
/// An implementor takes ownership of a [`BStack`].  [`BStackSlice`] handles
/// produced by [`alloc`](Self::alloc) borrow the allocator for lifetime `'_`,
/// which prevents the allocator from being consumed by
/// [`into_stack`](Self::into_stack) while any slice is alive.  The canonical
/// pattern:
///
/// ```rust,ignore
/// struct MyAllocator { stack: BStack }
///
/// impl BStackAllocator for MyAllocator {
///     fn stack(&self) -> &BStack { &self.stack }
///     fn alloc(&self, len: u64) -> io::Result<BStackSlice<'_, Self>> { ... }
///     fn realloc<'a>(&'a self, slice: BStackSlice<'a, Self>, new_len: u64)
///         -> io::Result<BStackSlice<'a, Self>> { ... }
///     fn into_stack(self) -> BStack { self.stack }
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
    /// Must be `Copy` (cheap to pass by value) and convertible to
    /// [`BStackSlice`] via [`TryInto`] for generic I/O use.  The conversion
    /// error must implement [`fmt::Debug`] and [`fmt::Display`].
    ///
    /// Simple allocators set `type Allocated<'a> = BStackSlice<'a, Self>`.
    /// Richer allocators may embed additional metadata in a newtype whose
    /// [`TryInto`] implementation is always infallible.
    ///
    /// All allocators provided by this library set `type Allocated<'a> = BStackSlice<'a, Self>`,
    /// which have blanket implementations by rust since `impl<T, U> TryInto<U>
    /// for T where T: Into<U>` and `impl<T, U> Into<U> for T` are provided
    /// by the standard library.
    type Allocated<'a>: Copy + TryInto<BStackSlice<'a, Self>, Error: fmt::Debug + fmt::Display>
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
    /// # Slice origin requirement
    ///
    /// `handle` **must** have been returned directly by [`alloc`](Self::alloc)
    /// or by a prior call to [`realloc`](Self::realloc) on this same allocator
    /// instance.  Passing an arbitrary sub-slice obtained via
    /// [`BStackSlice::subslice`], [`BStackSlice::subslice_range`], or a
    /// manually constructed [`BStackSlice::new`] is not supported and may
    /// corrupt the allocator's internal state.
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
    /// After calling `dealloc`, `handle` must not be used for further I/O.
    ///
    /// # Slice origin requirement
    ///
    /// `handle` **must** have been returned directly by [`alloc`](Self::alloc)
    /// or by [`realloc`](Self::realloc) on this same allocator instance.
    /// Passing an arbitrary sub-slice obtained via [`BStackSlice::subslice`],
    /// [`BStackSlice::subslice_range`], or a manually constructed
    /// [`BStackSlice::new`] is not supported and may corrupt the allocator's
    /// internal state.
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
    /// Handles may be supplied in any order.  An empty slice is a valid no-op.
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
        handles: impl AsRef<[Self::Allocated<'a>]>,
    ) -> Result<(), Self::Error>;
}

/// Convenience supertrait for the common case of a [`BStackAllocator`] whose
/// handle type is [`BStackSlice`] and whose error type is [`io::Error`].
///
/// Requires `'static` because the `for<'a>` higher-ranked bound implies the
/// allocator must outlive any borrow of its own slices (equivalent to
/// `Self: 'static`).  All allocators provided by this library own their data
/// and satisfy this bound automatically.
///
/// Generic code that does not need custom handle or error types can use
/// `A: BStackSliceAllocator` as a compact replacement for the three-part bound:
///
/// ```rust,ignore
/// // Verbose form:
/// A: 'static + BStackAllocator<Error = io::Error>,
/// for<'a> A: BStackAllocator<Allocated<'a> = BStackSlice<'a, A>>,
///
/// // Compact form:
/// A: BStackSliceAllocator,
/// ```
pub trait BStackSliceAllocator:
    'static
    + BStackAllocator<Error = io::Error>
    + for<'a> BStackAllocator<Allocated<'a> = BStackSlice<'a, Self>>
{
}

impl<A: 'static> BStackSliceAllocator for A
where
    A: BStackAllocator<Error = io::Error>,
    for<'a> A: BStackAllocator<Allocated<'a> = BStackSlice<'a, A>>,
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
pub mod debug_checking;
#[cfg(feature = "set")]
pub mod first_fit;
#[cfg(feature = "set")]
pub mod ghost_tree;
#[cfg(feature = "guarded")]
pub mod guarded;
pub mod linear;
pub mod manual;
#[cfg(feature = "set")]
pub mod slab;
#[cfg(feature = "set")]
pub mod vec;

#[cfg(feature = "set")]
pub use checked_slab::CheckedSlabBStackAllocator;
pub use debug_checking::{DebugCheckingAllocator, DebugHandle};
#[cfg(feature = "set")]
pub use first_fit::FirstFitBStackAllocator;
#[cfg(feature = "set")]
pub use ghost_tree::GhostTreeBstackAllocator;
#[cfg(all(feature = "guarded", feature = "atomic"))]
pub use guarded::{BStackAtomicGuardedSlice, BStackAtomicGuardedSliceSubview};
#[cfg(feature = "guarded")]
pub use guarded::{BStackGuardedSlice, BStackGuardedSliceSubview};
pub use linear::LinearBStackAllocator;
pub use manual::ManualAllocator;
#[cfg(feature = "set")]
pub use slab::SlabBStackAllocator;
#[cfg(feature = "set")]
pub use vec::{BStackByteVec, BStackByteVecIter};
