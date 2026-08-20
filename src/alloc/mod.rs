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
//! * [`BStackChunk<'a>`](BStackChunk) — a fixed-stride **view**, not an
//!   iterator, from [`BStackSlice::chunks`]/[`rchunks`](BStackSlice::rchunks).
//!   [`iter`](BStackChunk::iter) gives a lazy [`BStackChunkIter`]; `sort_by`,
//!   `binary_search_by`, and `select_nth_by` operate on the view directly.
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
//! * [`BStackUninitAllocator`] — opt-in extension trait for allocators with a
//!   cheaper uninitialised path.  [`alloc_uninit`](BStackUninitAllocator::alloc_uninit) /
//!   [`realloc_uninit`](BStackUninitAllocator::realloc_uninit) skip the
//!   zero-fill of newly allocated or grown bytes, returning **unspecified**
//!   (but always valid-to-read) contents for callers that overwrite the region
//!   immediately.  Implemented by [`SlabBStackAllocator`],
//!   [`GhostTreeBstackAllocator`], [`CheckedSlabBStackAllocator`],
//!   [`SegregatedBStackAllocator`] and [`FirstFitBStackAllocator`], and
//!   forwarded by [`DebugCheckingAllocator`]; see
//!   [Uninitialised allocation](#uninitialised-allocation).
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
//! * [`DebugCheckingAllocator`] — transparent debug wrapper that validates any
//!   allocator's behaviour at runtime (overlap, double-free, partial-free detection).
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
//!   O(1) alloc/dealloc.
//!
//! * [`CheckedSlabBStackAllocator`] — crash-recoverable slab variant (`alloc` + `set`).
//!   8-byte per-block header tracks state; double-frees caught.
//!
//! * [`SegregatedBStackAllocator`] — **experimental** segregated (binned)
//!   free-list allocator (`alloc` + `set`).  Generalises the checked slab to 33
//!   size classes sharing one arena; 8-byte per-block header, O(1) classed
//!   alloc/dealloc, crash-recoverable by linear scan.  `Send` in all
//!   configurations; `Send + Sync` with `atomic`.
//!
//! # Uninitialised allocation
//!
//! [`SlabBStackAllocator`], [`GhostTreeBstackAllocator`],
//! [`CheckedSlabBStackAllocator`], [`SegregatedBStackAllocator`] and
//! [`FirstFitBStackAllocator`] implement [`BStackUninitAllocator`], because for
//! each of them the caller-facing zero guarantee costs a write that a caller
//! overwriting the region has no use for.  What that write is, and therefore
//! what is saved, differs:
//!
//! | Allocator | What `alloc` writes to guarantee zeroes | What `alloc_uninit` saves |
//! |-----------|------------------------------------------|---------------------------|
//! | [`SlabBStackAllocator`]      | a whole-block `zero` after popping the free list           | the entire call — one durable sync per reused block |
//! | [`GhostTreeBstackAllocator`] | a 32-byte `zero` over the reclaimed block's stale AVL node | the entire call for requests of 32 bytes or more    |
//! | [`CheckedSlabBStackAllocator`] | a full block-sized claim buffer                         | everything past the 8-byte overhead word            |
//! | [`SegregatedBStackAllocator`] | a full block-sized claim buffer                          | everything past the overhead (and any copied prefix) |
//! | [`FirstFitBStackAllocator`]  | a full block image fused into the metadata write          | block-sized bytes and the staging buffer; the tail path also drops to a sparse extend |
//! | [`LinearBStackAllocator`]    | nothing — `extend` is a sparse `set_len`                  | *not implemented*: there is nothing to skip         |
//!
//! [`realloc_uninit`](BStackUninitAllocator::realloc_uninit) additionally drops
//! the `zero` calls that scrub bytes newly exposed inside a block the caller
//! already owns.  No allocator drops a write it needs for its own metadata
//! or free-space invariants, so crash consistency, recovery, and the `handle`
//! contract on failure are identical to the initialised methods.
//!
//! Because the saving is sometimes in bytes written and journalled rather than
//! in calls, a returned region may still read back as zero — notably for
//! [`CheckedSlabBStackAllocator`], which scrubs on free rather than on claim.
//! That is never something to rely on; the trait promises only unspecified
//! contents.
//!
//! [`DebugCheckingAllocator<A>`](DebugCheckingAllocator) forwards both methods,
//! with the same overlap and double-free tracking as `alloc`/`realloc`, but only
//! when `A` implements the trait itself — it never fabricates the trait for an
//! allocator that has no cheaper uninitialised path.
//!
//!
//! # Debug wrapper
//!
//! * [`DebugCheckingAllocator<A>`](DebugCheckingAllocator) — wraps any allocator
//!   (`alloc`).  Tracks allocated and freed regions in memory and panics on
//!   overlapping allocations, double-frees, partial-frees, and multi-span frees.
//!   Intended for tests and debugging only; the O(n) overlap checks add
//!   significant per-operation overhead.
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
//! # Foreign handles
//!
//! What the borrow checker does *not* prove is that a handle goes back to the
//! allocator that issued it.  Two allocators of the same type are the same
//! type, and `BStackOwnedSlice<'a, A>` is covariant in `'a`, so for `a1` and
//! `a2` of type `A` outliving `'a`, `a2.dealloc(h1)` compiles.  No lifetime or
//! type discipline rules that out.  It is also not a soundness problem: handles
//! are `(offset, len)` coordinates into a file, not pointers, and reach the
//! payload only through [`BStack`]'s bounds-checked I/O — the damage is `a2`
//! recording a free block it never owned, which is corruption, not undefined
//! behaviour.
//!
//! Rejecting a foreign handle is therefore the *allocator's* job, at run time.
//! Every allocator here checks ownership at the top of `realloc`,
//! `realloc_uninit`, `dealloc`, `dealloc_bulk`, and (where implemented)
//! `realloc_inplace` — before touching any metadata — and returns
//! [`io::ErrorKind::InvalidInput`] with the handle carried back intact.  The
//! owned-slice `try_join`/`try_join_inplace` likewise reject an `other` handle
//! from a different allocator.  Custom implementors should do the same;
//! [`BStackOwnedSlice::is_from`] is the check.
//!
//! # Feature flags
//!
//! The `alloc` Cargo feature enables this module, including all allocator traits,
//! handle types, [`LinearBStackAllocator`], [`GhostTreeBstackAllocator`], and
//! [`DebugCheckingAllocator`]:
//!
//! ```toml
//! bstack = { version = "0.1", features = ["alloc"] }
//! ```
//!
//! [`BStackSliceWriter`], [`FirstFitBStackAllocator`], [`SlabBStackAllocator`],
//! [`CheckedSlabBStackAllocator`], [`SegregatedBStackAllocator`] (experimental),
//! and [`BStackByteVec`] additionally require `set`:
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

pub mod chunk;
pub mod slice;
pub use chunk::{BStackChunk, BStackChunkIter};
#[cfg(feature = "set")]
pub use slice::BStackSliceWriter;
pub use slice::{BStackOwnedSlice, BStackRange, BStackSlice, BStackSliceReader};

/// Error returned by [`BStackAllocator::realloc`] and
/// [`BStackAllocator::dealloc`] when the operation fails.
///
/// A failed resize or free almost always leaves a valid allocation behind — the
/// original region is untouched, or the new region is fully committed (in which
/// case the operation should have succeeded). This type carries that surviving
/// allocation back to the caller so it can retry, fall back, or explicitly
/// [`dealloc`](BStackAllocator::dealloc) it rather than leak it. Because
/// [`BStackOwnedSlice`]'s `Drop` is a no-op, dropping the handle instead of
/// returning it here would silently leak the region.
///
/// It implements [`std::error::Error`] (delegating [`Display`](fmt::Display) to
/// [`source`](Self::source)), so `?` works within functions that return it.
/// Note that converting *out* to a bare `Self::Error` necessarily discards the
/// recovered handle, so that conversion is intentionally left explicit — the
/// caller must decide what to do with the allocation first.
pub struct BStackAllocError<'a, A: BStackAllocator + 'a> {
    /// The underlying error that caused the operation to fail.
    pub source: A::Error,
    /// The recovered ownership handle, if the region survived the failure.
    ///
    /// * `Some` — the original allocation is intact and owned by the caller
    ///   again. Implementations **must** return `Some` whenever the region
    ///   survives, which is the overwhelmingly common case.
    /// * `None` — the region was consumed or lost during the failed operation
    ///   (e.g. a free-then-allocate strategy whose second step failed, or a
    ///   crash mid-operation). Any bytes that remain are recoverable only
    ///   through the allocator's crash-recovery procedure.
    ///
    /// Most built-in [`realloc`](BStackAllocator::realloc) paths return `Some`
    /// on failure — the copy-to-new-region paths hand back either the untouched
    /// original or the fully committed new region. The exception is
    /// [`GhostTreeBstackAllocator`], whose non-tail shrink commits a non-atomic
    /// AVL insert: a torn insert must not be retried, so that path returns
    /// `None`. Built-in [`dealloc`](BStackAllocator::dealloc) likewise returns
    /// `None` on its free-list / AVL-insert paths, where handing back a
    /// partially-freed block would risk a double-free. Treat `None` as "not
    /// recoverable here," not as something that never happens.
    pub handle: Option<A::Allocated<'a>>,
}

impl<'a, A: BStackAllocator + 'a> BStackAllocError<'a, A> {
    /// Construct an error that hands the still-valid original handle back to
    /// the caller.
    #[inline]
    #[must_use]
    pub fn with_handle(source: A::Error, handle: A::Allocated<'a>) -> Self {
        Self {
            source,
            handle: Some(handle),
        }
    }

    /// Construct an error whose allocation was consumed or lost by the failed
    /// operation and cannot be returned.
    #[inline]
    #[must_use]
    pub fn lost(source: A::Error) -> Self {
        Self {
            source,
            handle: None,
        }
    }

    /// Consume the error and return the recovered handle, if any.
    #[inline]
    #[must_use]
    pub fn into_handle(self) -> Option<A::Allocated<'a>> {
        self.handle
    }
}

// Manual `Debug` (rather than derive) because `A::Allocated<'a>` is not bound
// by `Debug`. We surface only whether the handle was recovered, which is the
// diagnostically useful bit and needs no bound on `Allocated`.
impl<'a, A: BStackAllocator + 'a> fmt::Debug for BStackAllocError<'a, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BStackAllocError")
            .field("source", &self.source)
            .field("handle_recovered", &self.handle.is_some())
            .finish()
    }
}

impl<'a, A: BStackAllocator + 'a> fmt::Display for BStackAllocError<'a, A> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.source, f)
    }
}

// `A::Error` is only bound by `Debug + Display`, not `Error`, so `source()`
// cannot be forwarded; the default (`None`) is used.
impl<'a, A: BStackAllocator + 'a> std::error::Error for BStackAllocError<'a, A> {}

/// Error returned by [`BStackBulkAllocator::dealloc_bulk`] when the bulk free
/// fails, carrying back the handles that were **not** freed.
///
/// This is the bulk analogue of [`BStackAllocError`]: rather than a single
/// optional handle it holds a `Vec` of the still-owned handles, so a failed
/// bulk free does not silently leak the regions it could not reclaim (recall
/// that [`BStackOwnedSlice`]'s `Drop` is a no-op).
///
/// It implements [`std::error::Error`] (delegating [`Display`](fmt::Display) to
/// [`source`](Self::source)), so `?` works within functions that return it.
pub struct BStackBulkAllocError<'a, A: BStackAllocator + 'a> {
    /// The underlying error that caused the operation to fail.
    pub source: A::Error,
    /// The handles that were not freed and remain owned by the caller.
    ///
    /// For an atomic implementation this is **every** handle passed in — on
    /// failure the backing store is unchanged, so all handles survive. An
    /// implementation whose free is progressive (not all-or-nothing) returns
    /// only the handles it can still vouch for, and leaves this empty if a
    /// partial free means none can be safely returned; those regions are then
    /// recoverable only through the allocator's crash-recovery procedure.
    pub handles: Vec<A::Allocated<'a>>,
}

impl<'a, A: BStackAllocator + 'a> BStackBulkAllocError<'a, A> {
    /// Construct an error carrying the handles still owned by the caller.
    #[inline]
    #[must_use]
    pub fn with_handles(source: A::Error, handles: Vec<A::Allocated<'a>>) -> Self {
        Self { source, handles }
    }

    /// Consume the error and return the recovered handles.
    #[inline]
    #[must_use]
    pub fn into_handles(self) -> Vec<A::Allocated<'a>> {
        self.handles
    }
}

// Manual `Debug` (rather than derive) because `A::Allocated<'a>` is not bound
// by `Debug`. We surface how many handles were recovered, which needs no bound.
impl<'a, A: BStackAllocator + 'a> fmt::Debug for BStackBulkAllocError<'a, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BStackBulkAllocError")
            .field("source", &self.source)
            .field("handles_recovered", &self.handles.len())
            .finish()
    }
}

impl<'a, A: BStackAllocator + 'a> fmt::Display for BStackBulkAllocError<'a, A> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.source, f)
    }
}

impl<'a, A: BStackAllocator + 'a> std::error::Error for BStackBulkAllocError<'a, A> {}

/// Reject a handle that was not issued by `allocator`, handing it straight back.
///
/// The guard every allocator runs before a `realloc`/`dealloc` touches its
/// metadata. Passing a handle to the wrong allocator instance is a safe-code
/// programming error the type system cannot catch — see
/// [the module documentation](self#foreign-handles) for why it must be a
/// run-time check and why it is not a soundness issue.
///
/// `op` names the calling method for the error message.
#[inline]
pub(crate) fn ensure_own_handle<'a, A>(
    allocator: &'a A,
    handle: BStackOwnedSlice<'a, A>,
    op: &'static str,
) -> Result<BStackOwnedSlice<'a, A>, BStackAllocError<'a, A>>
where
    A: BStackAllocator<Error = io::Error, Allocated<'a> = BStackOwnedSlice<'a, A>> + 'a,
{
    if handle.is_from(allocator) {
        Ok(handle)
    } else {
        // The region is untouched and still owned by whoever holds it, so the
        // handle goes back with the error rather than being dropped (leaked).
        Err(BStackAllocError::with_handle(
            io::Error::new(io::ErrorKind::InvalidInput, foreign_handle_message(op)),
            handle,
        ))
    }
}

/// Bulk analogue of [`ensure_own_handle`]: rejects the whole batch if any handle
/// is foreign, returning every handle so none is leaked.
#[inline]
pub(crate) fn ensure_own_handles<'a, A>(
    allocator: &'a A,
    handles: Vec<BStackOwnedSlice<'a, A>>,
    op: &'static str,
) -> Result<Vec<BStackOwnedSlice<'a, A>>, BStackBulkAllocError<'a, A>>
where
    A: BStackAllocator<Error = io::Error, Allocated<'a> = BStackOwnedSlice<'a, A>> + 'a,
{
    if handles.iter().all(|h| h.is_from(allocator)) {
        Ok(handles)
    } else {
        Err(BStackBulkAllocError::with_handles(
            io::Error::new(io::ErrorKind::InvalidInput, foreign_handle_message(op)),
            handles,
        ))
    }
}

/// Shared message body for the two guards above.
fn foreign_handle_message(op: &'static str) -> String {
    format!("{op}: handle was issued by a different allocator instance")
}

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
///         -> Result<BStackOwnedSlice<'a, Self>, BStackAllocError<'a, Self>> { ... }
/// }
/// ```
///
/// On the failure path, `realloc`/`dealloc` return a [`BStackAllocError`]
/// carrying the surviving allocation (see that type for the `handle`
/// contract), so a failed operation never silently leaks the region.
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
    /// printed and propagated with `?`. [`realloc`](Self::realloc) and
    /// [`dealloc`](Self::dealloc) wrap this in a [`BStackAllocError`] so that a
    /// failed operation can hand the surviving allocation back to the caller.
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
    /// `handle` must have been issued by *this* allocator instance. That cannot
    /// be checked at compile time (see
    /// [Lifetime model](crate#lifetime-model)); implementations should reject a
    /// foreign handle with [`io::ErrorKind::InvalidInput`], using
    /// [`BStackOwnedSlice::is_from`], before touching any metadata.
    ///
    /// # Errors
    ///
    /// Returns a [`BStackAllocError`] on failure, including when the
    /// implementation does not support reallocation. Because a failed resize
    /// leaves the original region intact, implementations **must** populate
    /// [`BStackAllocError::handle`] with the untouched original handle
    /// (`Some`), reserving `None` for the rare case where the allocation was
    /// genuinely lost. Callers can then retry, fall back, or `dealloc` it.
    fn realloc<'a>(
        &'a self,
        handle: Self::Allocated<'a>,
        new_len: u64,
    ) -> Result<Self::Allocated<'a>, BStackAllocError<'a, Self>>;

    /// Release the region described by `handle`.
    ///
    /// The default implementation is a **no-op**.  Simple bump allocators
    /// accept this default; allocators with free-list tracking should override
    /// it.
    ///
    /// `handle` is consumed by value; the owned handle ceases to exist and
    /// no outstanding views (`BStackSlice`) tied to it can remain live (the
    /// borrow checker enforces this).  What the borrow checker cannot enforce is
    /// that `handle` came from *this* allocator instance — see
    /// [Lifetime model](crate#lifetime-model); overriding implementations
    /// should reject a foreign handle with [`io::ErrorKind::InvalidInput`],
    /// using [`BStackOwnedSlice::is_from`], before touching any metadata.
    ///
    /// # Errors
    ///
    /// The default never errors.  Overriding implementations may return a
    /// [`BStackAllocError`] from underlying operations. A failed free normally
    /// leaves the region still allocated, so implementations **must** return
    /// the handle in [`BStackAllocError::handle`] (`Some`) whenever it survives,
    /// reserving `None` for a genuinely lost allocation.
    #[inline]
    fn dealloc<'a>(
        &'a self,
        _handle: Self::Allocated<'a>,
    ) -> Result<(), BStackAllocError<'a, Self>> {
        Ok(())
    }

    /// Return the current logical length of the backing stack payload.
    ///
    /// Delegates to [`BStack::len`].
    #[inline]
    fn len(&self) -> io::Result<u64> {
        self.stack().len()
    }

    /// Return `true` if the backing stack is empty.
    ///
    /// Delegates to [`BStack::is_empty`].
    #[inline]
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
    /// single-ownership requirement.  Every handle must have been issued by
    /// *this* allocator instance; a batch containing a foreign handle should be
    /// rejected whole (see [Lifetime model](crate#lifetime-model)).
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
    /// Returns a [`BStackBulkAllocError`] on failure, carrying back the handles
    /// that were **not** freed (see that type's `handles` field for the
    /// contract). For an atomic implementation that is every handle passed in,
    /// so a failed bulk free never silently leaks the regions.
    fn dealloc_bulk<'a>(
        &'a self,
        handles: impl IntoIterator<Item = Self::Allocated<'a>>,
    ) -> Result<(), BStackBulkAllocError<'a, Self>>;
}

/// Extension trait for allocators that can skip zero-initialisation of newly
/// allocated or grown regions.
///
/// [`BStackAllocator::alloc`] guarantees a zero-initialised region and
/// [`realloc`](BStackAllocator::realloc) zero-fills any newly added bytes when
/// growing.  That guarantee costs a write: a region pulled from a free list may
/// hold leftover bytes from a previous allocation, which the allocator must
/// scrub before returning.  Callers that immediately overwrite the whole region
/// (for example, `write`-ing a serialized record right after `alloc`) have no
/// use for that zero-fill.  This trait lets them opt out of it.
///
/// The bytes in a region returned by [`alloc_uninit`](Self::alloc_uninit), or
/// in the newly added portion of a region returned by
/// [`realloc_uninit`](Self::realloc_uninit), are **unspecified**: they may be
/// zero, or may be leftover bytes from a previous allocation that occupied the
/// same on-disk space.  They are always valid to read — no undefined behavior
/// results, unlike `MaybeUninit<u8>` in memory — but callers must not rely on
/// their value until they have written to the region themselves.  This mirrors
/// `Vec::with_capacity` followed by `set_len`, except no analog of `set_len` is
/// needed since the bytes are always valid to read, just unspecified in value.
///
/// # Implementing this trait is optional
///
/// Implementing it signals that the allocator actually has a cheaper
/// uninitialised path.  Allocators for which zero-fill is already free — an
/// always-extend bump allocator whose growth goes through [`BStack::extend`]
/// (the tail is already zero via `set_len` on a sparse file), or an allocator
/// that scrubs blocks eagerly on free — gain nothing and may either implement
/// the trait as a thin wrapper around `alloc`/`realloc` or not implement it at
/// all.  The savings are concentrated in the free-list-reuse path, where a
/// previously-occupied block is handed back without being scrubbed first.
pub trait BStackUninitAllocator: BStackAllocator {
    /// Allocate `len` bytes without zero-initialising them.
    ///
    /// Equivalent to [`alloc`](BStackAllocator::alloc) except that the returned
    /// region's contents are unspecified rather than guaranteed zero.  The
    /// region is still durably synced before returning.  `len = 0` is valid.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` on failure.
    fn alloc_uninit(&self, len: u64) -> Result<Self::Allocated<'_>, Self::Error>;

    /// Resize the region described by `handle` to `new_len` bytes without
    /// zero-initialising any newly added bytes.
    ///
    /// Equivalent to [`realloc`](BStackAllocator::realloc) except that, when
    /// `new_len` is larger than the current length, the contents of the added
    /// bytes are unspecified rather than guaranteed zero.  Shrinking is
    /// unaffected, as no new bytes are introduced.  Existing bytes within
    /// `[0, min(old_len, new_len))` are always preserved, exactly as `realloc`.
    ///
    /// # Errors
    ///
    /// Returns a [`BStackAllocError`] on failure, carrying the surviving
    /// allocation under the same contract as
    /// [`realloc`](BStackAllocator::realloc), including when the implementation
    /// does not support reallocation.
    fn realloc_uninit<'a>(
        &'a self,
        handle: Self::Allocated<'a>,
        new_len: u64,
    ) -> Result<Self::Allocated<'a>, BStackAllocError<'a, Self>>;
}

/// Extension trait for allocators that can resize a region *at either edge*
/// without relocating its retained bytes.
///
/// [`realloc`](BStackAllocator::realloc) only ever moves the tail edge, and is
/// free to satisfy a request by copying the whole payload to a fresh region.
/// [`realloc_inplace`](Self::realloc_inplace) instead moves the front edge, the
/// back edge, or both in a single call, and **guarantees no relocation**: on
/// success the retained bytes occupy the same physical offsets they did before.
/// This bounds a front trim (log / ring-buffer workloads) at the number of
/// bytes actually added or removed rather than the size of the retained payload.
///
/// # The exact-position guarantee is the contract
///
/// A successful call returns a handle whose range is *exactly*
/// `(start - prepend, end + append)` for the input handle's `(start, end)`.
/// This is not an optimisation hint but the definition of the method: an
/// implementation that would have to relocate the retained bytes to satisfy the
/// request **must** fail with [`io::ErrorKind::Unsupported`] rather than return
/// a correctly-sized handle at a different offset. Callers built on this method
/// rely on a success meaning the retained bytes were not copied.
///
/// An allocator that cannot perform a given `(prepend, append)` combination in
/// place returns `Unsupported` (the same convention
/// [`LinearBStackAllocator::realloc`] uses for non-tail resize); supporting one
/// edge does not obligate it to support the other, or to support both at once.
pub trait BStackInPlaceResizeAllocator: BStackAllocator {
    /// Resize `handle` in place by `prepend` bytes at the front and `append`
    /// bytes at the back in one call.
    ///
    /// Positive grows that edge, negative shrinks it by the given magnitude;
    /// either may be zero. The new length is `handle.len() as i64 + prepend +
    /// append`. `append`-only reproduces a non-moving [`realloc`](BStackAllocator::realloc);
    /// `prepend`-only resizes the front; nonzero on both edges shifts the window
    /// while resizing it.
    ///
    /// # Empty handles
    ///
    /// An empty handle (`handle.len() == 0`) names no on-disk region, so there is
    /// no anchored position for the guarantee below to honor. Resizing one in
    /// place is therefore always [`io::ErrorKind::Unsupported`], for *every*
    /// `(prepend, append)` — including the `(0, 0)` no-op. Growing from empty is
    /// a fresh [`alloc`](BStackAllocator::alloc), not a resize; a caller holding
    /// an empty handle that wants it to stay empty simply keeps it.
    ///
    /// # Position guarantee
    ///
    /// On success, if the input handle's range was `(start, end)`, the returned
    /// handle's range is *exactly* `(start - prepend, end + append)` — never a
    /// correctly-sized region chosen elsewhere. See the trait docs.
    ///
    /// # Errors
    ///
    /// Returns a [`BStackAllocError`] carrying the untouched original handle
    /// (`handle: Some`) on failure, under the same recovery contract as
    /// [`realloc`](BStackAllocator::realloc):
    ///
    /// * [`io::ErrorKind::Unsupported`] — the allocator cannot satisfy this
    ///   `(prepend, append)` combination without relocating the retained bytes,
    ///   or the handle is empty (see "Empty handles" above).
    /// * [`io::ErrorKind::InvalidInput`] — the resulting length
    ///   `handle.len() as i64 + prepend + append` is negative, the handle does
    ///   not describe a valid allocation, or it was issued by a different
    ///   allocator instance (see the module's "Foreign handles" section). (This
    ///   is deliberately a recoverable error rather than a panic, so a caller bug
    ///   does not drop the handle — whose `Drop` is a no-op — and leak the region.)
    /// * Any other error propagated from the underlying [`BStack`] operations.
    ///
    /// A failure *after* the operation began mutating on-disk structure (rather
    /// than a clean pre-mutation rejection) may return `handle: None`; the bytes
    /// are then recoverable only through the allocator's crash-recovery
    /// procedure. Implementations must document which paths can do this.
    ///
    /// [`LinearBStackAllocator::realloc`]: crate::LinearBStackAllocator
    fn realloc_inplace<'a>(
        &'a self,
        handle: Self::Allocated<'a>,
        prepend: i64,
        append: i64,
    ) -> Result<Self::Allocated<'a>, BStackAllocError<'a, Self>>;
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
pub mod debug_checking;
#[cfg(feature = "set")]
pub mod first_fit;
#[cfg(feature = "set")]
pub mod ghost_tree;
#[cfg(feature = "guarded")]
pub mod guarded;
pub mod linear;
#[cfg(feature = "set")]
pub mod segregated;
#[cfg(feature = "set")]
pub mod slab;
#[cfg(feature = "set")]
pub mod vec;

#[cfg(feature = "set")]
pub use checked_slab::CheckedSlabBStackAllocator;
pub use debug_checking::DebugCheckingAllocator;
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
pub use segregated::SegregatedBStackAllocator;
#[cfg(feature = "set")]
pub use slab::SlabBStackAllocator;
#[cfg(feature = "set")]
pub use vec::{BStackByteVec, BStackByteVecIter};
