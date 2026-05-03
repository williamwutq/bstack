//! Lifecycle-hook slice abstraction for transparent I/O interception.
//!
//! Requires feature `guarded` (which implies `alloc`).
//!
//! # Overview
//!
//! [`BStackGuardedSlice`] is the core trait.  Implement [`raw_block`] to bind the trait
//! to a [`BStackSlice`], then override any of the four hook methods to intercept
//! I/O.  All read/write/cursor methods are derived automatically from the hooks.
//!
//! The allocator type `A` is a **generic parameter** of the trait rather than an
//! associated type, so a single implementing struct can satisfy
//! `BStackGuardedSlice<'a, A>` for any allocator `A` without coupling the trait
//! to a single concrete allocator.
//!
//! [`as_slice`]: BStackGuardedSlice::as_slice
//! [`raw_block`]: BStackGuardedSlice::raw_block

use crate::{BStackAllocator, BStackSlice};
use std::{borrow::Cow, io};

/// A [`BStackSlice`] abstraction with lifecycle hooks for transparent I/O
/// interception.
///
/// `A` is the allocator type, given as a **generic parameter** so that a single
/// implementing struct can satisfy `BStackGuardedSlice<'a, A>` for any allocator
/// without being locked to one concrete choice.
///
/// # Required method
///
/// Implement [`as_slice`](BStackGuardedSlice::as_slice) to bind the trait to an
/// underlying [`BStackSlice`].  All other methods have working defaults.
///
/// # Hook methods
///
/// Override any combination of the four hook methods to intercept I/O:
///
/// | Hook           | When it fires                                             |
/// |----------------|-----------------------------------------------------------|
/// | [`pre_read`]   | Before bytes are read from disk. Return `Err` to deny.    |
/// | [`post_read`]  | After bytes arrive from disk. Transform or pass through.  |
/// | [`pre_write`]  | Before bytes are sent to disk. Transform or pass through. |
/// | [`post_write`] | After a successful write. Audit or update metadata.       |
///
/// All four hooks default to no-ops.  `post_read` and `pre_write` return
/// `Cow::Borrowed`, so no allocation occurs in the non-transforming path.
///
/// # Lifetime
///
/// `'a` is the allocator lifetime, matching [`BStackSlice<'a, A>`].
/// All implementors must satisfy `Self: 'a` and `A: 'a`.
///
/// [`pre_read`]: BStackGuardedSlice::pre_read
/// [`post_read`]: BStackGuardedSlice::post_read
/// [`pre_write`]: BStackGuardedSlice::pre_write
/// [`post_write`]: BStackGuardedSlice::post_write
pub trait BStackGuardedSlice<'a, A: BStackAllocator + 'a>
where
    Self: 'a,
{
    /// The apparent slice for this guard view.
    ///
    /// All I/O methods appears to operate within this slice, and all hooks
    /// receive offsets relative to this slice. The implementation should only
    /// return `Some` if the usage of this view is somehow safe and reflect
    /// the actual underlying data. For example, exposing a cyphertext for
    /// encrypted data does not help the caller to understand the actual plaintext
    /// data, so for a guard that performs decryption, this method should return
    /// `None` since there is no clear mapping between the apparent slice and the
    /// underlying data.
    ///
    /// The returned slice should have length equal to called `len()`.
    fn as_slice(&self) -> Result<BStackSlice<'a, A>, io::Error> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "operation not supported on this guarded slice",
        ))
    }

    /// The length of the data in this guarded view.
    ///
    /// This should return the length of the apparent slice returned by `as_slice`,
    /// not the underlying raw block length.
    fn len(&self) -> u64;

    /// Returns `true` if this guarded view contains no data.
    ///
    /// This is a convenience method that defaults to `self.len() == 0`.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The raw I/O block for this guarded view.
    ///
    /// Defaults to [`as_slice`](BStackGuardedSlice::as_slice).  Override when
    /// hooks operate on a coarser granularity than the slice — for example, a
    /// block cipher that must process aligned 16-byte blocks.
    ///
    /// Used by [`FullBlockSubview`] to issue reads against the full block rather
    /// than the narrowed sub-range.
    ///
    /// ## Safety
    ///
    /// In general, calling this method is only safe when pre and post read/write
    /// hooks are not active or are called during the call, otherwise the caller
    /// may risk data corruption or undefined behavior. Prefer overriding `as_slice`
    /// when possible.
    unsafe fn raw_block(&self) -> BStackSlice<'a, A>;

    /// Called before a read at `[offset, offset + len)` within the Range.
    ///
    /// Calling this method directly is not recommended. Use `read` instead,
    /// which automatically fires the hooks and handles the necessary allocations.
    ///
    /// `offset` is absolute to the [`crate::BStack`], and `len` is the number of
    /// bytes of the raw block to be read (before any `post_read` transformation).
    /// Return `Err` to deny the operation.
    fn pre_read(&self, _offset: u64, _len: u64) -> io::Result<()> {
        Ok(())
    }

    /// Called with the raw bytes just read from the underlying store.
    ///
    /// Calling this method directly is not recommended. Use `read` instead,
    /// which automatically fires the hooks and handles the necessary allocations.
    ///
    /// Return `Cow::Borrowed` to pass through without allocation; return
    /// `Cow::Owned` for decryption, decompression, or other transformations.
    ///
    /// Callers that expect a fixed output size (e.g., [`read_into`]) will return
    /// `InvalidData` if the returned slice has a different length than `data`.
    ///
    /// [`read_into`]: BStackGuardedSlice::read_into
    fn post_read<'d>(&self, data: &'d [u8]) -> io::Result<Cow<'d, [u8]>> {
        Ok(Cow::Borrowed(data))
    }

    /// Called with the data about to be written.
    ///
    /// Calling this method directly is not recommended. Use `write` instead,
    /// which automatically fires the hooks and handles the necessary allocations.
    ///
    /// Return `Cow::Borrowed` to pass through without allocation; return
    /// `Cow::Owned` for encryption, compression, or other transformations.
    fn pre_write<'d>(&self, data: &'d [u8]) -> io::Result<Cow<'d, [u8]>> {
        Ok(Cow::Borrowed(data))
    }

    /// Called after a successful write at `[offset, offset + len)`.
    ///
    /// Calling this method directly is not recommended. Use `write` instead,
    /// which automatically fires the hooks and handles the necessary allocations.
    ///
    /// `offset` is absolute offset within the [`crate::BStack`], and `len` is the length of the
    /// original data passed to `pre_write` (not the transformed length returned by `pre_write`).
    /// (before any `pre_write` transformation).
    fn post_write(&self, _offset: u64, _len: u64) -> io::Result<()> {
        Ok(())
    }

    /// Read the entire slice into a newly allocated `Vec<u8>`.
    ///
    /// Fires `pre_read(0, slice.len())`, reads raw bytes, then passes them
    /// through `post_read`.  The transformation may change the returned length.
    ///
    /// This method at maximum allocates twice the slice length (once for the
    /// raw read and once for the transformed output).
    fn read(&self) -> io::Result<Vec<u8>> {
        let slice = unsafe { self.raw_block() };
        self.pre_read(slice.start(), slice.len())?;
        let raw = slice.read()?;
        let transformed = self.post_read(&raw)?;
        Ok(transformed.into_owned())
    }

    /// Overwrite the beginning of this slice with `data`.
    ///
    /// Passes `data` through `pre_write` before writing.  Writes
    /// `min(self.len(), data.len())` bytes, and passes the original length to `post_write`.
    ///
    /// Requires feature `set`.
    #[cfg(feature = "set")]
    fn write(&self, data: impl AsRef<[u8]>) -> io::Result<()> {
        let slice = unsafe { self.raw_block() };
        let raw = data.as_ref();
        let n = self.len().min(raw.len() as u64);
        let cooked = self.pre_write(&raw[..n as usize])?;
        slice.write(cooked.as_ref())?;
        self.post_write(0, n)
    }

    /// Zero the entire slice.
    ///
    /// Constructs a zero buffer, passes it through `pre_write` (allowing hooks
    /// to substitute a different fill pattern), then writes the result.
    ///
    /// Requires feature `set`.
    #[cfg(feature = "set")]
    fn zero(&self) -> io::Result<()> {
        let slice = unsafe { self.raw_block() };
        let n = self.len();
        let zeros = vec![0u8; n as usize];
        let cooked = self.pre_write(&zeros)?;
        slice.write(cooked.as_ref())?;
        self.post_write(0, n)
    }
}

/// Marker trait for [`BStackGuardedSlice`] implementations that guarantee
/// atomicity and crash safety.
///
/// # Safety
///
/// Implementors must uphold **both** of the following invariants:
///
/// 1. **Atomicity** — for each `read` or `write` call, the pre-hook, I/O, and
///    post-hook execute as an uninterruptible unit.  No other operation on the
///    same underlying slice can observe an intermediate state.
///
/// 2. **Crash safety** — if the process crashes after a `write` returns `Ok`,
///    the slice contains either the fully written new value or the previous
///    value.  Partially-written states must be impossible or automatically
///    recoverable on the next open.
///
/// Note: holding the bstack write lock alone does **not** satisfy invariant 2
/// unless the implementation can also ensure crash safety or recoverability at
/// the slice level.  For example, an implementation that writes to a temporary
/// location and atomically renames on success and an implementation that only
/// issue one write per slice satisfies the contract, but an implementation that
/// performs multiple writes that may result in partial updates does not.
///
/// Requires feature `atomic`.
#[cfg(feature = "atomic")]
pub unsafe trait BStackAtomicGuardedSlice<'a, A: BStackAllocator + 'a>:
    BStackGuardedSlice<'a, A>
where
    Self: 'a,
{
}

/// Extension trait for [`BStackGuardedSlice`] implementations that can produce
/// a narrowed sub-view while preserving the full hook scope of the parent.
///
/// [`raw_block`]: BStackGuardedSlice::raw_block
pub trait BStackGuardedSliceSubview<'a, A: BStackAllocator + 'a>:
    BStackGuardedSlice<'a, A>
where
    Self: 'a,
{
    /// Narrow this view to the sub-range `[start, end)` within the slice.
    ///
    /// `start` and `end` are relative to [`as_slice`](BStackGuardedSlice::as_slice),
    /// or equivalently in the range `[0, len())`. The returned view preserves
    /// the parent's hook scope — calls to `pre_read`, `post_read`, etc. on the
    /// subview delegate to the parent with appropriately translated offsets.
    ///
    /// # Panics
    ///
    /// Panics if the specified range is out of bounds of the apparent slice.
    fn subview(&self, start: u64, end: u64) -> impl BStackGuardedSliceSubview<'a, A> + '_;

    /// Narrow this view to the sub-range specified by `range`.
    ///
    /// `start` and `end` are relative to [`as_slice`](BStackGuardedSlice::as_slice),
    /// or equivalently in the range `[0, len())`. The returned view preserves
    /// the parent's hook scope — calls to `pre_read`, `post_read`, etc. on the
    /// subview delegate to the parent with appropriately translated offsets.
    ///
    /// # Panics
    ///
    /// Panics if the specified range is out of bounds of the apparent slice.
    fn subview_range(
        &self,
        range: std::ops::Range<u64>,
    ) -> impl BStackGuardedSliceSubview<'a, A> + '_ {
        self.subview(range.start, range.end)
    }
}

/// Marker trait for [`BStackGuardedSliceSubview`] implementations that also
/// satisfy [`BStackAtomicGuardedSlice`]'s atomicity and crash-safety contract.
///
/// # Safety
///
/// See [`BStackAtomicGuardedSlice`] for the full safety contract.
///
/// Requires feature `atomic`.
#[cfg(feature = "atomic")]
pub unsafe trait BStackAtomicGuardedSliceSubview<'a, A: BStackAllocator + 'a>:
    BStackAtomicGuardedSlice<'a, A> + BStackGuardedSliceSubview<'a, A>
where
    Self: 'a,
{
}
