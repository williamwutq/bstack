//! Allocator abstraction for [`BStack`]-backed region management.
//!
//! # Overview
//!
//! This module provides two public items:
//!
//! * [`BStackSlice`] — a lifetime-coupled handle to a contiguous region of a
//!   [`BStack`] payload.  It is a lightweight `Copy` value (one reference plus
//!   two `u64`s) that exposes [`read`](BStackSlice::read),
//!   [`read_into`](BStackSlice::read_into), and (with the `set` feature)
//!   [`write`](BStackSlice::write) and [`zero`](BStackSlice::zero).
//!
//! * [`BStackAllocator`] — a trait for types that own a [`BStack`] and manage
//!   regions within it.  It standardises [`alloc`](BStackAllocator::alloc),
//!   [`realloc`](BStackAllocator::realloc), [`dealloc`](BStackAllocator::dealloc),
//!   and [`into_stack`](BStackAllocator::into_stack).
//!
//! [`LinearBStackAllocator`] is the reference implementation: a simple bump
//! allocator that always appends to the tail.
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
//! This entire module requires the `alloc` Cargo feature:
//!
//! ```toml
//! bstack = { version = "0.1", features = ["alloc"] }
//! ```
//!
//! In-place slice writes additionally require the `set` feature:
//!
//! ```toml
//! bstack = { version = "0.1", features = ["alloc", "set"] }
//! ```
//!
//! # Realloc contract for slices
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

#![cfg(feature = "alloc")]

use crate::BStack;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io;
use std::ops::Range;

/// A lifetime-coupled handle to a contiguous region of a [`BStack`] payload.
///
/// `BStackSlice<'a, A>` is a lightweight `Copy` value that holds a shared
/// reference to the allocator `A` together with a logical `offset` and `len`.
/// It is the primary handle type produced by [`BStackAllocator::alloc`] and
/// consumed by [`BStackAllocator::realloc`] and [`BStackAllocator::dealloc`].
///
/// # Lifetime
///
/// `'a` is tied to the **allocator** borrow, not to the [`BStack`] directly.
/// This means the borrow checker prevents calling
/// [`into_stack`](BStackAllocator::into_stack) — which consumes the allocator
/// by value — while any slice is still alive.
///
/// # After `dealloc`
///
/// Once a slice has been passed to [`BStackAllocator::dealloc`], the handle
/// must not be used for further I/O.  The type system enforces this when the
/// slice is consumed by value, but callers who `Copy` the handle before
/// deallocating must uphold this invariant themselves.
pub struct BStackSlice<'a, A: BStackAllocator> {
    /// Shared reference to the allocator that owns the backing store.
    allocator: &'a A,
    /// Logical start offset within the [`BStack`] payload (inclusive).
    offset: u64,
    /// Number of bytes in this slice.
    len: u64,
}

// Manual impls so that `A: Copy` / `A: Clone` are not required —
// `&'a A` is always `Copy` regardless of whether `A` is.
impl<'a, A: BStackAllocator> Clone for BStackSlice<'a, A> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<'a, A: BStackAllocator> Copy for BStackSlice<'a, A> {}

impl<'a, A: BStackAllocator> fmt::Debug for BStackSlice<'a, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BStackSlice")
            .field("start", &self.start())
            .field("end", &self.end())
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

impl<'a, A: BStackAllocator> BStackSlice<'a, A> {
    /// Create a new `BStackSlice`.
    ///
    /// Does not validate that `offset + len <= stack.len()`.  Invalid slices
    /// produce errors on the first I/O call.
    #[inline]
    pub fn new(allocator: &'a A, offset: u64, len: u64) -> Self {
        Self {
            allocator,
            offset,
            len,
        }
    }

    /// Serialize this slice to a 16-byte array for on-disk storage.
    ///
    /// Layout: `offset` as 8 bytes little-endian, then `len` as 8 bytes
    /// little-endian.  Reconstruct with [`BStackSlice::from_bytes`].
    #[inline]
    pub fn to_bytes(&self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[..8].copy_from_slice(&self.offset.to_le_bytes());
        out[8..].copy_from_slice(&self.len.to_le_bytes());
        out
    }

    /// Reconstruct a `BStackSlice` from a 16-byte array produced by
    /// [`BStackSlice::to_bytes`].
    ///
    /// Does not validate that the encoded range lies within the payload.
    /// Invalid slices produce errors on the first I/O call.
    #[inline]
    pub fn from_bytes(allocator: &'a A, bytes: [u8; 16]) -> Self {
        let offset = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        let len = u64::from_le_bytes(bytes[8..].try_into().unwrap());
        Self {
            allocator,
            offset,
            len,
        }
    }

    /// Returns the start offset of this slice within the payload.
    #[inline]
    pub fn start(&self) -> u64 {
        self.offset
    }

    /// The exclusive end offset of this slice within the payload
    /// (`self.start() + self.len()`).
    #[inline]
    pub fn end(&self) -> u64 {
        self.offset + self.len
    }

    /// Returns the range of this slice as `start..end` within the payload.
    #[inline]
    pub fn range(&self) -> Range<u64> {
        self.start()..self.end()
    }

    /// Returns the length of this slice in bytes.
    #[inline]
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Returns `true` if this slice spans zero bytes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Return the underlying allocator.
    #[inline]
    pub fn allocator(&self) -> &'a A {
        self.allocator
    }

    /// Return the underlying stack.
    ///
    /// Note: `Bstack` does not require mutability for any of its operations,
    /// and directly mutating the stack without the knowledge of the allocator
    /// risks violating invariants.  Therefore, use this method with caution
    /// and prefer methods on [`BStackSlice`] such as [`read`](BStackSlice::read) and
    /// [`write`](BStackSlice::write) that delegate to the stack internally.
    #[inline]
    pub fn stack(&self) -> &BStack {
        self.allocator.stack()
    }

    /// Create a subslice of this slice.
    ///
    /// Returns a new `BStackSlice` that refers to the subrange `[start, end)` within
    /// this slice. The `start` and `end` parameters are relative to this slice's start.
    ///
    /// # Panics
    ///
    /// Panics if `start > end` or `end > self.len()`.
    #[inline]
    pub fn subslice(&self, start: u64, end: u64) -> BStackSlice<'a, A> {
        self.subslice_range(start..end)
    }

    /// Create a subslice of this slice.
    ///
    /// Returns a new `BStackSlice` that refers to the subrange `range` within
    /// this slice. The `range` is relative to this slice's start.
    ///
    /// # Panics
    ///
    /// Panics if `range.start > range.end` or `range.end > self.len()`.
    pub fn subslice_range(&self, range: Range<u64>) -> BStackSlice<'a, A> {
        assert!(range.start <= range.end, "range start must be <= end");
        assert!(range.end <= self.len, "range end must be <= slice length");
        BStackSlice {
            allocator: self.allocator,
            offset: self.offset + range.start,
            len: range.end - range.start,
        }
    }

    /// Read the entire slice into a newly allocated `Vec<u8>`.
    ///
    /// Delegates to [`BStack::get`].
    ///
    /// # Errors
    ///
    /// Returns an error if the range exceeds the current payload size.
    pub fn read(&self) -> io::Result<Vec<u8>> {
        self.stack().get(self.start(), self.end())
    }

    /// Read bytes from this slice into the caller-supplied `buf`.
    ///
    /// Reads `min(buf.len(), self.len() as usize)` bytes starting at
    /// `self.start()`.  If `buf` is shorter than the slice, only the first
    /// `buf.len()` bytes are read.  If `buf` is longer, only `self.len()` bytes
    /// are filled and the remainder of `buf` is left untouched.
    pub fn read_into(&self, buf: &mut [u8]) -> io::Result<()> {
        let n = (buf.len() as u64).min(self.len()) as usize;
        self.stack().get_into(self.start(), &mut buf[..n])
    }

    /// Read a sub-range `[start, start + buf.len())` relative to this slice
    /// into the caller-supplied buffer.
    ///
    /// `start` is relative to `self.start()`, not the payload start.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `start + buf.len()` exceeds
    /// `self.len()`.
    pub fn read_range_into(&self, start: u64, buf: &mut [u8]) -> io::Result<()> {
        let end_rel = start + buf.len() as u64;
        if end_rel > self.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "range [{start}, {end_rel}) exceeds slice length {}",
                    self.len()
                ),
            ));
        }
        self.stack().get_into(self.start() + start, buf)
    }

    /// Overwrite the beginning of this slice in place with `data`.
    ///
    /// Writes `min(data.len(), self.len() as usize)` bytes starting at
    /// `self.start()`.  If `data` is shorter than the slice, the remainder of
    /// the slice is left untouched.  If `data` is longer, only `self.len()`
    /// bytes are written.
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    pub fn write(&self, data: &[u8]) -> io::Result<()> {
        let n = (data.len() as u64).min(self.len()) as usize;
        self.stack().set(self.start(), &data[..n])
    }

    /// Overwrite a sub-range `[start, start + data.len())` within this slice
    /// in place.
    ///
    /// `start` is relative to `self.start()`.
    ///
    /// Requires the `set` feature.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `start + data.len()` exceeds
    /// `self.len()`.
    #[cfg(feature = "set")]
    pub fn write_range(&self, start: u64, data: &[u8]) -> io::Result<()> {
        let end_rel = start + data.len() as u64;
        if end_rel > self.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "range [{start}, {end_rel}) exceeds slice length {}",
                    self.len()
                ),
            ));
        }
        self.stack().set(self.start() + start, data)
    }

    /// Zero out the entire slice in place.
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    pub fn zero(&self) -> io::Result<()> {
        self.stack().zero(self.start(), self.len())
    }

    /// Zero a sub-range `[start, start + n)` within this slice in place.
    ///
    /// `start` is relative to `self.start()`.
    ///
    /// Requires the `set` feature.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `start + n` exceeds
    /// `self.len()`.
    #[cfg(feature = "set")]
    pub fn zero_range(&self, start: u64, n: u64) -> io::Result<()> {
        let end_rel = start + n;
        if end_rel > self.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "range [{start}, {end_rel}) exceeds slice length {}",
                    self.len()
                ),
            ));
        }
        self.stack().zero(self.start() + start, n)
    }

    /// Create a cursor-based reader positioned at the start of this slice.
    ///
    /// The reader implements [`io::Read`] and [`io::Seek`] in the coordinate
    /// space `[0, self.len())`.
    pub fn reader(&self) -> BStackSliceReader<'a, A> {
        BStackSliceReader {
            slice: *self,
            cursor: 0,
        }
    }

    /// Create a cursor-based reader positioned at `offset` bytes into this slice.
    ///
    /// `offset` is relative to `self.start()`.  Seeking past `self.len()` is
    /// allowed; subsequent reads return `Ok(0)`.
    pub fn reader_at(&self, offset: u64) -> BStackSliceReader<'a, A> {
        BStackSliceReader {
            slice: *self,
            cursor: offset,
        }
    }

    /// Create a cursor-based writer positioned at the start of this slice.
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    pub fn writer(&self) -> BStackSliceWriter<'a, A> {
        BStackSliceWriter {
            slice: *self,
            cursor: 0,
        }
    }

    /// Create a cursor-based writer positioned at `offset` bytes into this slice.
    ///
    /// `offset` is relative to `self.start()`.  Writing past `self.len()`
    /// returns `Ok(0)`.
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    pub fn writer_at(&self, offset: u64) -> BStackSliceWriter<'a, A> {
        BStackSliceWriter {
            slice: *self,
            cursor: offset,
        }
    }
}

/// Two slices are equal when their `offset` and `len` match.
///
/// The allocator is not compared — callers working across allocators should
/// compare [`start`](BStackSlice::start) and [`len`](BStackSlice::len)
/// explicitly if allocator identity matters.
impl<'a, A: BStackAllocator> PartialEq for BStackSlice<'a, A> {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset && self.len == other.len
    }
}

impl<'a, A: BStackAllocator> Eq for BStackSlice<'a, A> {}

/// Hashes `(offset, len)`, consistent with [`PartialEq`].
impl<'a, A: BStackAllocator> Hash for BStackSlice<'a, A> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.offset.hash(state);
        self.len.hash(state);
    }
}

impl<'a, A: BStackAllocator> PartialOrd for BStackSlice<'a, A> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Slices are ordered by start offset, then by length — consistent with [`Eq`].
impl<'a, A: BStackAllocator> Ord for BStackSlice<'a, A> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.offset
            .cmp(&other.offset)
            .then(self.len.cmp(&other.len))
    }
}

/// Serialize the slice to its 16-byte on-disk representation.
///
/// Equivalent to [`BStackSlice::to_bytes`].
impl<'a, A: BStackAllocator> From<BStackSlice<'a, A>> for [u8; 16] {
    fn from(slice: BStackSlice<'a, A>) -> Self {
        slice.to_bytes()
    }
}

/// Convert a slice into a reader positioned at the start.
///
/// Equivalent to [`BStackSlice::reader`].
impl<'a, A: BStackAllocator> From<BStackSlice<'a, A>> for BStackSliceReader<'a, A> {
    fn from(slice: BStackSlice<'a, A>) -> Self {
        slice.reader()
    }
}

/// A cursor-based reader over a [`BStackSlice`].
///
/// Implements [`io::Read`] and [`io::Seek`] within the coordinate space of the
/// slice — position 0 maps to `slice.offset` in the underlying payload, and
/// the reader cannot read past `slice.offset + slice.len`.
///
/// Constructed via [`BStackSlice::reader`] or [`BStackSlice::reader_at`].
pub struct BStackSliceReader<'a, A: BStackAllocator> {
    slice: BStackSlice<'a, A>,
    cursor: u64,
}

impl<'a, A: BStackAllocator> Clone for BStackSliceReader<'a, A> {
    fn clone(&self) -> Self {
        Self {
            slice: self.slice,
            cursor: self.cursor,
        }
    }
}

impl<'a, A: BStackAllocator> fmt::Debug for BStackSliceReader<'a, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BStackSliceReader")
            .field("start", &self.slice.start())
            .field("end", &self.slice.end())
            .field("len", &self.slice.len())
            .field("cursor", &self.cursor)
            .finish_non_exhaustive()
    }
}

impl<'a, A: BStackAllocator> BStackSliceReader<'a, A> {
    /// Return the current cursor position within the slice (not the payload).
    #[inline]
    pub fn position(&self) -> u64 {
        self.cursor
    }

    /// Return the underlying [`BStackSlice`].
    #[inline]
    pub fn slice(&self) -> BStackSlice<'a, A> {
        self.slice
    }
}

impl<'a, A: BStackAllocator> io::Read for BStackSliceReader<'a, A> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() || self.cursor >= self.slice.len {
            return Ok(0);
        }
        let available = (self.slice.len - self.cursor) as usize;
        let n = buf.len().min(available);
        let abs_start = self.slice.offset + self.cursor;
        self.slice.stack().get_into(abs_start, &mut buf[..n])?;
        self.cursor += n as u64;
        Ok(n)
    }
}

impl<'a, A: BStackAllocator> io::Seek for BStackSliceReader<'a, A> {
    /// Move the cursor within the slice's coordinate space.
    ///
    /// [`io::SeekFrom::End`] is relative to `self.slice.len`.  Seeking past
    /// the end is allowed; subsequent reads return `Ok(0)`.  Seeking before
    /// position 0 returns [`io::ErrorKind::InvalidInput`].
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let len = self.slice.len as i128;
        let new_pos = match pos {
            io::SeekFrom::Start(n) => n as i128,
            io::SeekFrom::End(n) => len + n as i128,
            io::SeekFrom::Current(n) => self.cursor as i128 + n as i128,
        };
        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek before beginning of slice",
            ));
        }
        self.cursor = new_pos as u64;
        Ok(self.cursor)
    }
}

/// Two readers are equal when they wrap equal slices and share the same cursor.
impl<'a, A: BStackAllocator> PartialEq for BStackSliceReader<'a, A> {
    fn eq(&self, other: &Self) -> bool {
        self.slice == other.slice && self.cursor == other.cursor
    }
}

impl<'a, A: BStackAllocator> Eq for BStackSliceReader<'a, A> {}

impl<'a, A: BStackAllocator> Hash for BStackSliceReader<'a, A> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.slice.hash(state);
        self.cursor.hash(state);
    }
}

impl<'a, A: BStackAllocator> PartialOrd for BStackSliceReader<'a, A> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Readers are ordered by absolute payload position (`slice.start() + cursor`),
/// then by slice length.
impl<'a, A: BStackAllocator> Ord for BStackSliceReader<'a, A> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let self_pos = self.slice.start() + self.cursor;
        let other_pos = other.slice.start() + other.cursor;
        self_pos
            .cmp(&other_pos)
            .then(self.slice.len().cmp(&other.slice.len()))
    }
}

/// Convert a reader back into its underlying slice, discarding the cursor.
///
/// Equivalent to [`BStackSliceReader::slice`].
impl<'a, A: BStackAllocator> From<BStackSliceReader<'a, A>> for BStackSlice<'a, A> {
    fn from(reader: BStackSliceReader<'a, A>) -> Self {
        reader.slice()
    }
}

/// A cursor-based writer over a [`BStackSlice`].
///
/// Implements [`io::Write`] and [`io::Seek`] within the coordinate space of
/// the slice — position 0 maps to `slice.offset` in the underlying payload,
/// and writes cannot exceed `slice.offset + slice.len`.
///
/// Every call to [`write`](io::Write::write) delegates to [`BStack::set`] and
/// is durably synced before returning.
///
/// Constructed via [`BStackSlice::writer`] or [`BStackSlice::writer_at`].
///
/// Requires the `set` feature.
#[cfg(feature = "set")]
pub struct BStackSliceWriter<'a, A: BStackAllocator> {
    slice: BStackSlice<'a, A>,
    cursor: u64,
}

#[cfg(feature = "set")]
impl<'a, A: BStackAllocator> Clone for BStackSliceWriter<'a, A> {
    fn clone(&self) -> Self {
        Self {
            slice: self.slice,
            cursor: self.cursor,
        }
    }
}

#[cfg(feature = "set")]
impl<'a, A: BStackAllocator> fmt::Debug for BStackSliceWriter<'a, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BStackSliceWriter")
            .field("start", &self.slice.start())
            .field("end", &self.slice.end())
            .field("len", &self.slice.len())
            .field("cursor", &self.cursor)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "set")]
impl<'a, A: BStackAllocator> BStackSliceWriter<'a, A> {
    /// Return the current cursor position within the slice (not the payload).
    #[inline]
    pub fn position(&self) -> u64 {
        self.cursor
    }

    /// Return the underlying [`BStackSlice`].
    #[inline]
    pub fn slice(&self) -> BStackSlice<'a, A> {
        self.slice
    }
}

#[cfg(feature = "set")]
impl<'a, A: BStackAllocator> io::Write for BStackSliceWriter<'a, A> {
    /// Write bytes at the current cursor position, then advance the cursor.
    ///
    /// Writes `min(buf.len(), remaining)` bytes where `remaining` is
    /// `self.slice.len() - self.cursor`.  Returns `Ok(0)` when the cursor is
    /// at or past the end of the slice.  Every call issues a durable sync.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() || self.cursor >= self.slice.len {
            return Ok(0);
        }
        let available = (self.slice.len - self.cursor) as usize;
        let n = buf.len().min(available);
        let abs_start = self.slice.offset + self.cursor;
        self.slice.stack().set(abs_start, &buf[..n])?;
        self.cursor += n as u64;
        Ok(n)
    }

    /// No-op: every [`write`](io::Write::write) is already durably synced.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(feature = "set")]
impl<'a, A: BStackAllocator> io::Seek for BStackSliceWriter<'a, A> {
    /// Move the cursor within the slice's coordinate space.
    ///
    /// [`io::SeekFrom::End`] is relative to `self.slice.len`.  Seeking past
    /// the end is allowed; subsequent writes return `Ok(0)`.  Seeking before
    /// position 0 returns [`io::ErrorKind::InvalidInput`].
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let len = self.slice.len as i128;
        let new_pos = match pos {
            io::SeekFrom::Start(n) => n as i128,
            io::SeekFrom::End(n) => len + n as i128,
            io::SeekFrom::Current(n) => self.cursor as i128 + n as i128,
        };
        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek before beginning of slice",
            ));
        }
        self.cursor = new_pos as u64;
        Ok(self.cursor)
    }
}

#[cfg(feature = "set")]
impl<'a, A: BStackAllocator> PartialEq for BStackSliceWriter<'a, A> {
    fn eq(&self, other: &Self) -> bool {
        self.slice == other.slice && self.cursor == other.cursor
    }
}

#[cfg(feature = "set")]
impl<'a, A: BStackAllocator> Eq for BStackSliceWriter<'a, A> {}

#[cfg(feature = "set")]
impl<'a, A: BStackAllocator> Hash for BStackSliceWriter<'a, A> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.slice.hash(state);
        self.cursor.hash(state);
    }
}

#[cfg(feature = "set")]
impl<'a, A: BStackAllocator> PartialOrd for BStackSliceWriter<'a, A> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Writers are ordered by absolute payload position (`slice.start() + cursor`),
/// then by slice length.
#[cfg(feature = "set")]
impl<'a, A: BStackAllocator> Ord for BStackSliceWriter<'a, A> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let self_pos = self.slice.start() + self.cursor;
        let other_pos = other.slice.start() + other.cursor;
        self_pos
            .cmp(&other_pos)
            .then(self.slice.len().cmp(&other.slice.len()))
    }
}

/// Convert a slice into a writer positioned at the start.
///
/// Equivalent to [`BStackSlice::writer`].
#[cfg(feature = "set")]
impl<'a, A: BStackAllocator> From<BStackSlice<'a, A>> for BStackSliceWriter<'a, A> {
    fn from(slice: BStackSlice<'a, A>) -> Self {
        slice.writer()
    }
}

/// Convert a writer back into its underlying slice, discarding the cursor.
///
/// Equivalent to [`BStackSliceWriter::slice`].
#[cfg(feature = "set")]
impl<'a, A: BStackAllocator> From<BStackSliceWriter<'a, A>> for BStackSlice<'a, A> {
    fn from(writer: BStackSliceWriter<'a, A>) -> Self {
        writer.slice()
    }
}

/// Convert a reader into a writer at the same position.
///
//// The reader and writer share the same underlying slice and cursor position.
#[cfg(feature = "set")]
impl<'a, A: BStackAllocator> From<BStackSliceReader<'a, A>> for BStackSliceWriter<'a, A> {
    fn from(reader: BStackSliceReader<'a, A>) -> Self {
        BStackSliceWriter {
            slice: reader.slice,
            cursor: reader.cursor,
        }
    }
}

/// Convert a writer into a reader at the same position.
///
/// The reader and writer share the same underlying slice and cursor position.
#[cfg(feature = "set")]
impl<'a, A: BStackAllocator> From<BStackSliceWriter<'a, A>> for BStackSliceReader<'a, A> {
    fn from(writer: BStackSliceWriter<'a, A>) -> Self {
        BStackSliceReader {
            slice: writer.slice,
            cursor: writer.cursor,
        }
    }
}

#[cfg(feature = "set")]
impl<'a, A: BStackAllocator> PartialEq<BStackSliceWriter<'a, A>> for BStackSliceReader<'a, A> {
    fn eq(&self, other: &BStackSliceWriter<'a, A>) -> bool {
        self.slice == other.slice && self.cursor == other.cursor
    }
}

#[cfg(feature = "set")]
impl<'a, A: BStackAllocator> PartialEq<BStackSliceReader<'a, A>> for BStackSliceWriter<'a, A> {
    fn eq(&self, other: &BStackSliceReader<'a, A>) -> bool {
        self.slice == other.slice && self.cursor == other.cursor
    }
}

impl<'a, A: BStackAllocator> PartialEq<BStackSlice<'a, A>> for BStackSliceReader<'a, A> {
    fn eq(&self, other: &BStackSlice<'a, A>) -> bool {
        &self.slice == other
    }
}

impl<'a, A: BStackAllocator> PartialEq<BStackSliceReader<'a, A>> for BStackSlice<'a, A> {
    fn eq(&self, other: &BStackSliceReader<'a, A>) -> bool {
        self == &other.slice
    }
}

#[cfg(feature = "set")]
impl<'a, A: BStackAllocator> PartialEq<BStackSlice<'a, A>> for BStackSliceWriter<'a, A> {
    fn eq(&self, other: &BStackSlice<'a, A>) -> bool {
        &self.slice == other
    }
}

#[cfg(feature = "set")]
impl<'a, A: BStackAllocator> PartialEq<BStackSliceWriter<'a, A>> for BStackSlice<'a, A> {
    fn eq(&self, other: &BStackSliceWriter<'a, A>) -> bool {
        self == &other.slice
    }
}

impl<'a, A: BStackAllocator> PartialOrd<BStackSliceReader<'a, A>> for BStackSlice<'a, A> {
    fn partial_cmp(&self, other: &BStackSliceReader<'a, A>) -> Option<std::cmp::Ordering> {
        Some(self.cmp(&other.slice()))
    }
}

impl<'a, A: BStackAllocator> PartialOrd<BStackSlice<'a, A>> for BStackSliceReader<'a, A> {
    fn partial_cmp(&self, other: &BStackSlice<'a, A>) -> Option<std::cmp::Ordering> {
        Some(self.slice().cmp(other))
    }
}

#[cfg(feature = "set")]
impl<'a, A: BStackAllocator> PartialOrd<BStackSliceReader<'a, A>> for BStackSlice<'a, A> {
    fn partial_cmp(&self, other: &BStackSliceReader<'a, A>) -> Option<std::cmp::Ordering> {
        Some(self.cmp(&other.slice()))
    }
}

#[cfg(feature = "set")]
impl<'a, A: BStackAllocator> PartialOrd<BStackSlice<'a, A>> for BStackSliceWriter<'a, A> {
    fn partial_cmp(&self, other: &BStackSlice<'a, A>) -> Option<std::cmp::Ordering> {
        Some(self.cmp(&other.slice()))
    }
}

#[cfg(feature = "set")]
impl<'a, A: BStackAllocator> PartialOrd<BStackSliceWriter<'a, A>> for BStackSliceReader<'a, A> {
    fn partial_cmp(&self, other: &BStackSliceWriter<'a, A>) -> Option<std::cmp::Ordering> {
        let self_pos = self.slice.start() + self.cursor;
        let other_pos = other.slice().start() + other.position();
        Some(
            self_pos
                .cmp(&other_pos)
                .then(self.slice.len().cmp(&other.slice().len())),
        )
    }
}

#[cfg(feature = "set")]
impl<'a, A: BStackAllocator> PartialOrd<BStackSliceReader<'a, A>> for BStackSliceWriter<'a, A> {
    fn partial_cmp(&self, other: &BStackSliceReader<'a, A>) -> Option<std::cmp::Ordering> {
        let self_pos = self.slice.start() + self.cursor;
        let other_pos = other.slice().start() + other.position();
        Some(
            self_pos
                .cmp(&other_pos)
                .then(self.slice.len().cmp(&other.slice().len())),
        )
    }
}

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
pub trait BStackAllocator: Sized {
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
    /// Propagates any [`io::Error`] from underlying operations.
    fn alloc(&self, len: u64) -> io::Result<BStackSlice<'_, Self>>;

    /// Resize `slice` to `new_len` bytes.
    ///
    /// Returns a (possibly different) [`BStackSlice`] for the resized region.
    /// The lifetime `'a` ties the returned slice to the same borrow as the
    /// input slice and the allocator.
    ///
    /// # Errors
    ///
    /// Propagates any [`io::Error`] from underlying operations, including
    /// `Unsupported` if the implementation does not support reallocation.
    fn realloc<'a>(
        &'a self,
        slice: BStackSlice<'a, Self>,
        new_len: u64,
    ) -> io::Result<BStackSlice<'a, Self>>;

    /// Release the region described by `slice`.
    ///
    /// The default implementation is a **no-op**.  Simple bump allocators
    /// accept this default; allocators with free-list tracking should override
    /// it.
    ///
    /// After calling `dealloc`, `slice` must not be used for further I/O.
    ///
    /// # Errors
    ///
    /// The default never errors.  Overriding implementations may propagate
    /// errors from underlying operations.
    fn dealloc(&self, _slice: BStackSlice<'_, Self>) -> io::Result<()> {
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

/// A simple bump allocator that owns a [`BStack`] and allocates regions
/// sequentially by appending to the tail.
///
/// # Realloc policy
///
/// `realloc` is O(1) for the tail allocation.  `realloc` of a non-tail
/// allocation returns [`io::ErrorKind::Unsupported`].
///
/// # Dealloc policy
///
/// `dealloc` reclaims the tail allocation via [`BStack::discard`].  For
/// non-tail allocations it is a no-op — the bytes remain on disk but are
/// logically unreachable through this allocator.
///
/// # Crash consistency
///
/// Every operation maps to exactly one [`BStack`] call and is therefore
/// crash-safe by inheritance:
///
/// | Operation            | Underlying call     |
/// |----------------------|---------------------|
/// | `alloc`              | [`BStack::extend`]  |
/// | `realloc` grow       | [`BStack::extend`]  |
/// | `realloc` shrink     | [`BStack::discard`] |
/// | `dealloc` (tail)     | [`BStack::discard`] |
/// | `dealloc` (non-tail) | no-op               |
///
/// # Example
///
/// ```no_run
/// use bstack::{BStack, BStackAllocator, LinearBStackAllocator};
///
/// # fn main() -> std::io::Result<()> {
/// let alloc = LinearBStackAllocator::new(BStack::open("data.bstack")?);
/// let slice = alloc.alloc(128)?;
/// let data = slice.read()?;
/// alloc.dealloc(slice)?;
/// let stack = alloc.into_stack();
/// # Ok(())
/// # }
/// ```
pub struct LinearBStackAllocator {
    stack: BStack,
}

impl LinearBStackAllocator {
    /// Create a new `LinearBStackAllocator` that takes ownership of `stack`.
    pub fn new(stack: BStack) -> Self {
        Self { stack }
    }
}

impl fmt::Debug for LinearBStackAllocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LinearBStackAllocator")
            .finish_non_exhaustive()
    }
}

impl From<BStack> for LinearBStackAllocator {
    fn from(stack: BStack) -> Self {
        Self::new(stack)
    }
}

impl From<LinearBStackAllocator> for BStack {
    fn from(alloc: LinearBStackAllocator) -> Self {
        alloc.into_stack()
    }
}

impl BStackAllocator for LinearBStackAllocator {
    fn stack(&self) -> &BStack {
        &self.stack
    }

    fn into_stack(self) -> BStack {
        self.stack
    }

    fn alloc(&self, len: u64) -> io::Result<BStackSlice<'_, Self>> {
        let offset = self.stack.extend(len)?;
        Ok(BStackSlice::new(self, offset, len))
    }

    fn realloc<'a>(
        &'a self,
        slice: BStackSlice<'a, Self>,
        new_len: u64,
    ) -> io::Result<BStackSlice<'a, Self>> {
        let current_tail = self.stack.len()?;
        if slice.end() != current_tail {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "LinearBStackAllocator::realloc: non-tail slice cannot be resized in place",
            ));
        }
        match new_len.cmp(&slice.len()) {
            std::cmp::Ordering::Equal => Ok(slice),
            std::cmp::Ordering::Greater => {
                self.stack.extend(new_len - slice.len())?;
                Ok(BStackSlice::new(self, slice.start(), new_len))
            }
            std::cmp::Ordering::Less => {
                self.stack.discard(slice.len() - new_len)?;
                Ok(BStackSlice::new(self, slice.start(), new_len))
            }
        }
    }

    fn dealloc(&self, slice: BStackSlice<'_, Self>) -> io::Result<()> {
        let current_tail = self.stack.len()?;
        if slice.end() == current_tail {
            self.stack.discard(slice.len())?;
        }
        Ok(())
    }
}
