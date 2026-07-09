use super::BStackAllocator;
use crate::BStack;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io;
use std::ops::Range;

/// A raw `(offset, len)` coordinate pair with no backing reference.
///
/// `BStackRange` is the serialization and persistence representation: store it on
/// disk, send it across sessions, or pass it through code that should not perform
/// I/O. To do anything with the region it describes — read, write, allocate, or
/// free — cast it `unsafe`ly into a [`BStackSlice`] (for I/O) or a
/// [`BStackOwnedSlice`] (for allocation operations).
///
/// `BStackRange` carries no validity guarantee. Any `(offset, len)` pair where
/// `offset + len` does not overflow `u64` is a valid `BStackRange`. Whether it
/// describes a live, allocator-owned region is the caller's responsibility when
/// casting.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct BStackRange {
    offset: u64,
    len: u64,
}

impl BStackRange {
    /// Construct a `BStackRange` from raw offset and length.
    ///
    /// # Safety
    /// `offset + len` must not overflow `u64`. The caller is responsible
    /// for ensuring that the range describes a valid region within the payload.
    pub unsafe fn from_raw_parts(offset: u64, len: u64) -> Self {
        Self { offset, len }
    }

    /// Construct a `BStackRange` from raw offset and length, checking
    /// for overflow of `offset + len`.
    ///
    /// Silently caps `len` to avoid overflow of `offset + len`.
    #[inline]
    pub fn new(offset: u64, len: u64) -> Self {
        // Cap len
        Self {
            offset,
            len: len.min(u64::MAX - offset),
        }
    }

    /// Construct a zero-length range anchored at offset 0.
    pub fn empty() -> Self {
        Self { offset: 0, len: 0 }
    }

    /// The inclusive start offset within the [`BStack`] payload.
    #[inline]
    pub fn start(&self) -> u64 {
        self.offset
    }

    /// The exclusive end offset (`start + len`).
    #[inline]
    pub fn end(&self) -> u64 {
        self.offset + self.len
    }

    /// Length of the region in bytes.
    #[inline]
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Returns `true` if the region spans zero bytes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the range as `start..end`.
    #[inline]
    pub fn range(&self) -> Range<u64> {
        self.offset..self.offset + self.len
    }

    /// Serialize to a 16-byte array: `offset` (8 bytes LE) then `len` (8 bytes LE).
    #[inline]
    pub fn to_bytes(self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[..8].copy_from_slice(&self.offset.to_le_bytes());
        out[8..].copy_from_slice(&self.len.to_le_bytes());
        out
    }

    /// Deserialize from a 16-byte array produced by [`to_bytes`](Self::to_bytes).
    ///
    /// Silently caps `len` to avoid overflow of `offset + len`.
    #[inline]
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        let offset = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        let len = u64::from_le_bytes(bytes[8..].try_into().unwrap());
        Self::new(offset, len)
    }
}

impl PartialOrd for BStackRange {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BStackRange {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.offset
            .cmp(&other.offset)
            .then(self.len.cmp(&other.len))
    }
}

impl From<BStackRange> for [u8; 16] {
    #[inline]
    fn from(r: BStackRange) -> Self {
        r.to_bytes()
    }
}

impl From<Range<u64>> for BStackRange {
    #[inline]
    fn from(r: Range<u64>) -> Self {
        // Saturating subtraction since `Range` does not guarantee `start <= end`.
        BStackRange {
            offset: r.start,
            len: r.end.saturating_sub(r.start),
        }
    }
}

impl From<BStackRange> for Range<u64> {
    #[inline]
    fn from(range: BStackRange) -> Self {
        range.offset..range.offset + range.len
    }
}

impl fmt::Debug for BStackRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BStackRange")
            .field("start", &self.start())
            .field("end", &self.end())
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

/// A borrowed, non-owning view of a contiguous region within a [`BStack`] payload.
///
/// `BStackSlice<'a>` is the I/O handle: it carries a `&'a BStack` reference plus
/// an offset and length and exposes read and write operations on that region.
///
/// # Not `Copy`
///
/// `BStackSlice` is deliberately non-`Copy`. This ensures that write methods —
/// which take `&mut self` — provide genuine single-writer exclusivity within safe
/// code: a `BStackSlice` cannot be silently duplicated out of a `&mut` borrow,
/// so two mutable aliases of the same region cannot coexist through safe code.
/// It is `Clone` for cases where an explicit second view is needed.
///
/// # Subslicing
///
/// [`subslice`](Self::subslice) and [`subslice_range`](Self::subslice_range)
/// produce sub-views. They return `BStackSlice<'a>` with the same original
/// lifetime — the sub-view borrows the same `&'a BStack`, not `self`. This means
/// a sub-view can outlive the slice it was derived from, which is intentional:
/// both reference the same underlying resource.
///
/// For mutable sub-views that must not alias, derive them sequentially (consume
/// or reborrow the first before creating the second).
///
/// # Drop
///
/// Drop is a no-op. The region persists on disk beyond this handle's scope.
/// Freeing a region requires an explicit [`BStackOwnedSlice`] and an allocator
/// call.
pub struct BStackSlice<'a> {
    stack: &'a BStack,
    range: BStackRange,
}

impl<'a> Clone for BStackSlice<'a> {
    #[inline]
    fn clone(&self) -> Self {
        BStackSlice {
            stack: self.stack,
            range: self.range,
        }
    }
}

impl<'a> fmt::Debug for BStackSlice<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BStackSlice")
            .field("start", &self.start())
            .field("end", &self.end())
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

impl<'a> BStackSlice<'a> {
    /// Construct a `BStackSlice` from raw parts.
    ///
    /// # Safety
    ///
    /// `offset + len` must not overflow `u64`. `[offset, offset + len)` should
    /// lie within the current payload of `stack` for I/O to succeed (out-of-bounds
    /// I/O returns `io::Error`, thus the caller is responsible for passing
    /// a meaningful coordinate).
    #[inline]
    pub unsafe fn from_raw_parts(stack: &'a BStack, offset: u64, len: u64) -> Self {
        Self {
            stack,
            range: unsafe { BStackRange::from_raw_parts(offset, len) },
        }
    }

    /// Construct a `BStackSlice` from a raw coordinate pair.
    ///
    /// # Safety
    ///
    /// `[offset, offset + len)` should lie within the current payload of `stack`
    /// for I/O to succeed (out-of-bounds I/O returns `io::Error`, thus the caller
    /// is responsible for passing a meaningful coordinate).
    #[inline]
    pub unsafe fn from_raw_range(stack: &'a BStack, range: BStackRange) -> Self {
        Self { stack, range }
    }

    /// Construct a zero-length slice anchored at offset 0.
    ///
    /// All I/O on an empty slice is a no-op or returns an empty result.
    #[inline]
    pub fn empty(stack: &'a BStack) -> Self {
        Self {
            stack,
            range: BStackRange::empty(),
        }
    }

    /// Start offset of this slice within the payload.
    #[inline]
    pub fn start(&self) -> u64 {
        self.range.start()
    }

    /// Exclusive end offset (`start + len`).
    #[inline]
    pub fn end(&self) -> u64 {
        self.range.end()
    }

    /// Length of the slice in bytes.
    #[inline]
    pub fn len(&self) -> u64 {
        self.range.len()
    }

    /// Returns `true` if the slice spans zero bytes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }

    /// Half-open byte range `start..end` of this slice.
    #[inline]
    pub fn range(&self) -> Range<u64> {
        self.range.range()
    }

    /// Return the underlying [`BStack`].
    #[inline]
    pub fn stack(&self) -> &'a BStack {
        self.stack
    }

    /// Create a sub-view `[start, end)` relative to this slice's start.
    ///
    /// Returns a `BStackSlice<'a>` with the same allocator lifetime, so the
    /// sub-view is independent of this slice's borrow.
    ///
    /// # Panics
    ///
    /// Panics if `start > end` or `end > self.len()`.
    #[inline]
    pub fn subslice(&self, start: u64, end: u64) -> BStackSlice<'a> {
        self.subslice_range(start..end)
    }

    /// Create a sub-view for `range` relative to this slice's start.
    ///
    /// # Panics
    ///
    /// Panics if `range.start > range.end` or `range.end > self.len()`.
    pub fn subslice_range(&self, range: Range<u64>) -> BStackSlice<'a> {
        assert!(range.start <= range.end, "range start must be <= end");
        assert!(range.end <= self.len(), "range end must be <= slice length");
        BStackSlice {
            stack: self.stack,
            // SAFETY: `range` is guaranteed to be within `self.len()`, so the resulting
            // slice is guaranteed to be within the payload.
            range: unsafe {
                BStackRange::from_raw_parts(self.start() + range.start, range.end - range.start)
            },
        }
    }

    /// Read the entire slice into a new `Vec<u8>`.
    #[inline]
    pub fn read(&self) -> io::Result<Vec<u8>> {
        self.stack.get(self.start(), self.end())
    }

    /// Read bytes into `buf`, up to `min(buf.len(), self.len())` bytes.
    #[inline]
    pub fn read_into(&self, buf: &mut [u8]) -> io::Result<()> {
        let n = (buf.len() as u64).min(self.len()) as usize;
        self.stack.get_into(self.start(), &mut buf[..n])
    }

    /// Read `[start, end)` relative to this slice into a new `Vec<u8>`.
    pub fn read_range(&self, start: u64, end: u64) -> io::Result<Vec<u8>> {
        if end > self.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("range [{start}, {end}) exceeds slice length {}", self.len()),
            ));
        }
        self.stack.get(self.start() + start, self.start() + end)
    }

    /// Read `[start, start + buf.len())` relative to this slice into `buf`.
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
        self.stack.get_into(self.start() + start, buf)
    }

    /// Overwrite the beginning of this slice with `data` (up to `self.len()` bytes).
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    pub fn write(&mut self, data: impl AsRef<[u8]>) -> io::Result<()> {
        let data = data.as_ref();
        let n = (data.len() as u64).min(self.len()) as usize;
        self.stack.set(self.start(), &data[..n])
    }

    /// Overwrite `[start, start + data.len())` relative to this slice.
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    pub fn write_range(&mut self, start: u64, data: impl AsRef<[u8]>) -> io::Result<()> {
        let data = data.as_ref();
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
        self.stack.set(self.start() + start, data)
    }

    /// Zero out the entire slice.
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    #[inline]
    pub fn zero(&mut self) -> io::Result<()> {
        self.stack.zero(self.start(), self.len())
    }

    /// Zero `[start, start + n)` within this slice.
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    pub fn zero_range(&mut self, start: u64, n: u64) -> io::Result<()> {
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
        self.stack.zero(self.start() + start, n)
    }

    /// Create a cursor-based reader positioned at the start of this slice.
    ///
    /// This clones the slice; the reader and the original slice are independent.
    #[inline]
    pub fn reader(&self) -> BStackSliceReader<'a> {
        BStackSliceReader {
            slice: self.clone(),
            cursor: 0,
        }
    }

    /// Create a cursor-based reader positioned at `offset` bytes into this slice.
    ///
    /// This clones the slice; the reader and the original slice are independent.
    #[inline]
    pub fn reader_at(&self, offset: u64) -> BStackSliceReader<'a> {
        BStackSliceReader {
            slice: self.clone(),
            cursor: offset,
        }
    }

    /// Create a cursor-based writer, consuming this slice.
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    #[inline]
    pub fn writer(self) -> BStackSliceWriter<'a> {
        BStackSliceWriter {
            slice: self,
            cursor: 0,
        }
    }

    /// Create a cursor-based writer positioned at `offset`, consuming this slice.
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    #[inline]
    pub fn writer_at(self, offset: u64) -> BStackSliceWriter<'a> {
        BStackSliceWriter {
            slice: self,
            cursor: offset,
        }
    }
}

impl<'a> PartialEq for BStackSlice<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.range == other.range
    }
}

impl<'a> Eq for BStackSlice<'a> {}

impl<'a> Hash for BStackSlice<'a> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.range.hash(state);
    }
}

impl<'a> PartialOrd for BStackSlice<'a> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for BStackSlice<'a> {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.range.cmp(&other.range)
    }
}

impl<'a> From<BStackSlice<'a>> for BStackRange {
    #[inline]
    fn from(s: BStackSlice<'a>) -> BStackRange {
        s.range
    }
}

impl<'a> From<BStackSlice<'a>> for [u8; 16] {
    #[inline]
    fn from(s: BStackSlice<'a>) -> Self {
        s.range.to_bytes()
    }
}

impl<'a> From<BStackSlice<'a>> for BStackSliceReader<'a> {
    #[inline]
    fn from(slice: BStackSlice<'a>) -> Self {
        BStackSliceReader { slice, cursor: 0 }
    }
}

#[cfg(feature = "set")]
impl<'a> From<BStackSlice<'a>> for BStackSliceWriter<'a> {
    #[inline]
    fn from(slice: BStackSlice<'a>) -> Self {
        BStackSliceWriter { slice, cursor: 0 }
    }
}

/// An owned allocation handle for a region managed by a [`BStackAllocator`].
///
/// `BStackOwnedSlice<'a, A>` represents exclusive ownership of one allocation.
/// It is the type returned by [`BStackAllocator::alloc`] and consumed by
/// [`BStackAllocator::realloc`] and [`BStackAllocator::dealloc`].
///
/// # Not `Copy` or `Clone`
///
/// An allocation has exactly one owner. Making this type non-`Copy` and
/// non-`Clone` turns use-after-free and use-after-realloc into **compile
/// errors**: once the handle is consumed by `realloc` or `dealloc`, no copy
/// survives to be misused. This is the primary safety guarantee of the type.
///
/// # No direct I/O
///
/// `BStackOwnedSlice` does not read or write. To access the region, convert it
/// to a borrowed view:
///
/// - [`as_slice`](Self::as_slice) — shared view, allows reads. The returned
///   [`BStackSlice`] borrows `self` and cannot outlive it.
/// - [`as_slice_mut`](Self::as_slice_mut) — exclusive view, allows reads and
///   writes. Blocks any other use of `self` while live.
///
/// # Drop
///
/// Drop is a no-op. The allocation persists on disk beyond this handle's scope.
/// Pass the handle to [`BStackAllocator::dealloc`] to explicitly free the region.
pub struct BStackOwnedSlice<'a, A: BStackAllocator> {
    allocator: &'a A,
    range: BStackRange,
}

impl<'a, A: BStackAllocator> fmt::Debug for BStackOwnedSlice<'a, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BStackOwnedSlice")
            .field("start", &self.start())
            .field("end", &self.end())
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

impl<'a, A: BStackAllocator> BStackOwnedSlice<'a, A> {
    /// Construct an owned handle from raw parts.
    ///
    /// # Safety
    ///
    /// `offset + len` must not overflow `u64`. `(offset, len)` must describe
    /// an allocation that was returned by `allocator.alloc` or a prior
    /// `allocator.realloc` and has not yet been freed. Passing an arbitrary
    /// or sub-slice coordinate and then passing it to `realloc` or `dealloc`
    /// may silently corrupt allocator metadata.
    #[inline]
    pub unsafe fn from_raw_parts(allocator: &'a A, offset: u64, len: u64) -> Self {
        Self {
            allocator,
            range: unsafe { BStackRange::from_raw_parts(offset, len) },
        }
    }

    /// Construct an owned handle from a raw coordinate pair.
    ///
    /// # Safety
    ///
    /// `(offset, len)` must describe an allocation that was returned by
    /// `allocator.alloc` or a prior `allocator.realloc` and has not yet been freed.
    /// Passing an arbitrary or sub-slice coordinate and then passing it to `realloc`
    /// or `dealloc` may silently corrupt allocator metadata.
    #[inline]
    pub unsafe fn from_raw_range(allocator: &'a A, range: BStackRange) -> Self {
        Self { allocator, range }
    }

    /// Construct an empty (zero-length) owned handle.
    ///
    /// Used as a sentinel. `dealloc` of an empty handle at offset 0 is a no-op
    /// in all library allocators.
    #[inline]
    pub fn empty(allocator: &'a A) -> Self {
        Self {
            allocator,
            range: BStackRange::empty(),
        }
    }

    /// Start offset of the allocation within the payload.
    #[inline]
    pub fn start(&self) -> u64 {
        self.range.start()
    }

    /// Exclusive end offset (`start + len`).
    #[inline]
    pub fn end(&self) -> u64 {
        self.range.end()
    }

    /// Length of the allocation in bytes.
    #[inline]
    pub fn len(&self) -> u64 {
        self.range.len()
    }

    /// Returns `true` if the allocation spans zero bytes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }

    /// Half-open byte range `start..end` of this allocation.
    #[inline]
    pub fn range(&self) -> Range<u64> {
        self.range.range()
    }

    /// Return the allocator that owns this handle.
    #[inline]
    pub fn allocator(&self) -> &'a A {
        self.allocator
    }

    /// Borrow this allocation as a shared [`BStackSlice`] for reads.
    ///
    /// The returned slice's lifetime is tied to `&self` — it cannot outlive
    /// this handle. This prevents a view from surviving `dealloc` or `realloc`,
    /// which both consume the handle by value.
    #[inline]
    pub fn as_slice<'s>(&'s self) -> BStackSlice<'s> {
        BStackSlice {
            stack: self.allocator.stack(),
            range: self.range,
        }
    }

    /// Borrow this allocation as an exclusive [`BStackSlice`] for reads and writes.
    ///
    /// The `&mut self` receiver makes the borrow exclusive: no other view of
    /// this allocation can be obtained while the returned slice is live. Within
    /// safe code this enforces single-writer access.
    #[inline]
    pub fn as_slice_mut<'s>(&'s mut self) -> BStackSlice<'s> {
        BStackSlice {
            stack: self.allocator.stack(),
            range: self.range,
        }
    }

    /// Read the entire allocation into a new `Vec<u8>`.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice)
    /// and delegates to [`BStackSlice::read`].
    #[inline]
    pub fn read(&self) -> io::Result<Vec<u8>> {
        self.as_slice().read()
    }

    /// Read up to `buf.len()` bytes into `buf`.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice)
    /// and delegates to [`BStackSlice::read_into`].
    #[inline]
    pub fn read_into(&self, buf: &mut [u8]) -> io::Result<()> {
        self.as_slice().read_into(buf)
    }

    /// Read `[start, end)` relative to this allocation into a new `Vec<u8>`.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice)
    /// and delegates to [`BStackSlice::read_range`].
    #[inline]
    pub fn read_range(&self, start: u64, end: u64) -> io::Result<Vec<u8>> {
        self.as_slice().read_range(start, end)
    }

    /// Read `[start, start + buf.len())` into `buf`.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice)
    /// and delegates to [`BStackSlice::read_range_into`].
    #[inline]
    pub fn read_range_into(&self, start: u64, buf: &mut [u8]) -> io::Result<()> {
        self.as_slice().read_range_into(start, buf)
    }

    /// Overwrite the beginning of this allocation with `data`.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice_mut`](Self::as_slice_mut)
    /// and delegates to [`BStackSlice::write`].
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    #[inline]
    pub fn write(&mut self, data: impl AsRef<[u8]>) -> io::Result<()> {
        self.as_slice_mut().write(data)
    }

    /// Overwrite `[start, start + data.len())` within this allocation.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice_mut`](Self::as_slice_mut)
    /// and delegates to [`BStackSlice::write_range`].
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    #[inline]
    pub fn write_range(&mut self, start: u64, data: impl AsRef<[u8]>) -> io::Result<()> {
        self.as_slice_mut().write_range(start, data)
    }

    /// Zero out the entire allocation.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice_mut`](Self::as_slice_mut)
    /// and delegates to [`BStackSlice::zero`].
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    #[inline]
    pub fn zero(&mut self) -> io::Result<()> {
        self.as_slice_mut().zero()
    }

    /// Zero `[start, start + n)` within this allocation.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice_mut`](Self::as_slice_mut)
    /// and delegates to [`BStackSlice::zero_range`].
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    #[inline]
    pub fn zero_range(&mut self, start: u64, n: u64) -> io::Result<()> {
        self.as_slice_mut().zero_range(start, n)
    }

    /// Create a cursor-based reader over this allocation.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice)
    /// and delegates to [`BStackSlice::reader`].
    #[inline]
    pub fn reader<'s>(&'s self) -> BStackSliceReader<'s> {
        self.as_slice().reader()
    }

    /// Create a cursor-based reader positioned at `offset` into this allocation.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice)
    /// and delegates to [`BStackSlice::reader_at`].
    #[inline]
    pub fn reader_at<'s>(&'s self, offset: u64) -> BStackSliceReader<'s> {
        self.as_slice().reader_at(offset)
    }

    /// Create a cursor-based writer over this allocation, borrowing it mutably.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice_mut`](Self::as_slice_mut)
    /// and delegates to [`BStackSlice::writer`].
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    #[inline]
    pub fn writer<'s>(&'s mut self) -> BStackSliceWriter<'s> {
        self.as_slice_mut().writer()
    }

    /// Create a cursor-based writer positioned at `offset`, borrowing this allocation mutably.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice_mut`](Self::as_slice_mut)
    /// and delegates to [`BStackSlice::writer_at`].
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    #[inline]
    pub fn writer_at<'s>(&'s mut self, offset: u64) -> BStackSliceWriter<'s> {
        self.as_slice_mut().writer_at(offset)
    }
}

impl<'a, A: BStackAllocator> PartialEq for BStackOwnedSlice<'a, A> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.range == other.range
    }
}

impl<'a, A: BStackAllocator> Eq for BStackOwnedSlice<'a, A> {}

impl<'a, A: BStackAllocator> Hash for BStackOwnedSlice<'a, A> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.range.hash(state);
    }
}

impl<'a, A: BStackAllocator> PartialOrd for BStackOwnedSlice<'a, A> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a, A: BStackAllocator> Ord for BStackOwnedSlice<'a, A> {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.range.cmp(&other.range)
    }
}

impl<'a, A: BStackAllocator> From<BStackOwnedSlice<'a, A>> for BStackRange {
    #[inline]
    fn from(s: BStackOwnedSlice<'a, A>) -> BStackRange {
        s.range
    }
}

impl<'a, A: BStackAllocator> From<BStackOwnedSlice<'a, A>> for [u8; 16] {
    #[inline]
    fn from(s: BStackOwnedSlice<'a, A>) -> Self {
        s.range.to_bytes()
    }
}

/// A cursor-based reader over a [`BStackSlice`].
///
/// Implements [`io::Read`] and [`io::Seek`] within the coordinate space of the
/// slice — position 0 maps to `slice.start()` in the underlying payload.
///
/// Created via [`BStackSlice::reader`], [`BStackSlice::reader_at`],
/// [`BStackOwnedSlice::reader`], or `From<BStackSlice>`.
///
/// Clone is provided because reading is non-destructive — two readers over the
/// same region can coexist.
pub struct BStackSliceReader<'a> {
    slice: BStackSlice<'a>,
    cursor: u64,
}

impl<'a> Clone for BStackSliceReader<'a> {
    fn clone(&self) -> Self {
        BStackSliceReader {
            slice: self.slice.clone(),
            cursor: self.cursor,
        }
    }
}

impl<'a> fmt::Debug for BStackSliceReader<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BStackSliceReader")
            .field("start", &self.slice.start())
            .field("end", &self.slice.end())
            .field("len", &self.slice.len())
            .field("cursor", &self.cursor)
            .finish_non_exhaustive()
    }
}

impl<'a> BStackSliceReader<'a> {
    /// Return the current cursor position within the slice (not the payload).
    #[inline]
    pub fn position(&self) -> u64 {
        self.cursor
    }

    /// Consume the reader and return the underlying [`BStackSlice`].
    #[inline]
    pub fn into_slice(self) -> BStackSlice<'a> {
        self.slice
    }

    /// Return a reference to the underlying [`BStackSlice`].
    #[inline]
    pub fn slice(&self) -> &BStackSlice<'a> {
        &self.slice
    }
}

impl<'a> io::Read for BStackSliceReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() || self.cursor >= self.slice.len() {
            return Ok(0);
        }
        let available = (self.slice.len() - self.cursor) as usize;
        let n = buf.len().min(available);
        let abs_start = self.slice.start() + self.cursor;
        self.slice.stack.get_into(abs_start, &mut buf[..n])?;
        self.cursor += n as u64;
        Ok(n)
    }
}

impl<'a> io::Seek for BStackSliceReader<'a> {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let len = self.slice.len() as i128;
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

impl<'a> PartialEq for BStackSliceReader<'a> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.slice == other.slice && self.cursor == other.cursor
    }
}

impl<'a> Eq for BStackSliceReader<'a> {}

impl<'a> Hash for BStackSliceReader<'a> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.slice.hash(state);
        self.cursor.hash(state);
    }
}

impl<'a> PartialOrd for BStackSliceReader<'a> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for BStackSliceReader<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let self_pos = self.slice.start() + self.cursor;
        let other_pos = other.slice.start() + other.cursor;
        self_pos
            .cmp(&other_pos)
            .then(self.slice.len().cmp(&other.slice.len()))
    }
}

impl<'a> From<BStackSliceReader<'a>> for BStackSlice<'a> {
    #[inline]
    fn from(r: BStackSliceReader<'a>) -> Self {
        r.into_slice()
    }
}

impl<'a> PartialEq<BStackSlice<'a>> for BStackSliceReader<'a> {
    #[inline]
    fn eq(&self, other: &BStackSlice<'a>) -> bool {
        &self.slice == other
    }
}

impl<'a> PartialEq<BStackSliceReader<'a>> for BStackSlice<'a> {
    #[inline]
    fn eq(&self, other: &BStackSliceReader<'a>) -> bool {
        self == &other.slice
    }
}

impl<'a> PartialOrd<BStackSlice<'a>> for BStackSliceReader<'a> {
    #[inline]
    fn partial_cmp(&self, other: &BStackSlice<'a>) -> Option<std::cmp::Ordering> {
        Some(self.slice.cmp(other))
    }
}

impl<'a> PartialOrd<BStackSliceReader<'a>> for BStackSlice<'a> {
    #[inline]
    fn partial_cmp(&self, other: &BStackSliceReader<'a>) -> Option<std::cmp::Ordering> {
        Some(self.cmp(&other.slice))
    }
}

/// A cursor-based writer over a [`BStackSlice`].
///
/// Implements [`io::Write`] and [`io::Seek`] within the coordinate space of the
/// slice. Every write delegates to [`BStack::set`] and is durably synced.
///
/// Created via [`BStackSlice::writer`], [`BStackSlice::writer_at`],
/// [`BStackOwnedSlice::writer`], or `From<BStackSlice>`.
///
/// Not `Clone` — the writer represents exclusive write intent over its slice.
///
/// Requires the `set` feature.
#[cfg(feature = "set")]
pub struct BStackSliceWriter<'a> {
    slice: BStackSlice<'a>,
    cursor: u64,
}

#[cfg(feature = "set")]
impl<'a> fmt::Debug for BStackSliceWriter<'a> {
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
impl<'a> BStackSliceWriter<'a> {
    /// Return the current cursor position within the slice (not the payload).
    #[inline]
    pub fn position(&self) -> u64 {
        self.cursor
    }

    /// Consume the writer and return the underlying [`BStackSlice`].
    #[inline]
    pub fn into_slice(self) -> BStackSlice<'a> {
        self.slice
    }

    /// Return a reference to the underlying [`BStackSlice`].
    #[inline]
    pub fn slice(&self) -> &BStackSlice<'a> {
        &self.slice
    }
}

#[cfg(feature = "set")]
impl<'a> io::Write for BStackSliceWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() || self.cursor >= self.slice.len() {
            return Ok(0);
        }
        let available = (self.slice.len() - self.cursor) as usize;
        let n = buf.len().min(available);
        let abs_start = self.slice.start() + self.cursor;
        self.slice.stack.set(abs_start, &buf[..n])?;
        self.cursor += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(feature = "set")]
impl<'a> io::Seek for BStackSliceWriter<'a> {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let len = self.slice.len() as i128;
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
impl<'a> PartialEq for BStackSliceWriter<'a> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.slice == other.slice && self.cursor == other.cursor
    }
}

#[cfg(feature = "set")]
impl<'a> Eq for BStackSliceWriter<'a> {}

#[cfg(feature = "set")]
impl<'a> Hash for BStackSliceWriter<'a> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.slice.hash(state);
        self.cursor.hash(state);
    }
}

#[cfg(feature = "set")]
impl<'a> PartialOrd for BStackSliceWriter<'a> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(feature = "set")]
impl<'a> Ord for BStackSliceWriter<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let self_pos = self.slice.start() + self.cursor;
        let other_pos = other.slice.start() + other.cursor;
        self_pos
            .cmp(&other_pos)
            .then(self.slice.len().cmp(&other.slice.len()))
    }
}

#[cfg(feature = "set")]
impl<'a> From<BStackSliceWriter<'a>> for BStackSlice<'a> {
    #[inline]
    fn from(w: BStackSliceWriter<'a>) -> Self {
        w.into_slice()
    }
}

#[cfg(feature = "set")]
impl<'a> From<BStackSliceReader<'a>> for BStackSliceWriter<'a> {
    #[inline]
    fn from(r: BStackSliceReader<'a>) -> Self {
        BStackSliceWriter {
            slice: r.slice,
            cursor: r.cursor,
        }
    }
}

#[cfg(feature = "set")]
impl<'a> From<BStackSliceWriter<'a>> for BStackSliceReader<'a> {
    #[inline]
    fn from(w: BStackSliceWriter<'a>) -> Self {
        BStackSliceReader {
            slice: w.slice,
            cursor: w.cursor,
        }
    }
}

#[cfg(feature = "set")]
impl<'a> PartialEq<BStackSliceWriter<'a>> for BStackSliceReader<'a> {
    #[inline]
    fn eq(&self, other: &BStackSliceWriter<'a>) -> bool {
        self.slice == other.slice && self.cursor == other.cursor
    }
}

#[cfg(feature = "set")]
impl<'a> PartialEq<BStackSliceReader<'a>> for BStackSliceWriter<'a> {
    #[inline]
    fn eq(&self, other: &BStackSliceReader<'a>) -> bool {
        self.slice == other.slice && self.cursor == other.cursor
    }
}

#[cfg(feature = "set")]
impl<'a> PartialEq<BStackSlice<'a>> for BStackSliceWriter<'a> {
    #[inline]
    fn eq(&self, other: &BStackSlice<'a>) -> bool {
        &self.slice == other
    }
}

#[cfg(feature = "set")]
impl<'a> PartialEq<BStackSliceWriter<'a>> for BStackSlice<'a> {
    #[inline]
    fn eq(&self, other: &BStackSliceWriter<'a>) -> bool {
        self == &other.slice
    }
}

#[cfg(feature = "set")]
impl<'a> PartialOrd<BStackSlice<'a>> for BStackSliceWriter<'a> {
    #[inline]
    fn partial_cmp(&self, other: &BStackSlice<'a>) -> Option<std::cmp::Ordering> {
        Some(self.slice.cmp(other))
    }
}

#[cfg(feature = "set")]
impl<'a> PartialOrd<BStackSliceWriter<'a>> for BStackSliceReader<'a> {
    fn partial_cmp(&self, other: &BStackSliceWriter<'a>) -> Option<std::cmp::Ordering> {
        let self_pos = self.slice.start() + self.cursor;
        let other_pos = other.slice.start() + other.cursor;
        Some(
            self_pos
                .cmp(&other_pos)
                .then(self.slice.len().cmp(&other.slice.len())),
        )
    }
}

#[cfg(feature = "set")]
impl<'a> PartialOrd<BStackSliceReader<'a>> for BStackSliceWriter<'a> {
    fn partial_cmp(&self, other: &BStackSliceReader<'a>) -> Option<std::cmp::Ordering> {
        let self_pos = self.slice.start() + self.cursor;
        let other_pos = other.slice.start() + other.cursor;
        Some(
            self_pos
                .cmp(&other_pos)
                .then(self.slice.len().cmp(&other.slice.len())),
        )
    }
}
