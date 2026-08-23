use super::BStackAllocator;
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
    ///
    /// # Deprecation
    ///
    /// This constructor is deprecated in favour of the explicitly-unsafe
    /// [`BStackSlice::from_raw_parts`], which makes the caller's
    /// responsibility visible at the call site.  Replace any call
    /// `BStackSlice::new(allocator, offset, len)` with
    /// `unsafe { BStackSlice::from_raw_parts(allocator, offset, len) }` and
    /// ensure the `# Safety` contract of `from_raw_parts` is upheld.
    #[deprecated(
        since = "0.1.10",
        note = "Use `unsafe { BStackSlice::from_raw_parts(allocator, offset, len) }` instead; \
                see `BStackSlice::from_raw_parts` for the required safety contract."
    )]
    #[inline]
    pub fn new(allocator: &'a A, offset: u64, len: u64) -> Self {
        Self {
            allocator,
            offset,
            len,
        }
    }

    /// Construct a `BStackSlice` from raw parts.
    ///
    /// This is the explicitly-unsafe replacement for the deprecated
    /// [`BStackSlice::new`].  The name reflects that an arbitrary
    /// `(offset, len)` pair can bypass invariants that allocators rely on.
    ///
    /// # Safety
    ///
    /// The caller must uphold **all** of the following:
    ///
    /// * `offset + len` must not overflow `u64`.
    /// * For I/O calls (`read`, `write`, `read_range`, etc.) the range
    ///   `[offset, offset + len)` should lie within the current payload of
    ///   the backing stack.  Out-of-bounds accesses produce `io::Error`
    ///   rather than unsound behaviour, so this is a correctness requirement,
    ///   not a soundness one.
    /// * **If the slice will be passed to [`BStackAllocator::realloc`] or
    ///   [`BStackAllocator::dealloc`]**, `(offset, len)` must describe an
    ///   allocation that was directly returned by [`BStackAllocator::alloc`]
    ///   or by a prior [`BStackAllocator::realloc`] on the **same allocator
    ///   instance**.  Passing an arbitrary offset or a sub-slice derived via
    ///   [`subslice`](BStackSlice::subslice) /
    ///   [`subslice_range`](BStackSlice::subslice_range) may silently corrupt
    ///   the allocator's persistent metadata in a way that is difficult or
    ///   impossible to recover from.
    #[inline]
    #[must_use]
    pub unsafe fn from_raw_parts(allocator: &'a A, offset: u64, len: u64) -> Self {
        Self {
            allocator,
            offset,
            len,
        }
    }

    /// Construct a zero-length `BStackSlice` anchored at offset 0.
    ///
    /// The resulting slice spans no bytes and all I/O methods on it are
    /// no-ops or return empty results.  It is safe to construct because an
    /// empty slice cannot produce out-of-bounds reads or writes and carries
    /// no allocator-origin requirement.
    ///
    /// Useful as a sentinel or default value when a slice field must be
    /// initialized before a real allocation is available.
    #[inline]
    #[must_use]
    pub fn empty(allocator: &'a A) -> Self {
        Self {
            allocator,
            offset: 0,
            len: 0,
        }
    }

    /// Serialize this slice to a 16-byte array for on-disk storage.
    ///
    /// Layout: `offset` as 8 bytes little-endian, then `len` as 8 bytes
    /// little-endian.  Reconstruct with [`BStackSlice::from_bytes`].
    #[inline]
    #[must_use]
    pub fn to_bytes(&self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[..8].copy_from_slice(&self.offset.to_le_bytes());
        out[8..].copy_from_slice(&self.len.to_le_bytes());
        out
    }

    /// Reconstruct a `BStackSlice` from a 16-byte array produced by
    /// [`BStackSlice::to_bytes`].
    ///
    /// # Safety
    ///
    /// The caller must ensure that `bytes` encodes a valid offset and length
    /// that lie within the bounds of the underlying allocator's payload.
    /// Passing an arbitrary or corrupted byte array is undefined behaviour.
    #[inline]
    #[must_use]
    pub unsafe fn from_bytes(allocator: &'a A, bytes: [u8; 16]) -> Self {
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
    #[must_use]
    pub fn start(&self) -> u64 {
        self.offset
    }

    /// The exclusive end offset of this slice within the payload
    /// (`self.start() + self.len()`).
    #[inline]
    #[must_use]
    pub fn end(&self) -> u64 {
        self.offset + self.len
    }

    /// Returns the range of this slice as `start..end` within the payload.
    #[inline]
    #[must_use]
    pub fn range(&self) -> Range<u64> {
        self.start()..self.end()
    }

    /// Returns the length of this slice in bytes.
    #[inline]
    #[must_use]
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Returns `true` if this slice spans zero bytes.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Return the underlying allocator.
    #[inline]
    #[must_use]
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
    #[must_use]
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
    #[must_use]
    #[track_caller]
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
    #[must_use]
    #[track_caller]
    pub fn subslice_range(&self, range: Range<u64>) -> BStackSlice<'a, A> {
        assert!(range.start <= range.end, "range start must be <= end");
        assert!(range.end <= self.len, "range end must be <= slice length");
        BStackSlice {
            allocator: self.allocator,
            offset: self.offset + range.start,
            len: range.end - range.start,
        }
    }

    /// Split into two sub-views at `mid`, relative to this slice's start.
    ///
    /// Equivalent to `(self.subslice(0, mid), self.subslice(mid, self.len()))`,
    /// following `std` slice naming.
    ///
    /// # Panics
    ///
    /// Panics if `mid > self.len()`.
    #[inline]
    #[track_caller]
    pub fn split_at(&self, mid: u64) -> (BStackSlice<'a, A>, BStackSlice<'a, A>) {
        assert!(mid <= self.len(), "split_at: mid must be <= slice length");
        (self.subslice(0, mid), self.subslice(mid, self.len()))
    }

    /// Split into two independent sub-views at `mid`, relative to this slice's
    /// start.
    ///
    /// The returned slices are independent — like [`subslice`](Self::subslice),
    /// they carry the original `&'a A` allocator lifetime rather than borrowing
    /// from `self`.
    ///
    /// # Panics
    ///
    /// Panics if `mid > self.len()`.
    #[inline]
    #[track_caller]
    pub fn split_at_mut(&mut self, mid: u64) -> (BStackSlice<'a, A>, BStackSlice<'a, A>) {
        assert!(
            mid <= self.len(),
            "split_at_mut: mid must be <= slice length"
        );
        (self.subslice(0, mid), self.subslice(mid, self.len()))
    }

    /// Return a sub-view of the first `n` bytes.
    ///
    /// The returned slice has length `min(n, self.len())`.
    #[inline]
    #[must_use]
    pub fn head(&self, n: u64) -> BStackSlice<'a, A> {
        let n = n.min(self.len());
        self.subslice(0, n)
    }

    /// Return a sub-view of the last `n` bytes.
    ///
    /// The returned slice has length `min(n, self.len())`.
    #[inline]
    #[must_use]
    pub fn tail(&self, n: u64) -> BStackSlice<'a, A> {
        let n = n.min(self.len());
        self.subslice(self.len() - n, self.len())
    }

    /// Read the byte at `index`, or `None` if out of bounds.
    #[inline]
    pub fn get(&self, index: u64) -> io::Result<Option<u8>> {
        if index >= self.len() {
            return Ok(None);
        }
        let mut buf = [0u8; 1];
        self.stack().get_into(self.start() + index, &mut buf)?;
        Ok(Some(buf[0]))
    }

    /// Returns `true` if the slice contains `needle`.
    #[inline]
    pub fn contains(&self, needle: u8) -> io::Result<bool> {
        Ok(self.read()?.contains(&needle))
    }

    /// Returns `true` if the slice begins with `prefix`.
    pub fn starts_with(&self, prefix: &[u8]) -> io::Result<bool> {
        let n = prefix.len() as u64;
        if n > self.len() {
            return Ok(false);
        }
        Ok(self.head(n).read()? == prefix)
    }

    /// Returns `true` if the slice ends with `suffix`.
    pub fn ends_with(&self, suffix: &[u8]) -> io::Result<bool> {
        let n = suffix.len() as u64;
        if n > self.len() {
            return Ok(false);
        }
        Ok(self.tail(n).read()? == suffix)
    }

    /// Returns the index of the first occurrence of `needle`, or `None` if not
    /// found.
    #[inline]
    pub fn find(&self, needle: u8) -> io::Result<Option<u64>> {
        Ok(self
            .read()?
            .iter()
            .position(|&b| b == needle)
            .map(|i| i as u64))
    }

    /// Returns the index of the last occurrence of `needle`, or `None` if not
    /// found.
    #[inline]
    pub fn rfind(&self, needle: u8) -> io::Result<Option<u64>> {
        Ok(self
            .read()?
            .iter()
            .rposition(|&b| b == needle)
            .map(|i| i as u64))
    }

    /// Returns the index of the first byte satisfying `predicate`, or `None`.
    #[inline]
    pub fn position(&self, predicate: impl Fn(u8) -> bool) -> io::Result<Option<u64>> {
        Ok(self
            .read()?
            .iter()
            .position(|&b| predicate(b))
            .map(|i| i as u64))
    }

    /// Returns the index of the last byte satisfying `predicate`, or `None`.
    #[inline]
    pub fn rposition(&self, predicate: impl Fn(u8) -> bool) -> io::Result<Option<u64>> {
        Ok(self
            .read()?
            .iter()
            .rposition(|&b| predicate(b))
            .map(|i| i as u64))
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

    /// Read a sub-range `[start, end)` relative to this slice into a newly
    /// allocated `Vec<u8>`.
    ///
    /// `start` and `end` are relative to `self.start()`, not the payload start.
    ///
    /// # Errors
    ///
    /// Returns an error if `start > end` or if `end` exceeds `self.len()`.
    pub fn read_range(&self, start: u64, end: u64) -> io::Result<Vec<u8>> {
        if end > self.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("range [{start}, {end}) exceeds slice length {}", self.len()),
            ));
        }
        self.stack().get(self.start() + start, self.start() + end)
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
    pub fn write(&self, data: impl AsRef<[u8]>) -> io::Result<()> {
        let data = data.as_ref();
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
    pub fn write_range(&self, start: u64, data: impl AsRef<[u8]>) -> io::Result<()> {
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

    /// Fill the entire slice with `value`.
    ///
    /// A single crash-atomic [`BStack::repeat`] call.
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    #[inline]
    pub fn fill(&mut self, value: u8) -> io::Result<()> {
        self.stack().repeat(self.start(), [value], self.len())
    }

    /// Fill the slice by calling `f` once per byte.
    ///
    /// The generated bytes are staged in memory and committed with a single
    /// crash-atomic [`write`](Self::write) call.
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    #[inline]
    pub fn fill_with(&mut self, mut f: impl FnMut() -> u8) -> io::Result<()> {
        let buf: Vec<u8> = (0..self.len()).map(|_| f()).collect();
        self.write(buf)
    }

    /// Copy `src` into this slice.
    ///
    /// A single crash-atomic [`BStack::set`] call.
    ///
    /// Requires the `set` feature.
    ///
    /// # Panics
    ///
    /// Panics if `src.len() != self.len()`.
    #[cfg(feature = "set")]
    #[inline]
    #[track_caller]
    pub fn copy_from_slice(&mut self, src: &[u8]) -> io::Result<()> {
        assert_eq!(
            src.len() as u64,
            self.len(),
            "copy_from_slice: length mismatch"
        );
        self.stack().set(self.start(), src)
    }

    /// Copy the contents of `src` into this slice.
    ///
    /// A single crash-atomic [`BStack::copy`] call. `src` and `self` may
    /// overlap or refer to the same region.
    ///
    /// Requires the `set` and `atomic` features.
    ///
    /// # Panics
    ///
    /// Panics if `src.len() != self.len()`.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `src` is backed by a
    /// different [`BStack`].
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[track_caller]
    pub fn copy_from_bstack_slice(&mut self, src: &BStackSlice<'_, A>) -> io::Result<()> {
        assert_eq!(
            src.len(),
            self.len(),
            "copy_from_bstack_slice: length mismatch"
        );
        if !std::ptr::eq(src.stack(), self.stack()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "BStackSlice::copy_from_bstack_slice: source belongs to a different BStack",
            ));
        }
        if self.is_empty() {
            return Ok(());
        }
        self.stack().copy(src.start(), self.start(), self.len())
    }

    /// Copy `src_range` (relative to this slice) to `dest` (relative to this
    /// slice), within this slice.
    ///
    /// A single crash-atomic [`BStack::copy`] call; overlapping source and
    /// destination are handled correctly.
    ///
    /// Requires the `set` and `atomic` features.
    ///
    /// # Panics
    ///
    /// Panics if `src_range.start > src_range.end`, if `src_range.end >
    /// self.len()`, or if `dest + src_range.len()` overflows `u64` or exceeds
    /// `self.len()`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[track_caller]
    pub fn copy_within(&mut self, src_range: Range<u64>, dest: u64) -> io::Result<()> {
        assert!(
            src_range.start <= src_range.end,
            "copy_within: range start must be <= end"
        );
        assert!(
            src_range.end <= self.len(),
            "copy_within: range end must be <= slice length"
        );
        let n = src_range.end - src_range.start;
        let dest_end = dest
            .checked_add(n)
            .expect("copy_within: dest + len overflows u64");
        assert!(
            dest_end <= self.len(),
            "copy_within: dest range exceeds slice length"
        );
        if n == 0 {
            return Ok(());
        }
        self.stack()
            .copy(self.start() + src_range.start, self.start() + dest, n)
    }

    /// Swap the contents of this slice with `other`.
    ///
    /// A single crash-atomic [`BStack::cross_exchange`] call.
    ///
    /// Requires the `set` and `atomic` features.
    ///
    /// # Panics
    ///
    /// Panics if `self.len() != other.len()`.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `other` is backed by a
    /// different [`BStack`].
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[track_caller]
    pub fn swap(&mut self, other: &mut BStackSlice<'_, A>) -> io::Result<()> {
        assert_eq!(self.len(), other.len(), "swap: length mismatch");
        if !std::ptr::eq(self.stack(), other.stack()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "BStackSlice::swap: slices belong to different BStacks",
            ));
        }
        if self.is_empty() || self.start() == other.start() {
            return Ok(());
        }
        self.stack()
            .cross_exchange(self.start(), other.start(), self.len())
    }

    /// Reverse the byte order of this slice in place.
    ///
    /// A single crash-atomic [`BStack::process`] call: the bytes are read,
    /// reversed in memory, then committed in one write.
    ///
    /// Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[inline]
    pub fn reverse(&mut self) -> io::Result<()> {
        self.stack()
            .process(self.start(), self.end(), |buf| buf.reverse())
    }

    /// Rotate the slice in place such that the bytes at `[mid, len)` move to
    /// the front.
    ///
    /// A single crash-atomic [`BStack::process`] call.
    ///
    /// Requires the `set` and `atomic` features.
    ///
    /// # Panics
    ///
    /// Panics if `mid > self.len()`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[track_caller]
    pub fn rotate_left(&mut self, mid: u64) -> io::Result<()> {
        assert!(
            mid <= self.len(),
            "rotate_left: mid must be <= slice length"
        );
        self.stack().process(self.start(), self.end(), |buf| {
            buf.rotate_left(mid as usize)
        })
    }

    /// Rotate the slice in place such that the last `k` bytes move to the
    /// front.
    ///
    /// A single crash-atomic [`BStack::process`] call.
    ///
    /// Requires the `set` and `atomic` features.
    ///
    /// # Panics
    ///
    /// Panics if `k > self.len()`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[track_caller]
    pub fn rotate_right(&mut self, k: u64) -> io::Result<()> {
        assert!(k <= self.len(), "rotate_right: k must be <= slice length");
        self.stack()
            .process(self.start(), self.end(), |buf| buf.rotate_right(k as usize))
    }

    /// Create a cursor-based reader positioned at the start of this slice.
    ///
    /// The reader implements [`io::Read`] and [`io::Seek`] in the coordinate
    /// space `[0, self.len())`.
    #[inline]
    #[must_use]
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
    #[inline]
    #[must_use]
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
    #[inline]
    #[must_use]
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
    #[inline]
    #[must_use]
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
    #[inline]
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
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, A: BStackAllocator> Copy for BStackSliceReader<'a, A> {}

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
    #[must_use]
    pub fn position(&self) -> u64 {
        self.cursor
    }

    /// Return the underlying [`BStackSlice`].
    #[inline]
    #[must_use]
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
        *self
    }
}

#[cfg(feature = "set")]
impl<'a, A: BStackAllocator> Copy for BStackSliceWriter<'a, A> {}

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
    #[must_use]
    pub fn position(&self) -> u64 {
        self.cursor
    }

    /// Return the underlying [`BStackSlice`].
    #[inline]
    #[must_use]
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
    #[inline]
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
/// The reader and writer share the same underlying slice and cursor position.
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
impl<'a, A: BStackAllocator> PartialOrd<BStackSlice<'a, A>> for BStackSliceWriter<'a, A> {
    fn partial_cmp(&self, other: &BStackSlice<'a, A>) -> Option<std::cmp::Ordering> {
        Some(self.slice().cmp(other))
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
