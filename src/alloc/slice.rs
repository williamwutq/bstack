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
    #[inline]
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
    #[inline]
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

    /// Returns `true` if this range and `other` share at least one byte.
    ///
    /// A zero-length range never overlaps anything, including another
    /// zero-length range at the same offset.
    #[inline]
    pub fn overlaps(&self, other: &Self) -> bool {
        !self.is_empty()
            && !other.is_empty()
            && self.offset < other.end()
            && other.offset < self.end()
    }

    /// Returns `true` if this range and `other` touch end-to-end with no gap
    /// and no overlap: `self.end() == other.start()` or `other.end() ==
    /// self.start()`.
    #[inline]
    pub fn adjacent_to(&self, other: &Self) -> bool {
        self.end() == other.start() || other.end() == self.start()
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

    /// Return the raw coordinate pair as a [`BStackRange`].
    #[inline]
    pub fn as_range(&self) -> BStackRange {
        self.range
    }

    /// Returns `true` if this slice and `other` share at least one byte.
    ///
    /// Delegates to [`BStackRange::overlaps`] on the underlying coordinates;
    /// does not check that both slices are backed by the same [`BStack`].
    #[inline]
    pub fn overlaps(&self, other: &Self) -> bool {
        self.range.overlaps(&other.range)
    }

    /// Returns `true` if this slice and `other` touch end-to-end with no gap
    /// and no overlap.
    ///
    /// Delegates to [`BStackRange::adjacent_to`] on the underlying
    /// coordinates; does not check that both slices are backed by the same
    /// [`BStack`].
    #[inline]
    pub fn adjacent_to(&self, other: &Self) -> bool {
        self.range.adjacent_to(&other.range)
    }

    /// Serialize the coordinate pair to a 16-byte array: `offset` (8 bytes LE) then `len` (8 bytes LE).
    ///
    /// Delegates to [`BStackRange::to_bytes`]. The result can be stored on disk
    /// and later reconstructed with [`from_bytes`](Self::from_bytes).
    #[inline]
    pub fn to_bytes(&self) -> [u8; 16] {
        self.range.to_bytes()
    }

    /// Deserialize a `BStackSlice` from a 16-byte array produced by [`to_bytes`](Self::to_bytes).
    ///
    /// # Safety
    ///
    /// `[offset, offset + len)` should lie within the current payload of `stack`
    /// for I/O to succeed.
    #[inline]
    pub unsafe fn from_bytes(stack: &'a BStack, bytes: [u8; 16]) -> Self {
        Self {
            stack,
            range: BStackRange::from_bytes(bytes),
        }
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

    /// Split into two sub-views at `mid`, relative to this slice's start.
    ///
    /// Equivalent to `(self.subslice(0, mid), self.subslice(mid, self.len()))`,
    /// following `std` slice naming.
    ///
    /// # Panics
    ///
    /// Panics if `mid > self.len()`.
    #[inline]
    pub fn split_at(&self, mid: u64) -> (BStackSlice<'a>, BStackSlice<'a>) {
        assert!(mid <= self.len(), "split_at: mid must be <= slice length");
        (self.subslice(0, mid), self.subslice(mid, self.len()))
    }

    /// Split into two independent sub-views at `mid`, relative to this slice's
    /// start.
    ///
    /// The returned slices are independent — like [`subslice`](Self::subslice),
    /// they carry the original `&'a BStack` lifetime rather than borrowing from
    /// `self`.
    ///
    /// # Panics
    ///
    /// Panics if `mid > self.len()`.
    #[inline]
    pub fn split_at_mut(&mut self, mid: u64) -> (BStackSlice<'a>, BStackSlice<'a>) {
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
    pub fn head(&self, n: u64) -> BStackSlice<'a> {
        let n = n.min(self.len());
        self.subslice(0, n)
    }

    /// Return a sub-view of the last `n` bytes.
    ///
    /// The returned slice has length `min(n, self.len())`.
    #[inline]
    pub fn tail(&self, n: u64) -> BStackSlice<'a> {
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
        self.stack.get_into(self.start() + index, &mut buf)?;
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

    /// Fill the entire slice with `value`.
    ///
    /// A single crash-atomic [`BStack::repeat`] call.
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    #[inline]
    pub fn fill(&mut self, value: u8) -> io::Result<()> {
        self.stack.repeat(self.start(), [value], self.len())
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
    pub fn copy_from_slice(&mut self, src: &[u8]) -> io::Result<()> {
        assert_eq!(
            src.len() as u64,
            self.len(),
            "copy_from_slice: length mismatch"
        );
        self.stack.set(self.start(), src)
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
    pub fn copy_from_bstack_slice(&mut self, src: &BStackSlice<'_>) -> io::Result<()> {
        assert_eq!(
            src.len(),
            self.len(),
            "copy_from_bstack_slice: length mismatch"
        );
        if !std::ptr::eq(src.stack(), self.stack) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "BStackSlice::copy_from_bstack_slice: source belongs to a different BStack",
            ));
        }
        if self.is_empty() {
            return Ok(());
        }
        self.stack.copy(src.start(), self.start(), self.len())
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
        self.stack
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
    pub fn swap(&mut self, other: &mut BStackSlice<'_>) -> io::Result<()> {
        assert_eq!(self.len(), other.len(), "swap: length mismatch");
        if !std::ptr::eq(self.stack, other.stack()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "BStackSlice::swap: slices belong to different BStacks",
            ));
        }
        if self.is_empty() || self.start() == other.start() {
            return Ok(());
        }
        self.stack
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
        self.stack
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
    pub fn rotate_left(&mut self, mid: u64) -> io::Result<()> {
        assert!(
            mid <= self.len(),
            "rotate_left: mid must be <= slice length"
        );
        self.stack.process(self.start(), self.end(), |buf| {
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
    pub fn rotate_right(&mut self, k: u64) -> io::Result<()> {
        assert!(k <= self.len(), "rotate_right: k must be <= slice length");
        self.stack
            .process(self.start(), self.end(), |buf| buf.rotate_right(k as usize))
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
    #[inline]
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

impl<'a> PartialEq<BStackRange> for BStackSlice<'a> {
    #[inline]
    fn eq(&self, other: &BStackRange) -> bool {
        self.range == *other
    }
}

impl<'a> PartialEq<BStackSlice<'a>> for BStackRange {
    #[inline]
    fn eq(&self, other: &BStackSlice<'a>) -> bool {
        *self == other.range
    }
}

impl<'a> PartialOrd<BStackRange> for BStackSlice<'a> {
    #[inline]
    fn partial_cmp(&self, other: &BStackRange) -> Option<std::cmp::Ordering> {
        Some(self.range.cmp(other))
    }
}

impl<'a> PartialOrd<BStackSlice<'a>> for BStackRange {
    #[inline]
    fn partial_cmp(&self, other: &BStackSlice<'a>) -> Option<std::cmp::Ordering> {
        Some(self.cmp(&other.range))
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
/// # Serialization
///
/// [`to_bytes`](Self::to_bytes) encodes the coordinate pair as a 16-byte
/// little-endian array for on-disk storage; [`from_bytes`](Self::from_bytes)
/// reconstructs the handle from those bytes. [`as_range`](Self::as_range)
/// extracts the raw [`BStackRange`] for passing to code that should not hold
/// an ownership handle.
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

    /// Return the raw coordinate pair as a [`BStackRange`].
    #[inline]
    pub fn as_range(&self) -> BStackRange {
        self.range
    }

    /// Serialize the coordinate pair to a 16-byte array: `offset` (8 bytes LE) then `len` (8 bytes LE).
    ///
    /// Delegates to [`BStackRange::to_bytes`]. The result can be stored on disk
    /// and later reconstructed with [`from_bytes`](Self::from_bytes).
    #[inline]
    pub fn to_bytes(&self) -> [u8; 16] {
        self.range.to_bytes()
    }

    /// Deserialize an owned handle from a 16-byte array produced by [`to_bytes`](Self::to_bytes).
    ///
    /// # Safety
    ///
    /// The decoded `(offset, len)` must describe an allocation that was returned
    /// by `allocator.alloc` or a prior `allocator.realloc` and has not yet been
    /// freed. Passing a stale, forged, or sub-slice coordinate and then passing
    /// the handle to `realloc` or `dealloc` may silently corrupt allocator metadata.
    #[inline]
    pub unsafe fn from_bytes(allocator: &'a A, bytes: [u8; 16]) -> Self {
        Self {
            allocator,
            range: BStackRange::from_bytes(bytes),
        }
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

    /// Return a sub-view of the first `n` bytes.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice)
    /// and delegates to [`BStackSlice::head`].
    #[inline]
    pub fn head<'s>(&'s self, n: u64) -> BStackSlice<'s> {
        self.as_slice().head(n)
    }

    /// Return a sub-view of the last `n` bytes.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice)
    /// and delegates to [`BStackSlice::tail`].
    #[inline]
    pub fn tail<'s>(&'s self, n: u64) -> BStackSlice<'s> {
        self.as_slice().tail(n)
    }

    /// Split into two sub-views at `mid`.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice)
    /// and delegates to [`BStackSlice::split_at`].
    #[inline]
    pub fn split_at<'s>(&'s self, mid: u64) -> (BStackSlice<'s>, BStackSlice<'s>) {
        self.as_slice().split_at(mid)
    }

    /// Split into two independent sub-views at `mid`.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice_mut`](Self::as_slice_mut)
    /// and delegates to [`BStackSlice::split_at_mut`].
    #[inline]
    pub fn split_at_mut<'s>(&'s mut self, mid: u64) -> (BStackSlice<'s>, BStackSlice<'s>) {
        let mut view = self.as_slice_mut();
        view.split_at_mut(mid)
    }

    /// Read the byte at `index`, or `None` if out of bounds.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice)
    /// and delegates to [`BStackSlice::get`].
    #[inline]
    pub fn get(&self, index: u64) -> io::Result<Option<u8>> {
        self.as_slice().get(index)
    }

    /// Returns `true` if the allocation contains `needle`.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice)
    /// and delegates to [`BStackSlice::contains`].
    #[inline]
    pub fn contains(&self, needle: u8) -> io::Result<bool> {
        self.as_slice().contains(needle)
    }

    /// Returns `true` if the allocation begins with `prefix`.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice)
    /// and delegates to [`BStackSlice::starts_with`].
    #[inline]
    pub fn starts_with(&self, prefix: &[u8]) -> io::Result<bool> {
        self.as_slice().starts_with(prefix)
    }

    /// Returns `true` if the allocation ends with `suffix`.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice)
    /// and delegates to [`BStackSlice::ends_with`].
    #[inline]
    pub fn ends_with(&self, suffix: &[u8]) -> io::Result<bool> {
        self.as_slice().ends_with(suffix)
    }

    /// Returns the index of the first occurrence of `needle`, or `None` if not
    /// found.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice)
    /// and delegates to [`BStackSlice::find`].
    #[inline]
    pub fn find(&self, needle: u8) -> io::Result<Option<u64>> {
        self.as_slice().find(needle)
    }

    /// Returns the index of the last occurrence of `needle`, or `None` if not
    /// found.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice)
    /// and delegates to [`BStackSlice::rfind`].
    #[inline]
    pub fn rfind(&self, needle: u8) -> io::Result<Option<u64>> {
        self.as_slice().rfind(needle)
    }

    /// Returns the index of the first byte satisfying `predicate`, or `None`.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice)
    /// and delegates to [`BStackSlice::position`].
    #[inline]
    pub fn position(&self, predicate: impl Fn(u8) -> bool) -> io::Result<Option<u64>> {
        self.as_slice().position(predicate)
    }

    /// Returns the index of the last byte satisfying `predicate`, or `None`.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice)
    /// and delegates to [`BStackSlice::rposition`].
    #[inline]
    pub fn rposition(&self, predicate: impl Fn(u8) -> bool) -> io::Result<Option<u64>> {
        self.as_slice().rposition(predicate)
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

    /// Fill the entire allocation with `value`.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice_mut`](Self::as_slice_mut)
    /// and delegates to [`BStackSlice::fill`].
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    #[inline]
    pub fn fill(&mut self, value: u8) -> io::Result<()> {
        self.as_slice_mut().fill(value)
    }

    /// Fill the allocation by calling `f` once per byte.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice_mut`](Self::as_slice_mut)
    /// and delegates to [`BStackSlice::fill_with`].
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    #[inline]
    pub fn fill_with(&mut self, f: impl FnMut() -> u8) -> io::Result<()> {
        self.as_slice_mut().fill_with(f)
    }

    /// Copy `src` into this allocation.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice_mut`](Self::as_slice_mut)
    /// and delegates to [`BStackSlice::copy_from_slice`].
    ///
    /// Requires the `set` feature.
    ///
    /// # Panics
    ///
    /// Panics if `src.len() != self.len()`.
    #[cfg(feature = "set")]
    #[inline]
    pub fn copy_from_slice(&mut self, src: &[u8]) -> io::Result<()> {
        self.as_slice_mut().copy_from_slice(src)
    }

    /// Copy the contents of `src` into this allocation.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice_mut`](Self::as_slice_mut)
    /// and delegates to [`BStackSlice::copy_from_bstack_slice`].
    ///
    /// Requires the `set` and `atomic` features.
    ///
    /// # Panics
    ///
    /// Panics if `src.len() != self.len()`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[inline]
    pub fn copy_from_bstack_slice(&mut self, src: &BStackSlice<'_>) -> io::Result<()> {
        self.as_slice_mut().copy_from_bstack_slice(src)
    }

    /// Copy `src_range` (relative to this allocation) to `dest`, within this
    /// allocation.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice_mut`](Self::as_slice_mut)
    /// and delegates to [`BStackSlice::copy_within`].
    ///
    /// Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[inline]
    pub fn copy_within(&mut self, src_range: Range<u64>, dest: u64) -> io::Result<()> {
        self.as_slice_mut().copy_within(src_range, dest)
    }

    /// Swap the contents of this allocation with `other`.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice_mut`](Self::as_slice_mut)
    /// and delegates to [`BStackSlice::swap`].
    ///
    /// Requires the `set` and `atomic` features.
    ///
    /// # Panics
    ///
    /// Panics if `self.len() != other.len()`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[inline]
    pub fn swap(&mut self, other: &mut BStackSlice<'_>) -> io::Result<()> {
        self.as_slice_mut().swap(other)
    }

    /// Reverse the byte order of this allocation in place.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice_mut`](Self::as_slice_mut)
    /// and delegates to [`BStackSlice::reverse`].
    ///
    /// Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[inline]
    pub fn reverse(&mut self) -> io::Result<()> {
        self.as_slice_mut().reverse()
    }

    /// Rotate the allocation in place such that the bytes at `[mid, len)` move
    /// to the front.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice_mut`](Self::as_slice_mut)
    /// and delegates to [`BStackSlice::rotate_left`].
    ///
    /// Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[inline]
    pub fn rotate_left(&mut self, mid: u64) -> io::Result<()> {
        self.as_slice_mut().rotate_left(mid)
    }

    /// Rotate the allocation in place such that the last `k` bytes move to the
    /// front.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice_mut`](Self::as_slice_mut)
    /// and delegates to [`BStackSlice::rotate_right`].
    ///
    /// Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[inline]
    pub fn rotate_right(&mut self, k: u64) -> io::Result<()> {
        self.as_slice_mut().rotate_right(k)
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

impl<'a, A: BStackAllocator> PartialEq<BStackSlice<'a>> for BStackOwnedSlice<'a, A> {
    #[inline]
    fn eq(&self, other: &BStackSlice<'a>) -> bool {
        self.range == other.range
    }
}

impl<'a, A: BStackAllocator> PartialEq<BStackOwnedSlice<'a, A>> for BStackSlice<'a> {
    #[inline]
    fn eq(&self, other: &BStackOwnedSlice<'a, A>) -> bool {
        self.range == other.range
    }
}

impl<'a, A: BStackAllocator> PartialEq<BStackRange> for BStackOwnedSlice<'a, A> {
    #[inline]
    fn eq(&self, other: &BStackRange) -> bool {
        self.range == *other
    }
}

impl<'a, A: BStackAllocator> PartialEq<BStackOwnedSlice<'a, A>> for BStackRange {
    #[inline]
    fn eq(&self, other: &BStackOwnedSlice<'a, A>) -> bool {
        *self == other.range
    }
}

impl<'a, A: BStackAllocator> PartialOrd<BStackSlice<'a>> for BStackOwnedSlice<'a, A> {
    #[inline]
    fn partial_cmp(&self, other: &BStackSlice<'a>) -> Option<std::cmp::Ordering> {
        Some(self.range.cmp(&other.range))
    }
}

impl<'a, A: BStackAllocator> PartialOrd<BStackOwnedSlice<'a, A>> for BStackSlice<'a> {
    #[inline]
    fn partial_cmp(&self, other: &BStackOwnedSlice<'a, A>) -> Option<std::cmp::Ordering> {
        Some(self.range.cmp(&other.range))
    }
}

impl<'a, A: BStackAllocator> PartialOrd<BStackRange> for BStackOwnedSlice<'a, A> {
    #[inline]
    fn partial_cmp(&self, other: &BStackRange) -> Option<std::cmp::Ordering> {
        Some(self.range.cmp(other))
    }
}

impl<'a, A: BStackAllocator> PartialOrd<BStackOwnedSlice<'a, A>> for BStackRange {
    #[inline]
    fn partial_cmp(&self, other: &BStackOwnedSlice<'a, A>) -> Option<std::cmp::Ordering> {
        Some(self.cmp(&other.range))
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
    #[inline]
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

    #[inline]
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
