use super::BStackAllocator;
#[cfg(all(feature = "set", feature = "atomic"))]
use super::BStackOwnedSliceAllocator;
use crate::BStack;
use std::borrow::Borrow;
#[cfg(feature = "expensive-slice-access-control")]
use crate::{BStackAccess, BStackAccessAuthorities, BStackAuthority};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io;
use std::ops::{Deref, Range};

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
    #[must_use]
    pub unsafe fn from_raw_parts(offset: u64, len: u64) -> Self {
        Self { offset, len }
    }

    /// Construct a `BStackRange` from raw offset and length, checking
    /// for overflow of `offset + len`.
    ///
    /// Silently caps `len` to avoid overflow of `offset + len`.
    #[inline]
    #[must_use]
    pub fn new(offset: u64, len: u64) -> Self {
        // Cap len
        Self {
            offset,
            len: len.min(u64::MAX - offset),
        }
    }

    /// Construct a zero-length range anchored at offset 0.
    #[inline]
    #[must_use]
    pub fn empty() -> Self {
        Self { offset: 0, len: 0 }
    }

    /// The inclusive start offset within the [`BStack`] payload.
    #[inline]
    #[must_use]
    pub fn start(&self) -> u64 {
        self.offset
    }

    /// The exclusive end offset (`start + len`).
    #[inline]
    #[must_use]
    pub fn end(&self) -> u64 {
        self.offset + self.len
    }

    /// Length of the region in bytes.
    #[inline]
    #[must_use]
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Returns `true` if the region spans zero bytes.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns `true` if this range and `other` share at least one byte.
    ///
    /// A zero-length range never overlaps anything, including another
    /// zero-length range at the same offset.
    #[inline]
    #[must_use]
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
    #[must_use]
    pub fn adjacent_to(&self, other: &Self) -> bool {
        self.end() == other.start() || other.end() == self.start()
    }

    /// Merge this range with `other` into a single range covering both.
    ///
    /// Succeeds if the ranges [`overlaps`](Self::overlaps), or if either is
    /// empty — an empty range acts as an identity element, so merging with
    /// one returns the other range unchanged. Returns `None` if both ranges
    /// are non-empty and disjoint (see [`merge_adjacent`](Self::merge_adjacent)
    /// for the touching-but-not-overlapping case).
    #[inline]
    #[must_use]
    pub fn merge(&self, other: &Self) -> Option<Self> {
        if self.is_empty() {
            return Some(*other);
        }
        if other.is_empty() {
            return Some(*self);
        }
        if !self.overlaps(other) {
            return None;
        }
        let start = self.offset.min(other.offset);
        let end = self.end().max(other.end());
        Some(Self::new(start, end - start))
    }

    /// Merge this range with `other` into a single range covering both,
    /// requiring them to be [`adjacent_to`](Self::adjacent_to) each other.
    ///
    /// Unlike [`merge`](Self::merge), an empty range never merges here: both
    /// ranges must be non-empty, in addition to touching end-to-end with no
    /// gap.
    #[inline]
    #[must_use]
    pub fn merge_adjacent(&self, other: &Self) -> Option<Self> {
        if self.is_empty() || other.is_empty() || !self.adjacent_to(other) {
            return None;
        }
        let start = self.offset.min(other.offset);
        let end = self.end().max(other.end());
        Some(Self::new(start, end - start))
    }

    /// Returns the range as `start..end`.
    #[inline]
    #[must_use]
    pub fn range(&self) -> Range<u64> {
        self.offset..self.offset + self.len
    }

    /// Serialize to a 16-byte array: `offset` (8 bytes LE) then `len` (8 bytes LE).
    #[inline]
    #[must_use]
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
    #[must_use]
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

/// The inverse of `From<BStackRange> for [u8; 16]`, via
/// [`from_bytes`](BStackRange::from_bytes) — `len` is silently capped so that
/// `offset + len` cannot overflow.
impl From<[u8; 16]> for BStackRange {
    #[inline]
    fn from(bytes: [u8; 16]) -> Self {
        BStackRange::from_bytes(bytes)
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

/// The zero-length range at offset 0 — the same sentinel [`empty`](BStackRange::empty)
/// returns, for use before a real allocation is available.
impl Default for BStackRange {
    #[inline]
    fn default() -> Self {
        Self::empty()
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

/// The half-open byte range, as `start..end` (e.g. `0..64`).
impl fmt::Display for BStackRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}..{}", self.start(), self.end())
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
    /// Authority this slice's I/O presents to the stack's access-control table.
    /// `NONE` unless granted via [`authorize`](BStackSlice::authorize); inherited
    /// by derived slices.
    #[cfg(feature = "expensive-slice-access-control")]
    auth: BStackAccessAuthorities,
}

impl<'a> Clone for BStackSlice<'a> {
    #[inline]
    fn clone(&self) -> Self {
        BStackSlice {
            stack: self.stack,
            range: self.range,
            #[cfg(feature = "expensive-slice-access-control")]
            auth: self.auth,
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

/// The region's half-open byte range within the payload, as `start..end`.
impl<'a> fmt::Display for BStackSlice<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}..{}", self.start(), self.end())
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
    #[must_use]
    pub unsafe fn from_raw_parts(stack: &'a BStack, offset: u64, len: u64) -> Self {
        Self {
            stack,
            range: unsafe { BStackRange::from_raw_parts(offset, len) },
            #[cfg(feature = "expensive-slice-access-control")]
            auth: BStackAccessAuthorities::NONE,
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
    #[must_use]
    pub unsafe fn from_raw_range(stack: &'a BStack, range: BStackRange) -> Self {
        Self {
            stack,
            range,
            #[cfg(feature = "expensive-slice-access-control")]
            auth: BStackAccessAuthorities::NONE,
        }
    }

    /// Construct a zero-length slice anchored at offset 0.
    ///
    /// All I/O on an empty slice is a no-op or returns an empty result.
    #[inline]
    #[must_use]
    pub fn empty(stack: &'a BStack) -> Self {
        Self {
            stack,
            range: BStackRange::empty(),
            #[cfg(feature = "expensive-slice-access-control")]
            auth: BStackAccessAuthorities::NONE,
        }
    }

    /// Start offset of this slice within the payload.
    #[inline]
    #[must_use]
    pub fn start(&self) -> u64 {
        self.range.start()
    }

    /// Exclusive end offset (`start + len`).
    #[inline]
    #[must_use]
    pub fn end(&self) -> u64 {
        self.range.end()
    }

    /// Length of the slice in bytes.
    #[inline]
    #[must_use]
    pub fn len(&self) -> u64 {
        self.range.len()
    }

    /// Returns `true` if the slice spans zero bytes.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }

    /// Half-open byte range `start..end` of this slice.
    #[inline]
    #[must_use]
    pub fn range(&self) -> Range<u64> {
        self.range.range()
    }

    /// Return the raw coordinate pair as a [`BStackRange`].
    #[inline]
    #[must_use]
    pub fn as_range(&self) -> BStackRange {
        self.range
    }

    /// Returns `true` if this slice and `other` share at least one byte.
    ///
    /// Delegates to [`BStackRange::overlaps`] on the underlying coordinates;
    /// does not check that both slices are backed by the same [`BStack`].
    #[inline]
    #[must_use]
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
    #[must_use]
    pub fn adjacent_to(&self, other: &Self) -> bool {
        self.range.adjacent_to(&other.range)
    }

    /// Merge this slice with `other` into a single slice covering both.
    ///
    /// Delegates to [`BStackRange::merge`] on the underlying coordinates.
    /// Returns `None` if the ranges are non-empty and disjoint, if `self` and
    /// `other` are backed by different [`BStack`]s, or (with the
    /// `expensive-slice-access-control` feature) if they carry different
    /// authorities.
    #[inline]
    #[must_use]
    pub fn merge(&self, other: &Self) -> Option<Self> {
        if !std::ptr::eq(self.stack, other.stack) {
            return None;
        }
        // Merging slices carrying different authorities would silently widen or
        // narrow the access one of them was granted; refuse it.
        #[cfg(feature = "expensive-slice-access-control")]
        if self.auth != other.auth {
            return None;
        }
        self.range.merge(&other.range).map(|range| Self {
            stack: self.stack,
            range,
            #[cfg(feature = "expensive-slice-access-control")]
            auth: self.auth,
        })
    }

    /// Merge this slice with `other` into a single slice covering both,
    /// requiring them to be adjacent and both non-empty.
    ///
    /// Delegates to [`BStackRange::merge_adjacent`] on the underlying
    /// coordinates. Returns `None` if the slices are not adjacent, either is
    /// empty, `self` and `other` are backed by different [`BStack`]s, or (with
    /// the `expensive-slice-access-control` feature) they carry different
    /// authorities.
    #[inline]
    #[must_use]
    pub fn merge_adjacent(&self, other: &Self) -> Option<Self> {
        if !std::ptr::eq(self.stack, other.stack) {
            return None;
        }
        #[cfg(feature = "expensive-slice-access-control")]
        if self.auth != other.auth {
            return None;
        }
        self.range.merge_adjacent(&other.range).map(|range| Self {
            stack: self.stack,
            range,
            #[cfg(feature = "expensive-slice-access-control")]
            auth: self.auth,
        })
    }

    /// Serialize the coordinate pair to a 16-byte array: `offset` (8 bytes LE) then `len` (8 bytes LE).
    ///
    /// Delegates to [`BStackRange::to_bytes`]. The result can be stored on disk
    /// and later reconstructed with [`from_bytes`](Self::from_bytes).
    #[inline]
    #[must_use]
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
    #[must_use]
    pub unsafe fn from_bytes(stack: &'a BStack, bytes: [u8; 16]) -> Self {
        Self {
            stack,
            range: BStackRange::from_bytes(bytes),
            #[cfg(feature = "expensive-slice-access-control")]
            auth: BStackAccessAuthorities::NONE,
        }
    }

    /// Return the underlying [`BStack`].
    #[inline]
    #[must_use]
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
    #[must_use]
    #[track_caller]
    pub fn subslice(&self, start: u64, end: u64) -> BStackSlice<'a> {
        self.subslice_range(start..end)
    }

    /// Create a sub-view for `range` relative to this slice's start.
    ///
    /// # Panics
    ///
    /// Panics if `range.start > range.end` or `range.end > self.len()`.
    #[must_use]
    #[track_caller]
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
            #[cfg(feature = "expensive-slice-access-control")]
            auth: self.auth,
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
    #[must_use]
    #[track_caller]
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
    #[must_use]
    #[track_caller]
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
    #[must_use]
    pub fn head(&self, n: u64) -> BStackSlice<'a> {
        let n = n.min(self.len());
        self.subslice(0, n)
    }

    /// Return a sub-view of the last `n` bytes.
    ///
    /// The returned slice has length `min(n, self.len())`.
    #[inline]
    #[must_use]
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
        self.s_get_into(self.start() + index, &mut buf)?;
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
        self.s_get(self.start(), self.end())
    }

    /// Read bytes into `buf`, up to `min(buf.len(), self.len())` bytes.
    #[inline]
    pub fn read_into(&self, buf: &mut [u8]) -> io::Result<()> {
        let n = (buf.len() as u64).min(self.len()) as usize;
        self.s_get_into(self.start(), &mut buf[..n])
    }

    /// Read `[start, end)` relative to this slice into a new `Vec<u8>`.
    pub fn read_range(&self, start: u64, end: u64) -> io::Result<Vec<u8>> {
        if end > self.len() {
            return Err(io_error!(
                InvalidInput,
                "range [{start}, {end}) exceeds slice length {}",
                self.len()
            ));
        }
        self.s_get(self.start() + start, self.start() + end)
    }

    /// Read `[start, start + buf.len())` relative to this slice into `buf`.
    pub fn read_range_into(&self, start: u64, buf: &mut [u8]) -> io::Result<()> {
        let end_rel = start + buf.len() as u64;
        if end_rel > self.len() {
            return Err(io_error!(
                InvalidInput,
                "range [{start}, {end_rel}) exceeds slice length {}",
                self.len()
            ));
        }
        self.s_get_into(self.start() + start, buf)
    }

    /// Overwrite the beginning of this slice with `data` (up to `self.len()` bytes).
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    pub fn write(&mut self, data: impl AsRef<[u8]>) -> io::Result<()> {
        let data = data.as_ref();
        let n = (data.len() as u64).min(self.len()) as usize;
        self.s_set(self.start(), &data[..n])
    }

    /// Overwrite `[start, start + data.len())` relative to this slice.
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    pub fn write_range(&mut self, start: u64, data: impl AsRef<[u8]>) -> io::Result<()> {
        let data = data.as_ref();
        let end_rel = start + data.len() as u64;
        if end_rel > self.len() {
            return Err(io_error!(
                InvalidInput,
                "range [{start}, {end_rel}) exceeds slice length {}",
                self.len()
            ));
        }
        self.s_set(self.start() + start, data)
    }

    /// Arm this slice's range with `mode` in the stack's access-control table.
    ///
    /// Crate-internal: the public protection entry is
    /// [`BStackOwnedSlice::protect`], which forwards here. The range is this
    /// slice's own `[start, end)` — a tokenless caller may only tighten a range
    /// currently at [`All`](BStackAccess::All).
    ///
    /// Requires the `expensive-slice-access-control` feature.
    #[cfg(feature = "expensive-slice-access-control")]
    #[inline]
    pub(crate) fn protect(&self, mode: BStackAccess) -> io::Result<()> {
        self.stack.protect(self.start(), self.len(), mode)
    }

    /// [`protect`](Self::protect) presenting an access token. Crate-internal; the
    /// public entry is [`BStackOwnedSlice::protect_as`].
    ///
    /// Requires the `expensive-slice-access-control` feature.
    #[cfg(feature = "expensive-slice-access-control")]
    #[inline]
    pub(crate) fn protect_as(
        &self,
        auth: impl BStackAuthority,
        mode: BStackAccess,
    ) -> io::Result<()> {
        self.stack.protect_as(auth, self.start(), self.len(), mode)
    }

    /// Grant this slice the authority carried by `auth`, so its subsequent I/O
    /// (and any view derived from it) may reach a [`Prot`](BStackAccess::Prot)/
    /// [`Alloc`](BStackAccess::Alloc) region it was authorized for. A token minted
    /// from a different stack grants nothing.
    ///
    /// Requires the `expensive-slice-access-control` feature.
    #[cfg(feature = "expensive-slice-access-control")]
    #[inline]
    pub fn authorize(&mut self, auth: impl BStackAuthority) {
        self.auth = auth.authorities_for(self.stack);
    }

    // Dispatch helpers: route slice I/O through the stack's token-carrying
    // entry points with this slice's stored authority, or the plain tokenless
    // ones without the access-control feature. Offsets are absolute.
    #[cfg(feature = "set")]
    #[inline]
    fn s_set(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        #[cfg(feature = "expensive-slice-access-control")]
        {
            self.stack.set_as(self.auth, offset, data)
        }
        #[cfg(not(feature = "expensive-slice-access-control"))]
        {
            self.stack.set(offset, data)
        }
    }

    #[cfg(feature = "set")]
    #[inline]
    fn s_zero(&self, offset: u64, n: u64) -> io::Result<()> {
        #[cfg(feature = "expensive-slice-access-control")]
        {
            self.stack.zero_as(self.auth, offset, n)
        }
        #[cfg(not(feature = "expensive-slice-access-control"))]
        {
            self.stack.zero(offset, n)
        }
    }

    #[cfg(feature = "set")]
    #[inline]
    fn s_repeat(&self, offset: u64, pattern: impl AsRef<[u8]>, count: u64) -> io::Result<()> {
        #[cfg(feature = "expensive-slice-access-control")]
        {
            self.stack.repeat_as(self.auth, offset, pattern, count)
        }
        #[cfg(not(feature = "expensive-slice-access-control"))]
        {
            self.stack.repeat(offset, pattern, count)
        }
    }

    #[inline]
    fn s_get(&self, start: u64, end: u64) -> io::Result<Vec<u8>> {
        #[cfg(feature = "expensive-slice-access-control")]
        {
            self.stack.get_as(self.auth, start, end)
        }
        #[cfg(not(feature = "expensive-slice-access-control"))]
        {
            self.stack.get(start, end)
        }
    }

    #[inline]
    fn s_get_into(&self, start: u64, buf: &mut [u8]) -> io::Result<()> {
        #[cfg(feature = "expensive-slice-access-control")]
        {
            self.stack.get_into_as(self.auth, start, buf)
        }
        #[cfg(not(feature = "expensive-slice-access-control"))]
        {
            self.stack.get_into(start, buf)
        }
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[inline]
    fn s_copy(&self, from: u64, to: u64, n: u64) -> io::Result<()> {
        #[cfg(feature = "expensive-slice-access-control")]
        {
            self.stack.copy_as(self.auth, from, to, n)
        }
        #[cfg(not(feature = "expensive-slice-access-control"))]
        {
            self.stack.copy(from, to, n)
        }
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[inline]
    fn s_process<F: FnOnce(&mut [u8])>(&self, start: u64, end: u64, f: F) -> io::Result<()> {
        #[cfg(feature = "expensive-slice-access-control")]
        {
            self.stack.process_as(self.auth, start, end, f)
        }
        #[cfg(not(feature = "expensive-slice-access-control"))]
        {
            self.stack.process(start, end, f)
        }
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[inline]
    fn s_cross_exchange(&self, a: u64, b: u64, n: u64) -> io::Result<()> {
        #[cfg(feature = "expensive-slice-access-control")]
        {
            self.stack.cross_exchange_as(self.auth, a, b, n)
        }
        #[cfg(not(feature = "expensive-slice-access-control"))]
        {
            self.stack.cross_exchange(a, b, n)
        }
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[inline]
    fn s_eq_crds(
        &self,
        a_offset: u64,
        a_expected: impl AsRef<[u8]>,
        b_offset: u64,
        b_buf: impl AsRef<[u8]>,
    ) -> io::Result<Option<Vec<u8>>> {
        #[cfg(feature = "expensive-slice-access-control")]
        {
            self.stack
                .eq_crds_as(self.auth, a_offset, a_expected, b_offset, b_buf)
        }
        #[cfg(not(feature = "expensive-slice-access-control"))]
        {
            self.stack.eq_crds(a_offset, a_expected, b_offset, b_buf)
        }
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[inline]
    fn s_ne_crds(
        &self,
        a_offset: u64,
        a_expected: impl AsRef<[u8]>,
        b_offset: u64,
        b_buf: impl AsRef<[u8]>,
    ) -> io::Result<Option<Vec<u8>>> {
        #[cfg(feature = "expensive-slice-access-control")]
        {
            self.stack
                .ne_crds_as(self.auth, a_offset, a_expected, b_offset, b_buf)
        }
        #[cfg(not(feature = "expensive-slice-access-control"))]
        {
            self.stack.ne_crds(a_offset, a_expected, b_offset, b_buf)
        }
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[inline]
    fn s_masked_eq_crds(
        &self,
        a_offset: u64,
        mask: impl AsRef<[u8]>,
        a_expected: impl AsRef<[u8]>,
        b_offset: u64,
        b_buf: impl AsRef<[u8]>,
    ) -> io::Result<Option<Vec<u8>>> {
        #[cfg(feature = "expensive-slice-access-control")]
        {
            self.stack
                .masked_eq_crds_as(self.auth, a_offset, mask, a_expected, b_offset, b_buf)
        }
        #[cfg(not(feature = "expensive-slice-access-control"))]
        {
            self.stack
                .masked_eq_crds(a_offset, mask, a_expected, b_offset, b_buf)
        }
    }

    /// Zero out the entire slice.
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    #[inline]
    pub fn zero(&mut self) -> io::Result<()> {
        self.s_zero(self.start(), self.len())
    }

    /// Zero `[start, start + n)` within this slice.
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    pub fn zero_range(&mut self, start: u64, n: u64) -> io::Result<()> {
        let end_rel = start + n;
        if end_rel > self.len() {
            return Err(io_error!(
                InvalidInput,
                "range [{start}, {end_rel}) exceeds slice length {}",
                self.len()
            ));
        }
        self.s_zero(self.start() + start, n)
    }

    /// Fill the entire slice with `value`.
    ///
    /// A single crash-atomic [`BStack::repeat`] call.
    ///
    /// Requires the `set` feature.
    #[cfg(feature = "set")]
    #[inline]
    pub fn fill(&mut self, value: u8) -> io::Result<()> {
        self.s_repeat(self.start(), [value], self.len())
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
        self.s_set(self.start(), src)
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
    pub fn copy_from_bstack_slice(&mut self, src: &BStackSlice<'_>) -> io::Result<()> {
        assert_eq!(
            src.len(),
            self.len(),
            "copy_from_bstack_slice: length mismatch"
        );
        if !std::ptr::eq(src.stack(), self.stack) {
            return Err(io_error!(
                InvalidInput,
                "BStackSlice::copy_from_bstack_slice: source belongs to a different BStack"
            ));
        }
        if self.is_empty() {
            return Ok(());
        }
        self.s_copy(src.start(), self.start(), self.len())
    }

    /// Copy this view's contents into a fresh allocation from `allocator`.
    ///
    /// Allocates `self.len()` bytes via [`BStackAllocator::alloc`], then issues
    /// one crash-atomic
    /// [`copy_from_bstack_slice`](Self::copy_from_bstack_slice) — no bytes are
    /// read into process memory, and the source is left unchanged. The returned
    /// [`BStackOwnedSlice`] is tied to `allocator`'s borrow, independent of this
    /// view's own lifetime.
    ///
    /// Requires the `set` and `atomic` features.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `allocator` is backed by a
    /// different [`BStack`] than this view — the copy primitive cannot cross
    /// stacks — or any [`io::Error`] from the allocation or copy. If the copy
    /// fails after the allocation succeeds, the fresh region is freed on a
    /// best-effort basis before returning the copy error; if that free itself
    /// fails, the region is left allocated but unreferenced (reclaimable by the
    /// allocator's recovery), exactly as a crash between the two steps would
    /// leave it.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn to_owned_in<'b, A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &'b A,
    ) -> io::Result<BStackOwnedSlice<'b, A>> {
        let mut dest = allocator.alloc(self.len())?;
        if let Err(e) = dest.as_slice_mut().copy_from_bstack_slice(self) {
            let _ = allocator.dealloc(dest);
            return Err(e);
        }
        Ok(dest)
    }

    /// Like [`to_owned_in`](Self::to_owned_in), but skips the destination's
    /// zero-fill via [`alloc_uninit`](super::BStackUninitAllocator::alloc_uninit).
    ///
    /// The fresh region is fully overwritten by the copy, so the zero-fill
    /// `alloc` would perform is pure waste here. Unlike a bare `alloc_uninit`,
    /// the returned handle carries fully-defined data (a copy of this view) with
    /// no caller-side overwrite obligation; the only difference from
    /// [`to_owned_in`](Self::to_owned_in) is the elided fill.
    ///
    /// Requires the `set` and `atomic` features, and an allocator implementing
    /// [`BStackUninitAllocator`](super::BStackUninitAllocator).
    ///
    /// # Errors
    ///
    /// As [`to_owned_in`](Self::to_owned_in).
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn to_owned_uninit_in<'b, A>(&self, allocator: &'b A) -> io::Result<BStackOwnedSlice<'b, A>>
    where
        A: super::BStackUninitAllocator + BStackOwnedSliceAllocator,
    {
        let mut dest = allocator.alloc_uninit(self.len())?;
        if let Err(e) = dest.as_slice_mut().copy_from_bstack_slice(self) {
            let _ = allocator.dealloc(dest);
            return Err(e);
        }
        Ok(dest)
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
        self.s_copy(self.start() + src_range.start, self.start() + dest, n)
    }

    /// Overwrite this slice with `new_bytes` if `guard`'s current contents
    /// equal `expected`.
    ///
    /// One crash-atomic [`BStack::eq_crds`] call: `guard`'s bytes are read
    /// and compared to `expected`, and if they match, `self` is overwritten
    /// with `new_bytes` — all under the same write lock, so no other thread
    /// can observe the comparison and the write as separate steps. Returns
    /// the prior contents of `self` as `Ok(Some(_))` if the swap ran, or
    /// `Ok(None)` if the comparison failed, leaving `self` untouched.
    ///
    /// `guard` may be a view into the same or a different region of `self`'s
    /// [`BStack`], including `self` itself.
    ///
    /// Requires the `set` and `atomic` features.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `guard` and `self` are
    /// backed by different [`BStack`]s, if `expected.as_ref().len() !=
    /// guard.len()`, or if `new_bytes.as_ref().len() != self.len()`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn cas_on(
        &mut self,
        guard: &BStackSlice<'_>,
        expected: impl AsRef<[u8]>,
        new_bytes: impl AsRef<[u8]>,
    ) -> io::Result<Option<Vec<u8>>> {
        let expected = expected.as_ref();
        let new_bytes = new_bytes.as_ref();
        if self.stack != guard.stack() {
            return Err(io_error!(
                InvalidInput,
                "BStackSlice::cas_on: guard belongs to a different BStack"
            ));
        }
        if expected.len() as u64 != guard.len() {
            return Err(io_error!(
                InvalidInput,
                "BStackSlice::cas_on: expected length ({}) != guard length ({})",
                expected.len(),
                guard.len()
            ));
        }
        if new_bytes.len() as u64 != self.len() {
            return Err(io_error!(
                InvalidInput,
                "BStackSlice::cas_on: new_bytes length ({}) != self length ({})",
                new_bytes.len(),
                self.len()
            ));
        }
        self.s_eq_crds(guard.start(), expected, self.start(), new_bytes)
    }

    /// Overwrite this slice with `new_bytes` if `guard`'s current contents do
    /// **not** equal `expected`.
    ///
    /// Like [`cas_on`](Self::cas_on) but wraps [`BStack::ne_crds`]: the swap
    /// runs when the comparison fails rather than when it succeeds.
    ///
    /// Requires the `set` and `atomic` features.
    ///
    /// # Errors
    ///
    /// Same conditions as [`cas_on`](Self::cas_on).
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn cas_on_ne(
        &mut self,
        guard: &BStackSlice<'_>,
        expected: impl AsRef<[u8]>,
        new_bytes: impl AsRef<[u8]>,
    ) -> io::Result<Option<Vec<u8>>> {
        let expected = expected.as_ref();
        let new_bytes = new_bytes.as_ref();
        if self.stack != guard.stack() {
            return Err(io_error!(
                InvalidInput,
                "BStackSlice::cas_on_ne: guard belongs to a different BStack"
            ));
        }
        if expected.len() as u64 != guard.len() {
            return Err(io_error!(
                InvalidInput,
                "BStackSlice::cas_on_ne: expected length ({}) != guard length ({})",
                expected.len(),
                guard.len()
            ));
        }
        if new_bytes.len() as u64 != self.len() {
            return Err(io_error!(
                InvalidInput,
                "BStackSlice::cas_on_ne: new_bytes length ({}) != self length ({})",
                new_bytes.len(),
                self.len()
            ));
        }
        self.s_ne_crds(guard.start(), expected, self.start(), new_bytes)
    }

    /// Overwrite this slice with `new_bytes` if `guard`'s current contents
    /// equal `expected` under a bitwise `mask`.
    ///
    /// Like [`cas_on`](Self::cas_on) but wraps [`BStack::masked_eq_crds`]:
    /// the condition is `(guard[i] & mask[i]) == (expected[i] & mask[i])` for
    /// every byte `i`.
    ///
    /// Requires the `set` and `atomic` features.
    ///
    /// # Errors
    ///
    /// Same conditions as [`cas_on`](Self::cas_on), plus
    /// [`io::ErrorKind::InvalidInput`] if `mask.as_ref().len() !=
    /// expected.as_ref().len()` (checked by [`BStack::masked_eq_crds`]
    /// itself).
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn cas_on_masked(
        &mut self,
        guard: &BStackSlice<'_>,
        mask: impl AsRef<[u8]>,
        expected: impl AsRef<[u8]>,
        new_bytes: impl AsRef<[u8]>,
    ) -> io::Result<Option<Vec<u8>>> {
        let mask = mask.as_ref();
        let expected = expected.as_ref();
        let new_bytes = new_bytes.as_ref();
        if self.stack != guard.stack() {
            return Err(io_error!(
                InvalidInput,
                "BStackSlice::cas_on_masked: guard belongs to a different BStack"
            ));
        }
        if expected.len() as u64 != guard.len() {
            return Err(io_error!(
                InvalidInput,
                "BStackSlice::cas_on_masked: expected length ({}) != guard length ({})",
                expected.len(),
                guard.len()
            ));
        }
        if new_bytes.len() as u64 != self.len() {
            return Err(io_error!(
                InvalidInput,
                "BStackSlice::cas_on_masked: new_bytes length ({}) != self length ({})",
                new_bytes.len(),
                self.len()
            ));
        }
        self.s_masked_eq_crds(guard.start(), mask, expected, self.start(), new_bytes)
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
    pub fn swap(&mut self, other: &mut BStackSlice<'_>) -> io::Result<()> {
        assert_eq!(self.len(), other.len(), "swap: length mismatch");
        if !std::ptr::eq(self.stack, other.stack()) {
            return Err(io_error!(
                InvalidInput,
                "BStackSlice::swap: slices belong to different BStacks"
            ));
        }
        if self.is_empty() || self.start() == other.start() {
            return Ok(());
        }
        self.s_cross_exchange(self.start(), other.start(), self.len())
    }

    /// Run a length-preserving transform over this slice's bytes in place.
    ///
    /// One crash-atomic [`BStack::process`] call: the slice's bytes are
    /// read, handed to `f` for in-memory mutation, then written back, all
    /// under the same write lock. `f` must not change the buffer's length —
    /// this only rewrites `self`'s existing bytes, so no allocator
    /// interaction is needed. [`reverse`](Self::reverse),
    /// [`rotate_left`](Self::rotate_left), and
    /// [`rotate_right`](Self::rotate_right) are built on the same primitive.
    ///
    /// Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[inline]
    pub fn process<F: FnOnce(&mut [u8])>(&mut self, f: F) -> io::Result<()> {
        self.s_process(self.start(), self.end(), f)
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
        self.s_process(self.start(), self.end(), |buf| buf.reverse())
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
        self.s_process(self.start(), self.end(), |buf| {
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
        self.s_process(self.start(), self.end(), |buf| buf.rotate_right(k as usize))
    }

    /// Create a cursor-based reader positioned at the start of this slice.
    ///
    /// This clones the slice; the reader and the original slice are independent.
    #[inline]
    #[must_use]
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
    #[must_use]
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
    #[must_use]
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
    #[must_use]
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

/// Borrows the coordinate pair. `Eq`, `Ord` and `Hash` on `BStackSlice` are
/// the range's own, so a `HashMap`/`BTreeMap` keyed by slices can be probed
/// with a bare [`BStackRange`].
impl<'a> Borrow<BStackRange> for BStackSlice<'a> {
    #[inline]
    fn borrow(&self) -> &BStackRange {
        &self.range
    }
}

impl<'a> AsRef<BStackRange> for BStackSlice<'a> {
    #[inline]
    fn as_ref(&self) -> &BStackRange {
        &self.range
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
    /// Authority the views borrowed from this handle carry into the stack's
    /// access-control table. `NONE` until granted via
    /// [`authorize`](BStackOwnedSlice::authorize).
    #[cfg(feature = "expensive-slice-access-control")]
    auth: BStackAccessAuthorities,
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

/// The allocation's half-open byte range within the payload, as `start..end`.
impl<'a, A: BStackAllocator> fmt::Display for BStackOwnedSlice<'a, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}..{}", self.start(), self.end())
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
    #[must_use]
    pub unsafe fn from_raw_parts(allocator: &'a A, offset: u64, len: u64) -> Self {
        Self {
            allocator,
            range: unsafe { BStackRange::from_raw_parts(offset, len) },
            #[cfg(feature = "expensive-slice-access-control")]
            auth: BStackAccessAuthorities::NONE,
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
    #[must_use]
    pub unsafe fn from_raw_range(allocator: &'a A, range: BStackRange) -> Self {
        Self {
            allocator,
            range,
            #[cfg(feature = "expensive-slice-access-control")]
            auth: BStackAccessAuthorities::NONE,
        }
    }

    /// Construct an empty (zero-length) owned handle.
    ///
    /// Used as a sentinel. `dealloc` of an empty handle at offset 0 is a no-op
    /// in all library allocators.
    #[inline]
    #[must_use]
    pub fn empty(allocator: &'a A) -> Self {
        Self {
            allocator,
            range: BStackRange::empty(),
            #[cfg(feature = "expensive-slice-access-control")]
            auth: BStackAccessAuthorities::NONE,
        }
    }

    /// Start offset of the allocation within the payload.
    #[inline]
    #[must_use]
    pub fn start(&self) -> u64 {
        self.range.start()
    }

    /// Exclusive end offset (`start + len`).
    #[inline]
    #[must_use]
    pub fn end(&self) -> u64 {
        self.range.end()
    }

    /// Length of the allocation in bytes.
    #[inline]
    #[must_use]
    pub fn len(&self) -> u64 {
        self.range.len()
    }

    /// Returns `true` if the allocation spans zero bytes.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }

    /// Half-open byte range `start..end` of this allocation.
    #[inline]
    #[must_use]
    pub fn range(&self) -> Range<u64> {
        self.range.range()
    }

    /// Return the raw coordinate pair as a [`BStackRange`].
    #[inline]
    #[must_use]
    pub fn as_range(&self) -> BStackRange {
        self.range
    }

    /// Serialize the coordinate pair to a 16-byte array: `offset` (8 bytes LE) then `len` (8 bytes LE).
    ///
    /// Delegates to [`BStackRange::to_bytes`]. The result can be stored on disk
    /// and later reconstructed with [`from_bytes`](Self::from_bytes).
    #[inline]
    #[must_use]
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
    #[must_use]
    pub unsafe fn from_bytes(allocator: &'a A, bytes: [u8; 16]) -> Self {
        Self {
            allocator,
            range: BStackRange::from_bytes(bytes),
            #[cfg(feature = "expensive-slice-access-control")]
            auth: BStackAccessAuthorities::NONE,
        }
    }

    /// Return the allocator that owns this handle.
    #[inline]
    #[must_use]
    pub fn allocator(&self) -> &'a A {
        self.allocator
    }

    /// Returns `true` if this handle was issued by `allocator`.
    ///
    /// A handle records which allocator produced it, but the type system cannot
    /// enforce that it is only ever handed back to *that* instance — two
    /// allocators of the same type are the same type, so `a2.dealloc(h1)`
    /// type-checks. Allocators use this to reject foreign handles at run time;
    /// see [the crate-level lifetime model](crate#lifetime-model).
    #[inline]
    #[must_use]
    pub fn is_from(&self, allocator: &A) -> bool {
        std::ptr::eq(self.allocator, allocator)
    }

    /// Borrow this allocation as a shared [`BStackSlice`] for reads.
    ///
    /// The returned slice's lifetime is tied to `&self` — it cannot outlive
    /// this handle. This prevents a view from surviving `dealloc` or `realloc`,
    /// which both consume the handle by value.
    #[inline]
    #[must_use]
    pub fn as_slice<'s>(&'s self) -> BStackSlice<'s> {
        BStackSlice {
            stack: self.allocator.stack(),
            range: self.range,
            #[cfg(feature = "expensive-slice-access-control")]
            auth: self.auth,
        }
    }

    /// Borrow this allocation as an exclusive [`BStackSlice`] for reads and writes.
    ///
    /// The `&mut self` receiver makes the borrow exclusive: no other view of
    /// this allocation can be obtained while the returned slice is live. Within
    /// safe code this enforces single-writer access.
    #[inline]
    #[must_use]
    pub fn as_slice_mut<'s>(&'s mut self) -> BStackSlice<'s> {
        BStackSlice {
            stack: self.allocator.stack(),
            range: self.range,
            #[cfg(feature = "expensive-slice-access-control")]
            auth: self.auth,
        }
    }

    /// Arm this allocation's range with `mode` in the stack's access-control
    /// table.
    ///
    /// The public entry point for protection: an owned handle proves the range is
    /// genuinely this caller's allocation, rather than an arbitrary span. A
    /// tokenless caller may only tighten a range currently at
    /// [`All`](BStackAccess::All); see [`protect_as`](Self::protect_as) to present
    /// a capability token.
    ///
    /// Requires the `expensive-slice-access-control` feature.
    #[cfg(feature = "expensive-slice-access-control")]
    #[inline]
    pub fn protect(&self, mode: BStackAccess) -> io::Result<()> {
        self.as_slice().protect(mode)
    }

    /// [`protect`](Self::protect) presenting an access token — the token sibling
    /// for arming [`Prot`](BStackAccess::Prot)/[`Alloc`](BStackAccess::Alloc)
    /// ranges or re-moding a range this token governs.
    ///
    /// Requires the `expensive-slice-access-control` feature.
    #[cfg(feature = "expensive-slice-access-control")]
    #[inline]
    pub fn protect_as(&self, auth: impl BStackAuthority, mode: BStackAccess) -> io::Result<()> {
        self.as_slice().protect_as(auth, mode)
    }

    /// Grant this handle the authority carried by `auth`, so every view borrowed
    /// from it ([`as_slice`](Self::as_slice) / [`as_slice_mut`](Self::as_slice_mut))
    /// and their I/O may reach a range it was authorized for. A token minted from
    /// a different stack grants nothing.
    ///
    /// Requires the `expensive-slice-access-control` feature.
    #[cfg(feature = "expensive-slice-access-control")]
    #[inline]
    pub fn authorize(&mut self, auth: impl BStackAuthority) {
        self.auth = auth.authorities_for(self.allocator.stack());
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
    #[must_use]
    pub fn head<'s>(&'s self, n: u64) -> BStackSlice<'s> {
        self.as_slice().head(n)
    }

    /// Return a sub-view of the last `n` bytes.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice)
    /// and delegates to [`BStackSlice::tail`].
    #[inline]
    #[must_use]
    pub fn tail<'s>(&'s self, n: u64) -> BStackSlice<'s> {
        self.as_slice().tail(n)
    }

    /// Split into two sub-views at `mid`.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice)
    /// and delegates to [`BStackSlice::split_at`].
    #[inline]
    #[must_use]
    #[track_caller]
    pub fn split_at<'s>(&'s self, mid: u64) -> (BStackSlice<'s>, BStackSlice<'s>) {
        self.as_slice().split_at(mid)
    }

    /// Split into two independent sub-views at `mid`.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice_mut`](Self::as_slice_mut)
    /// and delegates to [`BStackSlice::split_at_mut`].
    #[inline]
    #[must_use]
    #[track_caller]
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
    #[track_caller]
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
    #[track_caller]
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
    #[track_caller]
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
    #[track_caller]
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
    #[track_caller]
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
    #[track_caller]
    pub fn rotate_right(&mut self, k: u64) -> io::Result<()> {
        self.as_slice_mut().rotate_right(k)
    }

    /// Create a cursor-based reader over this allocation.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice)
    /// and delegates to [`BStackSlice::reader`].
    #[inline]
    #[must_use]
    pub fn reader<'s>(&'s self) -> BStackSliceReader<'s> {
        self.as_slice().reader()
    }

    /// Create a cursor-based reader positioned at `offset` into this allocation.
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice)
    /// and delegates to [`BStackSlice::reader_at`].
    #[inline]
    #[must_use]
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
    #[must_use]
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
    #[must_use]
    pub fn writer_at<'s>(&'s mut self, offset: u64) -> BStackSliceWriter<'s> {
        self.as_slice_mut().writer_at(offset)
    }
}

#[cfg(all(feature = "set", feature = "atomic"))]
impl<'a, A: BStackOwnedSliceAllocator> BStackOwnedSlice<'a, A> {
    /// Copy this allocation's contents into a second, independent allocation
    /// from the same allocator.
    ///
    /// `BStackOwnedSlice` is deliberately non-`Clone` — duplicating it would
    /// silently issue disk I/O behind an operator expected to be free. This
    /// explicit, fallible method makes the copy visible, mirroring
    /// [`std::fs::File::try_clone`]. Equivalent to
    /// [`self.as_slice().to_owned_in(self.allocator())`](BStackSlice::to_owned_in);
    /// no allocator argument, as the handle already carries one.
    ///
    /// Requires the `set` and `atomic` features.
    ///
    /// # Errors
    ///
    /// Any [`io::Error`] from the allocation or copy. Cannot fail cross-stack —
    /// the clone reuses this handle's own allocator.
    pub fn try_clone(&self) -> io::Result<BStackOwnedSlice<'a, A>> {
        self.as_slice().to_owned_in(self.allocator())
    }
}

#[cfg(all(feature = "set", feature = "atomic"))]
impl<'a, A> BStackOwnedSlice<'a, A>
where
    A: super::BStackUninitAllocator + BStackOwnedSliceAllocator,
{
    /// Like [`try_clone`](Self::try_clone), but skips the destination's
    /// zero-fill via [`to_owned_uninit_in`](BStackSlice::to_owned_uninit_in).
    ///
    /// Requires the `set` and `atomic` features, and an allocator implementing
    /// [`BStackUninitAllocator`](super::BStackUninitAllocator).
    ///
    /// # Errors
    ///
    /// As [`try_clone`](Self::try_clone).
    pub fn try_clone_uninit(&self) -> io::Result<BStackOwnedSlice<'a, A>> {
        self.as_slice().to_owned_uninit_in(self.allocator())
    }
}

/// Compute the `(prepend, append)` deltas that narrow a `len`-byte allocation
/// to its `[start, end)` window for
/// [`realloc_inplace`](crate::alloc::BStackInPlaceResizeAllocator::realloc_inplace),
/// or `None` if a length does not fit `i64`. Assumes `start <= end <= len`; the
/// callers check bounds first so they can report an out-of-bounds range and an
/// overflow with distinct messages.
#[inline]
fn subslice_deltas(len: u64, start: u64, end: u64) -> Option<(i64, i64)> {
    let len_i = i64::try_from(len).ok()?;
    let start_i = i64::try_from(start).ok()?;
    let end_i = i64::try_from(end).ok()?;
    Some((-start_i, end_i - len_i))
}

/// In-place narrowing built on
/// [`BStackInPlaceResizeAllocator`](crate::alloc::BStackInPlaceResizeAllocator).
///
/// [`try_subslice_inplace`](Self::try_subslice_inplace) uses only the
/// allocator's in-place resize and never copies the payload, so it needs no
/// `set`/`atomic` feature and surfaces [`io::ErrorKind::Unsupported`] when the
/// allocator cannot honour the request in place. The copying variants
/// ([`try_subslice`](Self::try_subslice), [`try_join`](Self::try_join),
/// [`try_join_inplace`](Self::try_join_inplace)) live in the `set + atomic`
/// impl below.
impl<'a, A> BStackOwnedSlice<'a, A>
where
    A: crate::alloc::BStackInPlaceResizeAllocator + crate::alloc::BStackOwnedSliceAllocator,
{
    /// Narrow this allocation to its `[start, end)` byte range **in place**,
    /// without copying the retained bytes.
    ///
    /// One [`realloc_inplace`](crate::alloc::BStackInPlaceResizeAllocator::realloc_inplace)
    /// call trims the front by `start` and the back to `end` together. Whatever
    /// that returns — including [`io::ErrorKind::Unsupported`] when the allocator
    /// cannot do it in place — is propagated as a
    /// [`BStackSliceError`](crate::alloc::BStackSliceError), with the original
    /// handle carried in the error on any recoverable failure.
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::InvalidInput`] (carrying the untouched handle) if
    /// `start > end` or `end > self.len()`, or if a length does not fit `i64`.
    pub fn try_subslice_inplace(
        self,
        start: u64,
        end: u64,
    ) -> Result<Self, crate::alloc::BStackSliceError<'a, A>> {
        // Validate the window before any I/O; hand the handle back on a bad one.
        // Bounds and overflow are reported separately.
        let len = self.len();
        if start > end || end > len {
            return Err(crate::alloc::BStackSliceError::with_handle(
                io_error!(InvalidInput, "try_subslice_inplace: range out of bounds"),
                self,
            ));
        }
        let (prepend, append) = match subslice_deltas(len, start, end) {
            Some(d) => d,
            None => {
                return Err(crate::alloc::BStackSliceError::with_handle(
                    io_error!(InvalidInput, "try_subslice_inplace: length overflows i64"),
                    self,
                ));
            }
        };
        self.allocator()
            .realloc_inplace(self, prepend, append)
            .map_err(|e| crate::alloc::BStackSliceError {
                source: e.source,
                handle: e.handle,
            })
    }
}

/// Copying subslice/join built on
/// [`BStackInPlaceResizeAllocator`](crate::alloc::BStackInPlaceResizeAllocator).
///
/// `try_subslice`/`try_join` add an `alloc` + copy + `dealloc` fallback to the
/// in-place path, so they never surface [`io::ErrorKind::Unsupported`] — only
/// genuine I/O failure. `try_join_inplace` keeps the in-place-only contract but
/// still copies the *moved* side, so it too needs the `set` + `atomic` copy.
///
/// # Post-commit cleanup
///
/// The copying methods produce their result before releasing the consumed
/// input(s). If every step through the result succeeds but a final `dealloc` of
/// a consumed input then fails, the method still returns the finished result
/// `Ok(...)` and the unfreed input is leaked (reclaimable through the
/// allocator's crash-recovery). Failures *before* the result is committed
/// instead return the intact inputs in the error for the caller to retry or
/// free.
#[cfg(all(feature = "set", feature = "atomic"))]
impl<'a, A> BStackOwnedSlice<'a, A>
where
    A: crate::alloc::BStackInPlaceResizeAllocator + crate::alloc::BStackOwnedSliceAllocator,
{
    /// Copy `src` (a view within the same `BStack`) into `[at, at + src.len())`
    /// of this allocation in one crash-atomic write. Caller guarantees the
    /// destination region is within bounds.
    #[inline]
    fn copy_from_at(&mut self, at: u64, src: &BStackSlice<'_>) -> io::Result<()> {
        let mut dst = self.as_slice_mut().subslice(at, at + src.len());
        dst.copy_from_bstack_slice(src)
    }

    /// Narrow this allocation to its `[start, end)` byte range, in place when the
    /// allocator supports it and otherwise by allocating a fresh region, copying
    /// the range, and freeing the original.
    ///
    /// Unlike [`try_subslice_inplace`](Self::try_subslice_inplace) this never
    /// surfaces [`io::ErrorKind::Unsupported`].
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::InvalidInput`] if `start > end` or `end > self.len()`;
    /// any I/O error from the underlying operations.
    pub fn try_subslice(
        self,
        start: u64,
        end: u64,
    ) -> Result<Self, crate::alloc::BStackSliceError<'a, A>> {
        let len = self.len();
        if start > end || end > len {
            return Err(crate::alloc::BStackSliceError::with_handle(
                io_error!(InvalidInput, "try_subslice: range out of bounds"),
                self,
            ));
        }
        // Fast path. On any failure that returns the handle (Unsupported or a
        // clean I/O error), fall back to alloc + copy + dealloc. A lost handle
        // (None) cannot be retried, so propagate it.
        let original = match self.try_subslice_inplace(start, end) {
            Ok(h) => return Ok(h),
            Err(e) => match e.handle {
                Some(h) => h,
                None => return Err(e),
            },
        };

        let sub_len = end - start;
        let alloc = original.allocator();
        let mut dst = match alloc.alloc(sub_len) {
            Ok(d) => d,
            Err(source) => {
                return Err(crate::alloc::BStackSliceError::with_handle(
                    source, original,
                ));
            }
        };
        // Copy the [start, end) window of `original` — a read-only view, no
        // temporary handle needed.
        let src = original.as_slice().subslice(start, end);
        if let Err(source) = dst.copy_from_at(0, &src) {
            // Copy failed: `original` untouched, drop the fresh region.
            let _ = alloc.dealloc(dst);
            return Err(crate::alloc::BStackSliceError::with_handle(
                source, original,
            ));
        }
        // Result committed; free the original. A failed free leaks it (see the
        // impl-level "Post-commit cleanup" note) but the subslice still succeeds.
        let _ = alloc.dealloc(original);
        Ok(dst)
    }

    /// Concatenate `self` followed by `other` into a single allocation **in
    /// place**, without copying whichever side is extended.
    ///
    /// Tries to grow `self`'s tail to hold `other`; failing that, tries to grow
    /// `other`'s front to hold `self`. Only the *moved* side's bytes are copied;
    /// the extended side never moves. Fails with the inputs recovered when
    /// neither direction is supported.
    ///
    /// # Errors
    ///
    /// A [`BStackJoinError`](crate::alloc::BStackJoinError) whose `first`/`second`
    /// fields carry back whichever inputs survived; includes
    /// [`io::ErrorKind::Unsupported`] when no in-place direction works.
    pub fn try_join_inplace(
        self,
        other: Self,
    ) -> Result<Self, crate::alloc::BStackJoinError<'a, A>> {
        self.join_core(other, false, "try_join_inplace")
    }

    /// Concatenate `self` followed by `other`, in place when possible and
    /// otherwise by allocating a fresh combined region and copying both inputs.
    ///
    /// Never surfaces [`io::ErrorKind::Unsupported`].
    ///
    /// # Errors
    ///
    /// A [`BStackJoinError`](crate::alloc::BStackJoinError) on genuine I/O
    /// failure, carrying back whichever inputs survived.
    pub fn try_join(self, other: Self) -> Result<Self, crate::alloc::BStackJoinError<'a, A>> {
        self.join_core(other, true, "try_join")
    }

    /// Shared body of [`try_join_inplace`](Self::try_join_inplace) (`fallback =
    /// false`) and [`try_join`](Self::try_join) (`fallback = true`). `op` names
    /// the calling method for error messages.
    ///
    /// The in-place directions are attempted first regardless of `fallback`;
    /// `fallback` only decides what happens when *both* directions decline while
    /// both inputs remain intact — a fresh `alloc` + two copies + two frees, or
    /// an error carrying both inputs.
    ///
    /// On failure the survivors are returned in the error's `first`/`second`
    /// fields: both `Some` when neither region was touched; a single survivor
    /// (the joined result in `first`, or the one input that could not be freed)
    /// when one was consumed or merged; `None` for a region lost mid-mutation
    /// (recoverable only through the allocator's crash-recovery).
    fn join_core(
        self,
        other: Self,
        fallback: bool,
        op: &'static str,
    ) -> Result<Self, crate::alloc::BStackJoinError<'a, A>> {
        use crate::alloc::BStackJoinError;
        // Empty inputs: concatenation is just the other side.
        if self.is_empty() {
            return Ok(other);
        }
        if other.is_empty() {
            return Ok(self);
        }
        // `other` must belong to the same allocator as `self`: the join
        // allocates, copies, and frees through `self`'s allocator, so a foreign
        // `other` would corrupt it. Reject before any mutation. (`self` is its
        // own allocator's by construction.)
        if !other.is_from(self.allocator()) {
            return Err(BStackJoinError::both(
                io_error!(
                    InvalidInput,
                    format!("{op}: `other` was issued by a different allocator")
                ),
                self,
                other,
            ));
        }
        let alloc = self.allocator();
        let sl = self.len();
        let ol = other.len();
        let (sl_i, ol_i) = match (i64::try_from(sl), i64::try_from(ol)) {
            (Ok(s), Ok(o)) => (s, o),
            _ => {
                return Err(BStackJoinError::both(
                    io_error!(InvalidInput, format!("{op}: length overflows i64")),
                    self,
                    other,
                ));
            }
        };

        // Attempt A: extend self's tail by `ol`, then copy `other` into it.
        match alloc.realloc_inplace(self, 0, ol_i) {
            Ok(mut grown) => {
                if let Err(source) = grown.copy_from_at(sl, &other.as_slice()) {
                    // Undo the grow to restore `self`; return both intact.
                    return Err(match alloc.realloc_inplace(grown, 0, -ol_i) {
                        Ok(self_again) => BStackJoinError::both(source, self_again, other),
                        // Undo failed: `self`'s region is `undo.handle` (or lost).
                        Err(undo) => BStackJoinError::new(source, undo.handle, Some(other)),
                    });
                }
                match alloc.dealloc(other) {
                    Ok(()) => Ok(grown),
                    // Join committed in `grown`; `other` could not be freed.
                    Err(e) => Err(BStackJoinError::new(e.source, Some(grown), None)),
                }
            }
            Err(ea) => {
                let self_back = match ea.handle {
                    Some(h) => h,
                    // `self` lost mid-grow: cannot retry or fall back.
                    None => return Err(BStackJoinError::new(ea.source, None, Some(other))),
                };
                // Attempt B: extend other's front by `sl`, then copy `self` in.
                match alloc.realloc_inplace(other, sl_i, 0) {
                    Ok(mut grown2) => {
                        if let Err(source) = grown2.copy_from_at(0, &self_back.as_slice()) {
                            return Err(match alloc.realloc_inplace(grown2, -sl_i, 0) {
                                Ok(other_again) => {
                                    BStackJoinError::both(source, self_back, other_again)
                                }
                                // Undo failed: `other`'s region is `undo.handle`.
                                Err(undo) => {
                                    BStackJoinError::new(source, Some(self_back), undo.handle)
                                }
                            });
                        }
                        match alloc.dealloc(self_back) {
                            // Join committed in `grown2` (the result).
                            Ok(()) => Ok(grown2),
                            Err(e) => Err(BStackJoinError::new(e.source, Some(grown2), None)),
                        }
                    }
                    Err(eb) => {
                        let other_back = match eb.handle {
                            Some(h) => h,
                            None => {
                                return Err(BStackJoinError::new(eb.source, Some(self_back), None));
                            }
                        };
                        // Both directions declined; both inputs intact.
                        if fallback {
                            fresh_join(alloc, self_back, other_back, sl, ol, op)
                        } else {
                            Err(BStackJoinError::both(eb.source, self_back, other_back))
                        }
                    }
                }
            }
        }
    }
}

/// Fresh-allocation join used by [`BStackOwnedSlice::try_join`]: allocate the
/// combined region, copy both inputs, then free them. A failed copy leaves both
/// inputs intact; a failed final free leaks that input but still returns the
/// joined result (see the impl-level "Post-commit cleanup" note). `op` names the
/// calling method for the overflow message.
#[cfg(all(feature = "set", feature = "atomic"))]
fn fresh_join<'a, A>(
    alloc: &'a A,
    a: BStackOwnedSlice<'a, A>,
    b: BStackOwnedSlice<'a, A>,
    al: u64,
    bl: u64,
    op: &'static str,
) -> Result<BStackOwnedSlice<'a, A>, crate::alloc::BStackJoinError<'a, A>>
where
    A: crate::alloc::BStackInPlaceResizeAllocator + crate::alloc::BStackOwnedSliceAllocator,
{
    use crate::alloc::BStackJoinError;
    let total = match al.checked_add(bl) {
        Some(t) => t,
        None => {
            return Err(BStackJoinError::both(
                io_error!(InvalidInput, format!("{op}: combined length overflows")),
                a,
                b,
            ));
        }
    };
    let mut dst = match alloc.alloc(total) {
        Ok(d) => d,
        Err(source) => return Err(BStackJoinError::both(source, a, b)),
    };
    if let Err(source) = dst.copy_from_at(0, &a.as_slice()) {
        let _ = alloc.dealloc(dst);
        return Err(BStackJoinError::both(source, a, b));
    }
    if let Err(source) = dst.copy_from_at(al, &b.as_slice()) {
        let _ = alloc.dealloc(dst);
        return Err(BStackJoinError::both(source, a, b));
    }
    // Result committed; free both inputs best-effort.
    let _ = alloc.dealloc(a);
    let _ = alloc.dealloc(b);
    Ok(dst)
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

/// Borrows the coordinate pair. `Eq`, `Ord` and `Hash` on `BStackOwnedSlice`
/// are the range's own, so a map keyed by handles can be probed with a bare
/// [`BStackRange`] — no allocator reference or `unsafe` handle needed.
impl<'a, A: BStackAllocator> Borrow<BStackRange> for BStackOwnedSlice<'a, A> {
    #[inline]
    fn borrow(&self) -> &BStackRange {
        &self.range
    }
}

impl<'a, A: BStackAllocator> AsRef<BStackRange> for BStackOwnedSlice<'a, A> {
    #[inline]
    fn as_ref(&self) -> &BStackRange {
        &self.range
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

/// The slice's range and the cursor, as `start..end@cursor` — the cursor
/// relative to the slice, as [`position`](Self::position) reports it.
impl<'a> fmt::Display for BStackSliceReader<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}..{}@{}",
            self.slice.start(),
            self.slice.end(),
            self.cursor
        )
    }
}

impl<'a> BStackSliceReader<'a> {
    /// Return the current cursor position within the slice (not the payload).
    #[inline]
    #[must_use]
    pub fn position(&self) -> u64 {
        self.cursor
    }

    /// Consume the reader and return the underlying [`BStackSlice`].
    #[inline]
    #[must_use]
    pub fn into_slice(self) -> BStackSlice<'a> {
        self.slice
    }

    /// Return a reference to the underlying [`BStackSlice`].
    #[inline]
    #[must_use]
    pub fn slice(&self) -> &BStackSlice<'a> {
        &self.slice
    }
}

/// Derefs to the underlying [`BStackSlice`], so the slice's shared-reference
/// API ([`len`](BStackSlice::len), [`read`](BStackSlice::read),
/// [`subslice`](BStackSlice::subslice), …) is reachable directly on the
/// reader. Equivalent to [`slice`](Self::slice).
///
/// There is deliberately no `DerefMut`: the slice's `&mut self` methods would
/// write outside the cursor, and [`io::Read::read`] shadows
/// [`BStackSlice::read`] on this type.
impl<'a> Deref for BStackSliceReader<'a> {
    type Target = BStackSlice<'a>;

    #[inline]
    fn deref(&self) -> &BStackSlice<'a> {
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
        self.slice.s_get_into(abs_start, &mut buf[..n])?;
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
            return Err(io_error!(InvalidInput, "seek before beginning of slice"));
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

/// The slice's range and the cursor, as `start..end@cursor` — the cursor
/// relative to the slice, as [`position`](Self::position) reports it.
#[cfg(feature = "set")]
impl<'a> fmt::Display for BStackSliceWriter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}..{}@{}",
            self.slice.start(),
            self.slice.end(),
            self.cursor
        )
    }
}

#[cfg(feature = "set")]
impl<'a> BStackSliceWriter<'a> {
    /// Return the current cursor position within the slice (not the payload).
    #[inline]
    #[must_use]
    pub fn position(&self) -> u64 {
        self.cursor
    }

    /// Consume the writer and return the underlying [`BStackSlice`].
    #[inline]
    #[must_use]
    pub fn into_slice(self) -> BStackSlice<'a> {
        self.slice
    }

    /// Return a reference to the underlying [`BStackSlice`].
    #[inline]
    #[must_use]
    pub fn slice(&self) -> &BStackSlice<'a> {
        &self.slice
    }
}

/// Derefs to the underlying [`BStackSlice`], so the slice's shared-reference
/// API ([`len`](BStackSlice::len), [`read`](BStackSlice::read),
/// [`subslice`](BStackSlice::subslice), …) is reachable directly on the
/// writer. Equivalent to [`slice`](Self::slice).
///
/// There is deliberately no `DerefMut`: it would hand out the exclusive slice
/// the writer holds, and [`BStackSlice::write`] — which overwrites from the
/// start of the region — would sit one shadow away from
/// [`io::Write::write`], which writes at the cursor.
#[cfg(feature = "set")]
impl<'a> Deref for BStackSliceWriter<'a> {
    type Target = BStackSlice<'a>;

    #[inline]
    fn deref(&self) -> &BStackSlice<'a> {
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
        self.slice.s_set(abs_start, &buf[..n])?;
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
            return Err(io_error!(InvalidInput, "seek before beginning of slice"));
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
