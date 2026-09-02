//! Fixed-stride chunked view over a [`BStackSlice`].
//!
//! Requires feature `alloc`. Sorting, selection, and mutation additionally
//! require `set` (+ `atomic` for the permutation ops); binary search,
//! `partition_point`, construction, iteration, `first`/
//! `last`, `split_at`, and `is_sorted_by` need no flag beyond `alloc`.

#[cfg(all(feature = "set", feature = "atomic"))]
use super::BStackOwnedSliceAllocator;
use super::{BStackAllocator, BStackOwnedSlice, BStackRange, BStackSlice};
use crate::BStack;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io;

/// One window / block for the out-of-core partial sort, in bytes. The sort
/// keeps at most [`SORT_WINDOWS`] windows resident, so its peak buffer is
/// `SORT_WINDOWS * SORT_BLOCK_BYTES` regardless of the region's total size.
/// This is the partial sort's own budget — not tied to any streaming buffer
/// elsewhere in the crate, and may be tuned independently.
#[cfg(all(feature = "set", feature = "atomic"))]
const SORT_BLOCK_BYTES: u64 = 2048;

/// Windows held resident at once by the out-of-core partial sort: two merge
/// inputs plus one carry/output window (equivalently, a three-block
/// run-formation load). See [`SORT_BLOCK_BYTES`].
#[cfg(all(feature = "set", feature = "atomic"))]
const SORT_WINDOWS: u64 = 3;

/// Total resident byte budget for the out-of-core sort and select engines:
/// [`SORT_WINDOWS`] windows of [`SORT_BLOCK_BYTES`]. No step holds more than this.
#[cfg(all(feature = "set", feature = "atomic"))]
const SORT_BUDGET: u64 = SORT_WINDOWS * SORT_BLOCK_BYTES;

/// Chunk-sized scratch buffers up to this many bytes live on the stack;
/// larger ones fall back to a single heap allocation. Covers the common case
/// (short fixed-width records) without allocating at all.
#[cfg(all(feature = "set", feature = "atomic"))]
const INLINE_SCRATCH_LEN: usize = 128;

/// Below this total region size in bytes, the `atomic`-feature
/// `binary_search_by`/`partition_point` skip the O(log n) probe-by-probe
/// [`BStack::get_batched_gen`] path and instead take the `not(atomic)`
/// variants' own path: one whole-region [`BStack::get`] read, searched in
/// memory. Still atomic as a single lock acquisition either way.
#[cfg(feature = "atomic")]
const BULK_READ_BUDGET: usize = 512;

/// A fixed-size-record view over a [`BStackSlice`] — a slice with a stride.
///
/// Divides an underlying region into `chunk_len`-byte records. `BStackChunk`
/// sits at the same semantic position as [`BStackSlice`]: it carries `&'a
/// BStack` directly, performs I/O only through explicit methods, and has no
/// allocator operations of its own.
///
/// `BStackChunk` is a **view**, not an iterator — it does not implement
/// [`Iterator`]. Call [`iter`](Self::iter) (or use `IntoIterator`) to walk
/// its chunks lazily, or call [`sort_by`](Self::sort_by),
/// [`binary_search_by`](Self::binary_search_by), or
/// [`select_nth_by`](Self::select_nth_by) directly on the view.
///
/// `BStackChunk` is always fully aligned — it covers only the whole-chunk
/// portion of its source. Obtained via [`BStackSlice::chunks`] or
/// [`BStackSlice::rchunks`], which each return `(BStackChunk, BStackSlice)`:
/// the aligned view alongside whatever leftover bytes (if `chunk_len` does
/// not evenly divide the source length) don't fit a whole chunk. The two
/// constructors differ only in which end of the source they align from, and
/// therefore which sub-region ends up in the chunk view versus the leftover
/// slice. A view can also be built directly from an already-aligned region
/// via [`from_raw_parts`](Self::from_raw_parts) or
/// [`from_raw_slice`](Self::from_raw_slice) (both `unsafe`), or
/// [`from_slice`](Self::from_slice) (safe, checked) — none of these split
/// off a remainder, so the input must already satisfy the alignment
/// invariant.
///
/// # Not `Copy`
///
/// Like `BStackSlice`, deliberately non-`Copy`: `&mut self` methods
/// ([`sort_by`](Self::sort_by), [`select_nth_by`](Self::select_nth_by), …)
/// give genuine single-writer exclusivity in safe code. `Clone` for cases
/// where an explicit second view is needed.
pub struct BStackChunk<'a> {
    aligned: BStackSlice<'a>,
    chunk_len: u64,
}

impl<'a> Clone for BStackChunk<'a> {
    #[inline]
    fn clone(&self) -> Self {
        BStackChunk {
            aligned: self.aligned.clone(),
            chunk_len: self.chunk_len,
        }
    }
}

impl<'a> fmt::Debug for BStackChunk<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BStackChunk")
            .field("chunk_len", &self.chunk_len)
            .field("chunk_count", &self.chunk_count())
            .finish_non_exhaustive()
    }
}

/// The underlying region's range and the stride, as `start..end/chunk_len`.
impl<'a> fmt::Display for BStackChunk<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}..{}/{}",
            self.aligned.start(),
            self.aligned.end(),
            self.chunk_len
        )
    }
}

/// Location equality: same stride *and* the same underlying region
/// (`self.as_slice() == other.as_slice()`), not content. No cross-type
/// `PartialEq` against a bare [`BStackSlice`] is provided — a `BStackChunk`
/// with stride 4 and one with stride 8 over the identical bytes are not
/// interchangeable, so comparing a chunk view directly to a plain slice
/// would silently discard the stride and invite confusion.
impl<'a> PartialEq for BStackChunk<'a> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.chunk_len == other.chunk_len && self.aligned == other.aligned
    }
}

impl<'a> Eq for BStackChunk<'a> {}

impl<'a> Hash for BStackChunk<'a> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.chunk_len.hash(state);
        self.aligned.hash(state);
    }
}

/// Ordered first by stride, then by the underlying region's own `Ord`
/// (`offset`, then `len`).
impl<'a> PartialOrd for BStackChunk<'a> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for BStackChunk<'a> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.chunk_len
            .cmp(&other.chunk_len)
            .then_with(|| self.aligned.cmp(&other.aligned))
    }
}

/// Discards the stride, yielding the aligned region. Equivalent to
/// [`into_slice`](BStackChunk::into_slice).
impl<'a> From<BStackChunk<'a>> for BStackSlice<'a> {
    #[inline]
    fn from(chunk: BStackChunk<'a>) -> Self {
        chunk.into_slice()
    }
}

/// Borrows the aligned region. Deliberately not `Deref`: autoderef would pull
/// the slice's byte-unit methods into this type's chunk-unit API.
impl<'a> AsRef<BStackSlice<'a>> for BStackChunk<'a> {
    #[inline]
    fn as_ref(&self) -> &BStackSlice<'a> {
        &self.aligned
    }
}

/// Discards the stride, yielding the aligned region's coordinate pair.
impl<'a> From<BStackChunk<'a>> for BStackRange {
    #[inline]
    fn from(chunk: BStackChunk<'a>) -> Self {
        chunk.aligned.as_range()
    }
}

/// Discards the stride, serialising the aligned region as `offset` then
/// `len`, 8 bytes LE each.
impl<'a> From<BStackChunk<'a>> for [u8; 16] {
    #[inline]
    fn from(chunk: BStackChunk<'a>) -> Self {
        chunk.aligned.to_bytes()
    }
}

impl<'a> BStackChunk<'a> {
    /// Construct a `BStackChunk` from raw parts: a stack, byte offset, byte
    /// length, and stride — no I/O, no validation.
    ///
    /// # Safety
    ///
    /// `offset + len` must not overflow `u64`. `[offset, offset + len)`
    /// should lie within the current payload of `stack` for I/O to succeed
    /// (out-of-bounds coordinates are not a memory-safety hazard by
    /// themselves — I/O on them just returns `io::Error`). The caller must
    /// additionally uphold `BStackChunk`'s "always fully aligned" invariant:
    /// `chunk_len` must be nonzero and evenly divide `len`. Violating this
    /// is not undefined behavior, but corrupts assumptions relied on by
    /// [`chunk_count`](Self::chunk_count), [`get`](Self::get),
    /// [`merge`](Self::merge), [`merge_adjacent`](Self::merge_adjacent), and
    /// the phase logic in [`same_phase`](Self::same_phase) — e.g.
    /// `chunk_count` silently truncates rather than panicking, so a
    /// misaligned raw chunk quietly drops trailing bytes.
    #[inline]
    #[must_use]
    pub unsafe fn from_raw_parts(stack: &'a BStack, offset: u64, len: u64, chunk_len: u64) -> Self {
        BStackChunk {
            aligned: unsafe { BStackSlice::from_raw_parts(stack, offset, len) },
            chunk_len,
        }
    }

    /// Construct a `BStackChunk` from an existing [`BStackSlice`] and a
    /// stride, without validating the "always fully aligned" invariant.
    ///
    /// # Safety
    ///
    /// `chunk_len` must be nonzero and evenly divide `aligned.len()`.
    #[inline]
    #[must_use]
    pub unsafe fn from_raw_slice(aligned: BStackSlice<'a>, chunk_len: u64) -> Self {
        BStackChunk { aligned, chunk_len }
    }

    /// Construct a `BStackChunk` from an existing [`BStackSlice`] and a
    /// stride, validating that the slice already satisfies the "always fully
    /// aligned" invariant.
    ///
    /// Returns `None` if `chunk_len == 0` or if `aligned.len()` is not a
    /// multiple of `chunk_len`. Unlike [`BStackSlice::chunks`]/[`rchunks`](BStackSlice::rchunks),
    /// this never splits off a remainder — the whole slice must already fit
    /// the stride exactly.
    #[inline]
    #[must_use]
    pub fn from_slice(aligned: BStackSlice<'a>, chunk_len: u64) -> Option<Self> {
        if chunk_len == 0 || !aligned.len().is_multiple_of(chunk_len) {
            return None;
        }
        Some(BStackChunk { aligned, chunk_len })
    }

    /// Length, in bytes, of one chunk.
    #[inline]
    #[must_use]
    pub fn chunk_len(&self) -> u64 {
        self.chunk_len
    }

    /// Number of complete chunks in the view.
    #[inline]
    #[must_use]
    pub fn chunk_count(&self) -> u64 {
        self.aligned.len() / self.chunk_len
    }

    /// Total bytes covered by complete chunks.
    #[inline]
    #[must_use]
    pub fn len(&self) -> u64 {
        self.aligned.len()
    }

    /// Returns `true` if there are no complete chunks.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.aligned.is_empty()
    }

    /// Returns `true` if this view and `other` use the same stride
    /// (`chunk_len`).
    #[inline]
    #[must_use]
    pub fn same_stride(&self, other: &Self) -> bool {
        self.chunk_len == other.chunk_len
    }

    /// Returns `true` if this view and `other` share a stride *and* their
    /// chunk boundaries fall on the same phase — i.e. their aligned regions'
    /// start offsets are congruent modulo `chunk_len`, so a chunk boundary in
    /// one view lines up with a chunk boundary in the other wherever the
    /// regions coincide.
    #[inline]
    #[must_use]
    pub fn same_phase(&self, other: &Self) -> bool {
        self.same_stride(other)
            && self.aligned.start() % self.chunk_len == other.aligned.start() % self.chunk_len
    }

    /// Returns `true` if this view and `other` are same-phase and their
    /// aligned regions touch end-to-end with no gap and no overlap.
    ///
    /// Always `false` if the views are not [`same_phase`](Self::same_phase).
    #[inline]
    #[must_use]
    pub fn adjacent_to(&self, other: &Self) -> bool {
        self.aligned.adjacent_to(&other.aligned) && self.same_phase(other)
    }

    /// Returns `true` if this view and `other` are same-phase and their
    /// aligned regions share at least one byte.
    ///
    /// Always `false` if the views are not [`same_phase`](Self::same_phase).
    #[inline]
    #[must_use]
    pub fn overlaps(&self, other: &Self) -> bool {
        self.aligned.overlaps(&other.aligned) && self.same_phase(other)
    }

    /// Merge this view with `other` into a single view covering both.
    ///
    /// Succeeds if the views [`overlaps`](Self::overlaps) (implying same
    /// stride and phase), or if either view [`is_empty`](Self::is_empty) and
    /// they [`same_stride`](Self::same_stride) — an empty view acts as an
    /// identity element, so merging with one returns the other, non-empty
    /// view unchanged regardless of phase.
    ///
    /// Returns `None` if the views use different strides, or if both are
    /// non-empty and their aligned regions don't overlap.
    #[must_use]
    pub fn merge(&self, other: &Self) -> Option<Self> {
        if !self.same_stride(other) {
            return None;
        }
        if self.is_empty() {
            return Some(other.clone());
        }
        if other.is_empty() {
            return Some(self.clone());
        }
        if !self.overlaps(other) {
            return None;
        }
        let aligned = self.aligned.merge(&other.aligned)?;
        Some(BStackChunk {
            aligned,
            chunk_len: self.chunk_len,
        })
    }

    /// Merge this view with `other` into a single view covering both,
    /// requiring them to be [`same_stride`](Self::same_stride) and touching
    /// end-to-end with both non-empty — the latter two are enforced by the
    /// underlying [`BStackSlice::merge_adjacent`] call.
    ///
    /// Same stride is the only phase-related precondition needed: for two
    /// non-empty, same-stride chunk views, byte-adjacency already forces the
    /// same phase (each view's aligned length is a multiple of its stride,
    /// so the touching endpoint is congruent to both starts mod stride), so
    /// checking [`same_phase`](Self::same_phase) here would be redundant.
    ///
    /// Returns `None` if the views are not adjacent.
    #[must_use]
    pub fn merge_adjacent(&self, other: &Self) -> Option<Self> {
        if !self.same_stride(other) {
            return None;
        }
        let aligned = self.aligned.merge_adjacent(&other.aligned)?;
        Some(BStackChunk {
            aligned,
            chunk_len: self.chunk_len,
        })
    }

    /// The aligned region covered by whole chunks, as a plain [`BStackSlice`].
    #[inline]
    #[must_use]
    pub fn as_slice(&self) -> BStackSlice<'a> {
        self.aligned.clone()
    }

    /// Consume this view, returning the aligned region as a plain
    /// [`BStackSlice`] without cloning.
    #[inline]
    #[must_use]
    pub fn into_slice(self) -> BStackSlice<'a> {
        self.aligned
    }

    /// Copy this view's aligned region into a fresh allocation from
    /// `allocator`, returning a plain [`BStackOwnedSlice`].
    ///
    /// Delegates to [`BStackSlice::to_owned_in`] on [`as_slice`](Self::as_slice).
    /// The result is a plain owned slice, not an owned chunk — re-chunk it with
    /// [`from_slice`](Self::from_slice)`(owned.as_slice(), self.chunk_len())`,
    /// which cannot fail since the copy preserves length and alignment.
    ///
    /// Requires the `set` and `atomic` features.
    ///
    /// # Errors
    ///
    /// As [`BStackSlice::to_owned_in`].
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn to_owned_in<'b, A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &'b A,
    ) -> io::Result<BStackOwnedSlice<'b, A>> {
        self.as_slice().to_owned_in(allocator)
    }

    /// Like [`to_owned_in`](Self::to_owned_in), but skips the destination's
    /// zero-fill via [`BStackSlice::to_owned_uninit_in`].
    ///
    /// Requires the `set` and `atomic` features, and an allocator implementing
    /// [`BStackUninitAllocator`](super::BStackUninitAllocator).
    ///
    /// # Errors
    ///
    /// As [`BStackSlice::to_owned_in`].
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn to_owned_uninit_in<'b, A>(&self, allocator: &'b A) -> io::Result<BStackOwnedSlice<'b, A>>
    where
        A: super::BStackUninitAllocator + BStackOwnedSliceAllocator,
    {
        self.as_slice().to_owned_uninit_in(allocator)
    }

    /// Re-divide this view's aligned region with a different stride,
    /// returning `(chunk_view, remainder)` — equivalent to
    /// `self.into_slice().chunks(new_stride)`.
    ///
    /// # Panics
    ///
    /// Panics if `new_stride == 0`.
    #[inline]
    #[must_use]
    #[track_caller]
    pub fn with_stride(self, new_stride: u64) -> (BStackChunk<'a>, BStackSlice<'a>) {
        self.aligned.chunks(new_stride)
    }

    /// Split into two chunk-granularity sub-views at chunk index `mid`: the
    /// first holds chunks `[0, mid)`, the second holds `[mid,
    /// chunk_count())`. Both share this view's stride and phase.
    ///
    /// No I/O — pure offset arithmetic, as cheap as
    /// [`with_stride`](Self::with_stride).
    ///
    /// # Panics
    ///
    /// Panics if `mid > self.chunk_count()`.
    #[inline]
    #[must_use]
    #[track_caller]
    pub fn split_at(&self, mid: u64) -> (BStackChunk<'a>, BStackChunk<'a>) {
        assert!(
            mid <= self.chunk_count(),
            "split_at: mid must be <= chunk_count"
        );
        let (left, right) = self.aligned.split_at(mid * self.chunk_len);
        (
            BStackChunk {
                aligned: left,
                chunk_len: self.chunk_len,
            },
            BStackChunk {
                aligned: right,
                chunk_len: self.chunk_len,
            },
        )
    }

    /// Return the underlying [`BStack`].
    #[inline]
    #[must_use]
    pub fn stack(&self) -> &'a BStack {
        self.aligned.stack()
    }

    /// Get the `index`-th chunk, or `None` if out of bounds.
    ///
    /// O(1), pure offset arithmetic — no I/O.
    #[inline]
    #[must_use]
    pub fn get(&self, index: u64) -> Option<BStackSlice<'a>> {
        if index >= self.chunk_count() {
            return None;
        }
        let start = index * self.chunk_len;
        Some(self.aligned.subslice(start, start + self.chunk_len))
    }

    /// The first chunk, or `None` if the view is empty.
    ///
    /// O(1), pure offset arithmetic — no I/O. Equivalent to `self.get(0)`.
    #[inline]
    #[must_use]
    pub fn first(&self) -> Option<BStackSlice<'a>> {
        self.get(0)
    }

    /// The last chunk, or `None` if the view is empty.
    ///
    /// O(1), pure offset arithmetic — no I/O.
    #[inline]
    #[must_use]
    pub fn last(&self) -> Option<BStackSlice<'a>> {
        let count = self.chunk_count();
        if count == 0 {
            return None;
        }
        self.get(count - 1)
    }

    /// Create a lazy iterator over the chunks of this view.
    ///
    /// This clones the view; the iterator and `self` are independent. See
    /// [`BStackChunkIter`] for the laziness guarantee.
    #[inline]
    #[must_use]
    pub fn iter(&self) -> BStackChunkIter<'a> {
        BStackChunkIter {
            remaining: self.aligned.clone(),
            chunk_len: self.chunk_len,
        }
    }

    /// Swap the chunks at `i` and `j`.
    ///
    /// A single crash-atomic [`BStack::cross_exchange`] call: after a crash
    /// the two chunks hold either their original contents or the fully
    /// swapped contents, never a half-swap. `i == j` is a valid no-op.
    ///
    /// Requires the `set` and `atomic` features.
    ///
    /// # Panics
    ///
    /// Panics if `i >= self.chunk_count()` or `j >= self.chunk_count()`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[track_caller]
    pub fn swap(&mut self, i: u64, j: u64) -> io::Result<()> {
        let count = self.chunk_count();
        assert!(i < count, "swap: i must be < chunk_count");
        assert!(j < count, "swap: j must be < chunk_count");
        if i == j {
            return Ok(());
        }
        let start = self.aligned.start();
        self.aligned.stack().cross_exchange(
            start + i * self.chunk_len,
            start + j * self.chunk_len,
            self.chunk_len,
        )
    }

    /// Reverse the order of the chunks in place.
    ///
    /// Chunk-granularity: whole chunks change position, but the bytes within
    /// each chunk are untouched. A single crash-atomic [`BStack::process`]
    /// call, same shape and same atomicity as [`sort_by`](Self::sort_by): a
    /// crash leaves either the pre-reverse order or the fully-reversed order,
    /// never an intermediate permutation.
    ///
    /// Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[inline]
    pub fn reverse(&mut self) -> io::Result<()> {
        let chunk_len = self.chunk_len as usize;
        let start = self.aligned.start();
        let end = self.aligned.end();
        self.aligned.stack().process(start, end, |buf| {
            reverse_chunks(buf, chunk_len);
        })
    }

    /// Rotate the chunks in place such that the chunk currently at index
    /// `k` becomes the first chunk.
    ///
    /// Chunk-granularity: equivalent to rotating the underlying bytes left
    /// by `k * chunk_len()` bytes, which — since that offset always falls on
    /// a chunk boundary — permutes whole chunks without disturbing the bytes
    /// within any of them. A single crash-atomic [`BStack::process`] call,
    /// same shape and same atomicity as [`sort_by`](Self::sort_by).
    ///
    /// Requires the `set` and `atomic` features.
    ///
    /// # Panics
    ///
    /// Panics if `k > self.chunk_count()`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[track_caller]
    pub fn rotate_left(&mut self, k: u64) -> io::Result<()> {
        assert!(
            k <= self.chunk_count(),
            "rotate_left: k must be <= chunk_count"
        );
        let mid = (k * self.chunk_len) as usize;
        let start = self.aligned.start();
        let end = self.aligned.end();
        self.aligned
            .stack()
            .process(start, end, |buf| buf.rotate_left(mid))
    }

    /// Rotate the chunks in place such that the last `k` chunks move to the
    /// front.
    ///
    /// Chunk-granularity companion to [`rotate_left`](Self::rotate_left); see
    /// there for the byte/chunk-boundary argument and the atomicity
    /// guarantee.
    ///
    /// Requires the `set` and `atomic` features.
    ///
    /// # Panics
    ///
    /// Panics if `k > self.chunk_count()`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[track_caller]
    pub fn rotate_right(&mut self, k: u64) -> io::Result<()> {
        assert!(
            k <= self.chunk_count(),
            "rotate_right: k must be <= chunk_count"
        );
        let mid = (k * self.chunk_len) as usize;
        let start = self.aligned.start();
        let end = self.aligned.end();
        self.aligned
            .stack()
            .process(start, end, |buf| buf.rotate_right(mid))
    }

    /// Fill every chunk in the view with a copy of `chunk`.
    ///
    /// A single crash-atomic [`BStack::repeat`] call: only `chunk` and the
    /// chunk count are journaled, not the whole region, so a crash-safe fill
    /// of a large view costs a fixed-size journal rather than one
    /// proportional to the view.
    ///
    /// Requires the `set` feature.
    ///
    /// # Panics
    ///
    /// Panics if `chunk.len() != self.chunk_len()`.
    #[cfg(feature = "set")]
    #[inline]
    #[track_caller]
    pub fn fill(&mut self, chunk: &[u8]) -> io::Result<()> {
        assert_eq!(
            chunk.len() as u64,
            self.chunk_len,
            "fill: chunk length must equal chunk_len"
        );
        let count = self.chunk_count();
        self.aligned
            .stack()
            .repeat(self.aligned.start(), chunk, count)
    }

    /// Overwrite the chunk at `index` with `bytes`.
    ///
    /// Delegates to [`get`](Self::get) plus
    /// [`BStackSlice::copy_from_slice`] — included for symmetry with `get`.
    ///
    /// Requires the `set` feature.
    ///
    /// # Panics
    ///
    /// Panics if `index >= self.chunk_count()`, or if `bytes.len()` does not
    /// equal `self.chunk_len()` (via `copy_from_slice`).
    #[cfg(feature = "set")]
    #[inline]
    #[track_caller]
    pub fn set(&mut self, index: u64, bytes: &[u8]) -> io::Result<()> {
        let mut chunk = self.get(index).expect("set: index must be < chunk_count");
        chunk.copy_from_slice(bytes)
    }

    /// Sort chunks in place by `cmp`, comparing each chunk's raw bytes.
    ///
    /// Crash-atomic as one atomic operation: a single [`BStack::process`]
    /// call reads the whole chunked region (excluding the remainder) into
    /// memory, sorts there, and commits the result in one write — a crash
    /// leaves either the pre-sort order or the fully-sorted order, never an
    /// intermediate permutation. Stable: chunks that compare equal keep
    /// their relative order, matching `[T]::sort_by`.
    ///
    /// Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn sort_by(&mut self, mut cmp: impl FnMut(&[u8], &[u8]) -> Ordering) -> io::Result<()> {
        let chunk_len = self.chunk_len as usize;
        let start = self.aligned.start();
        let end = self.aligned.end();
        self.aligned.stack().process(start, end, |buf| {
            sort_chunks_by(buf, chunk_len, &mut cmp);
        })
    }

    /// Sort chunks in place by a key extracted from each chunk's bytes.
    ///
    /// The key is computed once per chunk before sorting (not once per
    /// comparison). Same atomicity and stability guarantees as
    /// [`sort_by`](Self::sort_by).
    ///
    /// Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn sort_by_key<K: Ord>(&mut self, mut key: impl FnMut(&[u8]) -> K) -> io::Result<()> {
        let chunk_len = self.chunk_len as usize;
        let start = self.aligned.start();
        let end = self.aligned.end();
        self.aligned.stack().process(start, end, |buf| {
            let keys: Vec<K> = buf.chunks_exact(chunk_len).map(&mut key).collect();
            let mut order: Vec<usize> = (0..keys.len()).collect();
            order.sort_by(|&i, &j| keys[i].cmp(&keys[j]));
            apply_chunk_permutation(buf, chunk_len, &order);
        })
    }

    /// Binary search for a chunk matching `cmp`, over chunks already ordered
    /// by the same comparator (caller's responsibility, as with `std`).
    ///
    /// `cmp` should return [`Ordering::Less`] if the probed chunk sorts
    /// before the target, [`Ordering::Greater`] if after, and
    /// [`Ordering::Equal`] on a match — the same convention as
    /// `[T]::binary_search_by`.
    ///
    /// Reads only the probed chunks — O(log n) chunk reads, never the whole
    /// region — into one reused buffer (stack-allocated for `chunk_len <=
    /// BULK_READ_BUDGET`), not a fresh allocation per probe. Below
    /// [`BULK_READ_BUDGET`] total bytes, skips probing altogether and takes
    /// the `not(atomic)` path's own whole-region read instead.
    ///
    /// Atomic as a whole either way: the probing path runs every probe inside
    /// one [`BStack::get_batched_gen`] sequence, held under a single lock for
    /// the entire search (a shared read lock on Unix/Windows, so concurrent
    /// *readers* still proceed in parallel; only a concurrent write is
    /// excluded).
    ///
    /// Requires the `atomic` feature for O(log n) reads.
    ///
    /// Returns `Ok(Ok(index))` naming a matching chunk, or `Ok(Err(index))`
    /// with the index a matching chunk would need to be inserted at to keep
    /// the chunks ordered. `Err` propagates an I/O failure from probing a
    /// chunk.
    #[cfg(feature = "atomic")]
    pub fn binary_search_by(
        &self,
        mut cmp: impl FnMut(&[u8]) -> Ordering,
    ) -> io::Result<Result<u64, u64>> {
        let chunk_len = self.chunk_len as usize;

        if self.aligned.len() <= BULK_READ_BUDGET as u64 {
            let mut inline = [0u8; BULK_READ_BUDGET];
            self.aligned.read_into(&mut inline)?;
            return Ok(binary_search_in_memory(
                &inline,
                chunk_len,
                self.chunk_count(),
                &mut cmp,
            ));
        }

        // `get_batched_gen` protects the generator for the whole call, so the
        // generator must not *hold* a reference into either buffer — the
        // `&mut [u8]` it hands back has to be the only live reference to those
        // bytes while the read is in flight. It therefore captures raw
        // pointers only (never a slice), and materializes a short-lived
        // shared slice for `cmp` at the point of use. Each pointer is taken
        // exactly once, and neither buffer is named again until the call
        // returns.
        let mut inline = [0u8; BULK_READ_BUDGET];
        let inline_ptr = inline.as_mut_ptr();
        let mut heap;
        let probe_ptr: *mut u8 = if chunk_len <= BULK_READ_BUDGET {
            inline_ptr
        } else {
            heap = vec![0u8; chunk_len];
            heap.as_mut_ptr()
        };

        let start = self.aligned.start();
        let chunk_len_u64 = self.chunk_len;
        let mut size = self.chunk_count();
        let mut left = 0u64;
        let mut mid = 0u64;
        let mut half = 0u64;
        let mut state = 0;
        // Set on an exact match found mid-probe — short-circuits the search,
        // whether or not the remaining window would otherwise have collapsed
        // to a bulk read.
        let mut found: Option<u64> = None;

        self.aligned.stack().get_batched_gen(|| {
            if state == 1 {
                // SAFETY: the previous probe read `chunk_len` bytes into
                // `probe_ptr` and has completed, so the bytes are initialized
                // and no other reference to them is live. The slice dies with
                // the `cmp` call, before the next read is issued below.
                let probe = unsafe { core::slice::from_raw_parts(probe_ptr, chunk_len) };
                match cmp(probe) {
                    Ordering::Less => {
                        left = mid + 1;
                        size -= half + 1;
                    }
                    Ordering::Greater => size = half,
                    Ordering::Equal => {
                        found = Some(mid);
                        return None;
                    }
                }
            }
            if size == 0 || state == 2 {
                // Quit if there is nothing left or if we read within bulk.
                return None;
            }
            half = size / 2;
            mid = left + half;
            let rem_bytes = size * chunk_len_u64;
            let bulk = rem_bytes <= BULK_READ_BUDGET as u64;
            let offset = if bulk { left } else { mid };
            Some((
                start + offset * chunk_len_u64,
                // SAFETY: both buffers outlive the generator, and this is the
                // only live reference into the one it names — the shared
                // slice handed to `cmp` above is already dead.
                unsafe {
                    if bulk {
                        state = 2;
                        // `bulk` established `rem_bytes <= BULK_READ_BUDGET`,
                        // so the cast is lossless and the length fits `inline`.
                        core::slice::from_raw_parts_mut(inline_ptr, rem_bytes as usize)
                    } else {
                        state = 1;
                        core::slice::from_raw_parts_mut(probe_ptr, chunk_len)
                    }
                },
            ))
        })?;

        if let Some(idx) = found {
            return Ok(Ok(idx));
        }

        // Once the remaining window fits within the bulk-read budget, finish
        // the binary search entirely in memory.
        if state == 2 {
            // SAFETY: the bulk read filled `size * chunk_len` bytes at
            // `inline_ptr` (<= BULK_READ_BUDGET, so the cast is lossless), and
            // every `&mut` handed to `get_batched_gen` died with that call.
            let window =
                unsafe { core::slice::from_raw_parts(inline_ptr, (size * chunk_len_u64) as usize) };
            return Ok(
                match binary_search_in_memory(window, chunk_len, size, &mut cmp) {
                    Ok(idx) => Ok(left + idx),
                    Err(idx) => Err(left + idx),
                },
            );
        }

        Ok(Err(left))
    }

    /// Binary search for a chunk matching `cmp`, over chunks already ordered
    /// by the same comparator (caller's responsibility, as with `std`).
    ///
    /// `cmp` should return [`Ordering::Less`] if the probed chunk sorts
    /// before the target, [`Ordering::Greater`] if after, and
    /// [`Ordering::Equal`] on a match — the same convention as
    /// `[T]::binary_search_by`.
    ///
    /// Reads the whole aligned region in a single [`BStack::get`] call — one
    /// lock acquisition, so the search is atomic as a whole against
    /// concurrent mutation, on the same terms as [`is_sorted_by`](Self::is_sorted_by)
    /// — then searches in memory. No feature flag beyond `alloc`: read-only.
    /// Unlike the `atomic`-feature version of this method, this trades the
    /// O(log n) chunk-read bound for O(n) memory (the whole region is
    /// materialized once) in exchange for not needing `atomic`; enabling
    /// that feature switches to the bounded-memory alternative, which stays
    /// equally atomic via a different mechanism (a held lock across
    /// individual probes rather than one bulk read).
    ///
    /// Returns `Ok(Ok(index))` naming a matching chunk, or `Ok(Err(index))`
    /// with the index a matching chunk would need to be inserted at to keep
    /// the chunks ordered. `Err` propagates an I/O failure from reading the
    /// region.
    #[cfg(not(feature = "atomic"))]
    pub fn binary_search_by(
        &self,
        mut cmp: impl FnMut(&[u8]) -> Ordering,
    ) -> io::Result<Result<u64, u64>> {
        let chunk_len = self.chunk_len as usize;
        let buf = self.aligned.read()?;
        Ok(binary_search_in_memory(
            &buf,
            chunk_len,
            self.chunk_count(),
            &mut cmp,
        ))
    }

    /// Binary search for `target` by a key extracted from each probed
    /// chunk's bytes. See [`binary_search_by`](Self::binary_search_by) —
    /// atomic either way, via one of two mechanisms depending on whether
    /// the `atomic` feature is enabled.
    #[inline]
    pub fn binary_search_by_key<K: Ord>(
        &self,
        target: &K,
        mut key: impl FnMut(&[u8]) -> K,
    ) -> io::Result<Result<u64, u64>> {
        self.binary_search_by(|bytes| key(bytes).cmp(target))
    }

    /// Return the index of the first chunk for which `pred` returns `false`,
    /// assuming the view is already partitioned by `pred` — every chunk
    /// `pred` accepts sorts before every chunk it rejects (caller's
    /// responsibility, as with `std`). Mirrors `[T]::partition_point`.
    ///
    /// Reads only the probed chunks — O(log n) chunk reads, never the whole
    /// region — into one reused buffer (stack-allocated for `chunk_len <=
    /// BULK_READ_BUDGET`), not a fresh allocation per probe. Below
    /// [`BULK_READ_BUDGET`] total bytes, skips probing altogether and takes
    /// the `not(atomic)` path's own whole-region read instead.
    ///
    /// Atomic as a whole either way, on the same terms as the `atomic`-feature
    /// [`binary_search_by`](Self::binary_search_by) — one
    /// [`BStack::get_batched_gen`] sequence under a single lock for the
    /// probing path, or one bulk [`BStack::get`] under the same kind of lock
    /// for the small-region path.
    ///
    /// Requires the `atomic` feature for O(log n) reads.
    ///
    /// `Err` propagates an I/O failure from probing a chunk.
    #[cfg(feature = "atomic")]
    pub fn partition_point(&self, mut pred: impl FnMut(&[u8]) -> bool) -> io::Result<u64> {
        let chunk_len = self.chunk_len as usize;

        if self.aligned.len() <= BULK_READ_BUDGET as u64 {
            let mut inline = [0u8; BULK_READ_BUDGET];
            self.aligned.read_into(&mut inline)?;
            return Ok(partition_point_in_memory(
                &inline,
                chunk_len,
                self.chunk_count(),
                &mut pred,
            ));
        }

        // Raw pointers only in the generator, for the reason spelled out in
        // the `atomic` [`binary_search_by`](Self::binary_search_by).
        let mut inline = [0u8; BULK_READ_BUDGET];
        let inline_ptr = inline.as_mut_ptr();
        let mut heap;
        let probe_ptr: *mut u8 = if chunk_len <= BULK_READ_BUDGET {
            inline_ptr
        } else {
            heap = vec![0u8; chunk_len];
            heap.as_mut_ptr()
        };

        let start = self.aligned.start();
        let chunk_len_u64 = self.chunk_len;
        let mut size = self.chunk_count();
        let mut left = 0u64;
        let mut mid = 0u64;
        let mut half = 0u64;
        let mut state = 0;

        self.aligned.stack().get_batched_gen(|| {
            if state == 1 {
                // SAFETY: the previous probe read `chunk_len` bytes into
                // `probe_ptr` and has completed, so the bytes are initialized
                // and no other reference to them is live. The slice dies with
                // the `pred` call, before the next read is issued below.
                let probe = unsafe { core::slice::from_raw_parts(probe_ptr, chunk_len) };
                if pred(probe) {
                    left = mid + 1;
                    size -= half + 1;
                } else {
                    size = half;
                }
            }
            if size == 0 || state == 2 {
                // Quit if there is nothing left or if we read within bulk.
                return None;
            }
            half = size / 2;
            mid = left + half;
            let rem_bytes = size * chunk_len_u64;
            let bulk = rem_bytes <= BULK_READ_BUDGET as u64;
            let offset = if bulk { left } else { mid };
            Some((
                start + offset * chunk_len_u64,
                // SAFETY: both buffers outlive the generator, and this is the
                // only live reference into the one it names — the shared
                // slice handed to `pred` above is already dead.
                unsafe {
                    if bulk {
                        state = 2;
                        // `bulk` established `rem_bytes <= BULK_READ_BUDGET`,
                        // so the cast is lossless and the length fits `inline`.
                        core::slice::from_raw_parts_mut(inline_ptr, rem_bytes as usize)
                    } else {
                        state = 1;
                        core::slice::from_raw_parts_mut(probe_ptr, chunk_len)
                    }
                },
            ))
        })?;

        Ok(left
            // Once the remaining window fits within the bulk-read budget, finish
            // the partition point search entirely in memory.
            + if state == 2 {
                // SAFETY: the bulk read filled `size * chunk_len` bytes at
                // `inline_ptr` (<= BULK_READ_BUDGET, so the cast is lossless),
                // and every `&mut` handed to `get_batched_gen` died with that
                // call.
                let window = unsafe {
                    core::slice::from_raw_parts(inline_ptr, (size * chunk_len_u64) as usize)
                };
                partition_point_in_memory(window, chunk_len, size, &mut pred)
            } else {
                0
            })
    }

    /// Return the index of the first chunk for which `pred` returns `false`,
    /// assuming the view is already partitioned by `pred` — every chunk
    /// `pred` accepts sorts before every chunk it rejects (caller's
    /// responsibility, as with `std`). Mirrors `[T]::partition_point`.
    ///
    /// Reads the whole aligned region in a single [`BStack::get`] call — one
    /// lock acquisition, so the search is atomic as a whole against
    /// concurrent mutation, on the same terms as [`is_sorted_by`](Self::is_sorted_by)
    /// — then searches in memory. No feature flag beyond `alloc`: read-only.
    /// Unlike the `atomic`-feature version of this method, this trades the
    /// O(log n) chunk-read bound for O(n) memory (the whole region is
    /// materialized once) in exchange for not needing `atomic`.
    ///
    /// `Err` propagates an I/O failure from reading the region.
    #[cfg(not(feature = "atomic"))]
    pub fn partition_point(&self, mut pred: impl FnMut(&[u8]) -> bool) -> io::Result<u64> {
        let chunk_len = self.chunk_len as usize;
        let buf = self.aligned.read()?;
        Ok(partition_point_in_memory(
            &buf,
            chunk_len,
            self.chunk_count(),
            &mut pred,
        ))
    }

    /// Returns `true` if every chunk compares `<=` the chunk after it, per
    /// `cmp` — i.e. the view is already sorted by `cmp`.
    ///
    /// Reads the whole aligned region in a single [`BStack::get`] call — one
    /// lock acquisition, so the scan is atomic as a whole against concurrent
    /// mutation — then compares chunks pairwise in memory. No feature flag
    /// beyond `alloc`: read-only. Unlike the O(log n)-probe searches above,
    /// this already visits every chunk, so materializing the region once
    /// costs no more I/O than reading it piecewise would, and buys the
    /// atomicity for free; unlike [`sort_partial_by`](Self::sort_partial_by),
    /// there is no bounded-memory counterpart for regions too large for one
    /// buffer.
    ///
    /// `Err` propagates an I/O failure from reading the region.
    pub fn is_sorted_by(&self, mut cmp: impl FnMut(&[u8], &[u8]) -> Ordering) -> io::Result<bool> {
        let count = self.chunk_count();
        if count < 2 {
            return Ok(true);
        }
        let chunk_len = self.chunk_len as usize;
        let buf = self.aligned.read()?;
        Ok(buf
            .chunks_exact(chunk_len)
            .zip(buf.chunks_exact(chunk_len).skip(1))
            .all(|(a, b)| cmp(a, b) != Ordering::Greater))
    }

    /// Partition chunks in place so the chunk at `n` is in the position it
    /// would occupy if the view were fully sorted by `cmp`; order on either
    /// side of `n` is unspecified. Mirrors `[T]::select_nth_unstable_by`.
    ///
    /// Same single-operation atomicity as [`sort_by`](Self::sort_by): a
    /// crash leaves either the original order or a valid completed
    /// partition, never a half-applied one.
    ///
    /// Requires the `set` and `atomic` features.
    ///
    /// # Panics
    ///
    /// Panics if `n >= self.chunk_count()`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[track_caller]
    pub fn select_nth_by(
        &mut self,
        n: u64,
        mut cmp: impl FnMut(&[u8], &[u8]) -> Ordering,
    ) -> io::Result<()> {
        assert!(
            n < self.chunk_count(),
            "select_nth_by: n must be < chunk_count"
        );
        let chunk_len = self.chunk_len as usize;
        let start = self.aligned.start();
        let end = self.aligned.end();
        self.aligned.stack().process(start, end, |buf| {
            let count = buf.len() / chunk_len;
            let mut order: Vec<usize> = (0..count).collect();
            order.select_nth_unstable_by(n as usize, |&i, &j| {
                let a = &buf[i * chunk_len..(i + 1) * chunk_len];
                let b = &buf[j * chunk_len..(j + 1) * chunk_len];
                cmp(a, b)
            });
            apply_chunk_permutation(buf, chunk_len, &order);
        })
    }

    /// Key-extracting variant of [`select_nth_by`](Self::select_nth_by). The
    /// key is computed once per chunk before partitioning.
    ///
    /// Requires the `set` and `atomic` features.
    ///
    /// # Panics
    ///
    /// Panics if `n >= self.chunk_count()`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[track_caller]
    pub fn select_nth_by_key<K: Ord>(
        &mut self,
        n: u64,
        mut key: impl FnMut(&[u8]) -> K,
    ) -> io::Result<()> {
        assert!(
            n < self.chunk_count(),
            "select_nth_by_key: n must be < chunk_count"
        );
        let chunk_len = self.chunk_len as usize;
        let start = self.aligned.start();
        let end = self.aligned.end();
        self.aligned.stack().process(start, end, |buf| {
            let keys: Vec<K> = buf.chunks_exact(chunk_len).map(&mut key).collect();
            let mut order: Vec<usize> = (0..keys.len()).collect();
            order.select_nth_unstable_by(n as usize, |&i, &j| keys[i].cmp(&keys[j]));
            apply_chunk_permutation(buf, chunk_len, &order);
        })
    }

    /// Best-effort, bounded-memory, out-of-core sort of the chunks in place.
    ///
    /// Unlike [`sort_by`](Self::sort_by), which reads the whole region into one
    /// `Vec<u8>` and commits it in a single [`BStack::process`] call —
    /// bounded by available memory, so a region too large for one `Vec<u8>`
    /// cannot be sorted that way — this runs an **in-place bottom-up merge
    /// sort** that never holds more than a fixed budget of bytes resident
    /// (`SORT_WINDOWS * SORT_BLOCK_BYTES`, chosen internally — no knob), so peak
    /// memory is O(1) in the region's total size. Runs are formed with
    /// [`BStack::process`] and merged in place by a recursive rotation merge —
    /// [`BStack::process`] for sub-ranges that fit the budget, plus
    /// [`BStack::cross_exchange`] record swaps to rotate larger ranges; no
    /// scratch region is used.
    ///
    /// # "Partial" / convergence
    ///
    /// On success the region is **fully sorted**. Every step — each run-forming
    /// sort, each block swap, each merge window — is an independent crash-atomic
    /// operation that only permutes the bytes it touches, so a crash mid-run or
    /// an early return on I/O error leaves the region as some *valid
    /// permutation* of the original records (never lost or duplicated data),
    /// just not fully ordered. "Partial" names that interruptible middle state:
    /// re-running from any such permutation completes the sort, so as long as
    /// I/O eventually succeeds, repeated calls converge to fully sorted.
    ///
    /// Returns `Err` only on a genuine [`BStack::process`]/[`BStack::cross_exchange`]
    /// I/O failure. Not guaranteed stable — the out-of-core merge may reorder
    /// records that compare equal (use [`sort_by`](Self::sort_by) when the whole
    /// region fits memory and stability is required).
    ///
    /// Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[inline]
    pub fn sort_partial_by(
        &mut self,
        mut cmp: impl FnMut(&[u8], &[u8]) -> Ordering,
    ) -> io::Result<()> {
        merge_sort(&self.aligned, self.chunk_len, &mut cmp)
    }

    /// Key-extracting variant of [`sort_partial_by`](Self::sort_partial_by).
    ///
    /// Same bounded-memory, out-of-core, converging, per-step-atomic behavior;
    /// the key is derived from each chunk's bytes per comparison. Requires the
    /// `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[inline]
    pub fn sort_partial_by_key<K: Ord>(
        &mut self,
        mut key: impl FnMut(&[u8]) -> K,
    ) -> io::Result<()> {
        merge_sort(&self.aligned, self.chunk_len, &mut |a, b| {
            key(a).cmp(&key(b))
        })
    }

    /// Best-effort, bounded-memory, out-of-core selection: partition the chunks
    /// in place so the chunk at `n` is the one a full sort by `cmp` would put
    /// there, with every earlier chunk `<=` it and every later chunk `>=` it —
    /// the same result as [`select_nth_by`](Self::select_nth_by), computed
    /// without holding the whole region in memory.
    ///
    /// Unlike [`select_nth_by`](Self::select_nth_by), which runs
    /// `select_nth_unstable_by` on an in-memory copy under one
    /// [`BStack::process`], this is a bounded-memory quickselect: it narrows an
    /// active record range around `n`, choosing each pivot from a cross-region
    /// sample and partitioning in place with atomic [`BStack::cross_exchange`]
    /// swaps, until the range fits the budget and one `process` settles it
    /// exactly. Peak memory is O(1) in the region's total size; no scratch
    /// region is used.
    ///
    /// Every step is crash-atomic and permutation-preserving, so a crash or an
    /// early I/O-error return leaves the region as a valid permutation (never
    /// lost or duplicated data) and re-running completes the selection. Returns
    /// `Err` only on a genuine [`BStack::process`]/[`BStack::cross_exchange`]
    /// I/O failure. See `algos/SORTSELECT.md`.
    ///
    /// Requires the `set` and `atomic` features.
    ///
    /// # Panics
    ///
    /// Panics if `n >= self.chunk_count()`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[track_caller]
    pub fn select_nth_partial_by(
        &mut self,
        n: u64,
        mut cmp: impl FnMut(&[u8], &[u8]) -> Ordering,
    ) -> io::Result<()> {
        assert!(
            n < self.chunk_count(),
            "select_nth_partial_by: n must be < chunk_count"
        );
        select_nth(&self.aligned, self.chunk_len, n, &mut cmp)
    }

    /// Key-extracting variant of
    /// [`select_nth_partial_by`](Self::select_nth_partial_by). Same
    /// bounded-memory, out-of-core, per-step-atomic behavior; the key is derived
    /// from each chunk's bytes per comparison. Requires the `set` and `atomic`
    /// features.
    ///
    /// # Panics
    ///
    /// Panics if `n >= self.chunk_count()`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[track_caller]
    pub fn select_nth_partial_by_key<K: Ord>(
        &mut self,
        n: u64,
        mut key: impl FnMut(&[u8]) -> K,
    ) -> io::Result<()> {
        assert!(
            n < self.chunk_count(),
            "select_nth_partial_by_key: n must be < chunk_count"
        );
        select_nth(&self.aligned, self.chunk_len, n, &mut |a, b| {
            key(a).cmp(&key(b))
        })
    }
}

impl<'a> IntoIterator for BStackChunk<'a> {
    type Item = BStackSlice<'a>;
    type IntoIter = BStackChunkIter<'a>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        BStackChunkIter {
            remaining: self.aligned,
            chunk_len: self.chunk_len,
        }
    }
}

impl<'a> IntoIterator for &BStackChunk<'a> {
    type Item = BStackSlice<'a>;
    type IntoIter = BStackChunkIter<'a>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Sort `buf`, treated as consecutive `chunk_len`-byte records, in place.
///
/// Sorts a proxy index array by `cmp` (stable, per `[usize]::sort_by`) then
/// applies the resulting permutation to `buf` in one pass, rather than
/// moving records during comparison.
#[cfg(all(feature = "set", feature = "atomic"))]
fn sort_chunks_by(buf: &mut [u8], chunk_len: usize, cmp: &mut dyn FnMut(&[u8], &[u8]) -> Ordering) {
    let count = buf.len() / chunk_len;
    if count <= 1 {
        return;
    }
    let mut order: Vec<usize> = (0..count).collect();
    order.sort_by(|&i, &j| {
        let a = &buf[i * chunk_len..(i + 1) * chunk_len];
        let b = &buf[j * chunk_len..(j + 1) * chunk_len];
        cmp(a, b)
    });
    apply_chunk_permutation(buf, chunk_len, &order);
}

/// Reverse the order of `buf`'s `chunk_len`-byte records in place, in a
/// single pass of whole-record swaps — no scratch allocation beyond what
/// [`slice::swap_with_slice`] needs internally.
#[cfg(all(feature = "set", feature = "atomic"))]
fn reverse_chunks(buf: &mut [u8], chunk_len: usize) {
    let count = buf.len() / chunk_len;
    for i in 0..count / 2 {
        let j = count - 1 - i;
        let (left, right) = buf.split_at_mut(j * chunk_len);
        left[i * chunk_len..(i + 1) * chunk_len].swap_with_slice(&mut right[..chunk_len]);
    }
}

/// Shared tail of `binary_search_by`: search `count` `chunk_len`-byte records
/// in `buf` already read into memory. Used by the `not(atomic)` path
/// unconditionally and by the `atomic` path's small-region fast case.
fn binary_search_in_memory(
    buf: &[u8],
    chunk_len: usize,
    count: u64,
    cmp: &mut dyn FnMut(&[u8]) -> Ordering,
) -> Result<u64, u64> {
    let mut size = count;
    let mut left = 0u64;
    while size > 0 {
        let half = size / 2;
        let mid = left + half;
        let s = (mid as usize) * chunk_len;
        match cmp(&buf[s..s + chunk_len]) {
            Ordering::Less => {
                left = mid + 1;
                size -= half + 1;
            }
            Ordering::Greater => size = half,
            Ordering::Equal => return Ok(mid),
        }
    }
    Err(left)
}

/// Shared tail of `partition_point`: search `count` `chunk_len`-byte records
/// in `buf` already read into memory. Used by the `not(atomic)` path
/// unconditionally and by the `atomic` path's small-region fast case.
fn partition_point_in_memory(
    buf: &[u8],
    chunk_len: usize,
    count: u64,
    pred: &mut dyn FnMut(&[u8]) -> bool,
) -> u64 {
    let mut size = count;
    let mut left = 0u64;
    while size > 0 {
        let half = size / 2;
        let mid = left + half;
        let s = (mid as usize) * chunk_len;
        if pred(&buf[s..s + chunk_len]) {
            left = mid + 1;
            size -= half + 1;
        } else {
            size = half;
        }
    }
    left
}

/// Reorder `buf`'s `chunk_len`-byte records so record `order[dest]` ends up
/// at position `dest`, in place via cycle-following.
///
/// Only two scratch allocations exist for the whole call: `visited` (one
/// bool per record) and a single `chunk_len`-byte swap buffer — never a
/// second copy of the whole region. The swap buffer is stack-allocated for
/// `chunk_len <= INLINE_SCRATCH_LEN`.
#[cfg(all(feature = "set", feature = "atomic"))]
fn apply_chunk_permutation(buf: &mut [u8], chunk_len: usize, order: &[usize]) {
    let mut inline = [0u8; INLINE_SCRATCH_LEN];
    let mut heap;
    let temp: &mut [u8] = if chunk_len <= INLINE_SCRATCH_LEN {
        &mut inline[..chunk_len]
    } else {
        heap = vec![0u8; chunk_len];
        &mut heap[..]
    };

    let mut visited = vec![false; order.len()];
    for start in 0..order.len() {
        if visited[start] || order[start] == start {
            visited[start] = true;
            continue;
        }
        temp.copy_from_slice(&buf[start * chunk_len..(start + 1) * chunk_len]);
        let mut cur = start;
        while order[cur] != start {
            let src = order[cur];
            buf.copy_within(src * chunk_len..(src + 1) * chunk_len, cur * chunk_len);
            visited[cur] = true;
            cur = src;
        }
        buf[cur * chunk_len..(cur + 1) * chunk_len].copy_from_slice(temp);
        visited[cur] = true;
    }
}

// ===================== Out-of-core in-place merge sort ======================
//
// Engine for `BStackChunk::sort_partial_by`/`sort_partial_by_key`. Bottom-up
// 2-way merge sort that never holds more than `SORT_WINDOWS * SORT_BLOCK_BYTES`
// bytes resident: runs are formed with `BStack::process`, and adjacent runs are
// merged in place by a recursive rotation merge (`imerge`) — sub-ranges that
// fit the budget are sorted by one atomic `BStack::process`, and larger ranges
// are split and rotated past each other with atomic `BStack::cross_exchange`
// record swaps. Every remote op is individually crash-atomic and
// permutation-preserving, so any crash or early I/O-error return leaves a valid
// permutation of the records, and re-running completes the sort.

/// Internal record-geometry view shared by the out-of-core sort/select engines:
/// the target region plus the record stride in bytes (`c`) and as `usize`
/// (`cu`). `Copy` — three cheap fields threaded through the engine in place of
/// repeating them (and the low-level record ops) as arguments. Every method's
/// index arithmetic stays within [`count`](Self::count) (a record count of an
/// existing region, `<= 2^63`), so nothing can overflow for any real file;
/// `c >= 1` (the `BStackChunk` invariant) keeps the divisions safe.
#[cfg(all(feature = "set", feature = "atomic"))]
#[derive(Clone, Copy)]
struct Records<'s, 'a> {
    aligned: &'s BStackSlice<'a>,
    c: u64,
    cu: usize,
}

#[cfg(all(feature = "set", feature = "atomic"))]
impl<'s, 'a> Records<'s, 'a> {
    #[inline]
    fn new(aligned: &'s BStackSlice<'a>, c: u64) -> Self {
        Records {
            aligned,
            c,
            cu: c as usize,
        }
    }

    /// Number of whole records in the region.
    #[inline]
    fn count(&self) -> u64 {
        self.aligned.len() / self.c
    }

    /// Absolute byte offset of record `rec`.
    #[inline(always)]
    fn off(&self, rec: u64) -> u64 {
        self.aligned.start() + rec * self.c
    }

    /// Read record `rec` into `buf` (`buf.len() == c`).
    #[inline(always)]
    fn read(&self, rec: u64, buf: &mut [u8]) -> io::Result<()> {
        let s = rec * self.c;
        self.aligned.subslice(s, s + self.c).read_into(buf)
    }

    /// Atomically swap the single records at `i` and `j` (which must differ).
    #[inline(always)]
    fn swap(&self, i: u64, j: u64) -> io::Result<()> {
        self.aligned
            .stack()
            .cross_exchange(self.off(i), self.off(j), self.c)
    }

    /// Atomically read the record range `[lo, hi)`, permute it in memory via
    /// `f`, and commit it — one crash-atomic operation.
    #[inline(always)]
    fn process(&self, lo: u64, hi: u64, f: impl FnOnce(&mut [u8])) -> io::Result<()> {
        self.aligned.stack().process(self.off(lo), self.off(hi), f)
    }

    /// Reverse the records in `[x, y)` via single-record atomic swaps.
    #[inline(always)] // only called by `rotate`
    fn reverse(&self, mut x: u64, mut y: u64) -> io::Result<()> {
        while y > x + 1 {
            let b = y - 1;
            self.swap(x, b)?;
            x += 1;
            y = b;
        }
        Ok(())
    }

    /// Rotate: swap the adjacent record blocks `[a, b)` and `[b, d)` via three
    /// reversals. Crash-safe: each underlying swap is atomic, so any crash
    /// leaves a valid permutation.
    #[inline(always)] // only called by `imerge`
    fn rotate(&self, a: u64, b: u64, d: u64) -> io::Result<()> {
        self.reverse(a, b)?;
        self.reverse(b, d)?;
        self.reverse(a, d)
    }

    /// First index in `[lo, hi)` whose record is *not less than* `key`.
    #[inline(always)] // only called by `imerge`
    fn lower_bound(
        &self,
        mut lo: u64,
        mut hi: u64,
        key: &[u8],
        scratch: &mut [u8],
        cmp: &mut dyn FnMut(&[u8], &[u8]) -> Ordering,
    ) -> io::Result<u64> {
        while lo < hi {
            let m = lo + (hi - lo) / 2;
            self.read(m, scratch)?;
            if cmp(scratch, key) == Ordering::Less {
                lo = m + 1;
            } else {
                hi = m;
            }
        }
        Ok(lo)
    }

    /// First index in `[lo, hi)` whose record is *greater than* `key`.
    #[inline(always)] // only called by `imerge`
    fn upper_bound(
        &self,
        mut lo: u64,
        mut hi: u64,
        key: &[u8],
        scratch: &mut [u8],
        cmp: &mut dyn FnMut(&[u8], &[u8]) -> Ordering,
    ) -> io::Result<u64> {
        while lo < hi {
            let m = lo + (hi - lo) / 2;
            self.read(m, scratch)?;
            if cmp(scratch, key) == Ordering::Greater {
                hi = m;
            } else {
                lo = m + 1;
            }
        }
        Ok(lo)
    }

    /// Merge the adjacent sorted runs `[lo0, mid0)` and `[mid0, hi0)` in place.
    ///
    /// A rotation merge (SymMerge): a sub-range that fits [`SORT_BUDGET`] is
    /// merged by one atomic [`process`](Self::process) sort; a larger range is
    /// split by binary-searching a pivot from the larger run and rotating the two
    /// middle blocks past each other, then the two halves are merged the same
    /// way. Every rotation is a sequence of atomic single-record swaps and every
    /// base case is one atomic `process`, so O(1) records held resident (beyond
    /// `ms`) and any crash leaves a valid permutation. Handles arbitrary
    /// (including ragged) run lengths.
    ///
    /// Iterative, not recursive: the deferred half of each split is pushed onto
    /// `ms.stack` (cleared on entry) and the smaller half is continued in place.
    /// Since the halves partition the range exactly (`l_size + r_size = hi - lo`),
    /// the continued half is always `<= (hi - lo) / 2`, so `ms.stack` holds at
    /// most `⌈log2(hi - lo)⌉` entries — at most 63 for any region a u64-addressed
    /// file can hold (`hi - lo <= count <= 2^63`). No call-stack recursion.
    fn imerge(
        &self,
        lo0: u64,
        mid0: u64,
        hi0: u64,
        ms: &mut MergeScratch,
        cmp: &mut dyn FnMut(&[u8], &[u8]) -> Ordering,
    ) -> io::Result<()> {
        let cu = self.cu;
        ms.stack.clear();
        let (mut lo, mut mid, mut hi) = (lo0, mid0, hi0);
        loop {
            // Descend, splitting until the range is a base case. `hi - lo > 2`
            // guarantees `llen >= 2` in the `llen >= rlen` branch (so `i > lo`)
            // and is the base for records so wide that two exceed the budget;
            // with pivoting the larger run, every split leaves both halves
            // strictly smaller, so this terminates.
            while lo < mid && mid < hi && hi - lo > 2 && (hi - lo) * self.c > SORT_BUDGET {
                let (llen, rlen) = (mid - lo, hi - mid);
                let (i, j);
                if llen >= rlen {
                    i = lo + llen / 2; // i in (lo, mid): llen >= 2 here
                    self.read(i, &mut ms.bi)?; // pivot bytes held in `ms.bi`
                    j = self.lower_bound(mid, hi, &ms.bi, &mut ms.bj, cmp)?;
                } else {
                    j = mid + rlen / 2; // j in (mid, hi): rlen >= 2 here
                    self.read(j, &mut ms.bj)?; // pivot bytes held in `ms.bj`
                    i = self.upper_bound(lo, mid, &ms.bj, &mut ms.bi, cmp)?;
                }
                // Skip a no-op rotation (one side of the pivot is empty).
                if i < mid && mid < j {
                    self.rotate(i, mid, j)?;
                }
                let new_mid = i + (j - mid); // in [lo, hi]
                let (l_lo, l_mid, l_hi) = (lo, i, new_mid);
                let (r_lo, r_mid, r_hi) = (new_mid, j, hi);
                // Continue on the smaller half; defer the larger — bounds the stack.
                if l_hi - l_lo <= r_hi - r_lo {
                    ms.stack.push((r_lo, r_mid, r_hi));
                    lo = l_lo;
                    mid = l_mid;
                    hi = l_hi;
                } else {
                    ms.stack.push((l_lo, l_mid, l_hi));
                    lo = r_lo;
                    mid = r_mid;
                    hi = r_hi;
                }
            }
            // Base case: two already-sorted runs that fit the budget (or a 1-2
            // record range) — one atomic sort settles them. Degenerate ranges
            // (an empty side) fall through to the pop.
            if lo < mid && mid < hi {
                self.process(lo, hi, |buf| sort_chunks_by(buf, cu, &mut *cmp))?;
            }
            match ms.stack.pop() {
                Some((l, m, h)) => {
                    lo = l;
                    mid = m;
                    hi = h;
                }
                None => return Ok(()),
            }
        }
    }

    /// Choose a pivot record index in `[lo, hi)`: read a strided sample into
    /// `samp` and return the index of its median. `order` is reused scratch.
    fn sample_pivot(
        &self,
        lo: u64,
        hi: u64,
        samp: &mut [u8],
        order: &mut Vec<usize>,
        cmp: &mut dyn FnMut(&[u8], &[u8]) -> Ordering,
    ) -> io::Result<u64> {
        let cu = self.cu;
        let range = hi - lo;
        // As many samples as fit `samp`, but never more than the range holds.
        let s = ((samp.len() / cu) as u64).clamp(1, range);
        let stride = range / s; // >= 1: partition runs only when range > s
        for t in 0..s {
            let pos = lo + stride * t;
            self.read(pos, &mut samp[(t as usize) * cu..(t as usize + 1) * cu])?;
        }
        order.clear();
        order.extend(0..s as usize);
        order.sort_by(|&a, &b| cmp(&samp[a * cu..(a + 1) * cu], &samp[b * cu..(b + 1) * cu]));
        let med = order[order.len() / 2] as u64;
        Ok(lo + stride * med)
    }

    /// Lomuto partition of `[lo, hi)` around the record at `piv_idx`, in place via
    /// atomic single-record swaps. Returns the pivot's final index `p`: `[lo, p)`
    /// compare `< pivot`, `[p + 1, hi)` compare `>= pivot`, and the pivot record
    /// sits at `p` in its final position. Requires `hi - lo >= 2`.
    fn partition(
        &self,
        lo: u64,
        hi: u64,
        piv_idx: u64,
        pbuf: &mut [u8],
        one: &mut [u8],
        cmp: &mut dyn FnMut(&[u8], &[u8]) -> Ordering,
    ) -> io::Result<u64> {
        let last = hi - 1;
        self.read(piv_idx, pbuf)?; // pivot bytes captured before moving
        if piv_idx != last {
            self.swap(piv_idx, last)?;
        }
        let mut store = lo;
        let mut k = lo;
        while k < last {
            self.read(k, one)?;
            if cmp(one, pbuf) == Ordering::Less {
                if k != store {
                    self.swap(store, k)?;
                }
                store += 1;
            }
            k += 1;
        }
        if store != last {
            self.swap(store, last)?;
        }
        Ok(store)
    }
}

/// Reusable scratch for [`Records::imerge`]: two record-sized compare buffers
/// and the deferred-work stack. The stack is bounded to `⌈log2(count)⌉` entries
/// (<= 63 for any u64-addressed file); at ~24 bytes/entry a fixed array would be
/// ~1.5 KiB, too large for the call stack, so it is a lazily-grown heap `Vec`
/// reused across every merge.
#[cfg(all(feature = "set", feature = "atomic"))]
struct MergeScratch {
    bi: Vec<u8>,
    bj: Vec<u8>,
    stack: Vec<(u64, u64, u64)>,
}

/// Out-of-core in-place bottom-up merge sort of `aligned`, viewed as records of
/// `c` bytes. See `BStackChunk::sort_partial_by`.
#[cfg(all(feature = "set", feature = "atomic"))]
fn merge_sort(
    aligned: &BStackSlice<'_>,
    c: u64,
    cmp: &mut dyn FnMut(&[u8], &[u8]) -> Ordering,
) -> io::Result<()> {
    let r = Records::new(aligned, c);
    let count = r.count();
    if count <= 1 {
        return Ok(());
    }
    let k = (SORT_BLOCK_BYTES / c).max(1);
    let run0 = SORT_WINDOWS * k;
    let mut ms = MergeScratch {
        bi: vec![0u8; r.cu],
        bj: vec![0u8; r.cu],
        stack: Vec::new(),
    };
    // Phase 1: run formation — sort each `run0`-record window. Subtraction-form
    // comparisons keep every index within `count`, so nothing overflows.
    let mut p = 0u64;
    while p < count {
        let q = if count - p > run0 { p + run0 } else { count };
        if q - p > 1 {
            r.process(p, q, |buf| sort_chunks_by(buf, r.cu, &mut *cmp))?;
        }
        p = q;
    }
    // Phase 2: bottom-up merge passes.
    let mut width = run0;
    while width < count {
        let stop = count - width; // > 0 (loop guard)
        let mut lo = 0u64;
        while lo < stop {
            let mid = lo + width; // < count, since lo < count - width
            let hi = if count - mid > width {
                mid + width
            } else {
                count
            };
            r.imerge(lo, mid, hi, &mut ms, cmp)?;
            // Advance two runs; saturate so a near-u64::MAX sum only ends the pass.
            lo = lo.saturating_add(width).saturating_add(width);
        }
        width = width.saturating_mul(2);
    }
    Ok(())
}

// ===================== Out-of-core in-place quickselect =====================
//
// Engine for `BStackChunk::select_nth_partial_by`/`select_nth_partial_by_key`.
// Bounded-memory quickselect: narrow an active range `[lo, hi)` that holds rank
// `n` (every record left of `lo` <= it <= every record right of `hi`), picking
// each pivot from a cross-region sample and partitioning in place with atomic
// swaps, until the range fits the budget and one `process` settles it. Every
// step is crash-atomic and permutation-preserving.

/// Out-of-core in-place selection of rank `n` over `aligned`, viewed as records
/// of `c` bytes. `n < count` is the caller's precondition (asserted in the
/// public methods). See `BStackChunk::select_nth_partial_by`.
#[cfg(all(feature = "set", feature = "atomic"))]
fn select_nth(
    aligned: &BStackSlice<'_>,
    c: u64,
    n: u64,
    cmp: &mut dyn FnMut(&[u8], &[u8]) -> Ordering,
) -> io::Result<()> {
    let r = Records::new(aligned, c);
    let count = r.count();
    if count <= 1 {
        return Ok(());
    }
    // Sized to hold at least one record even when a record exceeds the budget
    // (`c > SORT_BUDGET`), so the pivot sample can always read one in.
    let mut samp = vec![0u8; (SORT_BUDGET as usize).max(r.cu)];
    let mut pbuf = vec![0u8; r.cu];
    let mut one = vec![0u8; r.cu];
    let mut order: Vec<usize> = Vec::new(); // reused pivot-sample scratch
    let (mut lo, mut hi) = (0u64, count);
    loop {
        // A single-record band already holds the answer (rank-band invariant).
        if hi - lo <= 1 {
            return Ok(());
        }
        // Band fits the budget: settle it exactly in one atomic pass. The band
        // invariant makes the local (n - lo)-th record the global n-th.
        if (hi - lo) * c <= SORT_BUDGET {
            let cu = r.cu;
            let local = (n - lo) as usize;
            return r.process(lo, hi, |buf| {
                let cnt = buf.len() / cu;
                let mut ord: Vec<usize> = (0..cnt).collect();
                ord.select_nth_unstable_by(local, |&i, &j| {
                    cmp(&buf[i * cu..(i + 1) * cu], &buf[j * cu..(j + 1) * cu])
                });
                apply_chunk_permutation(buf, cu, &ord);
            });
        }
        let piv = r.sample_pivot(lo, hi, &mut samp, &mut order, cmp)?;
        let p = r.partition(lo, hi, piv, &mut pbuf, &mut one, cmp)?;
        // Recurse into the side holding `n`, excluding the fixed pivot at `p`
        // (strict progress → termination).
        if n < p {
            hi = p;
        } else if n > p {
            lo = p + 1;
        } else {
            return Ok(());
        }
    }
}

impl<'a> BStackSlice<'a> {
    /// Divide this slice into `chunk_len`-byte chunks, aligned from the
    /// start, returning `(chunk_view, remainder)`. Any leftover bytes
    /// (`self.len() % chunk_len`) come back as the *trailing* remainder
    /// slice; it is empty if `chunk_len` evenly divides `self.len()`.
    ///
    /// No I/O — pure offset arithmetic, as cheap as
    /// [`subslice`](Self::subslice).
    ///
    /// # Panics
    ///
    /// Panics if `chunk_len == 0`.
    #[must_use]
    #[track_caller]
    pub fn chunks(&self, chunk_len: u64) -> (BStackChunk<'a>, BStackSlice<'a>) {
        assert!(chunk_len > 0, "chunks: chunk_len must be nonzero");
        let len = self.len();
        let aligned_len = (len / chunk_len) * chunk_len;
        let chunk = BStackChunk {
            aligned: self.subslice(0, aligned_len),
            chunk_len,
        };
        (chunk, self.subslice(aligned_len, len))
    }

    /// Divide this slice into `chunk_len`-byte chunks, aligned from the end,
    /// returning `(chunk_view, remainder)`. Any leftover bytes come back as
    /// the *leading* remainder slice.
    ///
    /// Produces the same [`BStackChunk`] type as [`chunks`](Self::chunks);
    /// the two differ only in which sub-region ends up aligned and which
    /// ends up in the returned remainder.
    ///
    /// No I/O — pure offset arithmetic.
    ///
    /// # Panics
    ///
    /// Panics if `chunk_len == 0`.
    #[must_use]
    #[track_caller]
    pub fn rchunks(&self, chunk_len: u64) -> (BStackChunk<'a>, BStackSlice<'a>) {
        assert!(chunk_len > 0, "rchunks: chunk_len must be nonzero");
        let len = self.len();
        let rem_len = len % chunk_len;
        let chunk = BStackChunk {
            aligned: self.subslice(rem_len, len),
            chunk_len,
        };
        (chunk, self.subslice(0, rem_len))
    }
}

impl<'a, A: BStackAllocator> BStackOwnedSlice<'a, A> {
    /// Borrowed equivalent of [`BStackSlice::chunks`].
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice);
    /// the returned view and remainder's lifetime is tied to `&self`.
    #[inline]
    #[must_use]
    #[track_caller]
    pub fn chunks<'s>(&'s self, chunk_len: u64) -> (BStackChunk<'s>, BStackSlice<'s>) {
        self.as_slice().chunks(chunk_len)
    }

    /// Borrowed equivalent of [`BStackSlice::rchunks`].
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice);
    /// the returned view and remainder's lifetime is tied to `&self`.
    #[inline]
    #[must_use]
    #[track_caller]
    pub fn rchunks<'s>(&'s self, chunk_len: u64) -> (BStackChunk<'s>, BStackSlice<'s>) {
        self.as_slice().rchunks(chunk_len)
    }
}

/// A lazy, zero-I/O iterator over the chunks of a [`BStackChunk`].
///
/// Each step (`next`/`next_back`) is pure offset arithmetic — the same cost
/// as [`BStackSlice::subslice`] — and performs no I/O by itself. Actual bytes
/// are only read when the caller calls `.read()`/`.read_into()` on an
/// individual yielded [`BStackSlice`]; the chunked region is never
/// materialized into memory as a whole by this iterator, regardless of how
/// many chunks it spans.
///
/// Implements [`ExactSizeIterator`]: the chunk count is tracked internally as
/// `u64` and is exact on 64-bit targets. On targets where `usize` is
/// narrower than `u64` (e.g. 32-bit), a chunk count that doesn't fit in
/// `usize` is clamped to `usize::MAX` rather than silently truncated —
/// `size_hint()`/`len()` then under-report, but never wrap to a smaller,
/// wrong value.
///
/// Constructed by [`BStackChunk::iter`] or `IntoIterator`.
pub struct BStackChunkIter<'a> {
    remaining: BStackSlice<'a>,
    chunk_len: u64,
}

impl<'a> Clone for BStackChunkIter<'a> {
    #[inline]
    fn clone(&self) -> Self {
        BStackChunkIter {
            remaining: self.remaining.clone(),
            chunk_len: self.chunk_len,
        }
    }
}

impl<'a> fmt::Debug for BStackChunkIter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BStackChunkIter")
            .field("remaining_len", &self.remaining.len())
            .field("chunk_len", &self.chunk_len)
            .finish_non_exhaustive()
    }
}

impl<'a> Iterator for BStackChunkIter<'a> {
    type Item = BStackSlice<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining.len() < self.chunk_len {
            return None;
        }
        let head = self.remaining.subslice(0, self.chunk_len);
        self.remaining = self
            .remaining
            .subslice(self.chunk_len, self.remaining.len());
        Some(head)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = (self.remaining.len() / self.chunk_len).min(usize::MAX as u64) as usize;
        (n, Some(n))
    }
}

impl<'a> DoubleEndedIterator for BStackChunkIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.remaining.len() < self.chunk_len {
            return None;
        }
        let len = self.remaining.len();
        let tail = self.remaining.subslice(len - self.chunk_len, len);
        self.remaining = self.remaining.subslice(0, len - self.chunk_len);
        Some(tail)
    }
}

impl<'a> ExactSizeIterator for BStackChunkIter<'a> {}

impl<'a> std::iter::FusedIterator for BStackChunkIter<'a> {}
