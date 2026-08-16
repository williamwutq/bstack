//! Fixed-stride chunked view over a [`BStackSlice`].
//!
//! Requires feature `alloc`. Sorting and selection additionally require
//! `set` + `atomic`; construction, iteration, and binary search need no flag
//! beyond `alloc`.

use super::{BStackAllocator, BStackOwnedSlice, BStackSlice};
use crate::BStack;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io;

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

    /// Re-divide this view's aligned region with a different stride,
    /// returning `(chunk_view, remainder)` — equivalent to
    /// `self.into_slice().chunks(new_stride)`.
    ///
    /// # Panics
    ///
    /// Panics if `new_stride == 0`.
    #[inline]
    #[must_use]
    pub fn with_stride(self, new_stride: u64) -> (BStackChunk<'a>, BStackSlice<'a>) {
        self.aligned.chunks(new_stride)
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

    /// Sort chunks in place by `cmp`, comparing each chunk's raw bytes.
    ///
    /// Crash-atomic as a single transaction: a single [`BStack::process`]
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
    /// INLINE_SCRATCH_LEN`), not a fresh allocation per probe. No feature
    /// flag beyond `alloc`: read-only.
    ///
    /// Returns `Ok(Ok(index))` naming a matching chunk, or `Ok(Err(index))`
    /// with the index a matching chunk would need to be inserted at to keep
    /// the chunks ordered. `Err` propagates an I/O failure from probing a
    /// chunk.
    pub fn binary_search_by(
        &self,
        mut cmp: impl FnMut(&[u8]) -> Ordering,
    ) -> io::Result<Result<u64, u64>> {
        let chunk_len = self.chunk_len as usize;
        let mut inline = [0u8; INLINE_SCRATCH_LEN];
        let mut heap;
        let buf: &mut [u8] = if chunk_len <= INLINE_SCRATCH_LEN {
            &mut inline[..chunk_len]
        } else {
            heap = vec![0u8; chunk_len];
            &mut heap[..]
        };

        let mut size = self.chunk_count();
        let mut left = 0u64;
        while size > 0 {
            let half = size / 2;
            let mid = left + half;
            let chunk = self
                .get(mid)
                .expect("binary_search_by: mid computed within bounds");
            chunk.read_into(buf)?;
            match cmp(buf) {
                Ordering::Less => {
                    left = mid + 1;
                    size -= half + 1;
                }
                Ordering::Greater => size = half,
                Ordering::Equal => return Ok(Ok(mid)),
            }
        }
        Ok(Err(left))
    }

    /// Binary search for `target` by a key extracted from each probed
    /// chunk's bytes. See [`binary_search_by`](Self::binary_search_by).
    #[inline]
    pub fn binary_search_by_key<K: Ord>(
        &self,
        target: &K,
        mut key: impl FnMut(&[u8]) -> K,
    ) -> io::Result<Result<u64, u64>> {
        self.binary_search_by(|bytes| key(bytes).cmp(target))
    }

    /// Partition chunks in place so the chunk at `n` is in the position it
    /// would occupy if the view were fully sorted by `cmp`; order on either
    /// side of `n` is unspecified. Mirrors `[T]::select_nth_unstable_by`.
    ///
    /// Same single-transaction atomicity as [`sort_by`](Self::sort_by): a
    /// crash leaves either the original order or a valid completed
    /// partition, never a half-applied one.
    ///
    /// Requires the `set` and `atomic` features.
    ///
    /// # Panics
    ///
    /// Panics if `n >= self.chunk_count()`.
    #[cfg(all(feature = "set", feature = "atomic"))]
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

/// Chunk-sized scratch buffers up to this many bytes live on the stack;
/// larger ones fall back to a single heap allocation. Covers the common case
/// (short fixed-width records) without allocating at all.
const INLINE_SCRATCH_LEN: usize = 128;

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
    pub fn chunks<'s>(&'s self, chunk_len: u64) -> (BStackChunk<'s>, BStackSlice<'s>) {
        self.as_slice().chunks(chunk_len)
    }

    /// Borrowed equivalent of [`BStackSlice::rchunks`].
    ///
    /// Internally borrows a [`BStackSlice`] via [`as_slice`](Self::as_slice);
    /// the returned view and remainder's lifetime is tied to `&self`.
    #[inline]
    #[must_use]
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
