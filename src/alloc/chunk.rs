//! Fixed-stride chunked view over a [`BStackSlice`].
//!
//! Requires feature `alloc`. Sorting and selection additionally require
//! `set` + `atomic`; construction, iteration, and binary search need no flag
//! beyond `alloc`.

#[cfg(all(feature = "set", feature = "atomic"))]
use super::BStackOwnedSliceAllocator;
use super::{BStackAllocator, BStackOwnedSlice, BStackSlice};
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
    /// `Vec<u8>` and commits it in a single [`BStack::process`] transaction —
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

/// Absolute byte offset of record `rec` within `aligned`.
#[cfg(all(feature = "set", feature = "atomic"))]
#[inline(always)]
fn rec_off(aligned: &BStackSlice<'_>, rec: u64, c: u64) -> u64 {
    aligned.start() + rec * c
}

/// Read record `rec` into `buf` (`buf.len() == c`).
#[cfg(all(feature = "set", feature = "atomic"))]
#[inline(always)]
fn read_record(aligned: &BStackSlice<'_>, rec: u64, c: u64, buf: &mut [u8]) -> io::Result<()> {
    let s = rec * c;
    aligned.subslice(s, s + c).read_into(buf)
}

/// Reverse the records in `[x, y)` in place via single-record atomic swaps.
#[cfg(all(feature = "set", feature = "atomic"))]
#[inline(always)] // Inline because only used in `rotate_records`
fn reverse_records(aligned: &BStackSlice<'_>, c: u64, mut x: u64, mut y: u64) -> io::Result<()> {
    while y > x + 1 {
        let b = y - 1;
        aligned
            .stack()
            .cross_exchange(rec_off(aligned, x, c), rec_off(aligned, b, c), c)?;
        x += 1;
        y = b;
    }
    Ok(())
}

/// Swap the two adjacent record blocks `[a, b)` and `[b, d)` (rotation) via
/// three reversals. Crash-safe: each underlying swap is atomic, so any crash
/// leaves a valid permutation.
#[cfg(all(feature = "set", feature = "atomic"))]
#[inline]
fn rotate_records(aligned: &BStackSlice<'_>, c: u64, a: u64, b: u64, d: u64) -> io::Result<()> {
    reverse_records(aligned, c, a, b)?;
    reverse_records(aligned, c, b, d)?;
    reverse_records(aligned, c, a, d)?;
    Ok(())
}

/// First index in `[lo, hi)` whose record is *not less than* `key` (lower bound).
#[cfg(all(feature = "set", feature = "atomic"))]
#[inline(always)] // Inline because only used in `imerge`
fn lower_bound(
    aligned: &BStackSlice<'_>,
    c: u64,
    mut lo: u64,
    mut hi: u64,
    key: &[u8],
    scratch: &mut [u8],
    cmp: &mut dyn FnMut(&[u8], &[u8]) -> Ordering,
) -> io::Result<u64> {
    while lo < hi {
        let m = lo + (hi - lo) / 2;
        read_record(aligned, m, c, scratch)?;
        if cmp(scratch, key) == Ordering::Less {
            lo = m + 1;
        } else {
            hi = m;
        }
    }
    Ok(lo)
}

/// First index in `[lo, hi)` whose record is *greater than* `key` (upper bound).
#[cfg(all(feature = "set", feature = "atomic"))]
#[inline(always)] // Inline because only used in `imerge`
fn upper_bound(
    aligned: &BStackSlice<'_>,
    c: u64,
    mut lo: u64,
    mut hi: u64,
    key: &[u8],
    scratch: &mut [u8],
    cmp: &mut dyn FnMut(&[u8], &[u8]) -> Ordering,
) -> io::Result<u64> {
    while lo < hi {
        let m = lo + (hi - lo) / 2;
        read_record(aligned, m, c, scratch)?;
        if cmp(scratch, key) == Ordering::Greater {
            hi = m;
        } else {
            lo = m + 1;
        }
    }
    Ok(lo)
}

/// Merge the adjacent sorted runs `[lo, mid)` and `[mid, hi)` in place.
///
/// A rotation merge (SymMerge): a sub-range that fits the memory budget is
/// merged by one atomic [`BStack::process`] sort; a larger range is split by
/// binary-searching a pivot from the larger run and rotating the two middle
/// blocks past each other with [`BStack::cross_exchange`], then the two halves
/// are merged the same way. Every rotation is a sequence of atomic single-record
/// swaps and every base case is one atomic `process`, so O(1) records held
/// resident (beyond the two scratch records) and any crash leaves a valid
/// permutation. Handles arbitrary (including ragged) run lengths.
///
/// Iterative, not recursive: the deferred half of each split is pushed onto
/// `stack` (cleared on entry, reused across calls) and the smaller half is
/// continued in place, so `stack` never holds more than `O(log(hi - lo))`
/// entries and there is no call-stack recursion to overflow.
#[cfg(all(feature = "set", feature = "atomic"))]
#[allow(clippy::too_many_arguments)]
fn imerge(
    aligned: &BStackSlice<'_>,
    c: u64,
    cu: usize,
    budget: u64,
    lo0: u64,
    mid0: u64,
    hi0: u64,
    bi: &mut [u8],
    bj: &mut [u8],
    stack: &mut Vec<(u64, u64, u64)>,
    cmp: &mut dyn FnMut(&[u8], &[u8]) -> Ordering,
) -> io::Result<()> {
    stack.clear();
    let (mut lo, mut mid, mut hi) = (lo0, mid0, hi0);
    loop {
        // Descend, splitting until the range is a base case. `hi - lo > 2`
        // guarantees `llen >= 2` in the `llen >= rlen` branch (so `i > lo`) and
        // is the base for records so wide that two of them exceed the budget;
        // combined with pivoting the larger run, every split leaves both halves
        // strictly smaller, so this terminates.
        while lo < mid && mid < hi && hi - lo > 2 && (hi - lo) * c > budget {
            let (llen, rlen) = (mid - lo, hi - mid);
            let (i, j);
            if llen >= rlen {
                i = lo + llen / 2; // i in (lo, mid): llen >= 2 here
                read_record(aligned, i, c, bi)?; // pivot bytes held in `bi`
                j = lower_bound(aligned, c, mid, hi, bi, bj, cmp)?;
            } else {
                j = mid + rlen / 2; // j in (mid, hi): rlen >= 2 here
                read_record(aligned, j, c, bj)?; // pivot bytes held in `bj`
                i = upper_bound(aligned, c, lo, mid, bj, bi, cmp)?;
            }
            // Skip a no-op rotation (one side of the pivot is empty).
            if i < mid && mid < j {
                rotate_records(aligned, c, i, mid, j)?;
            }
            let new_mid = i + (j - mid); // in [lo, hi]; see arithmetic note
            let (l_lo, l_mid, l_hi) = (lo, i, new_mid);
            let (r_lo, r_mid, r_hi) = (new_mid, j, hi);
            // Continue on the smaller half; defer the larger — bounds `stack`.
            if l_hi - l_lo <= r_hi - r_lo {
                stack.push((r_lo, r_mid, r_hi));
                lo = l_lo;
                mid = l_mid;
                hi = l_hi;
            } else {
                stack.push((l_lo, l_mid, l_hi));
                lo = r_lo;
                mid = r_mid;
                hi = r_hi;
            }
        }
        // Base case: two already-sorted runs that fit the budget (or a 1–2
        // record range) — one atomic sort settles them. Degenerate ranges
        // (an empty side) fall through to the pop.
        if lo < mid && mid < hi {
            let s = rec_off(aligned, lo, c);
            let e = rec_off(aligned, hi, c);
            aligned
                .stack()
                .process(s, e, |buf| sort_chunks_by(buf, cu, &mut *cmp))?;
        }
        match stack.pop() {
            Some((l, m, h)) => {
                lo = l;
                mid = m;
                hi = h;
            }
            None => return Ok(()),
        }
    }
}

/// Out-of-core in-place bottom-up merge sort of `aligned`, viewed as records of
/// `c` bytes. See the module-level note above and `BStackChunk::sort_partial_by`.
#[cfg(all(feature = "set", feature = "atomic"))]
fn merge_sort(
    aligned: &BStackSlice<'_>,
    c: u64,
    cmp: &mut dyn FnMut(&[u8], &[u8]) -> Ordering,
) -> io::Result<()> {
    let count = aligned.len() / c;
    if count <= 1 {
        return Ok(());
    }
    let cu = c as usize;
    // `c >= 1` (BStackChunk invariant), so the divisions are safe. `run0 * c <=
    // SORT_WINDOWS * SORT_BLOCK_BYTES` fits `u64`; all index arithmetic below
    // stays within `count` (record count of an existing region) via
    // subtraction-form comparisons, so nothing can overflow for any real file.
    let k = (SORT_BLOCK_BYTES / c).max(1);
    let budget = SORT_WINDOWS * SORT_BLOCK_BYTES;
    let run0 = SORT_WINDOWS * k;
    let mut bi = vec![0u8; cu];
    let mut bj = vec![0u8; cu];
    let mut stack: Vec<(u64, u64, u64)> = Vec::new();
    // Phase 1: run formation — sort each `run0`-record window.
    let mut p = 0u64;
    while p < count {
        let q = if count - p > run0 { p + run0 } else { count };
        if q - p > 1 {
            let s = rec_off(aligned, p, c);
            let e = rec_off(aligned, q, c);
            aligned
                .stack()
                .process(s, e, |buf| sort_chunks_by(buf, cu, &mut *cmp))?;
        }
        p = q;
    }
    // Phase 2: bottom-up merge passes.
    let mut width = run0;
    while width < count {
        // `count - width > 0` here (loop guard), so `count - width` is safe.
        let stop = count - width;
        let mut lo = 0u64;
        while lo < stop {
            let mid = lo + width; // < count, since lo < count - width
            let hi = if count - mid > width {
                mid + width
            } else {
                count
            };
            imerge(
                aligned, c, cu, budget, lo, mid, hi, &mut bi, &mut bj, &mut stack, cmp,
            )?;
            // Advance two runs; saturate so a near-`u64::MAX` sum can only end
            // the pass, never wrap.
            lo = lo.saturating_add(width).saturating_add(width);
        }
        width = width.saturating_mul(2);
    }
    Ok(())
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
