#ifndef BSTACK_ALLOC_H
#define BSTACK_ALLOC_H

#include "bstack.h"
#include <errno.h>

/*
 * bstack_alloc — region-management layer on top of bstack.
 *
 * Key types
 * ---------
 * bstack_slice_t            — lightweight handle (allocator ptr + offset + len)
 *                             to a contiguous region of a bstack payload.
 * bstack_allocator_t        — vtable base for types that own a bstack and manage
 *                             regions within it.  Vtable methods: stack, alloc,
 *                             realloc, dealloc.  Convenience helpers (inline):
 *                             bstack_allocator_len, bstack_allocator_is_empty.
 *                             Also carries a bulk_vtbl pointer (NULL when not
 *                             supported) and helpers:
 *                             bstack_allocator_alloc_bulk,
 *                             bstack_allocator_dealloc_bulk.
 * bstack_slice_reader_t     — cursor-based reader over a bstack_slice_t.
 * linear_bstack_allocator_t — bump allocator; every operation maps to one call.
 *
 * Compile with -DBSTACK_FEATURE_SET to enable bstack_slice_write and friends.
 * Both -DBSTACK_FEATURE_SET and -DBSTACK_FEATURE_ATOMIC together additionally
 * enable bstack_slice_cas_on, bstack_slice_cas_on_ne, bstack_slice_cas_on_masked,
 * and bstack_slice_process.
 */

/* -------------------------------------------------------------------------
 * Forward declaration — bstack_slice_t holds a bstack_allocator_t pointer.
 * ---------------------------------------------------------------------- */

typedef struct bstack_allocator bstack_allocator_t;

/* =========================================================================
 * bstack_slice_t
 * ====================================================================== */

typedef struct {
    bstack_allocator_t *allocator;
    uint64_t            offset;
    uint64_t            len;
} bstack_slice_t;

/*
 * Accessor macros — zero-cost field reads.
 *   bstack_slice_start(s)    → logical start offset in the payload
 *   bstack_slice_end(s)      → exclusive logical end offset
 *   bstack_slice_len(s)      → number of bytes in the slice
 *   bstack_slice_is_empty(s) → non-zero if the slice spans zero bytes
 */
#define bstack_slice_start(s)    ((s).offset)
#define bstack_slice_end(s)      ((s).offset + (s).len)
#define bstack_slice_len(s)      ((s).len)
#define bstack_slice_is_empty(s) ((s).len == 0)

/*
 * bstack_slice_is_from(s, a) → non-zero if s was issued by allocator a
 *
 * A slice records the allocator that produced it, but nothing checks at
 * compile time that it is only ever handed back to *that* instance: every
 * allocator of a given kind has the same type, so passing a1's slice to a2's
 * realloc/dealloc compiles.  See "Foreign slices" below; allocators use this
 * to reject a foreign slice at run time.  `a` is a bstack_allocator_t * — for
 * a concrete allocator, pass &alloc->base.
 */
#define bstack_slice_is_from(s, a) ((s).allocator == (a))

/*
 * Foreign slices
 * --------------
 * Handing a slice to an allocator that did not issue it is a caller error the
 * language cannot catch.  It is not a memory-safety problem: a slice is an
 * (offset, len) coordinate pair into a file, not a pointer, and every access
 * through it goes via bstack's bounds-checked I/O.  The damage would be to
 * on-disk bookkeeping — the receiving allocator recording a free block it
 * never owned.
 *
 * Rejecting it is therefore the allocator's job, at run time.  Every allocator
 * in this library checks slice ownership at the top of realloc, dealloc, and
 * dealloc_bulk, before touching any metadata, and fails with errno = EINVAL:
 * realloc returns -1 with the untouched slice written to *out; dealloc returns
 * -1; dealloc_bulk rejects the whole batch and frees nothing.  Custom
 * allocators should do the same; bstack_slice_is_from is the check.
 */

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Serialize to a 16-byte array: offset as 8 LE bytes, then len as 8 LE bytes.
 * Reconstruct with bstack_slice_from_bytes.
 */
void bstack_slice_to_bytes(bstack_slice_t s, uint8_t out[16]);

/*
 * Reconstruct a slice from a 16-byte array produced by bstack_slice_to_bytes.
 * Does not validate that the encoded range lies within the payload.
 */
bstack_slice_t bstack_slice_from_bytes(bstack_allocator_t *a,
                                        const uint8_t bytes[16]);

/*
 * Construct a zero-length slice anchored at offset 0.
 * All I/O operations on the returned slice are no-ops or return empty results.
 * Useful as a sentinel or default value before a real allocation is available.
 */
bstack_slice_t bstack_slice_empty(bstack_allocator_t *a);

/*
 * Read the entire slice into buf.
 * buf must have room for at least s.len bytes; no overflow check is done.
 * Returns 0 on success, -1 on failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_read(bstack_slice_t s, uint8_t *buf);

/*
 * Read min(buf_len, s.len) bytes from the start of the slice into buf.
 * Returns 0 on success, -1 on failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_read_into(bstack_slice_t s, uint8_t *buf, size_t buf_len);

/*
 * Read buf_len bytes starting at slice-relative offset start into buf.
 * Returns -1 with errno = EINVAL if start + buf_len exceeds s.len or would
 * overflow uint64_t.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_read_range_into(bstack_slice_t s, uint64_t start,
                                  uint8_t *buf, size_t buf_len);

/*
 * Read the half-open slice-relative byte range [start, end) into buf.
 * buf must have room for (end - start) bytes; no overflow check is done.
 * Returns -1 with errno = EINVAL if start > end or end exceeds s.len.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_read_range(bstack_slice_t s, uint64_t start, uint64_t end,
                             uint8_t *buf);

/*
 * Produce the sub-range [start, end) relative to this slice into *out.
 * start and end are 0-based within the slice (not the payload).
 * Returns -1 with errno = EINVAL if start > end or end > s.len.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_subslice(bstack_slice_t s, uint64_t start, uint64_t end,
                           bstack_slice_t *out);

#ifdef BSTACK_FEATURE_SET
/*
 * Overwrite the first min(data_len, s.len) bytes of the slice in place.
 * Requires -DBSTACK_FEATURE_SET.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_write(bstack_slice_t s,
                        const uint8_t *data, size_t data_len);

/*
 * Overwrite [start, start+data_len) within this slice in place.
 * start is 0-based within the slice.
 * Returns -1 with errno = EINVAL if start + data_len exceeds s.len.
 * Requires -DBSTACK_FEATURE_SET.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_write_range(bstack_slice_t s, uint64_t start,
                              const uint8_t *data, size_t data_len);

/*
 * Zero the entire slice in place.
 * Requires -DBSTACK_FEATURE_SET.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_zero(bstack_slice_t s);

/*
 * Zero [start, start+n) within this slice in place.
 * start is 0-based within the slice.
 * Returns -1 with errno = EINVAL if start + n exceeds s.len.
 * Requires -DBSTACK_FEATURE_SET.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_zero_range(bstack_slice_t s, uint64_t start, uint64_t n);
#endif /* BSTACK_FEATURE_SET */

#if defined(BSTACK_FEATURE_SET) && defined(BSTACK_FEATURE_ATOMIC)
/*
 * Overwrite s with new_bytes if guard's current contents equal expected.
 *
 * One crash-atomic bstack_eq_crds call: guard's expected_len bytes are read
 * and compared to expected, and if they match, s is overwritten with
 * new_bytes_len bytes from new_bytes and its prior contents are written to
 * old_buf, all under the same write lock.  *ok (if non-NULL) is set to 1 if
 * the swap ran, 0 if the comparison failed (s left untouched).  old_buf must
 * have room for s.len bytes unless s.len == 0, in which case it may be NULL.
 *
 * guard may be a view into the same or a different region of s's bstack,
 * including s itself, but must be backed by the same bstack_t.
 *
 * Returns -1 with errno = EINVAL if guard and s are backed by different
 * bstack_t instances, if expected_len != guard.len, or if new_bytes_len !=
 * s.len.
 *
 * Requires -DBSTACK_FEATURE_SET and -DBSTACK_FEATURE_ATOMIC.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_cas_on(bstack_slice_t s, bstack_slice_t guard,
                         const uint8_t *expected, size_t expected_len,
                         const uint8_t *new_bytes, size_t new_bytes_len,
                         uint8_t *old_buf, int *ok);

/*
 * Overwrite s with new_bytes if guard's current contents do NOT equal
 * expected.
 *
 * Like bstack_slice_cas_on but wraps bstack_ne_crds: the swap runs when the
 * comparison fails rather than when it succeeds.
 *
 * Requires -DBSTACK_FEATURE_SET and -DBSTACK_FEATURE_ATOMIC.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_cas_on_ne(bstack_slice_t s, bstack_slice_t guard,
                            const uint8_t *expected, size_t expected_len,
                            const uint8_t *new_bytes, size_t new_bytes_len,
                            uint8_t *old_buf, int *ok);

/*
 * Overwrite s with new_bytes if guard's current contents equal expected
 * under a bitwise mask.
 *
 * Like bstack_slice_cas_on but wraps bstack_masked_eq_crds: the condition is
 * (guard[i] & mask[i]) == (expected[i] & mask[i]) for every byte i.  mask
 * must have expected_len bytes.
 *
 * Requires -DBSTACK_FEATURE_SET and -DBSTACK_FEATURE_ATOMIC.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_cas_on_masked(bstack_slice_t s, bstack_slice_t guard,
                                const uint8_t *mask,
                                const uint8_t *expected, size_t expected_len,
                                const uint8_t *new_bytes, size_t new_bytes_len,
                                uint8_t *old_buf, int *ok);

/*
 * Run a length-preserving transform over the slice's bytes in place.
 *
 * One crash-atomic bstack_process call: the slice's bytes are read, handed
 * to cb for in-place modification, then written back, all under the same
 * write lock.  cb must not change the buffer's length — see bstack_process
 * for the callback contract.
 *
 * Requires -DBSTACK_FEATURE_SET and -DBSTACK_FEATURE_ATOMIC.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_process(bstack_slice_t s,
                          int (*cb)(uint8_t *buf, size_t len, void *ctx),
                          void *ctx);
#endif /* BSTACK_FEATURE_SET && BSTACK_FEATURE_ATOMIC */

/* =========================================================================
 * Chunked-view operations over a bstack_slice_t
 *
 * A "chunk view" is a slice plus a fixed record stride (chunk_len): the slice
 * is divided into s.len / chunk_len records of chunk_len bytes each.  Rather
 * than a dedicated chunk type, these take a bstack_slice_t and a chunk_len.
 *
 * Precondition (undefined behaviour if violated): chunk_len must be non-zero
 * and evenly divide s.len, so the slice is exactly chunk_count whole records
 * with no remainder.  Callers that may have a remainder should trim it off the
 * slice first.  The geometry helpers (overlaps/adjacent_to/merge/merge_adjacent)
 * additionally accept chunk_len == 0 to mean "plain slice, ignore phase" — see
 * each function.
 *
 * Comparators and predicates follow the qsort/bsearch convention: each receives
 * pointers to whole chunk_len-byte records (and, for the searches, a key), and
 * takes no user context pointer.
 * ====================================================================== */

/*
 * Non-zero if a and b share chunk phase: their start offsets are congruent
 * modulo chunk_len, so a record boundary in one lines up with a boundary in the
 * other wherever the regions coincide.  Pure coordinate test.  chunk_len == 0
 * always returns 0 (no stride, no phase to share).
 */
int bstack_slice_same_chunk_phase(bstack_slice_t a, bstack_slice_t b,
                                  uint64_t chunk_len);

/*
 * Non-zero if a and b touch end-to-end with no gap and no overlap
 * (end(a) == start(b) or end(b) == start(a)).  When chunk_len != 0 the two must
 * also be same-phase (see bstack_slice_same_chunk_phase); chunk_len == 0 drops
 * that requirement, giving the plain-slice test.  Pure coordinate test.
 */
int bstack_slice_adjacent_to(bstack_slice_t a, bstack_slice_t b,
                             uint64_t chunk_len);

/*
 * Non-zero if a and b share at least one byte.  A zero-length slice overlaps
 * nothing.  When chunk_len != 0 the two must also be same-phase; chunk_len == 0
 * gives the plain-slice test.  Pure coordinate test.
 */
int bstack_slice_overlaps(bstack_slice_t a, bstack_slice_t b,
                          uint64_t chunk_len);

/*
 * Merge a and b into the smallest slice covering both, into *out.
 *
 * Succeeds if they overlap (chunk_len != 0 also requires same phase), or if
 * either is empty (an empty slice is the identity: the other is returned
 * unchanged).  chunk_len == 0 gives the plain-slice merge.
 *
 * Returns 0 on success, -1 with errno = EINVAL if a and b come from different
 * allocators, or if both are non-empty and do not overlap.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_merge(bstack_slice_t a, bstack_slice_t b, uint64_t chunk_len,
                       bstack_slice_t *out);

/*
 * Merge a and b into the smallest slice covering both, into *out, requiring
 * them to be adjacent (touching end-to-end) and both non-empty.
 *
 * For two non-empty same-stride records-slices, byte adjacency already forces
 * the same phase, so chunk_len is not consulted here (kept for signature
 * symmetry).  Returns 0 on success, -1 with errno = EINVAL if a and b come from
 * different allocators, either is empty, or they are not adjacent.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_merge_adjacent(bstack_slice_t a, bstack_slice_t b,
                                uint64_t chunk_len, bstack_slice_t *out);

/*
 * Split s at byte offset mid into *left = [0, mid) and *right = [mid, s.len),
 * both relative to s.  Returns 0 on success, -1 with errno = EINVAL if
 * mid > s.len.  left and right may be NULL to discard that half.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_split_at(bstack_slice_t s, uint64_t mid,
                          bstack_slice_t *left, bstack_slice_t *right);

/*
 * Split s at chunk index mid: *left holds records [0, mid), *right holds
 * [mid, chunk_count).  Both inherit s's stride and phase.  Returns 0 on
 * success, -1 with errno = EINVAL if mid > chunk_count.  left and right may be
 * NULL to discard that half.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_split_chunk_at(bstack_slice_t s, uint64_t chunk_len,
                                uint64_t mid, bstack_slice_t *left,
                                bstack_slice_t *right);

/*
 * The index-th chunk of s as a bstack_slice_t, by pure offset arithmetic.
 *
 * No bounds check: index must be < chunk_count (UB otherwise), matching the
 * precondition style of this section.  Function-like macro — s, chunk_len, and
 * index are each evaluated more than once, so pass side-effect-free arguments.
 */
#define bstack_slice_nth_chunk(s, chunk_len, index)                        \
    ((bstack_slice_t){ (s).allocator,                                      \
                       (s).offset + (uint64_t)(index) * (uint64_t)(chunk_len), \
                       (uint64_t)(chunk_len) })

/*
 * Binary search s (already ordered by cmp) for a record matching key.
 *
 * cmp(key, chunk) returns < 0 if key sorts before chunk, > 0 if after, 0 on a
 * match — the bsearch convention.  On a match *out_index receives its index and
 * the call returns 0; with no match *out_index receives the index at which a
 * matching record would be inserted to keep s ordered and the call returns 1.
 * Returns -1 (errno set) on an I/O or allocation failure.
 *
 * With BSTACK_FEATURE_ATOMIC this probes O(log n) records under one lock;
 * otherwise it reads the whole region once (O(n) memory) under one lock.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_search(bstack_slice_t s, uint64_t chunk_len,
                        const void *key,
                        int (*cmp)(const void *key, const void *chunk),
                        uint64_t *out_index);

/*
 * Index of the first record of s for which pred is false, assuming s is already
 * partitioned by pred (every accepted record before every rejected one).
 *
 * pred(key, chunk) returns non-zero to accept.  *out_index receives the
 * partition point.  Returns 0 on success, -1 (errno set) on an I/O or
 * allocation failure.  Same O(log n)/whole-region behaviour as
 * bstack_slice_search.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_select(bstack_slice_t s, uint64_t chunk_len,
                        const void *key,
                        int (*pred)(const void *key, const void *chunk),
                        uint64_t *out_index);

/*
 * Set *out_sorted to non-zero iff every record of s compares <= the next by cmp
 * (qsort convention: cmp(a, b) < 0 when a sorts before b).  Reads the whole
 * region once under one lock, then scans in memory.  Returns 0 on success,
 * -1 (errno set) on an I/O or allocation failure.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_is_sorted(bstack_slice_t s, uint64_t chunk_len,
                           int (*cmp)(const void *a, const void *b),
                           int *out_sorted);

#if defined(BSTACK_FEATURE_SET) && defined(BSTACK_FEATURE_ATOMIC)
/*
 * Reverse the order of s's records in place — whole records change position,
 * the bytes within each are untouched.  One crash-atomic bstack_process call.
 * Returns 0 on success, -1 (errno set) on failure.
 *
 * Requires -DBSTACK_FEATURE_SET and -DBSTACK_FEATURE_ATOMIC.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_reverse_chunks(bstack_slice_t s, uint64_t chunk_len);

/*
 * Rotate s's records left in place so the record at index k becomes first —
 * equivalently, rotate the bytes left by k * chunk_len.  One crash-atomic
 * bstack_process call.  Returns 0 on success, -1 with errno = EINVAL if
 * k > chunk_count, else -1 (errno set) on I/O failure.
 *
 * Requires -DBSTACK_FEATURE_SET and -DBSTACK_FEATURE_ATOMIC.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_rotate_left(bstack_slice_t s, uint64_t chunk_len, uint64_t k);

/*
 * Rotate s's records right in place so the last k records move to the front.
 * Companion to bstack_slice_rotate_left.  Returns 0 on success, -1 with
 * errno = EINVAL if k > chunk_count, else -1 (errno set) on I/O failure.
 *
 * Requires -DBSTACK_FEATURE_SET and -DBSTACK_FEATURE_ATOMIC.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_rotate_right(bstack_slice_t s, uint64_t chunk_len, uint64_t k);

/*
 * Sort s's records in place by cmp (qsort convention).  Reads the whole region,
 * sorts in memory, and commits it in one crash-atomic bstack_process call.  Not
 * guaranteed stable (uses qsort internally).  Returns 0 on success, -1 (errno
 * set) on failure.
 *
 * Requires -DBSTACK_FEATURE_SET and -DBSTACK_FEATURE_ATOMIC.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_sort(bstack_slice_t s, uint64_t chunk_len,
                      int (*cmp)(const void *a, const void *b));

/*
 * Partition s's records in place so the record at index n lands where a full
 * sort by cmp would put it, with every earlier record <= it and every later
 * record >= it; order on either side of n is otherwise unspecified.  One
 * crash-atomic bstack_process call.  Returns 0 on success, -1 with errno =
 * EINVAL if n >= chunk_count, else -1 (errno set) on I/O failure.
 *
 * Requires -DBSTACK_FEATURE_SET and -DBSTACK_FEATURE_ATOMIC.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_partition(bstack_slice_t s, uint64_t chunk_len, uint64_t n,
                           int (*cmp)(const void *a, const void *b));

/*
 * Bounded-memory, out-of-core sort of s's records in place by cmp (qsort
 * convention).
 *
 * Unlike bstack_slice_sort, which reads the whole region into one buffer and
 * commits it in a single bstack_process — bounded by available memory, so a
 * region too large for one buffer cannot be sorted that way — this runs an
 * in-place bottom-up merge sort that never holds more than a fixed internal
 * byte budget resident (no knob), so peak memory is O(1) in the region's size.
 * Runs are formed with bstack_process and merged in place by a rotation merge:
 * bstack_process for sub-ranges that fit the budget, plus single-record
 * bstack_cross_exchange swaps to rotate wider ones.  No scratch region.
 *
 * "Partial" names the interruptible middle state, not the result: on success
 * the region is fully sorted.  Every step is an independent crash-atomic
 * operation that only permutes the bytes it touches, so a crash or an early
 * error return leaves the region as some valid permutation of the records —
 * never lost or duplicated data — just not fully ordered, and re-running from
 * any such state completes the sort.
 *
 * Not stable.  Returns 0 on success, -1 (errno set) on an allocation failure
 * or a genuine bstack_process / bstack_cross_exchange I/O failure.
 *
 * Requires -DBSTACK_FEATURE_SET and -DBSTACK_FEATURE_ATOMIC.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_sort_partial(bstack_slice_t s, uint64_t chunk_len,
                              int (*cmp)(const void *a, const void *b));

/*
 * Bounded-memory, out-of-core partition of s's records around rank n: the same
 * result as bstack_slice_partition — the record at index n lands where a full
 * sort by cmp would put it, every earlier record <= it and every later record
 * >= it — computed without holding the whole region in memory.
 *
 * A bounded-memory quickselect: it narrows an active record band around n,
 * choosing each pivot from a cross-region sample and partitioning in place with
 * atomic bstack_cross_exchange swaps, until the band fits the internal budget
 * and one bstack_process settles it exactly.  Peak memory is O(1) in the
 * region's size; no scratch region.
 *
 * Every step is crash-atomic and permutation-preserving, so a crash or an early
 * error return leaves a valid permutation and re-running completes the
 * selection.  Returns 0 on success, -1 with errno = EINVAL if n >= chunk_count,
 * else -1 (errno set) on an allocation or I/O failure.
 *
 * Requires -DBSTACK_FEATURE_SET and -DBSTACK_FEATURE_ATOMIC.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_partition_partial(bstack_slice_t s, uint64_t chunk_len,
                                   uint64_t n,
                                   int (*cmp)(const void *a, const void *b));
#endif /* BSTACK_FEATURE_SET && BSTACK_FEATURE_ATOMIC */

/* =========================================================================
 * bstack_guard_vtbl_t / bstack_guarded_slice_t
 *
 * Transparent wrapper around bstack_slice_t that intercepts reads, writes,
 * and subview operations via a vtable of optional hook and override functions.
 * Any member of the vtable may be NULL to leave that behaviour unmodified.
 *
 * Hook invocation order for read:
 *   pre_read  → (read override or bstack_slice_read)  → post_read
 * Hook invocation order for write:
 *   pre_write → (write override or bstack_slice_write) → post_write
 * Subview:
 *   subview override or bstack_slice_subslice (inherits vtbl/ctx)
 *
 * Any hook that returns -1 aborts the operation; errno should be set by the
 * hook before returning.
 * ====================================================================== */

typedef struct {
    /* --- lifecycle hooks ------------------------------------------------ */
    /* Called before the read; may abort by returning -1. */
    int (*pre_read) (void *ctx, bstack_slice_t s);
    /*
     * Called after a successful read.  buf points to the bytes just read;
     * len is the byte count.  May abort by returning -1.
     */
    int (*post_read)(void *ctx, bstack_slice_t s, uint8_t *buf, size_t len);
    /*
     * Called before the write.  *buf and *len are initialised to the caller's
     * buffer and byte count; the hook may redirect *buf (e.g. to an encrypted
     * scratch buffer) and/or shrink *len.  For zero operations *buf is NULL on
     * entry; leaving it NULL preserves zero semantics.  May abort by returning
     * -1.  After the call the implementation clips *len to the available slice
     * capacity.
     */
    int (*pre_write)(void *ctx, bstack_slice_t s,
                     const uint8_t **buf, size_t *len);
    /* Called after a successful write; may abort by returning -1. */
    int (*post_write)(void *ctx, bstack_slice_t s);

    /* --- IO overrides (NULL = use bstack_slice_* default) --------------- */
    /*
     * Replace the underlying read.  offset is slice-relative (0 = first byte
     * of slice).  Invoked between pre_read and post_read when non-NULL.
     */
    int (*read)   (void *ctx, bstack_slice_t s, uint64_t offset,
                   uint8_t *buf, size_t len);
    /*
     * Replace the underlying write.  offset is slice-relative.  buf == NULL
     * signals a zero-write (write len zeroed bytes starting at offset).
     * Invoked between pre_write and post_write when non-NULL.
     */
    int (*write)  (void *ctx, bstack_slice_t s, uint64_t offset,
                   const uint8_t *buf, size_t len);
    /*
     * Replace bstack_slice_subslice.  The resulting slice is wrapped with the
     * same vtbl/ctx as the parent guarded slice.
     */
    int (*subview)(void *ctx, bstack_slice_t s, uint64_t start, uint64_t end,
                   bstack_slice_t *out);
} bstack_guard_vtbl_t;

typedef struct {
    bstack_slice_t             slice;
    const bstack_guard_vtbl_t *vtbl;  /* NULL = unguarded pass-through */
    void                      *ctx;
} bstack_guarded_slice_t;

/*
 * Accessor macros — delegate to the underlying slice.
 */
#define bstack_guarded_slice_start(g)    bstack_slice_start((g).slice)
#define bstack_guarded_slice_end(g)      bstack_slice_end((g).slice)
#define bstack_guarded_slice_len(g)      bstack_slice_len((g).slice)
#define bstack_guarded_slice_is_empty(g) bstack_slice_is_empty((g).slice)

/* Construct a guarded slice.  vtbl may be NULL (no-op pass-through). */
static inline bstack_guarded_slice_t
bstack_guarded_slice(bstack_slice_t slice,
                     const bstack_guard_vtbl_t *vtbl, void *ctx)
{
    bstack_guarded_slice_t gs;
    gs.slice = slice;
    gs.vtbl  = vtbl;
    gs.ctx   = ctx;
    return gs;
}

/*
 * Read the entire slice into buf via the guard.
 * buf must have room for at least gs.slice.len bytes.
 * Returns 0 on success, -1 on failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_guarded_slice_read(bstack_guarded_slice_t gs, uint8_t *buf);

/*
 * Read min(buf_len, gs.slice.len) bytes from the start of the slice.
 * Returns 0 on success, -1 on failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_guarded_slice_read_into(bstack_guarded_slice_t gs,
                                    uint8_t *buf, size_t buf_len);

/*
 * Read buf_len bytes starting at slice-relative offset start into buf.
 * Returns -1 with errno = EINVAL if start + buf_len exceeds gs.slice.len.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_guarded_slice_read_range_into(bstack_guarded_slice_t gs,
                                          uint64_t start,
                                          uint8_t *buf, size_t buf_len);

/*
 * Read the half-open slice-relative byte range [start, end) into buf.
 * buf must have room for (end - start) bytes.
 * Returns -1 with errno = EINVAL if start > end or end > gs.slice.len.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_guarded_slice_read_range(bstack_guarded_slice_t gs,
                                     uint64_t start, uint64_t end,
                                     uint8_t *buf);

/*
 * Produce the sub-range [start, end) as a new guarded slice inheriting the
 * same vtbl/ctx.  start and end are 0-based within the slice.
 * Returns -1 with errno = EINVAL if start > end or end > gs.slice.len.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_guarded_slice_subslice(bstack_guarded_slice_t gs,
                                   uint64_t start, uint64_t end,
                                   bstack_guarded_slice_t *out);

#ifdef BSTACK_FEATURE_SET
/*
 * Write min(data_len, gs.slice.len) bytes into the slice via the guard.
 * Requires -DBSTACK_FEATURE_SET.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_guarded_slice_write(bstack_guarded_slice_t gs,
                                const uint8_t *data, size_t data_len);

/*
 * Write data_len bytes at slice-relative offset start via the guard.
 * Returns -1 with errno = EINVAL if start + data_len exceeds gs.slice.len.
 * Requires -DBSTACK_FEATURE_SET.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_guarded_slice_write_range(bstack_guarded_slice_t gs,
                                      uint64_t start,
                                      const uint8_t *data, size_t data_len);

/*
 * Zero the entire slice via the guard.
 * Requires -DBSTACK_FEATURE_SET.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_guarded_slice_zero(bstack_guarded_slice_t gs);

/*
 * Zero [start, start+n) within the slice via the guard.
 * Returns -1 with errno = EINVAL if start + n exceeds gs.slice.len.
 * Requires -DBSTACK_FEATURE_SET.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_guarded_slice_zero_range(bstack_guarded_slice_t gs,
                                     uint64_t start, uint64_t n);
#endif /* BSTACK_FEATURE_SET */

/* =========================================================================
 * bstack_slice_reader_t
 *
 * Cursor-based reader over a bstack_slice_t.
 * All positions and lengths are in slice-relative coordinates [0, slice.len).
 * ====================================================================== */

typedef struct {
    bstack_slice_t slice;
    uint64_t       cursor;
} bstack_slice_reader_t;

/* Current cursor position within the slice (0-based). */
#define bstack_slice_reader_position(r) ((r).cursor)
/* The underlying slice. */
#define bstack_slice_reader_slice(r)    ((r).slice)

/* Construct a reader positioned at the start of the slice. */
bstack_slice_reader_t bstack_slice_reader(bstack_slice_t s);

/* Construct a reader positioned at offset bytes into the slice. */
bstack_slice_reader_t bstack_slice_reader_at(bstack_slice_t s, uint64_t offset);

/*
 * Read up to buf_len bytes from the current cursor position into buf, then
 * advance the cursor by the number of bytes read.
 * If n_read is non-NULL it receives the number of bytes read.
 * Returns 0 on success (including end-of-slice where *n_read = 0).
 * Returns -1 on I/O failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_reader_read(bstack_slice_reader_t *r,
                              uint8_t *buf, size_t buf_len, size_t *n_read);

/*
 * Seek to an absolute position within the slice.
 * Seeking past slice.len is allowed; reads from that position return 0 bytes.
 * Always succeeds.  If out_pos is non-NULL it receives the new cursor position.
 */
int bstack_slice_reader_seek_start(bstack_slice_reader_t *r, uint64_t offset,
                                    uint64_t *out_pos);

/*
 * Seek relative to the current cursor (cursor += delta).
 * Returns -1 with errno = EINVAL if the resulting position would be negative.
 * If out_pos is non-NULL it receives the new cursor position.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_reader_seek_cur(bstack_slice_reader_t *r, int64_t delta,
                                  uint64_t *out_pos);

/*
 * Seek relative to the end of the slice (cursor = slice.len + delta).
 * Returns -1 with errno = EINVAL if the resulting position would be negative.
 * If out_pos is non-NULL it receives the new cursor position.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_slice_reader_seek_end(bstack_slice_reader_t *r, int64_t delta,
                                  uint64_t *out_pos);

/* =========================================================================
 * bstack_allocator_t — vtable interface
 *
 * Base type for region allocators backed by a bstack.  Embed as the first
 * member of a concrete allocator struct so that a pointer to the concrete
 * struct can be safely cast to bstack_allocator_t *.
 *
 * Vtable methods: stack, alloc, realloc, dealloc.
 * Convenience helpers (inline functions below): len, is_empty.
 * ====================================================================== */

typedef struct {
    /* Return the underlying bstack (non-owning pointer). */
    bstack_t *(*stack)(bstack_allocator_t *self);

    /*
     * Allocate len zero-initialised bytes; write the handle into *out.
     * Returns 0 on success, -1 on failure (errno set).  len = 0 is valid.
     */
    int (*alloc)(bstack_allocator_t *self, uint64_t len, bstack_slice_t *out);

    /*
     * Resize slice to new_len bytes; write the (possibly repositioned) handle
     * into *out.  Returns 0 on success.
     * On failure returns a negative code and leaves errno set:
     *   -1 — the original allocation survived the failure (either untouched,
     *        or a fully-committed replacement region); its handle is written
     *        to *out and remains safe to use or free.
     *   -2 — the allocation was lost mid-operation; *out is not meaningful
     *        and the original handle must not be reused (only recoverable,
     *        if at all, through crash recovery).
     * May return -1 with errno = ENOTSUP if the implementation does not
     * support the requested resize (e.g. non-tail resize on a bump allocator)
     * — the original is untouched in that case.
     * slice must have been issued by self; implementations reject a foreign
     * slice with -1 and errno = EINVAL, writing it back to *out untouched
     * (see "Foreign slices" above bstack_slice_to_bytes).
     */
    int (*realloc)(bstack_allocator_t *self, bstack_slice_t slice,
                   uint64_t new_len, bstack_slice_t *out);

    /*
     * Release the region described by slice.
     * Returns 0 on success — after which slice must not be used for further
     * I/O.
     * On failure returns a negative code and leaves errno set:
     *   -1 — the original allocation survived the failure and remains the
     *        same as the passed-in slice; it is still safe to use or retry
     *        freeing.
     *   -2 — the allocation was lost mid-operation (e.g. a partially
     *        committed free-list splice or tail truncation); slice must not
     *        be reused (only recoverable, if at all, through crash recovery).
     * May be NULL to indicate a permanent no-op; bstack_allocator_dealloc
     * checks for NULL before dispatching.
     * slice must have been issued by self; implementations reject a foreign
     * slice with -1 and errno = EINVAL, having modified nothing (see "Foreign
     * slices" above bstack_slice_to_bytes).
     */
    int (*dealloc)(bstack_allocator_t *self, bstack_slice_t slice);
} bstack_allocator_vtbl_t;

/*
 * bstack_bulk_allocator_vtbl_t — optional bulk extension vtable.
 *
 * The struct embeds bstack_allocator_vtbl_t as its first member so a pointer
 * to it may be safely cast to bstack_allocator_vtbl_t *.  Concrete allocators
 * that support bulk operations set base.vtbl = &bulk_vtbl->base and
 * base.bulk_vtbl = bulk_vtbl.
 */
typedef struct {
    bstack_allocator_vtbl_t base; /* must be first */

    /*
     * Allocate n zero-initialised regions.
     * lens[i] is the size in bytes for out_slices[i]; out_slices must hold
     * room for n bstack_slice_t values.  On failure, any slices already
     * allocated are rolled back and out_slices is left unmodified.
     * Returns 0 on success, -1 on failure (errno set).
     */
    int (*alloc_bulk)(bstack_allocator_t *self, const uint64_t *lens, size_t n,
                      bstack_slice_t *out_slices);

    /*
     * Free n slices.
     * All slices must originate from self.  A batch containing a slice issued
     * by another allocator is rejected whole: nothing is freed and the call
     * returns -1 with errno = EINVAL (see "Foreign slices" above
     * bstack_slice_to_bytes).
     * Returns 0 on success, -1 on failure (errno set).
     */
    int (*dealloc_bulk)(bstack_allocator_t *self, const bstack_slice_t *slices,
                        size_t n);
} bstack_bulk_allocator_vtbl_t;

struct bstack_allocator {
    const bstack_allocator_vtbl_t      *vtbl;
    const bstack_bulk_allocator_vtbl_t *bulk_vtbl; /* NULL if not supported */
};

/* -------------------------------------------------------------------------
 * Vtable forwarding helpers — thin static inline wrappers.
 * ---------------------------------------------------------------------- */

static inline bstack_t *
bstack_allocator_stack(bstack_allocator_t *a)
{
    return a->vtbl->stack(a);
}

BSTACK_WARN_UNUSED_RESULT
static inline int
bstack_allocator_alloc(bstack_allocator_t *a, uint64_t len, bstack_slice_t *out)
{
    return a->vtbl->alloc(a, len, out);
}

BSTACK_WARN_UNUSED_RESULT
static inline int
bstack_allocator_realloc(bstack_allocator_t *a, bstack_slice_t s,
                          uint64_t new_len, bstack_slice_t *out)
{
    return a->vtbl->realloc(a, s, new_len, out);
}

/*
 * Dispatch dealloc through the vtable.  If the vtable entry is NULL the call
 * is a no-op and returns 0 (equivalent to a default no-op dealloc).
 */
BSTACK_WARN_UNUSED_RESULT
static inline int
bstack_allocator_dealloc(bstack_allocator_t *a, bstack_slice_t s)
{
    if (a->vtbl->dealloc)
        return a->vtbl->dealloc(a, s);
    return 0;
}

/*
 * Return the current logical payload size via the allocator's stack.
 * Delegates to bstack_len; returns 0 on success, -1 on failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
static inline int
bstack_allocator_len(bstack_allocator_t *a, uint64_t *out_len)
{
    return bstack_len(bstack_allocator_stack(a), out_len);
}

/*
 * Set *out_empty to 1 if the backing stack is empty, 0 otherwise.
 * Delegates to bstack_len; returns 0 on success, -1 on failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
static inline int
bstack_allocator_is_empty(bstack_allocator_t *a, int *out_empty)
{
    uint64_t len;
    int r = bstack_allocator_len(a, &len);
    if (r == 0)
        *out_empty = (len == 0);
    return r;
}

/* Return the bulk vtable for this allocator, or NULL if not supported. */
static inline const bstack_bulk_allocator_vtbl_t *
bstack_allocator_bulk_vtbl(bstack_allocator_t *a)
{
    return a->bulk_vtbl;
}

/*
 * Allocate n slices in bulk.  Returns -1 with errno = ENOTSUP when the
 * allocator has no bulk vtable.
 */
BSTACK_WARN_UNUSED_RESULT
static inline int
bstack_allocator_alloc_bulk(bstack_allocator_t *a, const uint64_t *lens,
                             size_t n, bstack_slice_t *out_slices)
{
    if (!a->bulk_vtbl) { errno = ENOTSUP; return -1; }
    return a->bulk_vtbl->alloc_bulk(a, lens, n, out_slices);
}

/*
 * Free n slices in bulk.  Returns -1 with errno = ENOTSUP when the allocator
 * has no bulk vtable.
 */
BSTACK_WARN_UNUSED_RESULT
static inline int
bstack_allocator_dealloc_bulk(bstack_allocator_t *a,
                               const bstack_slice_t *slices, size_t n)
{
    if (!a->bulk_vtbl) { errno = ENOTSUP; return -1; }
    return a->bulk_vtbl->dealloc_bulk(a, slices, n);
}

/* =========================================================================
 * linear_bstack_allocator_t — bump allocator
 *
 * Allocates regions sequentially by appending to the tail.  Every operation
 * maps to exactly one bstack call and is therefore crash-safe by inheritance:
 *
 *   alloc              → bstack_extend
 *   realloc (grow)     → bstack_extend
 *   realloc (shrink)   → bstack_discard
 *   dealloc (tail)     → bstack_discard
 *   dealloc (non-tail) → no-op
 *
 * realloc of a non-tail slice returns -1 with errno = ENOTSUP.
 * ====================================================================== */

typedef struct {
    bstack_allocator_t base;   /* must be first — safe to cast to bstack_allocator_t * */
    bstack_t          *bs;
} linear_bstack_allocator_t;

/*
 * Allocate and initialise a linear_bstack_allocator_t that takes ownership of bs.
 * Returns NULL on allocation failure (errno = ENOMEM).
 * Cast the result to bstack_allocator_t * to use the generic allocator interface.
 */
BSTACK_WARN_UNUSED_RESULT
linear_bstack_allocator_t *linear_bstack_allocator_new(bstack_t *bs);

/*
 * Free the allocator wrapper without closing the underlying bstack.
 * The caller must have already retrieved the bstack via
 * linear_bstack_allocator_into_stack, or accepts losing the reference.
 */
void linear_bstack_allocator_free(linear_bstack_allocator_t *alloc);

/*
 * Consume the allocator: free the wrapper and return the underlying bstack.
 * The returned bstack_t * must eventually be passed to bstack_close.
 */
bstack_t *linear_bstack_allocator_into_stack(linear_bstack_allocator_t *alloc);

#ifdef BSTACK_FEATURE_SET
/* =========================================================================
 * first_fit_bstack_allocator_t — first-fit free-list allocator
 *
 * Manages a persistent heap inside a bstack using a doubly-linked free list
 * with immediate coalescing.  Requires -DBSTACK_FEATURE_SET.
 *
 * On-disk layout (all within the bstack payload):
 *   [0..16)  — reserved (OFFSET_SIZE)
 *   [16..48) — allocator header: magic[8] | flags[4] | reserved[4] | free_head[8]
 *              magic  = "ALFF\x00\x01\x04\x00"
 *              flags  = bit 0: recovery_needed (crash detected)
 *              free_head = payload offset of first free block's payload, or 0
 *   [48..)   — block arena
 *
 * Each block: [header:16B | payload:size | footer:8B]
 *   header: size(u64 LE) | flags(u32 LE) | reserved(u32 LE), bit 0 of flags = is_free
 *   footer: size(u64 LE) — mirrors header size (used for left-coalescing)
 *   free block payload starts with: next_free(u64 LE) | prev_free(u64 LE)
 *
 * Crash safety: recovery_needed is set before any multi-step modification and
 * cleared after.  On open, if recovery_needed is set, the arena is scanned
 * linearly and the free list is rebuilt from is_free flags alone.
 *
 * Thread safety: without -DBSTACK_FEATURE_ATOMIC, an allocator handle must be
 * used from one thread at a time — its operations mutate the on-disk free list
 * in several steps and would race under concurrent calls.  With
 * -DBSTACK_FEATURE_ATOMIC the handle owns an in-memory mutex (lock) that
 * serializes the two compound operations not already made atomic by bstack's
 * own per-call lock — free-list mutation and stack extension/discard — so a
 * single handle may then be shared across threads.  recovery_needed is updated
 * with bstack_cas (no extra cost over the disk write it performs regardless),
 * which also rejects operating on a stack left in a needs-recovery state.
 * ====================================================================== */

typedef struct {
    bstack_allocator_t base; /* must be first — safe cast to bstack_allocator_t * */
    bstack_t          *bs;
#ifdef BSTACK_FEATURE_ATOMIC
    /* Opaque pointer to a platform mutex (pthread_mutex_t / CRITICAL_SECTION),
     * allocated in first_fit_bstack_allocator_new and released on free.  Kept
     * opaque so this header need not pull in <pthread.h> / <windows.h>. */
    void              *lock;
#endif
} first_fit_bstack_allocator_t;

/*
 * Open or initialise a first_fit_bstack_allocator_t over bs.
 *
 * - Empty stack: writes the 48-byte allocator header and returns ready.
 * - Non-empty stack: validates the ALFF 0.1.x magic prefix; if the
 *   recovery_needed flag is set (crash during prior operation), scans the
 *   arena linearly and rebuilds the free list before returning.
 *
 * Returns NULL on failure (errno = EINVAL for bad magic/header, ENOMEM on
 * allocation failure, or the errno from any failing bstack operation).
 * Cast the result to bstack_allocator_t * to use the generic interface.
 */
BSTACK_WARN_UNUSED_RESULT
first_fit_bstack_allocator_t *first_fit_bstack_allocator_new(bstack_t *bs);

/*
 * Free the allocator wrapper without closing the underlying bstack.
 */
void first_fit_bstack_allocator_free(first_fit_bstack_allocator_t *alloc);

/*
 * Consume the allocator: free the wrapper and return the underlying bstack.
 * The returned bstack_t * must eventually be passed to bstack_close.
 */
bstack_t *first_fit_bstack_allocator_into_stack(first_fit_bstack_allocator_t *alloc);

/* =========================================================================
 * ghost_tree_bstack_allocator_t — best-fit AVL tree allocator
 *
 * Manages a persistent heap using an AVL tree of free blocks keyed on
 * (size, address).  Free blocks store a 32-byte AVL node inline at offset 0;
 * live allocations carry zero overhead.  All memory is kept zeroed.
 *
 * On-disk layout (all within the bstack payload):
 *   [0..32)  — reserved (user area)
 *   [32..40) — magic: "ALGT\x00\x01\x04\x00"
 *   [40..48) — AVL root pointer (8 B LE) — absolute payload offset of root
 *   [48..)   — block arena (32-byte aligned)
 *
 * All allocations are aligned to 32 bytes; minimum block size is 32 bytes.
 * No crash-recovery log: on open, adjacent free blocks are coalesced and the
 * tree is rebuilt optimally balanced.
 *
 * Thread safety: without -DBSTACK_FEATURE_ATOMIC, an allocator handle must be
 * used from one thread at a time — AVL tree mutations span multiple bstack
 * calls and would race under concurrent access.  With -DBSTACK_FEATURE_ATOMIC
 * the handle owns an in-memory mutex (lock) that serialises all AVL tree
 * mutations; tail operations use bstack_try_discard / bstack_try_extend_zeros,
 * which check-and-act atomically under bstack's own write lock without holding
 * the allocator mutex.
 * ====================================================================== */

typedef struct {
    bstack_allocator_t base; /* must be first — safe cast to bstack_allocator_t * */
    bstack_t          *bs;
#ifdef BSTACK_FEATURE_ATOMIC
    /* Opaque platform mutex; serialises AVL tree mutations so a single handle
     * may be shared across threads. */
    void              *lock;
#endif
} ghost_tree_bstack_allocator_t;

/*
 * Open or initialise a ghost_tree_bstack_allocator_t over bs.
 *
 * - Empty stack: writes the 48-byte allocator header and returns ready.
 * - Payload < 48 bytes: returns NULL (errno = EINVAL).
 * - Non-empty, misaligned tail: pads to the next 32-byte boundary.
 * - Non-empty: validates the ALGT magic prefix, then coalesces adjacent free
 *   blocks and rebuilds a balanced AVL tree before returning.
 *
 * Returns NULL on failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
ghost_tree_bstack_allocator_t *ghost_tree_bstack_allocator_new(bstack_t *bs);

/*
 * Free the allocator wrapper without closing the underlying bstack.
 */
void ghost_tree_bstack_allocator_free(ghost_tree_bstack_allocator_t *alloc);

/*
 * Consume the allocator: free the wrapper and return the underlying bstack.
 * The returned bstack_t * must eventually be passed to bstack_close.
 */
bstack_t *ghost_tree_bstack_allocator_into_stack(ghost_tree_bstack_allocator_t *alloc);

/* =========================================================================
 * slab_bstack_allocator_t — fixed-block slab allocator
 *
 * All blocks in the arena are exactly block_size bytes with no per-block
 * header or footer.  When a block is free its first 8 bytes hold the payload
 * offset of the next free block (little-endian uint64_t, sentinel 0); when
 * live those bytes belong entirely to the caller.
 *
 * On-disk layout (all within the bstack payload):
 *   [0..24)  — reserved (OFFSET_SIZE; available for caller use)
 *   [24..32) — magic: "ALSL\x00\x01\x02\x00"
 *   [32..40) — block_size (8 B LE)
 *   [40..48) — free_head  (8 B LE) — payload offset of first free block, or 0
 *   [48..)   — block arena
 *
 * Allocation:
 *   len == 0         → null handle (offset = 0, len = 0)
 *   len <= block_size → pop from free list, or extend tail by block_size
 *   len > block_size  → extend tail by len.div_ceil(block_size) * block_size
 *
 * Deallocation:
 *   oversized block at tail → bstack_discard (single call; crash-safe)
 *   all other cases         → each block_size chunk prepended to free list
 *
 * Reallocation growing: newly-exposed bytes are always zero-initialised.
 *
 * Crash consistency: without atomic, each free-list update is two bstack calls
 * (write next-pointer then update free_head); a crash between the two leaks the
 * block being added or removed but leaves the rest of the list intact.  With
 * atomic the push is a single bstack_cross_exchange and the pop a single
 * bstack_process_gen sequence, so the leak window is one bstack call.
 *
 * Thread safety: without -DBSTACK_FEATURE_ATOMIC, an allocator handle must be
 * used from one thread at a time — free-list mutations are a read then a write
 * of free_head as separate bstack calls, a TOCTOU race under concurrent access
 * that can result in two callers receiving the same block.  With
 * -DBSTACK_FEATURE_ATOMIC the handle carries no allocator-level lock at all:
 * free-list pop drives a single bstack_process_gen sequence (read free_head,
 * read the popped block's next-pointer, advance free_head — all under one held
 * bstack write lock, closing the ABA window a get/cas pair would leave open);
 * free-list push splices a single block (or a whole freed run) onto the head
 * with one bstack_cross_exchange; and tail grow/shrink use
 * bstack_try_extend_zeros / bstack_try_discard, which check-and-act atomically
 * under bstack's own write lock.  Every concurrent operation is therefore safe
 * through bstack's interior synchronisation alone — no mutex.
 *
 * Requires -DBSTACK_FEATURE_SET.
 * ====================================================================== */

typedef struct {
    bstack_allocator_t base; /* must be first — safe cast to bstack_allocator_t * */
    bstack_t          *bs;
    uint64_t           block_size;
} slab_bstack_allocator_t;

/*
 * Initialise a new slab_bstack_allocator_t over an empty bs.
 *
 * Writes the 48-byte allocator header (24 reserved bytes, magic, block_size,
 * and free_head = 0) and returns a ready allocator. block_size must be >= 8.
 *
 * Returns NULL on failure (errno = EINVAL if bs is non-empty or block_size < 8,
 * ENOMEM on allocation failure, or the errno from any failing bstack operation).
 * Cast the result to bstack_allocator_t * to use the generic interface.
 */
BSTACK_WARN_UNUSED_RESULT
slab_bstack_allocator_t *slab_bstack_allocator_new(bstack_t *bs,
                                                    uint64_t block_size);

/*
 * Open an existing slab_bstack_allocator_t from a non-empty bs.
 *
 * Validates the ALSL 0.1.x magic prefix and reads block_size from the stored
 * header.
 *
 * Returns NULL on failure (errno = EINVAL if bs is empty, the stack is too
 * short, magic is wrong, or the stored block_size is invalid; ENOMEM on
 * allocation failure; or the errno from any failing bstack operation).
 * Cast the result to bstack_allocator_t * to use the generic interface.
 */
BSTACK_WARN_UNUSED_RESULT
slab_bstack_allocator_t *slab_bstack_allocator_open(bstack_t *bs);

/*
 * Free the allocator wrapper without closing the underlying bstack.
 */
void slab_bstack_allocator_free(slab_bstack_allocator_t *alloc);

/*
 * Consume the allocator: free the wrapper and return the underlying bstack.
 * The returned bstack_t * must eventually be passed to bstack_close.
 */
bstack_t *slab_bstack_allocator_into_stack(slab_bstack_allocator_t *alloc);

/*
 * Return the block_size this allocator was created with.
 */
uint64_t slab_bstack_allocator_block_size(const slab_bstack_allocator_t *alloc);

/* =========================================================================
 * checked_slab_bstack_allocator_t — crash-recoverable fixed-block slab allocator
 *
 * Every block in the arena carries an 8-byte **overhead** prefix that records
 * the block's state, making double-free detection and post-crash recovery
 * possible.  The slice returned to the caller covers only the `data` region
 * after the overhead (data_size = block_size − 8 usable bytes per block).
 *
 * On-disk layout (all within the bstack payload):
 *   [0..24)  — reserved (OFFSET_SIZE; available for caller use)
 *   [24..48) — allocator header: magic[8] | block_size[8] | free_head[8]
 *              magic = "ALCK\x00\x01\x03\x00"
 *   [48..)   — block arena
 *
 * Each block in the arena:
 *   [ overhead(8) | data ... ]
 *
 * Overhead encoding:
 *   0x0000_0000_0000_0000 — free; data[0..8] = next free block offset (LE u64)
 *   0x8NNN_NNNN_NNNN_NNNN — in use; NNN... = number of blocks (high bit always 1)
 *
 * Allocation policy:
 *   len == 0            → null sentinel slice (offset = 0, len = 0)
 *   num_blocks == 1     → pop from free list, or extend tail by block_size
 *   num_blocks > 1      → always extend tail (multi-block never uses free list)
 *
 * Deallocation policy:
 *   high bit clear      → double-free error (errno = EINVAL)
 *   multi-block at tail → bstack_discard (crash-safe, single call)
 *   all other cases     → each block prepended to free list
 *
 * Crash consistency: each free-list mutation writes block payloads before the
 * splice (without atomic) or splices atomically with bstack_cross_exchange
 * (with atomic).  A crash leaks at most the block being operated on; the rest
 * of the list stays intact.  checked_slab_bstack_allocator_recover reclaims
 * leaked blocks by a linear arena scan.
 *
 * Thread safety: without -DBSTACK_FEATURE_ATOMIC, an allocator handle must be
 * used from one thread at a time — free-list mutations read then write free_head
 * as separate bstack calls, a TOCTOU race under concurrent access.  With
 * -DBSTACK_FEATURE_ATOMIC, alloc/dealloc/realloc take no allocator-level lock:
 * free-list pop drives a single bstack_process_gen sequence (read free_head,
 * read the popped block's overhead and next-pointer, advance free_head — all
 * under one held bstack write lock, closing the ABA window a get/cas pair would
 * leave open), free-list push splices a block or a whole freed run with one
 * bstack_cross_exchange, and tail grow/shrink use bstack_try_extend_zeros /
 * bstack_try_discard (check-and-act atomically under bstack's own write lock).
 * The handle still owns an in-memory mutex (lock), held only by recover to keep
 * it single-flight: the recover scan and its one optional tail discard run as a
 * single bstack_process_gen sequence (so the bstack write lock, not the mutex,
 * serialises the scan against alloc/dealloc), while the mutex only prevents two
 * concurrent recover runs from each reclaiming the same leaked block.  Ordinary
 * alloc/dealloc/realloc never take it.
 *
 * Requires -DBSTACK_FEATURE_SET.
 * ====================================================================== */

typedef struct {
    bstack_allocator_t base; /* must be first — safe cast to bstack_allocator_t * */
    bstack_t          *bs;
    uint64_t           block_size;
#ifdef BSTACK_FEATURE_ATOMIC
    /* Opaque platform mutex; held only by recover() to keep it single-flight
     * (alloc/dealloc/realloc are lock-free).  Shared across threads. */
    void              *lock;
#endif
} checked_slab_bstack_allocator_t;

/*
 * Initialise a new checked_slab_bstack_allocator_t over an empty bs.
 *
 * data_size is the number of usable bytes per slab block (excluding the 8-byte
 * overhead prefix); block_size = data_size + 8.  data_size must be >= 8.
 * bs must be empty (use checked_slab_bstack_allocator_open for existing files).
 *
 * Returns NULL on failure (errno = EINVAL if bs non-empty or data_size < 8,
 * ENOMEM on allocation failure, or the errno from any failing bstack op).
 */
BSTACK_WARN_UNUSED_RESULT
checked_slab_bstack_allocator_t *checked_slab_bstack_allocator_new(
    bstack_t *bs, uint64_t data_size);

/*
 * Open an existing checked_slab_bstack_allocator_t from a non-empty bs.
 *
 * Validates the ALCK 0.1.x magic prefix, reads block_size, checks the arena
 * tail alignment and the free_head pointer, then automatically calls
 * checked_slab_bstack_allocator_recover to reclaim leaked blocks and discard
 * any orphaned tail.
 *
 * Returns NULL on failure (errno = EINVAL if bs is empty; EINVAL/EPROTO for
 * bad magic, invalid stored values, or misaligned tail; ENOMEM on allocation
 * failure; or the errno from any failing bstack op).
 */
BSTACK_WARN_UNUSED_RESULT
checked_slab_bstack_allocator_t *checked_slab_bstack_allocator_open(
    bstack_t *bs);

/*
 * Repair the allocator after an unclean shutdown.
 *
 * Walks the arena linearly: reclaims leaked free-overhead blocks, resyncs
 * past suspicious regions, and discards any orphaned tail left by a failed
 * realloc truncation.  Writes *out_unsure (if non-NULL) with the count of
 * blocks that could not be classified with certainty (0 = fully recovered).
 *
 * Returns 0 on success, -1 on I/O error (errno set).
 * checked_slab_bstack_allocator_open calls this automatically.
 */
BSTACK_WARN_UNUSED_RESULT
int checked_slab_bstack_allocator_recover(checked_slab_bstack_allocator_t *alloc,
                                           uint64_t *out_unsure);

/*
 * Free the allocator wrapper without closing the underlying bstack.
 */
void checked_slab_bstack_allocator_free(checked_slab_bstack_allocator_t *alloc);

/*
 * Consume the allocator: free the wrapper and return the underlying bstack.
 * The returned bstack_t * must eventually be passed to bstack_close.
 */
bstack_t *checked_slab_bstack_allocator_into_stack(
    checked_slab_bstack_allocator_t *alloc);

/*
 * Return the usable bytes per slab block (data_size = block_size − 8).
 */
uint64_t checked_slab_bstack_allocator_data_size(
    const checked_slab_bstack_allocator_t *alloc);

/* =========================================================================
 * segregated_bstack_allocator_t — segregated (binned) free-list allocator
 *
 * Generalises the fixed-block checked slab to 33 size classes sharing one
 * arena: 16 linear (16‥256 B, step 16), 16 geometric (320‥4096 B, 4 per
 * octave), and one shared oversized bucket.  Each class is an independent
 * intrusive free list; the class is derived from the request by register
 * arithmetic (no tables) for O(1) classed alloc/dealloc.  The size→class
 * policy is a compile-time constant encoded by the magic version, not stored
 * per-file — a format change bumps the magic.
 *
 * On-disk layout (all within the bstack payload):
 *   [0..24)  — reserved (OFFSET_SIZE; available for caller use)
 *   [24..32) — magic: "ALSG\x00\x02\x02\x00"
 *   [32..40) — reserved (no field yet)
 *   [40..40+NUM_CLASSES*8) — free_head[NUM_CLASSES] (last entry = oversized list)
 *   [ARENA_START..) — block arena (16-byte aligned; ARENA_START = 304)
 *
 * Every arena block is [ overhead(8) | data(block − 8) ]; the caller pointer is
 * the data start (block_start + 8, always of the form 16n + 8).  The overhead is
 * a single tagged word carrying the physical block size >> 4 in its low 63 bits
 * under both tags: high bit set ⇒ in use, high bit clear ⇒ free (the size then
 * doubling as the class tag).  The caller's visible length is *not* recorded on
 * disk — it lives in the returned slice handle — so a live block may be
 * physically larger than its request needs (retained excess).  A free block
 * stores its next_free offset inline at the data start, so live allocations
 * carry no overhead beyond the 8-byte word.  Leaked blocks are reclaimable by a
 * linear scan and double-frees are caught.
 *
 * Allocation:
 *   len == 0           → null sentinel slice (offset = 0, len = 0)
 *   classed request    → pop the class head, else extend a fresh class block
 *   oversized request  → reuse an exact/large-enough oversized head (retaining
 *                        the whole block if the excess is below SPLIT_MIN, else
 *                        carving it), else extend
 *
 * Reallocation:
 *   fits the block     → retain in place — no metadata write; zero the newly
 *                        exposed tail on a visible grow
 *   grow past the block, at the tail
 *                      → extend in place (zero-filled), then record the new size
 *   tail shrink, excess ≥ SPLIT_MIN
 *                      → atomic: drop the excess via one LEN + SPLICE, recording
 *                        the new size in the same transaction; without atomic,
 *                        retain in place
 *   interior shrink, or excess < SPLIT_MIN → retain the excess in place — no write
 *   non-tail grow past the block → alloc new class, copy, dealloc old
 *
 * With -DBSTACK_FEATURE_ATOMIC the allocator also carries a bulk vtable: work is
 * bounded by the distinct classes a batch touches, not by its request count —
 * every touched class drains in one multi-class pop, the misses share one sparse
 * extend, and every claim commits in one batched set.  Without it, bulk_vtbl is
 * NULL and bstack_allocator_alloc_bulk fails with ENOTSUP.
 *
 * The visible length lives in the returned handle, not on disk, so a resize that
 * fits (or a shrink whose excess is retained) touches no metadata at all.  Crash
 * consistency: every path only ever *leaks* on a mid-op failure, never corrupts —
 * the leak-preferring tail grow leaves an orphaned zero tail that recover()
 * reclaims, and an atomic tail shrink commits the new size together with the
 * truncation as one transaction, so a crash leaves the block wholly un-shrunk or
 * fully shrunk — never a recorded size disagreeing with the block's physical
 * extent, which would make the recovery scan mis-stride.
 * segregated_bstack_allocator_recover rebuilds every free list from a single
 * linear arena scan and reclaims leaked blocks.
 *
 * Thread safety: without -DBSTACK_FEATURE_ATOMIC an allocator handle must be
 * used from one thread at a time — free-list mutations read then write a head as
 * separate bstack calls.  With -DBSTACK_FEATURE_ATOMIC alloc/dealloc/realloc take
 * no allocator-level lock: free-list pops ride a single bstack_process_gen
 * sequence, pushes and the oversized carve ride bstack_inplace_gen, the tail
 * shrink rides a BSTACK_GEN_LEN + BSTACK_GEN_SPLICE bstack_process_gen, and the
 * tail grow / oversized-discard paths use bstack_try_extend_zeros /
 * bstack_try_discard (check-and-act atomically under bstack's own write lock).
 * The handle carries no mutex at all; recover() is the sole exception and
 * requires a quiescent allocator (see its contract).
 *
 * Without -DBSTACK_FEATURE_ATOMIC a shrink cannot reclaim its freed excess —
 * recording the smaller size and dropping the excess cannot be fused without a
 * transaction, and either ordering leaves a crash window recover() mis-parses —
 * so that build simply retains the excess inside the still-recorded larger block
 * (zero extra writes, no move); a later grow back into that span fits in place.
 *
 * recover() reclaims free leaks and discards orphaned tails, and the allocator
 * creates no in-use orphans of its own (the realloc move commits its
 * new-live/old-free flip atomically). The free-neighbour coalescer
 * segregated_bstack_allocator_coalesce is implemented under BSTACK_FEATURE_ATOMIC.
 *
 * Requires -DBSTACK_FEATURE_SET.
 * ====================================================================== */

typedef struct {
    bstack_allocator_t base; /* must be first — safe cast to bstack_allocator_t * */
    bstack_t          *bs;
} segregated_bstack_allocator_t;

/*
 * Open or initialise a segregated_bstack_allocator_t over bs.
 *
 * - Empty stack: writes the allocator header (reserved + magic + zeroed
 *   free_head table) and returns a fresh allocator.
 * - Non-empty stack: validates the ALSG 0.2 magic prefix and arena alignment,
 *   pads a short header region with zeros, then runs recovery (rebuild every
 *   free list from a linear scan, reclaim leaks, discard any orphaned tail)
 *   before returning.
 *
 * Returns NULL on failure (errno = EINVAL for bad magic/misaligned arena,
 * ENOMEM on allocation failure, or the errno from any failing bstack op).
 * Cast the result to bstack_allocator_t * to use the generic interface.
 */
BSTACK_WARN_UNUSED_RESULT
segregated_bstack_allocator_t *segregated_bstack_allocator_new(bstack_t *bs);

/*
 * Repair the allocator after an unclean shutdown: rebuild every free list from a
 * single linear scan of the arena's overhead words (reclaiming any block leaked
 * by a crashed alloc pop/claim), discard a fully-zeroed orphaned tail, and
 * publish the rebuilt head table as one crash-atomic bstack_set.  Idempotent and
 * crash-safe by re-running.  Writes *out_unsure (if non-NULL) with the count of
 * blocks that could not be classified with certainty (0 = fully recovered).
 *
 * The caller must guarantee the allocator is QUIESCENT for the call: no other
 * thread may run any alloc/dealloc/realloc (or another recover) concurrently —
 * recover snapshots the arena then replaces the whole head table wholesale, and
 * a concurrent operation between the snapshot and the flip would be clobbered.
 * segregated_bstack_allocator_new satisfies this by running it before the handle
 * escapes.
 *
 * Returns 0 on success, -1 on I/O error (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int segregated_bstack_allocator_recover(segregated_bstack_allocator_t *alloc,
                                         uint64_t *out_unsure);

#ifdef BSTACK_FEATURE_ATOMIC
/*
 * Merge physically-adjacent free blocks.  Freed blocks only ever return to their
 * own class list, so adjacent free blocks accumulate without merging and no
 * oversized request can reuse the contiguous run; coalesce fuses them.  It is the
 * recover walk plus a merge: it strides the arena by the recorded physical sizes
 * and, on any run of two or more adjacent free blocks, writes one merged free
 * block in place and rebuilds every free list from the scan (the same wholesale
 * rebuild recover uses, so no swallowed block needs a per-block unlink).  Writes
 * *out_fused (if non-NULL) with the number of blocks fused into a neighbour
 * (0 = nothing was adjacent, and nothing is written).
 *
 * Unlike recover, this needs no quiescence: the whole scan-and-rewrite runs
 * inside one bstack_inplace_gen, striding the overhead words one at a time under
 * the held write lock and committing the merges as one journalled batch, so a
 * concurrent alloc/dealloc can neither observe an intermediate state nor be
 * clobbered — no allocator-level lock.  A torn commit re-parses as a valid arena
 * and is reclaimed by recover, so the pass is restartable; the scan follows only
 * physical sizes, never next_free, so a corrupt free list cannot cycle it.  A run
 * reaching the tail is merged like any other — tail discard is not attempted.
 *
 * Returns 0 on success, -1 on I/O or allocation error (errno set).
 * Requires -DBSTACK_FEATURE_ATOMIC (the merge rides bstack_inplace_gen).
 */
BSTACK_WARN_UNUSED_RESULT
int segregated_bstack_allocator_coalesce(segregated_bstack_allocator_t *alloc,
                                         uint64_t *out_fused);
#endif /* BSTACK_FEATURE_ATOMIC */

/*
 * Free the allocator wrapper without closing the underlying bstack.
 */
void segregated_bstack_allocator_free(segregated_bstack_allocator_t *alloc);

/*
 * Consume the allocator: free the wrapper and return the underlying bstack.
 * The returned bstack_t * must eventually be passed to bstack_close.
 */
bstack_t *segregated_bstack_allocator_into_stack(
    segregated_bstack_allocator_t *alloc);

#endif /* BSTACK_FEATURE_SET */

#ifdef __cplusplus
}
#endif

#endif /* BSTACK_ALLOC_H */
