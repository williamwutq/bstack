#ifndef BSTACK_ALLOC_H
#define BSTACK_ALLOC_H

#include "bstack.h"

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
 * bstack_slice_reader_t     — cursor-based reader over a bstack_slice_t.
 * linear_bstack_allocator_t — bump allocator; every operation maps to one call.
 *
 * Compile with -DBSTACK_FEATURE_SET to enable bstack_slice_write and friends.
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
 * Read the entire slice into buf.
 * buf must have room for at least s.len bytes; no overflow check is done.
 * Returns 0 on success, -1 on failure (errno set).
 */
int bstack_slice_read(bstack_slice_t s, uint8_t *buf);

/*
 * Read min(buf_len, s.len) bytes from the start of the slice into buf.
 * Returns 0 on success, -1 on failure (errno set).
 */
int bstack_slice_read_into(bstack_slice_t s, uint8_t *buf, size_t buf_len);

/*
 * Read buf_len bytes starting at slice-relative offset start into buf.
 * Returns -1 with errno = EINVAL if start + buf_len exceeds s.len or would
 * overflow uint64_t.
 */
int bstack_slice_read_range_into(bstack_slice_t s, uint64_t start,
                                  uint8_t *buf, size_t buf_len);

/*
 * Produce the sub-range [start, end) relative to this slice into *out.
 * start and end are 0-based within the slice (not the payload).
 * Returns -1 with errno = EINVAL if start > end or end > s.len.
 */
int bstack_slice_subslice(bstack_slice_t s, uint64_t start, uint64_t end,
                           bstack_slice_t *out);

#ifdef BSTACK_FEATURE_SET
/*
 * Overwrite the first min(data_len, s.len) bytes of the slice in place.
 * Requires -DBSTACK_FEATURE_SET.
 */
int bstack_slice_write(bstack_slice_t s,
                        const uint8_t *data, size_t data_len);

/*
 * Overwrite [start, start+data_len) within this slice in place.
 * start is 0-based within the slice.
 * Returns -1 with errno = EINVAL if start + data_len exceeds s.len.
 * Requires -DBSTACK_FEATURE_SET.
 */
int bstack_slice_write_range(bstack_slice_t s, uint64_t start,
                              const uint8_t *data, size_t data_len);

/*
 * Zero the entire slice in place.
 * Requires -DBSTACK_FEATURE_SET.
 */
int bstack_slice_zero(bstack_slice_t s);

/*
 * Zero [start, start+n) within this slice in place.
 * start is 0-based within the slice.
 * Returns -1 with errno = EINVAL if start + n exceeds s.len.
 * Requires -DBSTACK_FEATURE_SET.
 */
int bstack_slice_zero_range(bstack_slice_t s, uint64_t start, uint64_t n);
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
int bstack_slice_reader_seek_cur(bstack_slice_reader_t *r, int64_t delta,
                                  uint64_t *out_pos);

/*
 * Seek relative to the end of the slice (cursor = slice.len + delta).
 * Returns -1 with errno = EINVAL if the resulting position would be negative.
 * If out_pos is non-NULL it receives the new cursor position.
 */
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
     * into *out.  Returns 0 on success, -1 on failure (errno set).
     * May return -1 with errno = ENOTSUP if the implementation does not
     * support the requested resize (e.g. non-tail resize on a bump allocator).
     */
    int (*realloc)(bstack_allocator_t *self, bstack_slice_t slice,
                   uint64_t new_len, bstack_slice_t *out);

    /*
     * Release the region described by slice.
     * After dealloc, slice must not be used for further I/O.
     * May be NULL to indicate a permanent no-op; bstack_allocator_dealloc
     * checks for NULL before dispatching.
     * Returns 0 on success, -1 on failure (errno set).
     */
    int (*dealloc)(bstack_allocator_t *self, bstack_slice_t slice);
} bstack_allocator_vtbl_t;

struct bstack_allocator {
    const bstack_allocator_vtbl_t *vtbl;
};

/* -------------------------------------------------------------------------
 * Vtable forwarding helpers — thin static inline wrappers.
 * ---------------------------------------------------------------------- */

static inline bstack_t *
bstack_allocator_stack(bstack_allocator_t *a)
{
    return a->vtbl->stack(a);
}

static inline int
bstack_allocator_alloc(bstack_allocator_t *a, uint64_t len, bstack_slice_t *out)
{
    return a->vtbl->alloc(a, len, out);
}

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
static inline int
bstack_allocator_len(bstack_allocator_t *a, uint64_t *out_len)
{
    return bstack_len(bstack_allocator_stack(a), out_len);
}

/*
 * Set *out_empty to 1 if the backing stack is empty, 0 otherwise.
 * Delegates to bstack_len; returns 0 on success, -1 on failure (errno set).
 */
static inline int
bstack_allocator_is_empty(bstack_allocator_t *a, int *out_empty)
{
    uint64_t len;
    int r = bstack_allocator_len(a, &len);
    if (r == 0)
        *out_empty = (len == 0);
    return r;
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

#ifdef __cplusplus
}
#endif

#endif /* BSTACK_ALLOC_H */
