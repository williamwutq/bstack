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
     * All slices must originate from the same allocator instance.
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
 *              magic  = "ALFF\x00\x01\x03\x00"
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
 *   [32..40) — magic: "ALGT\x00\x01\x02\x00"
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
 *   [24..32) — magic: "ALSL\x00\x01\x01\x00"
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
 *              magic = "ALCK\x00\x01\x01\x00"
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

#endif /* BSTACK_FEATURE_SET */

#ifdef __cplusplus
}
#endif

#endif /* BSTACK_ALLOC_H */
