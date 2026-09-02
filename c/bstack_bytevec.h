#ifndef BSTACK_BYTEVEC_H
#define BSTACK_BYTEVEC_H

#include "bstack_alloc.h"

/*
 * bstack_bytevec — Growable byte vector backed by a BStack allocation.
 *
 * Requires -DBSTACK_FEATURE_SET (write-back support in the alloc layer).
 *
 * Memory layout
 * -------------
 * The underlying block contains a 16-byte header followed by the byte data:
 *
 *   ┌──────────────────────┬──────────────────────┬────────────────────────────┐
 *   │   len  (8 B, LE u64) │   cap  (8 B, LE u64) │   elements: [u8; cap]      │
 *   └──────────────────────┴──────────────────────┴────────────────────────────┘
 *     byte 0                 byte 8                  byte 16
 *
 * Both len and cap are re-read from the block header on every call so the
 * metadata is recoverable after a crash by reconstructing the handle from the
 * raw block via bstack_bytevec_from_raw_block.
 *
 * Growth strategy
 * ---------------
 * When bstack_bytevec_push would exceed the current capacity, the block is
 * reallocated to max(cap * 2, 4) bytes.  New element space is zero-initialised
 * by the allocator's realloc.
 *
 * Crash consistency
 * -----------------
 * Every individual bstack call (set, zero, extend, discard) is durably synced
 * before returning and is crash-safe in isolation.  Multi-step methods
 * (push, pop, truncate, resize, reserve) issue two or more such calls and are
 * NOT atomic with respect to process crashes.
 *
 * | Method   | Step order                              | Post-crash state        |
 * |----------|-----------------------------------------|-------------------------|
 * | push     | write element → increment len           | Element on disk but len |
 * |          |                                         | not updated; invisible. |
 * | pop      | read value → decrement len → zero slot  | Slot may remain with    |
 * |          |                                         | stale byte beyond len.  |
 * | truncate | write new len → zero removed slots      | Stale bytes may remain  |
 * |          |                                         | beyond new len.         |
 * | resize   | reserve → write elements → write len   | Elements between old    |
 * |          |                                         | and new len partially   |
 * |          |                                         | written.                |
 * | clear    | (delegates to truncate(0))              | See truncate.           |
 * | reserve  | realloc → write cap                    | cap may reflect old     |
 * |          |                                         | value; harmless.        |
 *
 * Thread safety
 * -------------
 * Multiple threads may call const (read-only) accessors concurrently.  Methods
 * that take a non-const pointer (push, pop, truncate, clear, reserve, resize)
 * require exclusive access and must not be called from multiple threads
 * simultaneously without external synchronisation.
 *
 * Feature flags
 * -------------
 * The entire API requires -DBSTACK_FEATURE_SET.
 */

#ifdef BSTACK_FEATURE_SET

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* =========================================================================
 * bstack_bytevec_t
 * ====================================================================== */

typedef struct {
    bstack_slice_t slice; /* full block: header (16 B) followed by byte data */
} bstack_bytevec_t;

/* Accessor macro: retrieve the allocator pointer from the vec.
 * Takes the struct by value: use bstack_bytevec_allocator(v) not
 * bstack_bytevec_allocator(v_ptr). */
#define bstack_bytevec_allocator(v) ((v).slice.allocator)

/* =========================================================================
 * Constructors
 * ====================================================================== */

/*
 * Create an empty bstack_bytevec_t with zero capacity.
 *
 * Allocates a 16-byte block for the header only; the header is
 * zero-initialised (len=0, cap=0) by the allocator.  The first
 * bstack_bytevec_push will trigger a reallocation to capacity 4.
 *
 * Returns 0 on success, -1 on failure (errno set).
 * Writes the initialised vec into *out.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_new(bstack_allocator_t *a, bstack_bytevec_t *out);

/*
 * Create an empty bstack_bytevec_t pre-sized for at least capacity bytes.
 *
 * Allocates a block of (16 + capacity) bytes; len is 0, cap is set to
 * capacity.
 *
 * Returns 0 on success, -1 on failure (errno set).  Sets errno=EINVAL if
 * capacity would overflow uint64_t when adding the 16-byte header.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_with_capacity(bstack_allocator_t *a, uint64_t capacity,
                                  bstack_bytevec_t *out);

/*
 * Allocate a bstack_bytevec_t and populate it from a byte array.
 *
 * The resulting vec has len == capacity == data_len.
 *
 * Returns 0 on success, -1 on failure (errno set).  Sets errno=EINVAL if
 * data_len would overflow uint64_t when adding the 16-byte header.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_from_data(bstack_allocator_t *a,
                              const uint8_t *data, size_t data_len,
                              bstack_bytevec_t *out);

/*
 * Reconstruct a bstack_bytevec_t from a raw block slice.
 *
 * slice must be the original allocation handle (not a sub-slice) returned
 * by one of the bstack_bytevec constructors on the same allocator, and the
 * block header must have been written by a bstack_bytevec.  Passing an
 * unrelated slice produces undefined behaviour.
 *
 * Ownership of slice is transferred to the returned vec.
 */
bstack_bytevec_t bstack_bytevec_from_raw_block(bstack_slice_t slice);

/* =========================================================================
 * Read-only accessors
 * ====================================================================== */

/*
 * Return the number of bytes currently stored.
 *
 * Re-reads len from the block header on every call.
 * Returns 0 on success, -1 on failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_len(const bstack_bytevec_t *v, uint64_t *out_len);

/*
 * Return the number of bytes the current allocation can hold without
 * reallocation.
 *
 * Re-reads cap from the block header on every call.
 * Returns 0 on success, -1 on failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_capacity(const bstack_bytevec_t *v, uint64_t *out_cap);

/*
 * Set *out_empty to 1 if the vec contains no bytes, 0 otherwise.
 * Returns 0 on success, -1 on failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_is_empty(const bstack_bytevec_t *v, int *out_empty);

/*
 * Read the byte at index.
 *
 * On success returns 0.  If index < len, sets *out_byte to the value and
 * *out_found to 1.  If index >= len, sets *out_found to 0 and leaves
 * *out_byte unmodified.
 *
 * Returns -1 on I/O failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_get(const bstack_bytevec_t *v, uint64_t index,
                        uint8_t *out_byte, int *out_found);

/*
 * Read all logical bytes and return them in a newly allocated buffer.
 *
 * On success, *out_buf receives a malloc-allocated buffer of *out_len bytes.
 * The caller is responsible for calling free(*out_buf).
 *
 * If len == 0, sets *out_buf to NULL and *out_len to 0 (no allocation).
 *
 * Returns 0 on success, -1 on failure (errno set; errno=ENOMEM on allocation
 * failure, errno=EINVAL if len exceeds SIZE_MAX).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_read_bytes(const bstack_bytevec_t *v,
                               uint8_t **out_buf, uint64_t *out_len);

/* =========================================================================
 * Raw block access
 * ====================================================================== */

/*
 * Return the underlying block slice (header + all allocated byte space).
 *
 * The returned slice is the original allocation handle and may be passed to
 * bstack_allocator_realloc or bstack_allocator_dealloc.
 *
 * WARNING: The returned handle is invalidated by any call that may
 * reallocate the vec (bstack_bytevec_push, bstack_bytevec_reserve,
 * bstack_bytevec_resize).  Using a stale handle with realloc or dealloc
 * can corrupt allocator state or lose data.  Re-fetch with
 * bstack_bytevec_raw_block after any mutation that may reallocate.
 */
bstack_slice_t bstack_bytevec_raw_block(const bstack_bytevec_t *v);

/*
 * Consume the vec and return the underlying block slice.
 *
 * The caller takes full responsibility for the allocation.  Reconstruct
 * the handle later with bstack_bytevec_from_raw_block.
 */
bstack_slice_t bstack_bytevec_into_raw_block(bstack_bytevec_t v);

/* =========================================================================
 * Mutating operations
 * ====================================================================== */

/*
 * Append value to the end of the vec.
 *
 * If len == capacity, reallocates to max(cap * 2, 4) bytes before writing.
 * Crash-safety: element is written before len is incremented; a crash after
 * the element write leaves the element on disk but invisible (len not yet
 * updated).
 *
 * Returns 0 on success, -1 on failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_push(bstack_bytevec_t *v, uint8_t value);

/*
 * Remove and return the last byte.
 *
 * On success returns 0.  If the vec is non-empty, writes the removed byte
 * into *out_byte (when non-NULL) and sets *out_popped to 1; decrements len
 * first, then zeros the vacated slot.  If the vec is empty, sets *out_popped
 * to 0 and leaves *out_byte unmodified.
 *
 * Returns -1 on I/O failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_pop(bstack_bytevec_t *v, uint8_t *out_byte, int *out_popped);

/*
 * Shorten the vec to new_len bytes.
 *
 * No-op when new_len >= len.  Writes the new len first, then zeros all
 * removed slots in one call; capacity is unchanged.
 *
 * Returns 0 on success, -1 on failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_truncate(bstack_bytevec_t *v, uint64_t new_len);

/*
 * Remove all bytes without releasing the allocation.
 *
 * Equivalent to bstack_bytevec_truncate(v, 0).
 * Returns 0 on success, -1 on failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_clear(bstack_bytevec_t *v);

/*
 * Reserve capacity for at least additional more bytes.
 *
 * After this call capacity() >= len() + additional.  Does nothing if the
 * current capacity is already sufficient.
 *
 * Returns 0 on success.  Returns -1 with errno=EINVAL if
 * len + additional overflows uint64_t.  Returns -1 with errno from the
 * failing bstack call on I/O error.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_reserve(bstack_bytevec_t *v, uint64_t additional);

/*
 * Set the length to new_len, filling any new slots with value.
 *
 * If new_len <= len, equivalent to bstack_bytevec_truncate(v, new_len) and
 * value is ignored.  For growth, elements between the old and new len are
 * written in one batched call before the new len is committed.
 *
 * Returns 0 on success.  Returns -1 with errno=EINVAL if the growth count
 * exceeds SIZE_MAX.  Returns -1 with errno=ENOMEM on allocation failure.
 * Returns -1 with errno from the failing bstack call on I/O error.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_resize(bstack_bytevec_t *v, uint64_t new_len, uint8_t value);

/*
 * Overwrite the byte at index with value.
 *
 * A single in-place, crash-atomic write to an existing slot; capacity and len
 * are unchanged.  If index < len the byte is written and *out_ok is set to 1;
 * if index >= len nothing is written and *out_ok is set to 0 (mirroring
 * bstack_bytevec_get).
 *
 * Returns 0 on success — including the out-of-range no-op — and -1 on I/O
 * failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_set(bstack_bytevec_t *v, uint64_t index, uint8_t value,
                        int *out_ok);

/*
 * Overwrite every logical byte with value.
 *
 * Backed by a single bstack_repeat, so the whole populated region is filled
 * crash-atomically with a fixed-size journal regardless of len.  A no-op on an
 * empty vec; capacity and len are unchanged.
 *
 * Returns 0 on success, -1 on I/O failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_fill(bstack_bytevec_t *v, uint8_t value);

/* =========================================================================
 * Atomic byte-mover operations
 * ====================================================================== */

#ifdef BSTACK_FEATURE_ATOMIC
/*
 * The operations in this block are built on the crash-atomic in-file byte
 * movers bstack_copy and bstack_cross_exchange, and so additionally require
 * -DBSTACK_FEATURE_ATOMIC.  When the library is compiled without it, none of
 * these functions are declared or defined; the base API above is unaffected.
 *
 * Crash-consistency classes
 * --------------------------
 * The append-only movers (extend_from_within, extend_from_bstack_slice,
 * append_from_owned) copy into spare capacity and commit len last, so a crash
 * before the commit leaves the extra bytes invisible — the same benign,
 * re-runnable model as push.
 *
 * The in-place movers (insert, remove, swap_remove, drain, split_off,
 * move_tail_into) mutate the live region before committing the new len.  Every
 * individual bstack call is still crash-atomic, so the on-disk (len, cap)
 * header is never left invalid, but the multi-step method is not atomic: a
 * crash between the byte move and the len commit leaves a logically torn (but
 * structurally valid) vec that is not automatically recovered.
 *
 * Cross-BStack misuse
 * -------------------
 * The cross-slice functions (extend_from_bstack_slice, copy_into_bstack_slice,
 * append_from_owned, move_tail_into) copy bytes within a single backing BStack.
 * Passing a slice or owned block backed by a different BStack is rejected with
 * errno = EINVAL, matching the overflow-check convention of
 * bstack_bytevec_with_capacity; append_from_owned still consumes (frees) its
 * argument on that path so the call is never a leak.
 */

/*
 * Append a copy of the existing bytes [start, start + count) to the end of the
 * vec.
 *
 * Backed by a single crash-atomic bstack_copy into spare capacity; benign
 * crash model identical to push.  If start + count is in bounds (does not
 * overflow uint64_t and is <= len) the bytes are appended and *out_ok is set
 * to 1; otherwise nothing is appended and *out_ok is set to 0.  An empty range
 * (count == 0) is a successful no-op with *out_ok = 1.
 *
 * Returns 0 on success — including the out-of-range no-op — and -1 on I/O
 * failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_extend_from_within(bstack_bytevec_t *v, uint64_t start,
                                       uint64_t count, int *out_ok);

/*
 * Append the bytes of an on-disk slice to the end of the vec.
 *
 * src must be backed by the same BStack as this vec (the bytes are copied
 * within one file); its issuing allocator need not be the same.  Backed by a
 * single crash-atomic bstack_copy into spare capacity; benign crash model
 * identical to push.  An empty src is a successful no-op.
 *
 * Returns 0 on success.  Returns -1 with errno = EINVAL if src is backed by a
 * different BStack; -1 with errno from the failing bstack call on I/O error.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_extend_from_bstack_slice(bstack_bytevec_t *v,
                                             bstack_slice_t src);

/*
 * Append the bytes of the owned raw block other to the vec, then deallocate
 * other — a move that consumes the handle.
 *
 * other's bytes are copied into spare capacity with a single crash-atomic
 * bstack_copy, len is committed, and other is freed through its allocator.
 * other must be backed by the same BStack as this vec.  The copy targets
 * invisible spare capacity and len is committed before the free, so a crash
 * before the free leaves the vec correct with other merely still allocated
 * (recoverable), never data loss.
 *
 * other is consumed — and, wherever possible, freed — on every path, so it is
 * never leaked silently.
 *
 * Returns 0 on success.  Returns -1 with errno = EINVAL if other is backed by
 * a different BStack (other is still freed); otherwise -1 propagates the
 * append or dealloc I/O error.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_append_from_owned(bstack_bytevec_t *v, bstack_slice_t other);

/*
 * Insert value at index, shifting every byte at or after index one slot to the
 * right.
 *
 * The shift is a single crash-atomic bstack_copy.  In-place mover (see the
 * block note).  If index <= len the byte is inserted and *out_ok is set to 1;
 * if index > len nothing is inserted and *out_ok is set to 0.
 *
 * Returns 0 on success — including the out-of-range no-op — and -1 on I/O
 * failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_insert(bstack_bytevec_t *v, uint64_t index, uint8_t value,
                           int *out_ok);

/*
 * Remove and return the byte at index, shifting every later byte one slot to
 * the left (preserves order).
 *
 * The shift is a single crash-atomic bstack_copy; the vacated tail slot is then
 * zeroed as in pop.  In-place mover (see the block note).  If index < len the
 * byte is removed, written into *out_byte (when non-NULL), and *out_ok is set
 * to 1; if index >= len nothing is removed and *out_ok is set to 0.
 *
 * Returns 0 on success — including the out-of-range no-op — and -1 on I/O
 * failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_remove(bstack_bytevec_t *v, uint64_t index,
                           uint8_t *out_byte, int *out_ok);

/*
 * Remove the byte at index and return it, replacing the hole with the last
 * byte (O(1), does NOT preserve order).
 *
 * Uses a single crash-atomic bstack_cross_exchange to swap the element into the
 * tail slot, which is then dropped as in pop.  In-place mover (see the block
 * note).  If index < len the byte is removed, written into *out_byte (when
 * non-NULL), and *out_ok is set to 1; if index >= len nothing is removed and
 * *out_ok is set to 0.
 *
 * Returns 0 on success — including the out-of-range no-op — and -1 on I/O
 * failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_swap_remove(bstack_bytevec_t *v, uint64_t index,
                                uint8_t *out_byte, int *out_ok);

/*
 * Copy bstack_slice_len(dst) bytes from the vec, starting at logical start,
 * into dst (overwriting it).
 *
 * dst must be backed by the same BStack as this vec.  A single crash-atomic
 * bstack_copy; the vec itself is not modified.  If start + dst.len is in bounds
 * (does not overflow uint64_t and is <= len) the copy is performed and *out_ok
 * is set to 1; otherwise nothing is copied and *out_ok is set to 0.  An empty
 * dst is a successful no-op with *out_ok = 1.
 *
 * Returns 0 on success — including the out-of-range no-op.  Returns -1 with
 * errno = EINVAL if dst is backed by a different BStack (a misuse, distinct
 * from an out-of-range request); otherwise -1 on I/O failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_copy_into_bstack_slice(const bstack_bytevec_t *v,
                                          uint64_t start, bstack_slice_t dst,
                                          int *out_ok);

/*
 * Move the last bstack_slice_len(dest) bytes of the vec into dest, shrinking
 * the vec by that many bytes.
 *
 * The tail is swapped into dest with a single crash-atomic
 * bstack_cross_exchange, and the vacated tail — now holding dest's former
 * contents — is dropped and zeroed by shrinking len via truncate.  dest must be
 * backed by the same BStack and sized to exactly the tail being moved.
 * In-place mover (see the block note).  If dest.len <= len the move is
 * performed and *out_ok is set to 1; if dest.len > len the vec is unchanged and
 * *out_ok is set to 0.  A zero-length dest is a successful no-op.
 *
 * Returns 0 on success — including the out-of-range no-op.  Returns -1 with
 * errno = EINVAL if dest is backed by a different BStack; otherwise -1 on I/O
 * failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_move_tail_into(bstack_bytevec_t *v, bstack_slice_t dest,
                                   int *out_ok);

/*
 * Split the vec in two at at: v keeps [0, at) and a new vec holding [at, len)
 * is written into *out.
 *
 * The new vec is allocated with exactly len - at bytes of capacity and the tail
 * is transferred with a single crash-atomic bstack_copy straight between the
 * two blocks.  In-place mover (see the block note).  If at <= len the split is
 * performed, the tail vec is written into *out, and *out_ok is set to 1
 * (at == len yields an empty tail); if at > len the vec is unchanged, *out is
 * left untouched, and *out_ok is set to 0.
 *
 * The caller owns the tail vec written to *out and must eventually
 * bstack_bytevec_dealloc it.
 *
 * Returns 0 on success — including the out-of-range no-op — and -1 on I/O
 * failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_split_off(bstack_bytevec_t *v, uint64_t at,
                              bstack_bytevec_t *out, int *out_ok);

/*
 * Remove the bytes in [start, end), shifting every later byte down to close the
 * gap, and return the removed bytes.
 *
 * The removed bytes are read out first, the tail (if any) is shifted down with
 * a single crash-atomic bstack_copy, and the shorter len is committed last.
 * In-place mover (see the block note).  If start <= end and end <= len the
 * bytes are removed and *out_ok is set to 1; otherwise the vec is unchanged and
 * *out_ok is set to 0.
 *
 * On a successful removal of a non-empty range, *out_buf receives a
 * malloc-allocated buffer of *out_len bytes that the caller must free().  An
 * empty range (start == end) is a successful no-op that sets *out_buf to NULL
 * and *out_len to 0 (no allocation).
 *
 * Returns 0 on success — including the out-of-range no-op.  Returns -1 with
 * errno = ENOMEM on allocation failure, errno = EINVAL if the removed count
 * exceeds SIZE_MAX, or errno from the failing bstack call on I/O error.
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_drain(bstack_bytevec_t *v, uint64_t start, uint64_t end,
                          uint8_t **out_buf, uint64_t *out_len, int *out_ok);

#endif /* BSTACK_FEATURE_ATOMIC */

/* =========================================================================
 * Deallocation
 * ====================================================================== */

/*
 * Deallocate the underlying block and consume the vec.
 *
 * After this call no further I/O on any slice handle derived from this vec
 * is valid.  The vec value must not be used after calling this function.
 *
 * Returns 0 on success, -1 on failure (errno set).
 */
BSTACK_WARN_UNUSED_RESULT
int bstack_bytevec_dealloc(bstack_bytevec_t v);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* BSTACK_FEATURE_SET */

#endif /* BSTACK_BYTEVEC_H */
