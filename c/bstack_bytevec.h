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
 * If a reallocation (growth or shrink) fails, the vec adopts whatever the
 * allocator's -1/-2 survivor signal reports: on -1 (original survived) it
 * keeps tracking the real region, which may not be the block it started
 * with; on -2 (original lost) it detaches to the empty sentinel so later
 * operations fail cleanly instead of risking corruption of an unrelated
 * allocation. Either way the bytevec call itself still just reports -1.
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
