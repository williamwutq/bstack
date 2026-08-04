#ifndef _WIN32
#  define _DARWIN_C_SOURCE
#  define _DEFAULT_SOURCE
#  define _POSIX_C_SOURCE 200809L
#  define _XOPEN_SOURCE 700
#endif

#include "bstack_bytevec.h"

#ifdef BSTACK_FEATURE_SET

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Byte offset of the first element within the block (past the 16-byte header). */
#define BYTEVEC_HEADER_LEN UINT64_C(16)

/* Absolute byte offset of element at index (0-based). */
#define bytevec_elem_offset(index) (BYTEVEC_HEADER_LEN + (uint64_t)(index))

/* =========================================================================
 * Private helpers
 * ====================================================================== */

/* Decode 8 bytes from buf[0..8) as a little-endian uint64_t. */
static uint64_t le64_read(const uint8_t b[8])
{
    int i;
    uint64_t v = 0;
    for (i = 0; i < 8; i++)
        v |= (uint64_t)b[i] << (8 * i);
    return v;
}

/* Encode val as 8 little-endian bytes into buf[0..8). */
static void le64_write(uint8_t b[8], uint64_t val)
{
    int i;
    for (i = 0; i < 8; i++)
        b[i] = (uint8_t)(val >> (8 * i));
}

/* Re-read (len, cap) from the 16-byte block header. */
static int bytevec_read_header(const bstack_bytevec_t *v,
                                uint64_t *out_len, uint64_t *out_cap)
{
    uint8_t hdr[16];
    if (bstack_slice_read_range(v->slice, 0, 16, hdr) != 0)
        return -1;
    *out_len = le64_read(hdr);
    *out_cap = le64_read(hdr + 8);
    return 0;
}

/* Write the len field (bytes 0..8) of the header. */
static int bytevec_write_len(const bstack_bytevec_t *v, uint64_t len)
{
    uint8_t b[8];
    le64_write(b, len);
    return bstack_slice_write_range(v->slice, 0, b, 8);
}

/* Write the cap field (bytes 8..16) of the header. */
static int bytevec_write_cap(const bstack_bytevec_t *v, uint64_t cap)
{
    uint8_t b[8];
    le64_write(b, cap);
    return bstack_slice_write_range(v->slice, 8, b, 8);
}

/* Write both len and cap in a single 16-byte write. */
static int bytevec_write_header(const bstack_bytevec_t *v,
                                  uint64_t len, uint64_t cap)
{
    uint8_t hdr[16];
    le64_write(hdr,     len);
    le64_write(hdr + 8, cap);
    return bstack_slice_write_range(v->slice, 0, hdr, 16);
}

/* Saturating double of cap, used for growth decisions. */
static uint64_t sat_double(uint64_t cap)
{
    return (cap >= UINT64_MAX / 2) ? UINT64_MAX : cap * 2;
}

/*
 * Reallocate the block to hold new_cap bytes of data (total block size is
 * BYTEVEC_HEADER_LEN + new_cap).  Handles both growth (push, reserve,
 * reserve_exact) and shrink (shrink_to, shrink_to_fit).
 *
 * On failure, bstack_allocator_realloc reports whether the original
 * allocation survived:
 *   -1 — survived (untouched, or a fully-committed replacement region);
 *        adopt the handle written to new_slice so v tracks the real region
 *        instead of a stale one.
 *   -2 — genuinely lost mid-operation.  Keeping the old handle would leave v
 *        pointing at a freed region a later allocation may reuse, so a
 *        subsequent push could corrupt an unrelated allocation.  Detach to
 *        the empty sentinel instead: v loses its backing (its contents are
 *        gone with the allocation) and every later operation on it fails
 *        cleanly rather than risking corruption.
 * Either way this function itself still reports plain failure (-1) to its
 * callers, matching the rest of the bytevec API.
 */
static int bytevec_grow_to(bstack_bytevec_t *v, uint64_t new_cap)
{
    uint64_t new_size;
    bstack_slice_t new_slice;
    bstack_allocator_t *a = v->slice.allocator;
    int r;

    if (new_cap > UINT64_MAX - BYTEVEC_HEADER_LEN) {
        errno = EINVAL;
        return -1;
    }
    new_size = BYTEVEC_HEADER_LEN + new_cap;
    r = bstack_allocator_realloc(a, v->slice, new_size, &new_slice);
    if (r == 0) {
        v->slice = new_slice;
        return 0;
    }
    if (r == -1) {
        v->slice = new_slice;
        return -1;
    }
    v->slice = bstack_slice_empty(a);
    return -1;
}

/* =========================================================================
 * Constructors
 * ====================================================================== */

int bstack_bytevec_new(bstack_allocator_t *a, bstack_bytevec_t *out)
{
    /* Allocate header-only block; header is zero-initialised: len=0, cap=0. */
    return bstack_allocator_alloc(a, BYTEVEC_HEADER_LEN, &out->slice);
}

int bstack_bytevec_with_capacity(bstack_allocator_t *a, uint64_t capacity,
                                  bstack_bytevec_t *out)
{
    uint64_t block_size;

    if (capacity > UINT64_MAX - BYTEVEC_HEADER_LEN) {
        errno = EINVAL;
        return -1;
    }
    block_size = BYTEVEC_HEADER_LEN + capacity;
    if (bstack_allocator_alloc(a, block_size, &out->slice) != 0)
        return -1;
    /* len is already 0 (zeroed by alloc); write the non-zero cap field. */
    if (bytevec_write_cap(out, capacity) != 0) {
        bstack_allocator_dealloc(a, out->slice);
        return -1;
    }
    return 0;
}

int bstack_bytevec_from_data(bstack_allocator_t *a,
                              const uint8_t *data, size_t data_len,
                              bstack_bytevec_t *out)
{
    uint64_t len       = (uint64_t)data_len;
    uint64_t block_size;

    if (len > UINT64_MAX - BYTEVEC_HEADER_LEN) {
        errno = EINVAL;
        return -1;
    }
    block_size = BYTEVEC_HEADER_LEN + len;
    if (bstack_allocator_alloc(a, block_size, &out->slice) != 0)
        return -1;
    if (len > 0) {
        if (bytevec_write_header(out, len, len) != 0 ||
            bstack_slice_write_range(out->slice, BYTEVEC_HEADER_LEN,
                                     data, data_len) != 0) {
            bstack_allocator_dealloc(a, out->slice);
            return -1;
        }
    }
    return 0;
}

bstack_bytevec_t bstack_bytevec_from_raw_block(bstack_slice_t slice)
{
    bstack_bytevec_t v;
    v.slice = slice;
    return v;
}

/* =========================================================================
 * Read-only accessors
 * ====================================================================== */

int bstack_bytevec_len(const bstack_bytevec_t *v, uint64_t *out_len)
{
    uint64_t cap;
    return bytevec_read_header(v, out_len, &cap);
}

int bstack_bytevec_capacity(const bstack_bytevec_t *v, uint64_t *out_cap)
{
    uint64_t len;
    return bytevec_read_header(v, &len, out_cap);
}

int bstack_bytevec_is_empty(const bstack_bytevec_t *v, int *out_empty)
{
    uint64_t len;
    if (bstack_bytevec_len(v, &len) != 0)
        return -1;
    *out_empty = (len == 0);
    return 0;
}

int bstack_bytevec_get(const bstack_bytevec_t *v, uint64_t index,
                        uint8_t *out_byte, int *out_found)
{
    uint64_t len, cap;
    if (bytevec_read_header(v, &len, &cap) != 0)
        return -1;
    if (index >= len) {
        *out_found = 0;
        return 0;
    }
    if (bstack_slice_read_range_into(v->slice, bytevec_elem_offset(index),
                                      out_byte, 1) != 0)
        return -1;
    *out_found = 1;
    return 0;
}

int bstack_bytevec_read_bytes(const bstack_bytevec_t *v,
                               uint8_t **out_buf, uint64_t *out_len)
{
    uint64_t len, cap;
    uint8_t *buf;

    if (bytevec_read_header(v, &len, &cap) != 0)
        return -1;
    if (len == 0) {
        *out_buf = NULL;
        *out_len = 0;
        return 0;
    }
#if UINT64_MAX > SIZE_MAX
    if (len > (uint64_t)SIZE_MAX) {
        errno = EINVAL;
        return -1;
    }
#endif
    buf = (uint8_t *)malloc((size_t)len);
    if (!buf) {
        errno = ENOMEM;
        return -1;
    }
    if (bstack_slice_read_range(v->slice,
                                 BYTEVEC_HEADER_LEN,
                                 BYTEVEC_HEADER_LEN + len,
                                 buf) != 0) {
        free(buf);
        return -1;
    }
    *out_buf = buf;
    *out_len = len;
    return 0;
}

/* =========================================================================
 * Raw block access
 * ====================================================================== */

bstack_slice_t bstack_bytevec_raw_block(const bstack_bytevec_t *v)
{
    return v->slice;
}

bstack_slice_t bstack_bytevec_into_raw_block(bstack_bytevec_t v)
{
    return v.slice;
}

/* =========================================================================
 * Mutating operations
 * ====================================================================== */

int bstack_bytevec_push(bstack_bytevec_t *v, uint8_t value)
{
    uint64_t len, cap;

    if (bytevec_read_header(v, &len, &cap) != 0)
        return -1;
    if (len == cap) {
        /* Saturating double, minimum 4 — mirrors Rust saturating_mul(2).max(4). */
        uint64_t new_cap = sat_double(cap);
        if (new_cap < 4) new_cap = 4;
        if (bytevec_grow_to(v, new_cap) != 0)
            return -1;
        if (bytevec_write_cap(v, new_cap) != 0)
            return -1;
    }
    if (bstack_slice_write_range(v->slice, bytevec_elem_offset(len),
                                  &value, 1) != 0)
        return -1;
    return bytevec_write_len(v, len + 1);
}

int bstack_bytevec_pop(bstack_bytevec_t *v, uint8_t *out_byte, int *out_popped)
{
    uint64_t len, cap;
    uint8_t  val;

    if (bytevec_read_header(v, &len, &cap) != 0)
        return -1;
    if (len == 0) {
        *out_popped = 0;
        return 0;
    }
    if (bstack_slice_read_range_into(v->slice,
                                      bytevec_elem_offset(len - 1),
                                      &val, 1) != 0)
        return -1;
    /* Decrement len first, then zero the vacated slot. */
    if (bytevec_write_len(v, len - 1) != 0)
        return -1;
    if (bstack_slice_zero_range(v->slice,
                                 bytevec_elem_offset(len - 1), 1) != 0)
        return -1;
    if (out_byte) *out_byte = val;
    *out_popped = 1;
    return 0;
}

int bstack_bytevec_truncate(bstack_bytevec_t *v, uint64_t new_len)
{
    uint64_t len, cap, start, removed;

    if (bytevec_read_header(v, &len, &cap) != 0)
        return -1;
    if (new_len >= len)
        return 0;
    start   = bytevec_elem_offset(new_len);
    removed = len - new_len;
    /* Write new len first, then zero the removed slots. */
    if (bytevec_write_len(v, new_len) != 0)
        return -1;
#if UINT64_MAX > SIZE_MAX
    if (removed > (uint64_t)SIZE_MAX) {
        errno = EINVAL;
        return -1;
    }
#endif
    return bstack_slice_zero_range(v->slice, start, removed);
}

int bstack_bytevec_clear(bstack_bytevec_t *v)
{
    return bstack_bytevec_truncate(v, 0);
}

int bstack_bytevec_reserve(bstack_bytevec_t *v, uint64_t additional)
{
    uint64_t len, cap, needed, new_cap;

    if (bytevec_read_header(v, &len, &cap) != 0)
        return -1;
    if (additional > UINT64_MAX - len) {
        errno = EINVAL;
        return -1;
    }
    needed = len + additional;
    if (needed <= cap)
        return 0;
    /* new_cap = max(needed, saturating_double(cap)) */
    new_cap = sat_double(cap);
    if (needed > new_cap) new_cap = needed;
    if (bytevec_grow_to(v, new_cap) != 0)
        return -1;
    return bytevec_write_cap(v, new_cap);
}

int bstack_bytevec_resize(bstack_bytevec_t *v, uint64_t new_len, uint8_t value)
{
    uint64_t len, cap, additional;
    uint8_t *fill;
    int r;

    if (bytevec_read_header(v, &len, &cap) != 0)
        return -1;
    if (new_len <= len)
        return bstack_bytevec_truncate(v, new_len);

    additional = new_len - len;
    if (bstack_bytevec_reserve(v, additional) != 0)
        return -1;

#if UINT64_MAX > SIZE_MAX
    if (additional > (uint64_t)SIZE_MAX) {
        errno = EINVAL;
        return -1;
    }
#endif
    fill = (uint8_t *)malloc((size_t)additional);
    if (!fill) {
        errno = ENOMEM;
        return -1;
    }
    memset(fill, value, (size_t)additional);
    /* Write all new elements in one call, then commit the new len. */
    r = bstack_slice_write_range(v->slice, bytevec_elem_offset(len),
                                  fill, (size_t)additional);
    free(fill);
    if (r != 0)
        return -1;
    return bytevec_write_len(v, new_len);
}

/* =========================================================================
 * Deallocation
 * ====================================================================== */

int bstack_bytevec_dealloc(bstack_bytevec_t v)
{
    return bstack_allocator_dealloc(v.slice.allocator, v.slice);
}

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* BSTACK_FEATURE_SET */
