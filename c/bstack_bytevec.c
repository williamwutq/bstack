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
 * Absolute payload offset of logical byte index within the backing bstack,
 * i.e. the coordinate accepted by bstack_copy, bstack_cross_exchange and
 * bstack_repeat.  Equal to the block start plus the 16-byte header plus
 * index.  Must be recomputed after any reallocation since the block's start
 * may move.
 */
static uint64_t bytevec_abs_offset(const bstack_bytevec_t *v, uint64_t index)
{
    return bstack_slice_start(v->slice) + BYTEVEC_HEADER_LEN + index;
}

/*
 * Reallocate the block to hold new_cap bytes of data (total block size is
 * BYTEVEC_HEADER_LEN + new_cap).  Updates v->slice on success.
 */
static int bytevec_grow_to(bstack_bytevec_t *v, uint64_t new_cap)
{
    uint64_t new_size;
    bstack_slice_t new_slice;

    if (new_cap > UINT64_MAX - BYTEVEC_HEADER_LEN) {
        errno = EINVAL;
        return -1;
    }
    new_size = BYTEVEC_HEADER_LEN + new_cap;
    if (bstack_allocator_realloc(v->slice.allocator, v->slice,
                                  new_size, &new_slice) != 0)
        return -1;
    v->slice = new_slice;
    return 0;
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
        (void)bstack_allocator_dealloc(a, out->slice);
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
            (void)bstack_allocator_dealloc(a, out->slice);
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

int bstack_bytevec_set(bstack_bytevec_t *v, uint64_t index, uint8_t value,
                        int *out_ok)
{
    uint64_t len, cap;

    if (bytevec_read_header(v, &len, &cap) != 0)
        return -1;
    if (index >= len) {
        *out_ok = 0;
        return 0;
    }
    if (bstack_slice_write_range(v->slice, bytevec_elem_offset(index),
                                  &value, 1) != 0)
        return -1;
    *out_ok = 1;
    return 0;
}

int bstack_bytevec_fill(bstack_bytevec_t *v, uint8_t value)
{
    uint64_t len, cap;

    if (bytevec_read_header(v, &len, &cap) != 0)
        return -1;
    if (len == 0)
        return 0;
    return bstack_repeat(bstack_allocator_stack(v->slice.allocator),
                         bytevec_abs_offset(v, 0), &value, 1, len);
}

/* =========================================================================
 * Atomic byte-mover operations
 * ====================================================================== */

#ifdef BSTACK_FEATURE_ATOMIC

int bstack_bytevec_extend_from_within(bstack_bytevec_t *v, uint64_t start,
                                       uint64_t count, int *out_ok)
{
    uint64_t len, cap;
    bstack_t *stack;

    if (count == 0) {
        *out_ok = 1;
        return 0;
    }
    if (bytevec_read_header(v, &len, &cap) != 0)
        return -1;
    /* Out of bounds (overflow or past len) → not ok, per the get()-style contract. */
    if (count > UINT64_MAX - start || start + count > len) {
        *out_ok = 0;
        return 0;
    }
    if (bstack_bytevec_reserve(v, count) != 0)
        return -1;
    /* Recompute offsets after reserve: a realloc may have moved the block. */
    stack = bstack_allocator_stack(v->slice.allocator);
    if (bstack_copy(stack, bytevec_abs_offset(v, start),
                    bytevec_abs_offset(v, len), count) != 0)
        return -1;
    if (bytevec_write_len(v, len + count) != 0)
        return -1;
    *out_ok = 1;
    return 0;
}

int bstack_bytevec_extend_from_bstack_slice(bstack_bytevec_t *v,
                                             bstack_slice_t src)
{
    uint64_t len, cap, n;
    bstack_t *stack = bstack_allocator_stack(v->slice.allocator);

    if (bstack_allocator_stack(src.allocator) != stack) {
        errno = EINVAL;
        return -1;
    }
    n = bstack_slice_len(src);
    if (n == 0)
        return 0;
    if (bytevec_read_header(v, &len, &cap) != 0)
        return -1;
    if (bstack_bytevec_reserve(v, n) != 0)
        return -1;
    if (bstack_copy(stack, bstack_slice_start(src),
                    bytevec_abs_offset(v, len), n) != 0)
        return -1;
    return bytevec_write_len(v, len + n);
}

int bstack_bytevec_append_from_owned(bstack_bytevec_t *v, bstack_slice_t other)
{
    uint64_t len, cap, n;
    bstack_t *stack = bstack_allocator_stack(v->slice.allocator);
    int appended = 0;
    int saved_errno = 0;
    int freed;

    if (bstack_allocator_stack(other.allocator) != stack) {
        /* Foreign BStack (a misuse).  Free other through its own allocator so
         * the call is not a leak; if that free itself fails, surface its I/O
         * error, otherwise report EINVAL.  Either way other is consumed. */
        if (bstack_allocator_dealloc(other.allocator, other) != 0)
            return -1;
        errno = EINVAL;
        return -1;
    }
    /* Append first, capturing any error, but always fall through to the free so
     * other is never leaked on an append failure. */
    n = bstack_slice_len(other);
    if (n > 0) {
        if (bytevec_read_header(v, &len, &cap) != 0 ||
            bstack_bytevec_reserve(v, n) != 0 ||
            bstack_copy(stack, bstack_slice_start(other),
                        bytevec_abs_offset(v, len), n) != 0 ||
            bytevec_write_len(v, len + n) != 0) {
            appended = -1;
            saved_errno = errno;
        }
    }
    freed = bstack_allocator_dealloc(other.allocator, other);
    if (appended != 0) {
        errno = saved_errno; /* prefer the append error over any dealloc error */
        return -1;
    }
    if (freed != 0)
        return -1;
    return 0;
}

int bstack_bytevec_insert(bstack_bytevec_t *v, uint64_t index, uint8_t value,
                           int *out_ok)
{
    uint64_t len, cap;
    bstack_t *stack;

    if (bytevec_read_header(v, &len, &cap) != 0)
        return -1;
    if (index > len) {
        *out_ok = 0;
        return 0;
    }
    if (bstack_bytevec_reserve(v, 1) != 0)
        return -1;
    stack = bstack_allocator_stack(v->slice.allocator);
    if (index < len) {
        uint64_t n = len - index;
        if (bstack_copy(stack, bytevec_abs_offset(v, index),
                        bytevec_abs_offset(v, index + 1), n) != 0)
            return -1;
    }
    if (bstack_slice_write_range(v->slice, bytevec_elem_offset(index),
                                  &value, 1) != 0)
        return -1;
    if (bytevec_write_len(v, len + 1) != 0)
        return -1;
    *out_ok = 1;
    return 0;
}

int bstack_bytevec_remove(bstack_bytevec_t *v, uint64_t index,
                           uint8_t *out_byte, int *out_ok)
{
    uint64_t len, cap, tail;
    uint8_t  value;

    if (bytevec_read_header(v, &len, &cap) != 0)
        return -1;
    if (index >= len) {
        *out_ok = 0;
        return 0;
    }
    if (bstack_slice_read_range_into(v->slice, bytevec_elem_offset(index),
                                      &value, 1) != 0)
        return -1;
    tail = len - index - 1;
    if (tail > 0) {
        bstack_t *stack = bstack_allocator_stack(v->slice.allocator);
        if (bstack_copy(stack, bytevec_abs_offset(v, index + 1),
                        bytevec_abs_offset(v, index), tail) != 0)
            return -1;
    }
    /* Commit the shorter len first, then zero the vacated tail slot, as in pop. */
    if (bytevec_write_len(v, len - 1) != 0)
        return -1;
    if (bstack_slice_zero_range(v->slice, bytevec_elem_offset(len - 1), 1) != 0)
        return -1;
    if (out_byte) *out_byte = value;
    *out_ok = 1;
    return 0;
}

int bstack_bytevec_swap_remove(bstack_bytevec_t *v, uint64_t index,
                                uint8_t *out_byte, int *out_ok)
{
    uint64_t len, cap, last;
    uint8_t  value;

    if (bytevec_read_header(v, &len, &cap) != 0)
        return -1;
    if (index >= len) {
        *out_ok = 0;
        return 0;
    }
    if (bstack_slice_read_range_into(v->slice, bytevec_elem_offset(index),
                                      &value, 1) != 0)
        return -1;
    last = len - 1;
    if (index != last) {
        bstack_t *stack = bstack_allocator_stack(v->slice.allocator);
        if (bstack_cross_exchange(stack, bytevec_abs_offset(v, index),
                                  bytevec_abs_offset(v, last), 1) != 0)
            return -1;
    }
    if (bytevec_write_len(v, last) != 0)
        return -1;
    if (bstack_slice_zero_range(v->slice, bytevec_elem_offset(last), 1) != 0)
        return -1;
    if (out_byte) *out_byte = value;
    *out_ok = 1;
    return 0;
}

int bstack_bytevec_copy_into_bstack_slice(const bstack_bytevec_t *v,
                                          uint64_t start, bstack_slice_t dst,
                                          int *out_ok)
{
    uint64_t len, cap, n;
    bstack_t *stack = bstack_allocator_stack(v->slice.allocator);

    if (bstack_allocator_stack(dst.allocator) != stack) {
        errno = EINVAL;
        return -1;
    }
    n = bstack_slice_len(dst);
    if (n == 0) {
        *out_ok = 1;
        return 0;
    }
    if (bytevec_read_header(v, &len, &cap) != 0)
        return -1;
    /* Out of bounds (overflow or past len) → not ok. */
    if (n > UINT64_MAX - start || start + n > len) {
        *out_ok = 0;
        return 0;
    }
    if (bstack_copy(stack, bytevec_abs_offset(v, start),
                    bstack_slice_start(dst), n) != 0)
        return -1;
    *out_ok = 1;
    return 0;
}

int bstack_bytevec_move_tail_into(bstack_bytevec_t *v, bstack_slice_t dest,
                                   int *out_ok)
{
    uint64_t len, cap, n, start;
    bstack_t *stack = bstack_allocator_stack(v->slice.allocator);

    if (bstack_allocator_stack(dest.allocator) != stack) {
        errno = EINVAL;
        return -1;
    }
    n = bstack_slice_len(dest);
    if (n == 0) {
        *out_ok = 1;
        return 0;
    }
    if (bytevec_read_header(v, &len, &cap) != 0)
        return -1;
    if (n > len) {
        *out_ok = 0;
        return 0;
    }
    start = len - n;
    if (bstack_cross_exchange(stack, bytevec_abs_offset(v, start),
                              bstack_slice_start(dest), n) != 0)
        return -1;
    if (bstack_bytevec_truncate(v, start) != 0)
        return -1;
    *out_ok = 1;
    return 0;
}

int bstack_bytevec_split_off(bstack_bytevec_t *v, uint64_t at,
                              bstack_bytevec_t *out, int *out_ok)
{
    uint64_t len, cap, tail_len;
    bstack_bytevec_t tail;

    if (bytevec_read_header(v, &len, &cap) != 0)
        return -1;
    if (at > len) {
        *out_ok = 0;
        return 0;
    }
    tail_len = len - at;
    /* Even when tail_len is zero, allocate a real header-only tail vec. */
    if (bstack_bytevec_with_capacity(v->slice.allocator, tail_len, &tail) != 0)
        return -1;
    if (tail_len > 0) {
        bstack_t *stack = bstack_allocator_stack(v->slice.allocator);
        if (bstack_copy(stack, bytevec_abs_offset(v, at),
                        bytevec_abs_offset(&tail, 0), tail_len) != 0) {
            (void)bstack_bytevec_dealloc(tail);
            return -1;
        }
        if (bytevec_write_len(&tail, tail_len) != 0) {
            (void)bstack_bytevec_dealloc(tail);
            return -1;
        }
    }
    if (bstack_bytevec_truncate(v, at) != 0) {
        (void)bstack_bytevec_dealloc(tail);
        return -1;
    }
    *out = tail;
    *out_ok = 1;
    return 0;
}

int bstack_bytevec_drain(bstack_bytevec_t *v, uint64_t start, uint64_t end,
                          uint8_t **out_buf, uint64_t *out_len, int *out_ok)
{
    uint64_t len, cap, count, tail;
    uint8_t *buf;

    if (bytevec_read_header(v, &len, &cap) != 0)
        return -1;
    if (start > end || end > len) {
        *out_ok = 0;
        return 0;
    }
    count = end - start;
    if (count == 0) {
        *out_buf = NULL;
        *out_len = 0;
        *out_ok = 1;
        return 0;
    }
#if UINT64_MAX > SIZE_MAX
    if (count > (uint64_t)SIZE_MAX) {
        errno = EINVAL;
        return -1;
    }
#endif
    buf = (uint8_t *)malloc((size_t)count);
    if (!buf) {
        errno = ENOMEM;
        return -1;
    }
    /* Read the removed bytes out before compacting. */
    if (bstack_slice_read_range(v->slice, bytevec_elem_offset(start),
                                 bytevec_elem_offset(end), buf) != 0) {
        free(buf);
        return -1;
    }
    tail = len - end;
    if (tail > 0) {
        bstack_t *stack = bstack_allocator_stack(v->slice.allocator);
        if (bstack_copy(stack, bytevec_abs_offset(v, end),
                        bytevec_abs_offset(v, start), tail) != 0) {
            free(buf);
            return -1;
        }
    }
    if (bstack_bytevec_truncate(v, len - count) != 0) {
        free(buf);
        return -1;
    }
    *out_buf = buf;
    *out_len = count;
    *out_ok = 1;
    return 0;
}

#endif /* BSTACK_FEATURE_ATOMIC */

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
