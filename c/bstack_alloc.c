#ifndef _WIN32
#  define _DARWIN_C_SOURCE
#  define _DEFAULT_SOURCE
#  define _POSIX_C_SOURCE 200809L
#  define _XOPEN_SOURCE 700
#endif

#include "bstack_alloc.h"

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* ENOTSUP is POSIX:2008 but absent on some toolchains (notably MSVC). */
#ifndef ENOTSUP
#  ifdef EOPNOTSUPP
#    define ENOTSUP EOPNOTSUPP
#  else
#    define ENOTSUP EINVAL
#  endif
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* -------------------------------------------------------------------------
 * Internal helper — extract the bstack from a slice without an extra call.
 * ---------------------------------------------------------------------- */

static inline bstack_t *slice_stack(bstack_slice_t s)
{
    return s.allocator->vtbl->stack(s.allocator);
}

/* =========================================================================
 * bstack_slice_t — serialization
 * ====================================================================== */

void bstack_slice_to_bytes(bstack_slice_t s, uint8_t out[16])
{
    int i;
    for (i = 0; i < 8; i++) {
        out[i]     = (uint8_t)(s.offset >> (8 * i));
        out[8 + i] = (uint8_t)(s.len    >> (8 * i));
    }
}

bstack_slice_t bstack_slice_from_bytes(bstack_allocator_t *a,
                                        const uint8_t bytes[16])
{
    bstack_slice_t s;
    int i;
    s.allocator = a;
    s.offset    = 0;
    s.len       = 0;
    for (i = 0; i < 8; i++) {
        s.offset |= (uint64_t)bytes[i]     << (8 * i);
        s.len    |= (uint64_t)bytes[8 + i] << (8 * i);
    }
    return s;
}

/* =========================================================================
 * bstack_slice_t — I/O
 * ====================================================================== */

int bstack_slice_read(bstack_slice_t s, uint8_t *buf)
{
    return bstack_get(slice_stack(s), s.offset, s.offset + s.len, buf);
}

int bstack_slice_read_into(bstack_slice_t s, uint8_t *buf, size_t buf_len)
{
    uint64_t n;
    if (buf_len == 0 || s.len == 0)
        return 0;
    n = ((uint64_t)buf_len < s.len) ? (uint64_t)buf_len : s.len;
    return bstack_get(slice_stack(s), s.offset, s.offset + n, buf);
}

int bstack_slice_read_range_into(bstack_slice_t s, uint64_t start,
                                  uint8_t *buf, size_t buf_len)
{
    if (buf_len == 0)
        return 0;
    /* Check start + buf_len <= s.len without overflow. */
    if (start > s.len || (uint64_t)buf_len > s.len - start) {
        errno = EINVAL;
        return -1;
    }
    return bstack_get(slice_stack(s),
                      s.offset + start,
                      s.offset + start + (uint64_t)buf_len,
                      buf);
}

int bstack_slice_subslice(bstack_slice_t s, uint64_t start, uint64_t end,
                           bstack_slice_t *out)
{
    if (start > end || end > s.len) {
        errno = EINVAL;
        return -1;
    }
    out->allocator = s.allocator;
    out->offset    = s.offset + start;
    out->len       = end - start;
    return 0;
}

/* =========================================================================
 * bstack_slice_t — in-place writes (BSTACK_FEATURE_SET)
 * ====================================================================== */

#ifdef BSTACK_FEATURE_SET

int bstack_slice_write(bstack_slice_t s, const uint8_t *data, size_t data_len)
{
    size_t n;
    if (data_len == 0 || s.len == 0)
        return 0;
    n = (data_len < (size_t)s.len) ? data_len : (size_t)s.len;
    return bstack_set(slice_stack(s), s.offset, data, n);
}

int bstack_slice_write_range(bstack_slice_t s, uint64_t start,
                              const uint8_t *data, size_t data_len)
{
    if (data_len == 0)
        return 0;
    if (start > s.len || (uint64_t)data_len > s.len - start) {
        errno = EINVAL;
        return -1;
    }
    return bstack_set(slice_stack(s), s.offset + start, data, data_len);
}

int bstack_slice_zero(bstack_slice_t s)
{
    if (s.len == 0)
        return 0;
#if UINT64_MAX > SIZE_MAX
    if (s.len > (uint64_t)SIZE_MAX) {
        errno = EINVAL;
        return -1;
    }
#endif
    return bstack_zero(slice_stack(s), s.offset, (size_t)s.len);
}

int bstack_slice_zero_range(bstack_slice_t s, uint64_t start, uint64_t n)
{
    if (n == 0)
        return 0;
    if (start > s.len || n > s.len - start) {
        errno = EINVAL;
        return -1;
    }
#if UINT64_MAX > SIZE_MAX
    if (n > (uint64_t)SIZE_MAX) {
        errno = EINVAL;
        return -1;
    }
#endif
    return bstack_zero(slice_stack(s), s.offset + start, (size_t)n);
}

#endif /* BSTACK_FEATURE_SET */

/* =========================================================================
 * bstack_slice_reader_t
 * ====================================================================== */

bstack_slice_reader_t bstack_slice_reader(bstack_slice_t s)
{
    bstack_slice_reader_t r;
    r.slice  = s;
    r.cursor = 0;
    return r;
}

bstack_slice_reader_t bstack_slice_reader_at(bstack_slice_t s, uint64_t offset)
{
    bstack_slice_reader_t r;
    r.slice  = s;
    r.cursor = offset;
    return r;
}

int bstack_slice_reader_read(bstack_slice_reader_t *r,
                              uint8_t *buf, size_t buf_len, size_t *n_read)
{
    uint64_t available, n;
    if (buf_len == 0 || r->cursor >= r->slice.len) {
        if (n_read) *n_read = 0;
        return 0;
    }
    available = r->slice.len - r->cursor;
    n = ((uint64_t)buf_len <= available) ? (uint64_t)buf_len : available;
    if (bstack_get(slice_stack(r->slice),
                   r->slice.offset + r->cursor,
                   r->slice.offset + r->cursor + n,
                   buf) != 0)
        return -1;
    r->cursor += n;
    if (n_read) *n_read = (size_t)n;
    return 0;
}

int bstack_slice_reader_seek_start(bstack_slice_reader_t *r, uint64_t offset,
                                    uint64_t *out_pos)
{
    r->cursor = offset;
    if (out_pos) *out_pos = r->cursor;
    return 0;
}

int bstack_slice_reader_seek_cur(bstack_slice_reader_t *r, int64_t delta,
                                  uint64_t *out_pos)
{
    if (delta < 0) {
        /* Compute |delta| as uint64_t safely, avoiding signed overflow. */
        uint64_t abs_delta = (uint64_t)(~delta) + 1;
        if (abs_delta > r->cursor) {
            errno = EINVAL;
            return -1;
        }
        r->cursor -= abs_delta;
    } else {
        r->cursor += (uint64_t)delta;
    }
    if (out_pos) *out_pos = r->cursor;
    return 0;
}

int bstack_slice_reader_seek_end(bstack_slice_reader_t *r, int64_t delta,
                                  uint64_t *out_pos)
{
    uint64_t len = r->slice.len;
    if (delta < 0) {
        uint64_t abs_delta = (uint64_t)(~delta) + 1;
        if (abs_delta > len) {
            errno = EINVAL;
            return -1;
        }
        r->cursor = len - abs_delta;
    } else {
        r->cursor = len + (uint64_t)delta;
    }
    if (out_pos) *out_pos = r->cursor;
    return 0;
}

/* =========================================================================
 * linear_bstack_allocator_t — vtable implementations
 * ====================================================================== */

static bstack_t *linear_vt_stack(bstack_allocator_t *self)
{
    return ((linear_bstack_allocator_t *)self)->bs;
}

static int linear_vt_alloc(bstack_allocator_t *self, uint64_t len,
                            bstack_slice_t *out)
{
    linear_bstack_allocator_t *a = (linear_bstack_allocator_t *)self;
    uint64_t offset;
#if UINT64_MAX > SIZE_MAX
    if (len > (uint64_t)SIZE_MAX) {
        errno = EINVAL;
        return -1;
    }
#endif
    if (bstack_extend(a->bs, (size_t)len, &offset) != 0)
        return -1;
    out->allocator = self;
    out->offset    = offset;
    out->len       = len;
    return 0;
}

static int linear_vt_realloc(bstack_allocator_t *self, bstack_slice_t slice,
                               uint64_t new_len, bstack_slice_t *out)
{
    linear_bstack_allocator_t *a = (linear_bstack_allocator_t *)self;
    uint64_t cur_tail, extra, shrink, dummy;
    if (bstack_len(a->bs, &cur_tail) != 0)
        return -1;
    if (slice.offset + slice.len != cur_tail) {
        errno = ENOTSUP;
        return -1;
    }
    if (new_len == slice.len) {
        *out = slice;
        return 0;
    }
    if (new_len > slice.len) {
        extra = new_len - slice.len;
#if UINT64_MAX > SIZE_MAX
        if (extra > (uint64_t)SIZE_MAX) {
            errno = EINVAL;
            return -1;
        }
#endif
        if (bstack_extend(a->bs, (size_t)extra, &dummy) != 0)
            return -1;
    } else {
        shrink = slice.len - new_len;
#if UINT64_MAX > SIZE_MAX
        if (shrink > (uint64_t)SIZE_MAX) {
            errno = EINVAL;
            return -1;
        }
#endif
        if (bstack_discard(a->bs, (size_t)shrink) != 0)
            return -1;
    }
    out->allocator = self;
    out->offset    = slice.offset;
    out->len       = new_len;
    return 0;
}

static int linear_vt_dealloc(bstack_allocator_t *self, bstack_slice_t slice)
{
    linear_bstack_allocator_t *a = (linear_bstack_allocator_t *)self;
    uint64_t cur_tail;
    if (bstack_len(a->bs, &cur_tail) != 0)
        return -1;
    if (slice.offset + slice.len == cur_tail) {
#if UINT64_MAX > SIZE_MAX
        if (slice.len > (uint64_t)SIZE_MAX) {
            errno = EINVAL;
            return -1;
        }
#endif
        return bstack_discard(a->bs, (size_t)slice.len);
    }
    return 0; /* non-tail slice: no-op */
}

static const bstack_allocator_vtbl_t linear_vtbl = {
    linear_vt_stack,
    linear_vt_alloc,
    linear_vt_realloc,
    linear_vt_dealloc
};

/* =========================================================================
 * linear_bstack_allocator_t — public API
 * ====================================================================== */

linear_bstack_allocator_t *linear_bstack_allocator_new(bstack_t *bs)
{
    linear_bstack_allocator_t *a = malloc(sizeof *a);
    if (!a) {
        errno = ENOMEM;
        return NULL;
    }
    a->base.vtbl = &linear_vtbl;
    a->bs        = bs;
    return a;
}

void linear_bstack_allocator_free(linear_bstack_allocator_t *alloc)
{
    free(alloc);
}

bstack_t *linear_bstack_allocator_into_stack(linear_bstack_allocator_t *alloc)
{
    bstack_t *bs = alloc->bs;
    free(alloc);
    return bs;
}

/* =========================================================================
 * first_fit_bstack_allocator_t — first-fit free-list allocator
 * Requires -DBSTACK_FEATURE_SET (depends on bstack_set and bstack_zero).
 * ====================================================================== */

#ifdef BSTACK_FEATURE_SET

/* ---- constants --------------------------------------------------------- */

#define ALFF_OFFSET_SIZE      UINT64_C(16)
#define ALFF_HEADER_SIZE      UINT64_C(32)
#define ALFF_BLOCK_HDR_SIZE   UINT64_C(16)
#define ALFF_BLOCK_FTR_SIZE   UINT64_C(8)
#define ALFF_BLOCK_OVERHEAD   UINT64_C(24)  /* HDR + FTR */
#define ALFF_MIN_PAYLOAD      UINT64_C(16)
/* OFFSET_SIZE(16) + magic(8) + flags(4) + reserved(4) = 32 */
#define ALFF_FREE_HEAD_OFFSET UINT64_C(32)
/* Byte offset of the flags field: OFFSET_SIZE + magic = 16+8 = 24 */
#define ALFF_FLAGS_OFFSET     UINT64_C(24)
/* First valid payload start: OFFSET_SIZE + HEADER_SIZE + BLOCK_HDR_SIZE */
#define ALFF_ARENA_START      UINT64_C(48)
#define ALFF_MIN_BLOCK_START  UINT64_C(64)  /* ARENA_START + BLOCK_HDR_SIZE */
#define ALFF_MIN_BLOCK_END    UINT64_C(80)  /* ARENA_START + BLOCK_HDR_SIZE + MIN_PAYLOAD */

static const uint8_t alff_magic[8]        = {'A','L','F','F',0,1,0,0};
static const uint8_t alff_magic_prefix[6] = {'A','L','F','F',0,1};

/* ---- LE codec helpers -------------------------------------------------- */

static inline uint64_t read_le64(const uint8_t *p)
{
    return (uint64_t)p[0]        | ((uint64_t)p[1] << 8)
         | ((uint64_t)p[2] << 16) | ((uint64_t)p[3] << 24)
         | ((uint64_t)p[4] << 32) | ((uint64_t)p[5] << 40)
         | ((uint64_t)p[6] << 48) | ((uint64_t)p[7] << 56);
}

static inline void write_le64(uint8_t *p, uint64_t v)
{
    p[0] = (uint8_t)(v);        p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16); p[3] = (uint8_t)(v >> 24);
    p[4] = (uint8_t)(v >> 32); p[5] = (uint8_t)(v >> 40);
    p[6] = (uint8_t)(v >> 48); p[7] = (uint8_t)(v >> 56);
}

static inline void write_le32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v);        p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16); p[3] = (uint8_t)(v >> 24);
}

/* ---- alignment --------------------------------------------------------- */

static inline uint64_t alff_align_len(uint64_t len)
{
    uint64_t m = (len > ALFF_MIN_PAYLOAD) ? len : ALFF_MIN_PAYLOAD;
    return (m + UINT64_C(7)) & ~UINT64_C(7);
}

/* ---- validation predicates -------------------------------------------- */

static inline int alff_is_impossible_block_size(uint64_t stack_len, uint64_t size)
{
    return size < ALFF_MIN_PAYLOAD || size > stack_len;
}

static inline int alff_is_impossible_block_start(uint64_t stack_len, uint64_t start)
{
    return (start % 8 != 0) || start < ALFF_MIN_BLOCK_START || start >= stack_len;
}

static inline int alff_is_impossible_block_end(uint64_t stack_len, uint64_t end)
{
    if (end < ALFF_MIN_BLOCK_END) return 1;
    if (stack_len < ALFF_BLOCK_FTR_SIZE) return 1;
    return end > stack_len - ALFF_BLOCK_FTR_SIZE;
}

/* ---- recovery flag management ----------------------------------------- */

static int alff_set_recovery_needed(bstack_t *bs)
{
    uint8_t flag[4];
    write_le32(flag, 1);
    return bstack_set(bs, ALFF_FLAGS_OFFSET, flag, 4);
}

static int alff_clear_recovery_needed(bstack_t *bs)
{
    uint8_t flag[4] = {0, 0, 0, 0};
    return bstack_set(bs, ALFF_FLAGS_OFFSET, flag, 4);
}

/* ---- free-list helpers ------------------------------------------------- */

/*
 * Remove the free block whose payload starts at payload_start from the
 * doubly-linked free list by stitching its neighbours together.
 * Does not touch the block's header or clear its is_free flag.
 */
static int alff_unlink_from_free_list(bstack_t *bs, uint64_t payload_start)
{
    uint8_t ptrs[16];
    uint64_t next, prev;
    uint8_t ptr_le[8];

    if (bstack_get(bs, payload_start, payload_start + 16, ptrs) != 0)
        return -1;
    next = read_le64(ptrs);
    prev = read_le64(ptrs + 8);

    write_le64(ptr_le, next);
    if (prev != 0) {
        if (bstack_set(bs, prev, ptr_le, 8) != 0) return -1;
    } else {
        if (bstack_set(bs, ALFF_FREE_HEAD_OFFSET, ptr_le, 8) != 0) return -1;
    }

    if (next != 0) {
        write_le64(ptr_le, prev);
        if (bstack_set(bs, next + 8, ptr_le, 8) != 0) return -1;
    }
    return 0;
}

/*
 * Add the block whose payload starts at block_start to the free list.
 * Immediately coalesces with free right and left neighbours, then prepends
 * the merged block to the free list head.  If the merged block reaches the
 * stack tail it is discarded entirely (tail reclamation).
 *
 * Caller must set recovery_needed before calling and clear it after.
 */
static int alff_add_to_free_list(bstack_t *bs, uint64_t block_start)
{
    uint64_t stack_len, block_header_start;
    uint8_t size_buf[8];
    uint64_t size;
    uint64_t result_header_start, result_start;
    uint8_t free_flag[4];

    if (bstack_len(bs, &stack_len) != 0) return -1;

    block_header_start = block_start - ALFF_BLOCK_HDR_SIZE;

    if (bstack_get(bs, block_header_start, block_header_start + 8, size_buf) != 0)
        return -1;
    size = read_le64(size_buf);
    result_header_start = block_header_start;

    /* Mark block as free early so recovery can find it on crash */
    write_le32(free_flag, 1);
    if (bstack_set(bs, block_header_start + 8, free_flag, 4) != 0) return -1;

    /* Right coalesce: absorb the immediately following block if it is free */
    {
        uint64_t next_header = block_header_start + ALFF_BLOCK_OVERHEAD + size;
        if (next_header + ALFF_BLOCK_HDR_SIZE <= stack_len) {
            uint8_t next_hdr[16];
            uint64_t next_size;
            if (bstack_get(bs, next_header, next_header + 16, next_hdr) != 0) return -1;
            next_size = read_le64(next_hdr);
            if ((next_hdr[8] & 1) != 0
                && next_size >= ALFF_MIN_PAYLOAD
                && next_size % 8 == 0
                && next_header + ALFF_BLOCK_OVERHEAD + next_size <= stack_len) {
                if (alff_unlink_from_free_list(bs,
                        next_header + ALFF_BLOCK_HDR_SIZE) != 0) return -1;
                size += next_size + ALFF_BLOCK_OVERHEAD;
            }
        }
    }

    /* Left coalesce: merge into the preceding block if it is free.
     * Read predecessor's footer to locate its header, then cross-check. */
    if (block_header_start > ALFF_ARENA_START) {
        uint8_t prev_footer_buf[8];
        uint64_t prev_size;
        if (bstack_get(bs,
                block_header_start - ALFF_BLOCK_FTR_SIZE,
                block_header_start, prev_footer_buf) != 0) return -1;
        prev_size = read_le64(prev_footer_buf);
        if (prev_size >= ALFF_MIN_PAYLOAD && prev_size % 8 == 0
            && prev_size + ALFF_BLOCK_OVERHEAD
               <= block_header_start - ALFF_ARENA_START) {
            uint64_t prev_header = block_header_start
                                   - prev_size - ALFF_BLOCK_OVERHEAD;
            if (prev_header >= ALFF_ARENA_START) {
                uint8_t prev_hdr[16];
                uint64_t prev_hdr_size;
                if (bstack_get(bs, prev_header, prev_header + 16,
                               prev_hdr) != 0) return -1;
                prev_hdr_size = read_le64(prev_hdr);
                if ((prev_hdr[8] & 1) != 0 && prev_hdr_size == prev_size) {
                    if (alff_unlink_from_free_list(bs,
                            prev_header + ALFF_BLOCK_HDR_SIZE) != 0) return -1;
                    size += prev_size + ALFF_BLOCK_OVERHEAD;
                    result_header_start = prev_header;
                }
            }
        }
    }

    result_start = result_header_start + ALFF_BLOCK_HDR_SIZE;

    /* Write merged size into header and footer */
    {
        uint8_t size_le[8];
        write_le64(size_le, size);
        if (bstack_set(bs, result_header_start, size_le, 8) != 0) return -1;
        if (bstack_set(bs, result_start + size, size_le, 8) != 0) return -1;
    }

    /* Prepend merged block to free list:
     * Write flags=1 + reserved=0 + next_free=old_head + prev_free=0 in one call
     * starting at the flags field of the block header. */
    {
        uint8_t head_buf[8];
        uint64_t next_block;
        uint8_t update_buf[24];
        uint8_t result_le[8];

        if (bstack_get(bs, ALFF_FREE_HEAD_OFFSET,
                       ALFF_FREE_HEAD_OFFSET + 8, head_buf) != 0) return -1;
        next_block = read_le64(head_buf);

        memset(update_buf, 0, 24);
        write_le32(update_buf, 1);              /* flags: is_free = 1 */
        write_le64(update_buf + 8, next_block); /* next_free = old head */
        /* prev_free = 0 (zeroed) */
        if (bstack_set(bs, result_start - ALFF_BLOCK_HDR_SIZE + 8,
                       update_buf, 24) != 0) return -1;

        write_le64(result_le, result_start);
        if (bstack_set(bs, ALFF_FREE_HEAD_OFFSET, result_le, 8) != 0) return -1;

        if (next_block != 0) {
            if (bstack_set(bs, next_block + 8, result_le, 8) != 0) return -1;
        }
    }
    return 0;
}

/*
 * Walk the free list for the first block whose payload size >= size.
 * Returns 0 on success; *out_start and *out_size are set to the payload
 * offset and size of the found block, or both 0 if no suitable block exists.
 */
static int alff_find_large_enough_block(bstack_t *bs, uint64_t size,
                                         uint64_t *out_start, uint64_t *out_size)
{
    uint64_t stack_len, head;
    uint8_t head_buf[8];

    *out_start = 0;
    *out_size  = 0;

    if (bstack_len(bs, &stack_len) != 0) return -1;
    if (bstack_get(bs, ALFF_FREE_HEAD_OFFSET,
                   ALFF_FREE_HEAD_OFFSET + 8, head_buf) != 0) return -1;
    head = read_le64(head_buf);

    while (head != 0) {
        /* Read block header (16 B) + first 8 B of payload (next_free) */
        uint8_t buf[24];
        uint64_t block_size, next;

        if (bstack_get(bs, head - ALFF_BLOCK_HDR_SIZE,
                       head - ALFF_BLOCK_HDR_SIZE + 24, buf) != 0) return -1;

        block_size = read_le64(buf);

        if ((buf[8] & 1) == 0) { /* is_free must be set */
            errno = EINVAL;
            return -1;
        }
        if (alff_is_impossible_block_size(stack_len, block_size)
            || block_size % 8 != 0) {
            errno = EINVAL;
            return -1;
        }

        if (block_size >= size) {
            *out_start = head;
            *out_size  = block_size;
            return 0;
        }

        next = read_le64(buf + 16); /* next_free from payload[0..8] */
        if (next != 0 && alff_is_impossible_block_start(stack_len, next)) {
            errno = EINVAL;
            return -1;
        }
        head = next;
    }
    return 0;
}

/*
 * Remove a free block from the free list and prepare it for allocation.
 *
 * content_buf has size (ALFF_BLOCK_OVERHEAD + requested_size).  Layout:
 *   [0..16)                    block header area (only [8..16) used)
 *   [16..16+requested_size)    payload (zeros for alloc, user data for realloc)
 *   [16+requested_size..24+..) footer slot
 *
 * Split path (block large enough to split):
 *   Splits the found block into a smaller free block (front) and the new
 *   allocated block (back).  Writes header/footer updates directly to disk.
 *
 * No-split path:
 *   Unlinks the block entirely, zeroes its is_free flag, and writes the
 *   combined flags+data region in one disk write.
 */
static int alff_unlink_block(bstack_t *bs,
                              uint64_t found_start, uint64_t found_size,
                              uint64_t requested_size, uint8_t *content_buf)
{
    if (found_size >= requested_size + ALFF_BLOCK_OVERHEAD + ALFF_MIN_PAYLOAD) {
        /* SPLIT: allocated block at the back, free block remains at the front */
        uint64_t remaining_size = found_size - requested_size - ALFF_BLOCK_OVERHEAD;
        uint8_t update_buf[24]; /* free_footer(8) | alloc_hdr_size(8) | alloc_flags+reserved(8) */
        uint8_t rem_le[8];
        size_t  tail_len;

        /* Fill footer in content_buf (allocated block's footer) */
        write_le64(content_buf + ALFF_BLOCK_HDR_SIZE + requested_size, requested_size);

        /* Update 1: free block footer + allocated block header (size | 0-flags) */
        memset(update_buf, 0, 24);
        write_le64(update_buf,     remaining_size);
        write_le64(update_buf + 8, requested_size);
        if (bstack_set(bs, found_start + remaining_size, update_buf, 24) != 0) return -1;

        /* Update 2: allocated block payload + footer */
        tail_len = (size_t)(requested_size + ALFF_BLOCK_FTR_SIZE);
        if (bstack_set(bs,
                found_start + remaining_size + ALFF_BLOCK_OVERHEAD,
                content_buf + ALFF_BLOCK_HDR_SIZE, tail_len) != 0) return -1;

        /* Update free block header size */
        write_le64(rem_le, remaining_size);
        if (bstack_set(bs, found_start - ALFF_BLOCK_HDR_SIZE, rem_le, 8) != 0) return -1;

        return 0;
    } else {
        /* NO-SPLIT: remove block entirely from free list */
        uint8_t pointers_buf[16];
        uint64_t next, prev;
        uint8_t ptr_le[8];

        if (bstack_get(bs, found_start, found_start + 16, pointers_buf) != 0) return -1;
        next = read_le64(pointers_buf);
        prev = read_le64(pointers_buf + 8);

        /* Commit backward pointer first */
        write_le64(ptr_le, next);
        if (prev != 0) {
            if (bstack_set(bs, prev, ptr_le, 8) != 0) return -1;
        } else {
            if (bstack_set(bs, ALFF_FREE_HEAD_OFFSET, ptr_le, 8) != 0) return -1;
        }

        /* Then commit forward pointer */
        if (next != 0) {
            write_le64(ptr_le, prev);
            if (bstack_set(bs, next + 8, ptr_le, 8) != 0) return -1;
        }

        /* Clear is_free flag and write user data in one call */
        memset(content_buf + 8, 0, 8); /* zero flags + reserved */
        {
            size_t write_len = (size_t)(8 + requested_size);
            if (bstack_set(bs, found_start - ALFF_BLOCK_HDR_SIZE + 8,
                           content_buf + 8, write_len) != 0) return -1;
        }
        return 0;
    }
}

/*
 * After discarding the tail block, cascade-discard any free blocks that are
 * now the new tail.  Maintains the invariant: the stack tail is always an
 * allocated block (or the arena is empty).
 */
static int alff_cascade_discard_free_tail(bstack_t *bs)
{
    int needs_clear = 0;

    for (;;) {
        uint64_t tail, sz, hdr;
        uint8_t footer_buf[8], hdr_buf[16];
        uint64_t hdr_size;
        size_t discard_n;

        if (bstack_len(bs, &tail) != 0) return -1;
        if (tail <= ALFF_ARENA_START) break;

        if (bstack_get(bs, tail - ALFF_BLOCK_FTR_SIZE, tail, footer_buf) != 0) return -1;
        sz = read_le64(footer_buf);

        if (sz < ALFF_MIN_PAYLOAD || sz % 8 != 0) break;
        if (sz + ALFF_BLOCK_OVERHEAD > tail - ALFF_ARENA_START) break;
        hdr = tail - sz - ALFF_BLOCK_OVERHEAD;
        if (hdr < ALFF_ARENA_START) break;

        if (bstack_get(bs, hdr, hdr + 16, hdr_buf) != 0) return -1;
        hdr_size = read_le64(hdr_buf);
        if ((hdr_buf[8] & 1) == 0 || hdr_size != sz) break;

        if (!needs_clear) {
            if (alff_set_recovery_needed(bs) != 0) return -1;
            needs_clear = 1;
        }
        if (alff_unlink_from_free_list(bs, hdr + ALFF_BLOCK_HDR_SIZE) != 0) return -1;
        discard_n = (size_t)(sz + ALFF_BLOCK_OVERHEAD);
        if (bstack_discard(bs, discard_n) != 0) return -1;
    }

    if (needs_clear)
        return alff_clear_recovery_needed(bs);
    return 0;
}

/*
 * Linear arena scan: rebuild the free list from is_free flags in block headers.
 * Truncates any partial tail block.  Ignores all stored pointer values.
 */
static int alff_recovery(bstack_t *bs)
{
    uint64_t stack_len, pos;
    uint64_t *free_blks = NULL;
    size_t    free_cnt  = 0, free_cap = 0;
    size_t    i;
    int       ret = 0;

    if (bstack_len(bs, &stack_len) != 0) return -1;
    pos = ALFF_ARENA_START;

    while (pos < stack_len) {
        uint64_t remaining = stack_len - pos;
        uint8_t  hdr_buf[16];
        uint64_t size, block_total;
        uint8_t  is_free;

        if (remaining < ALFF_BLOCK_OVERHEAD) {
#if UINT64_MAX > SIZE_MAX
            if (remaining > (uint64_t)SIZE_MAX) { ret = -1; goto done; }
#endif
            if (bstack_discard(bs, (size_t)remaining) != 0) { ret = -1; goto done; }
            break;
        }

        if (bstack_get(bs, pos, pos + 16, hdr_buf) != 0) { ret = -1; goto done; }
        size    = read_le64(hdr_buf);
        is_free = hdr_buf[8] & 1;

        if (size < ALFF_MIN_PAYLOAD || size % 8 != 0
            || size + ALFF_BLOCK_OVERHEAD > stack_len - pos) {
            uint64_t dn = stack_len - pos;
#if UINT64_MAX > SIZE_MAX
            if (dn > (uint64_t)SIZE_MAX) { ret = -1; goto done; }
#endif
            if (bstack_discard(bs, (size_t)dn) != 0) { ret = -1; goto done; }
            break;
        }
        block_total = size + ALFF_BLOCK_OVERHEAD;

        /* Detect partially-completed split:
         * outer footer (at pos+HDR+size) may say F != size.  If F fits the
         * three-point pattern, the header was never shrunk — fix it to R. */
        {
            uint8_t  outer_ftr[8];
            uint64_t f;
            if (bstack_get(bs, pos + ALFF_BLOCK_HDR_SIZE + size,
                           pos + ALFF_BLOCK_HDR_SIZE + size + 8,
                           outer_ftr) != 0) { ret = -1; goto done; }
            f = read_le64(outer_ftr);
            if (f != size && f >= ALFF_MIN_PAYLOAD && f % 8 == 0
                && f + ALFF_BLOCK_OVERHEAD <= size) {
                uint64_t r = size - f - ALFF_BLOCK_OVERHEAD;
                if (r >= ALFF_MIN_PAYLOAD && r % 8 == 0) {
                    uint64_t inner_ftr_pos  = pos + ALFF_BLOCK_HDR_SIZE + r;
                    uint64_t second_hdr_pos = inner_ftr_pos + ALFF_BLOCK_FTR_SIZE;
                    if (second_hdr_pos + ALFF_BLOCK_HDR_SIZE <= stack_len) {
                        uint8_t inner_ftr[8], second_size[8];
                        if (bstack_get(bs, inner_ftr_pos, inner_ftr_pos + 8,
                                       inner_ftr) != 0) { ret = -1; goto done; }
                        if (bstack_get(bs, second_hdr_pos, second_hdr_pos + 8,
                                       second_size) != 0) { ret = -1; goto done; }
                        if (read_le64(inner_ftr) == r && read_le64(second_size) == f) {
                            uint8_t r_le[8];
                            write_le64(r_le, r);
                            if (bstack_set(bs, pos, r_le, 8) != 0) {
                                ret = -1; goto done;
                            }
                            size        = r;
                            block_total = r + ALFF_BLOCK_OVERHEAD;
                        }
                    }
                }
            }
        }

        if (is_free) {
            if (free_cnt == free_cap) {
                size_t    nc  = free_cap ? free_cap * 2 : 16;
                uint64_t *tmp = realloc(free_blks, nc * sizeof *tmp);
                if (!tmp) { ret = -1; goto done; }
                free_blks = tmp;
                free_cap  = nc;
            }
            free_blks[free_cnt++] = pos + ALFF_BLOCK_HDR_SIZE;
        }
        pos += block_total;
    }

    /* Rebuild free list: rewrite next_free/prev_free in encounter order */
    for (i = 0; i < free_cnt; i++) {
        uint64_t curr = free_blks[i];
        uint64_t next = (i + 1 < free_cnt) ? free_blks[i + 1] : 0;
        uint64_t prev = (i > 0)             ? free_blks[i - 1] : 0;
        uint8_t  ptr_buf[16];
        write_le64(ptr_buf,     next);
        write_le64(ptr_buf + 8, prev);
        if (bstack_set(bs, curr, ptr_buf, 16) != 0) { ret = -1; goto done; }
    }

    {
        uint64_t new_head = free_cnt > 0 ? free_blks[0] : 0;
        uint8_t  head_le[8];
        write_le64(head_le, new_head);
        if (bstack_set(bs, ALFF_FREE_HEAD_OFFSET, head_le, 8) != 0) {
            ret = -1; goto done;
        }
    }

    ret = alff_clear_recovery_needed(bs);

done:
    free(free_blks);
    return ret;
}

/* =========================================================================
 * first_fit_bstack_allocator_t — vtable implementations
 * ====================================================================== */

static bstack_t *ff_vt_stack(bstack_allocator_t *self)
{
    return ((first_fit_bstack_allocator_t *)self)->bs;
}

static int ff_vt_alloc(bstack_allocator_t *self, uint64_t len, bstack_slice_t *out)
{
    first_fit_bstack_allocator_t *a = (first_fit_bstack_allocator_t *)self;
    uint64_t aligned_len = alff_align_len(len);
    uint64_t found_start = 0, found_size = 0;
    uint64_t payload;

    if (alff_find_large_enough_block(a->bs, aligned_len,
                                      &found_start, &found_size) != 0)
        return -1;

    if (found_start != 0) {
        /* Reuse a free block (split if large enough, otherwise take whole) */
        size_t   buf_sz;
        uint8_t *content_buf;

#if UINT64_MAX > SIZE_MAX
        if (ALFF_BLOCK_OVERHEAD + aligned_len > (uint64_t)SIZE_MAX) {
            errno = EINVAL;
            return -1;
        }
#endif
        buf_sz = (size_t)(ALFF_BLOCK_OVERHEAD + aligned_len);
        content_buf = calloc(1, buf_sz);
        if (!content_buf) return -1;

        if (alff_set_recovery_needed(a->bs) != 0
            || alff_unlink_block(a->bs, found_start, found_size,
                                  aligned_len, content_buf) != 0
            || alff_clear_recovery_needed(a->bs) != 0) {
            free(content_buf);
            return -1;
        }
        free(content_buf);

        /* Split puts allocated block at the back; no-split uses front */
        payload = (found_size >= aligned_len + ALFF_BLOCK_OVERHEAD + ALFF_MIN_PAYLOAD)
                  ? found_start + found_size - aligned_len
                  : found_start;
    } else {
        /* No free block fits: push a new block onto the tail */
        size_t   block_sz;
        uint8_t *block_buf;
        uint64_t push_offset;
        uint8_t  size_le[8];

#if UINT64_MAX > SIZE_MAX
        if (aligned_len + ALFF_BLOCK_OVERHEAD > (uint64_t)SIZE_MAX) {
            errno = EINVAL;
            return -1;
        }
#endif
        block_sz  = (size_t)(aligned_len + ALFF_BLOCK_OVERHEAD);
        block_buf = calloc(1, block_sz);
        if (!block_buf) return -1;

        write_le64(size_le, aligned_len);
        memcpy(block_buf, size_le, 8);
        memcpy(block_buf + ALFF_BLOCK_HDR_SIZE + aligned_len, size_le, 8);

        if (bstack_push(a->bs, block_buf, block_sz, &push_offset) != 0) {
            free(block_buf);
            return -1;
        }
        free(block_buf);
        payload = push_offset + ALFF_BLOCK_HDR_SIZE;
    }

    out->allocator = self;
    out->offset    = payload;
    out->len       = len;
    return 0;
}

static int ff_vt_dealloc(bstack_allocator_t *self, bstack_slice_t slice)
{
    first_fit_bstack_allocator_t *a = (first_fit_bstack_allocator_t *)self;
    uint64_t aligned_len = alff_align_len(slice.len);
    uint64_t stack_len;

    if (bstack_len(a->bs, &stack_len) != 0) return -1;

    if (alff_is_impossible_block_start(stack_len, slice.offset)
        || alff_is_impossible_block_end(stack_len, slice.offset + aligned_len)
        || alff_is_impossible_block_size(stack_len, aligned_len)) {
        errno = EINVAL;
        return -1;
    }

    /* Tail block fast path: just discard the block bytes */
    if (slice.offset + aligned_len == stack_len - ALFF_BLOCK_FTR_SIZE) {
        size_t discard_n;
#if UINT64_MAX > SIZE_MAX
        if (aligned_len + ALFF_BLOCK_OVERHEAD > (uint64_t)SIZE_MAX) {
            errno = EINVAL;
            return -1;
        }
#endif
        discard_n = (size_t)(aligned_len + ALFF_BLOCK_OVERHEAD);
        if (bstack_discard(a->bs, discard_n) != 0) return -1;
        return alff_cascade_discard_free_tail(a->bs);
    }

    if (alff_set_recovery_needed(a->bs) != 0) return -1;
    if (alff_add_to_free_list(a->bs, slice.offset) != 0) return -1;
    return alff_clear_recovery_needed(a->bs);
}

static int ff_vt_realloc(bstack_allocator_t *self, bstack_slice_t slice,
                          uint64_t new_len, bstack_slice_t *out)
{
    first_fit_bstack_allocator_t *a = (first_fit_bstack_allocator_t *)self;
    uint64_t aligned_current_len = alff_align_len(slice.len);
    uint64_t aligned_new_len;
    uint64_t stack_len;

    if (bstack_len(a->bs, &stack_len) != 0) return -1;

    if (alff_is_impossible_block_start(stack_len, slice.offset)
        || alff_is_impossible_block_end(stack_len,
               slice.offset + aligned_current_len)
        || alff_is_impossible_block_size(stack_len, aligned_current_len)) {
        errno = EINVAL;
        return -1;
    }

    aligned_new_len = alff_align_len(new_len);

    /* Case 1: same aligned bucket — no block resize needed */
    if (aligned_new_len == aligned_current_len) {
        if (new_len > slice.len) {
            size_t zero_n = (size_t)(new_len - slice.len);
            if (bstack_zero(a->bs, slice.offset + slice.len, zero_n) != 0) return -1;
        }
        out->allocator = self;
        out->offset    = slice.offset;
        out->len       = new_len;
        return 0;
    }

    /* Refresh stack_len for tail check */
    if (bstack_len(a->bs, &stack_len) != 0) return -1;

    /* Case 2: tail block — extend or shrink in place */
    if (slice.offset + aligned_current_len == stack_len - ALFF_BLOCK_FTR_SIZE) {
        uint8_t size_le[8];
        if (aligned_new_len > aligned_current_len) {
            uint64_t delta  = aligned_new_len - aligned_current_len;
            uint64_t zero_n = aligned_current_len + ALFF_BLOCK_FTR_SIZE - slice.len;
#if UINT64_MAX > SIZE_MAX
            if (delta > (uint64_t)SIZE_MAX || zero_n > (uint64_t)SIZE_MAX) {
                errno = EINVAL;
                return -1;
            }
#endif
            if (bstack_extend(a->bs, (size_t)delta, NULL) != 0) return -1;
            if (bstack_zero(a->bs, slice.offset + slice.len, (size_t)zero_n) != 0) return -1;
            write_le64(size_le, aligned_new_len);
            if (bstack_set(a->bs, slice.offset - ALFF_BLOCK_HDR_SIZE,
                           size_le, 8) != 0) return -1;
            if (bstack_set(a->bs, slice.offset + aligned_new_len,
                           size_le, 8) != 0) return -1;
        } else {
            uint64_t delta = aligned_current_len - aligned_new_len;
#if UINT64_MAX > SIZE_MAX
            if (delta > (uint64_t)SIZE_MAX) { errno = EINVAL; return -1; }
#endif
            write_le64(size_le, aligned_new_len);
            if (bstack_set(a->bs, slice.offset + aligned_new_len,
                           size_le, 8) != 0) return -1;
            if (bstack_set(a->bs, slice.offset - ALFF_BLOCK_HDR_SIZE,
                           size_le, 8) != 0) return -1;
            if (bstack_discard(a->bs, (size_t)delta) != 0) return -1;
        }
        out->allocator = self;
        out->offset    = slice.offset;
        out->len       = new_len;
        return 0;
    }

    /* Read actual block size from header */
    {
        uint8_t block_size_buf[8];
        uint64_t block_size;
        if (bstack_get(a->bs, slice.offset - ALFF_BLOCK_HDR_SIZE,
                       slice.offset - ALFF_BLOCK_HDR_SIZE + 8,
                       block_size_buf) != 0) return -1;
        block_size = read_le64(block_size_buf);

        /* Case 3: block already large enough for the new size */
        if (block_size >= aligned_new_len) {
            if (new_len > slice.len) {
                size_t zero_n = (size_t)(new_len - slice.len);
                if (bstack_zero(a->bs, slice.offset + slice.len, zero_n) != 0) return -1;
            }
            out->allocator = self;
            out->offset    = slice.offset;
            out->len       = new_len;
            return 0;
        }

        /* Case 4: try to merge with the free right neighbour in place */
        {
            uint64_t next_block = slice.offset + block_size + ALFF_BLOCK_OVERHEAD;
            if (bstack_len(a->bs, &stack_len) != 0) return -1;
            if (next_block <= stack_len - ALFF_BLOCK_FTR_SIZE - ALFF_MIN_PAYLOAD) {
                uint8_t next_hdr[16];
                uint64_t next_size;
                if (bstack_get(a->bs, next_block - ALFF_BLOCK_HDR_SIZE,
                               next_block - ALFF_BLOCK_HDR_SIZE + 16,
                               next_hdr) != 0) return -1;
                next_size = read_le64(next_hdr);

                if ((next_hdr[8] & 1) != 0
                    && next_size >= ALFF_MIN_PAYLOAD
                    && next_size % 8 == 0
                    && block_size + ALFF_BLOCK_OVERHEAD + next_size >= aligned_new_len) {

                    /* Pre-zero stale bytes beyond user-visible slice */
                    if (slice.len < block_size) {
                        size_t zero_n = (size_t)(block_size - slice.len);
                        if (bstack_zero(a->bs, slice.offset + slice.len,
                                        zero_n) != 0) return -1;
                    }

                    if (alff_set_recovery_needed(a->bs) != 0) return -1;
                    if (alff_unlink_from_free_list(a->bs, next_block) != 0) return -1;

                    {
                        uint64_t merged_size = block_size + ALFF_BLOCK_OVERHEAD + next_size;
                        size_t   zero_buf_sz =
                            (size_t)(next_size + ALFF_BLOCK_OVERHEAD + ALFF_BLOCK_FTR_SIZE);
                        uint8_t *zero_buff = calloc(1, zero_buf_sz);
                        if (!zero_buff) return -1;

                        if (merged_size >=
                            aligned_new_len + ALFF_BLOCK_OVERHEAD + ALFF_MIN_PAYLOAD) {
                            /* Sub-case 4a: split the merged block */
                            uint64_t remainder_size =
                                merged_size - aligned_new_len - ALFF_BLOCK_OVERHEAD;
                            uint64_t new_free_start =
                                slice.offset + aligned_new_len + ALFF_BLOCK_OVERHEAD;
                            uint8_t  head_buf[8];
                            uint64_t old_head;
                            size_t alloc_ftr_off =
                                (size_t)(aligned_new_len - block_size);
                            size_t free_hdr_off  =
                                alloc_ftr_off + (size_t)ALFF_BLOCK_FTR_SIZE;
                            size_t free_pay_off  =
                                alloc_ftr_off + (size_t)ALFF_BLOCK_OVERHEAD;
                            size_t free_ftr_off  =
                                (size_t)(next_size + ALFF_BLOCK_OVERHEAD);
                            uint8_t size_le[8];

                            if (bstack_get(a->bs, ALFF_FREE_HEAD_OFFSET,
                                           ALFF_FREE_HEAD_OFFSET + 8, head_buf) != 0) {
                                free(zero_buff); return -1;
                            }
                            old_head = read_le64(head_buf);

                            write_le64(zero_buff + alloc_ftr_off, aligned_new_len);
                            write_le64(zero_buff + free_hdr_off,  remainder_size);
                            write_le32(zero_buff + free_hdr_off + 8, 1); /* is_free */
                            write_le64(zero_buff + free_pay_off,  old_head);
                            write_le64(zero_buff + free_ftr_off,  remainder_size);

                            /* Set header to merged_size first (crash-detection sentinel) */
                            write_le64(size_le, merged_size);
                            if (bstack_set(a->bs, slice.offset - ALFF_BLOCK_HDR_SIZE,
                                           size_le, 8) != 0) {
                                free(zero_buff); return -1;
                            }
                            if (bstack_set(a->bs, slice.offset + block_size,
                                           zero_buff, zero_buf_sz) != 0) {
                                free(zero_buff); return -1;
                            }
                            free(zero_buff);

                            /* Shrink allocated block header */
                            write_le64(size_le, aligned_new_len);
                            if (bstack_set(a->bs, slice.offset - ALFF_BLOCK_HDR_SIZE,
                                           size_le, 8) != 0) return -1;

                            /* Forward link: free_head → new free block */
                            {
                                uint8_t nfs_le[8];
                                write_le64(nfs_le, new_free_start);
                                if (bstack_set(a->bs, ALFF_FREE_HEAD_OFFSET,
                                               nfs_le, 8) != 0) return -1;
                                if (old_head != 0) {
                                    if (bstack_set(a->bs, old_head + 8,
                                                   nfs_le, 8) != 0) return -1;
                                }
                            }
                        } else {
                            /* Sub-case 4b: use the entire merged block */
                            uint8_t size_le[8];
                            size_t  ftr_off = (size_t)(next_size + ALFF_BLOCK_OVERHEAD);
                            write_le64(zero_buff + ftr_off, merged_size);
                            write_le64(size_le, merged_size);
                            if (bstack_set(a->bs, slice.offset - ALFF_BLOCK_HDR_SIZE,
                                           size_le, 8) != 0) {
                                free(zero_buff); return -1;
                            }
                            if (bstack_set(a->bs, slice.offset + block_size,
                                           zero_buff, zero_buf_sz) != 0) {
                                free(zero_buff); return -1;
                            }
                            free(zero_buff);
                        }
                    }

                    if (alff_clear_recovery_needed(a->bs) != 0) return -1;
                    out->allocator = self;
                    out->offset    = slice.offset;
                    out->len       = new_len;
                    return 0;
                }
            }
        }

        /* Case 5: find another free block, copy data there */
        {
            uint64_t found_start = 0, found_size = 0;
            if (alff_find_large_enough_block(a->bs, aligned_new_len,
                                              &found_start, &found_size) != 0) return -1;

            if (found_start != 0) {
                size_t   buf_sz;
                uint8_t *data_buf;
                uint64_t copy_len, new_payload;

#if UINT64_MAX > SIZE_MAX
                if (ALFF_BLOCK_OVERHEAD + aligned_new_len > (uint64_t)SIZE_MAX) {
                    errno = EINVAL;
                    return -1;
                }
#endif
                buf_sz   = (size_t)(ALFF_BLOCK_OVERHEAD + aligned_new_len);
                data_buf = calloc(1, buf_sz);
                if (!data_buf) return -1;

                copy_len = slice.len < aligned_new_len ? slice.len : aligned_new_len;
                if (copy_len > 0) {
                    if (bstack_get(a->bs, slice.offset, slice.offset + copy_len,
                                   data_buf + ALFF_BLOCK_HDR_SIZE) != 0) {
                        free(data_buf); return -1;
                    }
                }

                if (alff_set_recovery_needed(a->bs) != 0) { free(data_buf); return -1; }
                if (alff_unlink_block(a->bs, found_start, found_size,
                                      aligned_new_len, data_buf) != 0) {
                    free(data_buf); return -1;
                }
                free(data_buf);

                new_payload =
                    (found_size >= aligned_new_len + ALFF_BLOCK_OVERHEAD + ALFF_MIN_PAYLOAD)
                    ? found_start + found_size - aligned_new_len
                    : found_start;

                if (alff_add_to_free_list(a->bs, slice.offset) != 0) return -1;
                if (alff_clear_recovery_needed(a->bs) != 0) return -1;

                out->allocator = self;
                out->offset    = new_payload;
                out->len       = new_len;
                return 0;
            }
        }
    } /* end block_size scope */

    /* Case 6: no free block fits — push a new tail block and free the old one */
    {
        size_t   block_sz;
        uint8_t *block_buf;
        uint64_t push_offset, new_ptr, copy_len;
        uint8_t  size_le[8];

#if UINT64_MAX > SIZE_MAX
        if (aligned_new_len + ALFF_BLOCK_OVERHEAD > (uint64_t)SIZE_MAX) {
            errno = EINVAL;
            return -1;
        }
#endif
        block_sz  = (size_t)(aligned_new_len + ALFF_BLOCK_OVERHEAD);
        block_buf = calloc(1, block_sz);
        if (!block_buf) return -1;

        write_le64(size_le, aligned_new_len);
        memcpy(block_buf, size_le, 8);

        /* Re-read actual block_size from header for copy_len */
        {
            uint8_t bsz_buf[8];
            uint64_t bsz;
            if (bstack_get(a->bs, slice.offset - ALFF_BLOCK_HDR_SIZE,
                           slice.offset - ALFF_BLOCK_HDR_SIZE + 8,
                           bsz_buf) != 0) { free(block_buf); return -1; }
            bsz = read_le64(bsz_buf);
            copy_len = slice.len < aligned_new_len ? slice.len : aligned_new_len;
            (void)bsz;
        }

        if (copy_len > 0) {
            if (bstack_get(a->bs, slice.offset, slice.offset + copy_len,
                           block_buf + ALFF_BLOCK_HDR_SIZE) != 0) {
                free(block_buf); return -1;
            }
        }
        memcpy(block_buf + ALFF_BLOCK_HDR_SIZE + aligned_new_len, size_le, 8);

        if (alff_set_recovery_needed(a->bs) != 0) { free(block_buf); return -1; }
        if (bstack_push(a->bs, block_buf, block_sz, &push_offset) != 0) {
            free(block_buf); return -1;
        }
        free(block_buf);
        new_ptr = push_offset + ALFF_BLOCK_HDR_SIZE;

        if (alff_add_to_free_list(a->bs, slice.offset) != 0) return -1;
        if (alff_clear_recovery_needed(a->bs) != 0) return -1;

        out->allocator = self;
        out->offset    = new_ptr;
        out->len       = new_len;
        return 0;
    }
}

static const bstack_allocator_vtbl_t ff_vtbl = {
    ff_vt_stack,
    ff_vt_alloc,
    ff_vt_realloc,
    ff_vt_dealloc
};

/* =========================================================================
 * first_fit_bstack_allocator_t — public API
 * ====================================================================== */

first_fit_bstack_allocator_t *first_fit_bstack_allocator_new(bstack_t *bs)
{
    first_fit_bstack_allocator_t *a;
    uint64_t stack_len;
    int      recovery_needed = 0;

    a = malloc(sizeof *a);
    if (!a) { errno = ENOMEM; return NULL; }
    a->base.vtbl = &ff_vtbl;
    a->bs        = bs;

    if (bstack_len(bs, &stack_len) != 0) { free(a); return NULL; }

    if (stack_len == 0) {
        /* Empty stack: write the 48-byte allocator header */
        uint8_t hdr[48];
        memset(hdr, 0, 48);
        memcpy(hdr + 16, alff_magic, 8); /* magic at OFFSET_SIZE offset */
        /* flags, reserved, free_head stay zero */
        if (bstack_push(bs, hdr, 48, NULL) != 0) { free(a); return NULL; }
        return a;
    }

    /* Non-empty: must have room for the full allocator header */
    if (stack_len < 48) {
        free(a);
        errno = EINVAL;
        return NULL;
    }

    {
        uint8_t  header[32];
        uint64_t free_head;

        /* Read the 32-byte allocator header at payload offset 16 */
        if (bstack_get(bs, 16, 48, header) != 0) { free(a); return NULL; }

        if (memcmp(header, alff_magic_prefix, 6) != 0) {
            free(a);
            errno = EINVAL;
            return NULL;
        }

        /* flags at header[8], bit 0 = recovery_needed */
        recovery_needed = (header[8] & 1) != 0;

        /* free_head at header[16] (= magic(8) + flags(4) + reserved(4)) */
        free_head = read_le64(header + 16);
        if (free_head != 0) {
            uint64_t min_valid = ALFF_OFFSET_SIZE + ALFF_HEADER_SIZE + ALFF_BLOCK_HDR_SIZE;
            if (free_head < min_valid || free_head >= stack_len)
                recovery_needed = 1;
        }
    }

    if (recovery_needed) {
        if (alff_recovery(bs) != 0) { free(a); return NULL; }
    }

    return a;
}

void first_fit_bstack_allocator_free(first_fit_bstack_allocator_t *alloc)
{
    free(alloc);
}

bstack_t *first_fit_bstack_allocator_into_stack(first_fit_bstack_allocator_t *alloc)
{
    bstack_t *bs = alloc->bs;
    free(alloc);
    return bs;
}

#endif /* BSTACK_FEATURE_SET */

#ifdef __cplusplus
}
#endif
