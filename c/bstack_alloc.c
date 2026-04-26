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

#ifdef __cplusplus
}
#endif
