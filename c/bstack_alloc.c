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

/* In-memory mutex for first_fit_bstack_allocator_t under -DBSTACK_FEATURE_ATOMIC.
 * Serializes free-list mutation and stack extension/discard so a single handle
 * may be shared across threads.  MUTEX_LOCK/MUTEX_UNLOCK compile to no-ops when the
 * atomic feature is off, leaving the single-threaded code path unchanged. */
#ifdef BSTACK_FEATURE_ATOMIC
#  ifdef _WIN32
#    include <windows.h>
#    define MUTEX_LOCK(a)   EnterCriticalSection((CRITICAL_SECTION *)(a)->lock)
#    define MUTEX_UNLOCK(a) LeaveCriticalSection((CRITICAL_SECTION *)(a)->lock)
#  else
#    include <pthread.h>
#    define MUTEX_LOCK(a)   ((void)pthread_mutex_lock((pthread_mutex_t *)(a)->lock))
#    define MUTEX_UNLOCK(a) ((void)pthread_mutex_unlock((pthread_mutex_t *)(a)->lock))
#  endif
#else
#  define MUTEX_LOCK(a)   ((void)0)
#  define MUTEX_UNLOCK(a) ((void)0)
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

bstack_slice_t bstack_slice_empty(bstack_allocator_t *a)
{
    bstack_slice_t s;
    s.allocator = a;
    s.offset    = 0;
    s.len       = 0;
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

int bstack_slice_read_range(bstack_slice_t s, uint64_t start, uint64_t end,
                            uint8_t *buf)
{
    if (start > end || end > s.len) {
        errno = EINVAL;
        return -1;
    }
    if (start == end)
        return 0;
    return bstack_get(slice_stack(s),
                      s.offset + start,
                      s.offset + end,
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
 * bstack_guarded_slice_t — I/O
 * ====================================================================== */

int bstack_guarded_slice_read(bstack_guarded_slice_t gs, uint8_t *buf)
{
    const bstack_guard_vtbl_t *v = gs.vtbl;
    if (v && v->pre_read && v->pre_read(gs.ctx, gs.slice) != 0)
        return -1;
    if (v && v->read) {
        if (v->read(gs.ctx, gs.slice, 0, buf, (size_t)gs.slice.len) != 0)
            return -1;
    } else {
        if (bstack_slice_read(gs.slice, buf) != 0)
            return -1;
    }
    if (v && v->post_read
            && v->post_read(gs.ctx, gs.slice, buf, (size_t)gs.slice.len) != 0)
        return -1;
    return 0;
}

int bstack_guarded_slice_read_into(bstack_guarded_slice_t gs,
                                    uint8_t *buf, size_t buf_len)
{
    const bstack_guard_vtbl_t *v = gs.vtbl;
    size_t n;
    if (buf_len == 0 || gs.slice.len == 0)
        return 0;
    n = ((uint64_t)buf_len < gs.slice.len) ? buf_len : (size_t)gs.slice.len;
    if (v && v->pre_read && v->pre_read(gs.ctx, gs.slice) != 0)
        return -1;
    if (v && v->read) {
        if (v->read(gs.ctx, gs.slice, 0, buf, n) != 0)
            return -1;
    } else {
        if (bstack_slice_read_into(gs.slice, buf, buf_len) != 0)
            return -1;
    }
    if (v && v->post_read && v->post_read(gs.ctx, gs.slice, buf, n) != 0)
        return -1;
    return 0;
}

int bstack_guarded_slice_read_range_into(bstack_guarded_slice_t gs,
                                          uint64_t start,
                                          uint8_t *buf, size_t buf_len)
{
    const bstack_guard_vtbl_t *v = gs.vtbl;
    if (buf_len == 0)
        return 0;
    if (start > gs.slice.len || (uint64_t)buf_len > gs.slice.len - start) {
        errno = EINVAL;
        return -1;
    }
    if (v && v->pre_read && v->pre_read(gs.ctx, gs.slice) != 0)
        return -1;
    if (v && v->read) {
        if (v->read(gs.ctx, gs.slice, start, buf, buf_len) != 0)
            return -1;
    } else {
        if (bstack_slice_read_range_into(gs.slice, start, buf, buf_len) != 0)
            return -1;
    }
    if (v && v->post_read && v->post_read(gs.ctx, gs.slice, buf, buf_len) != 0)
        return -1;
    return 0;
}

int bstack_guarded_slice_read_range(bstack_guarded_slice_t gs,
                                     uint64_t start, uint64_t end,
                                     uint8_t *buf)
{
    const bstack_guard_vtbl_t *v = gs.vtbl;
    size_t len;
    if (start > end || end > gs.slice.len) {
        errno = EINVAL;
        return -1;
    }
    if (start == end)
        return 0;
    len = (size_t)(end - start);
    if (v && v->pre_read && v->pre_read(gs.ctx, gs.slice) != 0)
        return -1;
    if (v && v->read) {
        if (v->read(gs.ctx, gs.slice, start, buf, len) != 0)
            return -1;
    } else {
        if (bstack_slice_read_range(gs.slice, start, end, buf) != 0)
            return -1;
    }
    if (v && v->post_read && v->post_read(gs.ctx, gs.slice, buf, len) != 0)
        return -1;
    return 0;
}

int bstack_guarded_slice_subslice(bstack_guarded_slice_t gs,
                                   uint64_t start, uint64_t end,
                                   bstack_guarded_slice_t *out)
{
    const bstack_guard_vtbl_t *v = gs.vtbl;
    int r;
    if (v && v->subview)
        r = v->subview(gs.ctx, gs.slice, start, end, &out->slice);
    else
        r = bstack_slice_subslice(gs.slice, start, end, &out->slice);
    if (r != 0)
        return -1;
    out->vtbl = gs.vtbl;
    out->ctx  = gs.ctx;
    return 0;
}

#ifdef BSTACK_FEATURE_SET

/* Clip n to cap after a pre_write hook may have changed it. */
static size_t guard_clip(size_t n, uint64_t cap)
{
    if (cap > (uint64_t)SIZE_MAX)
        cap = (uint64_t)SIZE_MAX;
    return (n > (size_t)cap) ? (size_t)cap : n;
}

int bstack_guarded_slice_write(bstack_guarded_slice_t gs,
                                const uint8_t *data, size_t data_len)
{
    const bstack_guard_vtbl_t *v = gs.vtbl;
    const uint8_t *buf = data;
    size_t n;
    if (data_len == 0 || gs.slice.len == 0)
        return 0;
    n = ((uint64_t)data_len < gs.slice.len) ? data_len : (size_t)gs.slice.len;
    if (v && v->pre_write) {
        if (v->pre_write(gs.ctx, gs.slice, &buf, &n) != 0)
            return -1;
        n = guard_clip(n, gs.slice.len);
    }
    if (n == 0)
        return 0;
    if (v && v->write) {
        if (v->write(gs.ctx, gs.slice, 0, buf, n) != 0)
            return -1;
    } else {
        if (bstack_set(slice_stack(gs.slice), gs.slice.offset, buf, n) != 0)
            return -1;
    }
    if (v && v->post_write && v->post_write(gs.ctx, gs.slice) != 0)
        return -1;
    return 0;
}

int bstack_guarded_slice_write_range(bstack_guarded_slice_t gs,
                                      uint64_t start,
                                      const uint8_t *data, size_t data_len)
{
    const bstack_guard_vtbl_t *v = gs.vtbl;
    const uint8_t *buf = data;
    size_t n = data_len;
    if (data_len == 0)
        return 0;
    if (start > gs.slice.len || (uint64_t)data_len > gs.slice.len - start) {
        errno = EINVAL;
        return -1;
    }
    if (v && v->pre_write) {
        if (v->pre_write(gs.ctx, gs.slice, &buf, &n) != 0)
            return -1;
        n = guard_clip(n, gs.slice.len - start);
    }
    if (n == 0)
        return 0;
    if (v && v->write) {
        if (v->write(gs.ctx, gs.slice, start, buf, n) != 0)
            return -1;
    } else {
        if (bstack_set(slice_stack(gs.slice), gs.slice.offset + start, buf, n) != 0)
            return -1;
    }
    if (v && v->post_write && v->post_write(gs.ctx, gs.slice) != 0)
        return -1;
    return 0;
}

int bstack_guarded_slice_zero(bstack_guarded_slice_t gs)
{
    const bstack_guard_vtbl_t *v = gs.vtbl;
    const uint8_t *buf = NULL;
    size_t n;
    if (gs.slice.len == 0)
        return 0;
#if UINT64_MAX > SIZE_MAX
    if (gs.slice.len > (uint64_t)SIZE_MAX) {
        errno = EINVAL;
        return -1;
    }
#endif
    n = (size_t)gs.slice.len;
    if (v && v->pre_write) {
        if (v->pre_write(gs.ctx, gs.slice, &buf, &n) != 0)
            return -1;
        n = guard_clip(n, gs.slice.len);
    }
    if (n == 0)
        return 0;
    if (v && v->write) {
        if (v->write(gs.ctx, gs.slice, 0, buf, n) != 0)
            return -1;
    } else if (buf != NULL) {
        if (bstack_set(slice_stack(gs.slice), gs.slice.offset, buf, n) != 0)
            return -1;
    } else {
        if (bstack_zero(slice_stack(gs.slice), gs.slice.offset, n) != 0)
            return -1;
    }
    if (v && v->post_write && v->post_write(gs.ctx, gs.slice) != 0)
        return -1;
    return 0;
}

int bstack_guarded_slice_zero_range(bstack_guarded_slice_t gs,
                                     uint64_t start, uint64_t n_bytes)
{
    const bstack_guard_vtbl_t *v = gs.vtbl;
    const uint8_t *buf = NULL;
    size_t n;
    if (n_bytes == 0)
        return 0;
    if (start > gs.slice.len || n_bytes > gs.slice.len - start) {
        errno = EINVAL;
        return -1;
    }
#if UINT64_MAX > SIZE_MAX
    if (n_bytes > (uint64_t)SIZE_MAX) {
        errno = EINVAL;
        return -1;
    }
#endif
    n = (size_t)n_bytes;
    if (v && v->pre_write) {
        if (v->pre_write(gs.ctx, gs.slice, &buf, &n) != 0)
            return -1;
        n = guard_clip(n, gs.slice.len - start);
    }
    if (n == 0)
        return 0;
    if (v && v->write) {
        if (v->write(gs.ctx, gs.slice, start, buf, n) != 0)
            return -1;
    } else if (buf != NULL) {
        if (bstack_set(slice_stack(gs.slice), gs.slice.offset + start, buf, n) != 0)
            return -1;
    } else {
        if (bstack_zero(slice_stack(gs.slice), gs.slice.offset + start, n) != 0)
            return -1;
    }
    if (v && v->post_write && v->post_write(gs.ctx, gs.slice) != 0)
        return -1;
    return 0;
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
    /*
     * Every failure path below leaves the original region untouched: the
     * validation checks reject before any mutation, and the extend/discard
     * calls are each a single bstack operation that either fully commits or
     * leaves the stack unchanged. So every failure here is survivable (-1),
     * never a loss (-2).
     */
    if (bstack_len(a->bs, &cur_tail) != 0) {
        *out = slice;
        return -1;
    }
    if (slice.offset + slice.len != cur_tail) {
        errno = ENOTSUP;
        *out = slice;
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
            *out = slice;
            return -1;
        }
#endif
        if (bstack_extend(a->bs, (size_t)extra, &dummy) != 0) {
            *out = slice;
            return -1;
        }
    } else {
        shrink = slice.len - new_len;
#if UINT64_MAX > SIZE_MAX
        if (shrink > (uint64_t)SIZE_MAX) {
            errno = EINVAL;
            *out = slice;
            return -1;
        }
#endif
        if (bstack_discard(a->bs, (size_t)shrink) != 0) {
            *out = slice;
            return -1;
        }
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
    /*
     * discard is a single bstack operation: on failure the stack is
     * unchanged, so slice always survives a failed dealloc here (-1),
     * never -2.
     */
    if (bstack_len(a->bs, &cur_tail) != 0)
        return -1;
    if (slice.offset + slice.len == cur_tail) {
#if UINT64_MAX > SIZE_MAX
        if (slice.len > (uint64_t)SIZE_MAX) {
            errno = EINVAL;
            return -1;
        }
#endif
        return bstack_discard(a->bs, (size_t)slice.len) == 0 ? 0 : -1;
    }
    return 0; /* non-tail slice: no-op */
}

static int
linear_vt_alloc_bulk(bstack_allocator_t *self, const uint64_t *lens, size_t n,
                     bstack_slice_t *out_slices)
{
    size_t i;
    for (i = 0; i < n; i++) {
        if (linear_vt_alloc(self, lens[i], &out_slices[i]) != 0) {
            /* roll back in reverse — each is the current tail */
            while (i > 0) {
                i--;
                linear_vt_dealloc(self, out_slices[i]);
            }
            return -1;
        }
    }
    return 0;
}

static int
linear_vt_dealloc_bulk(bstack_allocator_t *self, const bstack_slice_t *slices,
                       size_t n)
{
    size_t i;
    for (i = 0; i < n; i++) {
        if (linear_vt_dealloc(self, slices[i]) != 0)
            return -1;
    }
    return 0;
}

static const bstack_bulk_allocator_vtbl_t linear_bulk_vtbl = {
    { linear_vt_stack, linear_vt_alloc, linear_vt_realloc, linear_vt_dealloc },
    linear_vt_alloc_bulk,
    linear_vt_dealloc_bulk
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
    a->base.vtbl      = &linear_bulk_vtbl.base;
    a->base.bulk_vtbl = &linear_bulk_vtbl;
    a->bs             = bs;
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

static const uint8_t alff_magic[8]        = {'A','L','F','F',0,1,3,0};
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
#ifdef BSTACK_FEATURE_ATOMIC
    /* CAS 0 -> 1.  Mutual exclusion is provided by the allocator's in-memory
     * mutex; this CAS is a no-cost check over the disk write we must perform
     * anyway.  A failure means the flag was already set by a prior operation
     * that crashed or failed mid-mutation, so the stack needs recovery (reopen)
     * before it is safe to mutate — surface that rather than proceeding. */
    static const uint8_t zero[4] = {0, 0, 0, 0};
    uint8_t one[4];
    int ok = 0;
    write_le32(one, 1);
    if (bstack_cas(bs, ALFF_FLAGS_OFFSET, zero, one, 4, &ok) != 0) return -1;
    /* errno = EINVAL: the on-disk recovery_needed flag is not in a valid state
     * for this operation (it was already 1, meaning a prior op crashed mid-
     * mutation and the stack must be reopened to run recovery). */
    if (!ok) { errno = EINVAL; return -1; }
    return 0;
#else
    uint8_t flag[4];
    write_le32(flag, 1);
    return bstack_set(bs, ALFF_FLAGS_OFFSET, flag, 4);
#endif
}

static int alff_clear_recovery_needed(bstack_t *bs)
{
#ifdef BSTACK_FEATURE_ATOMIC
    /* CAS 1 -> 0, the inverse of alff_set_recovery_needed.  A failure means the
     * flag was not set when we expected it, indicating the paired set was lost
     * or the flag was disturbed out of band. */
    static const uint8_t zero[4] = {0, 0, 0, 0};
    uint8_t one[4];
    int ok = 0;
    write_le32(one, 1);
    if (bstack_cas(bs, ALFF_FLAGS_OFFSET, one, zero, 4, &ok) != 0) return -1;
    /* errno = EINVAL: the on-disk flag was not in the expected state (it was
     * already 0, meaning the paired set was lost or the flag was disturbed
     * out of band). */
    if (!ok) { errno = EINVAL; return -1; }
    return 0;
#else
    uint8_t flag[4] = {0, 0, 0, 0};
    return bstack_set(bs, ALFF_FLAGS_OFFSET, flag, 4);
#endif
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
    uint64_t stack_len, head, max_walk, walk_count;
    uint8_t head_buf[8];

    *out_start = 0;
    *out_size  = 0;

    if (bstack_len(bs, &stack_len) != 0) return -1;

    /* Upper bound on free blocks; used to detect cycles caused by corruption. */
    max_walk = (stack_len > ALFF_ARENA_START)
               ? (stack_len - ALFF_ARENA_START)
                 / (ALFF_MIN_PAYLOAD + ALFF_BLOCK_OVERHEAD) + 1
               : 1;
    walk_count = 0;

    if (bstack_get(bs, ALFF_FREE_HEAD_OFFSET,
                   ALFF_FREE_HEAD_OFFSET + 8, head_buf) != 0) return -1;
    head = read_le64(head_buf);

    while (head != 0) {
        /* Read block header (16 B) + first 8 B of payload (next_free) */
        uint8_t buf[24];
        uint64_t block_size, next;

        walk_count++;
        if (walk_count > max_walk) {
            errno = EINVAL;
            return -1;
        }

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
/*
 * Caller (ff_vt_dealloc tail path) must set recovery_needed before calling and
 * clear it after; this function does not touch the flag itself.  Holds no
 * locks: the only caller acquires the allocator mutex (under
 * BSTACK_FEATURE_ATOMIC) across the discard + cascade sequence.
 */
static int alff_cascade_discard_free_tail(bstack_t *bs)
{
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

        if (alff_unlink_from_free_list(bs, hdr + ALFF_BLOCK_HDR_SIZE) != 0) return -1;
        discard_n = (size_t)(sz + ALFF_BLOCK_OVERHEAD);
        if (bstack_discard(bs, discard_n) != 0) return -1;
    }

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

        /* The header does not describe a valid block (size below minimum,
         * unaligned, or overflowing u64). Two cases:
         *   * All-zero trailing region -> an interrupted tail-grow realloc,
         *     which extends (zero-filling) the payload before rewriting the
         *     header/footer to cover it. The valid block ends at pos and the
         *     zeros beyond it carry no header (size reads 0). A real block is
         *     never all-zero (size >= ALFF_MIN_PAYLOAD), so roll the extension
         *     back by truncating to pos — restoring the pre-grow tail the
         *     failed realloc already handed back to the caller.
         *   * Anything else -> genuine mid-arena corruption; fail loudly
         *     rather than silently discard the data that follows. */
        if (size < ALFF_MIN_PAYLOAD || size % 8 != 0
            || size > UINT64_MAX - ALFF_BLOCK_OVERHEAD) {
            uint8_t *trailing;
            int      all_zero = 1;
            uint64_t k;
#if UINT64_MAX > SIZE_MAX
            if (remaining > (uint64_t)SIZE_MAX) { errno = EINVAL; ret = -1; goto done; }
#endif
            trailing = malloc((size_t)remaining);
            if (!trailing) { ret = -1; goto done; }
            if (bstack_get(bs, pos, pos + remaining, trailing) != 0) {
                free(trailing); ret = -1; goto done;
            }
            for (k = 0; k < remaining; k++) {
                if (trailing[k] != 0) { all_zero = 0; break; }
            }
            free(trailing);
            if (all_zero) {
                if (bstack_discard(bs, (size_t)remaining) != 0) { ret = -1; goto done; }
                break;
            }
            errno = EINVAL;
            ret = -1;
            goto done;
        }
        /* Valid size but block extends past the stack end: partial tail write. */
        if (size + ALFF_BLOCK_OVERHEAD > stack_len - pos) {
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

        /* Normalize the footer to the (authoritative) header size. Every
         * block-resizing operation commits its new size to the header before the
         * matching footer (a coalescing free writes header then footer, a tail
         * grow writes header then footer, a split's header is fixed above), so a
         * crash between those two writes leaves the header correct and the footer
         * stale. The walk follows headers, so a stale footer slips through here
         * yet corrupts a later neighbour's coalesce (which reads this footer) and
         * eventually desyncs the walk. Rewriting the footer to match makes the
         * block whole; healthy blocks already agree, so this is a no-op. */
        {
            uint64_t footer_pos = pos + ALFF_BLOCK_HDR_SIZE + size;
            uint8_t  cur_ftr[8];
            if (bstack_get(bs, footer_pos, footer_pos + 8, cur_ftr) != 0) {
                ret = -1; goto done;
            }
            if (read_le64(cur_ftr) != size) {
                uint8_t size_le[8];
                write_le64(size_le, size);
                if (bstack_set(bs, footer_pos, size_le, 8) != 0) {
                    ret = -1; goto done;
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

    /* Authoritative reset: recovery may have been triggered with the on-disk
     * flag already clear (e.g. an out-of-range free_head), so write 0 directly
     * rather than via the CAS clear, which would fail when the flag is not 1. */
    {
        uint8_t z[4] = {0, 0, 0, 0};
        ret = bstack_set(bs, ALFF_FLAGS_OFFSET, z, 4);
    }

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

    /* Hold the lock across the free-list search and the mutation/extension that
     * follows, so the read-modify-write of the free list (and any tail push) is
     * atomic w.r.t. other threads.  recovery_needed is set only around the
     * actual free-list mutation below. */
    MUTEX_LOCK(a);

    if (alff_find_large_enough_block(a->bs, aligned_len,
                                      &found_start, &found_size) != 0) {
        MUTEX_UNLOCK(a);
        return -1;
    }

    if (found_start != 0) {
        /* Reuse a free block (split if large enough, otherwise take whole) */
        size_t   buf_sz;
        uint8_t *content_buf;

#if UINT64_MAX > SIZE_MAX
        if (ALFF_BLOCK_OVERHEAD + aligned_len > (uint64_t)SIZE_MAX) {
            MUTEX_UNLOCK(a);
            errno = EINVAL;
            return -1;
        }
#endif
        buf_sz = (size_t)(ALFF_BLOCK_OVERHEAD + aligned_len);
        content_buf = calloc(1, buf_sz);
        if (!content_buf) { MUTEX_UNLOCK(a); return -1; }

        if (alff_set_recovery_needed(a->bs) != 0
            || alff_unlink_block(a->bs, found_start, found_size,
                                  aligned_len, content_buf) != 0
            || alff_clear_recovery_needed(a->bs) != 0) {
            free(content_buf);
            MUTEX_UNLOCK(a);
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
            MUTEX_UNLOCK(a);
            errno = EINVAL;
            return -1;
        }
#endif
        block_sz  = (size_t)(aligned_len + ALFF_BLOCK_OVERHEAD);
        block_buf = calloc(1, block_sz);
        if (!block_buf) { MUTEX_UNLOCK(a); return -1; }

        write_le64(size_le, aligned_len);
        memcpy(block_buf, size_le, 8);
        memcpy(block_buf + ALFF_BLOCK_HDR_SIZE + aligned_len, size_le, 8);

        /* push is a single atomic bstack call; the lock above already excludes
         * concurrent tail modification, so no recovery_needed marking here. */
        if (bstack_push(a->bs, block_buf, block_sz, &push_offset) != 0) {
            free(block_buf);
            MUTEX_UNLOCK(a);
            return -1;
        }
        free(block_buf);
        payload = push_offset + ALFF_BLOCK_HDR_SIZE;
    }

    out->allocator = self;
    out->offset    = payload;
    out->len       = len;
    MUTEX_UNLOCK(a);
    return 0;
}

static int ff_vt_dealloc(bstack_allocator_t *self, bstack_slice_t slice)
{
    first_fit_bstack_allocator_t *a = (first_fit_bstack_allocator_t *)self;
    uint64_t aligned_len = alff_align_len(slice.len);
    uint64_t stack_len;
    int      r;

    /* Hold the lock across the tail check and the free-list mutation / tail
     * discard, so the read of the tail and the write that follows are atomic
     * w.r.t. other threads.  The validation reads below are harmless to do
     * under the lock and avoid a second bstack_len call. */
    MUTEX_LOCK(a);

    if (bstack_len(a->bs, &stack_len) != 0) { MUTEX_UNLOCK(a); return -1; }

    if (alff_is_impossible_block_start(stack_len, slice.offset)
        || alff_is_impossible_block_end(stack_len, slice.offset + aligned_len)
        || alff_is_impossible_block_size(stack_len, aligned_len)) {
        MUTEX_UNLOCK(a);
        errno = EINVAL;
        return -1;
    }

    /* Double-free detection: the block must not already be marked free. */
    {
        uint8_t flags_buf[4];
        if (bstack_get(a->bs,
                       slice.offset - ALFF_BLOCK_HDR_SIZE + 8,
                       slice.offset - ALFF_BLOCK_HDR_SIZE + 12,
                       flags_buf) != 0) {
            MUTEX_UNLOCK(a);
            return -1;
        }
        if (flags_buf[0] & 1) {
            MUTEX_UNLOCK(a);
            errno = EINVAL;
            return -1;
        }
    }

    /* Tail block fast path: discard the block bytes, then cascade-discard any
     * free blocks now at the tail.  recovery_needed is set before the discard
     * (0.2.1 fix) so a crash anywhere in the discard + cascade sequence is
     * detected on reopen.  This function is the sole manager of the flag for
     * the tail path; cascade does not touch it. */
    if (slice.offset + aligned_len == stack_len - ALFF_BLOCK_FTR_SIZE) {
        size_t discard_n;
#if UINT64_MAX > SIZE_MAX
        if (aligned_len + ALFF_BLOCK_OVERHEAD > (uint64_t)SIZE_MAX) {
            MUTEX_UNLOCK(a);
            errno = EINVAL;
            return -1;
        }
#endif
        discard_n = (size_t)(aligned_len + ALFF_BLOCK_OVERHEAD);
        if (alff_set_recovery_needed(a->bs) != 0) { MUTEX_UNLOCK(a); return -1; }
        /* Past this point the tail block is being physically discarded: a
         * failure below can no longer safely hand the original back. */
        if (bstack_discard(a->bs, discard_n) != 0) { MUTEX_UNLOCK(a); return -2; }
        if (alff_cascade_discard_free_tail(a->bs) != 0) { MUTEX_UNLOCK(a); return -2; }
        r = alff_clear_recovery_needed(a->bs);
        MUTEX_UNLOCK(a);
        return r == 0 ? 0 : -2;
    }

    if (alff_set_recovery_needed(a->bs) != 0) { MUTEX_UNLOCK(a); return -1; }
    /* Past this point the block is being pushed onto the free list: a
     * failure below can no longer safely hand the original back. */
    if (alff_add_to_free_list(a->bs, slice.offset) != 0) { MUTEX_UNLOCK(a); return -2; }
    if (alff_cascade_discard_free_tail(a->bs) != 0) { MUTEX_UNLOCK(a); return -2; }
    r = alff_clear_recovery_needed(a->bs);
    MUTEX_UNLOCK(a);
    return r == 0 ? 0 : -2;
}

static int ff_vt_realloc(bstack_allocator_t *self, bstack_slice_t slice,
                          uint64_t new_len, bstack_slice_t *out)
{
    first_fit_bstack_allocator_t *a = (first_fit_bstack_allocator_t *)self;
    uint64_t aligned_current_len = alff_align_len(slice.len);
    uint64_t aligned_new_len;
    uint64_t stack_len;
    /*
     * The surviving allocation to hand back on failure. Starts as the
     * original block; in the move-to-new-block paths it becomes the new
     * region once that region is committed and populated (which happens
     * before the old block is freed), so a later failure freeing the old
     * block still returns a valid, fully-resized handle. Every failure path
     * in this function leaves `recovered` valid, so realloc here always
     * fails with -1 (survived), never -2 (lost).
     */
    bstack_slice_t recovered = slice;
    recovered.allocator = self;

    /* Validation reads stack_len once.  No lock yet — only caller-owned bytes
     * are touched by the lock-free fast paths (cases 1 and 3). */
    if (bstack_len(a->bs, &stack_len) != 0) { *out = recovered; return -1; }

    if (alff_is_impossible_block_start(stack_len, slice.offset)
        || alff_is_impossible_block_end(stack_len,
               slice.offset + aligned_current_len)
        || alff_is_impossible_block_size(stack_len, aligned_current_len)) {
        errno = EINVAL;
        *out = recovered; return -1;
    }

    aligned_new_len = alff_align_len(new_len);

    /* Case 1: same aligned bucket — no block resize needed.  Lock-free: the
     * zero-fill touches only caller-owned bytes within an allocated block. */
    if (aligned_new_len == aligned_current_len) {
        if (new_len > slice.len) {
            size_t zero_n = (size_t)(new_len - slice.len);
            if (bstack_zero(a->bs, slice.offset + slice.len, zero_n) != 0) { *out = recovered; return -1; }
        }
        out->allocator = self;
        out->offset    = slice.offset;
        out->len       = new_len;
        return 0;
    }

    /* Case 2: tail block — extend or shrink in place.
     * Hold the lock for the whole tail check + resize: reading the tail and
     * then extending/discarding it must be atomic w.r.t. other threads' pushes.
     * If this is not the tail block the lock is released before the lock-free
     * in-place paths below run. */
    MUTEX_LOCK(a);
    if (bstack_len(a->bs, &stack_len) != 0) { MUTEX_UNLOCK(a); *out = recovered; return -1; }
    if (slice.offset + aligned_current_len == stack_len - ALFF_BLOCK_FTR_SIZE) {
        uint8_t size_le[8];
        if (aligned_new_len > aligned_current_len) {
            uint64_t delta  = aligned_new_len - aligned_current_len;
            uint64_t zero_n = aligned_current_len + ALFF_BLOCK_FTR_SIZE - slice.len;
#if UINT64_MAX > SIZE_MAX
            if (delta > (uint64_t)SIZE_MAX || zero_n > (uint64_t)SIZE_MAX) {
                MUTEX_UNLOCK(a);
                errno = EINVAL;
                *out = recovered; return -1;
            }
#endif
            /* Tail-grow is multi-step (extend + zero + header + footer); without
             * the recovery flag a crash after extend but before the header write
             * leaves an unrecoverable mid-arena layout (for delta >= 24 bytes
             * recovery would error on the zero "header" past the old block). */
            if (alff_set_recovery_needed(a->bs) != 0) { MUTEX_UNLOCK(a); *out = recovered; return -1; }
            if (bstack_extend(a->bs, (size_t)delta, NULL) != 0) { MUTEX_UNLOCK(a); *out = recovered; return -1; }
            if (bstack_zero(a->bs, slice.offset + slice.len, (size_t)zero_n) != 0) { MUTEX_UNLOCK(a); *out = recovered; return -1; }
            write_le64(size_le, aligned_new_len);
            if (bstack_set(a->bs, slice.offset - ALFF_BLOCK_HDR_SIZE,
                           size_le, 8) != 0) { MUTEX_UNLOCK(a); *out = recovered; return -1; }
            if (bstack_set(a->bs, slice.offset + aligned_new_len,
                           size_le, 8) != 0) { MUTEX_UNLOCK(a); *out = recovered; return -1; }
            if (alff_clear_recovery_needed(a->bs) != 0) { MUTEX_UNLOCK(a); *out = recovered; return -1; }
        } else {
            /* Tail shrink: keep the block; don't reclaim the tail in place. A
             * physical shrink needs a header write plus a discard (metadata +
             * size change) that cannot be one crash-atomic call, and the
             * block-walking recovery cannot parse the torn intermediate.
             * Narrowing only the user length (an oversized block, as a non-tail
             * shrink does) needs no writes; the tail is reclaimed on free. */
        }
        out->allocator = self;
        out->offset    = slice.offset;
        out->len       = new_len;
        MUTEX_UNLOCK(a);
        return 0;
    }
    MUTEX_UNLOCK(a);  /* not the tail block; release before lock-free in-place paths */

    /* Read actual block size from header.  Lock-free: this is the allocated
     * block's own header, owned by the caller and stable. */
    {
        uint8_t block_size_buf[8];
        uint64_t block_size;
        if (bstack_get(a->bs, slice.offset - ALFF_BLOCK_HDR_SIZE,
                       slice.offset - ALFF_BLOCK_HDR_SIZE + 8,
                       block_size_buf) != 0) { *out = recovered; return -1; }
        block_size = read_le64(block_size_buf);

        /* Case 3: block already large enough for the new size.  Lock-free: the
         * zero-fill stays within the caller's allocated block. */
        if (block_size >= aligned_new_len) {
            if (new_len > slice.len) {
                size_t zero_n = (size_t)(new_len - slice.len);
                if (bstack_zero(a->bs, slice.offset + slice.len, zero_n) != 0) { *out = recovered; return -1; }
            }
            out->allocator = self;
            out->offset    = slice.offset;
            out->len       = new_len;
            return 0;
        }

        /* From here on we either merge with the adjacent free block or allocate
         * a fresh one: both walk/mutate the free list (and the new-block path
         * pushes), so hold the lock for the rest of the function. */
        MUTEX_LOCK(a);

        /* Case 4: try to merge with the free right neighbour in place */
        {
            uint64_t next_block = slice.offset + block_size + ALFF_BLOCK_OVERHEAD;
            if (bstack_len(a->bs, &stack_len) != 0) { MUTEX_UNLOCK(a); *out = recovered; return -1; }
            if (next_block <= stack_len - ALFF_BLOCK_FTR_SIZE - ALFF_MIN_PAYLOAD) {
                uint8_t next_hdr[16];
                uint64_t next_size;
                if (bstack_get(a->bs, next_block - ALFF_BLOCK_HDR_SIZE,
                               next_block - ALFF_BLOCK_HDR_SIZE + 16,
                               next_hdr) != 0) { MUTEX_UNLOCK(a); *out = recovered; return -1; }
                next_size = read_le64(next_hdr);

                if ((next_hdr[8] & 1) != 0
                    && next_size >= ALFF_MIN_PAYLOAD
                    && next_size % 8 == 0
                    && block_size + ALFF_BLOCK_OVERHEAD + next_size >= aligned_new_len) {

                    /* Pre-zero stale bytes beyond user-visible slice */
                    if (slice.len < block_size) {
                        size_t zero_n = (size_t)(block_size - slice.len);
                        if (bstack_zero(a->bs, slice.offset + slice.len,
                                        zero_n) != 0) { MUTEX_UNLOCK(a); *out = recovered; return -1; }
                    }

                    if (alff_set_recovery_needed(a->bs) != 0) { MUTEX_UNLOCK(a); *out = recovered; return -1; }
                    if (alff_unlink_from_free_list(a->bs, next_block) != 0) { MUTEX_UNLOCK(a); *out = recovered; return -1; }

                    {
                        uint64_t merged_size = block_size + ALFF_BLOCK_OVERHEAD + next_size;
                        size_t   zero_buf_sz =
                            (size_t)(next_size + ALFF_BLOCK_OVERHEAD + ALFF_BLOCK_FTR_SIZE);
                        uint8_t *zero_buff = calloc(1, zero_buf_sz);
                        if (!zero_buff) { MUTEX_UNLOCK(a); *out = recovered; return -1; }

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
                                free(zero_buff); MUTEX_UNLOCK(a); *out = recovered; return -1;
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
                                free(zero_buff); MUTEX_UNLOCK(a); *out = recovered; return -1;
                            }
                            if (bstack_set(a->bs, slice.offset + block_size,
                                           zero_buff, zero_buf_sz) != 0) {
                                free(zero_buff); MUTEX_UNLOCK(a); *out = recovered; return -1;
                            }
                            free(zero_buff);

                            /* Shrink allocated block header */
                            write_le64(size_le, aligned_new_len);
                            if (bstack_set(a->bs, slice.offset - ALFF_BLOCK_HDR_SIZE,
                                           size_le, 8) != 0) { MUTEX_UNLOCK(a); *out = recovered; return -1; }

                            /* Forward link: free_head → new free block */
                            {
                                uint8_t nfs_le[8];
                                write_le64(nfs_le, new_free_start);
                                if (bstack_set(a->bs, ALFF_FREE_HEAD_OFFSET,
                                               nfs_le, 8) != 0) { MUTEX_UNLOCK(a); *out = recovered; return -1; }
                                if (old_head != 0) {
                                    if (bstack_set(a->bs, old_head + 8,
                                                   nfs_le, 8) != 0) { MUTEX_UNLOCK(a); *out = recovered; return -1; }
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
                                free(zero_buff); MUTEX_UNLOCK(a); *out = recovered; return -1;
                            }
                            if (bstack_set(a->bs, slice.offset + block_size,
                                           zero_buff, zero_buf_sz) != 0) {
                                free(zero_buff); MUTEX_UNLOCK(a); *out = recovered; return -1;
                            }
                            free(zero_buff);
                        }
                    }

                    if (alff_clear_recovery_needed(a->bs) != 0) { MUTEX_UNLOCK(a); *out = recovered; return -1; }
                    out->allocator = self;
                    out->offset    = slice.offset;
                    out->len       = new_len;
                    MUTEX_UNLOCK(a);
                    return 0;
                }
            }
        }

        /* Case 5: find another free block, copy data there */
        {
            uint64_t found_start = 0, found_size = 0;
            if (alff_find_large_enough_block(a->bs, aligned_new_len,
                                              &found_start, &found_size) != 0) { MUTEX_UNLOCK(a); *out = recovered; return -1; }

            if (found_start != 0) {
                size_t   buf_sz;
                uint8_t *data_buf;
                uint64_t copy_len, new_payload;

#if UINT64_MAX > SIZE_MAX
                if (ALFF_BLOCK_OVERHEAD + aligned_new_len > (uint64_t)SIZE_MAX) {
                    MUTEX_UNLOCK(a);
                    errno = EINVAL;
                    *out = recovered; return -1;
                }
#endif
                buf_sz   = (size_t)(ALFF_BLOCK_OVERHEAD + aligned_new_len);
                data_buf = calloc(1, buf_sz);
                if (!data_buf) { MUTEX_UNLOCK(a); *out = recovered; return -1; }

                copy_len = slice.len < aligned_new_len ? slice.len : aligned_new_len;
                if (copy_len > 0) {
                    if (bstack_get(a->bs, slice.offset, slice.offset + copy_len,
                                   data_buf + ALFF_BLOCK_HDR_SIZE) != 0) {
                        free(data_buf); MUTEX_UNLOCK(a); *out = recovered; return -1;
                    }
                }

                if (alff_set_recovery_needed(a->bs) != 0) { free(data_buf); MUTEX_UNLOCK(a); *out = recovered; return -1; }
                if (alff_unlink_block(a->bs, found_start, found_size,
                                      aligned_new_len, data_buf) != 0) {
                    free(data_buf); MUTEX_UNLOCK(a); *out = recovered; return -1;
                }
                free(data_buf);

                new_payload =
                    (found_size >= aligned_new_len + ALFF_BLOCK_OVERHEAD + ALFF_MIN_PAYLOAD)
                    ? found_start + found_size - aligned_new_len
                    : found_start;

                /* The new block is committed and populated; it is now the
                 * survivor, so a failure freeing the old block still returns
                 * a valid, fully-resized handle (the old block leaks until
                 * crash recovery). */
                recovered.allocator = self;
                recovered.offset    = new_payload;
                recovered.len       = new_len;

                if (alff_add_to_free_list(a->bs, slice.offset) != 0) { MUTEX_UNLOCK(a); *out = recovered; return -1; }
                if (alff_cascade_discard_free_tail(a->bs) != 0) { MUTEX_UNLOCK(a); *out = recovered; return -1; }
                if (alff_clear_recovery_needed(a->bs) != 0) { MUTEX_UNLOCK(a); *out = recovered; return -1; }

                out->allocator = self;
                out->offset    = new_payload;
                out->len       = new_len;
                MUTEX_UNLOCK(a);
                return 0;
            }
        }
    } /* end block_size scope — lock still held for case 6 */

    /* Case 6: no free block fits — push a new tail block and free the old one */
    {
        size_t   block_sz;
        uint8_t *block_buf;
        uint64_t push_offset, new_ptr, copy_len;
        uint8_t  size_le[8];

#if UINT64_MAX > SIZE_MAX
        if (aligned_new_len + ALFF_BLOCK_OVERHEAD > (uint64_t)SIZE_MAX) {
            MUTEX_UNLOCK(a);
            errno = EINVAL;
            *out = recovered; return -1;
        }
#endif
        block_sz  = (size_t)(aligned_new_len + ALFF_BLOCK_OVERHEAD);
        block_buf = calloc(1, block_sz);
        if (!block_buf) { MUTEX_UNLOCK(a); *out = recovered; return -1; }

        write_le64(size_le, aligned_new_len);
        memcpy(block_buf, size_le, 8);

        /* Re-read actual block_size from header for copy_len */
        {
            uint8_t bsz_buf[8];
            uint64_t bsz;
            if (bstack_get(a->bs, slice.offset - ALFF_BLOCK_HDR_SIZE,
                           slice.offset - ALFF_BLOCK_HDR_SIZE + 8,
                           bsz_buf) != 0) { free(block_buf); MUTEX_UNLOCK(a); *out = recovered; return -1; }
            bsz = read_le64(bsz_buf);
            copy_len = slice.len < aligned_new_len ? slice.len : aligned_new_len;
            (void)bsz;
        }

        if (copy_len > 0) {
            if (bstack_get(a->bs, slice.offset, slice.offset + copy_len,
                           block_buf + ALFF_BLOCK_HDR_SIZE) != 0) {
                free(block_buf); MUTEX_UNLOCK(a); *out = recovered; return -1;
            }
        }
        memcpy(block_buf + ALFF_BLOCK_HDR_SIZE + aligned_new_len, size_le, 8);

        if (alff_set_recovery_needed(a->bs) != 0) { free(block_buf); MUTEX_UNLOCK(a); *out = recovered; return -1; }
        if (bstack_push(a->bs, block_buf, block_sz, &push_offset) != 0) {
            free(block_buf); MUTEX_UNLOCK(a); *out = recovered; return -1;
        }
        free(block_buf);
        new_ptr = push_offset + ALFF_BLOCK_HDR_SIZE;

        /* The new block is committed and populated; it is now the survivor,
         * so a failure freeing the old block still returns a valid,
         * fully-resized handle (the old block leaks until crash recovery). */
        recovered.allocator = self;
        recovered.offset    = new_ptr;
        recovered.len       = new_len;

        if (alff_add_to_free_list(a->bs, slice.offset) != 0) { MUTEX_UNLOCK(a); *out = recovered; return -1; }
        if (alff_cascade_discard_free_tail(a->bs) != 0) { MUTEX_UNLOCK(a); *out = recovered; return -1; }
        if (alff_clear_recovery_needed(a->bs) != 0) { MUTEX_UNLOCK(a); *out = recovered; return -1; }

        out->allocator = self;
        out->offset    = new_ptr;
        out->len       = new_len;
        MUTEX_UNLOCK(a);
        return 0;
    }
}

static int
ff_vt_alloc_bulk(bstack_allocator_t *self, const uint64_t *lens, size_t n,
                 bstack_slice_t *out_slices)
{
    size_t i;
    for (i = 0; i < n; i++) {
        if (ff_vt_alloc(self, lens[i], &out_slices[i]) != 0) {
            while (i > 0) {
                i--;
                ff_vt_dealloc(self, out_slices[i]);
            }
            return -1;
        }
    }
    return 0;
}

static int
ff_vt_dealloc_bulk(bstack_allocator_t *self, const bstack_slice_t *slices,
                   size_t n)
{
    size_t i;
    for (i = 0; i < n; i++) {
        if (ff_vt_dealloc(self, slices[i]) != 0)
            return -1;
    }
    return 0;
}

static const bstack_bulk_allocator_vtbl_t ff_bulk_vtbl = {
    { ff_vt_stack, ff_vt_alloc, ff_vt_realloc, ff_vt_dealloc },
    ff_vt_alloc_bulk,
    ff_vt_dealloc_bulk
};

/* =========================================================================
 * first_fit_bstack_allocator_t — public API
 * ====================================================================== */

/* Destroy and free the allocator's in-memory mutex under BSTACK_FEATURE_ATOMIC.
 * No-op when the feature is off (the lock field doesn't exist). */
static void ff_destroy_lock(first_fit_bstack_allocator_t *a)
{
#ifdef BSTACK_FEATURE_ATOMIC
    if (a->lock) {
#  ifdef _WIN32
        DeleteCriticalSection((CRITICAL_SECTION *)a->lock);
#  else
        pthread_mutex_destroy((pthread_mutex_t *)a->lock);
#  endif
        free(a->lock);
        a->lock = NULL;
    }
#else
    (void)a;
#endif
}

first_fit_bstack_allocator_t *first_fit_bstack_allocator_new(bstack_t *bs)
{
    first_fit_bstack_allocator_t *a;
    uint64_t stack_len;
    int      recovery_needed = 0;

    a = malloc(sizeof *a);
    if (!a) { errno = ENOMEM; return NULL; }
    a->base.vtbl      = &ff_bulk_vtbl.base;
    a->base.bulk_vtbl = &ff_bulk_vtbl;
    a->bs             = bs;

#ifdef BSTACK_FEATURE_ATOMIC
    /* Allocate and initialise the in-memory mutex.  Kept opaque in the header
     * (void *) so <pthread.h> / <windows.h> need not be exposed there. */
#  ifdef _WIN32
    {
        CRITICAL_SECTION *cs = malloc(sizeof *cs);
        if (!cs) { free(a); errno = ENOMEM; return NULL; }
        InitializeCriticalSection(cs);
        a->lock = cs;
    }
#  else
    {
        pthread_mutex_t *m = malloc(sizeof *m);
        if (!m) { free(a); errno = ENOMEM; return NULL; }
        if (pthread_mutex_init(m, NULL) != 0) {
            free(m); free(a);
            errno = EINVAL;
            return NULL;
        }
        a->lock = m;
    }
#  endif
#endif

    if (bstack_len(bs, &stack_len) != 0) { ff_destroy_lock(a); free(a); return NULL; }

    if (stack_len == 0) {
        /* Empty stack: write the 48-byte allocator header */
        uint8_t hdr[48];
        memset(hdr, 0, 48);
        memcpy(hdr + 16, alff_magic, 8); /* magic at OFFSET_SIZE offset */
        /* flags, reserved, free_head stay zero */
        if (bstack_push(bs, hdr, 48, NULL) != 0) {
            ff_destroy_lock(a); free(a); return NULL;
        }
        return a;
    }

    /* Non-empty: must have room for the full allocator header */
    if (stack_len < 48) {
        ff_destroy_lock(a); free(a);
        errno = EINVAL;
        return NULL;
    }

    {
        uint8_t  header[32];
        uint64_t free_head;

        /* Read the 32-byte allocator header at payload offset 16 */
        if (bstack_get(bs, 16, 48, header) != 0) {
            ff_destroy_lock(a); free(a); return NULL;
        }

        if (memcmp(header, alff_magic_prefix, 6) != 0) {
            ff_destroy_lock(a); free(a);
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
        if (alff_recovery(bs) != 0) {
            ff_destroy_lock(a); free(a); return NULL;
        }
    }

    return a;
}

void first_fit_bstack_allocator_free(first_fit_bstack_allocator_t *alloc)
{
    if (!alloc) return;
    ff_destroy_lock(alloc);
    free(alloc);
}

bstack_t *first_fit_bstack_allocator_into_stack(first_fit_bstack_allocator_t *alloc)
{
    bstack_t *bs = alloc->bs;
    ff_destroy_lock(alloc);
    free(alloc);
    return bs;
}

/* =========================================================================
 * Mutex helpers shared by ghost_tree, slab, and checked_slab allocators.
 * Mirrors the pattern used by first_fit_bstack_allocator_t.
 * Under BSTACK_FEATURE_ATOMIC these allocate/free the opaque lock field;
 * without it they compile away to nothing.
 * ====================================================================== */

#ifdef BSTACK_FEATURE_ATOMIC
static int bstack_alloc_lock_init(void **out)
{
#  ifdef _WIN32
    CRITICAL_SECTION *cs = malloc(sizeof *cs);
    if (!cs) { errno = ENOMEM; return -1; }
    InitializeCriticalSection(cs);
    *out = cs;
    return 0;
#  else
    pthread_mutex_t *m = malloc(sizeof *m);
    if (!m) { errno = ENOMEM; return -1; }
    if (pthread_mutex_init(m, NULL) != 0) { free(m); errno = EINVAL; return -1; }
    *out = m;
    return 0;
#  endif
}

static void bstack_alloc_lock_destroy(void *lock)
{
    if (!lock) return;
#  ifdef _WIN32
    DeleteCriticalSection((CRITICAL_SECTION *)lock);
#  else
    pthread_mutex_destroy((pthread_mutex_t *)lock);
#  endif
    free(lock);
}
#endif /* BSTACK_FEATURE_ATOMIC */

/* =========================================================================
 * Crash-atomic tail shrink, shared by the ghost_tree and segregated reallocs.
 * ====================================================================== */

#ifdef BSTACK_FEATURE_ATOMIC
/*
 * BSTACK_GEN_LEN confirms the block is still the tail under
 * bstack_process_gen's single held write lock (so no concurrent push/discard
 * can move it between the check and the cut), then a NULL-`removed`
 * BSTACK_GEN_SPLICE (the atrunc form) cuts `cut` bytes off the tail and
 * re-appends `new_len` bytes from `new_buf` in their place — fusing the
 * truncation with whatever must land alongside it (sub-block padding zeros for
 * ghost_tree, the rewritten block for segregated) into one crash-atomic
 * transaction.  If the block is no longer the tail the sequence ends without
 * mutating and `truncated` stays 0, so the caller falls through to its non-tail
 * path.
 */
typedef struct {
    int            phase;
    int            truncated;     /* set once the splice is issued */
    uint64_t       expected_tail; /* stack length the block must still end at */
    uint64_t       cur;           /* BSTACK_GEN_LEN output */
    size_t         cut;           /* bytes to cut off the tail */
    const uint8_t *new_buf;       /* bytes to re-append in their place */
    size_t         new_len;
} bstack_alloc_tail_shrink_ctx_t;

static int bstack_alloc_tail_shrink_gen(bstack_gen_op_t *out_op, void *ctx_)
{
    bstack_alloc_tail_shrink_ctx_t *c = (bstack_alloc_tail_shrink_ctx_t *)ctx_;
    if (c->phase == 0) {
        c->phase = 1;
        out_op->kind = BSTACK_GEN_LEN;
        out_op->u.len.out = &c->cur;
        return 1;
    }
    if (c->phase == 1) {
        c->phase = 2;
        if (c->expected_tail != c->cur) return 0;  /* not the tail — change nothing */
        c->truncated = 1;
        out_op->kind = BSTACK_GEN_SPLICE;
        out_op->u.splice.removed = NULL;   /* atrunc form: discard the cut bytes */
        out_op->u.splice.n = c->cut;
        out_op->u.splice.new_buf = c->new_buf;
        out_op->u.splice.new_len = c->new_len;
        return 1;
    }
    return 0;
}
#endif /* BSTACK_FEATURE_ATOMIC */

/* =========================================================================
 * ghost_tree_bstack_allocator_t — best-fit AVL tree allocator
 * Requires -DBSTACK_FEATURE_SET (depends on bstack_set and bstack_zero).
 * ====================================================================== */

/* ---- constants --------------------------------------------------------- */

#define ALGT_MAGIC_OFFSET    UINT64_C(32)
#define ALGT_ROOT_OFFSET     UINT64_C(40)
#define ALGT_ARENA_START     UINT64_C(48)
#define ALGT_MIN_ALLOC       UINT64_C(32)
#define ALGT_NULL_PTR        UINT64_C(0)
/* Maximum recursion depth for AVL operations.  A balanced AVL tree never
 * exceeds ~60 levels; 128 gives headroom for post-crash imbalance while
 * reliably detecting cycles created by a partial rotation crash. */
#define ALGT_MAX_AVL_DEPTH   128u

static const uint8_t algt_magic[8]        = {'A','L','G','T',0,1,3,0};
static const uint8_t algt_magic_prefix[6] = {'A','L','G','T',0,1};

typedef struct {
    uint64_t ptr;
    uint64_t size;
    uint64_t left;
    uint64_t right;
    int      went_left;
    uint8_t  lh;         /* this node's cached child heights, read on the way   */
    uint8_t  rh;         /* down; the sibling's is stable for the up-pass write */
} algt_path_entry_t;

/* ---- alignment helpers ------------------------------------------------- */

/* Round len up to a multiple of 32, minimum 32. */
static inline uint64_t algt_align_up_len(uint64_t len)
{
    uint64_t a = (len + UINT64_C(31)) & ~UINT64_C(31);
    return a < ALGT_MIN_ALLOC ? ALGT_MIN_ALLOC : a;
}

/* Round ptr up to the next valid arena address (ptr ≡ 16 mod 32, ≥ 48).
 * Used to validate that a slice origin is on a real block boundary. */
static inline uint64_t algt_align_up_ptr(uint64_t ptr)
{
    return ((ptr + UINT64_C(15)) & ~UINT64_C(31)) + UINT64_C(16);
}

/* ---- root I/O ---------------------------------------------------------- */

static int algt_read_root(bstack_t *bs, uint64_t *out)
{
    uint8_t buf[8];
    if (bstack_get(bs, ALGT_ROOT_OFFSET, ALGT_ROOT_OFFSET + 8, buf) != 0)
        return -1;
    *out = read_le64(buf);
    return 0;
}

static int algt_write_root(bstack_t *bs, uint64_t root)
{
    uint8_t buf[8];
    write_le64(buf, root);
    return bstack_set(bs, ALGT_ROOT_OFFSET, buf, 8);
}

/* ---- node I/O ---------------------------------------------------------- */

/* AVL node layout within a free block (32 bytes at offset 0):
 *   [0..8)   size (u64 LE)
 *   [8]      balance_factor (i8)
 *   [9]      height (u8)
 *   [10]     cached height of left child (u8)   ─┐ denormalized so a parent read
 *   [11]     cached height of right child (u8)  ─┘ on the way down yields its
 *   [12..16) reserved / zero                       untouched sibling's height
 *   [16..24) left child ptr (u64 LE)
 *   [24..32) right child ptr (u64 LE)
 *
 * The cache is rebuilt from scratch by algt_coalesce_and_rebalance on open, so
 * old-format arenas (whose [10..16) were zero) self-upgrade transparently. */

static int algt_read_node(bstack_t *bs, uint64_t ptr,
    uint64_t *out_size, int8_t *out_bf, uint8_t *out_height,
    uint64_t *out_left, uint64_t *out_right)
{
    uint8_t buf[32];
    if (bstack_get(bs, ptr, ptr + 32, buf) != 0) return -1;
    *out_size   = read_le64(buf);
    *out_bf     = (int8_t)buf[8];
    *out_height = buf[9];
    *out_left   = read_le64(buf + 16);
    *out_right  = read_le64(buf + 24);
    return 0;
}

/* Read the node at ptr for a down-pass: size, left, right, and the node's cached
 * child heights (*out_lh, *out_rh).  Same single bstack_get as algt_read_node;
 * the cache lets the up-pass skip re-reading the untouched sibling child. */
static int algt_read_node_hc(bstack_t *bs, uint64_t ptr,
    uint64_t *out_size, uint64_t *out_left, uint64_t *out_right,
    uint8_t *out_lh, uint8_t *out_rh)
{
    uint8_t buf[32];
    if (bstack_get(bs, ptr, ptr + 32, buf) != 0) return -1;
    *out_size  = read_le64(buf);
    *out_left  = read_le64(buf + 16);
    *out_right = read_le64(buf + 24);
    *out_lh    = buf[10];
    *out_rh    = buf[11];
    return 0;
}

/* Write (size, left, right) to ptr, computing bf and height in one pass.
 *
 * A child height passed as >= 0 (known_lh / known_rh) is used directly — the
 * caller already knows it, e.g. from the node written in the previous up-pass
 * step or from a sibling untouched by a rotation — which avoids a bstack_get
 * (lock + syscall) to re-read that child.  A negative value reads the height
 * from the child.  Sets *out_bf and *out_height when non-NULL.  Returns 0/-1. */
static int algt_avl_write_h(bstack_t *bs, uint64_t ptr, uint64_t size,
    uint64_t left, uint64_t right, int known_lh, int known_rh,
    int8_t *out_bf, uint8_t *out_height)
{
    uint8_t lh = 0, rh = 0;
    if (known_lh >= 0) {
        lh = (uint8_t)known_lh;
    } else if (left != ALGT_NULL_PTR) {
        uint8_t buf[32];
        if (bstack_get(bs, left, left + 32, buf) != 0) return -1;
        lh = buf[9];
    }
    if (known_rh >= 0) {
        rh = (uint8_t)known_rh;
    } else if (right != ALGT_NULL_PTR) {
        uint8_t buf[32];
        if (bstack_get(bs, right, right + 32, buf) != 0) return -1;
        rh = buf[9];
    }
    {
        int16_t lh16 = (int16_t)lh, rh16 = (int16_t)rh;
        int8_t  bf     = (int8_t)(rh16 - lh16);
        uint8_t height = (uint8_t)(1 + (lh16 >= rh16 ? lh16 : rh16));
        uint8_t buf[32];
        memset(buf, 0, 32);
        write_le64(buf,      size);
        buf[8]  = (uint8_t)bf;
        buf[9]  = height;
        buf[10] = lh; /* cached left-child height  */
        buf[11] = rh; /* cached right-child height */
        write_le64(buf + 16, left);
        write_le64(buf + 24, right);
        if (bstack_set(bs, ptr, buf, 32) != 0) return -1;
        if (out_bf)     *out_bf     = bf;
        if (out_height) *out_height = height;
    }
    return 0;
}

/* Write (size, left, right) to ptr, reading both child heights.  Thin wrapper
 * over algt_avl_write_h.  Sets *out_bf if non-NULL.  Returns 0/-1. */
static int algt_avl_write_and_update(bstack_t *bs, uint64_t ptr,
    uint64_t size, uint64_t left, uint64_t right, int8_t *out_bf)
{
    return algt_avl_write_h(bs, ptr, size, left, right, -1, -1, out_bf, NULL);
}

/* ---- AVL helpers ------------------------------------------------------- */

/* Right-rotate around node; return the new subtree root and (if out_height is
 * non-NULL) its height. */
static int algt_avl_rotate_right(bstack_t *bs, uint64_t node,
    uint64_t *out_root, uint8_t *out_height)
{
    uint64_t node_sz, node_r, pivot, pivot_sz, pivot_l, pivot_r;
    uint8_t  node_lh, node_rh, pivot_lh, pivot_rh, node_h;
    /* Read both nodes' cached child heights so neither rewrite re-reads a child.
     * (node's left child is pivot, whose height node_lh is not needed.) */
    if (algt_read_node_hc(bs, node,  &node_sz,  &pivot,   &node_r,  &node_lh, &node_rh) != 0) return -1;
    if (algt_read_node_hc(bs, pivot, &pivot_sz, &pivot_l, &pivot_r, &pivot_lh, &pivot_rh) != 0) return -1;
    (void)node_lh;
    /* node's new children (pivot_r, node_r) → heights (pivot_rh, node_rh). */
    if (algt_avl_write_h(bs, node, node_sz, pivot_r, node_r,
                         (int)pivot_rh, (int)node_rh, NULL, &node_h) != 0) return -1;
    /* pivot's new children (pivot_l, node) → heights (pivot_lh, node_h). */
    if (algt_avl_write_h(bs, pivot, pivot_sz, pivot_l, node,
                         (int)pivot_lh, (int)node_h, NULL, out_height) != 0) return -1;
    *out_root = pivot;
    return 0;
}

/* Left-rotate around node; return the new subtree root and (if out_height is
 * non-NULL) its height. */
static int algt_avl_rotate_left(bstack_t *bs, uint64_t node,
    uint64_t *out_root, uint8_t *out_height)
{
    uint64_t node_sz, node_l, pivot, pivot_sz, pivot_l, pivot_r;
    uint8_t  node_lh, node_rh, pivot_lh, pivot_rh, node_h;
    /* Read both nodes' cached child heights so neither rewrite re-reads a child.
     * (node's right child is pivot, whose height node_rh is not needed.) */
    if (algt_read_node_hc(bs, node,  &node_sz,  &node_l,  &pivot,   &node_lh, &node_rh) != 0) return -1;
    if (algt_read_node_hc(bs, pivot, &pivot_sz, &pivot_l, &pivot_r, &pivot_lh, &pivot_rh) != 0) return -1;
    (void)node_rh;
    /* node's new children (node_l, pivot_l) → heights (node_lh, pivot_lh). */
    if (algt_avl_write_h(bs, node, node_sz, node_l, pivot_l,
                         (int)node_lh, (int)pivot_lh, NULL, &node_h) != 0) return -1;
    /* pivot's new children (node, pivot_r) → heights (node_h, pivot_rh). */
    if (algt_avl_write_h(bs, pivot, pivot_sz, node, pivot_r,
                         (int)node_h, (int)pivot_rh, NULL, out_height) != 0) return -1;
    *out_root = pivot;
    return 0;
}

/* Fix imbalance at node (uses < -1 / > 1 to handle post-crash excess).
 *
 * The caller passes the bf and height already computed by the algt_avl_write_h
 * that installed node's current children, so the common in-balance case needs
 * no further I/O.  Returns the (possibly new) subtree root and its height. */
static int algt_avl_rebalance(bstack_t *bs, uint64_t node, int8_t bf, uint8_t height,
    uint64_t *out_root, uint8_t *out_height)
{
    if (bf < -1) {
        uint64_t size, left, right, left_sz, left_l, left_r;
        int8_t nbf, left_bf; uint8_t nh, left_h;
        if (algt_read_node(bs, node, &size, &nbf, &nh, &left, &right) != 0) return -1;
        if (algt_read_node(bs, left, &left_sz, &left_bf, &left_h, &left_l, &left_r) != 0)
            return -1;
        if (left_bf > 0) {
            /* Left-right: rotate left child left first */
            uint64_t new_left;
            if (algt_avl_rotate_left(bs, left, &new_left, NULL) != 0) return -1;
            if (algt_avl_write_and_update(bs, node, size, new_left, right, NULL) != 0)
                return -1;
        }
        return algt_avl_rotate_right(bs, node, out_root, out_height);
    }
    if (bf > 1) {
        uint64_t size, left, right, right_sz, right_l, right_r;
        int8_t nbf, right_bf; uint8_t nh, right_h;
        if (algt_read_node(bs, node, &size, &nbf, &nh, &left, &right) != 0) return -1;
        if (algt_read_node(bs, right, &right_sz, &right_bf, &right_h, &right_l, &right_r) != 0)
            return -1;
        if (right_bf < 0) {
            /* Right-left: rotate right child right first */
            uint64_t new_right;
            if (algt_avl_rotate_right(bs, right, &new_right, NULL) != 0) return -1;
            if (algt_avl_write_and_update(bs, node, size, left, new_right, NULL) != 0)
                return -1;
        }
        return algt_avl_rotate_left(bs, node, out_root, out_height);
    }
    *out_root   = node;
    *out_height = height;
    return 0;
}

/* ---- AVL insert -------------------------------------------------------- */

static int algt_avl_insert(bstack_t *bs, uint64_t ptr, uint64_t size)
{
    algt_path_entry_t path[ALGT_MAX_AVL_DEPTH];
    size_t   path_len = 0;
    uint64_t root, current, child;
    int      i;

    if (algt_read_root(bs, &root) != 0) return -1;

    current = root;
    while (current != ALGT_NULL_PTR) {
        uint64_t root_sz, left, right;
        uint8_t lh, rh;
        int went_left;
        if (path_len >= ALGT_MAX_AVL_DEPTH) { errno = EINVAL; return -1; }
        if (algt_read_node_hc(bs, current, &root_sz, &left, &right, &lh, &rh) != 0)
            return -1;
        went_left = (size < root_sz || (size == root_sz && ptr < current));
        path[path_len].ptr       = current;
        path[path_len].size      = root_sz;
        path[path_len].left      = left;
        path[path_len].right     = right;
        path[path_len].went_left = went_left;
        path[path_len].lh        = lh;
        path[path_len].rh        = rh;
        path_len++;
        current = went_left ? left : right;
    }

    {
        uint8_t buf[32];
        memset(buf, 0, 32); /* null children → cached child heights [10],[11] = 0 */
        write_le64(buf, size);
        buf[9] = 1;
        if (bstack_set(bs, ptr, buf, 32) != 0) return -1;
    }

    /* Up-pass: install the new child pointer in each ancestor and rebalance.
     * Both child heights are known — the modified child from the previous
     * iteration, the untouched sibling from this node's cache read on the way
     * down — so algt_avl_write_h reads neither.  The (bf, height) it returns is
     * handed to algt_avl_rebalance, so the in-balance case does no further I/O:
     * one write per level, zero reads. */
    child = ptr;
    {
        uint8_t child_h = 1; /* leaf height */
        for (i = (int)path_len - 1; i >= 0; i--) {
            uint64_t new_left, new_right, new_child;
            int      known_lh, known_rh;
            int8_t   bf; uint8_t h, new_h;
            if (path[i].went_left) {
                new_left  = child;          new_right = path[i].right;
                known_lh  = (int)child_h;   known_rh  = (int)path[i].rh;
            } else {
                new_left  = path[i].left;   new_right = child;
                known_lh  = (int)path[i].lh; known_rh  = (int)child_h;
            }
            if (algt_avl_write_h(bs, path[i].ptr, path[i].size,
                                 new_left, new_right, known_lh, known_rh, &bf, &h) != 0)
                return -1;
            if (algt_avl_rebalance(bs, path[i].ptr, bf, h, &new_child, &new_h) != 0) return -1;
            child   = new_child;
            child_h = new_h;
        }
    }
    return algt_write_root(bs, child);
}

/* ---- AVL remove-min ---------------------------------------------------- */

/* Remove the minimum (leftmost) node from subtree rooted at root.
 * Sets *out_min_ptr and *out_min_size to the removed node; *out_new_root is the
 * rebalanced subtree after removal. */
static int algt_avl_remove_min(bstack_t *bs, uint64_t root,
    uint64_t *out_min_ptr, uint64_t *out_min_size, uint64_t *out_new_root)
{
    uint64_t stk_ptr    [ALGT_MAX_AVL_DEPTH];
    uint64_t stk_size   [ALGT_MAX_AVL_DEPTH];
    uint64_t stk_right  [ALGT_MAX_AVL_DEPTH];
    uint8_t  stk_right_h[ALGT_MAX_AVL_DEPTH]; /* cached right-child heights */
    size_t   stk_len  = 0;
    uint64_t current  = root;

    for (;;) {
        uint64_t size, left, right;
        uint8_t lh, rh;
        if (algt_read_node_hc(bs, current, &size, &left, &right, &lh, &rh) != 0)
            return -1;
        (void)lh;
        if (left == ALGT_NULL_PTR) {
            /* Replace current with its right child, whose height is current's
             * cached right height.  `child` is always the left side going up;
             * both child heights are known each step, so write_h reads neither. */
            uint64_t child = right;
            uint8_t  child_h = rh;
            int i;
            for (i = (int)stk_len - 1; i >= 0; i--) {
                uint64_t new_child;
                int8_t   bf; uint8_t h, new_h;
                if (algt_avl_write_h(bs, stk_ptr[i], stk_size[i],
                                     child, stk_right[i], (int)child_h, (int)stk_right_h[i],
                                     &bf, &h) != 0)
                    return -1;
                if (algt_avl_rebalance(bs, stk_ptr[i], bf, h, &new_child, &new_h) != 0) return -1;
                child   = new_child;
                child_h = new_h;
            }
            *out_min_ptr  = current;
            *out_min_size = size;
            *out_new_root = child;
            return 0;
        }
        if (stk_len >= ALGT_MAX_AVL_DEPTH) { errno = EINVAL; return -1; }
        stk_ptr    [stk_len] = current;
        stk_size   [stk_len] = size;
        stk_right  [stk_len] = right;
        stk_right_h[stk_len] = rh;
        stk_len++;
        current = left;
    }
}

/* ---- AVL best-fit search + remove -------------------------------------- */

/* Find and remove the best-fit (smallest block >= min_size) in one O(log n)
 * pass.  Sets *out_found_ptr = ALGT_NULL_PTR when no block fits. */
static int algt_avl_find_best_fit_and_remove(bstack_t *bs, uint64_t min_size,
    uint64_t *out_found_ptr, uint64_t *out_found_size)
{
    algt_path_entry_t path[ALGT_MAX_AVL_DEPTH];
    size_t   path_len    = 0;
    int      fit_found   = 0;
    size_t   last_fit_idx = 0;
    uint64_t root, current;
    uint64_t found_ptr, found_size, found_left, found_right;
    uint8_t  found_lh, found_rh;
    uint64_t child;
    int      i, child_h;

    if (algt_read_root(bs, &root) != 0) return -1;
    if (root == ALGT_NULL_PTR) {
        *out_found_ptr  = ALGT_NULL_PTR;
        *out_found_size = 0;
        return 0;
    }

    current = root;
    while (current != ALGT_NULL_PTR) {
        uint64_t root_sz, left, right;
        uint8_t lh, rh;
        int went_left;
        if (path_len >= ALGT_MAX_AVL_DEPTH) { errno = EINVAL; return -1; }
        if (algt_read_node_hc(bs, current, &root_sz, &left, &right, &lh, &rh) != 0)
            return -1;
        if (root_sz >= min_size) {
            last_fit_idx = path_len;
            fit_found    = 1;
            went_left    = 1;
        } else {
            went_left = 0;
        }
        path[path_len].ptr       = current;
        path[path_len].size      = root_sz;
        path[path_len].left      = left;
        path[path_len].right     = right;
        path[path_len].went_left = went_left;
        path[path_len].lh        = lh;
        path[path_len].rh        = rh;
        path_len++;
        current = went_left ? left : right;
    }

    if (!fit_found) {
        *out_found_ptr  = ALGT_NULL_PTR;
        *out_found_size = 0;
        return 0;
    }

    found_ptr   = path[last_fit_idx].ptr;
    found_size  = path[last_fit_idx].size;
    found_left  = path[last_fit_idx].left;
    found_right = path[last_fit_idx].right;
    found_lh    = path[last_fit_idx].lh; /* cached found_left  height */
    found_rh    = path[last_fit_idx].rh; /* cached found_right height */

    /* Seed the up-pass directly with the best-fit node's replacement.  child_h
     * is the replacement subtree's height — known in every case now (single
     * child from the cache, successor from its rebalance). */
    if (found_left == ALGT_NULL_PTR) {
        child   = found_right;
        child_h = (int)found_rh;
    } else if (found_right == ALGT_NULL_PTR) {
        child   = found_left;
        child_h = (int)found_lh;
    } else {
        uint64_t succ, succ_sz, new_right;
        int8_t   bf; uint8_t h, rh;
        if (algt_avl_remove_min(bs, found_right, &succ, &succ_sz, &new_right) != 0)
            return -1;
        if (algt_avl_write_h(bs, succ, succ_sz, found_left, new_right, -1, -1, &bf, &h) != 0)
            return -1;
        if (algt_avl_rebalance(bs, succ, bf, h, &child, &rh) != 0) return -1;
        child_h = (int)rh;
    }

    /* Up-pass: both child heights are known each step — the modified child
     * threaded from below, the untouched sibling from this node's cache — so
     * algt_avl_write_h reads neither and the in-balance case does no further I/O. */
    for (i = (int)last_fit_idx - 1; i >= 0; i--) {
        uint64_t new_left, new_right, new_child;
        int      known_lh, known_rh;
        int8_t   bf; uint8_t h, new_h;
        if (path[i].went_left) {
            new_left  = child;          new_right = path[i].right;
            known_lh  = child_h;        known_rh  = (int)path[i].rh;
        } else {
            new_left  = path[i].left;   new_right = child;
            known_lh  = (int)path[i].lh; known_rh  = child_h;
        }
        if (algt_avl_write_h(bs, path[i].ptr, path[i].size,
                             new_left, new_right, known_lh, known_rh, &bf, &h) != 0) return -1;
        if (algt_avl_rebalance(bs, path[i].ptr, bf, h, &new_child, &new_h) != 0) return -1;
        child   = new_child;
        child_h = (int)new_h;
    }
    if (algt_write_root(bs, child) != 0) return -1;

    *out_found_ptr  = found_ptr;
    *out_found_size = found_size;
    return 0;
}

/* ---- coalesce and rebalance -------------------------------------------- */

typedef struct { uint64_t ptr; uint64_t size; } algt_block_t;

static int algt_cmp_by_ptr(const void *a, const void *b)
{
    const algt_block_t *x = (const algt_block_t *)a;
    const algt_block_t *y = (const algt_block_t *)b;
    return (x->ptr > y->ptr) - (x->ptr < y->ptr);
}

static int algt_cmp_by_size_ptr(const void *a, const void *b)
{
    const algt_block_t *x = (const algt_block_t *)a;
    const algt_block_t *y = (const algt_block_t *)b;
    if (x->size != y->size)
        return (x->size > y->size) - (x->size < y->size);
    return (x->ptr > y->ptr) - (x->ptr < y->ptr);
}

struct algt_walk_ctx {
    algt_block_t *blocks;
    size_t        count;
    size_t        cap;
    int           err;
};

static void algt_avl_walk_inorder(bstack_t *bs, uint64_t root,
    struct algt_walk_ctx *ctx)
{
    uint64_t stk_ptr  [ALGT_MAX_AVL_DEPTH];
    uint64_t stk_right[ALGT_MAX_AVL_DEPTH];
    uint64_t stk_size [ALGT_MAX_AVL_DEPTH];
    size_t   stk_len  = 0;
    uint64_t current  = root;

    if (ctx->err) return;

    for (;;) {
        while (current != ALGT_NULL_PTR) {
            uint64_t size, left, right;
            int8_t bf; uint8_t height;
            if (stk_len >= ALGT_MAX_AVL_DEPTH) { ctx->err = 1; return; }
            if (algt_read_node(bs, current, &size, &bf, &height, &left, &right) != 0) {
                ctx->err = 1; return;
            }
            stk_ptr  [stk_len] = current;
            stk_right[stk_len] = right;
            stk_size [stk_len] = size;
            stk_len++;
            current = left;
        }
        if (stk_len == 0) return;
        stk_len--;
        {
            uint64_t ptr   = stk_ptr  [stk_len];
            uint64_t right = stk_right[stk_len];
            uint64_t size  = stk_size [stk_len];
            if (ctx->count == ctx->cap) {
                size_t       nc = ctx->cap ? ctx->cap * 2 : 16;
                algt_block_t *t = realloc(ctx->blocks, nc * sizeof *t);
                if (!t) { ctx->err = 1; return; }
                ctx->blocks = t;
                ctx->cap    = nc;
            }
            ctx->blocks[ctx->count].ptr  = ptr;
            ctx->blocks[ctx->count].size = size;
            ctx->count++;
            current = right;
        }
    }
}

/* Build an optimally balanced BST from a sorted (by key) block array.
 * Uses an explicit ops-stack to avoid recursion. */
typedef struct { int is_combine; size_t lo; size_t hi; } algt_build_op_t;

static int algt_build_tree(bstack_t *bs, const algt_block_t *blocks,
    size_t n, uint64_t *out_root)
{
    algt_build_op_t *ops     = NULL;
    uint64_t        *results = NULL;
    size_t           ops_len = 0, ops_cap = 0;
    size_t           res_len = 0, res_cap = 0;
    int              ret     = 0;

#define BUILD_OPS_PUSH(comb, lo_, hi_) do { \
    if (ops_len == ops_cap) { \
        size_t nc_ = ops_cap ? ops_cap * 2 : 32; \
        algt_build_op_t *t_ = realloc(ops, nc_ * sizeof *t_); \
        if (!t_) { ret = -1; goto build_done; } \
        ops = t_; ops_cap = nc_; \
    } \
    ops[ops_len].is_combine = (comb); \
    ops[ops_len].lo = (lo_); \
    ops[ops_len].hi = (hi_); \
    ops_len++; \
} while (0)

#define BUILD_RES_PUSH(v) do { \
    if (res_len == res_cap) { \
        size_t nc_ = res_cap ? res_cap * 2 : 32; \
        uint64_t *t_ = realloc(results, nc_ * sizeof *t_); \
        if (!t_) { ret = -1; goto build_done; } \
        results = t_; res_cap = nc_; \
    } \
    results[res_len++] = (v); \
} while (0)

    BUILD_OPS_PUSH(0, 0, n);

    while (ops_len > 0 && ret == 0) {
        algt_build_op_t op = ops[--ops_len];
        if (!op.is_combine) {
            size_t lo = op.lo, hi = op.hi;
            if (lo >= hi) {
                BUILD_RES_PUSH(ALGT_NULL_PTR);
            } else {
                size_t mid = lo + (hi - lo) / 2;
                BUILD_OPS_PUSH(1,  mid, 0);
                BUILD_OPS_PUSH(0,  mid + 1, hi);
                BUILD_OPS_PUSH(0,  lo, mid);
            }
        } else {
            size_t   mid        = op.lo;
            uint64_t right_root = results[--res_len];
            uint64_t left_root  = results[--res_len];
            if (algt_avl_write_and_update(bs,
                    blocks[mid].ptr, blocks[mid].size,
                    left_root, right_root, NULL) != 0) {
                ret = -1; goto build_done;
            }
            BUILD_RES_PUSH(blocks[mid].ptr);
        }
    }

    *out_root = (res_len > 0) ? results[res_len - 1] : ALGT_NULL_PTR;

build_done:
#undef BUILD_OPS_PUSH
#undef BUILD_RES_PUSH
    free(ops);
    free(results);
    return ret;
}

/* Collect all free blocks, merge adjacent ones, and rebuild a balanced AVL
 * tree.  Called by ghost_tree_bstack_allocator_new on every open. */
static int algt_coalesce_and_rebalance(bstack_t *bs)
{
    struct algt_walk_ctx ctx;
    algt_block_t *coalesced;
    uint64_t     *seams;
    size_t        coalesced_cnt, seam_cnt, i;
    uint64_t      root, new_root;
    int           ret = 0;

    if (algt_read_root(bs, &root) != 0) return -1;

    ctx.blocks = NULL; ctx.count = 0; ctx.cap = 0; ctx.err = 0;
    algt_avl_walk_inorder(bs, root, &ctx);
    if (ctx.err) { free(ctx.blocks); return -1; }
    if (ctx.count == 0) { free(ctx.blocks); return 0; }

    /* Sort by address, then deduplicate by ptr.  A partial rotation crash can
     * leave a node reachable from two parents; the in-order walk would visit
     * it twice and the rebuild would clobber child pointers from the first
     * visit with those of the second. */
    qsort(ctx.blocks, ctx.count, sizeof *ctx.blocks, algt_cmp_by_ptr);
    {
        size_t j = 0, k;
        for (k = 0; k < ctx.count; k++) {
            if (j == 0 || ctx.blocks[k].ptr != ctx.blocks[j - 1].ptr)
                ctx.blocks[j++] = ctx.blocks[k];
        }
        ctx.count = j;
    }

    coalesced = malloc(ctx.count * sizeof *coalesced);
    seams     = malloc(ctx.count * sizeof *seams);
    if (!coalesced || !seams) {
        free(coalesced); free(seams); free(ctx.blocks); return -1;
    }
    coalesced_cnt = 0;
    seam_cnt      = 0;

    for (i = 0; i < ctx.count; i++) {
        uint64_t ptr = ctx.blocks[i].ptr;
        uint64_t sz  = ctx.blocks[i].size;
        if (coalesced_cnt > 0 &&
            coalesced[coalesced_cnt - 1].ptr + coalesced[coalesced_cnt - 1].size == ptr) {
            seams[seam_cnt++] = ptr;
            coalesced[coalesced_cnt - 1].size += sz;
        } else {
            coalesced[coalesced_cnt].ptr  = ptr;
            coalesced[coalesced_cnt].size = sz;
            coalesced_cnt++;
        }
    }
    free(ctx.blocks);

    /* Zero the absorbed AVL node headers inside merged blocks */
    for (i = 0; i < seam_cnt && ret == 0; i++) {
        if (bstack_zero(bs, seams[i], (size_t)ALGT_MIN_ALLOC) != 0)
            ret = -1;
    }
    free(seams);
    if (ret != 0) { free(coalesced); return -1; }

    /* Sort by (size, ptr) — the AVL tree's key order — then build */
    qsort(coalesced, coalesced_cnt, sizeof *coalesced, algt_cmp_by_size_ptr);
    ret = algt_build_tree(bs, coalesced, coalesced_cnt, &new_root);
    free(coalesced);
    if (ret != 0) return -1;

    return algt_write_root(bs, new_root);
}

/* =========================================================================
 * ghost_tree_bstack_allocator_t — vtable implementations
 * ====================================================================== */

static bstack_t *gt_vt_stack(bstack_allocator_t *self)
{
    return ((ghost_tree_bstack_allocator_t *)self)->bs;
}

static int gt_vt_alloc(bstack_allocator_t *self, uint64_t len,
    bstack_slice_t *out)
{
    ghost_tree_bstack_allocator_t *a = (ghost_tree_bstack_allocator_t *)self;
    uint64_t aligned, found_ptr, found_size;

    if (len == 0) {
        out->allocator = self; out->offset = 0; out->len = 0;
        return 0;
    }

    aligned = algt_align_up_len(len);

    /* Lock covers AVL search and conditional insert (split case).
     * Released before bstack_zero (no-split) and before bstack_extend. */
    MUTEX_LOCK(a);
    if (algt_avl_find_best_fit_and_remove(a->bs, aligned,
                                           &found_ptr, &found_size) != 0) {
        MUTEX_UNLOCK(a);
        return -1;
    }

    if (found_ptr != ALGT_NULL_PTR) {
        uint64_t remainder = found_size - aligned;
        if (remainder >= ALGT_MIN_ALLOC) {
            /* Split: leading remainder becomes a new free block;
             * allocated block is at the back (tail of the found region). */
            if (algt_avl_insert(a->bs, found_ptr, remainder) != 0) {
                MUTEX_UNLOCK(a);
                return -1;
            }
            MUTEX_UNLOCK(a);
            out->allocator = self;
            out->offset    = found_ptr + remainder;
            out->len       = len;
            return 0;
        }
        /* No split: unlock before zeroing the stale 32-byte AVL node header. */
        MUTEX_UNLOCK(a);
        if (bstack_zero(a->bs, found_ptr, (size_t)ALGT_MIN_ALLOC) != 0)
            return -1;
        out->allocator = self;
        out->offset    = found_ptr;
        out->len       = len;
        return 0;
    }

    /* No free block fits: unlock before extending the stack. */
    MUTEX_UNLOCK(a);
#if UINT64_MAX > SIZE_MAX
    if (aligned > (uint64_t)SIZE_MAX) { errno = EINVAL; return -1; }
#endif
    {
        uint64_t block_start;
        if (bstack_extend(a->bs, (size_t)aligned, &block_start) != 0) return -1;
        out->allocator = self;
        out->offset    = block_start;
        out->len       = len;
    }
    return 0;
}

static int gt_vt_dealloc(bstack_allocator_t *self, bstack_slice_t slice)
{
    ghost_tree_bstack_allocator_t *a = (ghost_tree_bstack_allocator_t *)self;
    uint64_t true_len;

    if (slice.len == 0) return 0;

    if (slice.offset < ALGT_ARENA_START ||
        slice.offset != algt_align_up_ptr(slice.offset)) {
        errno = EINVAL;
        return -1;
    }

    true_len = algt_align_up_len(slice.len);
#if UINT64_MAX > SIZE_MAX
    if (true_len > (uint64_t)SIZE_MAX) { errno = EINVAL; return -1; }
#endif

#ifdef BSTACK_FEATURE_ATOMIC
    /* Atomic tail path: check-and-discard under BStack's own write lock. */
    {
        int ok = 0;
        if (bstack_try_discard(a->bs, slice.offset + true_len,
                               (size_t)true_len, &ok) != 0) return -1;
        if (ok) return 0;
    }
    /* Not at tail: zero block (caller owns it; no lock), then insert under lock.
     * The block is owned by the caller and untouched by other threads until it
     * appears in the AVL tree, so a failed zero leaves it fully intact (-1). */
    if (bstack_zero(a->bs, slice.offset, (size_t)true_len) != 0) return -1;
    {
        int r;
        MUTEX_LOCK(a);
        /* Past this point a torn (non-atomic) AVL insert cannot be safely
         * retried — GhostTree has no is_free flag to repair it in-process, so
         * a failure here can no longer hand the original back (-2). */
        r = algt_avl_insert(a->bs, slice.offset, true_len);
        MUTEX_UNLOCK(a);
        return r == 0 ? 0 : -2;
    }
#else
    {
        uint64_t stack_len;
        if (bstack_len(a->bs, &stack_len) != 0) return -1;
        if (slice.offset + true_len == stack_len) {
            /* Tail block: truncate instead of recycling through the tree.
             * A single atomic bstack call — on failure nothing changed. */
            return bstack_discard(a->bs, (size_t)true_len) == 0 ? 0 : -1;
        }
        /* Non-tail: zero entire block (upholds zeroed-memory invariant), insert.
         * The zero leaves the block intact on failure (-1); past that point a
         * torn AVL insert cannot be safely retried (-2), same reasoning as the
         * atomic path above. */
        if (bstack_zero(a->bs, slice.offset, (size_t)true_len) != 0) return -1;
        return algt_avl_insert(a->bs, slice.offset, true_len) == 0 ? 0 : -2;
    }
#endif
}

static int gt_vt_realloc(bstack_allocator_t *self, bstack_slice_t slice,
    uint64_t new_len, bstack_slice_t *out)
{
    ghost_tree_bstack_allocator_t *a = (ghost_tree_bstack_allocator_t *)self;
    uint64_t old_len, aligned_old, aligned_new;
    /*
     * The surviving allocation to hand back on failure. Starts as the
     * original block; on the grow-and-move path it becomes the new region
     * once that region is committed and populated (copy done, before the old
     * block is freed), so a later failure freeing the old block still
     * returns a valid, fully-resized handle.
     */
    bstack_slice_t recovered = slice;
    recovered.allocator = self;
#ifndef BSTACK_FEATURE_ATOMIC
    uint64_t stack_len;
    int      is_tail;
#endif

    if (slice.len == 0) {
        if (gt_vt_alloc(self, new_len, out) != 0) {
            out->allocator = self; out->offset = 0; out->len = 0;
            return -1;
        }
        return 0;
    }

    if (slice.offset < ALGT_ARENA_START ||
        slice.offset != algt_align_up_ptr(slice.offset)) {
        /* Invalid address: the caller's handle is unchanged, hand it back. */
        errno = EINVAL;
        *out = recovered;
        return -1;
    }

    if (new_len == 0) {
        /* dealloc consumes `slice`; propagate its own survivor signal
         * unchanged — on -1 it hands back exactly the original slice. */
        int dr = gt_vt_dealloc(self, slice);
        if (dr != 0) {
            if (dr == -1) *out = recovered;
            return dr;
        }
        out->allocator = self; out->offset = 0; out->len = 0;
        return 0;
    }

    old_len     = slice.len;
    aligned_old = algt_align_up_len(old_len);
    aligned_new = algt_align_up_len(new_len);

    if (aligned_new == aligned_old) {
        /* Same underlying block: just zero the gap on shrink. */
        if (new_len < old_len) {
            uint64_t gap = old_len - new_len;
#if UINT64_MAX > SIZE_MAX
            if (gap > (uint64_t)SIZE_MAX) { errno = EINVAL; *out = recovered; return -1; }
#endif
            if (bstack_zero(a->bs, slice.offset + new_len, (size_t)gap) != 0) {
                *out = recovered;
                return -1;
            }
        }
        out->allocator = self;
        out->offset    = slice.offset;
        out->len       = new_len;
        return 0;
    }

#ifndef BSTACK_FEATURE_ATOMIC
    if (bstack_len(a->bs, &stack_len) != 0) { *out = recovered; return -1; }
    is_tail = (slice.offset + aligned_old == stack_len);
#endif

    if (aligned_new < aligned_old) {
        /* Shrink. */
        uint64_t freed_tail = aligned_old - aligned_new;
        uint64_t tail_ptr   = slice.offset + aligned_new;

#ifdef BSTACK_FEATURE_ATOMIC
        /* Atomic tail path: fuse the truncation and padding zeroing into ONE
         * crash-atomic splice. As two calls (try_discard then zero) a crash
         * between them shrinks the stack yet leaves the padding un-zeroed —
         * violating the zeroed-memory invariant. bstack_process_gen holds one
         * write lock across the LEN tail-check and the SPLICE, so a fault is
         * consulted before any mutation and leaves the block fully intact. */
        {
            bstack_alloc_tail_shrink_ctx_t ctx;
            uint8_t pad[ALGT_MIN_ALLOC];   /* zeros re-appended in the cut's place */
#if UINT64_MAX > SIZE_MAX
            if (aligned_old - new_len > (uint64_t)SIZE_MAX) { errno = EINVAL; *out = recovered; return -1; }
#endif
            memset(pad, 0, sizeof pad);
            ctx.phase         = 0;
            ctx.truncated     = 0;
            ctx.expected_tail = slice.offset + aligned_old;
            ctx.cur           = 0;
            ctx.cut           = (size_t)(aligned_old - new_len);
            ctx.new_buf       = pad;
            ctx.new_len       = (size_t)(aligned_new - new_len);
            if (bstack_process_gen(a->bs, bstack_alloc_tail_shrink_gen, &ctx) != 0) {
                *out = recovered;
                return -1;
            }
            if (ctx.truncated) {
                out->allocator = self; out->offset = slice.offset; out->len = new_len;
                return 0;
            }
            /* Not the tail — fall through to the non-tail shrink below. */
        }
#else
        if (is_tail) {
#if UINT64_MAX > SIZE_MAX
            if (freed_tail > (uint64_t)SIZE_MAX) { errno = EINVAL; *out = recovered; return -1; }
#endif
            /* Discard the freed tail FIRST: a fault here fires before any
             * mutation, so the original is fully intact (-1, survived). Only
             * once the shrink is committed do we zero the retained block's
             * padding — a fault there can no longer hand back the (now
             * shorter) original, since the stack no longer covers old_len. */
            if (bstack_discard(a->bs, (size_t)freed_tail) != 0) {
                *out = recovered;
                return -1;
            }
            if (new_len < aligned_new) {
                uint64_t gap = aligned_new - new_len;
#if UINT64_MAX > SIZE_MAX
                if (gap > (uint64_t)SIZE_MAX) { errno = EINVAL; return -2; }
#endif
                if (bstack_zero(a->bs, slice.offset + new_len, (size_t)gap) != 0)
                    return -2;
            }
            out->allocator = self; out->offset = slice.offset; out->len = new_len;
            return 0;
        }
#endif

        /* Non-tail: zero gap + freed tail (no lock), insert freed tail under
         * lock. The zero leaves the block intact on failure (-1); past that
         * point a torn (non-atomic) AVL insert cannot be safely retried, so a
         * failure there is a genuine loss (-2). */
        {
            uint64_t zero_n = aligned_old - new_len;
#if UINT64_MAX > SIZE_MAX
            if (zero_n > (uint64_t)SIZE_MAX) { errno = EINVAL; *out = recovered; return -1; }
#endif
            if (bstack_zero(a->bs, slice.offset + new_len, (size_t)zero_n) != 0) {
                *out = recovered;
                return -1;
            }
        }
        {
            int r;
            MUTEX_LOCK(a);
            r = algt_avl_insert(a->bs, tail_ptr, freed_tail);
            MUTEX_UNLOCK(a);
            if (r != 0) return -2;
        }
        out->allocator = self; out->offset = slice.offset; out->len = new_len;
        return 0;
    }

    /* Grow. */
#ifdef BSTACK_FEATURE_ATOMIC
    /* Atomic tail path: try_extend_zeros atomically checks and extends. */
    {
        int ok = 0;
        uint64_t delta = aligned_new - aligned_old;
#if UINT64_MAX > SIZE_MAX
        if (delta > (uint64_t)SIZE_MAX) { errno = EINVAL; *out = recovered; return -1; }
#endif
        if (bstack_try_extend_zeros(a->bs, slice.offset + aligned_old,
                                    (size_t)delta, &ok) != 0) {
            *out = recovered;
            return -1;
        }
        if (ok) {
            out->allocator = self; out->offset = slice.offset; out->len = new_len;
            return 0;
        }
    }
#else
    if (is_tail) {
        /* Extend in place — no copy needed. A single atomic bstack call: on
         * failure the tail is unchanged, so the original survives (-1). */
        uint64_t delta = aligned_new - aligned_old;
#if UINT64_MAX > SIZE_MAX
        if (delta > (uint64_t)SIZE_MAX) { errno = EINVAL; *out = recovered; return -1; }
#endif
        if (bstack_extend(a->bs, (size_t)delta, NULL) != 0) {
            *out = recovered;
            return -1;
        }
        out->allocator = self;
        out->offset    = slice.offset;
        out->len       = new_len;
        return 0;
    }
#endif

    /* Grow (non-tail): alloc new block, copy old data, free old block. */
    {
        bstack_slice_t new_s;
        if (gt_vt_alloc(self, new_len, &new_s) != 0) {
            *out = recovered;
            return -1;
        }
#if UINT64_MAX > SIZE_MAX
        if (old_len > (uint64_t)SIZE_MAX) {
            errno = EINVAL;
            gt_vt_dealloc(self, new_s); /* best-effort rollback; original is untouched either way */
            *out = recovered;
            return -1;
        }
#endif
        {
            uint8_t *tmp = malloc((size_t)old_len);
            if (!tmp) {
                gt_vt_dealloc(self, new_s);
                *out = recovered;
                return -1;
            }
            if (bstack_get(a->bs, slice.offset, slice.offset + old_len, tmp) != 0 ||
                bstack_set(a->bs, new_s.offset, tmp, (size_t)old_len) != 0) {
                free(tmp);
                /* The new region was allocated but the copy failed. Roll it back
                 * (best-effort) so it is not leaked; the original still holds
                 * the data untouched, so it remains the survivor regardless of
                 * whether this rollback itself succeeds. */
                gt_vt_dealloc(self, new_s);
                *out = recovered;
                return -1;
            }
            free(tmp);
        }
        /* Data copied and the new region is fully committed; it is now the
         * survivor, so a failure freeing the old block still returns a
         * valid, fully-resized handle (the old block leaks until recovery). */
        recovered = new_s;
        if (gt_vt_dealloc(self, slice) != 0) {
            *out = recovered;
            return -1;
        }
        *out = new_s;
        return 0;
    }
}

static int
gt_vt_alloc_bulk(bstack_allocator_t *self, const uint64_t *lens, size_t n,
                 bstack_slice_t *out_slices)
{
    ghost_tree_bstack_allocator_t *a = (ghost_tree_bstack_allocator_t *)self;
    uint64_t *aligned;
    uint64_t  total, found_ptr, found_size, block_ptr, offset;
    size_t    i;

    if (n == 0) return 0;

    aligned = (uint64_t *)calloc(n, sizeof *aligned);
    if (!aligned) return -1;

    total = 0;
    for (i = 0; i < n; i++) {
        uint64_t al = (lens[i] == 0) ? 0 : algt_align_up_len(lens[i]);
        aligned[i] = al;
        if (al > UINT64_MAX - total) {
            free(aligned);
            errno = EINVAL;
            return -1;
        }
        total += al;
    }

    /* All zero-length: return null slices without touching the BStack. */
    if (total == 0) {
        for (i = 0; i < n; i++) {
            out_slices[i].allocator = self;
            out_slices[i].offset    = 0;
            out_slices[i].len       = 0;
        }
        free(aligned);
        return 0;
    }

    /* Allocate one contiguous block.  Lock covers AVL search and conditional
     * insert (split); released before bstack_zero and before bstack_extend. */
    block_ptr = ALGT_NULL_PTR; /* sentinel: no free block found */
    MUTEX_LOCK(a);
    if (algt_avl_find_best_fit_and_remove(a->bs, total,
                                           &found_ptr, &found_size) != 0) {
        MUTEX_UNLOCK(a);
        free(aligned);
        return -1;
    }

    if (found_ptr != ALGT_NULL_PTR) {
        uint64_t remainder = found_size - total;
        if (remainder >= ALGT_MIN_ALLOC) {
            /* Split: recycle the leading remainder as a new free block;
             * use the trailing `total` bytes for the allocation. */
            if (algt_avl_insert(a->bs, found_ptr, remainder) != 0) {
                MUTEX_UNLOCK(a);
                free(aligned);
                return -1;
            }
            block_ptr = found_ptr + remainder;
            MUTEX_UNLOCK(a);
        } else {
            /* No split: unlock before zeroing the stale AVL node header. */
            MUTEX_UNLOCK(a);
            if (bstack_zero(a->bs, found_ptr, (size_t)ALGT_MIN_ALLOC) != 0) {
                free(aligned);
                return -1;
            }
            block_ptr = found_ptr;
        }
    } else {
        MUTEX_UNLOCK(a); /* release before extending the stack */
    }

    if (block_ptr == ALGT_NULL_PTR) {
#if UINT64_MAX > SIZE_MAX
        if (total > (uint64_t)SIZE_MAX) { free(aligned); errno = EINVAL; return -1; }
#endif
        if (bstack_extend(a->bs, (size_t)total, &block_ptr) != 0) {
            free(aligned);
            return -1;
        }
    }

    /* Slice the contiguous block into per-request regions. */
    offset = 0;
    for (i = 0; i < n; i++) {
        if (lens[i] == 0) {
            out_slices[i].allocator = self;
            out_slices[i].offset    = 0;
            out_slices[i].len       = 0;
        } else {
            out_slices[i].allocator = self;
            out_slices[i].offset    = block_ptr + offset;
            out_slices[i].len       = lens[i];
            offset += aligned[i];
        }
    }

    free(aligned);
    return 0;
}

static int algt_cmp_pair_by_ptr(const void *a, const void *b)
{
    const algt_block_t *x = (const algt_block_t *)a;
    const algt_block_t *y = (const algt_block_t *)b;
    return (x->ptr > y->ptr) - (x->ptr < y->ptr);
}

static int
gt_vt_dealloc_bulk(bstack_allocator_t *self, const bstack_slice_t *slices,
                   size_t n)
{
    ghost_tree_bstack_allocator_t *a = (ghost_tree_bstack_allocator_t *)self;
    algt_block_t *pairs;
    size_t pairs_n, i;

    if (n == 0) return 0;

    pairs = (algt_block_t *)malloc(n * sizeof *pairs);
    if (!pairs) return -1;
    pairs_n = 0;

    /* Collect non-empty slices as (ptr, aligned_size) pairs. */
    for (i = 0; i < n; i++) {
        if (slices[i].len == 0) continue;
        if (slices[i].offset < ALGT_ARENA_START ||
            slices[i].offset != algt_align_up_ptr(slices[i].offset)) {
            free(pairs); errno = EINVAL; return -1;
        }
        pairs[pairs_n].ptr  = slices[i].offset;
        pairs[pairs_n].size = algt_align_up_len(slices[i].len);
        pairs_n++;
    }

    if (pairs_n == 0) { free(pairs); return 0; }

    /* Sort by address so contiguous blocks are adjacent. */
    qsort(pairs, pairs_n, sizeof *pairs, algt_cmp_pair_by_ptr);

    /* Merge contiguous (ptr, size) pairs. */
    {
        size_t out = 0;
        for (i = 1; i < pairs_n; i++) {
            if (pairs[out].ptr + pairs[out].size == pairs[i].ptr) {
                pairs[out].size += pairs[i].size;
            } else {
                out++;
                pairs[out] = pairs[i];
            }
        }
        pairs_n = out + 1;
    }

    /* Free each merged block.  The highest-address block may be at the tail;
     * attempt a lock-free discard on it first.  All remaining blocks are
     * zeroed outside the lock (each is owned by the caller), then inserted
     * into the AVL tree under the lock in one pass. */
    {
        algt_block_t last = pairs[pairs_n - 1];
        int          last_discarded = 0;

#ifdef BSTACK_FEATURE_ATOMIC
        {
            int ok = 0;
#if UINT64_MAX > SIZE_MAX
            if (last.size > (uint64_t)SIZE_MAX) { free(pairs); errno = EINVAL; return -1; }
#endif
            if (bstack_try_discard(a->bs, last.ptr + last.size,
                                   (size_t)last.size, &ok) != 0) {
                free(pairs); return -1;
            }
            last_discarded = ok;
        }
#else
        {
            uint64_t stack_len;
            if (bstack_len(a->bs, &stack_len) != 0) { free(pairs); return -1; }
            if (last.ptr + last.size == stack_len) {
#if UINT64_MAX > SIZE_MAX
                if (last.size > (uint64_t)SIZE_MAX) { free(pairs); errno = EINVAL; return -1; }
#endif
                if (bstack_discard(a->bs, (size_t)last.size) != 0) { free(pairs); return -1; }
                last_discarded = 1;
            }
        }
#endif

        if (last_discarded)
            pairs_n--; /* remove last from the insert list */
    }

    /* Zero all blocks to be inserted (outside the lock). */
    for (i = 0; i < pairs_n; i++) {
#if UINT64_MAX > SIZE_MAX
        if (pairs[i].size > (uint64_t)SIZE_MAX) { free(pairs); errno = EINVAL; return -1; }
#endif
        if (bstack_zero(a->bs, pairs[i].ptr, (size_t)pairs[i].size) != 0) {
            free(pairs); return -1;
        }
    }

    /* Insert all zeroed blocks under the lock. */
    if (pairs_n > 0) {
        MUTEX_LOCK(a);
        for (i = 0; i < pairs_n; i++) {
            if (algt_avl_insert(a->bs, pairs[i].ptr, pairs[i].size) != 0) {
                MUTEX_UNLOCK(a);
                free(pairs);
                return -1;
            }
        }
        MUTEX_UNLOCK(a);
    }

    free(pairs);
    return 0;
}

static const bstack_bulk_allocator_vtbl_t gt_bulk_vtbl = {
    { gt_vt_stack, gt_vt_alloc, gt_vt_realloc, gt_vt_dealloc },
    gt_vt_alloc_bulk,
    gt_vt_dealloc_bulk
};

/* =========================================================================
 * ghost_tree_bstack_allocator_t — public API
 * ====================================================================== */

ghost_tree_bstack_allocator_t *ghost_tree_bstack_allocator_new(bstack_t *bs)
{
    ghost_tree_bstack_allocator_t *a;
    uint64_t stack_len;

    a = malloc(sizeof *a);
    if (!a) { errno = ENOMEM; return NULL; }
    a->base.vtbl      = &gt_bulk_vtbl.base;
    a->base.bulk_vtbl = &gt_bulk_vtbl;
    a->bs             = bs;

#ifdef BSTACK_FEATURE_ATOMIC
    if (bstack_alloc_lock_init(&a->lock) != 0) { free(a); return NULL; }
#endif

    if (bstack_len(bs, &stack_len) != 0) {
#ifdef BSTACK_FEATURE_ATOMIC
        bstack_alloc_lock_destroy(a->lock);
#endif
        free(a); return NULL;
    }

    if (stack_len == 0) {
        /* Fresh init: 32 user-reserved bytes + 8 magic + 8 root pointer */
        uint8_t hdr[48];
        memset(hdr, 0, 48);
        memcpy(hdr + 32, algt_magic, 8);
        if (bstack_push(bs, hdr, 48, NULL) != 0) {
#ifdef BSTACK_FEATURE_ATOMIC
            bstack_alloc_lock_destroy(a->lock);
#endif
            free(a); return NULL;
        }
        return a;
    }

    if (stack_len < ALGT_ARENA_START) {
#ifdef BSTACK_FEATURE_ATOMIC
        bstack_alloc_lock_destroy(a->lock);
#endif
        free(a);
        errno = EINVAL;
        return NULL;
    }

    /* Verify magic prefix */
    {
        uint8_t prefix[6];
        if (bstack_get(bs, ALGT_MAGIC_OFFSET, ALGT_MAGIC_OFFSET + 6,
                        prefix) != 0) {
#ifdef BSTACK_FEATURE_ATOMIC
            bstack_alloc_lock_destroy(a->lock);
#endif
            free(a); return NULL;
        }
        if (memcmp(prefix, algt_magic_prefix, 6) != 0) {
#ifdef BSTACK_FEATURE_ATOMIC
            bstack_alloc_lock_destroy(a->lock);
#endif
            free(a); errno = EINVAL; return NULL;
        }
    }

    /* Pad tail to next 32-byte arena boundary if misaligned */
    {
        uint64_t remainder = (stack_len - ALGT_ARENA_START) % 32;
        if (remainder != 0) {
            uint64_t pad = 32 - remainder;
#if UINT64_MAX > SIZE_MAX
            if (pad > (uint64_t)SIZE_MAX) {
#ifdef BSTACK_FEATURE_ATOMIC
                bstack_alloc_lock_destroy(a->lock);
#endif
                free(a); errno = EINVAL; return NULL;
            }
#endif
            if (bstack_extend(bs, (size_t)pad, NULL) != 0) {
#ifdef BSTACK_FEATURE_ATOMIC
                bstack_alloc_lock_destroy(a->lock);
#endif
                free(a); return NULL;
            }
        }
    }

    if (algt_coalesce_and_rebalance(bs) != 0) {
#ifdef BSTACK_FEATURE_ATOMIC
        bstack_alloc_lock_destroy(a->lock);
#endif
        free(a); return NULL;
    }
    return a;
}

void ghost_tree_bstack_allocator_free(ghost_tree_bstack_allocator_t *alloc)
{
#ifdef BSTACK_FEATURE_ATOMIC
    bstack_alloc_lock_destroy(alloc->lock);
#endif
    free(alloc);
}

bstack_t *ghost_tree_bstack_allocator_into_stack(ghost_tree_bstack_allocator_t *alloc)
{
    bstack_t *bs = alloc->bs;
#ifdef BSTACK_FEATURE_ATOMIC
    bstack_alloc_lock_destroy(alloc->lock);
#endif
    free(alloc);
    return bs;
}

/* =========================================================================
 * slab_bstack_allocator_t — fixed-block slab allocator
 * Requires -DBSTACK_FEATURE_SET (depends on bstack_set and bstack_zero).
 * ====================================================================== */

/* ---- constants --------------------------------------------------------- */

#define SLAB_OFFSET_SIZE       UINT64_C(24)
#define SLAB_HEADER_SIZE       UINT64_C(24) /* magic[8] + block_size[8] + free_head[8] */
#define SLAB_ARENA_START       (SLAB_OFFSET_SIZE + SLAB_HEADER_SIZE)
#define SLAB_BLOCK_SIZE_OFFSET (SLAB_OFFSET_SIZE + UINT64_C(8))
#define SLAB_FREE_HEAD_OFFSET  (SLAB_OFFSET_SIZE + UINT64_C(16))
#define SLAB_MIN_BLOCK_SIZE    UINT64_C(8)
#define SLAB_SENTINEL          UINT64_C(0)

static const uint8_t alsl_magic[8]        = {'A','L','S','L',0,1,1,0};
static const uint8_t alsl_magic_prefix[6] = {'A','L','S','L',0,1};

/* ---- helpers ----------------------------------------------------------- */

static uint64_t slab_blocks_needed(uint64_t len, uint64_t block_size)
{
    if (len == 0) return 0;
    /* (len - 1) / block_size + 1 avoids the (len + block_size - 1) overflow.
     * Safe because block_size >= SLAB_MIN_BLOCK_SIZE (8), so the result fits
     * in uint64_t: max is (UINT64_MAX-1)/8 + 1 = 2305843009213693952. */
    return (len - 1) / block_size + 1;
}

/* free_head read/write helpers: only the non-atomic free-list paths use them;
 * the atomic paths drive free_head through process_gen / cross_exchange. */
#ifndef BSTACK_FEATURE_ATOMIC
static int slab_read_free_head(bstack_t *bs, uint64_t *out)
{
    uint8_t buf[8];
    if (bstack_get(bs, SLAB_FREE_HEAD_OFFSET,
                   SLAB_FREE_HEAD_OFFSET + 8, buf) != 0) return -1;
    *out = read_le64(buf);
    return 0;
}

static int slab_write_free_head(bstack_t *bs, uint64_t val)
{
    uint8_t buf[8];
    write_le64(buf, val);
    return bstack_set(bs, SLAB_FREE_HEAD_OFFSET, buf, 8);
}
#endif /* !BSTACK_FEATURE_ATOMIC */

/*
 * Pop the head block from the free list.
 * Sets *out_block to the block's payload offset, or SLAB_SENTINEL if empty.
 * Zeros the block after popping; failure is propagated to preserve the
 * allocator contract that returned bytes are zero-initialized.
 */
#ifdef BSTACK_FEATURE_ATOMIC
/*
 * Atomic pop: drive a single bstack_process_gen sequence — read free_head,
 * read the popped block's next-pointer, write that next back into free_head —
 * under one held write lock, so the read-read-write is indivisible with
 * respect to any other thread's free-list operation (closing the ABA window a
 * get/cas pair would leave open).  The popped block is zeroed in a separate
 * call afterwards, since by then it is exclusively owned by the caller.
 */
struct slab_pop_ctx {
    uint8_t  head_buf[8];
    uint8_t  next_buf[8];
    uint64_t popped;
    int      have_popped;
    int      step;
};

static int slab_pop_gen(bstack_gen_op_t *out_op, void *userctx)
{
    struct slab_pop_ctx *ctx = userctx;
    switch (ctx->step++) {
    case 0: /* read the current free-list head */
        out_op->kind          = BSTACK_GEN_READ;
        out_op->u.read.offset = SLAB_FREE_HEAD_OFFSET;
        out_op->u.read.buf    = ctx->head_buf;
        out_op->u.read.len    = 8;
        return 1;
    case 1: { /* empty list ends with no write; else read head's next-pointer */
        uint64_t head = read_le64(ctx->head_buf);
        if (head == SLAB_SENTINEL) return 0;
        ctx->popped      = head;
        ctx->have_popped = 1;
        out_op->kind          = BSTACK_GEN_READ;
        out_op->u.read.offset = head;
        out_op->u.read.buf    = ctx->next_buf;
        out_op->u.read.len    = 8;
        return 1;
    }
    case 2: /* advance free_head to the popped block's next-pointer */
        out_op->kind           = BSTACK_GEN_WRITE;
        out_op->u.write.offset = SLAB_FREE_HEAD_OFFSET;
        out_op->u.write.data   = ctx->next_buf;
        out_op->u.write.len    = 8;
        return 1;
    default:
        return 0;
    }
}

static int slab_pop_free_block(bstack_t *bs, uint64_t block_size,
                                uint64_t *out_block)
{
    struct slab_pop_ctx ctx;
    memset(&ctx, 0, sizeof ctx);

    if (bstack_process_gen(bs, slab_pop_gen, &ctx) != 0) return -1;
    if (!ctx.have_popped) { *out_block = SLAB_SENTINEL; return 0; }

    /* Zero the block after popping; on failure the popped block is leaked but
     * not returned to callers in a non-zero state. */
#if UINT64_MAX > SIZE_MAX
    if (block_size > (uint64_t)SIZE_MAX) { errno = EINVAL; return -1; }
#endif
    if (bstack_zero(bs, ctx.popped, (size_t)block_size) != 0) return -1;

    *out_block = ctx.popped;
    return 0;
}
#else /* !BSTACK_FEATURE_ATOMIC */
static int slab_pop_free_block(bstack_t *bs, uint64_t block_size,
                                uint64_t *out_block)
{
    uint8_t buf[8];
    uint64_t head, next;

    if (slab_read_free_head(bs, &head) != 0) return -1;
    if (head == SLAB_SENTINEL) { *out_block = SLAB_SENTINEL; return 0; }

    /* Read next-pointer before updating free_head: a crash between these two
     * calls leaves the block still at the head of the list. */
    if (bstack_get(bs, head, head + 8, buf) != 0) return -1;
    next = read_le64(buf);
    if (slab_write_free_head(bs, next) != 0) return -1;

    /* Zero the block after popping; on failure the popped block is leaked but
     * not returned to callers in a non-zero state. */
#if UINT64_MAX > SIZE_MAX
    if (block_size > (uint64_t)SIZE_MAX) { errno = EINVAL; return -1; }
#endif
    if (bstack_zero(bs, head, (size_t)block_size) != 0) return -1;

    *out_block = head;
    return 0;
}
#endif /* BSTACK_FEATURE_ATOMIC */

/*
 * Prepend the block at block_start to the free list.
 */
#ifdef BSTACK_FEATURE_ATOMIC
/*
 * Lock-free splice via bstack_cross_exchange: block_start is first seeded with
 * a self-pointer placeholder, then atomically swapped with free_head under one
 * write lock — free_head becomes block_start and block_start's next-pointer
 * becomes the old head, in a single indivisible step.  A crash between the two
 * calls leaks block_start rather than corrupting the list.
 */
static int slab_push_free_block(bstack_t *bs, uint64_t block_start)
{
    uint8_t buf[8];
    write_le64(buf, block_start); /* placeholder: replaced by old head below */
    if (bstack_set(bs, block_start, buf, 8) != 0) return -1;
    return bstack_cross_exchange(bs, block_start, SLAB_FREE_HEAD_OFFSET, 8);
}
#else /* !BSTACK_FEATURE_ATOMIC */
/*
 * Writes the next-pointer into the block before updating free_head:
 * a crash after the first write but before the second leaks the block rather
 * than corrupting the list.
 */
static int slab_push_free_block(bstack_t *bs, uint64_t block_start)
{
    uint8_t buf[8];
    uint64_t head;

    if (slab_read_free_head(bs, &head) != 0) return -1;
    write_le64(buf, head);
    if (bstack_set(bs, block_start, buf, 8) != 0) return -1;
    return slab_write_free_head(bs, block_start);
}
#endif /* BSTACK_FEATURE_ATOMIC */

/*
 * Prepend `count` contiguous blocks starting at `first_block` to the free list.
 *
 * Without atomic: exactly 3 IO calls regardless of count — one read of
 * free_head, one bulk write of all next-pointers into the freed region, and
 * one write of free_head.  A crash after the bulk write but before the
 * free_head update leaks the entire batch rather than corrupting the list.
 *
 * With atomic: generalises slab_push_free_block to a whole run.  The chain
 * first_block -> ... -> last_block is built in one buffer, with last_block's
 * next-pointer set to the placeholder first_block.  A single bulk bstack_set
 * writes the chain (still unreachable from free_head), then
 * bstack_cross_exchange atomically swaps last_block's next-pointer with
 * free_head — free_head becomes first_block and last_block's next-pointer
 * becomes the old head, splicing the whole run in under one write lock.
 */
static int slab_push_free_blocks(bstack_t *bs, uint64_t first_block,
                                  uint64_t count, uint64_t block_size)
{
    uint64_t i, buf_size;
    uint8_t *buf;
#ifndef BSTACK_FEATURE_ATOMIC
    uint64_t old_head;
#endif

    if (count == 0) return 0;
    if (count == 1) return slab_push_free_block(bs, first_block);

#ifndef BSTACK_FEATURE_ATOMIC
    if (slab_read_free_head(bs, &old_head) != 0) return -1;
#endif

    if (count > UINT64_MAX / block_size) { errno = EINVAL; return -1; }
    buf_size = count * block_size;
#if UINT64_MAX > SIZE_MAX
    if (buf_size > (uint64_t)SIZE_MAX) { errno = EINVAL; return -1; }
#endif
    buf = calloc(1, (size_t)buf_size);
    if (!buf) return -1;

    for (i = 0; i < count - 1; i++) {
        uint64_t next = first_block + (i + 1) * block_size;
        write_le64(buf + (size_t)(i * block_size), next);
    }
#ifdef BSTACK_FEATURE_ATOMIC
    /* Placeholder: replaced with the old free_head by cross_exchange below. */
    write_le64(buf + (size_t)((count - 1) * block_size), first_block);
#else
    write_le64(buf + (size_t)((count - 1) * block_size), old_head);
#endif

    if (bstack_set(bs, first_block, buf, (size_t)buf_size) != 0) {
        free(buf); return -1;
    }
    free(buf);

#ifdef BSTACK_FEATURE_ATOMIC
    {
        uint64_t last_block = first_block + (count - 1) * block_size;
        return bstack_cross_exchange(bs, last_block, SLAB_FREE_HEAD_OFFSET, 8);
    }
#else
    return slab_write_free_head(bs, first_block);
#endif
}

/* ---- vtable ------------------------------------------------------------ */

static bstack_t *slab_vtbl_stack(bstack_allocator_t *base)
{
    return ((slab_bstack_allocator_t *)base)->bs;
}

static int slab_vtbl_alloc(bstack_allocator_t *base, uint64_t len,
                            bstack_slice_t *out)
{
    slab_bstack_allocator_t *a = (slab_bstack_allocator_t *)base;
    uint64_t offset, n, total, block;

    if (len == 0) {
        out->allocator = base; out->offset = SLAB_SENTINEL; out->len = 0;
        return 0;
    }

    if (len <= a->block_size) {
        /* Free-list pop is lock-free under atomic (single process_gen
         * sequence); single-threaded otherwise. */
        if (slab_pop_free_block(a->bs, a->block_size, &block) != 0) return -1;
        if (block != SLAB_SENTINEL) {
            out->allocator = base; out->offset = block; out->len = len;
            return 0;
        }
        /* Free list empty: extend tail (no lock needed — bstack_extend is
         * internally serialised and returns a distinct region per call). */
#if UINT64_MAX > SIZE_MAX
        if (a->block_size > (uint64_t)SIZE_MAX) { errno = EINVAL; return -1; }
#endif
        if (bstack_extend(a->bs, (size_t)a->block_size, &offset) != 0) return -1;
        out->allocator = base; out->offset = offset; out->len = len;
        return 0;
    }

    /* Oversized: extend by the smallest multiple of block_size that fits len. */
    n = slab_blocks_needed(len, a->block_size);
    if (n > UINT64_MAX / a->block_size) { errno = EINVAL; return -1; }
    total = n * a->block_size;
#if UINT64_MAX > SIZE_MAX
    if (total > (uint64_t)SIZE_MAX) { errno = EINVAL; return -1; }
#endif
    if (bstack_extend(a->bs, (size_t)total, &offset) != 0) return -1;
    out->allocator = base; out->offset = offset; out->len = len;
    return 0;
}

static int slab_vtbl_dealloc(bstack_allocator_t *base, bstack_slice_t s)
{
    slab_bstack_allocator_t *a = (slab_bstack_allocator_t *)base;
    uint64_t n_blocks, backing_size;

    if (s.len == 0 && s.offset == SLAB_SENTINEL) return 0;

    n_blocks = slab_blocks_needed(s.len, a->block_size);
    if (n_blocks > UINT64_MAX / a->block_size) { errno = EINVAL; return -1; }
    backing_size = n_blocks * a->block_size;

    /* Tail discard path: only for oversized allocations (> 1 block).
     * try_discard atomically checks tail == slice_end and removes backing_size
     * bytes under BStack's own write lock — no allocator lock needed. */
    if (s.len > a->block_size) {
        if (s.offset > UINT64_MAX - backing_size) { errno = EINVAL; return -1; }
#if UINT64_MAX > SIZE_MAX
        if (backing_size > (uint64_t)SIZE_MAX) { errno = EINVAL; return -1; }
#endif
#ifdef BSTACK_FEATURE_ATOMIC
        {
            int ok = 0;
            if (bstack_try_discard(a->bs, s.offset + backing_size,
                                   (size_t)backing_size, &ok) != 0) return -1;
            if (ok) return 0;
        }
#else
        {
            uint64_t tail;
            if (bstack_len(a->bs, &tail) != 0) return -1;
            if (s.offset + backing_size == tail)
                return bstack_discard(a->bs, (size_t)backing_size);
        }
#endif
    }

    /* Not at tail (or single-block): push to the free list (lock-free under
     * atomic via cross_exchange; single-threaded otherwise). This mutates
     * multiple block links, so a mid-way failure may leave the blocks
     * partially freed — the handle can no longer be safely returned (-2). */
    return slab_push_free_blocks(a->bs, s.offset, n_blocks, a->block_size) == 0 ? 0 : -2;
}

static int slab_vtbl_realloc(bstack_allocator_t *base, bstack_slice_t s,
                              uint64_t new_len, bstack_slice_t *out)
{
    slab_bstack_allocator_t *a = (slab_bstack_allocator_t *)base;
    uint64_t old_n, new_n, old_backing, new_backing;
    /*
     * The surviving allocation to hand back on failure. Starts as the
     * original block; updated once a move commits a new region or a shrink
     * commits the retained region (both distinct from any blocks being
     * freed, so the handle is always safe to return). Every failure path in
     * this function leaves `recovered` valid, so realloc here always fails
     * with -1 (survived), never -2 (lost).
     */
    bstack_slice_t recovered = s;
    recovered.allocator = base;

    if (s.len == 0 && s.offset == SLAB_SENTINEL) {
        if (slab_vtbl_alloc(base, new_len, out) != 0) {
            out->allocator = base; out->offset = SLAB_SENTINEL; out->len = 0;
            return -1;
        }
        return 0;
    }

    if (new_len == 0) {
        /* dealloc consumes `s`; propagate its own survivor signal unchanged
         * — on -1 it hands back exactly the original slice. */
        int dr = slab_vtbl_dealloc(base, s);
        if (dr != 0) {
            if (dr == -1) *out = recovered;
            return dr;
        }
        out->allocator = base; out->offset = SLAB_SENTINEL; out->len = 0;
        return 0;
    }

    if (new_len == s.len) { *out = s; return 0; }

    old_n = slab_blocks_needed(s.len,   a->block_size);
    new_n = slab_blocks_needed(new_len, a->block_size);

    if (old_n == new_n) {
        /* Same backing blocks: zero newly-exposed bytes on grow (lock-free:
         * zero stays within the caller's allocated region). */
        if (new_len > s.len) {
            uint64_t to_zero = new_len - s.len;
#if UINT64_MAX > SIZE_MAX
            if (to_zero > (uint64_t)SIZE_MAX) { errno = EINVAL; *out = recovered; return -1; }
#endif
            if (bstack_zero(a->bs, s.offset + s.len, (size_t)to_zero) != 0) {
                *out = recovered;
                return -1;
            }
        }
        out->allocator = base; out->offset = s.offset; out->len = new_len;
        return 0;
    }

    if (old_n > UINT64_MAX / a->block_size) { errno = EINVAL; *out = recovered; return -1; }
    old_backing = old_n * a->block_size;
    if (new_n > UINT64_MAX / a->block_size) { errno = EINVAL; *out = recovered; return -1; }
    new_backing = new_n * a->block_size;
    if (s.offset > UINT64_MAX - old_backing) { errno = EINVAL; *out = recovered; return -1; }

    if (new_n > old_n) {
        /* Grow path.
         * With atomic: try_extend_zeros atomically checks tail == sentinel and
         * appends the delta — no allocator lock needed.
         * Without atomic: plain len() check then extend (single-threaded). */
        uint64_t to_extend = new_backing - old_backing;
        uint64_t sentinel  = s.offset + old_backing;
        int      tail_done = 0;
#if UINT64_MAX > SIZE_MAX
        if (to_extend > (uint64_t)SIZE_MAX) { errno = EINVAL; *out = recovered; return -1; }
#endif

#ifdef BSTACK_FEATURE_ATOMIC
        {
            int ok = 0;
            if (bstack_try_extend_zeros(a->bs, sentinel,
                                        (size_t)to_extend, &ok) != 0) {
                *out = recovered;
                return -1;
            }
            if (ok) tail_done = 1;
        }
#else
        {
            uint64_t tail;
            if (bstack_len(a->bs, &tail) != 0) { *out = recovered; return -1; }
            if (sentinel == tail) {
                if (bstack_extend(a->bs, (size_t)to_extend, NULL) != 0) {
                    *out = recovered;
                    return -1;
                }
                tail_done = 1;
            }
        }
#endif

        if (tail_done) {
            /* The extended bytes cover [old_backing, new_backing); this zero
             * only fills [s.len, new_len), which lies at or past old_len and
             * never touches the original's own bytes, so the original
             * (s.offset, s.len) stays a safe fallback even if it fails. */
            if (new_len > s.len) {
                uint64_t to_zero = new_len - s.len;
#if UINT64_MAX > SIZE_MAX
                if (to_zero > (uint64_t)SIZE_MAX) { errno = EINVAL; *out = recovered; return -1; }
#endif
                if (bstack_zero(a->bs, s.offset + s.len, (size_t)to_zero) != 0) {
                    *out = recovered;
                    return -1;
                }
            }
            out->allocator = base; out->offset = s.offset; out->len = new_len;
            return 0;
        }

        /* Not at tail (or tail moved under atomic): grow non-tail.
         * Read old data into a zeroed new_backing-sized buffer, push it as a
         * single atomic write, then free the old blocks (lock-free under
         * atomic via cross_exchange). */
        {
            uint8_t  *buf;
            uint64_t  push_offset;

#if UINT64_MAX > SIZE_MAX
            if (new_backing > (uint64_t)SIZE_MAX) { errno = EINVAL; *out = recovered; return -1; }
#endif
            buf = calloc(1, (size_t)new_backing);
            if (!buf) { *out = recovered; return -1; }

            if (s.len > 0) {
                if (bstack_get(a->bs, s.offset, s.offset + s.len, buf) != 0) {
                    free(buf);
                    *out = recovered;
                    return -1;
                }
            }

            if (bstack_push(a->bs, buf, (size_t)new_backing, &push_offset) != 0) {
                free(buf);
                *out = recovered;
                return -1;
            }
            free(buf);

            /* New region committed and populated; it is now the survivor, so
             * a failure freeing the old blocks returns the new region
             * instead (the old blocks leak until crash recovery). */
            recovered.allocator = base;
            recovered.offset    = push_offset;
            recovered.len       = new_len;

            if (slab_push_free_blocks(a->bs, s.offset, old_n,
                                      a->block_size) != 0) {
                *out = recovered;
                return -1;
            }

            out->allocator = base;
            out->offset    = push_offset;
            out->len       = new_len;
            return 0;
        }
    }

    /* Shrink path (new_n < old_n).
     * With atomic: try_discard atomically checks tail == sentinel and removes
     * the excess — no lock needed.  On failure the slice is not at the tail;
     * fall through to shrink non-tail.
     * Without atomic: plain len() check then discard (single-threaded). */
    {
        uint64_t to_discard = old_backing - new_backing;
        uint64_t sentinel   = s.offset + old_backing;
        int      tail_done  = 0;
#if UINT64_MAX > SIZE_MAX
        if (to_discard > (uint64_t)SIZE_MAX) { errno = EINVAL; *out = recovered; return -1; }
#endif

#ifdef BSTACK_FEATURE_ATOMIC
        {
            int ok = 0;
            if (bstack_try_discard(a->bs, sentinel,
                                   (size_t)to_discard, &ok) != 0) {
                *out = recovered;
                return -1;
            }
            if (ok) tail_done = 1;
        }
#else
        {
            uint64_t tail;
            if (bstack_len(a->bs, &tail) != 0) { *out = recovered; return -1; }
            if (sentinel == tail) {
                if (bstack_discard(a->bs, (size_t)to_discard) != 0) {
                    *out = recovered;
                    return -1;
                }
                tail_done = 1;
            }
        }
#endif

        if (tail_done) {
            out->allocator = base; out->offset = s.offset; out->len = new_len;
            return 0;
        }

        /* Shrink non-tail: the first new_n blocks are retained regardless of
         * the outcome, so the resized region is the survivor if freeing the
         * excess blocks fails. */
        recovered.allocator = base;
        recovered.offset    = s.offset;
        recovered.len       = new_len;
        if (slab_push_free_blocks(a->bs,
                                  s.offset + new_n * a->block_size,
                                  old_n - new_n, a->block_size) != 0) {
            *out = recovered;
            return -1;
        }
        out->allocator = base; out->offset = s.offset; out->len = new_len;
        return 0;
    }
}

static const bstack_allocator_vtbl_t slab_allocator_vtbl = {
    slab_vtbl_stack,
    slab_vtbl_alloc,
    slab_vtbl_realloc,
    slab_vtbl_dealloc,
};

/* ---- public API -------------------------------------------------------- */

slab_bstack_allocator_t *slab_bstack_allocator_new(bstack_t *bs,
                                                    uint64_t block_size)
{
    slab_bstack_allocator_t *a;
    int is_empty;
    uint8_t hdr[48]; /* SLAB_ARENA_START */

    if (bstack_is_empty(bs, &is_empty) != 0) return NULL;
    if (!is_empty) { errno = EINVAL; return NULL; }

    if (block_size < SLAB_MIN_BLOCK_SIZE) { errno = EINVAL; return NULL; }
#if UINT64_MAX > SIZE_MAX
    if (block_size > (uint64_t)SIZE_MAX) { errno = EINVAL; return NULL; }
#endif

    a = malloc(sizeof *a);
    if (!a) { errno = ENOMEM; return NULL; }

    memset(hdr, 0, sizeof hdr);
    memcpy(hdr + (size_t)SLAB_OFFSET_SIZE, alsl_magic, 8);
    write_le64(hdr + (size_t)SLAB_BLOCK_SIZE_OFFSET, block_size);
    /* free_head at SLAB_FREE_HEAD_OFFSET stays 0 (SLAB_SENTINEL) */

    if (bstack_push(bs, hdr, sizeof hdr, NULL) != 0) {
        free(a); return NULL;
    }

    a->base.vtbl      = &slab_allocator_vtbl;
    a->base.bulk_vtbl = NULL;
    a->bs             = bs;
    a->block_size     = block_size;
    return a;
}

slab_bstack_allocator_t *slab_bstack_allocator_open(bstack_t *bs)
{
    slab_bstack_allocator_t *a;
    int is_empty;
    uint8_t header[24]; /* SLAB_HEADER_SIZE */
    uint64_t stored_block_size, stack_len;

    if (bstack_is_empty(bs, &is_empty) != 0) return NULL;
    if (is_empty) { errno = EINVAL; return NULL; }

    if (bstack_len(bs, &stack_len) != 0) return NULL;
    if (stack_len < SLAB_ARENA_START) { errno = EINVAL; return NULL; }

    if (bstack_get(bs, SLAB_OFFSET_SIZE,
                   SLAB_OFFSET_SIZE + SLAB_HEADER_SIZE, header) != 0)
        return NULL;

    if (memcmp(header, alsl_magic_prefix, sizeof alsl_magic_prefix) != 0) {
        errno = EINVAL; return NULL;
    }

    stored_block_size = read_le64(header + 8);
    if (stored_block_size < SLAB_MIN_BLOCK_SIZE) { errno = EINVAL; return NULL; }

    {
        uint64_t stored_free_head = read_le64(header + 16);
        if (stored_free_head != SLAB_SENTINEL) {
            if (stored_free_head < SLAB_ARENA_START
                || (stored_free_head - SLAB_ARENA_START) % stored_block_size != 0
                || stored_free_head >= stack_len) {
                errno = EINVAL; return NULL;
            }
        }
        if (stack_len < SLAB_ARENA_START) { errno = EINVAL; return NULL; }
        if ((stack_len - SLAB_ARENA_START) % stored_block_size != 0) {
            errno = EINVAL; return NULL;
        }
    }

    a = malloc(sizeof *a);
    if (!a) {
        errno = ENOMEM;
        return NULL;
    }

    a->base.vtbl      = &slab_allocator_vtbl;
    a->base.bulk_vtbl = NULL;
    a->bs             = bs;
    a->block_size     = stored_block_size;
    return a;
}

void slab_bstack_allocator_free(slab_bstack_allocator_t *alloc)
{
    free(alloc);
}

bstack_t *slab_bstack_allocator_into_stack(slab_bstack_allocator_t *alloc)
{
    bstack_t *bs = alloc->bs;
    free(alloc);
    return bs;
}

uint64_t slab_bstack_allocator_block_size(const slab_bstack_allocator_t *alloc)
{
    return alloc->block_size;
}

/* =========================================================================
 * checked_slab_bstack_allocator_t — crash-recoverable fixed-block slab allocator
 * Requires -DBSTACK_FEATURE_SET (depends on bstack_set and bstack_zero).
 * ====================================================================== */

/* ---- constants --------------------------------------------------------- */

#define ALCK_OFFSET_SIZE      UINT64_C(24)
#define ALCK_HEADER_SIZE      UINT64_C(24) /* magic[8] + block_size[8] + free_head[8] */
#define ALCK_ARENA_START      (ALCK_OFFSET_SIZE + ALCK_HEADER_SIZE)
#define ALCK_FREE_HEAD_OFFSET (ALCK_OFFSET_SIZE + UINT64_C(16))
#define ALCK_OVERHEAD         UINT64_C(8)
#define ALCK_MIN_BLOCK_SIZE   UINT64_C(16)
#define ALCK_MIN_DATA_SIZE    (ALCK_MIN_BLOCK_SIZE - ALCK_OVERHEAD)
#define ALCK_SENTINEL         UINT64_C(0)
#define ALCK_IN_USE_BIT       UINT64_C(0x8000000000000000)
#define ALCK_BLOCKS_MASK      (~ALCK_IN_USE_BIT)
/* Maximum number of suspect blocks analysed in resync_tail before giving up. */
#define ALCK_MAX_RECOVER_REGION ((size_t)(1u << 26))

static const uint8_t alck_magic[8]        = {'A','L','C','K',0,1,2,0};
static const uint8_t alck_magic_prefix[6] = {'A','L','C','K',0,1};

/* ---- LE helpers (reuse read_le64 / write_le64 already defined above) --- */

/* ---- overhead I/O ------------------------------------------------------ */

static int alck_read_overhead(bstack_t *bs, uint64_t block_start, uint64_t *out)
{
    uint8_t buf[8];
    if (bstack_get(bs, block_start, block_start + 8, buf) != 0) return -1;
    *out = read_le64(buf);
    return 0;
}

static int alck_write_overhead(bstack_t *bs, uint64_t block_start, uint64_t value)
{
    uint8_t buf[8];
    write_le64(buf, value);
    return bstack_set(bs, block_start, buf, 8);
}

/* ---- free_head I/O ----------------------------------------------------- */

/* Only the non-atomic free-list paths read/write free_head directly; the
 * atomic paths drive it through process_gen / cross_exchange. */
#ifndef BSTACK_FEATURE_ATOMIC
static int alck_read_free_head(bstack_t *bs, uint64_t *out)
{
    uint8_t buf[8];
    if (bstack_get(bs, ALCK_FREE_HEAD_OFFSET,
                   ALCK_FREE_HEAD_OFFSET + 8, buf) != 0) return -1;
    *out = read_le64(buf);
    return 0;
}

static int alck_write_free_head(bstack_t *bs, uint64_t val)
{
    uint8_t buf[8];
    write_le64(buf, val);
    return bstack_set(bs, ALCK_FREE_HEAD_OFFSET, buf, 8);
}
#endif /* !BSTACK_FEATURE_ATOMIC */

/* ---- blocks_needed ----------------------------------------------------- */

/*
 * Number of block_size blocks required to hold len usable bytes plus the
 * 8-byte overhead prefix.
 */
static uint64_t alck_blocks_needed(uint64_t len, uint64_t block_size)
{
    uint64_t total;
    if (len == 0) return 0;
    /* total = len + OVERHEAD; div_ceil(total, block_size) */
    if (len > UINT64_MAX - ALCK_OVERHEAD) return UINT64_MAX; /* signal overflow */
    total = len + ALCK_OVERHEAD;
    return (total - 1) / block_size + 1;
}

/* ---- valid_in_use ------------------------------------------------------ */

/*
 * Check whether overhead at block_start is a valid in-use marker.
 *
 * Returns the block count (>= 1) on success, 0 on failure.
 * Rejects: high bit clear, count == 0, span overflows/exceeds stack_len, or
 * the span engulfs a known free block (free[] is sorted).
 */
static uint64_t alck_valid_in_use(uint64_t overhead, uint64_t p,
                                   uint64_t stack_len, uint64_t block_size,
                                   const uint64_t *free_arr, size_t free_cnt)
{
    uint64_t n, span, end;
    size_t   lo, hi, mid, idx;

    if ((overhead & ALCK_IN_USE_BIT) == 0) return 0;
    n = overhead & ALCK_BLOCKS_MASK;
    if (n == 0) return 0;
    if (n > UINT64_MAX / block_size) return 0;
    span = n * block_size;
    if (p > UINT64_MAX - span) return 0;
    end = p + span;
    if (end > stack_len) return 0;

    /* Binary search: find first free block > p */
    lo = 0; hi = free_cnt; idx = free_cnt;
    while (lo < hi) {
        mid = lo + (hi - lo) / 2;
        if (free_arr[mid] <= p) lo = mid + 1;
        else { idx = mid; hi = mid; }
    }
    if (idx < free_cnt && free_arr[idx] < end)
        return 0; /* free block inside span — invalid */

    return n;
}

/* ---- scan_free_list ---------------------------------------------------- */

/*
 * Walk the free list into a malloc'd sorted array of block offsets.
 * *out_free and *out_cnt are set on success; caller must free *out_free.
 * *out_corrupt is set to 1 if the walk was cut short by structural corruption.
 * Returns 0 on success, -1 on I/O failure.
 *
 * Only the non-atomic recovery path uses this; the atomic path performs the
 * equivalent walk inline inside its process_gen scan.
 */
#ifndef BSTACK_FEATURE_ATOMIC
static int alck_scan_free_list(bstack_t *bs, uint64_t stack_len,
                                uint64_t block_size,
                                uint64_t **out_free, size_t *out_cnt,
                                int *out_corrupt)
{
    uint64_t  head, max_blocks;
    uint64_t *arr      = NULL;
    size_t    cnt      = 0, cap = 0;
    int       corrupt  = 0;

    *out_free    = NULL;
    *out_cnt     = 0;
    *out_corrupt = 0;

    if (alck_read_free_head(bs, &head) != 0) return -1;

    max_blocks = (stack_len > ALCK_ARENA_START)
                 ? (stack_len - ALCK_ARENA_START) / block_size
                 : 0;

    while (head != ALCK_SENTINEL) {
        uint8_t  prefix[16];
        uint64_t overhead;
        size_t   i;

        /* Structural checks */
        if (head < ALCK_ARENA_START
            || (head - ALCK_ARENA_START) % block_size != 0
            || head >= stack_len
            || cnt >= (size_t)max_blocks + 1) { /* cycle guard */
            corrupt = 1; break;
        }

        /* Cycle detection is bounded by max_blocks (cnt > max_blocks implies a cycle/corruption). */
        (void)i;

        if (bstack_get(bs, head, head + 16, prefix) != 0) {
            free(arr); return -1;
        }
        overhead = read_le64(prefix);
        if (overhead != 0) { corrupt = 1; break; }

        /* Grow array if needed */
        if (cnt == cap) {
            size_t    nc  = cap ? cap * 2 : 16;
            uint64_t *tmp = realloc(arr, nc * sizeof *tmp);
            if (!tmp) { free(arr); return -1; }
            arr = tmp; cap = nc;
        }
        arr[cnt++] = head;
        head = read_le64(prefix + 8);
    }

    /* Sort the array for binary-search use in the caller */
    if (cnt > 1) {
        size_t i, j;
        /* Insertion sort — list is typically short */
        for (i = 1; i < cnt; i++) {
            uint64_t key = arr[i];
            j = i;
            while (j > 0 && arr[j - 1] > key) { arr[j] = arr[j - 1]; j--; }
            arr[j] = key;
        }
    }

    *out_free    = arr;
    *out_cnt     = cnt;
    *out_corrupt = corrupt;
    return 0;
}
#endif /* !BSTACK_FEATURE_ATOMIC */

/* ---- write_free_run ---------------------------------------------------- */

/*
 * Write the block prefixes for a run of `count` contiguous free blocks
 * starting at `first_block`, linking them into a chain.  Each block's overhead
 * is zeroed and data[0..8] holds the next block's offset.  All other data
 * bytes in the run are zeroed.  Does NOT update free_head.
 *
 * Without atomic the last block's data[0..8] holds the current free_head, which
 * the caller then writes into free_head directly.  With atomic it holds the
 * placeholder first_block; alck_push_free_blocks splices the whole run onto
 * free_head with bstack_cross_exchange afterwards.  Either way the single bulk
 * bstack_set is crash-safe: until the run is spliced in the whole run is simply
 * unreachable.
 */
static int alck_write_free_run(bstack_t *bs, uint64_t first_block,
                                uint64_t count, uint64_t block_size)
{
    uint64_t total, i;
    uint8_t *buf;
#ifndef BSTACK_FEATURE_ATOMIC
    uint64_t old_head;
    if (alck_read_free_head(bs, &old_head) != 0) return -1;
#endif

    if (count > UINT64_MAX / block_size) { errno = EINVAL; return -1; }
    total = count * block_size;
#if UINT64_MAX > SIZE_MAX
    if (total > (uint64_t)SIZE_MAX) { errno = EINVAL; return -1; }
#endif
    buf = calloc(1, (size_t)total);
    if (!buf) return -1;

    for (i = 0; i < count; i++) {
        uint64_t next;
        /* overhead at buf[i*block_size..+8] stays zero */
        if (i + 1 < count) {
            next = first_block + (i + 1) * block_size;
        } else {
#ifdef BSTACK_FEATURE_ATOMIC
            /* Placeholder: replaced with the old free_head by cross_exchange. */
            next = first_block;
#else
            next = old_head;
#endif
        }
        write_le64(buf + (size_t)(i * block_size) + 8, next);
    }

    if (bstack_set(bs, first_block, buf, (size_t)total) != 0) {
        free(buf); return -1;
    }
    free(buf);
    return 0;
}

/* ---- push_free_blocks -------------------------------------------------- */

/*
 * Prepend `count` contiguous blocks at `first_block` to the free list.
 * Clears their overhead bytes as part of the operation (transitions live →
 * free).  Does nothing if count == 0.
 *
 * With atomic the splice is a single bstack_cross_exchange on the last block's
 * next-pointer slot (data[0..8], at last_block + ALCK_OVERHEAD): it atomically
 * swaps that slot — currently the placeholder first_block — with free_head, so
 * free_head becomes first_block and the last block's next-pointer becomes the
 * old head, in one indivisible step.  For count == 1 the first and last block
 * coincide, mirroring slab_push_free_block.
 */
static int alck_push_free_blocks(bstack_t *bs, uint64_t first_block,
                                  uint64_t count, uint64_t block_size)
{
    if (count == 0) return 0;
    if (alck_write_free_run(bs, first_block, count, block_size) != 0) return -1;
#ifdef BSTACK_FEATURE_ATOMIC
    {
        uint64_t last_block = first_block + (count - 1) * block_size;
        return bstack_cross_exchange(bs, last_block + ALCK_OVERHEAD,
                                     ALCK_FREE_HEAD_OFFSET, 8);
    }
#else
    return alck_write_free_head(bs, first_block);
#endif
}

/* ---- pop_and_claim_block ----------------------------------------------- */

/*
 * Pop the head block from the free list, mark it in use with `num_blocks`,
 * zero the data region, and set *out_block_start to its payload offset.
 * Sets *out_block_start to ALCK_SENTINEL (0) if the list is empty.
 * Returns 0 on success, -1 on error.
 */
#ifdef BSTACK_FEATURE_ATOMIC
/*
 * Atomic pop: the read of free_head, the read of the popped block's overhead
 * and next-pointer (the 16-byte prefix), and the write that advances free_head
 * run as a single bstack_process_gen sequence under one held write lock — the
 * read-read-write is indivisible with respect to any other free-list operation
 * (mirrors slab_pop_free_block, plus the overhead-corruption check).  The
 * claim write (overhead + zeroed data) happens afterwards on the now-detached
 * block, which is exclusively owned by this call.
 */
struct alck_pop_ctx {
    uint8_t  head_buf[8];
    uint8_t  prefix_buf[16];
    uint64_t head;
    int      have_head;
    int      corrupt;
    int      step;
};

static int alck_pop_gen(bstack_gen_op_t *out_op, void *userctx)
{
    struct alck_pop_ctx *ctx = userctx;
    switch (ctx->step++) {
    case 0: /* read the current free-list head */
        out_op->kind          = BSTACK_GEN_READ;
        out_op->u.read.offset = ALCK_FREE_HEAD_OFFSET;
        out_op->u.read.buf    = ctx->head_buf;
        out_op->u.read.len    = 8;
        return 1;
    case 1: { /* empty list ends with no write; else read head's prefix */
        uint64_t head = read_le64(ctx->head_buf);
        if (head == ALCK_SENTINEL) return 0;
        ctx->head      = head;
        ctx->have_head = 1;
        out_op->kind          = BSTACK_GEN_READ;
        out_op->u.read.offset = head;
        out_op->u.read.buf    = ctx->prefix_buf;
        out_op->u.read.len    = 16;
        return 1;
    }
    case 2: { /* corrupt overhead ends with no write; else advance free_head */
        uint64_t overhead = read_le64(ctx->prefix_buf);
        if (overhead != 0) { ctx->corrupt = 1; return 0; }
        out_op->kind           = BSTACK_GEN_WRITE;
        out_op->u.write.offset = ALCK_FREE_HEAD_OFFSET;
        out_op->u.write.data   = ctx->prefix_buf + 8;
        out_op->u.write.len    = 8;
        return 1;
    }
    default:
        return 0;
    }
}

static int alck_pop_and_claim_block(bstack_t *bs, uint64_t num_blocks,
                                     uint64_t block_size,
                                     uint64_t *out_block_start)
{
    struct alck_pop_ctx ctx;
    uint8_t *block_buf;
    memset(&ctx, 0, sizeof ctx);

    if (bstack_process_gen(bs, alck_pop_gen, &ctx) != 0) return -1;
    if (!ctx.have_head) { *out_block_start = ALCK_SENTINEL; return 0; }
    if (ctx.corrupt) { errno = EINVAL; return -1; }

    /* Mark in-use and zero data in one bulk write on the detached block. */
#if UINT64_MAX > SIZE_MAX
    if (block_size > (uint64_t)SIZE_MAX) { errno = EINVAL; return -1; }
#endif
    block_buf = calloc(1, (size_t)block_size);
    if (!block_buf) return -1;
    write_le64(block_buf, ALCK_IN_USE_BIT | num_blocks);
    if (bstack_set(bs, ctx.head, block_buf, (size_t)block_size) != 0) {
        free(block_buf); return -1;
    }
    free(block_buf);
    *out_block_start = ctx.head;
    return 0;
}
#else /* !BSTACK_FEATURE_ATOMIC */
static int alck_pop_and_claim_block(bstack_t *bs, uint64_t num_blocks,
                                     uint64_t block_size,
                                     uint64_t *out_block_start)
{
    uint8_t  prefix[16], *block_buf;
    uint64_t head, overhead, next;

    if (alck_read_free_head(bs, &head) != 0) return -1;
    if (head == ALCK_SENTINEL) { *out_block_start = ALCK_SENTINEL; return 0; }

    if (bstack_get(bs, head, head + 16, prefix) != 0) return -1;
    overhead = read_le64(prefix);
    if (overhead != 0) { errno = EINVAL; return -1; }
    next = read_le64(prefix + 8);

    /* Advance free_head to the next block */
    if (alck_write_free_head(bs, next) != 0) return -1;

    /* Mark in-use and zero data in one bulk write */
#if UINT64_MAX > SIZE_MAX
    if (block_size > (uint64_t)SIZE_MAX) { errno = EINVAL; return -1; }
#endif
    block_buf = calloc(1, (size_t)block_size);
    if (!block_buf) return -1;
    write_le64(block_buf, ALCK_IN_USE_BIT | num_blocks);
    if (bstack_set(bs, head, block_buf, (size_t)block_size) != 0) {
        free(block_buf); return -1;
    }
    free(block_buf);
    *out_block_start = head;
    return 0;
}
#endif /* BSTACK_FEATURE_ATOMIC */

/* ---- recovery helpers -------------------------------------------------- */

/* These classification helpers serve the non-atomic sequential recover; the
 * atomic recover inlines the equivalent logic inside its process_gen scan. */
#ifndef BSTACK_FEATURE_ATOMIC

typedef enum {
    ALCK_CLASS_FREE,
    ALCK_CLASS_LEAKED,
    ALCK_CLASS_IN_USE,
    ALCK_CLASS_SUSPICIOUS
} alck_block_class_t;

/*
 * Classify the block at `p` for the recovery scan.
 * `in_use_count` receives the block-span if the class is ALCK_CLASS_IN_USE.
 */
static int alck_classify(bstack_t *bs, uint64_t p, uint64_t stack_len,
                          uint64_t block_size,
                          const uint64_t *free_arr, size_t free_cnt,
                          alck_block_class_t *out_class,
                          uint64_t *out_in_use_count)
{
    uint64_t overhead, n;
    size_t   lo, hi, mid;

    if (alck_read_overhead(bs, p, &overhead) != 0) return -1;

    *out_in_use_count = 0;

    if (overhead == 0) {
        /* Binary search in free_arr */
        int found = 0;
        lo = 0; hi = free_cnt;
        while (lo < hi) {
            mid = lo + (hi - lo) / 2;
            if      (free_arr[mid] < p) lo = mid + 1;
            else if (free_arr[mid] > p) hi = mid;
            else    { found = 1; break; }
        }
        *out_class = found ? ALCK_CLASS_FREE : ALCK_CLASS_LEAKED;
        return 0;
    }

    n = alck_valid_in_use(overhead, p, stack_len, block_size, free_arr, free_cnt);
    if (n > 0) {
        *out_class         = ALCK_CLASS_IN_USE;
        *out_in_use_count  = n;
    } else {
        *out_class = ALCK_CLASS_SUSPICIOUS;
    }
    return 0;
}

typedef enum {
    ALCK_RESYNC_RESYNC,
    ALCK_RESYNC_DISCARD_TAIL,
    ALCK_RESYNC_LEAVE_LEAKED
} alck_resync_outcome_t;

/*
 * Backward reachability pass over the region [p, stack_len).
 * Decides whether to resync at a later valid boundary (ALCK_RESYNC_RESYNC,
 * sets *out_resync_at), discard the orphaned tail (ALCK_RESYNC_DISCARD_TAIL),
 * or leave the region leaked (ALCK_RESYNC_LEAVE_LEAKED).
 */
static int alck_resync_tail(bstack_t *bs, uint64_t p, uint64_t stack_len,
                             uint64_t block_size,
                             const uint64_t *free_arr, size_t free_cnt,
                             alck_resync_outcome_t *out_outcome,
                             uint64_t *out_resync_at)
{
    uint64_t region_blocks;
    size_t   m, j;
    uint8_t *reach;

    *out_outcome   = ALCK_RESYNC_LEAVE_LEAKED;
    *out_resync_at = 0;

    region_blocks = (stack_len - p) / block_size;
    if (region_blocks > (uint64_t)ALCK_MAX_RECOVER_REGION
        || region_blocks > (uint64_t)SIZE_MAX) {
        return 0; /* LeaveLeaked */
    }
    m = (size_t)region_blocks;

    reach = calloc(m + 1, 1);
    if (!reach) return -1;
    reach[m] = 1;

    for (j = m; j > 0; j--) {
        uint64_t off = p + (uint64_t)(j - 1) * block_size;
        uint64_t overhead, n;
        if (alck_read_overhead(bs, off, &overhead) != 0) { free(reach); return -1; }
        if (overhead == 0) {
            reach[j - 1] = reach[j];
        } else {
            n = alck_valid_in_use(overhead, off, stack_len, block_size,
                                   free_arr, free_cnt);
            if (n > 0 && (j - 1) + (size_t)n <= m) {
                reach[j - 1] = reach[(j - 1) + (size_t)n];
            } else {
                reach[j - 1] = 0;
            }
        }
    }

    /* j=0 excluded (that's p itself, which is Suspicious) */
    for (j = 1; j < m; j++) {
        if (reach[j]) {
            *out_outcome   = ALCK_RESYNC_RESYNC;
            *out_resync_at = p + (uint64_t)j * block_size;
            free(reach);
            return 0;
        }
    }

    free(reach);
    *out_outcome = ALCK_RESYNC_DISCARD_TAIL;
    return 0;
}
#endif /* !BSTACK_FEATURE_ATOMIC */

/* ---- checked_slab_bstack_allocator_recover ----------------------------- */

#ifdef BSTACK_FEATURE_ATOMIC
/*
 * Atomic recovery: the free-list walk (with cycle detection), the linear arena
 * scan, and the single optional orphaned-tail discard run as ONE
 * bstack_process_gen sequence — every read happens under one held bstack write
 * lock, so a concurrent alloc/dealloc cannot mutate the free list between the
 * moment it is walked and the moment a block is classified against it.  The
 * scan is expressed as a pull-driven state machine: the gen callback contains
 * its own loop and only returns an op when it needs a Read/Len (or the
 * terminating discard); After the locked scan, each reclaimed leak is spliced
 * onto the free list lock-free (a leak is unreachable by alloc/dealloc, so its
 * state is stable across the gap, and the splice is itself an atomic
 * cross_exchange).
 *
 * The allocator Mutex is held for the whole call solely to make recovery
 * single-flight: two overlapping runs could each observe the same leak and
 * splice it in twice.  Ordinary alloc/dealloc/realloc never take it.
 */
enum alck_recover_st {
    ALCK_ST_READ_LEN,
    ALCK_ST_READ_FREE_HEAD,
    ALCK_ST_CONSUME_FREE_HEAD,
    ALCK_ST_CONSUME_NODE,
    ALCK_ST_CONSUME_ARENA,
    ALCK_ST_CONSUME_RESYNC,
    ALCK_ST_DONE
};

struct alck_recover_ctx {
    /* buffers filled by Len/Read ops */
    uint8_t    node_buf[16];
    uint8_t    head_buf[8];
    uint8_t    oh_buf[8];
    uint64_t   len_out;

    /* immutable inputs */
    uint64_t   block_size;

    /* scan cursors / authoritative size */
    uint64_t   stack_len;
    uint64_t   walk_head;   /* WALK_HEAD / CONSUME_NODE cursor */
    uint64_t   p;           /* ARENA_AT / CONSUME_ARENA cursor */

    /* free-list block offsets (sorted once the walk completes) */
    uint64_t  *free_arr;
    size_t     free_cnt, free_cap;
    uint64_t   max_blocks;  /* cycle-guard bound */

    /* backward reach DP over a suspicious tail region */
    uint8_t   *reach;
    size_t     reach_m;
    uint64_t   resync_p;
    size_t     rj;

    /* results consumed after the locked scan */
    uint64_t  *reclaim;
    size_t     reclaim_cnt, reclaim_cap;
    uint64_t   unsure;
    uint64_t   discard_from;

    int        free_corrupt;
    int        st;
};

/* Append to a grow-on-demand u64 array; returns -1 (errno=ENOMEM) on failure. */
static int alck_u64_push(uint64_t **arr, size_t *cnt, size_t *cap, uint64_t v)
{
    if (*cnt == *cap) {
        size_t    nc  = *cap ? *cap * 2 : 16;
        uint64_t *tmp = realloc(*arr, nc * sizeof *tmp);
        if (!tmp) { errno = ENOMEM; return -1; }
        *arr = tmp; *cap = nc;
    }
    (*arr)[(*cnt)++] = v;
    return 0;
}

/* Sort free_arr ascending (insertion sort — the list is typically short). */
static void alck_recover_sort_free(struct alck_recover_ctx *c)
{
    size_t i, j;
    for (i = 1; i < c->free_cnt; i++) {
        uint64_t key = c->free_arr[i];
        j = i;
        while (j > 0 && c->free_arr[j - 1] > key) {
            c->free_arr[j] = c->free_arr[j - 1];
            j--;
        }
        c->free_arr[j] = key;
    }
}

/* Binary search: is p present in the sorted free_arr? */
static int alck_recover_free_contains(const struct alck_recover_ctx *c, uint64_t p)
{
    size_t lo = 0, hi = c->free_cnt;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if      (c->free_arr[mid] < p) lo = mid + 1;
        else if (c->free_arr[mid] > p) hi = mid;
        else    return 1;
    }
    return 0;
}

/* First index in sorted free_arr whose offset is > p (== free_cnt if none). */
static size_t alck_recover_free_upper(const struct alck_recover_ctx *c, uint64_t p)
{
    size_t lo = 0, hi = c->free_cnt, idx = c->free_cnt;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (c->free_arr[mid] <= p) lo = mid + 1;
        else { idx = mid; hi = mid; }
    }
    return idx;
}

static int alck_recover_gen(bstack_gen_op_t *out_op, void *userctx)
{
    struct alck_recover_ctx *c = userctx;
    uint64_t bs_sz = c->block_size;

    switch (c->st) {
    /* Read the authoritative payload size first. */
    case ALCK_ST_READ_LEN:
        c->st = ALCK_ST_READ_FREE_HEAD;
        out_op->kind      = BSTACK_GEN_LEN;
        out_op->u.len.out = &c->len_out;
        return 1;

    case ALCK_ST_READ_FREE_HEAD:
        c->stack_len = c->len_out;
        if (c->stack_len <= ALCK_ARENA_START) return 0;
        c->max_blocks = (c->stack_len - ALCK_ARENA_START) / bs_sz;
        c->st = ALCK_ST_CONSUME_FREE_HEAD;
        out_op->kind          = BSTACK_GEN_READ;
        out_op->u.read.offset = ALCK_FREE_HEAD_OFFSET;
        out_op->u.read.buf    = c->head_buf;
        out_op->u.read.len    = 8;
        return 1;

    /* Free-list walk: read and validate the head, then read its node. */
    case ALCK_ST_CONSUME_FREE_HEAD: {
        c->walk_head = read_le64(c->head_buf);
    list_walk_head:
        if (c->walk_head == ALCK_SENTINEL) goto list_walk_done;
        if (c->walk_head < ALCK_ARENA_START || (c->walk_head - ALCK_ARENA_START) % bs_sz != 0
            || c->walk_head >= c->stack_len
            || (uint64_t)c->free_cnt > c->max_blocks /* cycle guard */) {
            goto list_walk_corrupt;
        }
        c->st = ALCK_ST_CONSUME_NODE;
        out_op->kind          = BSTACK_GEN_READ;
        out_op->u.read.offset = c->walk_head;
        out_op->u.read.buf    = c->node_buf;
        out_op->u.read.len    = 16;
        return 1;
    }

    case ALCK_ST_CONSUME_NODE: {
        if (read_le64(c->node_buf) == 0) {
            if (alck_u64_push(&c->free_arr, &c->free_cnt,
                              &c->free_cap, c->walk_head) != 0){
                return -1;
            }
            c->walk_head = read_le64(c->node_buf + 8);
            goto list_walk_head;
        } else {
        list_walk_corrupt:
            c->free_corrupt = 1;
        list_walk_done:
            alck_recover_sort_free(c);
            c->p = ALCK_ARENA_START;
            /* Fall through to arena scan */
        }

    /* Linear arena scan: read the overhead at the cursor. */
    arena_scan:
        if (c->p >= c->stack_len) return 0;
        c->st = ALCK_ST_CONSUME_ARENA;
        out_op->kind          = BSTACK_GEN_READ;
        out_op->u.read.offset = c->p;
        out_op->u.read.buf    = c->oh_buf;
        out_op->u.read.len    = 8;
        return 1;
    }

    case ALCK_ST_CONSUME_ARENA: {
        uint64_t overhead = read_le64(c->oh_buf);
        uint64_t n, span, m64;
        size_t   m, idx;

        if (overhead == 0) {
            if (alck_recover_free_contains(c, c->p)) {
                /* Free: reachable from free_head. */
            } else if (c->free_corrupt) {
                /* A bare zero-overhead block is only trustworthy as a leak
                 * while the free list walked cleanly. */
                c->unsure ++;
            } else {
                if (alck_u64_push(&c->reclaim, &c->reclaim_cnt,
                                  &c->reclaim_cap, c->p) != 0) return -1;
            }
            c->p += bs_sz;
            goto arena_scan;
        }
        n = alck_valid_in_use(overhead, c->p, c->stack_len, bs_sz,
                              c->free_arr, c->free_cnt);
        if (n > 0) {
            c->p += n * bs_sz;  /* valid_in_use bounds p + n*bs <= stack_len */
            goto arena_scan;
        }
        /* Suspicious: prefer a known-free block as resync anchor. */
        idx = alck_recover_free_upper(c, c->p);
        if (idx < c->free_cnt) {
            uint64_t f = c->free_arr[idx];
            c->unsure += (f - c->p) / bs_sz;
            c->p = f;
            goto arena_scan;
        }
        /* No anchor follows: set up the backward reach DP over
         * [p, stack_len) (see the non-atomic alck_resync_tail). */
        span = c->stack_len - c->p;
        m64  = span / bs_sz;
        if (m64 > (uint64_t)ALCK_MAX_RECOVER_REGION
            || m64 > (uint64_t)SIZE_MAX) {
            /* Region too large to analyse safely: leave leaked. */
            c->unsure += span / bs_sz;
            return 0;
        }
        m = (size_t)m64;  /* m >= 1 since p < stack_len and both are aligned */
        free(c->reach);
        c->reach = calloc(m + 1, 1);
        if (!c->reach) { errno = ENOMEM; return -1; }
        c->reach[m] = 1;
        c->reach_m  = m;
        c->resync_p = c->p;
        c->rj       = m - 1;
        c->st = ALCK_ST_CONSUME_RESYNC;
        out_op->kind          = BSTACK_GEN_READ;
        out_op->u.read.offset = c->resync_p + (uint64_t)c->rj * bs_sz;
        out_op->u.read.buf    = c->oh_buf;
        out_op->u.read.len    = 8;
        return 1;
    }

    { /* Resync DP */
        size_t   jr; uint64_t overhead;
    dp_found:
        c->unsure += (uint64_t)jr;
        c->p = c->resync_p + (uint64_t)jr * bs_sz;
        goto arena_scan;

    case ALCK_ST_CONSUME_RESYNC:
        overhead = read_le64(c->oh_buf);
        uint64_t off = c->resync_p + (uint64_t)c->rj * bs_sz;

        if (overhead == 0) {
            c->reach[c->rj] = c->reach[c->rj + 1];
        } else {
            uint64_t n = alck_valid_in_use(overhead, off, c->stack_len,
                                           bs_sz, c->free_arr, c->free_cnt);
            if (n > 0 && c->rj + (size_t)n <= c->reach_m) {
                c->reach[c->rj] = c->reach[c->rj + (size_t)n];
            } else {
                c->reach[c->rj] = 0;
            }
        }
        if (c->rj != 0) {
            c->rj -= 1;
            c->st = ALCK_ST_CONSUME_RESYNC;
            out_op->kind          = BSTACK_GEN_READ;
            out_op->u.read.offset = c->resync_p + (uint64_t)c->rj * bs_sz;
            out_op->u.read.buf    = c->oh_buf;
            out_op->u.read.len    = 8;
            return 1;
        }
        /* DP complete.  The smallest interior boundary that tiles cleanly to
         * the tail is a mid-arena gap to resync on; j=0 is excluded (it
         * would contradict the Suspicious classification that got us here). */
        for (jr = 1; jr < c->reach_m; jr++) if (c->reach[jr]) goto dp_found;
        /* Orphaned tail.  Only safe to discard when the free list is
         * trusted; otherwise leave it leaked. */
        if (c->free_corrupt) {
            c->unsure += (c->stack_len - c->resync_p) / bs_sz;
            return 0; /* no write */
        }
    }

        /* Emit the single permitted mutation: the orphaned-tail discard, as a
         * NULL-buffer Pop (drops the bytes without reading them back). */
        c->discard_from = c->resync_p;
        c->st = ALCK_ST_DONE;
        {
            uint64_t dlen = c->stack_len - c->discard_from;
#if UINT64_MAX > SIZE_MAX
            if (dlen > (uint64_t)SIZE_MAX) { errno = EINVAL; return -1; }
#endif
            out_op->kind      = BSTACK_GEN_POP;
            out_op->u.pop.buf = NULL;
            out_op->u.pop.len = (size_t)dlen;
        }
        return 1;

    case ALCK_ST_DONE:
    default:
        return 0;
    }
}

int checked_slab_bstack_allocator_recover(checked_slab_bstack_allocator_t *alloc,
                                           uint64_t *out_unsure)
{
    bstack_t               *bs = alloc->bs;
    struct alck_recover_ctx c;
    uint64_t                stack_len;
    size_t                  i;
    int                     ret = 0;

    /* Held for the entire call.  Phase-1 reads are serialised against
     * alloc/dealloc by the bstack write lock inside process_gen; this lock
     * instead prevents two recover runs from overlapping (which would let both
     * reclaim the same leaked block and double-link it). */
    MUTEX_LOCK(alloc);

    /* Cheap early-out hint; the authoritative size is read under the
     * process_gen lock below via BSTACK_GEN_LEN. */
    if (bstack_len(bs, &stack_len) != 0) { MUTEX_UNLOCK(alloc); return -1; }
    if (stack_len <= ALCK_ARENA_START) {
        if (out_unsure) *out_unsure = 0;
        MUTEX_UNLOCK(alloc); return 0;
    }

    memset(&c, 0, sizeof c);
    c.block_size = alloc->block_size;
    c.st         = ALCK_ST_READ_LEN;

    if (bstack_process_gen(bs, alck_recover_gen, &c) != 0) {
        ret = -1; goto done;
    }

    /* Phase 2: splice reclaimed leaks onto the free list, lock-free.  Each is
     * unreachable by alloc/dealloc, so its leaked state is stable across the
     * unlocked gap, and alck_push_free_blocks splices atomically. */
    for (i = 0; i < c.reclaim_cnt; i++) {
        if (alck_push_free_blocks(bs, c.reclaim[i], 1, alloc->block_size) != 0) {
            ret = -1; goto done;
        }
    }
    if (out_unsure) *out_unsure = c.unsure;

done:
    free(c.free_arr);
    free(c.reclaim);
    free(c.reach);
    MUTEX_UNLOCK(alloc);
    return ret;
}
#else /* !BSTACK_FEATURE_ATOMIC */
int checked_slab_bstack_allocator_recover(checked_slab_bstack_allocator_t *alloc,
                                           uint64_t *out_unsure)
{
    bstack_t  *bs         = alloc->bs;
    uint64_t   block_size = alloc->block_size;
    uint64_t   stack_len;
    uint64_t  *free_arr   = NULL;
    size_t     free_cnt   = 0;
    int        free_corrupt;
    uint64_t  *reclaim    = NULL;
    size_t     reclaim_cnt = 0, reclaim_cap = 0;
    uint64_t   unsure     = 0;
    uint64_t   tailcut    = ALCK_SENTINEL;
    uint64_t   p;
    size_t     i;
    int        ret        = 0;

    MUTEX_LOCK(alloc);

    if (bstack_len(bs, &stack_len) != 0) { MUTEX_UNLOCK(alloc); return -1; }
    if (stack_len <= ALCK_ARENA_START) {
        if (out_unsure) *out_unsure = 0;
        MUTEX_UNLOCK(alloc); return 0;
    }

    if (alck_scan_free_list(bs, stack_len, block_size,
                             &free_arr, &free_cnt, &free_corrupt) != 0) {
        MUTEX_UNLOCK(alloc); return -1;
    }

    p = ALCK_ARENA_START;
    while (p < stack_len) {
        alck_block_class_t cls;
        uint64_t           in_use_n = 0;

        if (alck_classify(bs, p, stack_len, block_size,
                           free_arr, free_cnt, &cls, &in_use_n) != 0) {
            ret = -1; goto done;
        }

        switch (cls) {
        case ALCK_CLASS_FREE:
            p += block_size;
            break;

        case ALCK_CLASS_LEAKED:
            if (free_corrupt) {
                unsure++;
            } else {
                if (reclaim_cnt == reclaim_cap) {
                    size_t    nc  = reclaim_cap ? reclaim_cap * 2 : 16;
                    uint64_t *tmp = realloc(reclaim, nc * sizeof *tmp);
                    if (!tmp) { ret = -1; goto done; }
                    reclaim = tmp; reclaim_cap = nc;
                }
                reclaim[reclaim_cnt++] = p;
            }
            p += block_size;
            break;

        case ALCK_CLASS_IN_USE:
            p += in_use_n * block_size;
            break;

        case ALCK_CLASS_SUSPICIOUS: {
            /* Prefer a known-free block as a reliable resync anchor */
            size_t   lo = 0, hi = free_cnt, idx = free_cnt;
            uint64_t anchor;
            while (lo < hi) {
                size_t mid = lo + (hi - lo) / 2;
                if (free_arr[mid] <= p) lo = mid + 1;
                else { idx = mid; hi = mid; }
            }
            if (idx < free_cnt) {
                anchor = free_arr[idx];
                unsure += (anchor - p) / block_size;
                p = anchor;
            } else {
                alck_resync_outcome_t outcome;
                uint64_t resync_at = 0;
                if (alck_resync_tail(bs, p, stack_len, block_size,
                                      free_arr, free_cnt,
                                      &outcome, &resync_at) != 0) {
                    ret = -1; goto done;
                }
                switch (outcome) {
                case ALCK_RESYNC_RESYNC:
                    unsure += (resync_at - p) / block_size;
                    p = resync_at;
                    break;
                case ALCK_RESYNC_DISCARD_TAIL:
                    if (free_corrupt) {
                        unsure += (stack_len - p) / block_size;
                    } else {
                        tailcut = p;
                    }
                    goto end_scan;
                case ALCK_RESYNC_LEAVE_LEAKED:
                    unsure += (stack_len - p) / block_size;
                    goto end_scan;
                }
            }
            break;
        }
        }
    }
end_scan:

    if (tailcut != ALCK_SENTINEL) {
        uint64_t discard_n = stack_len - tailcut;
#if UINT64_MAX > SIZE_MAX
        if (discard_n > (uint64_t)SIZE_MAX) { ret = -1; goto done; }
#endif
        if (bstack_discard(bs, (size_t)discard_n) != 0) { ret = -1; goto done; }
    }
    for (i = 0; i < reclaim_cnt; i++) {
        if (alck_push_free_blocks(bs, reclaim[i], 1, block_size) != 0) {
            ret = -1; goto done;
        }
    }
    if (out_unsure) *out_unsure = unsure;

done:
    free(free_arr);
    free(reclaim);
    MUTEX_UNLOCK(alloc);
    return ret;
}
#endif /* BSTACK_FEATURE_ATOMIC */

/* ---- vtable implementations -------------------------------------------- */

static bstack_t *alck_vt_stack(bstack_allocator_t *base)
{
    return ((checked_slab_bstack_allocator_t *)base)->bs;
}

static int alck_vt_alloc(bstack_allocator_t *base, uint64_t len,
                          bstack_slice_t *out)
{
    checked_slab_bstack_allocator_t *a = (checked_slab_bstack_allocator_t *)base;
    uint64_t num_blocks;

    if (len == 0) {
        out->allocator = base; out->offset = 0; out->len = 0;
        return 0;
    }

    num_blocks = alck_blocks_needed(len, a->block_size);
    if (num_blocks == UINT64_MAX) { errno = EINVAL; return -1; } /* overflow */

    if (num_blocks == 1) {
        /* Free-list pop is lock-free under atomic (single process_gen
         * sequence); single-threaded otherwise.  The tail-extend fallback below
         * uses bstack_extend, which is internally serialised and returns a
         * distinct region to each concurrent caller. */
        uint64_t block_start;
        if (alck_pop_and_claim_block(a->bs, 1, a->block_size, &block_start) != 0)
            return -1;
        if (block_start != ALCK_SENTINEL) {
            out->allocator = base;
            out->offset    = block_start + ALCK_OVERHEAD;
            out->len       = len;
            return 0;
        }
        /* Free list empty: extend tail */
        if (bstack_extend(a->bs, (size_t)a->block_size, &block_start) != 0)
            return -1;
        if (alck_write_overhead(a->bs, block_start, ALCK_IN_USE_BIT | 1) != 0)
            return -1;
        out->allocator = base;
        out->offset    = block_start + ALCK_OVERHEAD;
        out->len       = len;
        return 0;
    }

    /* Multi-block: always extend tail */
    {
        uint64_t total, block_start;
        if (num_blocks > UINT64_MAX / a->block_size) { errno = EINVAL; return -1; }
        total = num_blocks * a->block_size;
#if UINT64_MAX > SIZE_MAX
        if (total > (uint64_t)SIZE_MAX) { errno = EINVAL; return -1; }
#endif
        if (bstack_extend(a->bs, (size_t)total, &block_start) != 0) return -1;
        if (alck_write_overhead(a->bs, block_start,
                                ALCK_IN_USE_BIT | num_blocks) != 0) return -1;
        out->allocator = base;
        out->offset    = block_start + ALCK_OVERHEAD;
        out->len       = len;
        return 0;
    }
}

static int alck_vt_dealloc(bstack_allocator_t *base, bstack_slice_t s)
{
    checked_slab_bstack_allocator_t *a = (checked_slab_bstack_allocator_t *)base;
    uint64_t block_start, overhead, num_blocks, backing, slice_end;

    if (s.len == 0 && s.offset == 0) return 0;

    if (s.offset < ALCK_OVERHEAD) { errno = EINVAL; return -1; }
    block_start = s.offset - ALCK_OVERHEAD;

    /* read_overhead is a single bstack read from a block owned by the caller;
     * no lock required here. */
    if (alck_read_overhead(a->bs, block_start, &overhead) != 0) return -1;
    if ((overhead & ALCK_IN_USE_BIT) == 0) { errno = EINVAL; return -1; }

    num_blocks = overhead & ALCK_BLOCKS_MASK;
    if (num_blocks == 0) { errno = EINVAL; return -1; }

    if (num_blocks > UINT64_MAX / a->block_size) { errno = EINVAL; return -1; }
    backing = num_blocks * a->block_size;

    if (block_start > UINT64_MAX - backing) { errno = EINVAL; return -1; }
    slice_end = block_start + backing;

#if UINT64_MAX > SIZE_MAX
    if (backing > (uint64_t)SIZE_MAX) { errno = EINVAL; return -1; }
#endif

    /* Tail path: try_discard atomically checks tail == slice_end and removes
     * backing bytes under bstack's own write lock — no allocator lock needed. */
#ifdef BSTACK_FEATURE_ATOMIC
    {
        int ok = 0;
        if (bstack_try_discard(a->bs, slice_end, (size_t)backing, &ok) != 0)
            return -1;
        if (ok) return 0;
    }
#else
    {
        uint64_t tail;
        if (bstack_len(a->bs, &tail) != 0) return -1;
        if (slice_end == tail)
            return bstack_discard(a->bs, (size_t)backing);
    }
#endif

    /* Not at tail: push to the free list (lock-free under atomic via
     * cross_exchange; single-threaded otherwise). This mutates the block
     * overhead and multiple free-list links, so a mid-way failure may leave
     * the blocks partially freed — the handle can no longer be safely
     * returned (-2). */
    return alck_push_free_blocks(a->bs, block_start, num_blocks, a->block_size) == 0 ? 0 : -2;
}

static int alck_vt_realloc(bstack_allocator_t *base, bstack_slice_t s,
                             uint64_t new_len, bstack_slice_t *out)
{
    checked_slab_bstack_allocator_t *a = (checked_slab_bstack_allocator_t *)base;
    uint64_t block_start, overhead, old_n, new_n, old_backing, new_backing;
    /* tail and is_tail are now computed per-path in inner scopes */
    /*
     * The surviving allocation to hand back on failure. Starts as the
     * original block, becomes the new region once a non-tail grow commits
     * it, and becomes the shrunk view once the overhead commit point is
     * written (see below). Almost every failure path leaves `recovered`
     * valid and fails with -1 (survived); the sole exception is the non-tail
     * shrink window after the excess has been scrubbed but before the smaller
     * count is committed, where the region is neither cleanly old nor new and
     * is reported lost (-2).
     */
    bstack_slice_t recovered = s;
    recovered.allocator = base;

    if (s.len == 0 && s.offset == 0) {
        if (alck_vt_alloc(base, new_len, out) != 0) {
            out->allocator = base; out->offset = 0; out->len = 0;
            return -1;
        }
        return 0;
    }

    if (new_len == 0) {
        /* dealloc consumes `s`; propagate its own survivor signal unchanged
         * — on -1 it hands back exactly the original slice. */
        int dr = alck_vt_dealloc(base, s);
        if (dr != 0) {
            if (dr == -1) *out = recovered;
            return dr;
        }
        out->allocator = base; out->offset = 0; out->len = 0;
        return 0;
    }

    if (new_len == s.len) { *out = s; return 0; }

    if (s.offset < ALCK_OVERHEAD) { errno = EINVAL; *out = recovered; return -1; }
    block_start = s.offset - ALCK_OVERHEAD;

    if (alck_read_overhead(a->bs, block_start, &overhead) != 0) { *out = recovered; return -1; }
    if ((overhead & ALCK_IN_USE_BIT) == 0) { errno = EINVAL; *out = recovered; return -1; }

    old_n = overhead & ALCK_BLOCKS_MASK;
    if (old_n == 0) { errno = EINVAL; *out = recovered; return -1; }

    new_n = alck_blocks_needed(new_len, a->block_size);
    if (new_n == UINT64_MAX) { errno = EINVAL; *out = recovered; return -1; }

    if (old_n == new_n) {
        /* Same backing blocks: zero newly-exposed bytes on grow */
        if (new_len > s.len) {
            uint64_t to_zero = new_len - s.len;
#if UINT64_MAX > SIZE_MAX
            if (to_zero > (uint64_t)SIZE_MAX) { errno = EINVAL; *out = recovered; return -1; }
#endif
            if (bstack_zero(a->bs, s.offset + s.len, (size_t)to_zero) != 0) {
                *out = recovered;
                return -1;
            }
        }
        out->allocator = base; out->offset = s.offset; out->len = new_len;
        return 0;
    }

    if (old_n > UINT64_MAX / a->block_size) { errno = EINVAL; *out = recovered; return -1; }
    old_backing = old_n * a->block_size;
    if (new_n > UINT64_MAX / a->block_size) { errno = EINVAL; *out = recovered; return -1; }
    new_backing = new_n * a->block_size;
    if (block_start > UINT64_MAX - old_backing) { errno = EINVAL; *out = recovered; return -1; }

    /* Precompute the expected tail for this allocation (sentinel). */
    {
        uint64_t sentinel = block_start + old_backing;

        if (new_n > old_n) {
            /* Grow path.
             * With atomic: try_extend_zeros atomically checks tail == sentinel
             * and appends the delta under bstack's write lock — no allocator
             * lock needed.  write_overhead then writes only to the exclusively-
             * owned newly-extended region.  If try_extend_zeros returns false
             * the tail has moved and we are no longer the tail block — fall
             * through to grow non-tail.
             * Without atomic: plain len() check then extend (single-threaded). */
            uint64_t delta = new_backing - old_backing;
            int tail_done = 0;
#if UINT64_MAX > SIZE_MAX
            if (delta > (uint64_t)SIZE_MAX) { errno = EINVAL; *out = recovered; return -1; }
#endif

#ifdef BSTACK_FEATURE_ATOMIC
            {
                int ok = 0;
                if (bstack_try_extend_zeros(a->bs, sentinel,
                                            (size_t)delta, &ok) != 0) {
                    *out = recovered;
                    return -1;
                }
                if (ok) tail_done = 1;
            }
#else
            {
                uint64_t cur_tail;
                if (bstack_len(a->bs, &cur_tail) != 0) { *out = recovered; return -1; }
                if (sentinel == cur_tail) {
                    if (bstack_extend(a->bs, (size_t)delta, NULL) != 0) {
                        *out = recovered;
                        return -1;
                    }
                    tail_done = 1;
                }
            }
#endif

            if (tail_done) {
                if (new_len > s.len) {
                    uint64_t to_zero = new_len - s.len;
#if UINT64_MAX > SIZE_MAX
                    if (to_zero > (uint64_t)SIZE_MAX) { errno = EINVAL; *out = recovered; return -1; }
#endif
                    if (bstack_zero(a->bs, s.offset + s.len,
                                    (size_t)to_zero) != 0) {
                        *out = recovered;
                        return -1;
                    }
                }
                if (alck_write_overhead(a->bs, block_start,
                                        ALCK_IN_USE_BIT | new_n) != 0) {
                    *out = recovered;
                    return -1;
                }
                out->allocator = base; out->offset = s.offset; out->len = new_len;
                return 0;
            }

            /* Not at tail (or tail moved under atomic): grow non-tail.
             * alloc and dealloc each handle their own free-list and tail
             * operations independently. */
            {
                bstack_slice_t new_s;
                uint8_t       *tmp;
                uint64_t       copy_len;

                if (alck_vt_alloc(base, new_len, &new_s) != 0) {
                    *out = recovered;
                    return -1;
                }

                copy_len = s.len < new_len ? s.len : new_len;
                if (copy_len > 0) {
#if UINT64_MAX > SIZE_MAX
                    if (copy_len > (uint64_t)SIZE_MAX) {
                        errno = EINVAL;
                        alck_vt_dealloc(base, new_s); /* best-effort rollback; original untouched either way */
                        *out = recovered;
                        return -1;
                    }
#endif
                    tmp = malloc((size_t)copy_len);
                    if (!tmp) {
                        alck_vt_dealloc(base, new_s);
                        *out = recovered;
                        return -1;
                    }
                    if (bstack_get(a->bs, s.offset, s.offset + copy_len, tmp) != 0
                        || bstack_set(a->bs, new_s.offset, tmp,
                                      (size_t)copy_len) != 0) {
                        free(tmp);
                        /* The new region was allocated but the copy failed. Roll
                         * it back (best-effort) so it is not leaked; the
                         * original still holds the data untouched, so it
                         * remains the survivor regardless of whether this
                         * rollback itself succeeds. */
                        alck_vt_dealloc(base, new_s);
                        *out = recovered;
                        return -1;
                    }
                    free(tmp);
                }

                /* New region committed and populated; it is now the survivor,
                 * so a failure freeing the old block returns the new region
                 * instead (the old block leaks until crash recovery). */
                recovered = new_s;
                if (alck_vt_dealloc(base, s) != 0) {
                    *out = recovered;
                    return -1;
                }
                *out = new_s;
                return 0;
            }
        }

        /* Shrink path (new_n < old_n).  Lock-free under atomic: the overhead
         * write is the commit point, try_discard is atomic, and the non-tail
         * splice rides cross_exchange.  Single-threaded otherwise. */
        {
            uint64_t delta = old_backing - new_backing;
            uint64_t excess_start, excess_count = old_n - new_n;
#if UINT64_MAX > SIZE_MAX
            if (delta > (uint64_t)SIZE_MAX) { errno = EINVAL; *out = recovered; return -1; }
#endif
            if (new_n > UINT64_MAX / a->block_size) { errno = EINVAL; *out = recovered; return -1; }
            excess_start = block_start + new_n * a->block_size;

            /* Non-atomic tail fast path: commit the smaller count, then discard
             * the tail.  Commit-first is safe here — a crash before the discard
             * leaves an orphaned tail past new_n that recover() reclaims, and
             * nothing live follows the arena tail — and it avoids scrubbing
             * bytes that are about to be truncated away. */
#ifndef BSTACK_FEATURE_ATOMIC
            {
                uint64_t cur_tail;
                if (bstack_len(a->bs, &cur_tail) != 0) { *out = recovered; return -1; }
                if (sentinel == cur_tail) {
                    if (alck_write_overhead(a->bs, block_start,
                                            ALCK_IN_USE_BIT | new_n) != 0) {
                        *out = recovered;
                        return -1;
                    }
                    recovered.allocator = base;
                    recovered.offset    = s.offset;
                    recovered.len       = new_len;
                    if (bstack_discard(a->bs, (size_t)delta) != 0) {
                        *out = recovered;
                        return -1;
                    }
                    out->allocator = base; out->offset = s.offset;
                    out->len = new_len;
                    return 0;
                }
            }
#endif

            /* General shrink (non-tail, and every atomic shrink).
             *
             * Crash-order matters: the excess blocks must never be left holding
             * the allocation's stale payload once the header records the smaller
             * count.  Such bytes can mimic in-use overhead words and desync
             * recover()'s linear scan, which would then stride over and reclaim
             * an unrelated live allocation, corrupting it.  So the excess is
             * scrubbed to a clean, zero-overhead free run BEFORE the header is
             * shrunk:
             *
             *   1. write_free_run clears every excess overhead + payload byte and
             *      links the run internally (free_head untouched).  A fault here
             *      leaves the excess untouched and the original allocation whole,
             *      so it survives with the intact original (-1).
             *   2. Between the scrub and the commit the region is neither cleanly
             *      old (tail scrubbed) nor cleanly new (header still old_n); a
             *      fault here reports the region lost (-2).  The old header still
             *      tiles cleanly for recovery, so it is a leak, never corruption.
             *   3. write_overhead commits the smaller count — the shrunk view is
             *      now the survivor.  A fault before the splice leaves the
             *      scrubbed excess as zero-overhead leaked blocks that recover()
             *      reclaims one by one, staying aligned.
             *   4. Publish the run onto free_head (or discard it when it is the
             *      arena tail, on the atomic path). */
            if (alck_write_free_run(a->bs, excess_start, excess_count,
                                    a->block_size) != 0) {
                *out = recovered;
                return -1;
            }
            /* Excess scrubbed, count not yet shrunk: a failure now loses the
             * region (-2) rather than handing back torn contents. */
            if (alck_write_overhead(a->bs, block_start,
                                    ALCK_IN_USE_BIT | new_n) != 0) {
                return -2;
            }
            recovered.allocator = base;
            recovered.offset    = s.offset;
            recovered.len       = new_len;

#ifdef BSTACK_FEATURE_ATOMIC
            {
                /* Discard when the scrubbed excess is the tail; otherwise splice
                 * the pre-built run onto free_head with one cross_exchange. */
                int ok = 0;
                if (bstack_try_discard(a->bs, sentinel, (size_t)delta, &ok) != 0) {
                    *out = recovered;
                    return -1;
                }
                if (!ok) {
                    uint64_t last_block =
                        excess_start + (excess_count - 1) * a->block_size;
                    if (bstack_cross_exchange(a->bs, last_block + ALCK_OVERHEAD,
                                              ALCK_FREE_HEAD_OFFSET, 8) != 0) {
                        *out = recovered;
                        return -1;
                    }
                }
            }
#else
            /* Non-tail (tail handled above): the run already points at the old
             * head, so publishing its first block as the new head splices it. */
            if (alck_write_free_head(a->bs, excess_start) != 0) {
                *out = recovered;
                return -1;
            }
#endif
            out->allocator = base; out->offset = s.offset; out->len = new_len;
            return 0;
        }
    }
}

static const bstack_allocator_vtbl_t alck_allocator_vtbl = {
    alck_vt_stack,
    alck_vt_alloc,
    alck_vt_realloc,
    alck_vt_dealloc,
};

/* ---- public API -------------------------------------------------------- */

checked_slab_bstack_allocator_t *checked_slab_bstack_allocator_new(
    bstack_t *bs, uint64_t data_size)
{
    checked_slab_bstack_allocator_t *a;
    uint64_t block_size;
    int      is_empty;
    uint8_t  hdr[ALCK_ARENA_START];

    if (bstack_is_empty(bs, &is_empty) != 0) return NULL;
    if (!is_empty) { errno = EINVAL; return NULL; }

    if (data_size < ALCK_MIN_DATA_SIZE) { errno = EINVAL; return NULL; }
    if (data_size > UINT64_MAX - ALCK_OVERHEAD) { errno = EINVAL; return NULL; }
    block_size = data_size + ALCK_OVERHEAD;

#if UINT64_MAX > SIZE_MAX
    if (block_size > (uint64_t)SIZE_MAX) { errno = EINVAL; return NULL; }
#endif

    a = malloc(sizeof *a);
    if (!a) { errno = ENOMEM; return NULL; }

#ifdef BSTACK_FEATURE_ATOMIC
    if (bstack_alloc_lock_init(&a->lock) != 0) { free(a); return NULL; }
#endif

    memset(hdr, 0, sizeof hdr);
    memcpy(hdr + (size_t)ALCK_OFFSET_SIZE, alck_magic, 8);
    write_le64(hdr + (size_t)ALCK_OFFSET_SIZE + 8, block_size);
    /* free_head at ALCK_FREE_HEAD_OFFSET stays 0 (ALCK_SENTINEL) */

    if (bstack_push(bs, hdr, sizeof hdr, NULL) != 0) {
#ifdef BSTACK_FEATURE_ATOMIC
        bstack_alloc_lock_destroy(a->lock);
#endif
        free(a); return NULL;
    }

    a->base.vtbl      = &alck_allocator_vtbl;
    a->base.bulk_vtbl = NULL;
    a->bs             = bs;
    a->block_size     = block_size;
    return a;
}

checked_slab_bstack_allocator_t *checked_slab_bstack_allocator_open(bstack_t *bs)
{
    checked_slab_bstack_allocator_t *a;
    int      is_empty;
    uint64_t stack_len, stored_block_size, stored_free_head, arena_bytes;
    uint8_t  header[ALCK_HEADER_SIZE];

    if (bstack_is_empty(bs, &is_empty) != 0) return NULL;
    if (is_empty) { errno = EINVAL; return NULL; }

    if (bstack_len(bs, &stack_len) != 0) return NULL;
    if (stack_len < ALCK_ARENA_START) { errno = EINVAL; return NULL; }

    if (bstack_get(bs, ALCK_OFFSET_SIZE,
                   ALCK_OFFSET_SIZE + ALCK_HEADER_SIZE, header) != 0)
        return NULL;

    if (memcmp(header, alck_magic_prefix, sizeof alck_magic_prefix) != 0) {
        errno = EINVAL; return NULL;
    }

    stored_block_size = read_le64(header + 8);
    if (stored_block_size < ALCK_MIN_BLOCK_SIZE) { errno = EINVAL; return NULL; }
#if UINT64_MAX > SIZE_MAX
    if (stored_block_size > (uint64_t)SIZE_MAX) { errno = EINVAL; return NULL; }
#endif

    stored_free_head = read_le64(header + 16);
    if (stored_free_head != ALCK_SENTINEL) {
        if (stored_free_head < ALCK_ARENA_START
            || (stored_free_head - ALCK_ARENA_START) % stored_block_size != 0
            || stored_free_head >= stack_len) {
            errno = EINVAL; return NULL;
        }
    }

    arena_bytes = stack_len - ALCK_ARENA_START;
    if (arena_bytes % stored_block_size != 0) { errno = EINVAL; return NULL; }

    /* Verify free-list head is itself a free block */
    if (stored_free_head != ALCK_SENTINEL) {
        uint8_t prefix[8];
        if (bstack_get(bs, stored_free_head,
                       stored_free_head + 8, prefix) != 0) return NULL;
        if (read_le64(prefix) != 0) { errno = EINVAL; return NULL; }
    }

    a = malloc(sizeof *a);
    if (!a) { errno = ENOMEM; return NULL; }

#ifdef BSTACK_FEATURE_ATOMIC
    if (bstack_alloc_lock_init(&a->lock) != 0) { free(a); return NULL; }
#endif

    a->base.vtbl      = &alck_allocator_vtbl;
    a->base.bulk_vtbl = NULL;
    a->bs             = bs;
    a->block_size     = stored_block_size;

    /* Auto-recover: reclaim leaked blocks and repair orphaned tails */
    if (checked_slab_bstack_allocator_recover(a, NULL) != 0) {
#ifdef BSTACK_FEATURE_ATOMIC
        bstack_alloc_lock_destroy(a->lock);
#endif
        free(a); return NULL;
    }
    return a;
}

void checked_slab_bstack_allocator_free(checked_slab_bstack_allocator_t *alloc)
{
#ifdef BSTACK_FEATURE_ATOMIC
    bstack_alloc_lock_destroy(alloc->lock);
#endif
    free(alloc);
}

bstack_t *checked_slab_bstack_allocator_into_stack(
    checked_slab_bstack_allocator_t *alloc)
{
    bstack_t *bs = alloc->bs;
#ifdef BSTACK_FEATURE_ATOMIC
    bstack_alloc_lock_destroy(alloc->lock);
#endif
    free(alloc);
    return bs;
}

uint64_t checked_slab_bstack_allocator_data_size(
    const checked_slab_bstack_allocator_t *alloc)
{
    return alloc->block_size - ALCK_OVERHEAD;
}

/* =========================================================================
 * segregated_bstack_allocator_t — segregated (binned) free-list allocator
 * Requires -DBSTACK_FEATURE_SET (depends on bstack_set / bstack_zero and, under
 * BSTACK_FEATURE_ATOMIC, bstack_process_gen / bstack_inplace_gen).
 *
 * Port of the Rust SegregatedBStackAllocator; see bstack_alloc.h for the layout,
 * class scheme, and thread-safety contract.
 * ====================================================================== */

/* ---- class scheme (compile-time, encoded by the magic version) --------- */

#define ALSG_QUANTUM         UINT64_C(16)
#define ALSG_OVERHEAD        UINT64_C(8)
#define ALSG_LINEAR_MAX      UINT64_C(256)
#define ALSG_LINEAR_OCTAVE   8u                              /* log2(LINEAR_MAX) */
#define ALSG_MAX_OCTAVE      12u                             /* log2(MAX_CLASS)  */
#define ALSG_MAX_CLASS       (UINT64_C(1) << ALSG_MAX_OCTAVE)             /* 4096 */
#define ALSG_SUBCLASS_BITS   2u
#define ALSG_SUBCLASSES      (UINT64_C(1) << ALSG_SUBCLASS_BITS)         /* 4 */
#define ALSG_LINEAR_CLASSES  (ALSG_LINEAR_MAX / ALSG_QUANTUM)           /* 16 */
#define ALSG_GEO_CLASSES     ((uint64_t)(ALSG_MAX_OCTAVE - ALSG_LINEAR_OCTAVE) \
                              * ALSG_SUBCLASSES)                         /* 16 */
#define ALSG_NUM_CLASSES     (ALSG_LINEAR_CLASSES + ALSG_GEO_CLASSES + 1) /* 33 */
#define ALSG_OVERSIZED_CLASS (ALSG_NUM_CLASSES - 1)                     /* 32 */

/* ---- header layout (compile-time, fixed offsets) ----------------------- */

#define ALSG_OFFSET_SIZE     UINT64_C(24)
/* offset 32: 8 reserved bytes — no field yet. */
#define ALSG_FREE_HEAD_BASE  UINT64_C(40)
/* header rounded up to the 16-B quantum: 40 + 33*8 = 304 (already aligned). */
#define ALSG_ARENA_START     ((ALSG_FREE_HEAD_BASE + ALSG_NUM_CLASSES * 8 + 15) \
                              & ~UINT64_C(15))                          /* 304 */
#define ALSG_MAX_CARVE_PIECES 3
#define ALSG_SENTINEL        UINT64_C(0)
#define ALSG_IN_USE_BIT      UINT64_C(0x8000000000000000)

static const uint8_t alsg_magic[8]        = {'A','L','S','G',0,1,0,0};
static const uint8_t alsg_magic_prefix[6] = {'A','L','S','G',0,1};

#define alsg_head_off(cls) (ALSG_FREE_HEAD_BASE + (uint64_t)(cls) * 8)

/* ---- classification math (no tables, no recursion) --------------------- */

/* floor(log2(v)) for v >= 1 — the register form of 63 - v.leading_zeros(). */
static uint32_t alsg_ilog2_u64(uint64_t v)
{
    uint32_t r = 0;
    while (v >>= 1) r++;
    return r;
}

/* Round a caller len up to the physical need round_up(len + 8, 16).
 * Returns 0 to signal overflow (a valid need is always >= 16). */
static uint64_t alsg_phys_need(uint64_t len)
{
    uint64_t n;
    if (len > UINT64_MAX - (ALSG_OVERHEAD + ALSG_QUANTUM - 1)) return 0;
    n = len + (ALSG_OVERHEAD + ALSG_QUANTUM - 1);
    return n & ~(ALSG_QUANTUM - 1);
}

/* Snap a physical need up to its class block size. */
static uint64_t alsg_class_blocksize(uint64_t need)
{
    if (need <= ALSG_LINEAR_MAX) return need;
    if (need <= ALSG_MAX_CLASS) {
        uint32_t k = alsg_ilog2_u64(need - 1);      /* need in (2^k, 2^{k+1}] */
        uint64_t w = UINT64_C(1) << (k - ALSG_SUBCLASS_BITS);
        return (need + w - 1) & ~(w - 1);
    }
    return need;
}

/* Map a physical block size (multiple of 16, >= 16) to its free-list head. */
static uint64_t alsg_classify(uint64_t size)
{
    if (size <= ALSG_LINEAR_MAX) return (size >> 4) - 1;
    if (size <= ALSG_MAX_CLASS) {
        uint32_t k   = alsg_ilog2_u64(size - 1);
        uint64_t sub = (size - 1 - (UINT64_C(1) << k)) >> (k - ALSG_SUBCLASS_BITS);
        return ALSG_LINEAR_CLASSES
             + ((uint64_t)k - ALSG_LINEAR_OCTAVE) * ALSG_SUBCLASSES + sub;
    }
    return ALSG_OVERSIZED_CLASS;
}

/* Largest class block size <= v (16 <= v <= MAX_CLASS, v a multiple of 16). */
static uint64_t alsg_largest_class_le(uint64_t v)
{
    uint32_t k;
    uint64_t w;
    if (v <= ALSG_LINEAR_MAX) return v;
    k = alsg_ilog2_u64(v);                           /* 2^k <= v < 2^{k+1} */
    w = UINT64_C(1) << (k - ALSG_SUBCLASS_BITS);
    return v & ~(w - 1);
}

/* Validate a caller data pointer and return its block base offset. */
static int alsg_block_start_of(uint64_t ptr, uint64_t *out)
{
    if (ptr < ALSG_ARENA_START + ALSG_OVERHEAD || ptr % ALSG_QUANTUM != ALSG_OVERHEAD) {
        errno = EINVAL; return -1;
    }
    *out = ptr - ALSG_OVERHEAD;
    return 0;
}

/* ---- claim buffer ------------------------------------------------------ */

/* Build the block-byte claim buffer: overhead = IN_USE|len, an optional prefix
 * of n bytes read straight from payload offset src, the rest zero.  Returns a
 * malloc'd buffer (caller frees) or NULL on failure. */
static uint8_t *alsg_claim_buf(bstack_t *bs, uint64_t block, uint64_t len,
                               int has_copy, uint64_t src, uint64_t n)
{
    uint8_t *buf;
#if UINT64_MAX > SIZE_MAX
    if (block > (uint64_t)SIZE_MAX) { errno = EINVAL; return NULL; }
#endif
    buf = calloc(1, (size_t)block);
    if (!buf) return NULL;
    write_le64(buf, ALSG_IN_USE_BIT | len);
    if (has_copy && n > 0) {
        if (bstack_get(bs, src, src + n, buf + 8)) { free(buf); return NULL; }
    }
    return buf;
}

/* ---- free-list pop ----------------------------------------------------- */

#ifdef BSTACK_FEATURE_ATOMIC
/* Pop the head of `class`: read head, read its next_free, advance head — one
 * process_gen sequence under a held write lock, closing the ABA window. */
struct alsg_pop_ctx {
    uint64_t head_off;
    uint8_t  head_buf[8];
    uint8_t  next_buf[8];
    uint64_t popped;
    int      have;
    int      step;
};

static int alsg_pop_class_gen(bstack_gen_op_t *op, void *userctx)
{
    struct alsg_pop_ctx *c = userctx;
    switch (c->step++) {
    case 0:
        op->kind = BSTACK_GEN_READ;
        op->u.read.offset = c->head_off;
        op->u.read.buf = c->head_buf; op->u.read.len = 8; return 1;
    case 1: {
        uint64_t head = read_le64(c->head_buf);
        if (head == ALSG_SENTINEL) return 0;
        c->popped = head; c->have = 1;
        // Despite looking similar, this is not the same as the last read
        op->kind = BSTACK_GEN_READ;
        op->u.read.offset = head + ALSG_OVERHEAD;
        op->u.read.buf = c->next_buf; op->u.read.len = 8; return 1;
    }
    case 2:
        op->kind = BSTACK_GEN_WRITE; op->u.write.offset = c->head_off;
        op->u.write.data = c->next_buf; op->u.write.len = 8; return 1;
    default:
        return 0;
    }
}

static int alsg_pop_class(bstack_t *bs, uint64_t class,
                          int *out_have, uint64_t *out_block)
{
    struct alsg_pop_ctx c;
    memset(&c, 0, sizeof c);
    c.head_off = alsg_head_off(class);
    if (bstack_process_gen(bs, alsg_pop_class_gen, &c)) return -1;
    *out_have = c.have;
    if (c.have) *out_block = c.popped;
    return 0;
}

/* Pop the oversized head if its stored size >= need; the overhead word and
 * inline next_free are contiguous, fetched in one 16-byte read. */
struct alsg_pop_ovs_ctx {
    uint64_t head_off, need, head, size;
    uint8_t  head_buf[8];
    uint8_t  oh_next[16];
    int      have, step;
};

static int alsg_pop_ovs_gen(bstack_gen_op_t *op, void *userctx)
{
    struct alsg_pop_ovs_ctx *c = userctx;
    switch (c->step++) {
    case 0:
        op->kind = BSTACK_GEN_READ; op->u.read.offset = c->head_off;
        op->u.read.buf = c->head_buf; op->u.read.len = 8; return 1;
    case 1: {
        uint64_t head = read_le64(c->head_buf);
        if (head == ALSG_SENTINEL) return 0;
        c->head = head;
        op->kind = BSTACK_GEN_READ; op->u.read.offset = head;
        op->u.read.buf = c->oh_next; op->u.read.len = 16; return 1;
    }
    case 2: {
        uint64_t word = read_le64(c->oh_next);
        c->size = word << 4;
        if ((word & ALSG_IN_USE_BIT) != 0 || c->size < c->need) return 0;
        c->have = 1;
        op->kind = BSTACK_GEN_WRITE; op->u.write.offset = c->head_off;
        op->u.write.data = c->oh_next + 8; op->u.write.len = 8; return 1;
    }
    default:
        return 0;
    }
}

static int alsg_pop_oversized(bstack_t *bs, uint64_t need, int *out_have,
                              uint64_t *out_block, uint64_t *out_size)
{
    struct alsg_pop_ovs_ctx c;
    memset(&c, 0, sizeof c);
    c.head_off = alsg_head_off(ALSG_OVERSIZED_CLASS);
    c.need     = need;
    if (bstack_process_gen(bs, alsg_pop_ovs_gen, &c)) return -1;
    *out_have = c.have;
    if (c.have) { *out_block = c.head; *out_size = c.size; }
    return 0;
}
#else /* !BSTACK_FEATURE_ATOMIC */
static int alsg_pop_class(bstack_t *bs, uint64_t class,
                          int *out_have, uint64_t *out_block)
{
    uint64_t head_off = alsg_head_off(class);
    uint8_t  buf[8];
    uint64_t head, next;
    if (bstack_get(bs, head_off, head_off + 8, buf)) return -1;
    head = read_le64(buf);
    if (head == ALSG_SENTINEL) { *out_have = 0; return 0; }
    if (bstack_get(bs, head + ALSG_OVERHEAD, head + ALSG_OVERHEAD + 8, buf)) return -1;
    next = read_le64(buf);
    write_le64(buf, next);
    if (bstack_set(bs, head_off, buf, 8)) return -1;
    *out_have = 1; *out_block = head;
    return 0;
}

static int alsg_pop_oversized(bstack_t *bs, uint64_t need, int *out_have,
                              uint64_t *out_block, uint64_t *out_size)
{
    uint64_t head_off = alsg_head_off(ALSG_OVERSIZED_CLASS);
    uint8_t  buf8[8], buf16[16];
    uint64_t head, word, size, next;
    if (bstack_get(bs, head_off, head_off + 8, buf8)) return -1;
    head = read_le64(buf8);
    if (head == ALSG_SENTINEL) { *out_have = 0; return 0; }
    /* overhead || next_free are contiguous: one 16-byte read. */
    if (bstack_get(bs, head, head + 16, buf16)) return -1;
    word = read_le64(buf16);
    size = word << 4;
    if ((word & ALSG_IN_USE_BIT) || size < need) { *out_have = 0; return 0; }
    next = read_le64(buf16 + 8);
    write_le64(buf8, next);
    if (bstack_set(bs, head_off, buf8, 8)) return -1;
    *out_have = 1; *out_block = head; *out_size = size;
    return 0;
}
#endif /* BSTACK_FEATURE_ATOMIC */

/* ---- free-list push ---------------------------------------------------- */

#ifdef BSTACK_FEATURE_ATOMIC
/* Push block_start onto head[class] via one inplace_gen transaction: read the
 * old head into next_free's half, write overhead||next_free, repoint the head. */
struct alsg_push_ctx {
    uint64_t head_off, block_start;
    uint8_t  start_bytes[8];
    uint8_t  overhead_buf[16]; /* [0..8]=free|size, [8..16]=old head (read in) */
    int      step;
};

static int alsg_push_gen(bstack_gen_op_t *op, void *userctx)
{
    struct alsg_push_ctx *c = userctx;
    switch (c->step++) {
    case 0: /* read current head into the next_free half */
        op->kind = BSTACK_GEN_READ; op->u.read.offset = c->head_off;
        op->u.read.buf = c->overhead_buf + 8; op->u.read.len = 8; return 1;
    case 1: /* overhead || next_free */
        op->kind = BSTACK_GEN_WRITE; op->u.write.offset = c->block_start;
        op->u.write.data = c->overhead_buf; op->u.write.len = 16; return 1;
    case 2: /* head[class] <- block_start */
        op->kind = BSTACK_GEN_WRITE; op->u.write.offset = c->head_off;
        op->u.write.data = c->start_bytes; op->u.write.len = 8; return 1;
    default:
        return 0;
    }
}

static int alsg_push(bstack_t *bs, uint64_t block_start, uint64_t size,
                     uint64_t class)
{
    struct alsg_push_ctx c;
    c.head_off    = alsg_head_off(class);
    c.block_start = block_start;
    write_le64(c.overhead_buf, size >> 4);
    write_le64(c.overhead_buf + 8, 0); /* next_free half will be filled in by read */
    write_le64(c.start_bytes, block_start);
    c.step = 0;
    return bstack_inplace_gen(bs, alsg_push_gen, &c, NULL);
}
#else /* !BSTACK_FEATURE_ATOMIC */
static int alsg_push(bstack_t *bs, uint64_t block_start, uint64_t size,
                     uint64_t class)
{
    uint64_t head_off = alsg_head_off(class);
    uint8_t  overhead_buf[16], head_buf[8];
    uint64_t head;
    write_le64(overhead_buf, size >> 4);      /* free tag: high bit clear */
    if (bstack_get(bs, head_off, head_off + 8, head_buf)) return -1;
    head = read_le64(head_buf);
    write_le64(overhead_buf + 8, head);
    if (bstack_set(bs, block_start, overhead_buf, 16)) return -1;
    write_le64(head_buf, block_start);
    return bstack_set(bs, head_off, head_buf, 8);
}
#endif /* BSTACK_FEATURE_ATOMIC */

/* ---- commit_carve ------------------------------------------------------ */

/* Shared scratch for carving up to ALSG_MAX_CARVE_PIECES free blocks out of a
 * region; also doubles as the userctx for alsg_carve_gen() under
 * BSTACK_FEATURE_ATOMIC. prefix_off/k/prefix/prefix_len/step are unused (and
 * left uninitialized) outside that build. */
struct alsg_carve_ctx {
    uint64_t       prefix_off;
    size_t         k;
    const uint8_t *prefix;
    size_t         prefix_len;
    size_t         step;
    uint64_t       block_offs[ALSG_MAX_CARVE_PIECES];
    uint64_t       head_offs[ALSG_MAX_CARVE_PIECES];
    uint8_t        blockoff_bytes[ALSG_MAX_CARVE_PIECES][8];
    uint8_t        overhead_next[ALSG_MAX_CARVE_PIECES][16];
};

#ifdef BSTACK_FEATURE_ATOMIC
/* Steps: [0, k) read each head into that piece's next_free half; k writes the
 * prefix (the commit point); then 2 writes per piece (overhead||next, head);
 * then 0 commits the batch. */
static int alsg_carve_gen(bstack_gen_op_t *op, void *userctx)
{
    struct alsg_carve_ctx *c = userctx;
    size_t step = c->step++;
    if (step < c->k) {
        op->kind = BSTACK_GEN_READ;
        op->u.read.offset = c->head_offs[step];
        op->u.read.buf    = c->overhead_next[step] + 8;
        op->u.read.len    = 8;
        return 1;
    }
    if (step == c->k) {
        op->kind = BSTACK_GEN_WRITE;
        op->u.write.offset = c->prefix_off;
        op->u.write.data   = c->prefix;
        op->u.write.len    = c->prefix_len;
        return 1;
    }
    if (step < c->k + 1 + 2 * c->k) {
        size_t j = step - (c->k + 1);
        size_t i = j / 2;
        op->kind = BSTACK_GEN_WRITE;
        if (j % 2 == 0) {                 /* overhead || next_free */
            op->u.write.offset = c->block_offs[i];
            op->u.write.data   = c->overhead_next[i];
            op->u.write.len    = 16;
        } else {                          /* head[class] <- this block */
            op->u.write.offset = c->head_offs[i];
            op->u.write.data   = c->blockoff_bytes[i];
            op->u.write.len    = 8;
        }
        return 1;
    }
    return 0;
}
#endif /* BSTACK_FEATURE_ATOMIC */

/* Commit a `prefix` write and free a contiguous `region` together.  The region
 * is greedily decomposed into class free blocks (largest class <= remainder,
 * repeated; a region > MAX_CLASS becomes one oversized block), each a distinct
 * class, so <= MAX_CARVE_PIECES pieces.  With atomic the prefix + carve ride one
 * inplace_gen transaction; without atomic the pieces are laid down first and the
 * single prefix write is the commit point that exposes them.  region_size may be
 * 0 (a plain prefix write). */
static int alsg_commit_carve(bstack_t *bs, uint64_t prefix_off,
                             const uint8_t *prefix, size_t prefix_len,
                             uint64_t region_start, uint64_t region_size)
{
    struct alsg_carve_ctx c;
    uint64_t off = region_start, rem = region_size;
    size_t   k = 0;

#ifdef BSTACK_FEATURE_ATOMIC
    c.step       = 0;
    c.prefix_off = prefix_off;
    c.prefix     = prefix;
    c.prefix_len = prefix_len;
#endif

    while (rem > 0) {
        uint64_t ps = (rem > ALSG_MAX_CLASS) ? rem : alsg_largest_class_le(rem);
        if (k >= ALSG_MAX_CARVE_PIECES) { errno = EINVAL; return -1; }
        c.block_offs[k] = off;
        c.head_offs[k]  = alsg_head_off(alsg_classify(ps));
        write_le64(c.overhead_next[k], ps >> 4);
        write_le64(c.overhead_next[k] + 8, 0); /* next_free half will be filled in by read */
        write_le64(c.blockoff_bytes[k], off);
        off += ps; rem -= ps; k++;
    }

#ifndef BSTACK_FEATURE_ATOMIC
    {
        size_t i;
        for (i = 0; i < k; i++) {
            uint8_t shared[16];
            memcpy(shared, c.overhead_next[i], 16);
            /* next_free <- current head of this class (read straight in). */
            if (bstack_get(bs, c.head_offs[i], c.head_offs[i] + 8, shared + 8)) return -1;
            if (bstack_set(bs, c.block_offs[i], shared, 16)) return -1;
            if (bstack_set(bs, c.head_offs[i], c.blockoff_bytes[i], 8)) return -1;
        }
        return bstack_set(bs, prefix_off, prefix, prefix_len);
    }
#else
    c.k = k;
    return bstack_inplace_gen(bs, alsg_carve_gen, &c, NULL);
#endif
}

/* ---- alloc_raw --------------------------------------------------------- */

/* Place a `len`-byte block whose payload begins with `copy_n` bytes copied from
 * payload offset `src` (has_copy != 0; rest zeroed) and return its data pointer
 * (block_start + OVERHEAD) in *out_ptr. */
static int alsg_alloc_raw(bstack_t *bs, uint64_t len, int has_copy,
                          uint64_t src, uint64_t copy_n, uint64_t *out_ptr)
{
    uint64_t need = alsg_phys_need(len);
    uint64_t block;
    uint8_t *buf;
    uint64_t bs_off;

    if (need == 0) { errno = EINVAL; return -1; }
    block = alsg_class_blocksize(need);

    if (block <= ALSG_MAX_CLASS) {
        int have = 0;
        if (alsg_pop_class(bs, alsg_classify(block), &have, &bs_off)) return -1;
        if (have) {
            buf = alsg_claim_buf(bs, block, len, has_copy, src, copy_n);
            if (!buf) return -1;
            if (bstack_set(bs, bs_off, buf, (size_t)block)) goto cleanup;
            goto success;
        }
    } else {
        int have = 0;
        uint64_t actual = 0;
        if (alsg_pop_oversized(bs, block, &have, &bs_off, &actual))
            return -1;
        if (have) {
            buf = alsg_claim_buf(bs, block, len, has_copy, src, copy_n);
            if (!buf) return -1;
            if (actual == block) {
                if (bstack_set(bs, bs_off, buf, (size_t)block)) goto cleanup;
            } else {
                /* Non-exact reuse: claim `block` bytes (the claim buffer is the
                 * prefix) and carve the excess as one crash-atomic transaction. */
                if (alsg_commit_carve(bs, bs_off, buf, (size_t)block,
                                      bs_off + block, actual - block))
                goto cleanup;
            }
            goto success;
        }
    }

    /* Miss: grow the whole zero-filled block in one sparse write. */
    buf = alsg_claim_buf(bs, block, len, has_copy, src, copy_n);
    if (!buf) return -1;
    if (bstack_extend_sparse(bs, buf, (size_t)block, block, &bs_off)) {
cleanup:
        free(buf); return -1;
    }
success:
    free(buf);
    *out_ptr = bs_off + ALSG_OVERHEAD;
    return 0;
}

/* ---- recover ----------------------------------------------------------- */

int segregated_bstack_allocator_recover(segregated_bstack_allocator_t *alloc,
                                        uint64_t *out_unsure)
{
    bstack_t *bs = alloc->bs;
    uint64_t  stack_len, unsure = 0, p;
    uint64_t  heads[ALSG_NUM_CLASSES];
    uint8_t   head_bytes[ALSG_NUM_CLASSES * 8];
    uint8_t   wbuf[8];
    size_t    c;

    memset(heads, 0, sizeof heads);
    if (bstack_len(bs, &stack_len)) return -1;
    if (stack_len <= ALSG_ARENA_START) {
        if (out_unsure) *out_unsure = 0;
        return 0;
    }

    p = ALSG_ARENA_START;
    while (p < stack_len) {
        uint64_t word;
        if (bstack_get(bs, p, p + 8, wbuf)) return -1;
        word = read_le64(wbuf);
        if (word & ALSG_IN_USE_BIT) {
            /* Live: stride by the physical size implied by the stored len. */
            uint64_t ulen = word & ~ALSG_IN_USE_BIT;
            uint64_t need = alsg_phys_need(ulen);
            uint64_t size;
            if (need == 0) { unsure += (stack_len - p) / ALSG_QUANTUM; break; }
            size = alsg_class_blocksize(need);
            if (size == 0 || size > stack_len - p) {
                unsure += (stack_len - p) / ALSG_QUANTUM; break;
            }
            p += size;
        } else if (word == 0) {
            /* Zeroed tail from a crashed extend: discard it. */
            uint64_t dn = stack_len - p;
#if UINT64_MAX > SIZE_MAX
            if (dn > (uint64_t)SIZE_MAX) { errno = EINVAL; return -1; }
#endif
            if (bstack_discard(bs, (size_t)dn)) return -1;
            break;
        } else {
            /* Free: relink by the stored physical size (reclaims leaks too). */
            uint64_t size = word << 4;
            uint64_t cc;
            if (size < ALSG_QUANTUM || size % ALSG_QUANTUM || size > stack_len - p) {
                unsure += (stack_len - p) / ALSG_QUANTUM; break;
            }
            cc = alsg_classify(size);
            /* Prepend: next_free <- current head of this class, head <- p. */
            write_le64(wbuf, heads[cc]);
            if (bstack_set(bs, p + ALSG_OVERHEAD, wbuf, 8)) return -1;
            heads[cc] = p;
            p += size;
        }
    }

    /* Publish the rebuilt head table as a single crash-atomic set. */
    for (c = 0; c < ALSG_NUM_CLASSES; c++)
        write_le64(head_bytes + c * 8, heads[c]);
    if (bstack_set(bs, ALSG_FREE_HEAD_BASE, head_bytes, sizeof head_bytes)) return -1;
    if (out_unsure) *out_unsure = unsure;
    return 0;
}

/* ---- vtable implementations -------------------------------------------- */

static bstack_t *alsg_vt_stack(bstack_allocator_t *base)
{
    return ((segregated_bstack_allocator_t *)base)->bs;
}

static int alsg_vt_alloc(bstack_allocator_t *base, uint64_t len,
                         bstack_slice_t *out)
{
    segregated_bstack_allocator_t *a = (segregated_bstack_allocator_t *)base;
    uint64_t ptr;
    if (len == 0) {
        out->offset = 0; out->len = 0; goto success;
    }
    if (alsg_alloc_raw(a->bs, len, 0, 0, 0, &ptr)) return -1;
    out->offset = ptr; out->len = len;
success:
    out->allocator = base; return 0;
}

static int alsg_vt_dealloc(bstack_allocator_t *base, bstack_slice_t s)
{
    segregated_bstack_allocator_t *a = (segregated_bstack_allocator_t *)base;
    bstack_t *bs = a->bs;
    uint64_t block_start, word, need, size, class, end;
    uint8_t  buf[8];
#define RETURN_INVALID do { errno = EINVAL; return -1; } while (0)

    if (s.len == 0) return 0;                       /* empty handle */

    /* Validation errors leave the caller's block untouched (survived, -1). */
    if (alsg_block_start_of(s.offset, &block_start)) return -1;
    if (bstack_get(bs, block_start, block_start + 8, buf)) return -1;
    word = read_le64(buf);
    if (!(word & ALSG_IN_USE_BIT)) RETURN_INVALID;  /* double free */
    if ((word & ~ALSG_IN_USE_BIT) != s.len) RETURN_INVALID; /* mismatch */

    need = alsg_phys_need(s.len);
    if (!need) RETURN_INVALID;
    size  = alsg_class_blocksize(need);
    class = alsg_classify(size);

    if (size > ALSG_MAX_CLASS) {
        if (block_start > UINT64_MAX - size) RETURN_INVALID;
        end = block_start + size;
#if UINT64_MAX > SIZE_MAX
        if (size > (uint64_t)SIZE_MAX) RETURN_INVALID;
#endif
#undef RETURN_INVALID
        /* Oversized tail block: hand its bytes back to the stack. */
#ifdef BSTACK_FEATURE_ATOMIC
        {
            int ok = 0;
            if (bstack_try_discard(bs, end, (size_t)size, &ok)) return -1;
            if (ok) return 0;
        }
#else
        {
            uint64_t tail;
            if (bstack_len(bs, &tail)) return -1;
            if (end == tail)
                return bstack_discard(bs, (size_t)size) == 0 ? 0 : -1;
        }
#endif /* BSTACK_FEATURE_ATOMIC */
    }

    /* Free-list splice: a mid-way failure may leave the block partially
     * spliced, so the handle can no longer be safely returned (-2). */
    return alsg_push(bs, block_start, size, class) == 0 ? 0 : -2;
}

static int alsg_vt_realloc(bstack_allocator_t *base, bstack_slice_t s,
                           uint64_t new_len, bstack_slice_t *out)
{
    segregated_bstack_allocator_t *a = (segregated_bstack_allocator_t *)base;
    bstack_t      *bs = a->bs;
    bstack_slice_t recovered, result;
    uint64_t       start = s.offset, old_len = s.len;
    uint64_t       block_start, word, old_size, new_size, old_end, need_o, need_n;
    uint8_t        buf[8];

    /* Empty handle: realloc is a fresh alloc. */
    if (s.len == 0 && s.offset == 0) {
        if (alsg_vt_alloc(base, new_len, out)) {
            out->allocator = base; out->offset = 0; out->len = 0;
            return -1;
        }
        return 0;
    }
    /* new_len == 0: dealloc consumes s; propagate its survivor signal. */
    if (new_len == 0) {
        int dr = alsg_vt_dealloc(base, s);
        if (dr) {
            if (dr == -1) { out->allocator = base; out->offset = start; out->len = old_len; }
            return dr;
        }
        out->allocator = base; out->offset = 0; out->len = 0;
        return 0;
    }

    /* The surviving allocation to hand back on failure — the original block,
     * updated to the new region only once a move has committed it.  Every path
     * below leaves it valid, so `goto fail_recover` always fails -1 (survived).
     * `result` is the region to hand back on success (`goto success`). */
    recovered.allocator = base; recovered.offset = start; recovered.len = old_len;

    if (alsg_block_start_of(start, &block_start)) goto fail_recover;
    if (new_len == old_len) {
        result.offset = start; result.len = old_len; goto success;
    }

    if (bstack_get(bs, block_start, block_start + 8, buf)) goto fail_recover;
    word = read_le64(buf);
    if ((word & ALSG_IN_USE_BIT) == 0) goto fail_invalid;
    if ((word & ~ALSG_IN_USE_BIT) != old_len) goto fail_invalid;

    need_o = alsg_phys_need(old_len);
    need_n = alsg_phys_need(new_len);
    if (need_o == 0 || need_n == 0) goto fail_invalid;
    old_size = alsg_class_blocksize(need_o);
    new_size = alsg_class_blocksize(need_n);

    /* Same class: the block already fits.  Zero the newly-exposed bytes on grow
     * before the length commit so a crash never exposes them. */
    if (new_size == old_size) {
        if (new_len > old_len) {
            uint64_t z = new_len - old_len;
#if UINT64_MAX > SIZE_MAX
            if (z > (uint64_t)SIZE_MAX) goto fail_invalid;
#endif
            if (bstack_zero(bs, start + old_len, (size_t)z)) goto fail_recover;
        }
        write_le64(buf, ALSG_IN_USE_BIT | new_len);
        if (bstack_set(bs, block_start, buf, 8)) goto fail_recover;
        result.offset = start; result.len = new_len; goto success;
    }

    old_end = block_start + old_size;

    /* Grow at the tail: extend in place, zero the old slack, commit the length. */
    if (new_size > old_size) {
        uint64_t delta = new_size - old_size;
        int grew = 0;
#if UINT64_MAX > SIZE_MAX
        if (delta > (uint64_t)SIZE_MAX) goto fail_invalid;
#endif
#ifdef BSTACK_FEATURE_ATOMIC
        {
            int ok = 0;
            if (bstack_try_extend_zeros(bs, old_end, (size_t)delta, &ok)) goto fail_recover;
            grew = ok;
        }
#else
        {
            uint64_t tail;
            if (bstack_len(bs, &tail)) goto fail_recover;
            if (old_end == tail) {
                if (bstack_extend(bs, (size_t)delta, NULL)) goto fail_recover;
                grew = 1;
            }
        }
#endif
        if (grew) {
            uint64_t slack = (old_size - ALSG_OVERHEAD) - old_len;
            if (slack > 0) {
#if UINT64_MAX > SIZE_MAX
                if (slack > (uint64_t)SIZE_MAX) goto fail_invalid;
#endif
                if (bstack_zero(bs, start + old_len, (size_t)slack)) goto fail_recover;
            }
            write_le64(buf, ALSG_IN_USE_BIT | new_len);
            if (bstack_set(bs, block_start, buf, 8)) goto fail_recover;
            result.offset = start; result.len = new_len; goto success;
        }
        /* Not at tail: fall through to the move. */
    }

    /* Shrink at the tail: replace the whole block with its shrunk self in ONE
     * crash-atomic transaction.  The length commit and the truncation must not
     * be separate calls: committing new_len first makes the recovery scan stride
     * new_size, so a failure before the discard lands leaves the caller's still
     * live tail bytes being read as an overhead word — a zero run there (ordinary
     * data) looks like a crashed extend and recovery discards the whole arena
     * behind it.  Truncating first is no better: the header would still read
     * old_len, whose stride overruns the payload end.  So LEN confirms the tail
     * under the held write lock and one SPLICE cuts the old block and re-appends
     * the new one at the same offset.
     *
     * Without atomic there is no way to fuse them, so that build falls through to
     * the move below — as the non-tail shrink already does. */
#ifdef BSTACK_FEATURE_ATOMIC
    if (new_size < old_size) {
        bstack_alloc_tail_shrink_ctx_t c;
        uint8_t *nb;
        int rc;
#if UINT64_MAX > SIZE_MAX
        if (old_size > (uint64_t)SIZE_MAX || new_size > (uint64_t)SIZE_MAX)
            goto fail_invalid;
#endif
        /* The replacement block, built exactly as a fresh claim would be:
         * overhead(IN_USE|new_len), the surviving prefix read straight out of the
         * old block, then zero padding to the class size. */
        nb = alsg_claim_buf(bs, new_size, new_len, 1, start, new_len);
        if (!nb) goto fail_recover;
        c.phase         = 0;
        c.truncated     = 0;
        c.expected_tail = old_end;
        c.cur           = 0;
        c.cut           = (size_t)old_size;
        c.new_buf       = nb;
        c.new_len       = (size_t)new_size;
        rc = bstack_process_gen(bs, bstack_alloc_tail_shrink_gen, &c);
        free(nb);
        if (rc) goto fail_recover;
        if (c.truncated) { result.offset = start; result.len = new_len; goto success; }
        /* Not the tail — fall through to the in-place carve. */
    }

    /* Non-tail shrink: keep the block at the new class and free the excess tail
     * in place as one crash-atomic carve. */
    if (new_size < old_size) {
        uint8_t prefix[8];
        write_le64(prefix, ALSG_IN_USE_BIT | new_len);
        if (alsg_commit_carve(bs, block_start, prefix, 8,
            block_start + new_size, old_size - new_size))
            goto fail_recover;
        result.offset = start; result.len = new_len; goto success;
    }
#endif /* BSTACK_FEATURE_ATOMIC */

    /* Move: alloc the new class, having it read the surviving prefix straight
     * from the old block into its claim buffer, then free the old block.
     * Handles non-tail grow (both builds) and, without atomic, every shrink
     * that fell through above. */
    {
        uint64_t copy_len = old_len < new_len ? old_len : new_len;
        uint64_t new_ptr;
        bstack_slice_t old_s;
        if (alsg_alloc_raw(bs, new_len, 1, start, copy_len, &new_ptr)) goto fail_recover;
        /* New region committed and populated; it is now the survivor. */
        recovered.allocator = base; recovered.offset = new_ptr; recovered.len = new_len;
        old_s.allocator = base; old_s.offset = start; old_s.len = old_len;
        if (alsg_vt_dealloc(base, old_s)) goto fail_recover;
        result.offset = new_ptr; result.len = new_len; goto success;
    }

success:
    result.allocator = base;
    *out = result;
    return 0;
fail_invalid:
    errno = EINVAL;
fail_recover:
    *out = recovered;
    return -1;
}

static const bstack_allocator_vtbl_t alsg_allocator_vtbl = {
    alsg_vt_stack,
    alsg_vt_alloc,
    alsg_vt_realloc,
    alsg_vt_dealloc,
};

/* ---- public API -------------------------------------------------------- */

segregated_bstack_allocator_t *segregated_bstack_allocator_new(bstack_t *bs)
{
    segregated_bstack_allocator_t *a;
    int is_empty;

    if (bstack_is_empty(bs, &is_empty)) return NULL;

    a = malloc(sizeof *a);
    if (!a) { errno = ENOMEM; return NULL; }
    a->base.vtbl      = &alsg_allocator_vtbl;
    a->base.bulk_vtbl = NULL;
    a->bs             = bs;

    if (is_empty) {
        /* Fresh: write the header (reserved + magic) and a zeroed free_head
         * table, growing straight to the arena start in one sparse write. */
        uint8_t hdr[ALSG_OFFSET_SIZE + 8];
        memset(hdr, 0, sizeof hdr);
        memcpy(hdr + (size_t)ALSG_OFFSET_SIZE, alsg_magic, 8);
        if (bstack_extend_sparse(bs, hdr, sizeof hdr, ALSG_ARENA_START, NULL)) goto fail_free;
        return a;
    }

    /* Reopen: validate the header, pad a short table region, then recover. */
    {
        uint64_t stack_len;
        uint8_t  magic[8];
        if (bstack_len(bs, &stack_len)) goto fail_free;
        if (stack_len < ALSG_FREE_HEAD_BASE) goto fail_invalid;
        if (bstack_get(bs, ALSG_OFFSET_SIZE, ALSG_OFFSET_SIZE + 8, magic)) goto fail_free;
        if (memcmp(magic, alsg_magic_prefix, sizeof alsg_magic_prefix)) goto fail_invalid;
        if (stack_len < ALSG_ARENA_START) {
            uint64_t needed = ALSG_ARENA_START - stack_len;
#if UINT64_MAX > SIZE_MAX
            if (needed > (uint64_t)SIZE_MAX) goto fail_invalid;
#endif
            if (bstack_extend(bs, (size_t)needed, NULL)) goto fail_free;
        } else if ((stack_len - ALSG_ARENA_START) % ALSG_QUANTUM) {
fail_invalid:
            errno = EINVAL; free(a); return NULL;
        }
        /* Quiescent: `a` has not escaped this function. */
        if (segregated_bstack_allocator_recover(a, NULL)) {
fail_free:
            free(a); return NULL;
        }
        return a;
    }
}

void segregated_bstack_allocator_free(segregated_bstack_allocator_t *alloc)
{
    free(alloc);
}

bstack_t *segregated_bstack_allocator_into_stack(
    segregated_bstack_allocator_t *alloc)
{
    bstack_t *bs = alloc->bs; free(alloc);
    return bs;
}

#endif /* BSTACK_FEATURE_SET */

#ifdef __cplusplus
}
#endif
