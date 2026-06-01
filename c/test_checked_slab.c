/* checked_slab_bstack_allocator_t — smoke + fuzz tests.
 * Requires -DBSTACK_FEATURE_SET.
 * Mirrors test_slab.c in structure, with additional coverage for overhead
 * tracking, double-free detection, and crash recovery. */

#ifndef _WIN32
#  define _DARWIN_C_SOURCE
#  define _DEFAULT_SOURCE
#  define _POSIX_C_SOURCE 200809L
#endif

#include "bstack_alloc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef _WIN32
#  define WIN32_LEAN_AND_MEAN
#  include <windows.h>
#  include <process.h>
#  define csl_unlink(p)  DeleteFileA(p)
#  define csl_getpid()   ((unsigned long)_getpid())
#else
#  include <unistd.h>
#  define csl_unlink(p)  unlink(p)
#  define csl_getpid()   ((unsigned long)getpid())
#endif

/* =========================================================================
 * Harness
 * ====================================================================== */

static int g_total = 0, g_passed = 0;

#define CHECK(cond)                                                    \
    do {                                                               \
        if (!(cond)) {                                                 \
            fprintf(stderr, "  FAIL %s:%d  %s\n",                     \
                    __func__, __LINE__, #cond);                        \
            return -1;                                                 \
        }                                                              \
    } while (0)

typedef int (*test_fn)(void);

static void run(const char *name, test_fn fn)
{
    g_total++;
    if (fn() == 0) { printf("PASS  %s\n", name); g_passed++; }
    else            printf("FAIL  %s\n", name);
}

#define T(fn) run(#fn, fn)

#ifdef _WIN32
static void make_tmp(char *buf, size_t n)
{
    static volatile LONG seq = 0;
    LONG s = InterlockedIncrement(&seq);
    snprintf(buf, n, "bstack_csl_%lu_%ld.tmp",
             (unsigned long)GetCurrentProcessId(), (long)s);
    DeleteFileA(buf);
}
#else
static void make_tmp(char *buf, size_t n)
{
    snprintf(buf, n, "/tmp/bstack_csl_XXXXXX");
    int fd = mkstemp(buf);
    if (fd >= 0) { close(fd); unlink(buf); }
}
#endif

/* =========================================================================
 * splitmix64 PRNG
 * ====================================================================== */

static uint64_t g_rng;

static void rng_seed(uint64_t s) { g_rng = s; }

static uint64_t rng_next(void)
{
    g_rng += UINT64_C(0x9e3779b97f4a7c15);
    uint64_t z = g_rng;
    z = (z ^ (z >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)) * UINT64_C(0x94d049bb133111eb);
    return z ^ (z >> 31);
}

static uint64_t rng_range(uint64_t lo, uint64_t hi)
{
    return lo + rng_next() % (hi - lo + 1);
}

static int rng_bool(double p)
{
    return rng_next() < (uint64_t)(p * (double)UINT64_MAX);
}

/* =========================================================================
 * Data-pattern helpers
 * ====================================================================== */

static uint8_t id_byte(uint64_t id, size_t i)
{
    return (uint8_t)((id >> ((i % 8) * 8)) & 0xFF);
}

static void id_fill(uint8_t *buf, size_t len, uint64_t id)
{
    size_t i;
    for (i = 0; i < len; i++) buf[i] = id_byte(id, i);
}

static int id_verify(const uint8_t *buf, size_t len, uint64_t id,
                     const char *ctx)
{
    size_t i;
    for (i = 0; i < len; i++) {
        uint8_t want = id_byte(id, i);
        if (buf[i] != want) {
            fprintf(stderr,
                "  corruption %s byte %zu: want 0x%02x got 0x%02x (id=%llu)\n",
                ctx, i, want, buf[i], (unsigned long long)id);
            return -1;
        }
    }
    return 0;
}

/* =========================================================================
 * csl_vec_t — growable array of live-allocation records
 * ====================================================================== */

typedef struct { uint64_t offset; uint64_t len; uint64_t id; } csl_entry_t;
typedef struct { csl_entry_t *data; size_t count; size_t cap; } csl_vec_t;

static int csl_vec_push(csl_vec_t *v, csl_entry_t e)
{
    if (v->count == v->cap) {
        size_t nc = v->cap ? v->cap * 2 : 16;
        csl_entry_t *t = realloc(v->data, nc * sizeof *t);
        if (!t) return -1;
        v->data = t; v->cap = nc;
    }
    v->data[v->count++] = e;
    return 0;
}

static csl_entry_t csl_vec_swap_remove(csl_vec_t *v, size_t idx)
{
    csl_entry_t e = v->data[idx];
    v->data[idx] = v->data[--v->count];
    return e;
}

static void csl_vec_free(csl_vec_t *v)
{
    free(v->data); v->data = NULL; v->count = 0; v->cap = 0;
}

static bstack_slice_t entry_slice(bstack_allocator_t *a, csl_entry_t e)
{
    bstack_slice_t s; s.allocator = a; s.offset = e.offset; s.len = e.len;
    return s;
}

/* =========================================================================
 * new() / open() validation tests
 * ====================================================================== */

static int test_new_initialises_header(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    checked_slab_bstack_allocator_t *a =
        checked_slab_bstack_allocator_new(bs, 8);
    CHECK(a);
    CHECK(checked_slab_bstack_allocator_data_size(a) == 8);

    /* ARENA_START = OFFSET_SIZE(24) + HEADER_SIZE(24) = 48 */
    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 48);

    bstack_close(checked_slab_bstack_allocator_into_stack(a));
    csl_unlink(tmp); return 0;
}

static int test_new_rejects_small_data_size(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    checked_slab_bstack_allocator_t *a =
        checked_slab_bstack_allocator_new(bs, 7); /* < MIN_DATA_SIZE=8 */
    CHECK(a == NULL);
    bstack_close(bs); csl_unlink(tmp); return 0;
}

static int test_new_rejects_nonempty_stack(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    uint8_t dummy[4] = {1,2,3,4};
    CHECK(bstack_push(bs, dummy, 4, NULL) == 0);
    checked_slab_bstack_allocator_t *a =
        checked_slab_bstack_allocator_new(bs, 8);
    CHECK(a == NULL);
    bstack_close(bs); csl_unlink(tmp); return 0;
}

static int test_open_rejects_empty_stack(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    checked_slab_bstack_allocator_t *a =
        checked_slab_bstack_allocator_open(bs);
    CHECK(a == NULL);
    bstack_close(bs); csl_unlink(tmp); return 0;
}

static int test_open_rejects_bad_magic(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    uint8_t zeros[48]; memset(zeros, 0, 48);
    CHECK(bstack_push(bs, zeros, 48, NULL) == 0);
    bstack_close(bs);

    bs = bstack_open(tmp); CHECK(bs);
    checked_slab_bstack_allocator_t *a =
        checked_slab_bstack_allocator_open(bs);
    CHECK(a == NULL);
    bstack_close(bs); csl_unlink(tmp); return 0;
}

static int test_open_rejects_misaligned_tail(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        checked_slab_bstack_allocator_t *a =
            checked_slab_bstack_allocator_new(bs, 8);
        CHECK(a);
        bstack_close(checked_slab_bstack_allocator_into_stack(a));
    }
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        uint8_t one = 0xFF;
        CHECK(bstack_push(bs, &one, 1, NULL) == 0); /* misalign the tail */
        bstack_close(bs);
    }
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        checked_slab_bstack_allocator_t *a =
            checked_slab_bstack_allocator_open(bs);
        CHECK(a == NULL);
        bstack_close(bs);
    }
    csl_unlink(tmp); return 0;
}

static int test_open_restores_data_size(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        checked_slab_bstack_allocator_t *a =
            checked_slab_bstack_allocator_new(bs, 24);
        CHECK(a);
        bstack_close(checked_slab_bstack_allocator_into_stack(a));
    }
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        checked_slab_bstack_allocator_t *a =
            checked_slab_bstack_allocator_open(bs);
        CHECK(a);
        CHECK(checked_slab_bstack_allocator_data_size(a) == 24);
        bstack_close(checked_slab_bstack_allocator_into_stack(a));
    }
    csl_unlink(tmp); return 0;
}

/* =========================================================================
 * Allocation behaviour
 * ====================================================================== */

static int test_zero_alloc_returns_empty(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    checked_slab_bstack_allocator_t *a =
        checked_slab_bstack_allocator_new(bs, 8);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t s;
    CHECK(bstack_allocator_alloc(al, 0, &s) == 0);
    CHECK(s.len == 0);

    bstack_close(checked_slab_bstack_allocator_into_stack(a));
    csl_unlink(tmp); return 0;
}

static int test_alloc_write_read(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    checked_slab_bstack_allocator_t *a =
        checked_slab_bstack_allocator_new(bs, 8);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t s;
    CHECK(bstack_allocator_alloc(al, 8, &s) == 0);
    CHECK(s.len == 8);

    uint8_t wbuf[8] = {0x11,0x22,0x33,0x44,0x55,0x66,0x77,0x88};
    uint8_t rbuf[8];
    CHECK(bstack_slice_write(s, wbuf, 8) == 0);
    CHECK(bstack_slice_read(s, rbuf) == 0);
    CHECK(memcmp(wbuf, rbuf, 8) == 0);

    bstack_close(checked_slab_bstack_allocator_into_stack(a));
    csl_unlink(tmp); return 0;
}

static int test_dealloc_pushes_to_free_list(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    checked_slab_bstack_allocator_t *a =
        checked_slab_bstack_allocator_new(bs, 8);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t s1, s2;
    CHECK(bstack_allocator_alloc(al, 8, &s1) == 0);
    uint64_t off1 = s1.offset;
    CHECK(bstack_allocator_dealloc(al, s1) == 0);

    CHECK(bstack_allocator_alloc(al, 8, &s2) == 0);
    CHECK(s2.offset == off1); /* free-list reuse */
    CHECK(bstack_allocator_dealloc(al, s2) == 0);

    bstack_close(checked_slab_bstack_allocator_into_stack(a));
    csl_unlink(tmp); return 0;
}

static int test_free_list_recycles_all_blocks(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    checked_slab_bstack_allocator_t *a =
        checked_slab_bstack_allocator_new(bs, 8);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t sa, sb, sc;
    CHECK(bstack_allocator_alloc(al, 8, &sa) == 0);
    CHECK(bstack_allocator_alloc(al, 8, &sb) == 0);
    CHECK(bstack_allocator_alloc(al, 8, &sc) == 0);

    uint64_t offsets[3];
    offsets[0] = sa.offset; offsets[1] = sb.offset; offsets[2] = sc.offset;

    CHECK(bstack_allocator_dealloc(al, sa) == 0);
    CHECK(bstack_allocator_dealloc(al, sb) == 0);
    CHECK(bstack_allocator_dealloc(al, sc) == 0);

    bstack_slice_t r1, r2, r3;
    CHECK(bstack_allocator_alloc(al, 8, &r1) == 0);
    CHECK(bstack_allocator_alloc(al, 8, &r2) == 0);
    CHECK(bstack_allocator_alloc(al, 8, &r3) == 0);

    /* Every recycled offset must be one of the originals */
    uint64_t recycled[3];
    recycled[0] = r1.offset; recycled[1] = r2.offset; recycled[2] = r3.offset;

    size_t i, j, found = 0;
    for (i = 0; i < 3; i++) {
        for (j = 0; j < 3; j++) {
            if (recycled[i] == offsets[j]) { found++; break; }
        }
    }
    CHECK(found == 3);

    CHECK(bstack_allocator_dealloc(al, r1) == 0);
    CHECK(bstack_allocator_dealloc(al, r2) == 0);
    CHECK(bstack_allocator_dealloc(al, r3) == 0);

    bstack_close(checked_slab_bstack_allocator_into_stack(a));
    csl_unlink(tmp); return 0;
}

static int test_oversized_tail_dealloc_shrinks_stack(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    /* data_size=8, block_size=16: 17 bytes needs ceil((17+8)/16) = 2 blocks */
    checked_slab_bstack_allocator_t *a =
        checked_slab_bstack_allocator_new(bs, 8);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    uint64_t before;
    CHECK(bstack_len(bs, &before) == 0);

    bstack_slice_t s;
    CHECK(bstack_allocator_alloc(al, 17, &s) == 0);
    CHECK(s.len == 17);

    CHECK(bstack_allocator_dealloc(al, s) == 0);

    uint64_t after;
    CHECK(bstack_len(bs, &after) == 0);
    CHECK(after == before); /* stack fully reclaimed */

    bstack_close(checked_slab_bstack_allocator_into_stack(a));
    csl_unlink(tmp); return 0;
}

static int test_double_free_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    checked_slab_bstack_allocator_t *a =
        checked_slab_bstack_allocator_new(bs, 8);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t s;
    CHECK(bstack_allocator_alloc(al, 8, &s) == 0);
    CHECK(bstack_allocator_dealloc(al, s) == 0);
    /* Second dealloc must fail */
    CHECK(bstack_allocator_dealloc(al, s) != 0);

    bstack_close(checked_slab_bstack_allocator_into_stack(a));
    csl_unlink(tmp); return 0;
}

/* =========================================================================
 * Realloc tests
 * ====================================================================== */

static int test_realloc_same_block_grows_zeroes(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    checked_slab_bstack_allocator_t *a =
        checked_slab_bstack_allocator_new(bs, 24);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t s, s2;
    uint8_t wbuf[8]; memset(wbuf, 0xAB, 8);
    CHECK(bstack_allocator_alloc(al, 8, &s) == 0);
    CHECK(bstack_slice_write(s, wbuf, 8) == 0);

    /* Grow within same block (data_size=24, so 16 bytes still fits in 1 block) */
    CHECK(bstack_allocator_realloc(al, s, 16, &s2) == 0);
    CHECK(s2.offset == s.offset);
    CHECK(s2.len == 16);

    uint8_t rbuf[16];
    CHECK(bstack_slice_read(s2, rbuf) == 0);
    CHECK(memcmp(rbuf, wbuf, 8) == 0);
    { int ok=1,i; for(i=8;i<16;i++) if(rbuf[i]){ok=0;break;} CHECK(ok); }

    CHECK(bstack_allocator_dealloc(al, s2) == 0);
    bstack_close(checked_slab_bstack_allocator_into_stack(a));
    csl_unlink(tmp); return 0;
}

static int test_realloc_tail_grow_shrink(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    checked_slab_bstack_allocator_t *a =
        checked_slab_bstack_allocator_new(bs, 8);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t s, s2, s3;
    uint8_t wbuf[8] = {1,2,3,4,5,6,7,8};
    CHECK(bstack_allocator_alloc(al, 8, &s) == 0);
    CHECK(bstack_slice_write(s, wbuf, 8) == 0);

    /* Grow to 2 blocks (at tail) */
    CHECK(bstack_allocator_realloc(al, s, 17, &s2) == 0);
    CHECK(s2.offset == s.offset);
    CHECK(s2.len == 17);

    uint8_t rbuf[17];
    CHECK(bstack_slice_read(s2, rbuf) == 0);
    CHECK(memcmp(rbuf, wbuf, 8) == 0);

    /* Shrink back to 1 block */
    CHECK(bstack_allocator_realloc(al, s2, 5, &s3) == 0);
    CHECK(s3.offset == s.offset);
    CHECK(s3.len == 5);

    CHECK(bstack_allocator_dealloc(al, s3) == 0);
    bstack_close(checked_slab_bstack_allocator_into_stack(a));
    csl_unlink(tmp); return 0;
}

static int test_realloc_nontail_shrink(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    checked_slab_bstack_allocator_t *a =
        checked_slab_bstack_allocator_new(bs, 8);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    /* Alloc a 2-block slice, then alloc a pin to prevent it being at the tail */
    bstack_slice_t big, pin, shrank;
    uint8_t pat[8] = {0xAA,0xBB,0xCC,0xDD,0,0,0,0};
    CHECK(bstack_allocator_alloc(al, 17, &big) == 0);
    CHECK(bstack_slice_write(big, pat, 4) == 0);
    CHECK(bstack_allocator_alloc(al, 8, &pin) == 0);

    CHECK(bstack_allocator_realloc(al, big, 4, &shrank) == 0);
    CHECK(shrank.offset == big.offset);
    CHECK(shrank.len == 4);

    uint8_t rbuf[4];
    CHECK(bstack_slice_read(shrank, rbuf) == 0);
    CHECK(memcmp(rbuf, pat, 4) == 0);

    CHECK(bstack_allocator_dealloc(al, shrank) == 0);
    CHECK(bstack_allocator_dealloc(al, pin) == 0);
    bstack_close(checked_slab_bstack_allocator_into_stack(a));
    csl_unlink(tmp); return 0;
}

static int test_realloc_nontail_grow(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    checked_slab_bstack_allocator_t *a =
        checked_slab_bstack_allocator_new(bs, 8);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t small, pin, grown;
    uint8_t wbuf[8] = {1,2,3,4,5,6,7,8};
    CHECK(bstack_allocator_alloc(al, 8, &small) == 0);
    CHECK(bstack_slice_write(small, wbuf, 8) == 0);
    CHECK(bstack_allocator_alloc(al, 8, &pin) == 0);

    CHECK(bstack_allocator_realloc(al, small, 17, &grown) == 0);
    CHECK(grown.len == 17);

    uint8_t rbuf[17];
    CHECK(bstack_slice_read(grown, rbuf) == 0);
    CHECK(memcmp(rbuf, wbuf, 8) == 0);

    CHECK(bstack_allocator_dealloc(al, grown) == 0);
    CHECK(bstack_allocator_dealloc(al, pin) == 0);
    bstack_close(checked_slab_bstack_allocator_into_stack(a));
    csl_unlink(tmp); return 0;
}

static int test_realloc_to_zero_deallocates(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    checked_slab_bstack_allocator_t *a =
        checked_slab_bstack_allocator_new(bs, 8);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t s, s2;
    CHECK(bstack_allocator_alloc(al, 8, &s) == 0);
    CHECK(bstack_allocator_realloc(al, s, 0, &s2) == 0);
    CHECK(s2.len == 0);

    bstack_close(checked_slab_bstack_allocator_into_stack(a));
    csl_unlink(tmp); return 0;
}

/* =========================================================================
 * Persist / reopen
 * ====================================================================== */

static int test_data_survives_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    uint64_t saved_offset;
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        checked_slab_bstack_allocator_t *a =
            checked_slab_bstack_allocator_new(bs, 8);
        CHECK(a);
        bstack_allocator_t *al = (bstack_allocator_t *)a;
        bstack_slice_t s;
        CHECK(bstack_allocator_alloc(al, 8, &s) == 0);
        saved_offset = s.offset;
        uint8_t data[8]; memset(data, 0x42, 8);
        CHECK(bstack_slice_write(s, data, 8) == 0);
        bstack_close(checked_slab_bstack_allocator_into_stack(a));
    }
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        checked_slab_bstack_allocator_t *a =
            checked_slab_bstack_allocator_open(bs);
        CHECK(a);
        uint8_t rbuf[8];
        CHECK(bstack_get(bstack_allocator_stack((bstack_allocator_t *)a),
                         saved_offset, saved_offset + 8, rbuf) == 0);
        { int ok=1,i; for(i=0;i<8;i++) if(rbuf[i]!=0x42){ok=0;break;} CHECK(ok); }
        bstack_close(checked_slab_bstack_allocator_into_stack(a));
    }
    csl_unlink(tmp); return 0;
}

/* =========================================================================
 * Recovery smoke test
 * ====================================================================== */

static int test_recover_reclaims_leaked_block(void)
{
    /* Simulate a leaked block: allocate, then manually zero the overhead so
     * it looks free but is not in the free list.  Reopen should reclaim it. */
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    uint64_t leaked_block_start;
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        checked_slab_bstack_allocator_t *a =
            checked_slab_bstack_allocator_new(bs, 8);
        CHECK(a);
        bstack_allocator_t *al = (bstack_allocator_t *)a;

        bstack_slice_t pin, leaked;
        CHECK(bstack_allocator_alloc(al, 8, &leaked) == 0);
        CHECK(bstack_allocator_alloc(al, 8, &pin) == 0);

        leaked_block_start = leaked.offset - 8; /* block_start = offset - OVERHEAD */

        /* Corrupt the overhead to simulate a leak (clear in-use bit) */
        uint8_t zero8[8]; memset(zero8, 0, 8);
        CHECK(bstack_set(bs, leaked_block_start, zero8, 8) == 0);

        bstack_close(checked_slab_bstack_allocator_into_stack(a));
    }
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        checked_slab_bstack_allocator_t *a =
            checked_slab_bstack_allocator_open(bs); /* runs recover */
        CHECK(a);
        bstack_allocator_t *al = (bstack_allocator_t *)a;

        /* The leaked block must now be available for reallocation */
        bstack_slice_t s1, s2;
        CHECK(bstack_allocator_alloc(al, 8, &s1) == 0);
        CHECK(bstack_allocator_alloc(al, 8, &s2) == 0);

        int found = (s1.offset == leaked_block_start + 8)
                 || (s2.offset == leaked_block_start + 8);
        CHECK(found);

        CHECK(bstack_allocator_dealloc(al, s1) == 0);
        CHECK(bstack_allocator_dealloc(al, s2) == 0);
        bstack_close(checked_slab_bstack_allocator_into_stack(a));
    }
    csl_unlink(tmp); return 0;
}

/* =========================================================================
 * Fuzz tests
 * ====================================================================== */

#define FUZZ_ITERS       10000
#define FUZZ_MAX_SIZE    UINT64_C(128)   /* kept < 2*data_size for variety */
#define FUZZ_SESSIONS    20
#define FUZZ_OPS_SESSION 200

static int fuzz_alloc_dealloc(uint64_t data_size)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    uint64_t seed = (uint64_t)(unsigned long)time(NULL)
                  ^ (uint64_t)(unsigned long)csl_getpid()
                  ^ data_size;
    rng_seed(seed);

    bstack_t *bs = bstack_open(tmp);
    if (!bs) return -1;
    checked_slab_bstack_allocator_t *a =
        checked_slab_bstack_allocator_new(bs, data_size);
    if (!a) { bstack_close(bs); return -1; }
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    csl_vec_t live = {0};
    uint64_t next_id = 0;
    int ret = 0, iter;

    for (iter = 0; iter < FUZZ_ITERS; iter++) {
        if (rng_bool(0.7) || live.count == 0) {
            uint64_t size = rng_range(0, FUZZ_MAX_SIZE);
            bstack_slice_t s;
            if (bstack_allocator_alloc(al, size, &s) != 0) continue;

            uint64_t id = next_id++;

            if (size > 0) {
                uint8_t *wbuf = malloc((size_t)size);
                if (!wbuf) { ret = -1; break; }
                id_fill(wbuf, (size_t)size, id);
                if (bstack_slice_write(s, wbuf, (size_t)size) != 0) {
                    free(wbuf); ret = -1; break;
                }
                free(wbuf);
            }

            csl_entry_t e; e.offset = s.offset; e.len = size; e.id = id;
            if (csl_vec_push(&live, e) != 0) { ret = -1; break; }
        } else {
            size_t idx = (size_t)rng_range(0, (uint64_t)(live.count - 1));
            csl_entry_t e = csl_vec_swap_remove(&live, idx);
            bstack_slice_t s = entry_slice(al, e);

            if (e.len > 0) {
                uint8_t *rbuf = malloc((size_t)e.len);
                if (!rbuf) { ret = -1; break; }
                if (bstack_slice_read(s, rbuf) != 0) {
                    free(rbuf); ret = -1; break;
                }
                if (id_verify(rbuf, (size_t)e.len, e.id, "fuzz dealloc") != 0) {
                    free(rbuf); ret = -1; break;
                }
                free(rbuf);
            }
            if (bstack_allocator_dealloc(al, s) != 0) { ret = -1; break; }
        }
    }

    /* Clean up remaining live allocations */
    {
        size_t i;
        for (i = 0; i < live.count; i++)
            bstack_allocator_dealloc(al, entry_slice(al, live.data[i]));
    }
    csl_vec_free(&live);
    bstack_close(checked_slab_bstack_allocator_into_stack(a));
    csl_unlink(tmp);
    return ret;
}

static int test_fuzz_data_size_8(void)  { return fuzz_alloc_dealloc(8);  }
static int test_fuzz_data_size_16(void) { return fuzz_alloc_dealloc(16); }
static int test_fuzz_data_size_64(void) { return fuzz_alloc_dealloc(64); }

/* Fuzz with realloc mixed in */
static int fuzz_with_realloc(uint64_t data_size)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    uint64_t seed = (uint64_t)(unsigned long)time(NULL)
                  ^ (uint64_t)(unsigned long)csl_getpid()
                  ^ (data_size << 32);
    rng_seed(seed);

    bstack_t *bs = bstack_open(tmp);
    if (!bs) return -1;
    checked_slab_bstack_allocator_t *a =
        checked_slab_bstack_allocator_new(bs, data_size);
    if (!a) { bstack_close(bs); return -1; }
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    csl_vec_t live = {0};
    uint64_t next_id = 0;
    int ret = 0, iter;

    for (iter = 0; iter < FUZZ_ITERS; iter++) {
        int op = (int)rng_range(0, 2); /* 0=alloc, 1=dealloc, 2=realloc */
        if (live.count == 0) op = 0;

        if (op == 0) {
            uint64_t size = rng_range(1, FUZZ_MAX_SIZE);
            bstack_slice_t s;
            if (bstack_allocator_alloc(al, size, &s) != 0) continue;
            uint64_t id = next_id++;
            uint8_t *wbuf = malloc((size_t)size);
            if (!wbuf) { ret = -1; break; }
            id_fill(wbuf, (size_t)size, id);
            if (bstack_slice_write(s, wbuf, (size_t)size) != 0) {
                free(wbuf); ret = -1; break;
            }
            free(wbuf);
            csl_entry_t e; e.offset = s.offset; e.len = size; e.id = id;
            if (csl_vec_push(&live, e) != 0) { ret = -1; break; }

        } else if (op == 1) {
            size_t idx = (size_t)rng_range(0, (uint64_t)(live.count - 1));
            csl_entry_t e = csl_vec_swap_remove(&live, idx);
            bstack_slice_t s = entry_slice(al, e);
            uint8_t *rbuf = malloc((size_t)e.len);
            if (!rbuf) { ret = -1; break; }
            if (bstack_slice_read(s, rbuf) != 0 ||
                id_verify(rbuf, (size_t)e.len, e.id, "fuzz dealloc") != 0) {
                free(rbuf); ret = -1; break;
            }
            free(rbuf);
            if (bstack_allocator_dealloc(al, s) != 0) { ret = -1; break; }

        } else {
            size_t idx = (size_t)rng_range(0, (uint64_t)(live.count - 1));
            csl_entry_t *ep = &live.data[idx];
            uint64_t new_size = rng_range(1, FUZZ_MAX_SIZE);
            bstack_slice_t old_s = entry_slice(al, *ep);
            bstack_slice_t new_s;
            if (bstack_allocator_realloc(al, old_s, new_size, &new_s) != 0) continue;

            /* Verify old content up to min(old_len, new_size) is preserved */
            uint64_t copy_len = ep->len < new_size ? ep->len : new_size;
            if (copy_len > 0) {
                uint8_t *rbuf = malloc((size_t)copy_len);
                if (!rbuf) { ret = -1; break; }
                if (bstack_slice_read_into(new_s, rbuf, (size_t)copy_len) != 0 ||
                    id_verify(rbuf, (size_t)copy_len, ep->id, "fuzz realloc") != 0) {
                    free(rbuf); ret = -1; break;
                }
                free(rbuf);
            }

            /* Stamp the entire new slice with a fresh id so future
             * dealloc verification always checks the full length. */
            uint64_t new_id = next_id++;
            uint8_t *wbuf = malloc((size_t)new_size);
            if (!wbuf) { ret = -1; break; }
            id_fill(wbuf, (size_t)new_size, new_id);
            if (bstack_slice_write(new_s, wbuf, (size_t)new_size) != 0) {
                free(wbuf); ret = -1; break;
            }
            free(wbuf);
            ep->offset = new_s.offset;
            ep->len    = new_size;
            ep->id     = new_id;
        }
    }

    {
        size_t i;
        for (i = 0; i < live.count; i++)
            bstack_allocator_dealloc(al, entry_slice(al, live.data[i]));
    }
    csl_vec_free(&live);
    bstack_close(checked_slab_bstack_allocator_into_stack(a));
    csl_unlink(tmp);
    return ret;
}

static int test_fuzz_realloc_8(void)  { return fuzz_with_realloc(8);  }
static int test_fuzz_realloc_24(void) { return fuzz_with_realloc(24); }

/* Persist + reopen fuzz */
static int fuzz_persist_reopen(uint64_t data_size)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    uint64_t seed = (uint64_t)(unsigned long)time(NULL)
                  ^ (uint64_t)(unsigned long)csl_getpid()
                  ^ (data_size * 7919);
    rng_seed(seed);
    int ret = 0;

    {
        bstack_t *bs = bstack_open(tmp);
        if (!bs) return -1;
        checked_slab_bstack_allocator_t *a =
            checked_slab_bstack_allocator_new(bs, data_size);
        if (!a) { bstack_close(bs); return -1; }
        bstack_allocator_t *al = (bstack_allocator_t *)a;

        csl_vec_t live = {0};
        uint64_t next_id = 0;
        int i;
        for (i = 0; i < FUZZ_OPS_SESSION; i++) {
            if (rng_bool(0.6) || live.count == 0) {
                uint64_t sz = rng_range(1, FUZZ_MAX_SIZE);
                bstack_slice_t s;
                if (bstack_allocator_alloc(al, sz, &s) != 0) continue;
                uint64_t id = next_id++;
                uint8_t *wb = malloc((size_t)sz);
                if (!wb) { ret = -1; break; }
                id_fill(wb, (size_t)sz, id);
                if (bstack_slice_write(s, wb, (size_t)sz) != 0) {
                    free(wb); ret = -1; break;
                }
                free(wb);
                csl_entry_t e; e.offset = s.offset; e.len = sz; e.id = id;
                if (csl_vec_push(&live, e) != 0) { ret = -1; break; }
            } else {
                size_t idx = (size_t)rng_range(0, (uint64_t)(live.count - 1));
                csl_entry_t e = csl_vec_swap_remove(&live, idx);
                bstack_allocator_dealloc(al, entry_slice(al, e));
            }
        }

        /* Verify all surviving allocations before close */
        if (ret == 0) {
            size_t j;
            for (j = 0; j < live.count; j++) {
                csl_entry_t *ep = &live.data[j];
                uint8_t *rb = malloc((size_t)ep->len);
                if (!rb) { ret = -1; break; }
                bstack_slice_t s = entry_slice(al, *ep);
                if (bstack_slice_read(s, rb) != 0 ||
                    id_verify(rb, (size_t)ep->len, ep->id, "pre-close") != 0) {
                    free(rb); ret = -1; break;
                }
                free(rb);
            }
        }

        /* Leave all live allocations un-freed — reopen should handle it */
        csl_vec_free(&live);
        bstack_close(checked_slab_bstack_allocator_into_stack(a));
    }

    if (ret == 0) {
        bstack_t *bs = bstack_open(tmp);
        if (!bs) return -1;
        checked_slab_bstack_allocator_t *a =
            checked_slab_bstack_allocator_open(bs);
        if (!a) { bstack_close(bs); ret = -1; }
        else     bstack_close(checked_slab_bstack_allocator_into_stack(a));
    }

    csl_unlink(tmp);
    return ret;
}

static int test_fuzz_persist_8(void)  { return fuzz_persist_reopen(8);  }
static int test_fuzz_persist_16(void) { return fuzz_persist_reopen(16); }

/* =========================================================================
 * main
 * ====================================================================== */

int main(void)
{
    /* ── new() / open() ─────────────────────────────────────────────────── */
    T(test_new_initialises_header);
    T(test_new_rejects_small_data_size);
    T(test_new_rejects_nonempty_stack);
    T(test_open_rejects_empty_stack);
    T(test_open_rejects_bad_magic);
    T(test_open_rejects_misaligned_tail);
    T(test_open_restores_data_size);

    /* ── allocation behaviour ─────────────────────────────────────────── */
    T(test_zero_alloc_returns_empty);
    T(test_alloc_write_read);
    T(test_dealloc_pushes_to_free_list);
    T(test_free_list_recycles_all_blocks);
    T(test_oversized_tail_dealloc_shrinks_stack);
    T(test_double_free_returns_error);

    /* ── realloc ──────────────────────────────────────────────────────── */
    T(test_realloc_same_block_grows_zeroes);
    T(test_realloc_tail_grow_shrink);
    T(test_realloc_nontail_shrink);
    T(test_realloc_nontail_grow);
    T(test_realloc_to_zero_deallocates);

    /* ── persist / reopen ─────────────────────────────────────────────── */
    T(test_data_survives_reopen);

    /* ── recovery ─────────────────────────────────────────────────────── */
    T(test_recover_reclaims_leaked_block);

    /* ── fuzz ─────────────────────────────────────────────────────────── */
    T(test_fuzz_data_size_8);
    T(test_fuzz_data_size_16);
    T(test_fuzz_data_size_64);
    T(test_fuzz_realloc_8);
    T(test_fuzz_realloc_24);
    T(test_fuzz_persist_8);
    T(test_fuzz_persist_16);

    printf("\n%d / %d passed\n", g_passed, g_total);
    return g_passed == g_total ? 0 : 1;
}
