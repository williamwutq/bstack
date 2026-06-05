/* ghost_tree_bstack_allocator_t — smoke + fuzz tests.
 * Requires -DBSTACK_FEATURE_SET.
 * Mirrors test_first_fit.c in structure and coverage. */

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
#  define gt_unlink(p)  DeleteFileA(p)
#  define gt_getpid()   ((unsigned long)_getpid())
#else
#  include <unistd.h>
#  define gt_unlink(p)  unlink(p)
#  define gt_getpid()   ((unsigned long)getpid())
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
    snprintf(buf, n, "bstack_gt_%lu_%ld.tmp",
             (unsigned long)GetCurrentProcessId(), (long)s);
    DeleteFileA(buf);
}
#else
static void make_tmp(char *buf, size_t n)
{
    snprintf(buf, n, "/tmp/bstack_gt_XXXXXX");
    int fd = mkstemp(buf);
    if (fd >= 0) { close(fd); unlink(buf); }
}
#endif

/* =========================================================================
 * splitmix64 PRNG
 * ====================================================================== */

static uint64_t g_rng;
static void     rng_seed(uint64_t s) { g_rng = s; }

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

static int id_verify(const uint8_t *buf, size_t len, uint64_t id, const char *ctx)
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

static uint8_t pat_byte(uint8_t pattern, size_t i)
{
    return (uint8_t)((uint8_t)pattern + (uint8_t)i);
}

static void pat_fill(uint8_t *buf, size_t len, uint8_t pattern)
{
    size_t i;
    for (i = 0; i < len; i++) buf[i] = pat_byte(pattern, i);
}

static int pat_verify(const uint8_t *buf, size_t len, uint8_t pattern, const char *ctx)
{
    size_t i;
    for (i = 0; i < len; i++) {
        uint8_t want = pat_byte(pattern, i);
        if (buf[i] != want) {
            fprintf(stderr,
                "  corruption %s byte %zu: want 0x%02x got 0x%02x (pattern=%u)\n",
                ctx, i, want, buf[i], (unsigned)pattern);
            return -1;
        }
    }
    return 0;
}

/* =========================================================================
 * gt_vec_t — growable array of live-allocation records
 * ====================================================================== */

typedef struct { uint64_t offset; uint64_t len; uint64_t id; } gt_entry_t;
typedef struct { gt_entry_t *data; size_t count; size_t cap; } gt_vec_t;

static int gt_vec_push(gt_vec_t *v, gt_entry_t e)
{
    if (v->count == v->cap) {
        size_t      nc = v->cap ? v->cap * 2 : 16;
        gt_entry_t *t  = realloc(v->data, nc * sizeof *t);
        if (!t) return -1;
        v->data = t; v->cap = nc;
    }
    v->data[v->count++] = e;
    return 0;
}

static gt_entry_t gt_vec_swap_remove(gt_vec_t *v, size_t idx)
{
    gt_entry_t e = v->data[idx];
    v->data[idx] = v->data[--v->count];
    return e;
}

static void gt_vec_free(gt_vec_t *v)
{
    free(v->data); v->data = NULL; v->count = 0; v->cap = 0;
}

static bstack_slice_t entry_slice(bstack_allocator_t *a, gt_entry_t e)
{
    bstack_slice_t s;
    s.allocator = a; s.offset = e.offset; s.len = e.len;
    return s;
}

/* =========================================================================
 * Smoke tests
 * ====================================================================== */

static int test_alloc_small(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);

    bstack_slice_t s;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 5, &s) == 0);
    CHECK(s.len == 5);
    {
        uint8_t data[5] = {1, 2, 3, 4, 5};
        bstack_slice_write(s, data, 5);
    }
    CHECK(bstack_allocator_dealloc((bstack_allocator_t *)a, s) == 0);

    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

static int test_alloc_write_read(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);

    bstack_slice_t s;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 16, &s) == 0);
    {
        uint8_t wbuf[16], rbuf[16];
        memset(wbuf, 0xAB, 16);
        CHECK(bstack_slice_write(s, wbuf, 16) == 0);
        CHECK(bstack_slice_read(s, rbuf) == 0);
        CHECK(memcmp(wbuf, rbuf, 16) == 0);
    }

    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

static int test_realloc_grow(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);

    bstack_slice_t s, s2;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 32, &s) == 0);
    {
        uint8_t wbuf[32];
        memset(wbuf, 0xCD, 32);
        CHECK(bstack_slice_write(s, wbuf, 32) == 0);
        CHECK(bstack_allocator_realloc((bstack_allocator_t *)a, s, 64, &s2) == 0);
        CHECK(s2.offset == s.offset); /* tail block grows in place */
        CHECK(s2.len == 64);
        {
            uint8_t rbuf[64];
            int i, ok = 1;
            CHECK(bstack_slice_read(s2, rbuf) == 0);
            CHECK(memcmp(rbuf, wbuf, 32) == 0);
            for (i = 32; i < 64; i++) if (rbuf[i]) { ok = 0; break; }
            CHECK(ok);
        }
    }

    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

/* Verify best-fit: freed 32-byte slot is reused for a 16-byte request
 * (align_up_len(16) == 32, exact fit, no split). */
static int test_slot_reuse(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);

    bstack_slice_t sa, sb, sc;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 32, &sa) == 0);
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 32, &sb) == 0);
    {
        uint64_t a_off = sa.offset;
        CHECK(bstack_allocator_dealloc((bstack_allocator_t *)a, sa) == 0);
        /* Best-fit: the 32-byte block (only free block) is the exact fit. */
        CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 16, &sc) == 0);
        CHECK(sc.offset == a_off);
    }

    bstack_allocator_dealloc((bstack_allocator_t *)a, sc);
    bstack_allocator_dealloc((bstack_allocator_t *)a, sb);
    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

/* Verify best-fit chooses the smallest fitting block when multiple are free. */
static int test_best_fit(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);

    /* Lay out: [A:32][B:64][C:32] */
    bstack_slice_t sa, sb, sc, sd;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 32, &sa) == 0);
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 64, &sb) == 0);
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 32, &sc) == 0);
    /* Allocate a sentinel so C is not the tail */
    bstack_slice_t sentinel;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 32, &sentinel) == 0);

    uint64_t b_off = sb.offset;
    CHECK(bstack_allocator_dealloc((bstack_allocator_t *)a, sa) == 0);
    CHECK(bstack_allocator_dealloc((bstack_allocator_t *)a, sc) == 0);
    /* Free list has two 32-byte blocks and sb (64 B) is live.
     * alloc(1) → aligned 32 → best-fit picks a 32-byte block, NOT the 64-byte sb. */
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 1, &sd) == 0);
    CHECK(sd.offset != b_off); /* must not have consumed sb */
    CHECK(sd.len == 1);

    bstack_allocator_dealloc((bstack_allocator_t *)a, sd);
    bstack_allocator_dealloc((bstack_allocator_t *)a, sb);
    bstack_allocator_dealloc((bstack_allocator_t *)a, sentinel);
    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

static int test_persist_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    uint64_t saved;
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
        CHECK(a);
        bstack_slice_t s;
        CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 24, &s) == 0);
        saved = s.offset;
        {
            uint8_t data[24];
            memset(data, 0x77, 24);
            CHECK(bstack_slice_write(s, data, 24) == 0);
        }
        bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    }
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
        CHECK(a);
        {
            uint8_t rbuf[24];
            int ok = 1, i;
            CHECK(bstack_get(bstack_allocator_stack((bstack_allocator_t *)a),
                             saved, saved + 24, rbuf) == 0);
            for (i = 0; i < 24; i++) if (rbuf[i] != 0x77) { ok = 0; break; }
            CHECK(ok);
        }
        /* Previous block still live; new alloc goes to a fresh slot. */
        bstack_slice_t s;
        CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 24, &s) == 0);
        CHECK(s.offset != saved);
        bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    }
    gt_unlink(tmp);
    return 0;
}

static int test_realloc_small(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);

    bstack_slice_t s, s2;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 5, &s) == 0);
    {
        uint8_t wbuf[5] = {10, 20, 30, 40, 50};
        CHECK(bstack_slice_write(s, wbuf, 5) == 0);
        CHECK(bstack_allocator_realloc((bstack_allocator_t *)a, s, 10, &s2) == 0);
        CHECK(s2.len == 10);
        {
            uint8_t rbuf[10];
            int ok = 1, i;
            CHECK(bstack_slice_read(s2, rbuf) == 0);
            CHECK(memcmp(rbuf, wbuf, 5) == 0);
            for (i = 5; i < 10; i++) if (rbuf[i]) { ok = 0; break; }
            CHECK(ok);
        }
    }
    CHECK(bstack_allocator_dealloc((bstack_allocator_t *)a, s2) == 0);
    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

static int test_slice_read_range(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);

    bstack_slice_t s;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 16, &s) == 0);
    {
        uint8_t wbuf[16], rbuf[16];
        int i;
        for (i = 0; i < 16; i++) wbuf[i] = (uint8_t)(i * 3);
        CHECK(bstack_slice_write(s, wbuf, 16) == 0);

        memset(rbuf, 0, sizeof rbuf);
        CHECK(bstack_slice_read_range(s, 0, 16, rbuf) == 0);
        CHECK(memcmp(rbuf, wbuf, 16) == 0);

        memset(rbuf, 0, sizeof rbuf);
        CHECK(bstack_slice_read_range(s, 4, 12, rbuf) == 0);
        CHECK(memcmp(rbuf, wbuf + 4, 8) == 0);

        CHECK(bstack_slice_read_range(s, 7, 7, rbuf) == 0);
        CHECK(bstack_slice_read_range(s, 10, 4, rbuf) == -1);
        CHECK(bstack_slice_read_range(s, 0, 17, rbuf) == -1);
    }

    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

/* =========================================================================
 * Unit tests
 * ====================================================================== */

#define ALGT_MIN_ALLOC UINT64_C(32)

/* ── basic alloc / dealloc ─────────────────────────────────────────────── */

static int test_alloc_returns_zeroed(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t s;
    CHECK(bstack_allocator_alloc(al, 64, &s) == 0);
    CHECK(s.len == 64);
    {
        uint8_t buf[64]; int ok = 1, i;
        CHECK(bstack_slice_read(s, buf) == 0);
        for (i = 0; i < 64; i++) if (buf[i]) { ok = 0; break; }
        CHECK(ok);
    }

    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

static int test_alloc_zero_len(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t s;
    CHECK(bstack_allocator_alloc(al, 0, &s) == 0);
    CHECK(s.len == 0);
    CHECK(s.offset == 0);

    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

static int test_dealloc_zero_len_noop(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    uint64_t before, after;
    CHECK(bstack_allocator_len(al, &before) == 0);
    bstack_slice_t s;
    CHECK(bstack_allocator_alloc(al, 0, &s) == 0);
    CHECK(bstack_allocator_dealloc(al, s) == 0);
    CHECK(bstack_allocator_len(al, &after) == 0);
    CHECK(after == before);

    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

static int test_dealloc_tail_shrinks_stack(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    uint64_t before, after_alloc, after_dealloc;
    CHECK(bstack_allocator_len(al, &before) == 0);
    bstack_slice_t s;
    CHECK(bstack_allocator_alloc(al, 64, &s) == 0);
    CHECK(bstack_allocator_len(al, &after_alloc) == 0);
    CHECK(after_alloc > before);
    CHECK(bstack_allocator_dealloc(al, s) == 0);
    CHECK(bstack_allocator_len(al, &after_dealloc) == 0);
    CHECK(after_dealloc == before);

    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

static int test_dealloc_nontail_reuses(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t sa, sb, sc;
    CHECK(bstack_allocator_alloc(al, 64, &sa) == 0);
    CHECK(bstack_allocator_alloc(al, 64, &sb) == 0);
    uint64_t a_start = sa.offset;
    CHECK(bstack_allocator_dealloc(al, sa) == 0);
    uint64_t stack_len, stack_len2;
    CHECK(bstack_allocator_len(al, &stack_len) == 0);
    CHECK(bstack_allocator_alloc(al, 64, &sc) == 0);
    CHECK(sc.offset == a_start);
    CHECK(bstack_allocator_len(al, &stack_len2) == 0);
    CHECK(stack_len2 == stack_len);

    bstack_allocator_dealloc(al, sc);
    bstack_allocator_dealloc(al, sb);
    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

static int test_freed_block_is_zeroed(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t sa, sb;
    CHECK(bstack_allocator_alloc(al, 64, &sa) == 0);
    CHECK(bstack_allocator_alloc(al, 64, &sb) == 0);
    uint64_t a_start = sa.offset;
    {
        uint8_t fill[64];
        memset(fill, 0xAA, 64);
        CHECK(bstack_slice_write(sa, fill, 64) == 0);
    }
    CHECK(bstack_allocator_dealloc(al, sa) == 0);
    /* AVL node header occupies [0..32]; bytes [32..64] must be zero. */
    {
        uint8_t raw[64]; int ok = 1, i;
        CHECK(bstack_get(bstack_allocator_stack(al), a_start, a_start + 64, raw) == 0);
        for (i = 32; i < 64; i++) if (raw[i]) { ok = 0; break; }
        CHECK(ok);
    }

    bstack_allocator_dealloc(al, sb);
    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

/* ── alignment ─────────────────────────────────────────────────────────── */

static int test_alignment(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t slices[16]; int i;
    for (i = 0; i < 16; i++) {
        uint64_t len = (uint64_t)(i * 7 + 1);
        CHECK(bstack_allocator_alloc(al, len, &slices[i]) == 0);
        /* ARENA_START=48; payload offsets ≡ 16 (mod 32) → 32-byte aligned on disk */
        CHECK(slices[i].offset % 32 == 16);
    }
    for (i = 0; i < 16; i++) bstack_allocator_dealloc(al, slices[i]);

    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

static int test_alloc_rounds_up_to_min_alloc(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    uint64_t before, after;
    CHECK(bstack_allocator_len(al, &before) == 0);
    bstack_slice_t s;
    CHECK(bstack_allocator_alloc(al, 1, &s) == 0);
    CHECK(bstack_allocator_len(al, &after) == 0);
    CHECK(after - before == ALGT_MIN_ALLOC);
    bstack_allocator_dealloc(al, s);

    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

/* ── split behaviour ───────────────────────────────────────────────────── */

static int test_large_free_block_is_split(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t sa, anchor, sb, sc;
    CHECK(bstack_allocator_alloc(al, 128, &sa) == 0);
    CHECK(bstack_allocator_alloc(al, 32, &anchor) == 0);
    uint64_t a_start = sa.offset;
    CHECK(bstack_allocator_dealloc(al, sa) == 0);
    /* Split: 128-byte block → 96-byte remainder + 32-byte allocation */
    CHECK(bstack_allocator_alloc(al, 32, &sb) == 0);
    CHECK(sb.offset == a_start + 96);
    /* Remainder (96 bytes) can be reused immediately */
    CHECK(bstack_allocator_alloc(al, 96, &sc) == 0);
    CHECK(sc.offset == a_start);

    bstack_allocator_dealloc(al, sb);
    bstack_allocator_dealloc(al, sc);
    bstack_allocator_dealloc(al, anchor);
    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

/* ── realloc ───────────────────────────────────────────────────────────── */

static int test_realloc_to_zero(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    uint64_t before, after;
    CHECK(bstack_allocator_len(al, &before) == 0);
    bstack_slice_t s, s2;
    CHECK(bstack_allocator_alloc(al, 64, &s) == 0);
    CHECK(bstack_allocator_realloc(al, s, 0, &s2) == 0);
    CHECK(s2.len == 0);
    CHECK(bstack_allocator_len(al, &after) == 0);
    CHECK(after == before);

    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

static int test_realloc_same_aligned_size(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t s, s2;
    CHECK(bstack_allocator_alloc(al, 32, &s) == 0);
    {
        uint8_t fill[32];
        memset(fill, 0x5A, 32);
        CHECK(bstack_slice_write(s, fill, 32) == 0);
    }
    uint64_t start = s.offset;
    /* Realloc 32→16: same aligned block size (32), tail bytes get zeroed */
    CHECK(bstack_allocator_realloc(al, s, 16, &s2) == 0);
    CHECK(s2.offset == start);
    {
        uint8_t buf[16]; int ok = 1, i;
        CHECK(bstack_slice_read(s2, buf) == 0);
        for (i = 0; i < 16; i++) if (buf[i] != 0x5A) { ok = 0; break; }
        CHECK(ok);
    }
    bstack_allocator_dealloc(al, s2);

    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

static int test_realloc_shrink_tail(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t s, s2;
    CHECK(bstack_allocator_alloc(al, 128, &s) == 0);
    uint64_t start = s.offset;
    {
        uint8_t fill[128];
        memset(fill, 0xBB, 128);
        CHECK(bstack_slice_write(s, fill, 128) == 0);
    }
    CHECK(bstack_allocator_realloc(al, s, 32, &s2) == 0);
    CHECK(s2.offset == start);
    uint64_t stack_len;
    CHECK(bstack_allocator_len(al, &stack_len) == 0);
    CHECK(stack_len == start + 32);
    {
        uint8_t buf[32]; int ok = 1, i;
        CHECK(bstack_slice_read(s2, buf) == 0);
        for (i = 0; i < 32; i++) if (buf[i] != 0xBB) { ok = 0; break; }
        CHECK(ok);
    }
    bstack_allocator_dealloc(al, s2);

    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

static int test_realloc_shrink_nontail(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t s, anchor, s2, r;
    CHECK(bstack_allocator_alloc(al, 128, &s) == 0);
    CHECK(bstack_allocator_alloc(al, 32, &anchor) == 0);
    uint64_t start = s.offset;
    {
        uint8_t fill[128];
        memset(fill, 0xCC, 128);
        CHECK(bstack_slice_write(s, fill, 128) == 0);
    }
    uint64_t stack_len, stack_len2;
    CHECK(bstack_allocator_len(al, &stack_len) == 0);
    CHECK(bstack_allocator_realloc(al, s, 32, &s2) == 0);
    CHECK(s2.offset == start);
    CHECK(bstack_allocator_len(al, &stack_len2) == 0);
    CHECK(stack_len2 == stack_len);
    CHECK(bstack_allocator_alloc(al, 96, &r) == 0);
    CHECK(r.offset == start + 32);

    bstack_allocator_dealloc(al, s2);
    bstack_allocator_dealloc(al, r);
    bstack_allocator_dealloc(al, anchor);
    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

static int test_realloc_grow_tail(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t s, s2;
    CHECK(bstack_allocator_alloc(al, 32, &s) == 0);
    uint64_t start = s.offset;
    {
        uint8_t fill[32];
        memset(fill, 0xDD, 32);
        CHECK(bstack_slice_write(s, fill, 32) == 0);
    }
    CHECK(bstack_allocator_realloc(al, s, 96, &s2) == 0);
    CHECK(s2.offset == start);
    {
        uint8_t buf[96]; int ok = 1, i;
        CHECK(bstack_slice_read(s2, buf) == 0);
        for (i = 0;  i < 32; i++) if (buf[i] != 0xDD) { ok = 0; break; }
        for (i = 32; i < 96; i++) if (buf[i] != 0x00) { ok = 0; break; }
        CHECK(ok);
    }
    bstack_allocator_dealloc(al, s2);

    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

static int test_realloc_grow_nontail(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t s, anchor, s2;
    CHECK(bstack_allocator_alloc(al, 32, &s) == 0);
    CHECK(bstack_allocator_alloc(al, 32, &anchor) == 0);
    {
        uint8_t fill[32];
        memset(fill, 0xEE, 32);
        CHECK(bstack_slice_write(s, fill, 32) == 0);
    }
    CHECK(bstack_allocator_realloc(al, s, 96, &s2) == 0);
    CHECK(s2.offset != anchor.offset);
    {
        uint8_t buf[96]; int ok = 1, i;
        CHECK(bstack_slice_read(s2, buf) == 0);
        for (i = 0;  i < 32; i++) if (buf[i] != 0xEE) { ok = 0; break; }
        for (i = 32; i < 96; i++) if (buf[i] != 0x00) { ok = 0; break; }
        CHECK(ok);
    }

    bstack_allocator_dealloc(al, s2);
    bstack_allocator_dealloc(al, anchor);
    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

/* ── invalid input ─────────────────────────────────────────────────────── */

static int test_dealloc_misaligned_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t s;
    CHECK(bstack_allocator_alloc(al, 64, &s) == 0);
    {
        bstack_slice_t bad;
        bad.allocator = al; bad.offset = s.offset + 1; bad.len = 32;
        CHECK(bstack_allocator_dealloc(al, bad) == -1);
    }
    bstack_allocator_dealloc(al, s);

    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

static int test_realloc_misaligned_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t s;
    CHECK(bstack_allocator_alloc(al, 64, &s) == 0);
    {
        bstack_slice_t bad, out;
        bad.allocator = al; bad.offset = s.offset + 1; bad.len = 32;
        CHECK(bstack_allocator_realloc(al, bad, 64, &out) == -1);
    }
    bstack_allocator_dealloc(al, s);

    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

/* ── alloc_bulk / dealloc_bulk ─────────────────────────────────────────── */

static int test_alloc_bulk_contiguous(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    uint64_t lens[3] = {32, 64, 32};
    bstack_slice_t slices[3];
    CHECK(bstack_allocator_alloc_bulk(al, lens, 3, slices) == 0);
    CHECK(slices[0].len == 32);
    CHECK(slices[1].len == 64);
    CHECK(slices[2].len == 32);
    CHECK(slices[1].offset == slices[0].offset + 32);
    CHECK(slices[2].offset == slices[1].offset + 64);
    { int i; for (i = 0; i < 3; i++) bstack_allocator_dealloc(al, slices[i]); }

    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

static int test_alloc_bulk_with_zeros(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    uint64_t lens[3] = {0, 32, 0};
    bstack_slice_t slices[3];
    CHECK(bstack_allocator_alloc_bulk(al, lens, 3, slices) == 0);
    CHECK(slices[0].len == 0);
    CHECK(slices[0].offset == 0);
    CHECK(slices[2].len == 0);
    CHECK(slices[2].offset == 0);
    CHECK(slices[1].len == 32);
    { int i; for (i = 0; i < 3; i++) bstack_allocator_dealloc(al, slices[i]); }

    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

static int test_dealloc_bulk_merges(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    uint64_t before, after;
    CHECK(bstack_allocator_len(al, &before) == 0);
    uint64_t lens[3] = {64, 64, 64};
    bstack_slice_t slices[3];
    CHECK(bstack_allocator_alloc_bulk(al, lens, 3, slices) == 0);
    CHECK(bstack_allocator_dealloc_bulk(al, slices, 3) == 0);
    CHECK(bstack_allocator_len(al, &after) == 0);
    CHECK(after == before);

    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return 0;
}

/* ── coalesce and rebalance on reopen ──────────────────────────────────── */

static int test_coalesce_on_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    uint64_t a_start, anchor_start;

    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
        CHECK(a);
        bstack_allocator_t *al = (bstack_allocator_t *)a;
        bstack_slice_t sa, sb, anchor;
        CHECK(bstack_allocator_alloc(al, 64, &sa) == 0);
        CHECK(bstack_allocator_alloc(al, 64, &sb) == 0);
        CHECK(bstack_allocator_alloc(al, 32, &anchor) == 0);
        a_start = sa.offset;
        anchor_start = anchor.offset;
        CHECK(bstack_allocator_dealloc(al, sa) == 0);
        CHECK(bstack_allocator_dealloc(al, sb) == 0);
        /* anchor is live — not freed */
        bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    }

    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
        CHECK(a);
        bstack_allocator_t *al = (bstack_allocator_t *)a;
        /* On reopen, coalesce_and_rebalance merges the two 64-byte free blocks. */
        bstack_slice_t c;
        CHECK(bstack_allocator_alloc(al, 128, &c) == 0);
        CHECK(c.offset == a_start);
        CHECK(bstack_allocator_dealloc(al, c) == 0);
        {
            bstack_slice_t anchor2;
            anchor2.allocator = al; anchor2.offset = anchor_start; anchor2.len = 32;
            CHECK(bstack_allocator_dealloc(al, anchor2) == 0);
        }
        bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    }

    gt_unlink(tmp);
    return 0;
}

static int test_data_survives_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    uint64_t start;

    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
        CHECK(a);
        bstack_allocator_t *al = (bstack_allocator_t *)a;
        bstack_slice_t s;
        CHECK(bstack_allocator_alloc(al, 64, &s) == 0);
        start = s.offset;
        {
            uint8_t fill[64];
            memset(fill, 0xAB, 64);
            CHECK(bstack_slice_write(s, fill, 64) == 0);
        }
        bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    }

    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
        CHECK(a);
        bstack_allocator_t *al = (bstack_allocator_t *)a;
        {
            bstack_slice_t s;
            s.allocator = al; s.offset = start; s.len = 64;
            uint8_t buf[64]; int ok = 1, i;
            CHECK(bstack_slice_read(s, buf) == 0);
            for (i = 0; i < 64; i++) if (buf[i] != 0xAB) { ok = 0; break; }
            CHECK(ok);
            bstack_allocator_dealloc(al, s);
        }
        bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    }

    gt_unlink(tmp);
    return 0;
}

/* ── new() error cases ─────────────────────────────────────────────────── */

static int test_new_rejects_partial_header(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    /* 4 bytes — too small for the 48-byte ghost tree header */
    CHECK(bstack_extend(bs, 4, NULL) == 0);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a == NULL);
    /* ghost_tree_bstack_allocator_new returns NULL without consuming bs */
    bstack_close(bs);
    gt_unlink(tmp);
    return 0;
}

/* =========================================================================
 * Fuzz tests
 * ====================================================================== */

#define FUZZ_ITERS       10000
#define FUZZ_MIN_SIZE    UINT64_C(1)
#define FUZZ_MAX_SIZE    UINT64_C(1024)
#define FUZZ_SESSIONS    20
#define FUZZ_OPS_SESSION 100

static int test_fuzz_alloc_dealloc(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    uint64_t seed = (uint64_t)(unsigned long)time(NULL)
                  ^ (uint64_t)(unsigned long)gt_getpid();
    rng_seed(seed);

    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    gt_vec_t live = {0};
    uint64_t next_id = 0;
    int ret = 0, iter;

    for (iter = 0; iter < FUZZ_ITERS; iter++) {
        if (rng_bool(0.7) || live.count == 0) {
            uint64_t size = rng_range(FUZZ_MIN_SIZE, FUZZ_MAX_SIZE);
            bstack_slice_t s;
            if (bstack_allocator_alloc(al, size, &s) != 0) continue;

            uint64_t id = next_id++;
            uint8_t *buf = malloc((size_t)size);
            if (!buf) { ret = -1; break; }
            id_fill(buf, (size_t)size, id);
            bstack_slice_write(s, buf, (size_t)size);
            free(buf);

            gt_entry_t e; e.offset = s.offset; e.len = s.len; e.id = id;
            if (gt_vec_push(&live, e) != 0) { ret = -1; break; }
        } else {
            size_t idx = (size_t)(rng_next() % live.count);
            gt_entry_t e = gt_vec_swap_remove(&live, idx);
            bstack_slice_t s = entry_slice(al, e);

            uint8_t *buf = malloc((size_t)e.len);
            if (!buf) { ret = -1; break; }
            if (bstack_slice_read(s, buf) != 0 ||
                id_verify(buf, (size_t)e.len, e.id, "dealloc") != 0) {
                free(buf);
                fprintf(stderr, "  seed=%llu iter=%d\n",
                        (unsigned long long)seed, iter);
                ret = -1; break;
            }
            free(buf);
            if (bstack_allocator_dealloc(al, s) != 0) { ret = -1; break; }
        }
    }

    { size_t i; for (i = 0; i < live.count; i++)
        bstack_allocator_dealloc(al, entry_slice(al, live.data[i])); }
    gt_vec_free(&live);
    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return ret;
}

static int test_fuzz_alloc_realloc_dealloc(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    uint64_t seed = (uint64_t)(unsigned long)time(NULL)
                  ^ (uint64_t)(unsigned long)gt_getpid()
                  ^ UINT64_C(0xdeadbeef);
    rng_seed(seed);

    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    gt_vec_t live = {0};
    uint64_t next_id = 0;
    int ret = 0, iter;

    for (iter = 0; iter < FUZZ_ITERS; iter++) {
        if (rng_bool(0.6) || live.count == 0) {
            uint64_t size = rng_range(FUZZ_MIN_SIZE, FUZZ_MAX_SIZE);
            bstack_slice_t s;
            if (bstack_allocator_alloc(al, size, &s) != 0) continue;

            uint64_t id = next_id++;
            uint8_t *buf = malloc((size_t)size);
            if (!buf) { ret = -1; break; }
            id_fill(buf, (size_t)size, id);
            bstack_slice_write(s, buf, (size_t)size);
            free(buf);

            gt_entry_t e; e.offset = s.offset; e.len = s.len; e.id = id;
            if (gt_vec_push(&live, e) != 0) { ret = -1; break; }
        } else {
            size_t idx = (size_t)(rng_next() % live.count);
            gt_entry_t old_e = live.data[idx];
            bstack_slice_t old_s = entry_slice(al, old_e);

            uint8_t *old_buf = malloc((size_t)old_e.len);
            if (!old_buf) { ret = -1; break; }
            if (bstack_slice_read(old_s, old_buf) != 0 ||
                id_verify(old_buf, (size_t)old_e.len, old_e.id, "pre-op") != 0) {
                free(old_buf);
                fprintf(stderr, "  seed=%llu iter=%d\n",
                        (unsigned long long)seed, iter);
                ret = -1; break;
            }
            free(old_buf);

            if (rng_bool(0.8)) {
                uint64_t new_size = rng_range(FUZZ_MIN_SIZE, FUZZ_MAX_SIZE);
                bstack_slice_t new_s;
                if (bstack_allocator_realloc(al, old_s, new_size, &new_s) == 0) {
                    uint64_t verify_len =
                        old_e.len < new_size ? old_e.len : new_size;
                    uint8_t *new_buf = malloc((size_t)new_size);
                    if (!new_buf) { ret = -1; break; }
                    if (bstack_slice_read(new_s, new_buf) != 0) {
                        free(new_buf); ret = -1; break;
                    }
                    if (id_verify(new_buf, (size_t)verify_len,
                                  old_e.id, "realloc-prefix") != 0) {
                        free(new_buf);
                        fprintf(stderr, "  seed=%llu iter=%d\n",
                                (unsigned long long)seed, iter);
                        ret = -1; break;
                    }
                    if (new_size > old_e.len) {
                        uint64_t i;
                        for (i = old_e.len; i < new_size; i++) {
                            if (new_buf[i] != 0) {
                                fprintf(stderr,
                                    "  not zero-init byte %llu after realloc"
                                    " (seed=%llu iter=%d)\n",
                                    (unsigned long long)i,
                                    (unsigned long long)seed, iter);
                                free(new_buf); ret = -1; break;
                            }
                        }
                        if (ret != 0) break;
                    }
                    id_fill(new_buf, (size_t)new_size, old_e.id);
                    bstack_slice_write(new_s, new_buf, (size_t)new_size);
                    free(new_buf);

                    live.data[idx].offset = new_s.offset;
                    live.data[idx].len    = new_s.len;
                }
            } else {
                gt_vec_swap_remove(&live, idx);
                if (bstack_allocator_dealloc(al, old_s) != 0) {
                    ret = -1; break;
                }
            }
        }
    }

    { size_t i; for (i = 0; i < live.count; i++)
        bstack_allocator_dealloc(al, entry_slice(al, live.data[i])); }
    gt_vec_free(&live);
    bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    gt_unlink(tmp);
    return ret;
}

static int test_fuzz_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    uint64_t seed = (uint64_t)(unsigned long)time(NULL)
                  ^ (uint64_t)(unsigned long)gt_getpid()
                  ^ UINT64_C(0xcafef00d);
    rng_seed(seed);

    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
        CHECK(a);
        bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    }

    typedef struct { uint64_t start; uint64_t len; uint8_t pattern; } rec_t;
    rec_t  *live     = NULL;
    size_t  live_cnt = 0, live_cap = 0;
    uint8_t next_pat = 1;
    int     ret      = 0;
    int     session;

    for (session = 0; session < FUZZ_SESSIONS && ret == 0; session++) {
        bstack_t *bs = bstack_open(tmp);
        if (!bs) { ret = -1; break; }
        ghost_tree_bstack_allocator_t *a = ghost_tree_bstack_allocator_new(bs);
        if (!a) { bstack_close(bs); ret = -1; break; }
        bstack_allocator_t *al = (bstack_allocator_t *)a;

        /* Verify all live records survived the reopen */
        {
            size_t i;
            for (i = 0; i < live_cnt && ret == 0; i++) {
                rec_t rec = live[i];
                bstack_slice_t s;
                s.allocator = al; s.offset = rec.start; s.len = rec.len;
                uint8_t *buf = malloc((size_t)rec.len);
                if (!buf) { ret = -1; break; }
                if (bstack_slice_read(s, buf) != 0 ||
                    pat_verify(buf, (size_t)rec.len,
                               rec.pattern, "after-reopen") != 0) {
                    fprintf(stderr, "  session=%d slot=%zu seed=%llu\n",
                            session, i, (unsigned long long)seed);
                    ret = -1;
                }
                free(buf);
            }
        }
        if (ret != 0) { bstack_close(ghost_tree_bstack_allocator_into_stack(a)); break; }

        {
            int op;
            for (op = 0; op < FUZZ_OPS_SESSION && ret == 0; op++) {
                unsigned choice = live_cnt == 0 ? 0u
                                : (unsigned)(rng_next() % 4u);

                if (choice == 0) {
                    uint64_t len = rng_range(FUZZ_MIN_SIZE, 512);
                    bstack_slice_t s;
                    if (bstack_allocator_alloc(al, len, &s) != 0) continue;

                    uint8_t pat = next_pat;
                    next_pat = (uint8_t)(next_pat == 255 ? 1 : next_pat + 1);

                    uint8_t *buf = malloc((size_t)len);
                    if (!buf) { ret = -1; break; }
                    pat_fill(buf, (size_t)len, pat);
                    bstack_slice_write(s, buf, (size_t)len);
                    free(buf);

                    if (live_cnt == live_cap) {
                        size_t nc = live_cap ? live_cap * 2 : 16;
                        rec_t *t  = realloc(live, nc * sizeof *t);
                        if (!t) { ret = -1; break; }
                        live = t; live_cap = nc;
                    }
                    live[live_cnt].start   = s.offset;
                    live[live_cnt].len     = s.len;
                    live[live_cnt].pattern = pat;
                    live_cnt++;

                } else if (choice == 1) {
                    size_t idx = (size_t)(rng_next() % live_cnt);
                    rec_t *rec = &live[idx];
                    uint64_t new_len = rng_range(FUZZ_MIN_SIZE, 512);
                    bstack_slice_t old_s, new_s;
                    old_s.allocator = al;
                    old_s.offset    = rec->start;
                    old_s.len       = rec->len;
                    if (bstack_allocator_realloc(al, old_s, new_len, &new_s) != 0)
                        continue;

                    uint64_t overlap = rec->len < new_len ? rec->len : new_len;
                    uint8_t *buf = malloc((size_t)new_len);
                    if (!buf) { ret = -1; break; }
                    if (bstack_slice_read(new_s, buf) != 0 ||
                        pat_verify(buf, (size_t)overlap,
                                   rec->pattern, "realloc-prefix") != 0) {
                        free(buf);
                        fprintf(stderr, "  session=%d op=%d seed=%llu\n",
                                session, op, (unsigned long long)seed);
                        ret = -1; break;
                    }
                    uint8_t pat = next_pat;
                    next_pat = (uint8_t)(next_pat == 255 ? 1 : next_pat + 1);
                    pat_fill(buf, (size_t)new_len, pat);
                    bstack_slice_write(new_s, buf, (size_t)new_len);
                    free(buf);
                    rec->start   = new_s.offset;
                    rec->len     = new_s.len;
                    rec->pattern = pat;

                } else if (choice == 2) {
                    size_t idx = (size_t)(rng_next() % live_cnt);
                    rec_t rec  = live[idx];
                    live[idx]  = live[--live_cnt];
                    bstack_slice_t s;
                    s.allocator = al; s.offset = rec.start; s.len = rec.len;

                    uint8_t *buf = malloc((size_t)rec.len);
                    if (!buf) { ret = -1; break; }
                    if (bstack_slice_read(s, buf) != 0 ||
                        pat_verify(buf, (size_t)rec.len,
                                   rec.pattern, "before-dealloc") != 0) {
                        free(buf);
                        fprintf(stderr, "  session=%d op=%d seed=%llu\n",
                                session, op, (unsigned long long)seed);
                        ret = -1; break;
                    }
                    free(buf);
                    bstack_allocator_dealloc(al, s);

                } else {
                    size_t idx = (size_t)(rng_next() % live_cnt);
                    rec_t rec  = live[idx];
                    bstack_slice_t s;
                    s.allocator = al; s.offset = rec.start; s.len = rec.len;

                    uint8_t *buf = malloc((size_t)rec.len);
                    if (!buf) { ret = -1; break; }
                    if (bstack_slice_read(s, buf) != 0 ||
                        pat_verify(buf, (size_t)rec.len,
                                   rec.pattern, "verify-only") != 0) {
                        free(buf);
                        fprintf(stderr, "  session=%d op=%d seed=%llu\n",
                                session, op, (unsigned long long)seed);
                        ret = -1; break;
                    }
                    free(buf);
                }
            }
        }

        bstack_close(ghost_tree_bstack_allocator_into_stack(a));
    }

    free(live);
    gt_unlink(tmp);
    return ret;
}

/* =========================================================================
 * main
 * ====================================================================== */

int main(void)
{
    /* Unit */
    T(test_alloc_returns_zeroed);
    T(test_alloc_zero_len);
    T(test_dealloc_zero_len_noop);
    T(test_dealloc_tail_shrinks_stack);
    T(test_dealloc_nontail_reuses);
    T(test_freed_block_is_zeroed);
    T(test_alignment);
    T(test_alloc_rounds_up_to_min_alloc);
    T(test_large_free_block_is_split);
    T(test_realloc_to_zero);
    T(test_realloc_same_aligned_size);
    T(test_realloc_shrink_tail);
    T(test_realloc_shrink_nontail);
    T(test_realloc_grow_tail);
    T(test_realloc_grow_nontail);
    T(test_dealloc_misaligned_error);
    T(test_realloc_misaligned_error);
    T(test_alloc_bulk_contiguous);
    T(test_alloc_bulk_with_zeros);
    T(test_dealloc_bulk_merges);
    T(test_coalesce_on_reopen);
    T(test_data_survives_reopen);
    T(test_new_rejects_partial_header);

    /* Smoke */
    T(test_alloc_small);
    T(test_alloc_write_read);
    T(test_realloc_grow);
    T(test_slot_reuse);
    T(test_best_fit);
    T(test_persist_reopen);
    T(test_realloc_small);
    T(test_slice_read_range);

    /* Fuzz */
    T(test_fuzz_alloc_dealloc);
    T(test_fuzz_alloc_realloc_dealloc);
    T(test_fuzz_reopen);

    printf("\n%d/%d passed\n", g_passed, g_total);
    return (g_passed == g_total) ? 0 : 1;
}
