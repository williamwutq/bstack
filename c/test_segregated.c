/* segregated_bstack_allocator_t — smoke + fuzz tests.
 * Requires -DBSTACK_FEATURE_SET.
 * Mirrors test_checked_slab.c in structure; ports the behavioural coverage of
 * the Rust segregated unit tests (classification, reuse, cross-class realloc,
 * oversized blocks, reopen/recover). */

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
#  define sg_unlink(p)  DeleteFileA(p)
#else
#  include <unistd.h>
#  define sg_unlink(p)  unlink(p)
#endif

/* ARENA_START = round_up(40 + 33*8, 16) = 304 */
#define SG_ARENA_START 304

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
    snprintf(buf, n, "bstack_sg_%lu_%ld.tmp",
             (unsigned long)GetCurrentProcessId(), (long)s);
    DeleteFileA(buf);
}
#else
static void make_tmp(char *buf, size_t n)
{
    snprintf(buf, n, "/tmp/bstack_sg_XXXXXX");
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

/* =========================================================================
 * Data-pattern helpers
 * ====================================================================== */

static uint8_t id_byte(uint64_t id, size_t i)
{
    return (uint8_t)(((id + i) * UINT64_C(0x9e3779b1)) >> 24);
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

/* Fill a live slice with its id pattern. */
static int slice_fill(bstack_slice_t s, uint64_t id)
{
    uint8_t *tmp;
    int r;
    if (s.len == 0) return 0;
    tmp = malloc((size_t)s.len);
    if (!tmp) return -1;
    id_fill(tmp, (size_t)s.len, id);
    r = bstack_slice_write(s, tmp, (size_t)s.len);
    free(tmp);
    return r;
}

/* Verify the first `len` bytes of a slice against its id pattern. */
static int slice_verify(bstack_slice_t s, uint64_t len, uint64_t id,
                        const char *ctx)
{
    uint8_t *tmp;
    int r;
    if (len == 0) return 0;
    tmp = malloc((size_t)len);
    if (!tmp) return -1;
    if (bstack_slice_read_into(s, tmp, (size_t)len) != 0) { free(tmp); return -1; }
    r = id_verify(tmp, (size_t)len, id, ctx);
    free(tmp);
    return r;
}

/* =========================================================================
 * sg_vec_t — growable array of live-allocation records
 * ====================================================================== */

typedef struct { uint64_t offset; uint64_t len; uint64_t id; } sg_entry_t;
typedef struct { sg_entry_t *data; size_t count; size_t cap; } sg_vec_t;

static int sg_vec_push(sg_vec_t *v, sg_entry_t e)
{
    if (v->count == v->cap) {
        size_t nc = v->cap ? v->cap * 2 : 16;
        sg_entry_t *t = realloc(v->data, nc * sizeof *t);
        if (!t) return -1;
        v->data = t; v->cap = nc;
    }
    v->data[v->count++] = e;
    return 0;
}
static sg_entry_t sg_vec_swap_remove(sg_vec_t *v, size_t idx)
{
    sg_entry_t e = v->data[idx];
    v->data[idx] = v->data[--v->count];
    return e;
}
static void sg_vec_free(sg_vec_t *v)
{
    free(v->data); v->data = NULL; v->count = 0; v->cap = 0;
}
static bstack_slice_t entry_slice(bstack_allocator_t *a, sg_entry_t e)
{
    bstack_slice_t s; s.allocator = a; s.offset = e.offset; s.len = e.len;
    return s;
}

/* =========================================================================
 * new() / reopen validation
 * ====================================================================== */

static int test_new_initialises_header(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
    CHECK(a);

    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == SG_ARENA_START);

    bstack_close(segregated_bstack_allocator_into_stack(a));
    sg_unlink(tmp); return 0;
}

static int test_reopen_empty_arena(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
        CHECK(a);
        bstack_close(segregated_bstack_allocator_into_stack(a));
    }
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
        CHECK(a); /* reopen of a fresh header must succeed */
        uint64_t len;
        CHECK(bstack_len(bs, &len) == 0);
        CHECK(len == SG_ARENA_START);
        bstack_close(segregated_bstack_allocator_into_stack(a));
    }
    sg_unlink(tmp); return 0;
}

static int test_reopen_rejects_bad_magic(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    uint8_t zeros[SG_ARENA_START]; memset(zeros, 0, sizeof zeros);
    CHECK(bstack_push(bs, zeros, sizeof zeros, NULL) == 0);
    bstack_close(bs);

    bs = bstack_open(tmp); CHECK(bs);
    segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
    CHECK(a == NULL);
    bstack_close(bs); sg_unlink(tmp); return 0;
}

static int test_reopen_rejects_misaligned_arena(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
        CHECK(a);
        bstack_close(segregated_bstack_allocator_into_stack(a));
    }
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        uint8_t one = 0xFF;
        CHECK(bstack_push(bs, &one, 1, NULL) == 0); /* misalign the arena */
        bstack_close(bs);
    }
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
        CHECK(a == NULL);
        bstack_close(bs);
    }
    sg_unlink(tmp); return 0;
}

/* =========================================================================
 * Behaviour
 * ====================================================================== */

static int test_alloc_zero_is_empty(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *base = (bstack_allocator_t *)a;

    bstack_slice_t s;
    CHECK(bstack_allocator_alloc(base, 0, &s) == 0);
    CHECK(s.offset == 0 && s.len == 0);
    /* dealloc of an empty handle is a no-op */
    CHECK(bstack_allocator_dealloc(base, s) == 0);

    bstack_close(segregated_bstack_allocator_into_stack(a));
    sg_unlink(tmp); return 0;
}

static int test_alloc_roundtrip_all_ranges(void)
{
    static const uint64_t sizes[] = {
        1, 8, 16, 100, 200, 255, 256, 257, 300, 500, 1000, 3000,
        4096, 4097, 5000, 20000
    };
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *base = (bstack_allocator_t *)a;

    size_t i;
    bstack_slice_t held[sizeof sizes / sizeof sizes[0]];
    for (i = 0; i < sizeof sizes / sizeof sizes[0]; i++) {
        CHECK(bstack_allocator_alloc(base, sizes[i], &held[i]) == 0);
        CHECK(held[i].len == sizes[i]);
        /* data pointer is always of the form 16n + 8 */
        CHECK(held[i].offset % 16 == 8);
        CHECK(slice_fill(held[i], 0x100 + i) == 0);
    }
    for (i = 0; i < sizeof sizes / sizeof sizes[0]; i++)
        CHECK(slice_verify(held[i], sizes[i], 0x100 + i, "roundtrip") == 0);

    bstack_close(segregated_bstack_allocator_into_stack(a));
    sg_unlink(tmp); return 0;
}

static int test_dealloc_reuses_block(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *base = (bstack_allocator_t *)a;

    /* Two same-class allocations, free the first, then re-alloc: the free-list
     * pop must return the just-freed block (LIFO). */
    bstack_slice_t a0, a1, a2;
    CHECK(bstack_allocator_alloc(base, 100, &a0) == 0);
    CHECK(bstack_allocator_alloc(base, 100, &a1) == 0);
    CHECK(bstack_allocator_dealloc(base, a0) == 0);
    CHECK(bstack_allocator_alloc(base, 100, &a2) == 0);
    CHECK(a2.offset == a0.offset);
    (void)a1;

    bstack_close(segregated_bstack_allocator_into_stack(a));
    sg_unlink(tmp); return 0;
}

static int test_double_free_and_mismatch_detected(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *base = (bstack_allocator_t *)a;

    bstack_slice_t s;
    CHECK(bstack_allocator_alloc(base, 100, &s) == 0);

    /* Mismatched length is rejected, original survives (-1). */
    bstack_slice_t bad = s; bad.len = 50;
    CHECK(bstack_allocator_dealloc(base, bad) == -1);

    /* First free succeeds; second is a detected double free. */
    CHECK(bstack_allocator_dealloc(base, s) == 0);
    CHECK(bstack_allocator_dealloc(base, s) == -1);

    bstack_close(segregated_bstack_allocator_into_stack(a));
    sg_unlink(tmp); return 0;
}

/* alloc len, fill, realloc to new_len, verify the preserved prefix survives. */
static int realloc_case(bstack_allocator_t *base, uint64_t len, uint64_t new_len)
{
    bstack_slice_t s, out;
    uint64_t keep = len < new_len ? len : new_len;
    CHECK(bstack_allocator_alloc(base, len, &s) == 0);
    CHECK(slice_fill(s, 0xABCD) == 0);
    CHECK(bstack_allocator_realloc(base, s, new_len, &out) == 0);
    CHECK(out.len == new_len);
    CHECK(out.offset % 16 == 8);
    CHECK(slice_verify(out, keep, 0xABCD, "realloc-prefix") == 0);
    CHECK(bstack_allocator_dealloc(base, out) == 0);
    return 0;
}

static int test_realloc_paths(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *base = (bstack_allocator_t *)a;

    CHECK(realloc_case(base, 100, 104) == 0);   /* same class grow  */
    CHECK(realloc_case(base, 104, 100) == 0);   /* same class shrink */
    CHECK(realloc_case(base, 100, 500) == 0);   /* cross-class grow  */
    CHECK(realloc_case(base, 500, 100) == 0);   /* cross-class shrink */
    CHECK(realloc_case(base, 100, 5000) == 0);  /* classed -> oversized */
    CHECK(realloc_case(base, 5000, 100) == 0);  /* oversized -> classed */
    CHECK(realloc_case(base, 5000, 6000) == 0); /* oversized grow    */
    CHECK(realloc_case(base, 6000, 5000) == 0); /* oversized shrink  */

    /* Interleave a live block so shrinks are forced non-tail. */
    {
        bstack_slice_t a0, a1, out;
        CHECK(bstack_allocator_alloc(base, 300, &a0) == 0);
        CHECK(slice_fill(a0, 0x11) == 0);
        CHECK(bstack_allocator_alloc(base, 300, &a1) == 0); /* pins a0's tail */
        CHECK(bstack_allocator_realloc(base, a0, 100, &out) == 0); /* non-tail shrink */
        CHECK(slice_verify(out, 100, 0x11, "non-tail-shrink") == 0);
        CHECK(bstack_allocator_dealloc(base, out) == 0);
        CHECK(bstack_allocator_dealloc(base, a1) == 0);
    }

    bstack_close(segregated_bstack_allocator_into_stack(a));
    sg_unlink(tmp); return 0;
}

/* Tail shrink.  With atomic it is one LEN + SPLICE transaction: the block is
 * replaced by its shrunk self at the same offset and the excess goes back to the
 * stack.  Without atomic the length commit and the truncation cannot be fused —
 * either ordering leaves a crash window recover() mis-parses — so the shrink
 * takes the move path and the block relocates, its old block recycled. */
static int test_realloc_tail_shrink(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *base = (bstack_allocator_t *)a;

    bstack_slice_t s, out;
    uint64_t before, after, off;
    CHECK(bstack_allocator_alloc(base, 200, &s) == 0);  /* block 208, at the tail */
    CHECK(slice_fill(s, 0x3C3C) == 0);
    off = s.offset;
    CHECK(bstack_len(bs, &before) == 0);
    CHECK(bstack_allocator_realloc(base, s, 100, &out) == 0); /* class 112 < 208 */
    CHECK(out.len == 100);
    CHECK(slice_verify(out, 100, 0x3C3C, "tail-shrink") == 0);
    CHECK(bstack_len(bs, &after) == 0);

#ifdef BSTACK_FEATURE_ATOMIC
    CHECK(out.offset == off);       /* replaced in place */
    CHECK(after < before);          /* excess returned to the stack */
#else
    {
        bstack_slice_t reused;
        CHECK(out.offset != off);   /* moved */
        /* The vacated 208-byte block is back on its free list and gets reused. */
        CHECK(bstack_allocator_alloc(base, 200, &reused) == 0);
        CHECK(reused.offset == off);
        CHECK(bstack_allocator_dealloc(base, reused) == 0);
    }
    (void)before; (void)after;
#endif

    CHECK(bstack_allocator_dealloc(base, out) == 0);
    bstack_close(segregated_bstack_allocator_into_stack(a));
    sg_unlink(tmp); return 0;
}

static int test_reopen_preserves_live_and_free(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    uint64_t off_keep[4];

    /* Populate, free a couple (non-tail so they enter the free list), then
     * close cleanly. */
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
        CHECK(a);
        bstack_allocator_t *base = (bstack_allocator_t *)a;

        bstack_slice_t s[6];
        int i;
        for (i = 0; i < 6; i++) {
            CHECK(bstack_allocator_alloc(base, 200, &s[i]) == 0);
            CHECK(slice_fill(s[i], 0x500 + i) == 0);
        }
        /* Free two interior blocks (non-tail). */
        CHECK(bstack_allocator_dealloc(base, s[1]) == 0);
        CHECK(bstack_allocator_dealloc(base, s[3]) == 0);
        off_keep[0] = s[0].offset; off_keep[1] = s[2].offset;
        off_keep[2] = s[4].offset; off_keep[3] = s[5].offset;
        bstack_close(segregated_bstack_allocator_into_stack(a));
    }

    /* Reopen (runs recover): live blocks intact, free list rebuilt and reusable. */
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
        CHECK(a);
        bstack_allocator_t *base = (bstack_allocator_t *)a;

        bstack_slice_t live;
        live.allocator = base; live.len = 200;
        live.offset = off_keep[0];
        CHECK(slice_verify(live, 200, 0x500 + 0, "reopen-live0") == 0);
        live.offset = off_keep[1];
        CHECK(slice_verify(live, 200, 0x500 + 2, "reopen-live2") == 0);
        live.offset = off_keep[3];
        CHECK(slice_verify(live, 200, 0x500 + 5, "reopen-live5") == 0);

        /* Two same-class allocs must recycle the two reclaimed free blocks
         * rather than extend the arena. */
        uint64_t len_before;
        CHECK(bstack_len(bs, &len_before) == 0);
        bstack_slice_t r0, r1;
        CHECK(bstack_allocator_alloc(base, 200, &r0) == 0);
        CHECK(bstack_allocator_alloc(base, 200, &r1) == 0);
        uint64_t len_after;
        CHECK(bstack_len(bs, &len_after) == 0);
        CHECK(len_after == len_before); /* both served from the free list */

        bstack_close(segregated_bstack_allocator_into_stack(a));
    }
    sg_unlink(tmp); return 0;
}

/* =========================================================================
 * Fuzz — mixed alloc/realloc/dealloc against a shadow model, with periodic
 * reopen to exercise recover().
 * ====================================================================== */

static int fuzz_verify_all(bstack_allocator_t *base, sg_vec_t *live)
{
    size_t i;
    for (i = 0; i < live->count; i++) {
        bstack_slice_t s = entry_slice(base, live->data[i]);
        if (slice_verify(s, live->data[i].len, live->data[i].id, "fuzz") != 0)
            return -1;
    }
    return 0;
}

static int test_fuzz_mixed(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    sg_vec_t live = {0};
    uint64_t next_id = 1;
    int      round;
    int      rc = 0;

    rng_seed(0x5eeded ^ (uint64_t)time(NULL));

    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *base = (bstack_allocator_t *)a;

    for (round = 0; round < 4000; round++) {
        uint64_t roll = rng_range(0, 99);
        if (roll < 45 || live.count == 0) {
            /* alloc */
            uint64_t len = rng_range(1, 4200); /* spans classed + oversized */
            bstack_slice_t s;
            sg_entry_t e;
            if (bstack_allocator_alloc(base, len, &s) != 0) { rc = -1; break; }
            if (slice_fill(s, next_id) != 0) { rc = -1; break; }
            e.offset = s.offset; e.len = s.len; e.id = next_id++;
            if (sg_vec_push(&live, e) != 0) { rc = -1; break; }
        } else if (roll < 65) {
            /* realloc */
            size_t idx = (size_t)rng_range(0, live.count - 1);
            sg_entry_t e = live.data[idx];
            uint64_t new_len = rng_range(1, 4200);
            uint64_t keep = e.len < new_len ? e.len : new_len;
            bstack_slice_t s = entry_slice(base, e), out;
            if (bstack_allocator_realloc(base, s, new_len, &out) != 0) { rc = -1; break; }
            /* preserved prefix must survive the resize */
            if (slice_verify(out, keep, e.id, "fuzz-realloc") != 0) { rc = -1; break; }
            /* re-establish the full-length invariant for the new size */
            live.data[idx].offset = out.offset;
            live.data[idx].len    = out.len;
            if (slice_fill(entry_slice(base, live.data[idx]), e.id) != 0) { rc = -1; break; }
        } else {
            /* dealloc */
            size_t idx = (size_t)rng_range(0, live.count - 1);
            sg_entry_t e = sg_vec_swap_remove(&live, idx);
            if (bstack_allocator_dealloc(base, entry_slice(base, e)) != 0) { rc = -1; break; }
        }

        /* Periodic clean reopen: into_stack + close, reopen, verify survivors. */
        if (round % 800 == 799) {
            bstack_close(segregated_bstack_allocator_into_stack(a));
            bs = bstack_open(tmp);
            if (!bs) { rc = -1; a = NULL; break; }
            a = segregated_bstack_allocator_new(bs);
            if (!a) { rc = -1; break; }
            base = (bstack_allocator_t *)a;
            if (fuzz_verify_all(base, &live) != 0) { rc = -1; break; }
        }
    }

    if (rc == 0) rc = fuzz_verify_all(base, &live);

    if (a) bstack_close(segregated_bstack_allocator_into_stack(a));
    else if (bs) bstack_close(bs);
    sg_vec_free(&live);
    sg_unlink(tmp);
    CHECK(rc == 0);
    return 0;
}

/* =========================================================================
 * main
 * ====================================================================== */

int main(void)
{
    T(test_new_initialises_header);
    T(test_reopen_empty_arena);
    T(test_reopen_rejects_bad_magic);
    T(test_reopen_rejects_misaligned_arena);
    T(test_alloc_zero_is_empty);
    T(test_alloc_roundtrip_all_ranges);
    T(test_dealloc_reuses_block);
    T(test_double_free_and_mismatch_detected);
    T(test_realloc_paths);
    T(test_realloc_tail_shrink);
    T(test_reopen_preserves_live_and_free);
    T(test_fuzz_mixed);

    printf("\n%d/%d tests passed\n", g_passed, g_total);
    return g_passed == g_total ? 0 : 1;
}
