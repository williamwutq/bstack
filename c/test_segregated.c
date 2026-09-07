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

    /* A length too large for the block is rejected, original survives (-1).
     * (The word now records the physical size, so a length that fits — e.g. 50 —
     * is trusted; only one exceeding the block's capacity is a mismatch.) */
    bstack_slice_t bad = s; bad.len = 200;   /* phys_need(200)=208 > block 112 */
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

/* A non-tail grow forces a move: the block is pinned off the tail, so realloc
 * must place a larger block, copy the surviving prefix, and free the old one.
 * Under atomic this is one crash-atomic commit_move — the new block is staged
 * free, then flipped live as the old is freed, so no in-use orphan can leak.
 * The result is a clean arena that recover() fully accounts for. */
static int test_realloc_non_tail_move(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *base = (bstack_allocator_t *)a;
    bstack_slice_t s, pin, out;
    uint64_t unsure = 1;
    uint8_t rd[300];
    size_t i;

    CHECK(bstack_allocator_alloc(base, 100, &s) == 0);        /* block 112 */
    CHECK(slice_fill(s, 0x77) == 0);
    CHECK(bstack_allocator_alloc(base, 100, &pin) == 0);       /* pins s off the tail */
    CHECK(bstack_allocator_realloc(base, s, 300, &out) == 0);  /* non-tail grow -> move */
    CHECK(out.offset != s.offset);                            /* interior grow moves */
    CHECK(out.len == 300);
    CHECK(slice_verify(out, 100, 0x77, "move-prefix") == 0);  /* surviving prefix */
    CHECK(bstack_slice_read_into(out, rd, 300) == 0);
    for (i = 100; i < 300; i++) CHECK(rd[i] == 0);            /* grown tail is zero */

    /* No leak: recover fully accounts for the arena (old block freed, none
     * orphaned in use), and the moved block still reads back intact. */
    CHECK(segregated_bstack_allocator_recover(a, &unsure) == 0);
    CHECK(unsure == 0);
    CHECK(slice_verify(out, 100, 0x77, "move-after-recover") == 0);

    CHECK(bstack_allocator_dealloc(base, out) == 0);
    CHECK(bstack_allocator_dealloc(base, pin) == 0);
    bstack_close(segregated_bstack_allocator_into_stack(a));
    sg_unlink(tmp); return 0;
}

/* Tail shrink whose excess reaches SPLIT_MIN (256).  With atomic it is one
 * LEN + SPLICE transaction: the block is replaced by its shrunk self at the same
 * offset and the excess goes back to the stack.  Without atomic recording the
 * smaller size and dropping the excess cannot be fused — either ordering leaves
 * a crash window recover() mis-parses — so the shrink retains the excess in
 * place: the block keeps its physical size and offset with no move, and a later
 * grow back into that retained span fits without moving. */
static int test_realloc_tail_shrink(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *base = (bstack_allocator_t *)a;

    bstack_slice_t s, out;
    uint64_t before, after, off;
    CHECK(bstack_allocator_alloc(base, 4500, &s) == 0); /* block 4512, at the tail */
    CHECK(slice_fill(s, 0x3C3C) == 0);
    off = s.offset;
    CHECK(bstack_len(bs, &before) == 0);
    /* class 112, excess 4400 >= SPLIT_MIN */
    CHECK(bstack_allocator_realloc(base, s, 100, &out) == 0);
    CHECK(out.len == 100);
    CHECK(slice_verify(out, 100, 0x3C3C, "tail-shrink") == 0);
    CHECK(bstack_len(bs, &after) == 0);

#ifdef BSTACK_FEATURE_ATOMIC
    CHECK(out.offset == off);       /* replaced in place */
    CHECK(after < before);          /* excess returned to the stack */
#else
    CHECK(out.offset == off);       /* retained in place, no move */
    CHECK(after == before);         /* nothing discarded: the excess is retained */
    {
        /* The retained 512-byte block absorbs a grow back to 400 with no move. */
        bstack_slice_t grown;
        CHECK(bstack_allocator_realloc(base, out, 400, &grown) == 0);
        CHECK(grown.offset == off);
        CHECK(slice_verify(grown, 100, 0x3C3C, "tail-shrink-grow-back") == 0);
        out = grown;
    }
#endif

    CHECK(bstack_allocator_dealloc(base, out) == 0);
    bstack_close(segregated_bstack_allocator_into_stack(a));
    sg_unlink(tmp); return 0;
}

/* A shrink whose excess is below SPLIT_MIN is retained inside the live block (in
 * either build): no metadata write, no move, and the block keeps its larger
 * physical size so a later grow back fits in place with the re-exposed tail
 * zeroed. */
static int test_realloc_small_shrink_retained(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *base = (bstack_allocator_t *)a;

    bstack_slice_t s, pin, out, grown;
    uint64_t off, unsure, len_before, len_after;
    CHECK(bstack_allocator_alloc(base, 200, &s) == 0);   /* block 208 (class 12) */
    CHECK(slice_fill(s, 0x5A) == 0);
    off = s.offset;
    CHECK(bstack_allocator_alloc(base, 100, &pin) == 0);  /* pins s's tail */
    CHECK(bstack_len(bs, &len_before) == 0);
    /* need 112, excess 96 < SPLIT_MIN -> retain in place, no move, no write. */
    CHECK(bstack_allocator_realloc(base, s, 100, &out) == 0);
    CHECK(out.offset == off);
    CHECK(slice_verify(out, 100, 0x5A, "small-shrink") == 0);
    CHECK(bstack_len(bs, &len_after) == 0);
    CHECK(len_after == len_before);

    /* The block still spans 208 bytes, so growing back to 200 fits in place. */
    CHECK(bstack_allocator_realloc(base, out, 200, &grown) == 0);
    CHECK(grown.offset == off);
    CHECK(slice_verify(grown, 100, 0x5A, "grow-back-prefix") == 0);
    {
        uint8_t data[200];
        size_t i;
        CHECK(bstack_slice_read_into(grown, data, 200) == 0);
        for (i = 100; i < 200; i++) CHECK(data[i] == 0);  /* re-exposed tail zeroed */
    }
    CHECK(segregated_bstack_allocator_recover(a, &unsure) == 0);
    CHECK(unsure == 0);

    CHECK(bstack_allocator_dealloc(base, grown) == 0);
    CHECK(bstack_allocator_dealloc(base, pin) == 0);
    bstack_close(segregated_bstack_allocator_into_stack(a));
    sg_unlink(tmp); return 0;
}

/* Oversized reuse whose excess is below SPLIT_MIN hands back the popped block
 * whole, recording its full physical size; a later free returns it to the
 * oversized list at that size and it recycles. */
static int test_oversized_reuse_retains_small_excess(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *base = (bstack_allocator_t *)a;

    bstack_slice_t x, pin, y, z;
    uint64_t off_x, unsure;
    CHECK(bstack_allocator_alloc(base, 5000, &x) == 0);  /* oversized, block 5008 */
    off_x = x.offset;
    CHECK(bstack_allocator_alloc(base, 50, &pin) == 0);  /* pins the tail so X interior */
    CHECK(bstack_allocator_dealloc(base, x) == 0);       /* X -> oversized list (5008) */
    /* Y needs block 4864 (oversized); reuses X whole (excess 144 < SPLIT_MIN). */
    CHECK(bstack_allocator_alloc(base, 4850, &y) == 0);
    CHECK(y.offset == off_x);
    CHECK(segregated_bstack_allocator_recover(a, &unsure) == 0);
    CHECK(unsure == 0);                                  /* block strides its full size */

    /* Freed, the retained block returns to the oversized list at its full size. */
    CHECK(bstack_allocator_dealloc(base, y) == 0);
    CHECK(bstack_allocator_alloc(base, 5000, &z) == 0);
    CHECK(z.offset == off_x);                            /* retained 5008 block recycled */

    CHECK(bstack_allocator_dealloc(base, z) == 0);
    CHECK(bstack_allocator_dealloc(base, pin) == 0);
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

/* A slice issued by one allocator instance must be refused by another: the
 * language cannot catch it, so the allocator does, at run time, before
 * touching any metadata.  See "Foreign slices" in bstack_alloc.h. */
static int test_foreign_slice_is_rejected(void)
{
    char t1[64], t2[64];
    make_tmp(t1, sizeof t1);
    make_tmp(t2, sizeof t2);
    {
        bstack_t *b1 = bstack_open(t1); CHECK(b1);
        bstack_t *b2 = bstack_open(t2); CHECK(b2);
        segregated_bstack_allocator_t *a1 = segregated_bstack_allocator_new(b1); CHECK(a1);
        segregated_bstack_allocator_t *a2 = segregated_bstack_allocator_new(b2); CHECK(a2);
        bstack_allocator_t *g1 = (bstack_allocator_t *)a1;
        bstack_allocator_t *g2 = (bstack_allocator_t *)a2;
        bstack_slice_t s, out, own;

        CHECK(bstack_allocator_alloc(g1, 64, &s) == 0);
        CHECK(bstack_slice_is_from(s, g1));
        CHECK(!bstack_slice_is_from(s, g2));

        errno = 0;
        CHECK(bstack_allocator_dealloc(g2, s) == -1);
        CHECK(errno == EINVAL);

        /* realloc reports "original survived" and hands the slice back
         * unchanged in *out. */
        errno = 0;
        CHECK(bstack_allocator_realloc(g2, s, 128, &out) == -1);
        CHECK(errno == EINVAL);
        CHECK(out.offset == s.offset);
        CHECK(out.len == s.len);

        /* Neither allocator's bookkeeping was touched: a2 still round-trips
         * its own allocation, and a1 can still free the original region. */
        CHECK(bstack_allocator_alloc(g2, 64, &own) == 0);
        CHECK(bstack_allocator_dealloc(g2, own) == 0);
        CHECK(bstack_allocator_dealloc(g1, s) == 0);

        bstack_close(segregated_bstack_allocator_into_stack(a1));
        bstack_close(segregated_bstack_allocator_into_stack(a2));
    }
    sg_unlink(t1); sg_unlink(t2); return 0;
}

/* =========================================================================
 * Bulk (BStackBulkAllocator) — requires -DBSTACK_FEATURE_ATOMIC
 * ====================================================================== */
#ifdef BSTACK_FEATURE_ATOMIC

#define SG_HEAD_OFF(c) (40u + (uint64_t)(c) * 8u)

static void sg_wr64(uint8_t *p, uint64_t v)
{
    int i;
    for (i = 0; i < 8; i++) p[i] = (uint8_t)(v >> (8 * i));
}

static uint64_t sg_rd64(const uint8_t *p)
{
    uint64_t v = 0;
    int i;
    for (i = 7; i >= 0; i--) v = (v << 8) | p[i];
    return v;
}

/* n == 0 is a no-op; a zero-length entry yields the null sentinel slice. */
static int test_bulk_empty_and_zero_lengths(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *base = (bstack_allocator_t *)a;

    uint64_t lens[3] = { 0, 40, 0 };
    bstack_slice_t out[3];
    uint64_t unsure;

    CHECK(bstack_allocator_alloc_bulk(base, NULL, 0, NULL) == 0);
    CHECK(bstack_allocator_alloc_bulk(base, lens, 3, out) == 0);
    CHECK(out[0].offset == 0 && out[0].len == 0);
    CHECK(out[2].offset == 0 && out[2].len == 0);
    CHECK(out[1].offset != 0 && out[1].len == 40);
    CHECK(bstack_allocator_dealloc_bulk(base, out, 3) == 0);
    CHECK(segregated_bstack_allocator_recover(a, &unsure) == 0);
    CHECK(unsure == 0);

    bstack_close(segregated_bstack_allocator_into_stack(a));
    sg_unlink(tmp); return 0;
}

/* One batch spanning linear, geometric, and oversized classes hands back
 * distinct, independently writable regions of the requested length. */
static int test_bulk_alloc_distinct_usable(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *base = (bstack_allocator_t *)a;

    uint64_t lens[4] = { 8, 100, 300, 5000 };
    bstack_slice_t out[4];
    uint8_t buf[5000], got[5000];
    size_t i, j;
    uint64_t unsure;

    CHECK(bstack_allocator_alloc_bulk(base, lens, 4, out) == 0);
    for (i = 0; i < 4; i++) {
        CHECK(out[i].len == lens[i]);
        CHECK(out[i].offset % 16 == 8);
        for (j = 0; j < 4; j++)
            if (i != j) CHECK(out[i].offset != out[j].offset);
    }
    for (i = 0; i < 4; i++) {
        memset(buf, (int)(i + 1), (size_t)lens[i]);
        CHECK(bstack_slice_write(out[i], buf, (size_t)lens[i]) == 0);
    }
    for (i = 0; i < 4; i++) {
        memset(buf, (int)(i + 1), (size_t)lens[i]);
        CHECK(bstack_slice_read_into(out[i], got, (size_t)lens[i]) == 0);
        CHECK(memcmp(got, buf, (size_t)lens[i]) == 0);
    }
    CHECK(bstack_allocator_dealloc_bulk(base, out, 4) == 0);
    CHECK(segregated_bstack_allocator_recover(a, &unsure) == 0);
    CHECK(unsure == 0);

    bstack_close(segregated_bstack_allocator_into_stack(a));
    sg_unlink(tmp); return 0;
}

/* A freed batch is drained back out of its class lists, and a recycled block
 * reads back zero (the claim scrubs the previous occupant's bytes). */
static int test_bulk_reuses_and_scrubs(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *base = (bstack_allocator_t *)a;

    uint64_t lens[3] = { 100, 100, 100 };
    bstack_slice_t first[3], second[3];
    uint8_t buf[100], got[100];
    size_t i, j;
    int found;

    CHECK(bstack_allocator_alloc_bulk(base, lens, 3, first) == 0);
    memset(buf, 0x5A, sizeof buf);
    for (i = 0; i < 3; i++) CHECK(bstack_slice_write(first[i], buf, 100) == 0);
    CHECK(bstack_allocator_dealloc_bulk(base, first, 3) == 0);

    CHECK(bstack_allocator_alloc_bulk(base, lens, 3, second) == 0);
    for (i = 0; i < 3; i++) {
        found = 0;
        for (j = 0; j < 3; j++) if (second[i].offset == first[j].offset) found = 1;
        CHECK(found); /* every block came off the class free list */
        CHECK(bstack_slice_read_into(second[i], got, 100) == 0);
        for (j = 0; j < 100; j++) CHECK(got[j] == 0); /* scrubbed */
    }

    bstack_close(segregated_bstack_allocator_into_stack(a));
    sg_unlink(tmp); return 0;
}

/* An oversized request matches a freed oversized block; slack at or above
 * SPLIT_MIN is carved back into a reusable block. */
static int test_bulk_oversized_match_and_carve(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *base = (bstack_allocator_t *)a;

    bstack_slice_t x, pin, out[1], rem;
    uint64_t off_x, lens[1], unsure;

    CHECK(bstack_allocator_alloc(base, 9000, &x) == 0); /* oversized, block 9008 */
    off_x = x.offset;
    CHECK(bstack_allocator_alloc(base, 50, &pin) == 0); /* keeps X off the tail */
    CHECK(bstack_allocator_dealloc(base, x) == 0);      /* X -> oversized list */

    /* 4200 -> block 4208 (oversized); excess 4800 >= SPLIT_MIN, so it carves. */
    lens[0] = 4200;
    CHECK(bstack_allocator_alloc_bulk(base, lens, 1, out) == 0);
    CHECK(out[0].offset == off_x);
    CHECK(segregated_bstack_allocator_recover(a, &unsure) == 0);
    CHECK(unsure == 0); /* every carved piece strides its recorded size */

    /* The 4800-byte remainder (> MAX_CLASS) is one oversized block and recycles. */
    CHECK(bstack_allocator_alloc(base, 4790, &rem) == 0); /* need 4800, oversized reuse */
    CHECK(rem.offset == off_x - 8 + 4208 + 8);

    CHECK(bstack_allocator_dealloc(base, rem) == 0);
    CHECK(bstack_allocator_dealloc_bulk(base, out, 1) == 0);
    CHECK(bstack_allocator_dealloc(base, pin) == 0);

    bstack_close(segregated_bstack_allocator_into_stack(a));
    sg_unlink(tmp); return 0;
}

/* A batch repeating a block, or freeing one twice, is rejected before any
 * write; a foreign slice rejects the whole batch. */
static int test_bulk_dealloc_rejects_bad_batches(void)
{
    char t1[64], t2[64]; make_tmp(t1, sizeof t1); make_tmp(t2, sizeof t2);
    bstack_t *b1 = bstack_open(t1); CHECK(b1);
    bstack_t *b2 = bstack_open(t2); CHECK(b2);
    segregated_bstack_allocator_t *a1 = segregated_bstack_allocator_new(b1);
    segregated_bstack_allocator_t *a2 = segregated_bstack_allocator_new(b2);
    CHECK(a1 && a2);
    bstack_allocator_t *g1 = (bstack_allocator_t *)a1;
    bstack_allocator_t *g2 = (bstack_allocator_t *)a2;

    uint64_t lens[2] = { 40, 40 };
    bstack_slice_t out[2], dup[2], foreign[2];

    CHECK(bstack_allocator_alloc_bulk(g1, lens, 2, out) == 0);

    dup[0] = out[0]; dup[1] = out[0];              /* same block twice */
    CHECK(bstack_allocator_dealloc_bulk(g1, dup, 2) == -1);
    /* Nothing was freed: the real batch still succeeds. */
    CHECK(bstack_allocator_dealloc_bulk(g1, out, 2) == 0);
    /* Now they are free: a second pass is a double free. */
    CHECK(bstack_allocator_dealloc_bulk(g1, out, 2) == -1);

    CHECK(bstack_allocator_alloc_bulk(g2, lens, 2, foreign) == 0);
    CHECK(bstack_allocator_dealloc_bulk(g1, foreign, 2) == -1);
    CHECK(bstack_allocator_dealloc_bulk(g2, foreign, 2) == 0);

    bstack_close(segregated_bstack_allocator_into_stack(a1));
    bstack_close(segregated_bstack_allocator_into_stack(a2));
    sg_unlink(t1); sg_unlink(t2); return 0;
}

/* recover() relinks a non-class size onto the largest class <= size, so
 * alloc_bulk must accept it there too (single alloc already does). */
static int test_bulk_reuses_recovered_non_class_size(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *base = (bstack_allocator_t *)a;

    bstack_slice_t x, pin, out[1];
    uint64_t xb, lens[1], unsure;
    uint8_t w[8];

    /* Block 320 (class 16) at the arena start; the second alloc pins it off the
     * tail so dealloc frees it rather than discarding it. */
    CHECK(bstack_allocator_alloc(base, 300, &x) == 0);
    xb = x.offset - 8;
    CHECK(xb == SG_ARENA_START);
    CHECK(bstack_allocator_alloc(base, 300, &pin) == 0);
    CHECK(bstack_allocator_dealloc(base, x) == 0);

    /* Split the 320 into a free 272 (not a class size) plus a free 48, so
     * recover()'s linear scan still walks cleanly into the pinned block. */
    sg_wr64(w, 272u >> 4); CHECK(bstack_set(bs, xb, w, 8) == 0);
    sg_wr64(w, 48u >> 4);  CHECK(bstack_set(bs, xb + 272, w, 8) == 0);
    CHECK(segregated_bstack_allocator_recover(a, &unsure) == 0);
    CHECK(unsure == 0);

    /* largest_class_le(272) == 256, so it lands on class 15. */
    CHECK(bstack_get(bs, SG_HEAD_OFF(15), SG_HEAD_OFF(15) + 8, w) == 0);
    CHECK(sg_rd64(w) == xb);

    /* A class-15 request (block 256) reuses it instead of failing the batch. */
    lens[0] = 248;
    CHECK(bstack_allocator_alloc_bulk(base, lens, 1, out) == 0);
    CHECK(out[0].offset == xb + 8);

    CHECK(bstack_allocator_dealloc(base, pin) == 0);
    bstack_close(segregated_bstack_allocator_into_stack(a));
    sg_unlink(tmp); return 0;
}

/* A cycle in a classed free list is rejected with no write. */
static int test_bulk_rejects_free_list_cycle(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *base = (bstack_allocator_t *)a;

    bstack_slice_t p[2], out[2];
    uint64_t lens[2] = { 40, 40 };
    uint8_t w[8];

    CHECK(bstack_allocator_alloc_bulk(base, lens, 2, p) == 0);
    CHECK(bstack_allocator_dealloc_bulk(base, p, 2) == 0);
    /* Point the list head's block back at itself. */
    CHECK(bstack_get(bs, SG_HEAD_OFF(2), SG_HEAD_OFF(2) + 8, w) == 0);
    CHECK(bstack_set(bs, sg_rd64(w) + 8, w, 8) == 0);
    CHECK(bstack_allocator_alloc_bulk(base, lens, 2, out) == -1);

    bstack_close(segregated_bstack_allocator_into_stack(a));
    sg_unlink(tmp); return 0;
}

/* ---- coalesce ---------------------------------------------------------- */

static int test_coalesce_merges_adjacent(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *base = (bstack_allocator_t *)a;

    /* Four contiguous equal blocks, then a tail pin so freeing routes them to
     * the free list rather than discarding. */
    bstack_slice_t b0, b1, b2, b3, pin, big;
    uint64_t fused = 999, unsure = 999, base_off;
    CHECK(bstack_allocator_alloc(base, 100, &b0) == 0);
    CHECK(bstack_allocator_alloc(base, 100, &b1) == 0);
    CHECK(bstack_allocator_alloc(base, 100, &b2) == 0);
    CHECK(bstack_allocator_alloc(base, 100, &b3) == 0);
    CHECK(bstack_allocator_alloc(base, 100, &pin) == 0);
    base_off = b0.offset;
    CHECK(bstack_allocator_dealloc(base, b0) == 0);
    CHECK(bstack_allocator_dealloc(base, b1) == 0);
    CHECK(bstack_allocator_dealloc(base, b2) == 0);
    CHECK(bstack_allocator_dealloc(base, b3) == 0);

    /* Four adjacent free blocks fuse into one: three blocks absorbed. */
    CHECK(segregated_bstack_allocator_coalesce(a, &fused) == 0);
    CHECK(fused == 3);
    /* The merged run backs a request no single 112-block could serve. */
    CHECK(bstack_allocator_alloc(base, 440, &big) == 0);
    CHECK(big.offset == base_off);
    CHECK(segregated_bstack_allocator_recover(a, &unsure) == 0);
    CHECK(unsure == 0);
    (void)pin;

    bstack_close(segregated_bstack_allocator_into_stack(a));
    sg_unlink(tmp); return 0;
}

static int test_coalesce_noop_without_adjacency(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *base = (bstack_allocator_t *)a;

    /* Free the outer two but keep the middle live, so the frees are not adjacent. */
    bstack_slice_t b0, b1, b2, pin, x, y;
    uint64_t fused = 999, s0, s2, g0, g1;
    CHECK(bstack_allocator_alloc(base, 100, &b0) == 0);
    CHECK(bstack_allocator_alloc(base, 100, &b1) == 0);
    CHECK(bstack_allocator_alloc(base, 100, &b2) == 0);
    CHECK(bstack_allocator_alloc(base, 100, &pin) == 0);
    s0 = b0.offset; s2 = b2.offset;
    CHECK(bstack_allocator_dealloc(base, b0) == 0);
    CHECK(bstack_allocator_dealloc(base, b2) == 0);

    CHECK(segregated_bstack_allocator_coalesce(a, &fused) == 0);
    CHECK(fused == 0);
    /* Both freed blocks stay individually reusable (lists untouched by the no-op). */
    CHECK(bstack_allocator_alloc(base, 100, &x) == 0);
    CHECK(bstack_allocator_alloc(base, 100, &y) == 0);
    g0 = x.offset; g1 = y.offset;
    CHECK((g0 == s0 && g1 == s2) || (g0 == s2 && g1 == s0));
    (void)b1; (void)pin;

    bstack_close(segregated_bstack_allocator_into_stack(a));
    sg_unlink(tmp); return 0;
}

static int test_coalesce_empty_arena(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
    CHECK(a);

    uint64_t fused = 999;
    CHECK(segregated_bstack_allocator_coalesce(a, &fused) == 0);
    CHECK(fused == 0);

    bstack_close(segregated_bstack_allocator_into_stack(a));
    sg_unlink(tmp); return 0;
}

static int test_coalesce_partial_run(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    segregated_bstack_allocator_t *a = segregated_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *base = (bstack_allocator_t *)a;

    /* Layout: live, free, free, live, tail-pin. Only the middle pair merges. */
    bstack_slice_t live0, f0, f1, live1, pin, merged;
    uint64_t fused = 999, unsure = 999, f0_off;
    CHECK(bstack_allocator_alloc(base, 100, &live0) == 0);
    CHECK(bstack_allocator_alloc(base, 100, &f0) == 0);
    CHECK(bstack_allocator_alloc(base, 100, &f1) == 0);
    CHECK(bstack_allocator_alloc(base, 100, &live1) == 0);
    CHECK(bstack_allocator_alloc(base, 100, &pin) == 0);
    f0_off = f0.offset;
    CHECK(bstack_allocator_dealloc(base, f0) == 0);
    CHECK(bstack_allocator_dealloc(base, f1) == 0);

    CHECK(segregated_bstack_allocator_coalesce(a, &fused) == 0);
    CHECK(fused == 1);
    /* Two 112-blocks fuse to a 224-block; request the size that maps to that class. */
    CHECK(bstack_allocator_alloc(base, 216, &merged) == 0);
    CHECK(merged.offset == f0_off);
    CHECK(segregated_bstack_allocator_recover(a, &unsure) == 0);
    CHECK(unsure == 0);
    (void)live0; (void)live1; (void)pin;

    bstack_close(segregated_bstack_allocator_into_stack(a));
    sg_unlink(tmp); return 0;
}

#endif /* BSTACK_FEATURE_ATOMIC */

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
    T(test_realloc_non_tail_move);
    T(test_realloc_tail_shrink);
    T(test_realloc_small_shrink_retained);
    T(test_oversized_reuse_retains_small_excess);
    T(test_reopen_preserves_live_and_free);
    T(test_fuzz_mixed);

    T(test_foreign_slice_is_rejected);

#ifdef BSTACK_FEATURE_ATOMIC
    T(test_bulk_empty_and_zero_lengths);
    T(test_bulk_alloc_distinct_usable);
    T(test_bulk_reuses_and_scrubs);
    T(test_bulk_oversized_match_and_carve);
    T(test_bulk_dealloc_rejects_bad_batches);
    T(test_bulk_reuses_recovered_non_class_size);
    T(test_bulk_rejects_free_list_cycle);

    T(test_coalesce_merges_adjacent);
    T(test_coalesce_noop_without_adjacency);
    T(test_coalesce_empty_arena);
    T(test_coalesce_partial_run);
#endif

    printf("\n%d/%d tests passed\n", g_passed, g_total);
    return g_passed == g_total ? 0 : 1;
}
