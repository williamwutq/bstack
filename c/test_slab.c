/* slab_bstack_allocator_t — smoke + fuzz tests.
 * Requires -DBSTACK_FEATURE_SET.
 * Mirrors alloc_fuzz_tests.rs in structure and coverage. */

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
#  define sl_unlink(p)  DeleteFileA(p)
#  define sl_getpid()   ((unsigned long)_getpid())
#else
#  include <unistd.h>
#  define sl_unlink(p)  unlink(p)
#  define sl_getpid()   ((unsigned long)getpid())
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
    snprintf(buf, n, "bstack_sl_%lu_%ld.tmp",
             (unsigned long)GetCurrentProcessId(), (long)s);
    DeleteFileA(buf);
}
#else
static void make_tmp(char *buf, size_t n)
{
    snprintf(buf, n, "/tmp/bstack_sl_XXXXXX");
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
 * Data-pattern helpers (identical to test_first_fit.c)
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

static uint8_t pat_byte(uint8_t pattern, size_t i)
{
    return (uint8_t)((uint8_t)pattern + (uint8_t)i);
}

static void pat_fill(uint8_t *buf, size_t len, uint8_t pattern)
{
    size_t i;
    for (i = 0; i < len; i++) buf[i] = pat_byte(pattern, i);
}

static int pat_verify(const uint8_t *buf, size_t len, uint8_t pattern,
                      const char *ctx)
{
    size_t i;
    for (i = 0; i < len; i++) {
        uint8_t want = pat_byte(pattern, i);
        if (buf[i] != want) {
            fprintf(stderr,
                "  corruption %s byte %zu: want 0x%02x got 0x%02x "
                "(pattern=%u)\n",
                ctx, i, want, buf[i], (unsigned)pattern);
            return -1;
        }
    }
    return 0;
}

/* =========================================================================
 * sl_vec_t — growable array of live-allocation records
 * ====================================================================== */

typedef struct { uint64_t offset; uint64_t len; uint64_t id; } sl_entry_t;
typedef struct { sl_entry_t *data; size_t count; size_t cap; } sl_vec_t;

static int sl_vec_push(sl_vec_t *v, sl_entry_t e)
{
    if (v->count == v->cap) {
        size_t nc = v->cap ? v->cap * 2 : 16;
        sl_entry_t *t = realloc(v->data, nc * sizeof *t);
        if (!t) return -1;
        v->data = t; v->cap = nc;
    }
    v->data[v->count++] = e;
    return 0;
}

static sl_entry_t sl_vec_swap_remove(sl_vec_t *v, size_t idx)
{
    sl_entry_t e = v->data[idx];
    v->data[idx] = v->data[--v->count];
    return e;
}

static void sl_vec_free(sl_vec_t *v)
{
    free(v->data); v->data = NULL; v->count = 0; v->cap = 0;
}

static bstack_slice_t entry_slice(bstack_allocator_t *a, sl_entry_t e)
{
    bstack_slice_t s; s.allocator = a; s.offset = e.offset; s.len = e.len;
    return s;
}

/* =========================================================================
 * Smoke tests
 * ====================================================================== */

static int test_alloc_write_read(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    slab_bstack_allocator_t *a = slab_bstack_allocator_new(bs, 16);
    CHECK(a);
    CHECK(slab_bstack_allocator_block_size(a) == 16);

    bstack_slice_t s;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 12, &s) == 0);
    CHECK(s.len == 12);
    uint8_t wbuf[12], rbuf[12];
    memset(wbuf, 0xAB, 12);
    CHECK(bstack_slice_write(s, wbuf, 12) == 0);
    CHECK(bstack_slice_read(s, rbuf) == 0);
    CHECK(memcmp(wbuf, rbuf, 12) == 0);

    bstack_close(slab_bstack_allocator_into_stack(a));
    sl_unlink(tmp); return 0;
}

static int test_slot_reuse(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    slab_bstack_allocator_t *a = slab_bstack_allocator_new(bs, 16);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    /* Allocate two slab blocks; free the first; a new alloc must reuse it. */
    bstack_slice_t sa, sb, sc;
    CHECK(bstack_allocator_alloc(al, 8, &sa) == 0);
    CHECK(bstack_allocator_alloc(al, 8, &sb) == 0);
    uint64_t a_off = sa.offset;
    CHECK(bstack_allocator_dealloc(al, sa) == 0);
    CHECK(bstack_allocator_alloc(al, 8, &sc) == 0);
    CHECK(sc.offset == a_off); /* free-list reuse */

    bstack_allocator_dealloc(al, sc);
    bstack_allocator_dealloc(al, sb);
    bstack_close(slab_bstack_allocator_into_stack(a));
    sl_unlink(tmp); return 0;
}

static int test_oversized_tail_discard(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    slab_bstack_allocator_t *a = slab_bstack_allocator_new(bs, 16);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    uint64_t len_before;
    CHECK(bstack_len(bs, &len_before) == 0);

    /* Alloc oversized (64 > block_size 16), dealloc as tail: stack must shrink. */
    bstack_slice_t s;
    CHECK(bstack_allocator_alloc(al, 64, &s) == 0);
    CHECK(bstack_allocator_dealloc(al, s) == 0);

    uint64_t len_after;
    CHECK(bstack_len(bs, &len_after) == 0);
    CHECK(len_after == len_before);

    bstack_close(slab_bstack_allocator_into_stack(a));
    sl_unlink(tmp); return 0;
}

static int test_realloc_grow_shrink(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    slab_bstack_allocator_t *a = slab_bstack_allocator_new(bs, 16);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t s, s2, s3;
    uint8_t wbuf[8] = {1,2,3,4,5,6,7,8};

    CHECK(bstack_allocator_alloc(al, 8, &s) == 0);
    CHECK(bstack_slice_write(s, wbuf, 8) == 0);

    /* Grow within same block */
    CHECK(bstack_allocator_realloc(al, s, 14, &s2) == 0);
    CHECK(s2.offset == s.offset);
    CHECK(s2.len == 14);
    uint8_t rbuf[14];
    CHECK(bstack_slice_read(s2, rbuf) == 0);
    CHECK(memcmp(rbuf, wbuf, 8) == 0);
    /* New bytes must be zero */
    { int ok = 1, i; for (i = 8; i < 14; i++) if (rbuf[i]) { ok=0; break; } CHECK(ok); }

    /* Shrink back */
    CHECK(bstack_allocator_realloc(al, s2, 4, &s3) == 0);
    CHECK(s3.offset == s.offset);
    CHECK(s3.len == 4);
    CHECK(bstack_slice_read(s3, rbuf) == 0);
    CHECK(memcmp(rbuf, wbuf, 4) == 0);

    bstack_allocator_dealloc(al, s3);
    bstack_close(slab_bstack_allocator_into_stack(a));
    sl_unlink(tmp); return 0;
}

static int test_persist_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    uint64_t saved;
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        slab_bstack_allocator_t *a = slab_bstack_allocator_new(bs, 16);
        CHECK(a);
        bstack_slice_t s;
        CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 12, &s) == 0);
        saved = s.offset;
        uint8_t data[12]; memset(data, 0x55, 12);
        CHECK(bstack_slice_write(s, data, 12) == 0);
        bstack_close(slab_bstack_allocator_into_stack(a));
    }
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        slab_bstack_allocator_t *a = slab_bstack_allocator_open(bs);
        CHECK(a);
        uint8_t rbuf[12];
        CHECK(bstack_get(bstack_allocator_stack((bstack_allocator_t *)a),
                         saved, saved + 12, rbuf) == 0);
        { int ok=1,i; for(i=0;i<12;i++) if(rbuf[i]!=0x55){ok=0;break;} CHECK(ok); }
        bstack_close(slab_bstack_allocator_into_stack(a));
    }
    sl_unlink(tmp); return 0;
}

static int test_zero_alloc(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    slab_bstack_allocator_t *a = slab_bstack_allocator_new(bs, 16);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t s;
    int i;
    for (i = 0; i < 64; i++) {
        CHECK(bstack_allocator_alloc(al, 0, &s) == 0);
        CHECK(s.len == 0);
        CHECK(bstack_allocator_dealloc(al, s) == 0);
    }

    bstack_close(slab_bstack_allocator_into_stack(a));
    sl_unlink(tmp); return 0;
}

static int test_block_size_mismatch(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        slab_bstack_allocator_t *a = slab_bstack_allocator_new(bs, 32);
        CHECK(a);
        bstack_close(slab_bstack_allocator_into_stack(a));
    }
    sl_unlink(tmp); return 0;
}

/* =========================================================================
 * Fuzz tests
 * ====================================================================== */

#define FUZZ_ITERS       10000
#define FUZZ_MAX_SIZE    UINT64_C(1024)
#define FUZZ_SESSIONS    20
#define FUZZ_OPS_SESSION 100

/* Generic fuzz helpers parameterised on block_size. */

static int fuzz_alloc_dealloc(uint64_t block_size)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    uint64_t seed = (uint64_t)(unsigned long)time(NULL)
                  ^ (uint64_t)(unsigned long)sl_getpid()
                  ^ block_size;
    rng_seed(seed);

    bstack_t *bs = bstack_open(tmp);
    if (!bs) return -1;
    slab_bstack_allocator_t *a = slab_bstack_allocator_new(bs, block_size);
    if (!a) { bstack_close(bs); return -1; }
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    sl_vec_t live = {0};
    uint64_t next_id = 0;
    int ret = 0, iter;

    for (iter = 0; iter < FUZZ_ITERS; iter++) {
        if (rng_bool(0.7) || live.count == 0) {
            uint64_t size = rng_range(0, FUZZ_MAX_SIZE);
            bstack_slice_t s;
            if (bstack_allocator_alloc(al, size, &s) != 0) continue;

            uint64_t id = next_id++;
            if (size > 0) {
                uint8_t *buf = malloc((size_t)size);
                if (!buf) { ret = -1; break; }
                id_fill(buf, (size_t)size, id);
                bstack_slice_write(s, buf, (size_t)size);
                free(buf);
            }
            sl_entry_t e; e.offset = s.offset; e.len = s.len; e.id = id;
            if (sl_vec_push(&live, e) != 0) { ret = -1; break; }
        } else {
            size_t idx = (size_t)(rng_next() % live.count);
            sl_entry_t e = sl_vec_swap_remove(&live, idx);
            bstack_slice_t s = entry_slice(al, e);

            if (e.len > 0) {
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
            }
            if (bstack_allocator_dealloc(al, s) != 0) { ret = -1; break; }
        }
    }

    { size_t i; for (i = 0; i < live.count; i++)
        bstack_allocator_dealloc(al, entry_slice(al, live.data[i])); }
    sl_vec_free(&live);
    bstack_close(slab_bstack_allocator_into_stack(a));
    sl_unlink(tmp);
    return ret;
}

static int fuzz_alloc_realloc_dealloc(uint64_t block_size)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    uint64_t seed = (uint64_t)(unsigned long)time(NULL)
                  ^ (uint64_t)(unsigned long)sl_getpid()
                  ^ block_size ^ UINT64_C(0xdeadbeef);
    rng_seed(seed);

    bstack_t *bs = bstack_open(tmp);
    if (!bs) return -1;
    slab_bstack_allocator_t *a = slab_bstack_allocator_new(bs, block_size);
    if (!a) { bstack_close(bs); return -1; }
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    sl_vec_t live = {0};
    uint64_t next_id = 0;
    int ret = 0, iter;

    for (iter = 0; iter < FUZZ_ITERS; iter++) {
        if (rng_bool(0.6) || live.count == 0) {
            uint64_t size = rng_range(0, FUZZ_MAX_SIZE);
            bstack_slice_t s;
            if (bstack_allocator_alloc(al, size, &s) != 0) continue;

            uint64_t id = next_id++;
            if (size > 0) {
                uint8_t *buf = malloc((size_t)size);
                if (!buf) { ret = -1; break; }
                id_fill(buf, (size_t)size, id);
                bstack_slice_write(s, buf, (size_t)size);
                free(buf);
            }
            sl_entry_t e; e.offset = s.offset; e.len = s.len; e.id = id;
            if (sl_vec_push(&live, e) != 0) { ret = -1; break; }
        } else {
            size_t idx = (size_t)(rng_next() % live.count);
            sl_entry_t old_e = live.data[idx];
            bstack_slice_t old_s = entry_slice(al, old_e);

            if (old_e.len > 0) {
                uint8_t *buf = malloc((size_t)old_e.len);
                if (!buf) { ret = -1; break; }
                if (bstack_slice_read(old_s, buf) != 0 ||
                    id_verify(buf, (size_t)old_e.len, old_e.id, "pre-op") != 0) {
                    free(buf);
                    fprintf(stderr, "  seed=%llu iter=%d\n",
                            (unsigned long long)seed, iter);
                    ret = -1; break;
                }
                free(buf);
            }

            if (rng_bool(0.8)) {
                uint64_t new_size = rng_range(0, FUZZ_MAX_SIZE);
                bstack_slice_t new_s;
                if (bstack_allocator_realloc(al, old_s, new_size, &new_s) == 0) {
                    uint64_t verify_len =
                        old_e.len < new_size ? old_e.len : new_size;
                    if (new_size > 0) {
                        uint8_t *buf = malloc((size_t)new_size);
                        if (!buf) { ret = -1; break; }
                        if (bstack_slice_read(new_s, buf) != 0) {
                            free(buf); ret = -1; break;
                        }
                        if (verify_len > 0 &&
                            id_verify(buf, (size_t)verify_len,
                                      old_e.id, "realloc-prefix") != 0) {
                            free(buf);
                            fprintf(stderr, "  seed=%llu iter=%d\n",
                                    (unsigned long long)seed, iter);
                            ret = -1; break;
                        }
                        if (new_size > old_e.len) {
                            uint64_t i;
                            for (i = old_e.len; i < new_size; i++) {
                                if (buf[i] != 0) {
                                    fprintf(stderr,
                                        "  not zero-init byte %llu after "
                                        "realloc (seed=%llu iter=%d)\n",
                                        (unsigned long long)i,
                                        (unsigned long long)seed, iter);
                                    free(buf); ret = -1; break;
                                }
                            }
                            if (ret != 0) break;
                        }
                        id_fill(buf, (size_t)new_size, old_e.id);
                        bstack_slice_write(new_s, buf, (size_t)new_size);
                        free(buf);
                    }
                    live.data[idx].offset = new_s.offset;
                    live.data[idx].len    = new_s.len;
                }
            } else {
                sl_vec_swap_remove(&live, idx);
                if (bstack_allocator_dealloc(al, old_s) != 0) {
                    ret = -1; break;
                }
            }
        }
    }

    { size_t i; for (i = 0; i < live.count; i++)
        bstack_allocator_dealloc(al, entry_slice(al, live.data[i])); }
    sl_vec_free(&live);
    bstack_close(slab_bstack_allocator_into_stack(a));
    sl_unlink(tmp);
    return ret;
}

static int fuzz_reopen(uint64_t block_size)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    uint64_t seed = (uint64_t)(unsigned long)time(NULL)
                  ^ (uint64_t)(unsigned long)sl_getpid()
                  ^ block_size ^ UINT64_C(0xcafef00d);
    rng_seed(seed);

    {
        bstack_t *bs = bstack_open(tmp);
        if (!bs) return -1;
        slab_bstack_allocator_t *a = slab_bstack_allocator_new(bs, block_size);
        if (!a) { bstack_close(bs); return -1; }
        bstack_close(slab_bstack_allocator_into_stack(a));
    }

    typedef struct { uint64_t start; uint64_t len; uint8_t pattern; } rec_t;
    rec_t  *live     = NULL;
    size_t  live_cnt = 0, live_cap = 0;
    uint8_t next_pat = 1;
    int     ret      = 0, session;

    for (session = 0; session < FUZZ_SESSIONS && ret == 0; session++) {
        bstack_t *bs = bstack_open(tmp);
        if (!bs) { ret = -1; break; }
        slab_bstack_allocator_t *a = slab_bstack_allocator_open(bs);
        if (!a) { bstack_close(bs); ret = -1; break; }
        bstack_allocator_t *al = (bstack_allocator_t *)a;

        /* Verify all live records survived the reopen. */
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
        if (ret != 0) {
            bstack_close(slab_bstack_allocator_into_stack(a)); break;
        }

        /* Random operations. */
        {
            int op;
            for (op = 0; op < FUZZ_OPS_SESSION && ret == 0; op++) {
                unsigned choice = live_cnt == 0 ? 0u
                                : (unsigned)(rng_next() % 4u);

                if (choice == 0) {
                    uint64_t len = rng_range(1, 512);
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
                        rec_t *t = realloc(live, nc * sizeof *t);
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
                    uint64_t new_len = rng_range(1, 512);
                    bstack_slice_t old_s, new_s;
                    old_s.allocator = al;
                    old_s.offset    = rec->start;
                    old_s.len       = rec->len;
                    if (bstack_allocator_realloc(al, old_s, new_len,
                                                  &new_s) != 0) continue;
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
                    rec_t rec = live[idx];
                    live[idx] = live[--live_cnt];
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
                    rec_t rec = live[idx];
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

        bstack_close(slab_bstack_allocator_into_stack(a));
    }

    free(live);
    sl_unlink(tmp);
    return ret;
}

/* Per-block-size test wrappers */

#define FUZZ_SUITE(bsz)                                                 \
static int test_fuzz_alloc_dealloc_##bsz(void)                         \
    { return fuzz_alloc_dealloc(bsz); }                                 \
static int test_fuzz_alloc_realloc_dealloc_##bsz(void)                 \
    { return fuzz_alloc_realloc_dealloc(bsz); }                        \
static int test_fuzz_reopen_##bsz(void)                                \
    { return fuzz_reopen(bsz); }

FUZZ_SUITE(8)
FUZZ_SUITE(16)
FUZZ_SUITE(64)

/* =========================================================================
 * Concurrent tests (requires -DBSTACK_FEATURE_ATOMIC)
 *
 * Without ATOMIC the allocator is not Sync: free-list mutations span two
 * bstack calls with no lock, so sharing one allocator across threads is
 * undefined and these tests would race on the on-disk free list with no
 * protection.
 * ====================================================================== */

#ifdef BSTACK_FEATURE_ATOMIC
#include <pthread.h>

#define SL_THREADS  8
#define SL_ITERS    200

typedef struct {
    bstack_allocator_t *a;
    int                 tid;
    int                 ok;
} sl_worker_arg_t;

/* --- concurrent alloc / dealloc data integrity ------------------------ */

static void *sl_alloc_dealloc_worker(void *raw)
{
    sl_worker_arg_t *w = raw;
    uint8_t pat = (uint8_t)w->tid;
    w->ok = 1;

    for (int i = 0; i < SL_ITERS; i++) {
        bstack_slice_t s;
        if (bstack_allocator_alloc(w->a, 16, &s) != 0) { w->ok = 0; return NULL; }

        uint8_t wbuf[16], rbuf[16];
        memset(wbuf, pat, 16);
        if (bstack_slice_write(s, wbuf, 16) != 0 ||
            bstack_slice_read(s, rbuf)      != 0 ||
            memcmp(wbuf, rbuf, 16)          != 0) {
            w->ok = 0; return NULL;
        }
        if (bstack_allocator_dealloc(w->a, s) != 0) { w->ok = 0; return NULL; }
    }
    return NULL;
}

static int test_concurrent_alloc_dealloc_data_integrity(void)
{
    /* Verify that the free-list lock prevents two threads from receiving the
     * same block.  Each thread writes its pattern after alloc and reads it
     * back before dealloc; a duplicate-block race would produce a clobber. */
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    slab_bstack_allocator_t *a = slab_bstack_allocator_new(bs, 16);
    CHECK(a);

    pthread_t       threads[SL_THREADS];
    sl_worker_arg_t args[SL_THREADS];
    for (int i = 0; i < SL_THREADS; i++) {
        args[i].a   = (bstack_allocator_t *)a;
        args[i].tid = i;
        args[i].ok  = 1;
        pthread_create(&threads[i], NULL, sl_alloc_dealloc_worker, &args[i]);
    }
    for (int i = 0; i < SL_THREADS; i++) pthread_join(threads[i], NULL);
    for (int i = 0; i < SL_THREADS; i++) CHECK(args[i].ok);

    bstack_close(slab_bstack_allocator_into_stack(a));
    sl_unlink(tmp); return 0;
}

/* --- concurrent realloc grow/shrink tail paths ------------------------ */

#define SL_REALLOC_THREADS  6
#define SL_REALLOC_ITERS    150

typedef struct {
    bstack_allocator_t *a;
    int                 tid;
    int                 ok;
} sl_realloc_arg_t;

static void *sl_realloc_worker(void *raw)
{
    /* Each thread owns one allocation and repeatedly grows then shrinks it.
     * Whichever allocation is at the tail exercises try_extend_zeros /
     * try_discard; others hit copy-grow / block-recycle paths.  Both branches
     * are exercised every round because threads race for the tail.
     * block_size = 16: alloc(12) → 1 block; alloc(33) → 3 blocks. */
    sl_realloc_arg_t *w = raw;
    uint8_t pat = (uint8_t)(w->tid + 0x40);
    w->ok = 1;

    bstack_slice_t s;
    if (bstack_allocator_alloc(w->a, 12, &s) != 0) { w->ok = 0; return NULL; }
    uint8_t init[12]; memset(init, pat, 12);
    if (bstack_slice_write(s, init, 12) != 0) { w->ok = 0; return NULL; }

    for (int i = 0; i < SL_REALLOC_ITERS; i++) {
        bstack_slice_t s2, s3;
        uint8_t rbuf[33];

        /* Grow: tail → try_extend_zeros; non-tail → copy. */
        if (bstack_allocator_realloc(w->a, s, 33, &s2) != 0) {
            w->ok = 0; return NULL;
        }
        if (bstack_slice_read(s2, rbuf) != 0) { w->ok = 0; return NULL; }
        for (size_t j = 0; j < 12; j++) {
            if (rbuf[j] != pat) {
                fprintf(stderr,
                    "  grow clobber tid=%d byte %zu got 0x%02x want 0x%02x\n",
                    w->tid, j, rbuf[j], pat);
                w->ok = 0; return NULL;
            }
        }

        /* Shrink: tail → try_discard; non-tail → recycle excess blocks. */
        if (bstack_allocator_realloc(w->a, s2, 12, &s3) != 0) {
            w->ok = 0; return NULL;
        }
        if (bstack_slice_read(s3, rbuf) != 0) { w->ok = 0; return NULL; }
        for (size_t j = 0; j < 12; j++) {
            if (rbuf[j] != pat) {
                fprintf(stderr,
                    "  shrink clobber tid=%d byte %zu got 0x%02x want 0x%02x\n",
                    w->tid, j, rbuf[j], pat);
                w->ok = 0; return NULL;
            }
        }
        s = s3;
    }

    if (bstack_allocator_dealloc(w->a, s) != 0) { w->ok = 0; return NULL; }
    return NULL;
}

static int test_concurrent_realloc_tail_paths(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    slab_bstack_allocator_t *a = slab_bstack_allocator_new(bs, 16);
    CHECK(a);

    pthread_t        threads[SL_REALLOC_THREADS];
    sl_realloc_arg_t args[SL_REALLOC_THREADS];
    for (int i = 0; i < SL_REALLOC_THREADS; i++) {
        args[i].a   = (bstack_allocator_t *)a;
        args[i].tid = i;
        args[i].ok  = 1;
        pthread_create(&threads[i], NULL, sl_realloc_worker, &args[i]);
    }
    for (int i = 0; i < SL_REALLOC_THREADS; i++) pthread_join(threads[i], NULL);
    for (int i = 0; i < SL_REALLOC_THREADS; i++) CHECK(args[i].ok);

    bstack_close(slab_bstack_allocator_into_stack(a));
    sl_unlink(tmp); return 0;
}

/* ---- bulk (BStackBulkAllocator) --------------------------------------- */

static int test_bulk_alloc_distinct_usable(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    slab_bstack_allocator_t *a = slab_bstack_allocator_new(bs, 16);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    uint64_t lens[3] = { 8, 16, 40 }; /* 40 -> oversized (3 blocks) */
    bstack_slice_t out[3];
    CHECK(bstack_allocator_alloc_bulk(al, lens, 3, out) == 0);
    CHECK(out[0].len == 8 && out[1].len == 16 && out[2].len == 40);
    CHECK(out[0].offset != out[1].offset && out[1].offset != out[2].offset);
    for (size_t i = 0; i < 3; i++) {
        uint8_t buf[40]; memset(buf, (int)(i + 1), (size_t)lens[i]);
        CHECK(bstack_slice_write(out[i], buf, (size_t)lens[i]) == 0);
    }
    for (size_t i = 0; i < 3; i++) {
        uint8_t r[40], want[40]; memset(want, (int)(i + 1), (size_t)lens[i]);
        CHECK(bstack_slice_read(out[i], r) == 0);
        CHECK(memcmp(r, want, (size_t)lens[i]) == 0);
    }
    CHECK(bstack_allocator_dealloc_bulk(al, out, 3) == 0);
    bstack_close(slab_bstack_allocator_into_stack(a));
    sl_unlink(tmp); return 0;
}

static int test_bulk_zero_length_null_slices(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    slab_bstack_allocator_t *a = slab_bstack_allocator_new(bs, 16);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    uint64_t lens[3] = { 0, 16, 0 };
    bstack_slice_t out[3];
    CHECK(bstack_allocator_alloc_bulk(al, lens, 3, out) == 0);
    CHECK(out[0].len == 0 && out[1].len == 16 && out[2].len == 0);
    bstack_close(slab_bstack_allocator_into_stack(a));
    sl_unlink(tmp); return 0;
}

static int test_bulk_reuses_freed_blocks(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    slab_bstack_allocator_t *a = slab_bstack_allocator_new(bs, 16);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    uint64_t lens[3] = { 16, 16, 16 };
    bstack_slice_t first[3], second[3];
    CHECK(bstack_allocator_alloc_bulk(al, lens, 3, first) == 0);
    /* Write a pattern, then bulk-free. */
    for (size_t i = 0; i < 3; i++) {
        uint8_t buf[16]; memset(buf, 0x5A, 16);
        CHECK(bstack_slice_write(first[i], buf, 16) == 0);
    }
    uint64_t freed[3] = { first[0].offset, first[1].offset, first[2].offset };
    CHECK(bstack_allocator_dealloc_bulk(al, first, 3) == 0);
    /* Realloc: all three reused, and scrubbed back to zero. */
    CHECK(bstack_allocator_alloc_bulk(al, lens, 3, second) == 0);
    for (size_t i = 0; i < 3; i++) {
        int found = 0;
        for (size_t j = 0; j < 3; j++) if (second[i].offset == freed[j]) found = 1;
        CHECK(found);
        uint8_t r[16], zero[16]; memset(zero, 0, 16);
        CHECK(bstack_slice_read(second[i], r) == 0);
        CHECK(memcmp(r, zero, 16) == 0);
    }
    bstack_close(slab_bstack_allocator_into_stack(a));
    sl_unlink(tmp); return 0;
}

static int test_bulk_dealloc_rejects_foreign(void)
{
    char t1[64], t2[64]; make_tmp(t1, sizeof t1); make_tmp(t2, sizeof t2);
    bstack_t *b1 = bstack_open(t1); CHECK(b1);
    bstack_t *b2 = bstack_open(t2); CHECK(b2);
    slab_bstack_allocator_t *a1 = slab_bstack_allocator_new(b1, 32); CHECK(a1);
    slab_bstack_allocator_t *a2 = slab_bstack_allocator_new(b2, 32); CHECK(a2);

    bstack_slice_t own, foreign;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a2, 32, &own) == 0);
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a1, 32, &foreign) == 0);
    bstack_slice_t batch[2] = { own, foreign };
    errno = 0;
    CHECK(bstack_allocator_dealloc_bulk((bstack_allocator_t *)a2, batch, 2) == -1);
    CHECK(errno == EINVAL);

    bstack_close(slab_bstack_allocator_into_stack(a1));
    bstack_close(slab_bstack_allocator_into_stack(a2));
    sl_unlink(t1); sl_unlink(t2); return 0;
}

static int test_bulk_alloc_detects_cycle(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    slab_bstack_allocator_t *a = slab_bstack_allocator_new(bs, 16);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    bstack_slice_t sa, sb;
    CHECK(bstack_allocator_alloc(al, 16, &sa) == 0);
    CHECK(bstack_allocator_alloc(al, 16, &sb) == 0);
    uint64_t b_off = sb.offset;
    CHECK(bstack_allocator_dealloc(al, sa) == 0);
    CHECK(bstack_allocator_dealloc(al, sb) == 0);
    /* Corrupt the free list into a self-cycle: b.next -> b (next at block start). */
    uint8_t nb[8];
    for (int k = 0; k < 8; k++) nb[k] = (uint8_t)(b_off >> (8 * k));
    CHECK(bstack_set(bs, b_off, nb, 8) == 0);
    bstack_slice_t out[3];
    errno = 0;
    CHECK(bstack_allocator_alloc_bulk(al, (uint64_t[]){16,16,16}, 3, out) == -1);
    CHECK(errno == EINVAL);

    bstack_close(slab_bstack_allocator_into_stack(a));
    sl_unlink(tmp); return 0;
}

#endif /* BSTACK_FEATURE_ATOMIC */

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
        slab_bstack_allocator_t *a1 = slab_bstack_allocator_new(b1, 64); CHECK(a1);
        slab_bstack_allocator_t *a2 = slab_bstack_allocator_new(b2, 64); CHECK(a2);
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

        bstack_close(slab_bstack_allocator_into_stack(a1));
        bstack_close(slab_bstack_allocator_into_stack(a2));
    }
    sl_unlink(t1); sl_unlink(t2); return 0;
}

/* =========================================================================
 * main
 * ====================================================================== */

int main(void)
{
    /* Smoke */
    T(test_alloc_write_read);
    T(test_slot_reuse);
    T(test_oversized_tail_discard);
    T(test_realloc_grow_shrink);
    T(test_persist_reopen);
    T(test_zero_alloc);
    T(test_block_size_mismatch);

    /* Fuzz — block_size 8 */
    T(test_fuzz_alloc_dealloc_8);
    T(test_fuzz_alloc_realloc_dealloc_8);
    T(test_fuzz_reopen_8);

    /* Fuzz — block_size 16 */
    T(test_fuzz_alloc_dealloc_16);
    T(test_fuzz_alloc_realloc_dealloc_16);
    T(test_fuzz_reopen_16);

    /* Fuzz — block_size 64 */
    T(test_fuzz_alloc_dealloc_64);
    T(test_fuzz_alloc_realloc_dealloc_64);
    T(test_fuzz_reopen_64);

#ifdef BSTACK_FEATURE_ATOMIC
    /* Concurrent */
    T(test_concurrent_alloc_dealloc_data_integrity);
    T(test_concurrent_realloc_tail_paths);

    /* Bulk */
    T(test_bulk_alloc_distinct_usable);
    T(test_bulk_zero_length_null_slices);
    T(test_bulk_reuses_freed_blocks);
    T(test_bulk_dealloc_rejects_foreign);
    T(test_bulk_alloc_detects_cycle);
#endif

    T(test_foreign_slice_is_rejected);

    printf("\n%d/%d passed\n", g_passed, g_total);
    return (g_passed == g_total) ? 0 : 1;
}
