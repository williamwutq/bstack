/* first_fit_bstack_allocator_t — smoke + fuzz tests.
 * Requires -DBSTACK_FEATURE_SET.
 * Mirrors alloc_fuzz_tests.rs in structure and coverage. */

#ifndef _WIN32
#  define _DARWIN_C_SOURCE
#  define _DEFAULT_SOURCE
#  define _POSIX_C_SOURCE 200809L
#endif

#include "bstack_alloc.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef _WIN32
#  define WIN32_LEAN_AND_MEAN
#  include <windows.h>
#  include <process.h>
#  define ff_unlink(p)  DeleteFileA(p)
#  define ff_getpid()   ((unsigned long)_getpid())
#else
#  include <unistd.h>
#  define ff_unlink(p)  unlink(p)
#  define ff_getpid()   ((unsigned long)getpid())
#endif

/* =========================================================================
 * Harness — identical style to test_bstack.c
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
    snprintf(buf, n, "bstack_ff_%lu_%ld.tmp",
             (unsigned long)GetCurrentProcessId(), (long)s);
    DeleteFileA(buf);
}
#else
static void make_tmp(char *buf, size_t n)
{
    snprintf(buf, n, "/tmp/bstack_ff_XXXXXX");
    int fd = mkstemp(buf);
    if (fd >= 0) { close(fd); unlink(buf); }
}
#endif

/* =========================================================================
 * splitmix64 — lightweight PRNG (avoids rand() quality issues)
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

/* Uniform in [lo, hi] */
static uint64_t rng_range(uint64_t lo, uint64_t hi)
{
    return lo + rng_next() % (hi - lo + 1);
}

/* True with probability p */
static int rng_bool(double p)
{
    return rng_next() < (uint64_t)(p * (double)UINT64_MAX);
}

/* =========================================================================
 * Data-pattern helpers
 *
 * ID pattern (alloc/dealloc/realloc tests):
 *   byte i = (id >> ((i%8)*8)) & 0xFF  — encodes the 8-byte LE id, repeating.
 *
 * Pat pattern (reopen test):
 *   byte i = (uint8_t)(pattern + i)     — wrapping add, matches Rust fill().
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

/* Returns 0 if buf matches the ID pattern, -1 on mismatch. */
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
 * ff_vec_t — growable array of live-allocation records
 * ====================================================================== */

typedef struct { uint64_t offset; uint64_t len; uint64_t id; } ff_entry_t;
typedef struct { ff_entry_t *data; size_t count; size_t cap; } ff_vec_t;

static int ff_vec_push(ff_vec_t *v, ff_entry_t e)
{
    if (v->count == v->cap) {
        size_t nc = v->cap ? v->cap * 2 : 16;
        ff_entry_t *t = realloc(v->data, nc * sizeof *t);
        if (!t) return -1;
        v->data = t; v->cap = nc;
    }
    v->data[v->count++] = e;
    return 0;
}

static ff_entry_t ff_vec_swap_remove(ff_vec_t *v, size_t idx)
{
    ff_entry_t e = v->data[idx];
    v->data[idx] = v->data[--v->count];
    return e;
}

static void ff_vec_free(ff_vec_t *v)
{
    free(v->data); v->data = NULL; v->count = 0; v->cap = 0;
}

static bstack_slice_t entry_slice(bstack_allocator_t *a, ff_entry_t e)
{
    bstack_slice_t s; s.allocator = a; s.offset = e.offset; s.len = e.len;
    return s;
}

/* =========================================================================
 * Smoke tests
 * ====================================================================== */

static int test_alloc_small(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    CHECK(a);

    /* alloc(5) and dealloc must not error — exercises the align_len fix */
    bstack_slice_t s;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 5, &s) == 0);
    CHECK(s.len == 5);
    uint8_t data[5] = {1,2,3,4,5};
    bstack_slice_write(s, data, 5);
    CHECK(bstack_allocator_dealloc((bstack_allocator_t *)a, s) == 0);

    bstack_close(first_fit_bstack_allocator_into_stack(a));
    ff_unlink(tmp); return 0;
}

static int test_alloc_write_read(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    CHECK(a);

    bstack_slice_t s;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 16, &s) == 0);
    uint8_t wbuf[16], rbuf[16];
    memset(wbuf, 0xAB, 16);
    CHECK(bstack_slice_write(s, wbuf, 16) == 0);
    CHECK(bstack_slice_read(s, rbuf) == 0);
    CHECK(memcmp(wbuf, rbuf, 16) == 0);

    bstack_close(first_fit_bstack_allocator_into_stack(a));
    ff_unlink(tmp); return 0;
}

static int test_realloc_grow(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    CHECK(a);

    bstack_slice_t s, s2;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 32, &s) == 0);
    uint8_t wbuf[32]; memset(wbuf, 0xCD, 32);
    CHECK(bstack_slice_write(s, wbuf, 32) == 0);
    CHECK(bstack_allocator_realloc((bstack_allocator_t *)a, s, 64, &s2) == 0);
    CHECK(s2.offset == s.offset); /* tail block grows in place */
    CHECK(s2.len == 64);
    uint8_t rbuf[64];
    CHECK(bstack_slice_read(s2, rbuf) == 0);
    CHECK(memcmp(rbuf, wbuf, 32) == 0);
    { int ok = 1, i; for (i = 32; i < 64; i++) if (rbuf[i]) { ok=0; break; } CHECK(ok); }

    bstack_close(first_fit_bstack_allocator_into_stack(a));
    ff_unlink(tmp); return 0;
}

static int test_slot_reuse(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    CHECK(a);

    bstack_slice_t sa, sb, sc;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 32, &sa) == 0);
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 32, &sb) == 0);
    uint64_t a_off = sa.offset;
    CHECK(bstack_allocator_dealloc((bstack_allocator_t *)a, sa) == 0);
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 16, &sc) == 0);
    CHECK(sc.offset == a_off); /* first-fit reuses the freed slot */

    bstack_allocator_dealloc((bstack_allocator_t *)a, sc);
    bstack_allocator_dealloc((bstack_allocator_t *)a, sb);
    bstack_close(first_fit_bstack_allocator_into_stack(a));
    ff_unlink(tmp); return 0;
}

static int test_persist_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    uint64_t saved;
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
        CHECK(a);
        bstack_slice_t s;
        CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 24, &s) == 0);
        saved = s.offset;
        uint8_t data[24]; memset(data, 0x77, 24);
        CHECK(bstack_slice_write(s, data, 24) == 0);
        bstack_close(first_fit_bstack_allocator_into_stack(a));
    }
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
        CHECK(a);
        /* Read directly at saved offset — data must survive close/reopen */
        uint8_t rbuf[24];
        CHECK(bstack_get(bstack_allocator_stack((bstack_allocator_t *)a),
                         saved, saved + 24, rbuf) == 0);
        { int ok=1,i; for(i=0;i<24;i++) if(rbuf[i]!=0x77){ok=0;break;} CHECK(ok); }
        /* Session-1 block still allocated; new alloc goes to a fresh slot */
        bstack_slice_t s;
        CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 24, &s) == 0);
        CHECK(s.offset != saved);
        bstack_close(first_fit_bstack_allocator_into_stack(a));
    }
    ff_unlink(tmp); return 0;
}

static int test_realloc_small(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    CHECK(a);

    bstack_slice_t s, s2;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 5, &s) == 0);
    uint8_t wbuf[5] = {10,20,30,40,50};
    CHECK(bstack_slice_write(s, wbuf, 5) == 0);
    CHECK(bstack_allocator_realloc((bstack_allocator_t *)a, s, 10, &s2) == 0);
    CHECK(s2.len == 10);
    uint8_t rbuf[10];
    CHECK(bstack_slice_read(s2, rbuf) == 0);
    CHECK(memcmp(rbuf, wbuf, 5) == 0);
    { int ok=1,i; for(i=5;i<10;i++) if(rbuf[i]){ok=0;break;} CHECK(ok); }

    CHECK(bstack_allocator_dealloc((bstack_allocator_t *)a, s2) == 0);
    bstack_close(first_fit_bstack_allocator_into_stack(a));
    ff_unlink(tmp); return 0;
}

static int test_slice_read_range(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    CHECK(a);

    bstack_slice_t s;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 16, &s) == 0);
    uint8_t wbuf[16], rbuf[16];
    int i;
    for (i = 0; i < 16; i++) wbuf[i] = (uint8_t)(i * 3);
    CHECK(bstack_slice_write(s, wbuf, 16) == 0);

    /* Full range */
    memset(rbuf, 0, sizeof rbuf);
    CHECK(bstack_slice_read_range(s, 0, 16, rbuf) == 0);
    CHECK(memcmp(rbuf, wbuf, 16) == 0);

    /* Sub-range [4, 12) */
    memset(rbuf, 0, sizeof rbuf);
    CHECK(bstack_slice_read_range(s, 4, 12, rbuf) == 0);
    CHECK(memcmp(rbuf, wbuf + 4, 8) == 0);

    /* Empty range is a no-op */
    CHECK(bstack_slice_read_range(s, 7, 7, rbuf) == 0);

    /* start > end => error */
    CHECK(bstack_slice_read_range(s, 10, 4, rbuf) == -1);

    /* end > s.len => error */
    CHECK(bstack_slice_read_range(s, 0, 17, rbuf) == -1);

    bstack_close(first_fit_bstack_allocator_into_stack(a));
    ff_unlink(tmp); return 0;
}

/* =========================================================================
 * Fuzz tests
 *
 * FUZZ_ITERS / sessions match the Rust fuzz test counts.
 * FUZZ_MIN_SIZE = 1 exercises the align_len fix for sub-16-byte slices.
 * ====================================================================== */

#define FUZZ_ITERS       10000
#define FUZZ_MIN_SIZE    UINT64_C(1)
#define FUZZ_MAX_SIZE    UINT64_C(1024)
#define FUZZ_SESSIONS    20
#define FUZZ_OPS_SESSION 100

/* fuzz_alloc_dealloc: random alloc / dealloc with per-block data verification */
static int test_fuzz_alloc_dealloc(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    uint64_t seed = (uint64_t)(unsigned long)time(NULL)
                  ^ (uint64_t)(unsigned long)ff_getpid();
    rng_seed(seed);

    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    ff_vec_t live = {0};
    uint64_t next_id = 0;
    int ret = 0, iter;

    for (iter = 0; iter < FUZZ_ITERS; iter++) {
        if (rng_bool(0.7) || live.count == 0) {
            /* Allocate */
            uint64_t size = rng_range(FUZZ_MIN_SIZE, FUZZ_MAX_SIZE);
            bstack_slice_t s;
            if (bstack_allocator_alloc(al, size, &s) != 0) continue;

            uint64_t id = next_id++;
            uint8_t *buf = malloc((size_t)size);
            if (!buf) { ret = -1; break; }
            id_fill(buf, (size_t)size, id);
            bstack_slice_write(s, buf, (size_t)size);
            free(buf);

            ff_entry_t e; e.offset = s.offset; e.len = s.len; e.id = id;
            if (ff_vec_push(&live, e) != 0) { ret = -1; break; }
        } else {
            /* Dealloc a random live entry */
            size_t idx = (size_t)(rng_next() % live.count);
            ff_entry_t e = ff_vec_swap_remove(&live, idx);
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
    ff_vec_free(&live);
    bstack_close(first_fit_bstack_allocator_into_stack(a));
    ff_unlink(tmp);
    return ret;
}

/* fuzz_alloc_realloc_dealloc: random alloc / realloc / dealloc with data
 * verification and zero-init checks on the grown region */
static int test_fuzz_alloc_realloc_dealloc(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    uint64_t seed = (uint64_t)(unsigned long)time(NULL)
                  ^ (uint64_t)(unsigned long)ff_getpid()
                  ^ UINT64_C(0xdeadbeef);
    rng_seed(seed);

    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    CHECK(a);
    bstack_allocator_t *al = (bstack_allocator_t *)a;

    ff_vec_t live = {0};
    uint64_t next_id = 0;
    int ret = 0, iter;

    for (iter = 0; iter < FUZZ_ITERS; iter++) {
        if (rng_bool(0.6) || live.count == 0) {
            /* Allocate */
            uint64_t size = rng_range(FUZZ_MIN_SIZE, FUZZ_MAX_SIZE);
            bstack_slice_t s;
            if (bstack_allocator_alloc(al, size, &s) != 0) continue;

            uint64_t id = next_id++;
            uint8_t *buf = malloc((size_t)size);
            if (!buf) { ret = -1; break; }
            id_fill(buf, (size_t)size, id);
            bstack_slice_write(s, buf, (size_t)size);
            free(buf);

            ff_entry_t e; e.offset = s.offset; e.len = s.len; e.id = id;
            if (ff_vec_push(&live, e) != 0) { ret = -1; break; }
        } else {
            size_t idx = (size_t)(rng_next() % live.count);
            ff_entry_t old_e = live.data[idx];
            bstack_slice_t old_s = entry_slice(al, old_e);

            /* Read and verify old data before any mutation */
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
                /* Realloc */
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
                    /* Prefix must match old ID pattern */
                    if (id_verify(new_buf, (size_t)verify_len,
                                  old_e.id, "realloc-prefix") != 0) {
                        free(new_buf);
                        fprintf(stderr, "  seed=%llu iter=%d\n",
                                (unsigned long long)seed, iter);
                        ret = -1; break;
                    }
                    /* Grown region must be zero-initialised */
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
                    /* Re-fill with the same ID so subsequent reads can verify */
                    id_fill(new_buf, (size_t)new_size, old_e.id);
                    bstack_slice_write(new_s, new_buf, (size_t)new_size);
                    free(new_buf);

                    live.data[idx].offset = new_s.offset;
                    live.data[idx].len    = new_s.len;
                }
            } else {
                /* Dealloc */
                ff_vec_swap_remove(&live, idx);
                if (bstack_allocator_dealloc(al, old_s) != 0) {
                    ret = -1; break;
                }
            }
        }
    }

    { size_t i; for (i = 0; i < live.count; i++)
        bstack_allocator_dealloc(al, entry_slice(al, live.data[i])); }
    ff_vec_free(&live);
    bstack_close(first_fit_bstack_allocator_into_stack(a));
    ff_unlink(tmp);
    return ret;
}

/* fuzz_reopen: multiple sessions each reopening the file, reconstructing
 * handles from saved (offset, len, pattern) records, verifying data
 * survived the reopen, then performing random alloc/realloc/dealloc/verify. */
static int test_fuzz_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    uint64_t seed = (uint64_t)(unsigned long)time(NULL)
                  ^ (uint64_t)(unsigned long)ff_getpid()
                  ^ UINT64_C(0xcafef00d);
    rng_seed(seed);

    /* Create file with allocator header */
    {
        bstack_t *bs = bstack_open(tmp); CHECK(bs);
        first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
        CHECK(a);
        bstack_close(first_fit_bstack_allocator_into_stack(a));
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
        first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
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
        if (ret != 0) { bstack_close(first_fit_bstack_allocator_into_stack(a)); break; }

        /* Random operations */
        {
            int op;
            for (op = 0; op < FUZZ_OPS_SESSION && ret == 0; op++) {
                unsigned choice = live_cnt == 0 ? 0u
                                : (unsigned)(rng_next() % 4u);

                if (choice == 0) {
                    /* Alloc */
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

                    /* Push into live array */
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
                    /* Realloc */
                    size_t idx = (size_t)(rng_next() % live_cnt);
                    rec_t *rec = &live[idx];
                    uint64_t new_len = rng_range(FUZZ_MIN_SIZE, 512);
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
                    /* Dealloc */
                    size_t idx = (size_t)(rng_next() % live_cnt);
                    rec_t rec = live[idx];
                    live[idx] = live[--live_cnt]; /* swap-remove */
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
                    /* Verify-only (read without mutation) */
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

        bstack_close(first_fit_bstack_allocator_into_stack(a));
    }

    free(live);
    ff_unlink(tmp);
    return ret;
}

/* =========================================================================
 * Concurrency tests — only meaningful when the allocator owns an internal
 * mutex, which it does under BSTACK_FEATURE_ATOMIC.  Without the feature,
 * sharing one allocator across threads is undefined and these tests would
 * race on the on-disk free list with no protection.
 * ====================================================================== */

#ifdef BSTACK_FEATURE_ATOMIC

/* ALFF header layout — mirrored from bstack_alloc.c for the flag poke. */
#define ALFF_FLAGS_OFFSET   24  /* OFFSET_SIZE(16) + magic(8) */
#define ALFF_HDR_OFFSET     48  /* OFFSET_SIZE(16) + HEADER_SIZE(32) — arena start */

/* --- concurrent alloc / dealloc data integrity ------------------------ */

#define FF_THREADS        8
#define FF_ITERS          200

typedef struct {
    bstack_allocator_t *a;
    int                 tid;
    int                 ok;
} ff_worker_arg_t;

static void *ff_alloc_dealloc_worker(void *raw)
{
    ff_worker_arg_t *w = raw;
    static const uint64_t sizes[] = {16, 24, 40, 64, 96, 128};
    w->ok = 1;

    for (int i = 0; i < FF_ITERS; i++) {
        uint64_t len = sizes[i % (int)(sizeof sizes / sizeof sizes[0])];
        bstack_slice_t s;
        if (bstack_allocator_alloc(w->a, len, &s) != 0) { w->ok = 0; return NULL; }

        uint8_t pat = (uint8_t)(w->tid + i);
        uint8_t *wbuf = malloc((size_t)len);
        uint8_t *rbuf = malloc((size_t)len);
        if (!wbuf || !rbuf) { free(wbuf); free(rbuf); w->ok = 0; return NULL; }
        memset(wbuf, pat, (size_t)len);

        if (bstack_slice_write(s, wbuf, (size_t)len) != 0 ||
            bstack_slice_read(s, rbuf) != 0 ||
            memcmp(wbuf, rbuf, (size_t)len) != 0) {
            free(wbuf); free(rbuf); w->ok = 0; return NULL;
        }
        free(wbuf); free(rbuf);

        if (bstack_allocator_dealloc(w->a, s) != 0) { w->ok = 0; return NULL; }
    }
    return NULL;
}

static int test_concurrent_alloc_dealloc_data_integrity(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    CHECK(a);

    pthread_t       threads[FF_THREADS];
    ff_worker_arg_t args[FF_THREADS];
    for (int i = 0; i < FF_THREADS; i++) {
        args[i].a   = (bstack_allocator_t *)a;
        args[i].tid = i;
        args[i].ok  = 1;
        pthread_create(&threads[i], NULL, ff_alloc_dealloc_worker, &args[i]);
    }
    for (int i = 0; i < FF_THREADS; i++) pthread_join(threads[i], NULL);
    for (int i = 0; i < FF_THREADS; i++) CHECK(args[i].ok);

    bstack_close(first_fit_bstack_allocator_into_stack(a));
    ff_unlink(tmp); return 0;
}

/* --- concurrent realloc grow/shrink data integrity --------------------- */

#define FF_REALLOC_THREADS  6
#define FF_REALLOC_ITERS    120

typedef struct {
    bstack_allocator_t *a;
    int                 tid;
    int                 ok;
} ff_realloc_arg_t;

static void *ff_realloc_worker(void *raw)
{
    ff_realloc_arg_t *w = raw;
    static const uint64_t sizes[] = {32, 64, 24, 96, 16, 128, 48};
    uint8_t pat = (uint8_t)(w->tid + 0x40);
    w->ok = 1;

    bstack_slice_t s;
    if (bstack_allocator_alloc(w->a, 16, &s) != 0) { w->ok = 0; return NULL; }
    uint8_t init_buf[16]; memset(init_buf, pat, 16);
    if (bstack_slice_write(s, init_buf, 16) != 0) { w->ok = 0; return NULL; }
    uint64_t prev_len = 16;

    for (int i = 0; i < FF_REALLOC_ITERS; i++) {
        uint64_t new_len = sizes[i % (int)(sizeof sizes / sizeof sizes[0])];
        bstack_slice_t s2;
        if (bstack_allocator_realloc(w->a, s, new_len, &s2) != 0) {
            w->ok = 0; return NULL;
        }
        s = s2;

        size_t keep = (prev_len < new_len) ? (size_t)prev_len : (size_t)new_len;
        uint8_t *rbuf = malloc((size_t)new_len);
        if (!rbuf) { w->ok = 0; return NULL; }
        if (bstack_slice_read(s, rbuf) != 0) { free(rbuf); w->ok = 0; return NULL; }
        for (size_t j = 0; j < keep; j++) {
            if (rbuf[j] != pat) {
                fprintf(stderr,
                    "  thread %d iter %d: byte %zu clobbered "
                    "(got 0x%02x, want 0x%02x)\n",
                    w->tid, i, j, rbuf[j], pat);
                free(rbuf); w->ok = 0; return NULL;
            }
        }
        free(rbuf);

        uint8_t *wbuf = malloc((size_t)new_len);
        if (!wbuf) { w->ok = 0; return NULL; }
        memset(wbuf, pat, (size_t)new_len);
        if (bstack_slice_write(s, wbuf, (size_t)new_len) != 0) {
            free(wbuf); w->ok = 0; return NULL;
        }
        free(wbuf);
        prev_len = new_len;
    }

    if (bstack_allocator_dealloc(w->a, s) != 0) { w->ok = 0; return NULL; }
    return NULL;
}

static int test_concurrent_realloc_grow_shrink_data_integrity(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    CHECK(a);

    pthread_t        threads[FF_REALLOC_THREADS];
    ff_realloc_arg_t args[FF_REALLOC_THREADS];
    for (int i = 0; i < FF_REALLOC_THREADS; i++) {
        args[i].a   = (bstack_allocator_t *)a;
        args[i].tid = i;
        args[i].ok  = 1;
        pthread_create(&threads[i], NULL, ff_realloc_worker, &args[i]);
    }
    for (int i = 0; i < FF_REALLOC_THREADS; i++) pthread_join(threads[i], NULL);
    for (int i = 0; i < FF_REALLOC_THREADS; i++) CHECK(args[i].ok);

    {
        uint64_t len;
        CHECK(bstack_allocator_len((bstack_allocator_t *)a, &len) == 0);
        CHECK(len == ALFF_HDR_OFFSET);
    }

    bstack_close(first_fit_bstack_allocator_into_stack(a));
    ff_unlink(tmp); return 0;
}

/* --- concurrent tail thrash ------------------------------------------- */

#define FF_TAIL_THREADS  6
#define FF_TAIL_ITERS    80

static void *ff_tail_worker(void *raw)
{
    bstack_allocator_t *a = raw;
    for (int i = 0; i < FF_TAIL_ITERS; i++) {
        bstack_slice_t s, s2, s3;
        if (bstack_allocator_alloc(a, 32, &s) != 0) return (void *)(intptr_t)-1;
        if (bstack_allocator_realloc(a, s, 64, &s2) != 0)
            return (void *)(intptr_t)-1;
        if (bstack_allocator_realloc(a, s2, 16, &s3) != 0)
            return (void *)(intptr_t)-1;
        if (bstack_allocator_dealloc(a, s3) != 0)
            return (void *)(intptr_t)-1;
    }
    return NULL;
}

static int test_concurrent_tail_thrash(void)
{
    /* Hammer the tail paths from many threads.  Whether each individual op
     * actually lands on the tail depends on the interleaving (another
     * thread's allocation can sit above this one), so this is about
     * exercising the recovery_needed CAS, the tail-grow extend, the
     * in-bucket realloc shrink, and the dealloc/cascade paths under
     * contention — not about asserting a specific final stack size. */
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    CHECK(a);

    pthread_t threads[FF_TAIL_THREADS];
    for (int i = 0; i < FF_TAIL_THREADS; i++)
        pthread_create(&threads[i], NULL, ff_tail_worker,
                       (bstack_allocator_t *)a);
    for (int i = 0; i < FF_TAIL_THREADS; i++) {
        void *ret = NULL;
        pthread_join(threads[i], &ret);
        CHECK(ret == NULL);
    }

    /* With cascade_discard_free_tail called after every alff_add_to_free_list,
     * a shrunken block whose dealloc goes through the non-tail path is still
     * reclaimed as soon as it coalesces to the tail. */
    {
        uint64_t len;
        CHECK(bstack_allocator_len((bstack_allocator_t *)a, &len) == 0);
        CHECK(len == ALFF_HDR_OFFSET);
    }

    bstack_close(first_fit_bstack_allocator_into_stack(a));
    ff_unlink(tmp); return 0;
}

/* --- recovery_needed CAS error propagation ---------------------------- */

static int test_recovery_needed_already_set_rejects_mutation(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    CHECK(a);

    /* Populate the free list so the next alloc must walk and unlink — that
     * is the path that calls set_recovery_needed.  alloc on an empty free
     * list just pushes the tail and bypasses the flag. */
    bstack_slice_t sa, sb;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 64, &sa) == 0);
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 64, &sb) == 0);
    CHECK(bstack_allocator_dealloc((bstack_allocator_t *)a, sa) == 0);

    /* Manually poke recovery_needed = 1 on disk. */
    bstack_t *stack = bstack_allocator_stack((bstack_allocator_t *)a);
    uint8_t one[4] = {1, 0, 0, 0};
    CHECK(bstack_set(stack, ALFF_FLAGS_OFFSET, one, 4) == 0);

    /* Reusing the freed slot now walks the free list, which calls
     * set_recovery_needed and must fail because the flag is already 1. */
    bstack_slice_t sc;
    errno = 0;
    int rc = bstack_allocator_alloc((bstack_allocator_t *)a, 64, &sc);
    CHECK(rc == -1);
    CHECK(errno == EINVAL);

    /* Reopen: the on-disk flag is still set, so first_fit_bstack_allocator_new
     * must run recovery and return a usable allocator. */
    bstack_close(first_fit_bstack_allocator_into_stack(a));
    bstack_t *bs2 = bstack_open(tmp); CHECK(bs2);
    first_fit_bstack_allocator_t *a2 = first_fit_bstack_allocator_new(bs2);
    CHECK(a2);
    bstack_slice_t probe;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a2, 16, &probe) == 0);
    bstack_close(first_fit_bstack_allocator_into_stack(a2));

    ff_unlink(tmp); return 0;
}

/* --- regression: cascade after non-tail dealloc reclaims arena --------- */

#define FF_CASCADE_THREADS  4
#define FF_CASCADE_ITERS    100

static void *ff_cascade_dealloc_worker(void *raw)
{
    bstack_allocator_t *a = raw;
    static const uint64_t sizes[] = {16, 32, 64, 128};
    for (int i = 0; i < FF_CASCADE_ITERS; i++) {
        uint64_t len = sizes[i % (int)(sizeof sizes / sizeof sizes[0])];
        bstack_slice_t s;
        if (bstack_allocator_alloc(a, len, &s) != 0) return (void *)(intptr_t)-1;
        if (bstack_allocator_dealloc(a, s) != 0)     return (void *)(intptr_t)-1;
    }
    return NULL;
}

static int test_dealloc_non_tail_cascade_reclaims_arena(void)
{
    /* Regression for missing alff_cascade_discard_free_tail after
     * alff_add_to_free_list in ff_vt_dealloc's non-tail path.  With
     * concurrent threads the last dealloc going through the non-tail path
     * can coalesce free neighbours to the physical tail, leaving a free
     * block in the free list permanently unless cascade is called. */
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    CHECK(a);

    pthread_t threads[FF_CASCADE_THREADS];
    for (int i = 0; i < FF_CASCADE_THREADS; i++)
        pthread_create(&threads[i], NULL, ff_cascade_dealloc_worker,
                       (bstack_allocator_t *)a);
    for (int i = 0; i < FF_CASCADE_THREADS; i++) {
        void *ret = NULL;
        pthread_join(threads[i], &ret);
        CHECK(ret == NULL);
    }

    {
        uint64_t len;
        CHECK(bstack_allocator_len((bstack_allocator_t *)a, &len) == 0);
        CHECK(len == ALFF_HDR_OFFSET);
    }

    bstack_close(first_fit_bstack_allocator_into_stack(a));
    ff_unlink(tmp); return 0;
}

/* --- regression: cascade after realloc copy-and-move reclaims arena ---- */

#define FF_REALLOC_CASCADE_THREADS  4
#define FF_REALLOC_CASCADE_ITERS    60

static void *ff_realloc_cascade_worker(void *raw)
{
    bstack_allocator_t *a = raw;
    for (int i = 0; i < FF_REALLOC_CASCADE_ITERS; i++) {
        bstack_slice_t s, s2;
        /* Grow far beyond the original block to force the copy-and-move
         * path: the old 16-byte block is freed via alff_add_to_free_list. */
        if (bstack_allocator_alloc(a, 16, &s) != 0)       return (void *)(intptr_t)-1;
        if (bstack_allocator_realloc(a, s, 128, &s2) != 0) return (void *)(intptr_t)-1;
        if (bstack_allocator_dealloc(a, s2) != 0)          return (void *)(intptr_t)-1;
    }
    return NULL;
}

static int test_realloc_copy_move_cascade_reclaims_arena(void)
{
    /* Regression for missing alff_cascade_discard_free_tail after
     * alff_add_to_free_list in ff_vt_realloc's copy-and-move paths.
     * After realloc copies data to a new block and frees the old one,
     * the freed old block can coalesce to the tail if cascade is not called. */
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    CHECK(a);

    pthread_t threads[FF_REALLOC_CASCADE_THREADS];
    for (int i = 0; i < FF_REALLOC_CASCADE_THREADS; i++)
        pthread_create(&threads[i], NULL, ff_realloc_cascade_worker,
                       (bstack_allocator_t *)a);
    for (int i = 0; i < FF_REALLOC_CASCADE_THREADS; i++) {
        void *ret = NULL;
        pthread_join(threads[i], &ret);
        CHECK(ret == NULL);
    }

    {
        uint64_t len;
        CHECK(bstack_allocator_len((bstack_allocator_t *)a, &len) == 0);
        CHECK(len == ALFF_HDR_OFFSET);
    }

    bstack_close(first_fit_bstack_allocator_into_stack(a));
    ff_unlink(tmp); return 0;
}

/* =========================================================================
 * bstack_slice_t: cas_on / cas_on_ne / cas_on_masked / process
 * (BSTACK_FEATURE_SET + BSTACK_FEATURE_ATOMIC).  Allocator-agnostic; a
 * first_fit allocator is just the simplest source of two live slices.
 * ====================================================================== */

static int test_slice_cas_on_match_swaps_and_returns_old(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    CHECK(a);

    bstack_slice_t guard, target;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 2, &guard) == 0);
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 2, &target) == 0);
    uint8_t guard_buf[2] = {1, 2};
    uint8_t target_buf[2] = {9, 9};
    CHECK(bstack_slice_write(guard, guard_buf, 2) == 0);
    CHECK(bstack_slice_write(target, target_buf, 2) == 0);

    uint8_t expected[2] = {1, 2};
    uint8_t new_bytes[2] = {3, 4};
    uint8_t old_buf[2];
    int ok = 0;
    CHECK(bstack_slice_cas_on(target, guard, expected, 2, new_bytes, 2,
                              old_buf, &ok) == 0);
    CHECK(ok == 1);
    CHECK(memcmp(old_buf, target_buf, 2) == 0);
    uint8_t rbuf[2];
    CHECK(bstack_slice_read(target, rbuf) == 0);
    CHECK(memcmp(rbuf, new_bytes, 2) == 0);

    bstack_close(first_fit_bstack_allocator_into_stack(a));
    ff_unlink(tmp); return 0;
}

static int test_slice_cas_on_no_match_leaves_target_untouched(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    CHECK(a);

    bstack_slice_t guard, target;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 2, &guard) == 0);
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 2, &target) == 0);
    uint8_t guard_buf[2] = {1, 2};
    uint8_t target_buf[2] = {9, 9};
    CHECK(bstack_slice_write(guard, guard_buf, 2) == 0);
    CHECK(bstack_slice_write(target, target_buf, 2) == 0);

    uint8_t expected[2] = {0, 0};
    uint8_t new_bytes[2] = {3, 4};
    uint8_t old_buf[2];
    int ok = 1;
    CHECK(bstack_slice_cas_on(target, guard, expected, 2, new_bytes, 2,
                              old_buf, &ok) == 0);
    CHECK(ok == 0);
    uint8_t rbuf[2];
    CHECK(bstack_slice_read(target, rbuf) == 0);
    CHECK(memcmp(rbuf, target_buf, 2) == 0);

    bstack_close(first_fit_bstack_allocator_into_stack(a));
    ff_unlink(tmp); return 0;
}

static int test_slice_cas_on_expected_length_mismatch_errors(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    CHECK(a);

    bstack_slice_t guard, target;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 2, &guard) == 0);
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 2, &target) == 0);

    uint8_t expected[1] = {0};
    uint8_t new_bytes[2] = {3, 4};
    uint8_t old_buf[2];
    errno = 0;
    CHECK(bstack_slice_cas_on(target, guard, expected, 1, new_bytes, 2,
                              old_buf, NULL) == -1);
    CHECK(errno == EINVAL);

    bstack_close(first_fit_bstack_allocator_into_stack(a));
    ff_unlink(tmp); return 0;
}

static int test_slice_cas_on_new_bytes_length_mismatch_errors(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    CHECK(a);

    bstack_slice_t guard, target;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 2, &guard) == 0);
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 2, &target) == 0);
    uint8_t guard_buf[2] = {1, 2};
    CHECK(bstack_slice_write(guard, guard_buf, 2) == 0);

    uint8_t expected[2] = {1, 2};
    uint8_t new_bytes[1] = {3};
    uint8_t old_buf[2];
    errno = 0;
    CHECK(bstack_slice_cas_on(target, guard, expected, 2, new_bytes, 1,
                              old_buf, NULL) == -1);
    CHECK(errno == EINVAL);

    bstack_close(first_fit_bstack_allocator_into_stack(a));
    ff_unlink(tmp); return 0;
}

static int test_slice_cas_on_cross_stack_errors(void)
{
    char t1[64], t2[64];
    make_tmp(t1, sizeof t1);
    make_tmp(t2, sizeof t2);
    bstack_t *b1 = bstack_open(t1); CHECK(b1);
    bstack_t *b2 = bstack_open(t2); CHECK(b2);
    first_fit_bstack_allocator_t *a1 = first_fit_bstack_allocator_new(b1); CHECK(a1);
    first_fit_bstack_allocator_t *a2 = first_fit_bstack_allocator_new(b2); CHECK(a2);

    bstack_slice_t guard, target;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a1, 2, &guard) == 0);
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a2, 2, &target) == 0);

    uint8_t expected[2] = {0, 0};
    uint8_t new_bytes[2] = {1, 1};
    uint8_t old_buf[2];
    errno = 0;
    CHECK(bstack_slice_cas_on(target, guard, expected, 2, new_bytes, 2,
                              old_buf, NULL) == -1);
    CHECK(errno == EINVAL);

    bstack_close(first_fit_bstack_allocator_into_stack(a1));
    bstack_close(first_fit_bstack_allocator_into_stack(a2));
    ff_unlink(t1); ff_unlink(t2); return 0;
}

static int test_slice_cas_on_ne_no_match_swaps_and_returns_old(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    CHECK(a);

    bstack_slice_t guard, target;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 2, &guard) == 0);
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 2, &target) == 0);
    uint8_t guard_buf[2] = {1, 2};
    uint8_t target_buf[2] = {9, 9};
    CHECK(bstack_slice_write(guard, guard_buf, 2) == 0);
    CHECK(bstack_slice_write(target, target_buf, 2) == 0);

    uint8_t expected[2] = {0, 0};
    uint8_t new_bytes[2] = {3, 4};
    uint8_t old_buf[2];
    int ok = 0;
    CHECK(bstack_slice_cas_on_ne(target, guard, expected, 2, new_bytes, 2,
                                 old_buf, &ok) == 0);
    CHECK(ok == 1);
    CHECK(memcmp(old_buf, target_buf, 2) == 0);
    uint8_t rbuf[2];
    CHECK(bstack_slice_read(target, rbuf) == 0);
    CHECK(memcmp(rbuf, new_bytes, 2) == 0);

    bstack_close(first_fit_bstack_allocator_into_stack(a));
    ff_unlink(tmp); return 0;
}

static int test_slice_cas_on_ne_match_returns_none(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    CHECK(a);

    bstack_slice_t guard, target;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 2, &guard) == 0);
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 2, &target) == 0);
    uint8_t guard_buf[2] = {1, 2};
    uint8_t target_buf[2] = {9, 9};
    CHECK(bstack_slice_write(guard, guard_buf, 2) == 0);
    CHECK(bstack_slice_write(target, target_buf, 2) == 0);

    uint8_t expected[2] = {1, 2};
    uint8_t new_bytes[2] = {3, 4};
    uint8_t old_buf[2];
    int ok = 1;
    CHECK(bstack_slice_cas_on_ne(target, guard, expected, 2, new_bytes, 2,
                                 old_buf, &ok) == 0);
    CHECK(ok == 0);
    uint8_t rbuf[2];
    CHECK(bstack_slice_read(target, rbuf) == 0);
    CHECK(memcmp(rbuf, target_buf, 2) == 0);

    bstack_close(first_fit_bstack_allocator_into_stack(a));
    ff_unlink(tmp); return 0;
}

static int test_slice_cas_on_masked_match_swaps_and_returns_old(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    CHECK(a);

    bstack_slice_t guard, target;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 2, &guard) == 0);
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 2, &target) == 0);
    uint8_t guard_buf[2] = {0xff, 0x0f};
    uint8_t target_buf[2] = {9, 9};
    CHECK(bstack_slice_write(guard, guard_buf, 2) == 0);
    CHECK(bstack_slice_write(target, target_buf, 2) == 0);

    /* mask = [0xff, 0xf0]: masked guard = [0xff, 0x00],
     * masked expected = [0xff, 0x00] -> match */
    uint8_t mask[2] = {0xff, 0xf0};
    uint8_t expected[2] = {0xff, 0x0f};
    uint8_t new_bytes[2] = {3, 4};
    uint8_t old_buf[2];
    int ok = 0;
    CHECK(bstack_slice_cas_on_masked(target, guard, mask, expected, 2,
                                     new_bytes, 2, old_buf, &ok) == 0);
    CHECK(ok == 1);
    CHECK(memcmp(old_buf, target_buf, 2) == 0);
    uint8_t rbuf[2];
    CHECK(bstack_slice_read(target, rbuf) == 0);
    CHECK(memcmp(rbuf, new_bytes, 2) == 0);

    bstack_close(first_fit_bstack_allocator_into_stack(a));
    ff_unlink(tmp); return 0;
}

static int test_slice_cas_on_masked_no_match_returns_none(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    CHECK(a);

    bstack_slice_t guard, target;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 1, &guard) == 0);
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 2, &target) == 0);
    uint8_t guard_buf[1] = {0x0f};
    uint8_t target_buf[2] = {9, 9};
    CHECK(bstack_slice_write(guard, guard_buf, 1) == 0);
    CHECK(bstack_slice_write(target, target_buf, 2) == 0);

    uint8_t mask[1] = {0xff};
    uint8_t expected[1] = {0xff};
    uint8_t new_bytes[2] = {3, 4};
    uint8_t old_buf[2];
    int ok = 1;
    CHECK(bstack_slice_cas_on_masked(target, guard, mask, expected, 1,
                                     new_bytes, 2, old_buf, &ok) == 0);
    CHECK(ok == 0);
    uint8_t rbuf[2];
    CHECK(bstack_slice_read(target, rbuf) == 0);
    CHECK(memcmp(rbuf, target_buf, 2) == 0);

    bstack_close(first_fit_bstack_allocator_into_stack(a));
    ff_unlink(tmp); return 0;
}

static int double_bytes_cb(uint8_t *buf, size_t len, void *ctx)
{
    (void)ctx;
    for (size_t i = 0; i < len; i++)
        buf[i] = (uint8_t)(buf[i] * 2);
    return 0;
}

static int test_slice_process_transforms_in_place(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp); CHECK(bs);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    CHECK(a);

    bstack_slice_t s;
    CHECK(bstack_allocator_alloc((bstack_allocator_t *)a, 4, &s) == 0);
    uint8_t wbuf[4] = {1, 2, 3, 4};
    CHECK(bstack_slice_write(s, wbuf, 4) == 0);
    CHECK(bstack_slice_process(s, double_bytes_cb, NULL) == 0);
    uint8_t rbuf[4];
    CHECK(bstack_slice_read(s, rbuf) == 0);
    uint8_t expect[4] = {2, 4, 6, 8};
    CHECK(memcmp(rbuf, expect, 4) == 0);

    bstack_close(first_fit_bstack_allocator_into_stack(a));
    ff_unlink(tmp); return 0;
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
        first_fit_bstack_allocator_t *a1 = first_fit_bstack_allocator_new(b1); CHECK(a1);
        first_fit_bstack_allocator_t *a2 = first_fit_bstack_allocator_new(b2); CHECK(a2);
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

        /* One foreign slice rejects the whole batch — nothing is freed. */
        {{
            bstack_slice_t batch[2];
            CHECK(bstack_allocator_alloc(g2, 32, &batch[0]) == 0);
            batch[1] = s;
            errno = 0;
            CHECK(bstack_allocator_dealloc_bulk(g2, batch, 2) == -1);
            CHECK(errno == EINVAL);
            CHECK(bstack_allocator_dealloc(g2, batch[0]) == 0);
        }}

        /* Neither allocator's bookkeeping was touched: a2 still round-trips
         * its own allocation, and a1 can still free the original region. */
        CHECK(bstack_allocator_alloc(g2, 64, &own) == 0);
        CHECK(bstack_allocator_dealloc(g2, own) == 0);
        CHECK(bstack_allocator_dealloc(g1, s) == 0);

        bstack_close(first_fit_bstack_allocator_into_stack(a1));
        bstack_close(first_fit_bstack_allocator_into_stack(a2));
    }
    ff_unlink(t1); ff_unlink(t2); return 0;
}

/* =========================================================================
 * main
 * ====================================================================== */

int main(void)
{
    /* Smoke */
    T(test_alloc_small);
    T(test_alloc_write_read);
    T(test_realloc_grow);
    T(test_slot_reuse);
    T(test_persist_reopen);
    T(test_realloc_small);
    T(test_slice_read_range);

    /* Fuzz */
    T(test_fuzz_alloc_dealloc);
    T(test_fuzz_alloc_realloc_dealloc);
    T(test_fuzz_reopen);

#ifdef BSTACK_FEATURE_ATOMIC
    /* Concurrency — only meaningful when the allocator owns an internal
     * mutex (BSTACK_FEATURE_ATOMIC).  Without it, sharing one handle
     * across threads is undefined. */
    T(test_concurrent_alloc_dealloc_data_integrity);
    T(test_concurrent_realloc_grow_shrink_data_integrity);
    T(test_concurrent_tail_thrash);
    T(test_dealloc_non_tail_cascade_reclaims_arena);
    T(test_realloc_copy_move_cascade_reclaims_arena);
    T(test_recovery_needed_already_set_rejects_mutation);

    /* bstack_slice_t cas_on / cas_on_ne / cas_on_masked / process. */
    T(test_slice_cas_on_match_swaps_and_returns_old);
    T(test_slice_cas_on_no_match_leaves_target_untouched);
    T(test_slice_cas_on_expected_length_mismatch_errors);
    T(test_slice_cas_on_new_bytes_length_mismatch_errors);
    T(test_slice_cas_on_cross_stack_errors);
    T(test_slice_cas_on_ne_no_match_swaps_and_returns_old);
    T(test_slice_cas_on_ne_match_returns_none);
    T(test_slice_cas_on_masked_match_swaps_and_returns_old);
    T(test_slice_cas_on_masked_no_match_returns_none);
    T(test_slice_process_transforms_in_place);
#endif

    T(test_foreign_slice_is_rejected);

    printf("\n%d/%d passed\n", g_passed, g_total);
    return (g_passed == g_total) ? 0 : 1;
}
