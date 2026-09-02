#ifndef _WIN32
#  define _DARWIN_C_SOURCE
#  define _DEFAULT_SOURCE
#  define _POSIX_C_SOURCE 200809L
#endif

#include "bstack_bytevec.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#  define WIN32_LEAN_AND_MEAN
#  include <windows.h>
#  include <process.h>
#  define bv_unlink(p) DeleteFileA(p)
#else
#  include <unistd.h>
#  define bv_unlink(p) unlink(p)
#endif

static int g_total = 0;
static int g_passed = 0;

#define CHECK(cond)                                                    \
    do {                                                               \
        if (!(cond)) {                                                 \
            fprintf(stderr, "  FAIL %s:%d  %s\n",                  \
                    __func__, __LINE__, #cond);                        \
            return -1;                                                 \
        }                                                              \
    } while (0)

typedef int (*test_fn)(void);

static void run(const char *name, test_fn fn)
{
    g_total++;
    if (fn() == 0) {
        printf("PASS  %s\n", name);
        g_passed++;
    } else {
        printf("FAIL  %s\n", name);
    }
}

#define T(fn) run(#fn, fn)

#ifdef _WIN32
static void make_tmp(char *buf, size_t n)
{
    static volatile LONG seq = 0;
    LONG s = InterlockedIncrement(&seq);
    snprintf(buf, n, "bstack_bv_%lu_%ld.tmp",
             (unsigned long)GetCurrentProcessId(), (long)s);
    DeleteFileA(buf);
}
#else
static void make_tmp(char *buf, size_t n)
{
    snprintf(buf, n, "/tmp/bstack_bv_XXXXXX");
    int fd = mkstemp(buf);
    if (fd >= 0) { close(fd); unlink(buf); }
}
#endif

/* ── shared test fixtures ──────────────────────────────────────────────────── */

typedef struct {
    char                          tmp[64];
    bstack_t                     *bs;
    first_fit_bstack_allocator_t *ff;
} env_t;

static int env_open(env_t *e)
{
    make_tmp(e->tmp, sizeof e->tmp);
    e->bs = bstack_open(e->tmp);
    if (!e->bs)
        return -1;
    e->ff = first_fit_bstack_allocator_new(e->bs);
    if (!e->ff)
        return -1;
    return 0;
}

static bstack_allocator_t *env_alloc(env_t *e)
{
    return (bstack_allocator_t *)e->ff;
}

static void env_close(env_t *e)
{
    bstack_close(first_fit_bstack_allocator_into_stack(e->ff));
    bv_unlink(e->tmp);
}

/* Compare a vec's logical contents to exp[0..n). Returns 1 on match. */
static int vec_eq(const bstack_bytevec_t *v, const uint8_t *exp, uint64_t n)
{
    uint64_t len = 0, bl = 0;
    uint8_t *buf = NULL;
    int ok;

    if (bstack_bytevec_len(v, &len) != 0 || len != n)
        return 0;
    if (n == 0)
        return 1;
    if (bstack_bytevec_read_bytes(v, &buf, &bl) != 0)
        return 0;
    ok = (bl == n) && (memcmp(buf, exp, (size_t)n) == 0);
    free(buf);
    return ok;
}

static int test_bytevec_push_get_pop(void)
{
    char tmp[64];
    make_tmp(tmp, sizeof tmp);

    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    linear_bstack_allocator_t *lin = linear_bstack_allocator_new(bs);
    CHECK(lin != NULL);

    bstack_bytevec_t v;
    CHECK(bstack_bytevec_new((bstack_allocator_t *)lin, &v) == 0);
    CHECK(bstack_bytevec_push(&v, 0x11) == 0);
    CHECK(bstack_bytevec_push(&v, 0x22) == 0);

    uint64_t len = 0;
    CHECK(bstack_bytevec_len(&v, &len) == 0);
    CHECK(len == 2);

    uint8_t b = 0;
    int found = 0;
    CHECK(bstack_bytevec_get(&v, 1, &b, &found) == 0);
    CHECK(found == 1);
    CHECK(b == 0x22);

    uint8_t *buf = NULL;
    uint64_t out_len = 0;
    CHECK(bstack_bytevec_read_bytes(&v, &buf, &out_len) == 0);
    CHECK(out_len == 2);
    CHECK(buf != NULL);
    CHECK(buf[0] == 0x11 && buf[1] == 0x22);
    free(buf);

    uint8_t popped = 0;
    int popped_ok = 0;
    CHECK(bstack_bytevec_pop(&v, &popped, &popped_ok) == 0);
    CHECK(popped_ok == 1);
    CHECK(popped == 0x22);

    CHECK(bstack_bytevec_dealloc(v) == 0);
    bstack_close(linear_bstack_allocator_into_stack(lin));
    bv_unlink(tmp);
    return 0;
}

/* ── set / fill (BSTACK_FEATURE_SET) ───────────────────────────────────────── */

static int test_bytevec_set(void)
{
    env_t e;
    CHECK(env_open(&e) == 0);

    static const uint8_t data[] = {1, 2, 3, 4};
    bstack_bytevec_t v;
    CHECK(bstack_bytevec_from_data(env_alloc(&e), data, sizeof data, &v) == 0);

    int ok = -1;
    CHECK(bstack_bytevec_set(&v, 2, 0x99, &ok) == 0);
    CHECK(ok == 1);

    static const uint8_t after[] = {1, 2, 0x99, 4};
    CHECK(vec_eq(&v, after, sizeof after));

    /* Out of range: no write, ok == 0, contents unchanged. */
    ok = -1;
    CHECK(bstack_bytevec_set(&v, 4, 0x77, &ok) == 0);
    CHECK(ok == 0);
    CHECK(vec_eq(&v, after, sizeof after));

    CHECK(bstack_bytevec_dealloc(v) == 0);
    env_close(&e);
    return 0;
}

static int test_bytevec_fill(void)
{
    env_t e;
    CHECK(env_open(&e) == 0);

    static const uint8_t data[] = {1, 2, 3, 4, 5};
    bstack_bytevec_t v;
    CHECK(bstack_bytevec_from_data(env_alloc(&e), data, sizeof data, &v) == 0);

    CHECK(bstack_bytevec_fill(&v, 0xAB) == 0);
    static const uint8_t filled[] = {0xAB, 0xAB, 0xAB, 0xAB, 0xAB};
    CHECK(vec_eq(&v, filled, sizeof filled));

    CHECK(bstack_bytevec_dealloc(v) == 0);

    /* Fill of an empty vec is a no-op. */
    bstack_bytevec_t empty;
    CHECK(bstack_bytevec_new(env_alloc(&e), &empty) == 0);
    CHECK(bstack_bytevec_fill(&empty, 0xCD) == 0);
    uint64_t len = 1;
    CHECK(bstack_bytevec_len(&empty, &len) == 0);
    CHECK(len == 0);
    CHECK(bstack_bytevec_dealloc(empty) == 0);

    env_close(&e);
    return 0;
}

#ifdef BSTACK_FEATURE_ATOMIC

/* ── atomic byte-movers (BSTACK_FEATURE_SET + BSTACK_FEATURE_ATOMIC) ────────── */

static int test_bytevec_extend_from_within(void)
{
    env_t e;
    CHECK(env_open(&e) == 0);

    static const uint8_t data[] = {1, 2, 3, 4};
    bstack_bytevec_t v;
    CHECK(bstack_bytevec_from_data(env_alloc(&e), data, sizeof data, &v) == 0);

    int ok = -1;
    CHECK(bstack_bytevec_extend_from_within(&v, 1, 2, &ok) == 0);
    CHECK(ok == 1);
    static const uint8_t after[] = {1, 2, 3, 4, 2, 3};
    CHECK(vec_eq(&v, after, sizeof after));

    /* count == 0 is a successful no-op even with an otherwise-huge start. */
    ok = -1;
    CHECK(bstack_bytevec_extend_from_within(&v, 1000, 0, &ok) == 0);
    CHECK(ok == 1);
    CHECK(vec_eq(&v, after, sizeof after));

    /* Out of range (start + count > len) → ok == 0, unchanged. */
    ok = -1;
    CHECK(bstack_bytevec_extend_from_within(&v, 5, 2, &ok) == 0);
    CHECK(ok == 0);
    CHECK(vec_eq(&v, after, sizeof after));

    CHECK(bstack_bytevec_dealloc(v) == 0);
    env_close(&e);
    return 0;
}

static int test_bytevec_insert(void)
{
    env_t e;
    CHECK(env_open(&e) == 0);

    static const uint8_t data[] = {1, 2, 4};
    bstack_bytevec_t v;
    CHECK(bstack_bytevec_from_data(env_alloc(&e), data, sizeof data, &v) == 0);

    int ok = -1;
    CHECK(bstack_bytevec_insert(&v, 2, 3, &ok) == 0); /* insert into middle */
    CHECK(ok == 1);
    static const uint8_t mid[] = {1, 2, 3, 4};
    CHECK(vec_eq(&v, mid, sizeof mid));

    CHECK(bstack_bytevec_insert(&v, 4, 5, &ok) == 0); /* insert at end (== len) */
    CHECK(ok == 1);
    static const uint8_t end[] = {1, 2, 3, 4, 5};
    CHECK(vec_eq(&v, end, sizeof end));

    ok = -1;
    CHECK(bstack_bytevec_insert(&v, 6, 9, &ok) == 0); /* index > len → no-op */
    CHECK(ok == 0);
    CHECK(vec_eq(&v, end, sizeof end));

    CHECK(bstack_bytevec_dealloc(v) == 0);
    env_close(&e);
    return 0;
}

static int test_bytevec_remove(void)
{
    env_t e;
    CHECK(env_open(&e) == 0);

    static const uint8_t data[] = {1, 2, 3, 4};
    bstack_bytevec_t v;
    CHECK(bstack_bytevec_from_data(env_alloc(&e), data, sizeof data, &v) == 0);

    uint8_t got = 0;
    int ok = -1;
    CHECK(bstack_bytevec_remove(&v, 1, &got, &ok) == 0); /* remove middle */
    CHECK(ok == 1);
    CHECK(got == 2);
    static const uint8_t after[] = {1, 3, 4};
    CHECK(vec_eq(&v, after, sizeof after));

    ok = -1;
    CHECK(bstack_bytevec_remove(&v, 3, &got, &ok) == 0); /* index >= len → no-op */
    CHECK(ok == 0);
    CHECK(vec_eq(&v, after, sizeof after));

    CHECK(bstack_bytevec_dealloc(v) == 0);
    env_close(&e);
    return 0;
}

static int test_bytevec_swap_remove(void)
{
    env_t e;
    CHECK(env_open(&e) == 0);

    static const uint8_t data[] = {1, 2, 3, 4};
    bstack_bytevec_t v;
    CHECK(bstack_bytevec_from_data(env_alloc(&e), data, sizeof data, &v) == 0);

    uint8_t got = 0;
    int ok = -1;
    CHECK(bstack_bytevec_swap_remove(&v, 1, &got, &ok) == 0); /* hole filled by last */
    CHECK(ok == 1);
    CHECK(got == 2);
    static const uint8_t after[] = {1, 4, 3};
    CHECK(vec_eq(&v, after, sizeof after));

    /* Removing the last element is the degenerate (index == last) case. */
    CHECK(bstack_bytevec_swap_remove(&v, 2, &got, &ok) == 0);
    CHECK(ok == 1);
    CHECK(got == 3);
    static const uint8_t after2[] = {1, 4};
    CHECK(vec_eq(&v, after2, sizeof after2));

    ok = -1;
    CHECK(bstack_bytevec_swap_remove(&v, 5, &got, &ok) == 0);
    CHECK(ok == 0);
    CHECK(vec_eq(&v, after2, sizeof after2));

    CHECK(bstack_bytevec_dealloc(v) == 0);
    env_close(&e);
    return 0;
}

static int test_bytevec_extend_from_bstack_slice(void)
{
    env_t e;
    CHECK(env_open(&e) == 0);

    static const uint8_t data[] = {1, 2};
    bstack_bytevec_t v;
    CHECK(bstack_bytevec_from_data(env_alloc(&e), data, sizeof data, &v) == 0);

    /* A raw block on the same BStack, holding {3, 4, 5}. */
    static const uint8_t src_bytes[] = {3, 4, 5};
    bstack_slice_t src;
    CHECK(bstack_allocator_alloc(env_alloc(&e), sizeof src_bytes, &src) == 0);
    CHECK(bstack_slice_write_range(src, 0, src_bytes, sizeof src_bytes) == 0);

    CHECK(bstack_bytevec_extend_from_bstack_slice(&v, src) == 0);
    static const uint8_t after[] = {1, 2, 3, 4, 5};
    CHECK(vec_eq(&v, after, sizeof after));
    CHECK(bstack_allocator_dealloc(env_alloc(&e), src) == 0);

    /* Cross-BStack misuse → -1 / EINVAL. */
    env_t e2;
    CHECK(env_open(&e2) == 0);
    bstack_slice_t foreign;
    CHECK(bstack_allocator_alloc(env_alloc(&e2), 2, &foreign) == 0);
    errno = 0;
    CHECK(bstack_bytevec_extend_from_bstack_slice(&v, foreign) == -1);
    CHECK(errno == EINVAL);
    CHECK(vec_eq(&v, after, sizeof after)); /* unchanged */
    CHECK(bstack_allocator_dealloc(env_alloc(&e2), foreign) == 0);
    env_close(&e2);

    CHECK(bstack_bytevec_dealloc(v) == 0);
    env_close(&e);
    return 0;
}

static int test_bytevec_copy_into_bstack_slice(void)
{
    env_t e;
    CHECK(env_open(&e) == 0);

    static const uint8_t data[] = {1, 2, 3, 4, 5};
    bstack_bytevec_t v;
    CHECK(bstack_bytevec_from_data(env_alloc(&e), data, sizeof data, &v) == 0);

    bstack_slice_t dst;
    CHECK(bstack_allocator_alloc(env_alloc(&e), 3, &dst) == 0);

    int ok = -1;
    CHECK(bstack_bytevec_copy_into_bstack_slice(&v, 1, dst, &ok) == 0);
    CHECK(ok == 1);
    uint8_t got[3] = {0};
    CHECK(bstack_slice_read_range(dst, 0, 3, got) == 0);
    CHECK(got[0] == 2 && got[1] == 3 && got[2] == 4);

    /* Out of range (start + dst.len > len) → ok == 0. */
    ok = -1;
    CHECK(bstack_bytevec_copy_into_bstack_slice(&v, 3, dst, &ok) == 0);
    CHECK(ok == 0);

    /* Cross-BStack misuse → -1 / EINVAL. */
    env_t e2;
    CHECK(env_open(&e2) == 0);
    bstack_slice_t foreign;
    CHECK(bstack_allocator_alloc(env_alloc(&e2), 3, &foreign) == 0);
    errno = 0;
    CHECK(bstack_bytevec_copy_into_bstack_slice(&v, 0, foreign, &ok) == -1);
    CHECK(errno == EINVAL);
    CHECK(bstack_allocator_dealloc(env_alloc(&e2), foreign) == 0);
    env_close(&e2);

    CHECK(bstack_allocator_dealloc(env_alloc(&e), dst) == 0);
    CHECK(bstack_bytevec_dealloc(v) == 0);
    env_close(&e);
    return 0;
}

static int test_bytevec_append_from_owned(void)
{
    env_t e;
    CHECK(env_open(&e) == 0);

    static const uint8_t data[] = {1, 2};
    bstack_bytevec_t v;
    CHECK(bstack_bytevec_from_data(env_alloc(&e), data, sizeof data, &v) == 0);

    static const uint8_t owned_bytes[] = {3, 4, 5};
    bstack_slice_t owned;
    CHECK(bstack_allocator_alloc(env_alloc(&e), sizeof owned_bytes, &owned) == 0);
    CHECK(bstack_slice_write_range(owned, 0, owned_bytes, sizeof owned_bytes) == 0);

    /* Consumes and frees `owned`; must not dealloc it afterwards. */
    CHECK(bstack_bytevec_append_from_owned(&v, owned) == 0);
    static const uint8_t after[] = {1, 2, 3, 4, 5};
    CHECK(vec_eq(&v, after, sizeof after));

    /* Cross-BStack misuse → -1 / EINVAL, and `foreign` is still consumed
     * (freed through its own allocator), so no double free below. */
    env_t e2;
    CHECK(env_open(&e2) == 0);
    bstack_slice_t foreign;
    CHECK(bstack_allocator_alloc(env_alloc(&e2), 2, &foreign) == 0);
    errno = 0;
    CHECK(bstack_bytevec_append_from_owned(&v, foreign) == -1);
    CHECK(errno == EINVAL);
    CHECK(vec_eq(&v, after, sizeof after)); /* unchanged */
    env_close(&e2);

    CHECK(bstack_bytevec_dealloc(v) == 0);
    env_close(&e);
    return 0;
}

static int test_bytevec_move_tail_into(void)
{
    env_t e;
    CHECK(env_open(&e) == 0);

    static const uint8_t data[] = {1, 2, 3, 4, 5};
    bstack_bytevec_t v;
    CHECK(bstack_bytevec_from_data(env_alloc(&e), data, sizeof data, &v) == 0);

    bstack_slice_t dest;
    CHECK(bstack_allocator_alloc(env_alloc(&e), 2, &dest) == 0);

    int ok = -1;
    CHECK(bstack_bytevec_move_tail_into(&v, dest, &ok) == 0);
    CHECK(ok == 1);
    static const uint8_t after[] = {1, 2, 3};
    CHECK(vec_eq(&v, after, sizeof after));
    uint8_t moved[2] = {0};
    CHECK(bstack_slice_read_range(dest, 0, 2, moved) == 0);
    CHECK(moved[0] == 4 && moved[1] == 5);
    CHECK(bstack_allocator_dealloc(env_alloc(&e), dest) == 0);

    /* dest.len > len → ok == 0, unchanged. */
    bstack_slice_t big;
    CHECK(bstack_allocator_alloc(env_alloc(&e), 4, &big) == 0);
    ok = -1;
    CHECK(bstack_bytevec_move_tail_into(&v, big, &ok) == 0);
    CHECK(ok == 0);
    CHECK(vec_eq(&v, after, sizeof after));
    CHECK(bstack_allocator_dealloc(env_alloc(&e), big) == 0);

    /* Cross-BStack misuse → -1 / EINVAL. */
    env_t e2;
    CHECK(env_open(&e2) == 0);
    bstack_slice_t foreign;
    CHECK(bstack_allocator_alloc(env_alloc(&e2), 1, &foreign) == 0);
    errno = 0;
    CHECK(bstack_bytevec_move_tail_into(&v, foreign, &ok) == -1);
    CHECK(errno == EINVAL);
    CHECK(bstack_allocator_dealloc(env_alloc(&e2), foreign) == 0);
    env_close(&e2);

    CHECK(bstack_bytevec_dealloc(v) == 0);
    env_close(&e);
    return 0;
}

static int test_bytevec_split_off(void)
{
    env_t e;
    CHECK(env_open(&e) == 0);

    static const uint8_t data[] = {1, 2, 3, 4, 5};
    bstack_bytevec_t v;
    CHECK(bstack_bytevec_from_data(env_alloc(&e), data, sizeof data, &v) == 0);

    bstack_bytevec_t tail;
    int ok = -1;
    CHECK(bstack_bytevec_split_off(&v, 2, &tail, &ok) == 0);
    CHECK(ok == 1);
    static const uint8_t head[] = {1, 2};
    static const uint8_t tbytes[] = {3, 4, 5};
    CHECK(vec_eq(&v, head, sizeof head));
    CHECK(vec_eq(&tail, tbytes, sizeof tbytes));
    CHECK(bstack_bytevec_dealloc(tail) == 0);

    /* at == len yields an empty tail. */
    bstack_bytevec_t empty_tail;
    CHECK(bstack_bytevec_split_off(&v, 2, &empty_tail, &ok) == 0);
    CHECK(ok == 1);
    uint64_t tlen = 1;
    CHECK(bstack_bytevec_len(&empty_tail, &tlen) == 0);
    CHECK(tlen == 0);
    CHECK(bstack_bytevec_dealloc(empty_tail) == 0);

    /* at > len → ok == 0, `out` untouched, `v` unchanged. */
    bstack_bytevec_t untouched;
    memset(&untouched, 0xEE, sizeof untouched);
    ok = -1;
    CHECK(bstack_bytevec_split_off(&v, 3, &untouched, &ok) == 0);
    CHECK(ok == 0);
    CHECK(vec_eq(&v, head, sizeof head));

    CHECK(bstack_bytevec_dealloc(v) == 0);
    env_close(&e);
    return 0;
}

static int test_bytevec_drain(void)
{
    env_t e;
    CHECK(env_open(&e) == 0);

    static const uint8_t data[] = {1, 2, 3, 4, 5};
    bstack_bytevec_t v;
    CHECK(bstack_bytevec_from_data(env_alloc(&e), data, sizeof data, &v) == 0);

    uint8_t  *buf = NULL;
    uint64_t  n   = 0;
    int       ok  = -1;
    CHECK(bstack_bytevec_drain(&v, 1, 4, &buf, &n, &ok) == 0);
    CHECK(ok == 1);
    CHECK(n == 3);
    CHECK(buf != NULL);
    CHECK(buf[0] == 2 && buf[1] == 3 && buf[2] == 4);
    free(buf);
    static const uint8_t after[] = {1, 5};
    CHECK(vec_eq(&v, after, sizeof after));

    /* Empty range → success, NULL buffer, len 0. */
    buf = (uint8_t *)0x1;
    n = 99;
    ok = -1;
    CHECK(bstack_bytevec_drain(&v, 1, 1, &buf, &n, &ok) == 0);
    CHECK(ok == 1);
    CHECK(buf == NULL);
    CHECK(n == 0);
    CHECK(vec_eq(&v, after, sizeof after));

    /* Out of range (end > len) → ok == 0, unchanged. */
    ok = -1;
    CHECK(bstack_bytevec_drain(&v, 0, 3, &buf, &n, &ok) == 0);
    CHECK(ok == 0);
    CHECK(vec_eq(&v, after, sizeof after));

    /* Inverted range (start > end) → ok == 0. */
    ok = -1;
    CHECK(bstack_bytevec_drain(&v, 2, 1, &buf, &n, &ok) == 0);
    CHECK(ok == 0);

    CHECK(bstack_bytevec_dealloc(v) == 0);
    env_close(&e);
    return 0;
}

#endif /* BSTACK_FEATURE_ATOMIC */

int main(void)
{
    T(test_bytevec_push_get_pop);
    T(test_bytevec_set);
    T(test_bytevec_fill);
#ifdef BSTACK_FEATURE_ATOMIC
    T(test_bytevec_extend_from_within);
    T(test_bytevec_insert);
    T(test_bytevec_remove);
    T(test_bytevec_swap_remove);
    T(test_bytevec_extend_from_bstack_slice);
    T(test_bytevec_copy_into_bstack_slice);
    T(test_bytevec_append_from_owned);
    T(test_bytevec_move_tail_into);
    T(test_bytevec_split_off);
    T(test_bytevec_drain);
#endif
    printf("\n%d/%d passed\n", g_passed, g_total);
    return (g_passed == g_total) ? 0 : 1;
}
