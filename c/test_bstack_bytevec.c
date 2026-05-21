#ifndef _WIN32
#  define _DARWIN_C_SOURCE
#  define _DEFAULT_SOURCE
#  define _POSIX_C_SOURCE 200809L
#endif

#include "bstack_bytevec.h"

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

int main(void)
{
    T(test_bytevec_push_get_pop);
    printf("\n%d/%d passed\n", g_passed, g_total);
    return (g_passed == g_total) ? 0 : 1;
}
