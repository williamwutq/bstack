/* Mirror of the Rust bstack test suite, adapted for the C API. */

#ifndef _WIN32
#  define _DARWIN_C_SOURCE
#  define _DEFAULT_SOURCE
#  define _POSIX_C_SOURCE 200809L
#  define _XOPEN_SOURCE 700
#endif

#include "bstack.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef _WIN32
#  define WIN32_LEAN_AND_MEAN
#  include <windows.h>
#endif

/* =========================================================================
 * Harness
 * ====================================================================== */

static int g_total  = 0;
static int g_passed = 0;

/* Returns -1 immediately from the enclosing test function on failure. */
#define CHECK(cond)                                                  \
    do {                                                             \
        if (!(cond)) {                                               \
            fprintf(stderr, "  FAIL %s:%d  %s\n",                   \
                    __func__, __LINE__, #cond);                      \
            return -1;                                               \
        }                                                            \
    } while (0)

typedef int (*test_fn)(void);

static void run(const char *name, test_fn fn)
{
    g_total++;
    int r = fn();
    if (r == 0) {
        printf("PASS  %s\n", name);
        g_passed++;
    } else {
        printf("FAIL  %s\n", name);
    }
}

#define T(fn) run(#fn, fn)

/* Create a unique temp path and remove any pre-existing file at it so
 * bstack_open starts with a fresh empty file. */
#ifdef _WIN32
static void make_tmp(char *buf, size_t n)
{
    /* Use short relative names so paths fit in the char[64] test buffers. */
    static volatile LONG seq = 0;
    LONG s = InterlockedIncrement(&seq);
    snprintf(buf, n, "bst_%lu_%ld.tmp",
             (unsigned long)GetCurrentProcessId(), (long)s);
    DeleteFileA(buf); /* ensure clean start */
}
#else
static void make_tmp(char *buf, size_t n)
{
    snprintf(buf, n, "/tmp/bstack_test_XXXXXX");
    int fd = mkstemp(buf);
    if (fd >= 0) { close(fd); unlink(buf); }
}
#endif

#ifdef _WIN32
static ssize_t pread(int fd, void *buf, size_t count, off_t offset) {
    off_t saved = lseek(fd, 0, SEEK_CUR);
    if (saved == -1) return -1;
    if (lseek(fd, offset, SEEK_SET) == -1) return -1;
    ssize_t result = read(fd, buf, count);
    lseek(fd, saved, SEEK_SET);
    return result;
}
#endif

/* Read 8-byte little-endian value from absolute file offset (raw fd). */
static uint64_t raw_read_le64(int fd, off_t offset)
{
    uint8_t b[8];
    if (pread(fd, b, 8, offset) != 8) return (uint64_t)-1;
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v |= (uint64_t)b[i] << (8 * i);
    return v;
}

/* =========================================================================
 * Functional tests
 * ====================================================================== */

static int test_push_returns_correct_offsets(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    uint64_t off0, off1, off2;
    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, &off0) == 0);
    CHECK(bstack_push(bs, (uint8_t *)"world", 5, &off1) == 0);
    CHECK(bstack_push(bs, (uint8_t *)"!",     1, &off2) == 0);

    CHECK(off0 == 0);
    CHECK(off1 == 5);
    CHECK(off2 == 10);

    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 11);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_pop_returns_correct_bytes_and_shrinks(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    CHECK(bstack_push(bs, (uint8_t *)"world", 5, NULL) == 0);

    uint8_t buf[5]; size_t written;
    CHECK(bstack_pop(bs, 5, buf, &written) == 0);
    CHECK(written == 5);
    CHECK(memcmp(buf, "world", 5) == 0);

    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 5);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_pop_across_push_boundary(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    CHECK(bstack_push(bs, (uint8_t *)"world", 5, NULL) == 0);

    uint8_t buf[7]; size_t written;
    CHECK(bstack_pop(bs, 7, buf, &written) == 0);
    CHECK(written == 7);
    CHECK(memcmp(buf, "loworld", 7) == 0);    /* last 7 of "helloworld" */

    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 3);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_pop_on_empty_file_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    uint8_t buf[1];
    int r = bstack_pop(bs, 1, buf, NULL);
    CHECK(r == -1);
    CHECK(errno == EINVAL);

    /* File must still be empty after the failed pop. */
    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_pop_n_exceeds_size_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"abc", 3, NULL) == 0);

    uint8_t buf[4];
    int r = bstack_pop(bs, 4, buf, NULL);
    CHECK(r == -1);
    CHECK(errno == EINVAL);

    /* File must be unchanged. */
    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 3);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_peek_reads_from_offset_to_end(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    CHECK(bstack_push(bs, (uint8_t *)"world", 5, NULL) == 0);

    uint8_t buf[10]; size_t w;

    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(w == 10);
    CHECK(memcmp(buf, "helloworld", 10) == 0);

    CHECK(bstack_peek(bs, 5, buf, &w) == 0);
    CHECK(w == 5);
    CHECK(memcmp(buf, "world", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_peek_offset_exceeds_size_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hi", 2, NULL) == 0);

    uint8_t buf[4];
    int r = bstack_peek(bs, 3, buf, NULL);
    CHECK(r == -1);
    CHECK(errno == EINVAL);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_get_reads_half_open_range(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    CHECK(bstack_push(bs, (uint8_t *)"world", 5, NULL) == 0);

    uint8_t buf[5];
    CHECK(bstack_get(bs, 3, 8, buf) == 0);
    CHECK(memcmp(buf, "lowor", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

/* =========================================================================
 * Range validation
 * ====================================================================== */

static int test_get_end_exceeds_size_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"abc", 3, NULL) == 0);

    uint8_t buf[4];
    int r = bstack_get(bs, 0, 4, buf);
    CHECK(r == -1);
    CHECK(errno == EINVAL);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_get_end_less_than_start_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"abc", 3, NULL) == 0);

    uint8_t buf[1];
    int r = bstack_get(bs, 2, 1, buf);
    CHECK(r == -1);
    CHECK(errno == EINVAL);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_get_does_not_modify_file(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);

    uint8_t buf[3];
    CHECK(bstack_get(bs, 1, 4, buf) == 0);

    /* Next push must start at offset 5, not somewhere corrupted. */
    uint64_t off;
    CHECK(bstack_push(bs, (uint8_t *)"!", 1, &off) == 0);
    CHECK(off == 5);

    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 6);

    bstack_close(bs); unlink(tmp);
    return 0;
}

/* =========================================================================
 * Persistence
 * ====================================================================== */

static int test_reopen_reads_back_correct_data(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"persist", 7, NULL) == 0);
        bstack_close(bs);
    }

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);

        uint64_t len;
        CHECK(bstack_len(bs, &len) == 0);
        CHECK(len == 7);

        uint8_t buf[7]; size_t w;
        CHECK(bstack_peek(bs, 0, buf, &w) == 0);
        CHECK(w == 7);
        CHECK(memcmp(buf, "persist", 7) == 0);

        bstack_close(bs);
    }

    unlink(tmp);
    return 0;
}

static int test_reopen_and_continue_pushing(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
        bstack_close(bs);
    }

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);

        uint64_t off;
        CHECK(bstack_push(bs, (uint8_t *)"world", 5, &off) == 0);
        CHECK(off == 5);

        uint8_t buf[10]; size_t w;
        CHECK(bstack_peek(bs, 0, buf, &w) == 0);
        CHECK(w == 10);
        CHECK(memcmp(buf, "helloworld", 10) == 0);

        bstack_close(bs);
    }

    unlink(tmp);
    return 0;
}

static int test_reopen_after_pop_sees_truncated_file(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
        uint8_t buf[5];
        CHECK(bstack_pop(bs, 5, buf, NULL) == 0);
        bstack_close(bs);
    }

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);

        uint64_t len;
        CHECK(bstack_len(bs, &len) == 0);
        CHECK(len == 5);

        bstack_close(bs);
    }

    unlink(tmp);
    return 0;
}

/* =========================================================================
 * Boundary / zero-value handling
 * ====================================================================== */

static int test_push_empty_slice(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"abc", 3, NULL) == 0);

    uint64_t off;
    CHECK(bstack_push(bs, (uint8_t *)"", 0, &off) == 0);
    CHECK(off == 3);  /* returns current end, not a new slot */

    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 3);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_pop_zero_bytes(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"abc", 3, NULL) == 0);

    uint8_t buf[1]; size_t w = 99;
    CHECK(bstack_pop(bs, 0, buf, &w) == 0);
    CHECK(w == 0);

    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 3);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_peek_at_end_offset_on_empty_file(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    uint8_t buf[1]; size_t w = 99;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(w == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_get_zero_range_on_empty_file(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    uint8_t buf[1];
    CHECK(bstack_get(bs, 0, 0, buf) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_drain_to_zero_then_push_starts_at_offset_zero(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    uint8_t buf[5];
    CHECK(bstack_pop(bs, 5, buf, NULL) == 0);

    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 0);

    uint64_t off;
    CHECK(bstack_push(bs, (uint8_t *)"new", 3, &off) == 0);
    CHECK(off == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

/* =========================================================================
 * Data integrity
 * ====================================================================== */

static int test_peek_does_not_modify_file(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    uint8_t buf[5];
    CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(bstack_peek(bs, 0, buf, NULL) == 0);

    /* Push must still go to offset 5. */
    uint64_t off;
    CHECK(bstack_push(bs, (uint8_t *)"!", 1, &off) == 0);
    CHECK(off == 5);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_binary_roundtrip_all_byte_values(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    uint8_t all[256];
    for (int i = 0; i < 256; i++) all[i] = (uint8_t)i;

    CHECK(bstack_push(bs, all, 256, NULL) == 0);

    uint8_t out[256]; size_t w;
    CHECK(bstack_pop(bs, 256, out, &w) == 0);
    CHECK(w == 256);
    CHECK(memcmp(out, all, 256) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_large_payload_roundtrip(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    const size_t MiB = 1024 * 1024;
    uint8_t *big = malloc(MiB);
    CHECK(big != NULL);
    for (size_t i = 0; i < MiB; i++) big[i] = (uint8_t)(i & 0xFF);

    CHECK(bstack_push(bs, big, MiB, NULL) == 0);

    uint8_t *out = malloc(MiB);
    CHECK(out != NULL);
    CHECK(bstack_get(bs, 0, MiB, out) == 0);
    CHECK(memcmp(out, big, MiB) == 0);

    free(big); free(out);
    bstack_close(bs); unlink(tmp);
    return 0;
}

/* =========================================================================
 * Header / magic
 * ====================================================================== */

static const uint8_t MAGIC[8]        = {'B','S','T','K', 0, 1, 4, 0};
static const uint8_t MAGIC_PREFIX[6] = {'B','S','T','K', 0, 1};

static int test_new_file_has_valid_header(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    bstack_close(bs);

    int fd = open(tmp, O_RDONLY);
    CHECK(fd >= 0);

    struct stat st; fstat(fd, &st);
    CHECK(st.st_size == 16);

    uint8_t hdr[16];
    CHECK(pread(fd, hdr, 16, 0) == 16);
    CHECK(memcmp(hdr,     MAGIC,        8) == 0);
    CHECK(memcmp(hdr + 8, "\0\0\0\0\0\0\0\0", 8) == 0);

    close(fd); unlink(tmp);
    return 0;
}

static int test_header_clen_matches_after_pushes(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    CHECK(bstack_push(bs, (uint8_t *)"world", 5, NULL) == 0);
    bstack_close(bs);

    int fd = open(tmp, O_RDONLY);
    CHECK(fd >= 0);
    CHECK(raw_read_le64(fd, 8) == 10);
    close(fd); unlink(tmp);
    return 0;
}

static int test_header_clen_matches_after_pop(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    uint8_t buf[4];
    CHECK(bstack_pop(bs, 4, buf, NULL) == 0);
    bstack_close(bs);

    int fd = open(tmp, O_RDONLY);
    CHECK(fd >= 0);
    CHECK(raw_read_le64(fd, 8) == 6);
    close(fd); unlink(tmp);
    return 0;
}

static int test_open_rejects_bad_magic(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    CHECK(fd >= 0);
    uint8_t garbage[16];
    memcpy(garbage, "GARBAGE!", 8);
    memset(garbage + 8, 0, 8);
    CHECK(write(fd, garbage, 16) == 16);
    close(fd);

    bstack_t *bs = bstack_open(tmp);
    CHECK(bs == NULL);
    CHECK(errno == EINVAL);

    unlink(tmp);
    return 0;
}

static int test_open_rejects_truncated_header(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    CHECK(fd >= 0);
    /* Write only 6 bytes — valid prefix but far too short to be a header. */
    CHECK(write(fd, MAGIC_PREFIX, 6) == 6);
    close(fd);

    bstack_t *bs = bstack_open(tmp);
    CHECK(bs == NULL);
    CHECK(errno == EINVAL);

    unlink(tmp);
    return 0;
}

/* =========================================================================
 * Crash recovery
 * ====================================================================== */

static int test_recovery_truncates_partial_push(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    /* Commit "hello" (clen == 5, file == 16+5 == 21 bytes). */
    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
        bstack_close(bs);
    }

    /* Simulate a push that wrote 3 extra bytes but crashed before updating
     * the committed-length field in the header. */
    {
        int fd = open(tmp, O_WRONLY);
        CHECK(fd >= 0);
        CHECK(ftruncate(fd, 16 + 5 + 3) == 0);
        /* clen at offset 8 still says 5 — do NOT update it. */
        close(fd);
    }

    /* Reopen: recovery must truncate back to 16+5. */
    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);

        uint64_t len;
        CHECK(bstack_len(bs, &len) == 0);
        CHECK(len == 5);

        uint8_t buf[5]; size_t w;
        CHECK(bstack_pop(bs, 5, buf, &w) == 0);
        CHECK(memcmp(buf, "hello", 5) == 0);

        bstack_close(bs);
    }

    unlink(tmp);
    return 0;
}

static int test_recovery_repairs_header_after_partial_pop(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    /* Commit "helloworld" (clen == 10, file == 26 bytes). */
    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
        bstack_close(bs);
    }

    /* Simulate a pop that truncated the file to 16+5 but crashed before
     * writing the new committed length to the header. */
    {
        int fd = open(tmp, O_WRONLY);
        CHECK(fd >= 0);
        CHECK(ftruncate(fd, 16 + 5) == 0);
        /* clen at offset 8 still says 10 — do NOT update it. */
        close(fd);
    }

    /* Reopen: recovery must set clen = actual == 5. */
    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);

        uint64_t len;
        CHECK(bstack_len(bs, &len) == 0);
        CHECK(len == 5);

        uint8_t buf[5]; size_t w;
        CHECK(bstack_pop(bs, 5, buf, &w) == 0);
        CHECK(memcmp(buf, "hello", 5) == 0);

        bstack_close(bs);
    }

    unlink(tmp);
    return 0;
}

/* =========================================================================
 * Concurrency
 * ====================================================================== */

/* --- concurrent reads -------------------------------------------------- */

#define READ_THREADS 32
#define READ_ITERS   50

typedef struct {
    bstack_t      *bs;
    const uint8_t *expected; /* 64 bytes */
    int            ok;
} reader_arg_t;

static void *concurrent_reader(void *raw)
{
    reader_arg_t *a = raw;
    a->ok = 1;
    uint8_t buf[64];

    for (int i = 0; i < READ_ITERS; i++) {
        size_t w;
        if (bstack_peek(a->bs, 0, buf, &w) != 0 || w != 64 ||
            memcmp(buf, a->expected, 64) != 0) {
            a->ok = 0; return NULL;
        }
        if (bstack_get(a->bs, 8, 16, buf) != 0 ||
            memcmp(buf, a->expected + 8, 8) != 0) {
            a->ok = 0; return NULL;
        }
    }
    return NULL;
}

static int test_concurrent_reads_do_not_serialise(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    uint8_t expected[64];
    for (int i = 0; i < 8; i++) {
        uint8_t rec[8];
        for (int j = 0; j < 8; j++) rec[j] = (uint8_t)(i * 8 + j);
        memcpy(expected + i * 8, rec, 8);
        CHECK(bstack_push(bs, rec, 8, NULL) == 0);
    }

    pthread_t threads[READ_THREADS];
    reader_arg_t args[READ_THREADS];
    for (int i = 0; i < READ_THREADS; i++) {
        args[i] = (reader_arg_t){ .bs = bs, .expected = expected, .ok = 1 };
        pthread_create(&threads[i], NULL, concurrent_reader, &args[i]);
    }
    for (int i = 0; i < READ_THREADS; i++) pthread_join(threads[i], NULL);
    for (int i = 0; i < READ_THREADS; i++) CHECK(args[i].ok);

    bstack_close(bs); unlink(tmp);
    return 0;
}

/* --- concurrent pushes ------------------------------------------------- */

#define PUSH_THREADS 8
#define PUSH_COUNT   100
#define RECORD_SIZE  16

typedef struct {
    bstack_t *bs;
    int       id;
    uint64_t  offsets[PUSH_COUNT];
} push_arg_t;

static void *push_worker(void *raw)
{
    push_arg_t *a = raw;
    for (int i = 0; i < PUSH_COUNT; i++) {
        uint8_t rec[RECORD_SIZE];
        memset(rec, 0, RECORD_SIZE);
        rec[0] = (uint8_t)a->id;
        rec[1] = (uint8_t)i;
        if (bstack_push(a->bs, rec, RECORD_SIZE, &a->offsets[i]) != 0)
            a->offsets[i] = (uint64_t)-1;
    }
    return NULL;
}

static int test_concurrent_pushes_non_overlapping(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    pthread_t  threads[PUSH_THREADS];
    push_arg_t args[PUSH_THREADS];
    for (int i = 0; i < PUSH_THREADS; i++) {
        args[i].bs = bs;
        args[i].id = i;
        pthread_create(&threads[i], NULL, push_worker, &args[i]);
    }
    for (int i = 0; i < PUSH_THREADS; i++) pthread_join(threads[i], NULL);

    /* Total length must be exact. */
    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == (uint64_t)(PUSH_THREADS * PUSH_COUNT * RECORD_SIZE));

    /* Every offset must be a valid record boundary and unique. */
    for (int i = 0; i < PUSH_THREADS; i++) {
        for (int j = 0; j < PUSH_COUNT; j++) {
            uint64_t off = args[i].offsets[j];
            CHECK(off != (uint64_t)-1);
            CHECK(off % RECORD_SIZE == 0);
            /* Uniqueness: compare against all later (i,j) pairs. */
            for (int k = i; k < PUSH_THREADS; k++) {
                int sl = (k == i) ? j + 1 : 0;
                for (int l = sl; l < PUSH_COUNT; l++)
                    CHECK(off != args[k].offsets[l]);
            }
        }
    }

    /* Data integrity: each record must contain the right thread/index. */
    for (int i = 0; i < PUSH_THREADS; i++) {
        for (int j = 0; j < PUSH_COUNT; j++) {
            uint64_t off = args[i].offsets[j];
            uint8_t rec[RECORD_SIZE];
            CHECK(bstack_get(bs, off, off + RECORD_SIZE, rec) == 0);
            CHECK(rec[0] == (uint8_t)args[i].id);
            CHECK(rec[1] == (uint8_t)j);
        }
    }

    bstack_close(bs); unlink(tmp);
    return 0;
}

/* --- concurrent len is a multiple of item size ------------------------- */

#define LEN_PUSH_THREADS 4
#define LEN_PUSH_COUNT   200
#define LEN_ITEM_SIZE    8
#define LEN_READ_COUNT   2000

typedef struct {
    bstack_t *bs;
    int       ok;
} len_reader_arg_t;

static void *len_reader(void *raw)
{
    len_reader_arg_t *a = raw;
    a->ok = 1;
    for (int i = 0; i < LEN_READ_COUNT; i++) {
        uint64_t l;
        if (bstack_len(a->bs, &l) != 0 || l % LEN_ITEM_SIZE != 0) {
            a->ok = 0; return NULL;
        }
    }
    return NULL;
}

static void *len_pusher(void *raw)
{
    bstack_t *bs = raw;
    uint8_t item[LEN_ITEM_SIZE];
    memset(item, 0xAB, LEN_ITEM_SIZE);
    for (int i = 0; i < LEN_PUSH_COUNT; i++)
        bstack_push(bs, item, LEN_ITEM_SIZE, NULL);
    return NULL;
}

static int test_concurrent_len_is_multiple_of_item_size(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    pthread_t push_threads[LEN_PUSH_THREADS];
    pthread_t reader;
    len_reader_arg_t rarg = { .bs = bs, .ok = 1 };

    pthread_create(&reader, NULL, len_reader, &rarg);
    for (int i = 0; i < LEN_PUSH_THREADS; i++)
        pthread_create(&push_threads[i], NULL, len_pusher, bs);

    for (int i = 0; i < LEN_PUSH_THREADS; i++)
        pthread_join(push_threads[i], NULL);
    pthread_join(reader, NULL);

    CHECK(rarg.ok);

    uint64_t final_len;
    CHECK(bstack_len(bs, &final_len) == 0);
    CHECK(final_len == (uint64_t)(LEN_PUSH_THREADS * LEN_PUSH_COUNT * LEN_ITEM_SIZE));

    bstack_close(bs); unlink(tmp);
    return 0;
}

/* =========================================================================
 * Interleaved push / pop
 * ====================================================================== */

static int test_interleaved_push_pop_correct_state(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    uint64_t off0, off1, off2;
    CHECK(bstack_push(bs, (uint8_t *)"AAAA", 4, &off0) == 0);  CHECK(off0 == 0);
    CHECK(bstack_push(bs, (uint8_t *)"BBBB", 4, &off1) == 0);  CHECK(off1 == 4);

    uint8_t pop1[4]; size_t w;
    CHECK(bstack_pop(bs, 4, pop1, &w) == 0);
    CHECK(w == 4);
    CHECK(memcmp(pop1, "BBBB", 4) == 0);

    CHECK(bstack_push(bs, (uint8_t *)"CCCC", 4, &off2) == 0);  CHECK(off2 == 4);

    uint8_t pop2[8];
    CHECK(bstack_pop(bs, 8, pop2, &w) == 0);
    CHECK(w == 8);
    CHECK(memcmp(pop2, "AAAACCCC", 8) == 0);

    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

/* =========================================================================
 * bstack_discard
 * ====================================================================== */

static int test_discard_removes_bytes_from_tail(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"abcde", 5, NULL) == 0);
    CHECK(bstack_push(bs, (uint8_t *)"fghij", 5, NULL) == 0);

    CHECK(bstack_discard(bs, 5) == 0);

    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 5);

    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(w == 5);
    CHECK(memcmp(buf, "abcde", 5) == 0);

    CHECK(bstack_discard(bs, 5) == 0);
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_discard_zero_is_noop(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"abc", 3, NULL) == 0);
    CHECK(bstack_discard(bs, 0) == 0);

    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 3);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_discard_exceeds_size_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"abc", 3, NULL) == 0);

    int r = bstack_discard(bs, 10);
    CHECK(r == -1);
    CHECK(errno == EINVAL);

    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 3);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_discard_on_empty_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    int r = bstack_discard(bs, 1);
    CHECK(r == -1);
    CHECK(errno == EINVAL);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_discard_leaves_correct_tail(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_discard(bs, 5) == 0);

    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 5);

    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hello", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_discard_persists_across_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
        CHECK(bstack_push(bs, (uint8_t *)"world", 5, NULL) == 0);
        CHECK(bstack_discard(bs, 5) == 0);
        bstack_close(bs);
    }

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);

        uint64_t len;
        CHECK(bstack_len(bs, &len) == 0);
        CHECK(len == 5);

        uint8_t buf[5]; size_t w;
        CHECK(bstack_peek(bs, 0, buf, &w) == 0);
        CHECK(memcmp(buf, "hello", 5) == 0);

        bstack_close(bs);
    }

    unlink(tmp);
    return 0;
}

/* =========================================================================
 * bstack_set  (compiled only with -DBSTACK_FEATURE_SET)
 * ====================================================================== */

#ifdef BSTACK_FEATURE_SET

static int test_set_overwrites_middle_bytes(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_set(bs, 5, (uint8_t *)"WORLD", 5) == 0);

    uint8_t buf[10]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(w == 10);
    CHECK(memcmp(buf, "helloWORLD", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_set_at_start(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"abcde", 5, NULL) == 0);
    CHECK(bstack_set(bs, 0, (uint8_t *)"XY", 2) == 0);

    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "XYcde", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_set_at_exact_end_boundary(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"abcde", 5, NULL) == 0);
    /* Write 2 bytes ending exactly at the last byte. */
    CHECK(bstack_set(bs, 3, (uint8_t *)"ZZ", 2) == 0);

    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "abcZZ", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_set_empty_slice_is_noop(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    CHECK(bstack_set(bs, 0, (uint8_t *)"", 0) == 0);

    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hello", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_set_does_not_change_file_size(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    CHECK(bstack_set(bs, 1, (uint8_t *)"ELL", 3) == 0);

    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 5);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_set_rejects_write_past_end(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);

    int r = bstack_set(bs, 3, (uint8_t *)"XXX", 3); /* 3+3=6 > 5 */
    CHECK(r == -1);
    CHECK(errno == EINVAL);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_set_persists_across_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
        CHECK(bstack_set(bs, 0, (uint8_t *)"HELLO", 5) == 0);
        bstack_close(bs);
    }

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        uint8_t buf[5]; size_t w;
        CHECK(bstack_peek(bs, 0, buf, &w) == 0);
        CHECK(memcmp(buf, "HELLO", 5) == 0);
        bstack_close(bs);
    }

    unlink(tmp);
    return 0;
}

#endif /* BSTACK_FEATURE_SET */

/* =========================================================================
 * bstack_extend
 * ====================================================================== */

static int test_extend_appends_zeros(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"abc", 3, NULL) == 0);
    uint64_t off;
    CHECK(bstack_extend(bs, 3, &off) == 0);
    CHECK(off == 3);

    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 6);

    uint8_t buf[6]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(w == 6);
    CHECK(memcmp(buf, "abc\x00\x00\x00", 6) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_extend_zero_is_noop(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    uint64_t off;
    CHECK(bstack_extend(bs, 0, &off) == 0);
    CHECK(off == 5);

    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 5);

    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hello", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_extend_persists_across_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"hi", 2, NULL) == 0);
        CHECK(bstack_extend(bs, 2, NULL) == 0);
        bstack_close(bs);
    }

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        uint8_t buf[4]; size_t w;
        CHECK(bstack_peek(bs, 0, buf, &w) == 0);
        CHECK(memcmp(buf, "hi\x00\x00", 4) == 0);
        bstack_close(bs);
    }

    unlink(tmp);
    return 0;
}

#ifdef BSTACK_FEATURE_SET

/* =========================================================================
 * bstack_zero  (compiled only with -DBSTACK_FEATURE_SET)
 * ====================================================================== */

static int test_zero_overwrites_with_zeros(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_zero(bs, 5, 5) == 0);

    uint8_t buf[10]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(w == 10);
    CHECK(memcmp(buf, "hello\x00\x00\x00\x00\x00", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_zero_at_start(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_zero(bs, 0, 5) == 0);

    uint8_t buf[10]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "\x00\x00\x00\x00\x00world", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_zero_at_exact_end_boundary(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    /* Write 2 bytes ending exactly at the last byte. */
    CHECK(bstack_zero(bs, 3, 2) == 0);

    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hel\x00\x00", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_zero_zero_is_noop(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    CHECK(bstack_zero(bs, 2, 0) == 0);

    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hello", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_zero_does_not_change_file_size(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    CHECK(bstack_zero(bs, 1, 3) == 0);

    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 5);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_zero_rejects_write_past_end(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);

    int r = bstack_zero(bs, 3, 3); /* 3+3=6 > 5 */
    CHECK(r == -1);
    CHECK(errno == EINVAL);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_zero_persists_across_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
        CHECK(bstack_zero(bs, 0, 5) == 0);
        bstack_close(bs);
    }

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        uint8_t buf[5]; size_t w;
        CHECK(bstack_peek(bs, 0, buf, &w) == 0);
        CHECK(memcmp(buf, "\x00\x00\x00\x00\x00", 5) == 0);
        bstack_close(bs);
    }

    unlink(tmp);
    return 0;
}

#endif /* BSTACK_FEATURE_SET */

/* =========================================================================
 * bstack_atrunc / bstack_splice / bstack_try_extend / bstack_try_discard
 * (compiled only with -DBSTACK_FEATURE_ATOMIC)
 * ====================================================================== */

#ifdef BSTACK_FEATURE_ATOMIC

static int test_atrunc_net_truncation(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_atrunc(bs, 7, (uint8_t *)"XY", 2) == 0);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);
    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "helXY", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_atrunc_net_extension(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    CHECK(bstack_atrunc(bs, 2, (uint8_t *)"WORLD", 5) == 0);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 8);
    uint8_t buf[8]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "helWORLD", 8) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_atrunc_same_size(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_atrunc(bs, 5, (uint8_t *)"WORLD", 5) == 0);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 10);
    uint8_t buf[10]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "helloWORLD", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_atrunc_n_zero_pure_append(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    CHECK(bstack_atrunc(bs, 0, (uint8_t *)"!!", 2) == 0);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 7);
    uint8_t buf[7]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hello!!", 7) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_atrunc_buf_empty_pure_discard(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_atrunc(bs, 4, NULL, 0) == 0);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 6);
    uint8_t buf[6]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hellow", 6) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_atrunc_noop(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    CHECK(bstack_atrunc(bs, 0, NULL, 0) == 0);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_atrunc_exceeds_size_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    int r = bstack_atrunc(bs, 10, (uint8_t *)"x", 1);
    CHECK(r == -1);
    CHECK(errno == EINVAL);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_atrunc_persists_across_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
        CHECK(bstack_atrunc(bs, 5, (uint8_t *)"AB", 2) == 0);
        bstack_close(bs);
    }
    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 7);
        uint8_t buf[7]; size_t w;
        CHECK(bstack_peek(bs, 0, buf, &w) == 0);
        CHECK(memcmp(buf, "helloAB", 7) == 0);
        bstack_close(bs);
    }

    unlink(tmp);
    return 0;
}

/* ---- bstack_splice -------------------------------------------------------- */

static int test_splice_returns_popped_bytes(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);

    uint8_t removed[5];
    CHECK(bstack_splice(bs, removed, 5, (uint8_t *)"XYZ", 3) == 0);
    CHECK(memcmp(removed, "world", 5) == 0);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 8);
    uint8_t buf[8]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "helloXYZ", 8) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_splice_net_extension(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    uint8_t removed[2];
    CHECK(bstack_splice(bs, removed, 2, (uint8_t *)"LONG!!", 6) == 0);
    CHECK(memcmp(removed, "lo", 2) == 0);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 9);
    uint8_t buf[9]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "helLONG!!", 9) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_splice_net_truncation(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"abcdefghij", 10, NULL) == 0);
    uint8_t removed[6];
    CHECK(bstack_splice(bs, removed, 6, (uint8_t *)"XX", 2) == 0);
    CHECK(memcmp(removed, "efghij", 6) == 0);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 6);
    uint8_t buf[6]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "abcdXX", 6) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_splice_same_size(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    uint8_t removed[5];
    CHECK(bstack_splice(bs, removed, 5, (uint8_t *)"WORLD", 5) == 0);
    CHECK(memcmp(removed, "world", 5) == 0);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 10);
    uint8_t buf[10]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "helloWORLD", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_splice_n_zero_pure_append(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    CHECK(bstack_splice(bs, NULL, 0, (uint8_t *)"!!", 2) == 0);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 7);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_splice_buf_empty_acts_like_pop(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    uint8_t removed[5];
    CHECK(bstack_splice(bs, removed, 5, NULL, 0) == 0);
    CHECK(memcmp(removed, "world", 5) == 0);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_splice_exceeds_size_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"abc", 3, NULL) == 0);
    uint8_t removed[10];
    int r = bstack_splice(bs, removed, 10, (uint8_t *)"x", 1);
    CHECK(r == -1);
    CHECK(errno == EINVAL);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 3);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_splice_persists_across_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
        uint8_t removed[5];
        CHECK(bstack_splice(bs, removed, 5, (uint8_t *)"XYZ", 3) == 0);
        CHECK(memcmp(removed, "world", 5) == 0);
        bstack_close(bs);
    }
    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 8);
        uint8_t buf[8]; size_t w;
        CHECK(bstack_peek(bs, 0, buf, &w) == 0);
        CHECK(memcmp(buf, "helloXYZ", 8) == 0);
        bstack_close(bs);
    }

    unlink(tmp);
    return 0;
}

/* ---- bstack_try_extend ---------------------------------------------------- */

static int test_try_extend_matching_returns_true(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    int ok = -1;
    CHECK(bstack_try_extend(bs, 5, (uint8_t *)"world", 5, &ok) == 0);
    CHECK(ok == 1);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 10);
    uint8_t buf[10]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "helloworld", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_try_extend_mismatching_returns_false(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    int ok = -1;
    CHECK(bstack_try_extend(bs, 3, (uint8_t *)"world", 5, &ok) == 0);
    CHECK(ok == 0);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_try_extend_empty_buf_matching(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    int ok = -1;
    CHECK(bstack_try_extend(bs, 5, NULL, 0, &ok) == 0);
    CHECK(ok == 1);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_try_extend_persists_across_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
        CHECK(bstack_try_extend(bs, 5, (uint8_t *)"world", 5, NULL) == 0);
        bstack_close(bs);
    }
    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        uint8_t buf[10]; size_t w;
        CHECK(bstack_peek(bs, 0, buf, &w) == 0);
        CHECK(memcmp(buf, "helloworld", 10) == 0);
        bstack_close(bs);
    }

    unlink(tmp);
    return 0;
}

/* ---- bstack_try_discard --------------------------------------------------- */

static int test_try_discard_matching_returns_true(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    int ok = -1;
    CHECK(bstack_try_discard(bs, 10, 5, &ok) == 0);
    CHECK(ok == 1);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);
    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hello", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_try_discard_mismatching_returns_false(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    int ok = -1;
    CHECK(bstack_try_discard(bs, 7, 5, &ok) == 0);
    CHECK(ok == 0);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 10);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_try_discard_n_zero_matching(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    int ok = -1;
    CHECK(bstack_try_discard(bs, 5, 0, &ok) == 0);
    CHECK(ok == 1);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_try_discard_n_zero_mismatching(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    int ok = -1;
    CHECK(bstack_try_discard(bs, 3, 0, &ok) == 0);
    CHECK(ok == 0);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_try_discard_n_exceeds_size_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    int r = bstack_try_discard(bs, 5, 10, NULL);
    CHECK(r == -1);
    CHECK(errno == EINVAL);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_try_discard_persists_across_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
        CHECK(bstack_try_discard(bs, 10, 5, NULL) == 0);
        bstack_close(bs);
    }
    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);
        uint8_t buf[5]; size_t w;
        CHECK(bstack_peek(bs, 0, buf, &w) == 0);
        CHECK(memcmp(buf, "hello", 5) == 0);
        bstack_close(bs);
    }

    unlink(tmp);
    return 0;
}

/* -----------------------------------------------------------------------
 * bstack_replace — callbacks and tests
 * -------------------------------------------------------------------- */

/* Uppercases all bytes; always same-length result. */
static int cb_replace_toupper(const uint8_t *old, size_t old_len,
                               uint8_t **new_buf, size_t *new_len, void *ctx)
{
    (void)ctx;
    *new_len = old_len;
    if (old_len == 0) { *new_buf = NULL; return 0; }
    *new_buf = (uint8_t *)malloc(old_len);
    if (!*new_buf) return -1;
    for (size_t i = 0; i < old_len; i++)
        (*new_buf)[i] = (uint8_t)toupper((unsigned char)old[i]);
    return 0;
}

/* Returns a fixed caller-supplied buffer. */
struct replace_fixed_ctx { const uint8_t *data; size_t len; };
static int cb_replace_fixed(const uint8_t *old, size_t old_len,
                             uint8_t **new_buf, size_t *new_len, void *ctx)
{
    (void)old; (void)old_len;
    const struct replace_fixed_ctx *c = (const struct replace_fixed_ctx *)ctx;
    *new_len = c->len;
    if (c->len == 0) { *new_buf = NULL; return 0; }
    *new_buf = (uint8_t *)malloc(c->len);
    if (!*new_buf) return -1;
    memcpy(*new_buf, c->data, c->len);
    return 0;
}

/* Captures input into ctx->buf then echoes it back unchanged. */
struct replace_capture_ctx { uint8_t buf[64]; size_t len; };
static int cb_replace_capture_echo(const uint8_t *old, size_t old_len,
                                    uint8_t **new_buf, size_t *new_len,
                                    void *ctx)
{
    struct replace_capture_ctx *c = (struct replace_capture_ctx *)ctx;
    c->len = old_len < sizeof(c->buf) ? old_len : sizeof(c->buf) - 1;
    if (old_len > 0) memcpy(c->buf, old, c->len);
    *new_len = old_len;
    if (old_len == 0) { *new_buf = NULL; return 0; }
    *new_buf = (uint8_t *)malloc(old_len);
    if (!*new_buf) return -1;
    memcpy(*new_buf, old, old_len);
    return 0;
}

static int test_replace_same_size(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello world", 11, NULL) == 0);
    CHECK(bstack_replace(bs, 5, cb_replace_toupper, NULL) == 0);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 11);
    uint8_t buf[11]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hello WORLD", 11) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_replace_net_extension(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    struct replace_fixed_ctx ctx = { (uint8_t *)"WORLD", 5 };
    CHECK(bstack_replace(bs, 2, cb_replace_fixed, &ctx) == 0);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 8);
    uint8_t buf[8]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "helWORLD", 8) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_replace_net_truncation(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    struct replace_fixed_ctx ctx = { (uint8_t *)"XY", 2 };
    CHECK(bstack_replace(bs, 7, cb_replace_fixed, &ctx) == 0);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);
    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "helXY", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_replace_n_zero_acts_as_append(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    struct replace_fixed_ctx ctx = { (uint8_t *)"!!", 2 };
    CHECK(bstack_replace(bs, 0, cb_replace_fixed, &ctx) == 0);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 7);
    uint8_t buf[7]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hello!!", 7) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_replace_empty_result_acts_as_discard(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    struct replace_fixed_ctx ctx = { NULL, 0 };
    CHECK(bstack_replace(bs, 4, cb_replace_fixed, &ctx) == 0);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 6);
    uint8_t buf[6]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hellow", 6) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_replace_callback_receives_correct_bytes(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    struct replace_capture_ctx ctx = {{0}, 0};
    CHECK(bstack_replace(bs, 5, cb_replace_capture_echo, &ctx) == 0);
    CHECK(ctx.len == 5);
    CHECK(memcmp(ctx.buf, "world", 5) == 0);
    /* File unchanged — callback echoed input back. */
    uint8_t buf[10]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "helloworld", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_replace_exceeds_size_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    struct replace_fixed_ctx ctx = { NULL, 0 };
    int r = bstack_replace(bs, 10, cb_replace_fixed, &ctx);
    CHECK(r == -1);
    CHECK(errno == EINVAL);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);
    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hello", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_replace_persists_across_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
        CHECK(bstack_replace(bs, 5, cb_replace_toupper, NULL) == 0);
        bstack_close(bs);
    }
    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 10);
        uint8_t buf[10]; size_t w;
        CHECK(bstack_peek(bs, 0, buf, &w) == 0);
        CHECK(memcmp(buf, "helloWORLD", 10) == 0);
        bstack_close(bs);
    }

    unlink(tmp);
    return 0;
}

#endif /* BSTACK_FEATURE_ATOMIC */

/* =========================================================================
 * bstack_swap / bstack_cas
 * (compiled only with -DBSTACK_FEATURE_ATOMIC and -DBSTACK_FEATURE_SET)
 * ====================================================================== */

#if defined(BSTACK_FEATURE_ATOMIC) && defined(BSTACK_FEATURE_SET)

static int test_swap_returns_old_stores_new(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    uint8_t old[5];
    CHECK(bstack_swap(bs, 5, old, (uint8_t *)"WORLD", 5) == 0);
    CHECK(memcmp(old, "world", 5) == 0);
    uint8_t buf[10]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "helloWORLD", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_swap_len_zero_is_noop(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    CHECK(bstack_swap(bs, 0, NULL, NULL, 0) == 0);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_swap_at_start(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    uint8_t old[5];
    CHECK(bstack_swap(bs, 0, old, (uint8_t *)"HELLO", 5) == 0);
    CHECK(memcmp(old, "hello", 5) == 0);
    uint8_t buf[10]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "HELLOworld", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_swap_does_not_change_file_size(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"abcde", 5, NULL) == 0);
    uint8_t old[3];
    CHECK(bstack_swap(bs, 1, old, (uint8_t *)"XYZ", 3) == 0);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);
    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "aXYZe", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_swap_exceeds_size_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    uint8_t old[7];
    int r = bstack_swap(bs, 3, old, (uint8_t *)"TOOLONG", 7);
    CHECK(r == -1);
    CHECK(errno == EINVAL);
    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hello", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_swap_persists_across_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
        uint8_t old[5];
        CHECK(bstack_swap(bs, 5, old, (uint8_t *)"WORLD", 5) == 0);
        bstack_close(bs);
    }
    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        uint8_t buf[10]; size_t w;
        CHECK(bstack_peek(bs, 0, buf, &w) == 0);
        CHECK(memcmp(buf, "helloWORLD", 10) == 0);
        bstack_close(bs);
    }

    unlink(tmp);
    return 0;
}

/* ---- bstack_cas ----------------------------------------------------------- */

static int test_cas_matching_performs_exchange(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    int ok = -1;
    CHECK(bstack_cas(bs, 5, (uint8_t *)"world", (uint8_t *)"WORLD", 5, &ok) == 0);
    CHECK(ok == 1);
    uint8_t buf[10]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "helloWORLD", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_cas_mismatch_returns_false_no_change(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    int ok = -1;
    CHECK(bstack_cas(bs, 5, (uint8_t *)"xxxxx", (uint8_t *)"WORLD", 5, &ok) == 0);
    CHECK(ok == 0);
    uint8_t buf[10]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "helloworld", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_cas_len_zero_returns_true(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    int ok = -1;
    CHECK(bstack_cas(bs, 0, NULL, NULL, 0, &ok) == 0);
    CHECK(ok == 1);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_cas_does_not_change_file_size(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"abcde", 5, NULL) == 0);
    CHECK(bstack_cas(bs, 1, (uint8_t *)"bcd", (uint8_t *)"XYZ", 3, NULL) == 0);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);
    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "aXYZe", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_cas_exceeds_size_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    int r = bstack_cas(bs, 3, (uint8_t *)"TOOLONG", (uint8_t *)"TOOLONG", 7, NULL);
    CHECK(r == -1);
    CHECK(errno == EINVAL);
    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hello", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_cas_persists_across_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
        CHECK(bstack_cas(bs, 5, (uint8_t *)"world", (uint8_t *)"WORLD", 5, NULL) == 0);
        bstack_close(bs);
    }
    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        uint8_t buf[10]; size_t w;
        CHECK(bstack_peek(bs, 0, buf, &w) == 0);
        CHECK(memcmp(buf, "helloWORLD", 10) == 0);
        bstack_close(bs);
    }

    unlink(tmp);
    return 0;
}

/* -----------------------------------------------------------------------
 * bstack_process — callbacks and tests
 * -------------------------------------------------------------------- */

/* Uppercases all bytes in place. */
static int cb_proc_toupper(uint8_t *buf, size_t len, void *ctx)
{
    (void)ctx;
    for (size_t i = 0; i < len; i++)
        buf[i] = (uint8_t)toupper((unsigned char)buf[i]);
    return 0;
}

/* Fills buffer with 'X'. */
static int cb_proc_fill_x(uint8_t *buf, size_t len, void *ctx)
{
    (void)ctx;
    memset(buf, 'X', len);
    return 0;
}

/* Captures bytes into ctx->buf without modifying them. */
struct proc_capture_ctx { uint8_t buf[64]; size_t len; };
static int cb_proc_capture_noop(uint8_t *buf, size_t len, void *ctx)
{
    struct proc_capture_ctx *c = (struct proc_capture_ctx *)ctx;
    c->len = len < sizeof(c->buf) ? len : sizeof(c->buf) - 1;
    memcpy(c->buf, buf, c->len);
    return 0; /* leave buf unmodified */
}

/* No-op callback that records whether it was called. */
static int cb_proc_was_called(uint8_t *buf, size_t len, void *ctx)
{
    (void)buf; (void)len;
    *(int *)ctx = 1;
    return 0;
}

static int test_process_mutates_range(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello world", 11, NULL) == 0);
    CHECK(bstack_process(bs, 6, 11, cb_proc_toupper, NULL) == 0);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 11);
    uint8_t buf[11]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hello WORLD", 11) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_process_middle_range(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"abcdefgh", 8, NULL) == 0);
    CHECK(bstack_process(bs, 2, 5, cb_proc_fill_x, NULL) == 0);

    uint8_t buf[8]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "abXXXfgh", 8) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_process_callback_receives_correct_bytes(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    struct proc_capture_ctx ctx = {{0}, 0};
    CHECK(bstack_process(bs, 5, 10, cb_proc_capture_noop, &ctx) == 0);
    CHECK(ctx.len == 5);
    CHECK(memcmp(ctx.buf, "world", 5) == 0);
    /* File unchanged — callback did not modify buffer. */
    uint8_t buf[10]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "helloworld", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_process_start_end_equal_is_noop(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    int called = 0;
    CHECK(bstack_process(bs, 3, 3, cb_proc_was_called, &called) == 0);
    CHECK(called == 1);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);
    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hello", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_process_does_not_change_file_size(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"abcde", 5, NULL) == 0);
    CHECK(bstack_process(bs, 1, 4, cb_proc_fill_x, NULL) == 0);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_process_end_less_than_start_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    int r = bstack_process(bs, 3, 2, cb_proc_toupper, NULL);
    CHECK(r == -1);
    CHECK(errno == EINVAL);
    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hello", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_process_end_exceeds_size_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    int r = bstack_process(bs, 2, 10, cb_proc_toupper, NULL);
    CHECK(r == -1);
    CHECK(errno == EINVAL);
    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hello", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_process_persists_across_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
        CHECK(bstack_process(bs, 5, 10, cb_proc_toupper, NULL) == 0);
        bstack_close(bs);
    }
    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        uint8_t buf[10]; size_t w;
        CHECK(bstack_peek(bs, 0, buf, &w) == 0);
        CHECK(memcmp(buf, "helloWORLD", 10) == 0);
        bstack_close(bs);
    }

    unlink(tmp);
    return 0;
}

#endif /* BSTACK_FEATURE_ATOMIC && BSTACK_FEATURE_SET */

/* =========================================================================
 * main
 * ====================================================================== */

int main(void)
{
    /* Functional */
    T(test_push_returns_correct_offsets);
    T(test_pop_returns_correct_bytes_and_shrinks);
    T(test_pop_across_push_boundary);
    T(test_pop_on_empty_file_returns_error);
    T(test_pop_n_exceeds_size_returns_error);
    T(test_peek_reads_from_offset_to_end);
    T(test_peek_offset_exceeds_size_returns_error);
    T(test_get_reads_half_open_range);

    /* Range validation */
    T(test_get_end_exceeds_size_returns_error);
    T(test_get_end_less_than_start_returns_error);
    T(test_get_does_not_modify_file);

    /* Persistence */
    T(test_reopen_reads_back_correct_data);
    T(test_reopen_and_continue_pushing);
    T(test_reopen_after_pop_sees_truncated_file);

    /* Boundary / zero */
    T(test_push_empty_slice);
    T(test_pop_zero_bytes);
    T(test_peek_at_end_offset_on_empty_file);
    T(test_get_zero_range_on_empty_file);
    T(test_drain_to_zero_then_push_starts_at_offset_zero);

    /* Data integrity */
    T(test_peek_does_not_modify_file);
    T(test_binary_roundtrip_all_byte_values);
    T(test_large_payload_roundtrip);

    /* Header / magic */
    T(test_new_file_has_valid_header);
    T(test_header_clen_matches_after_pushes);
    T(test_header_clen_matches_after_pop);
    T(test_open_rejects_bad_magic);
    T(test_open_rejects_truncated_header);

    /* Crash recovery */
    T(test_recovery_truncates_partial_push);
    T(test_recovery_repairs_header_after_partial_pop);

    /* Concurrency */
    T(test_concurrent_reads_do_not_serialise);
    T(test_concurrent_pushes_non_overlapping);
    T(test_concurrent_len_is_multiple_of_item_size);

    /* Interleaved */
    T(test_interleaved_push_pop_correct_state);

    /* bstack_discard */
    T(test_discard_removes_bytes_from_tail);
    T(test_discard_zero_is_noop);
    T(test_discard_exceeds_size_returns_error);
    T(test_discard_on_empty_returns_error);
    T(test_discard_leaves_correct_tail);
    T(test_discard_persists_across_reopen);

    /* bstack_extend */
    T(test_extend_appends_zeros);
    T(test_extend_zero_is_noop);
    T(test_extend_persists_across_reopen);

#ifdef BSTACK_FEATURE_SET
    /* bstack_set */
    T(test_set_overwrites_middle_bytes);
    T(test_set_at_start);
    T(test_set_at_exact_end_boundary);
    T(test_set_empty_slice_is_noop);
    T(test_set_does_not_change_file_size);
    T(test_set_rejects_write_past_end);
    T(test_set_persists_across_reopen);

    /* bstack_zero */
    T(test_zero_overwrites_with_zeros);
    T(test_zero_at_start);
    T(test_zero_at_exact_end_boundary);
    T(test_zero_zero_is_noop);
    T(test_zero_does_not_change_file_size);
    T(test_zero_rejects_write_past_end);
    T(test_zero_persists_across_reopen);
#endif

#ifdef BSTACK_FEATURE_ATOMIC
    /* bstack_atrunc */
    T(test_atrunc_net_truncation);
    T(test_atrunc_net_extension);
    T(test_atrunc_same_size);
    T(test_atrunc_n_zero_pure_append);
    T(test_atrunc_buf_empty_pure_discard);
    T(test_atrunc_noop);
    T(test_atrunc_exceeds_size_returns_error);
    T(test_atrunc_persists_across_reopen);

    /* bstack_splice */
    T(test_splice_returns_popped_bytes);
    T(test_splice_net_extension);
    T(test_splice_net_truncation);
    T(test_splice_same_size);
    T(test_splice_n_zero_pure_append);
    T(test_splice_buf_empty_acts_like_pop);
    T(test_splice_exceeds_size_returns_error);
    T(test_splice_persists_across_reopen);

    /* bstack_try_extend */
    T(test_try_extend_matching_returns_true);
    T(test_try_extend_mismatching_returns_false);
    T(test_try_extend_empty_buf_matching);
    T(test_try_extend_persists_across_reopen);

    /* bstack_try_discard */
    T(test_try_discard_matching_returns_true);
    T(test_try_discard_mismatching_returns_false);
    T(test_try_discard_n_zero_matching);
    T(test_try_discard_n_zero_mismatching);
    T(test_try_discard_n_exceeds_size_returns_error);
    T(test_try_discard_persists_across_reopen);

    /* bstack_replace */
    T(test_replace_same_size);
    T(test_replace_net_extension);
    T(test_replace_net_truncation);
    T(test_replace_n_zero_acts_as_append);
    T(test_replace_empty_result_acts_as_discard);
    T(test_replace_callback_receives_correct_bytes);
    T(test_replace_exceeds_size_returns_error);
    T(test_replace_persists_across_reopen);
#endif

#if defined(BSTACK_FEATURE_ATOMIC) && defined(BSTACK_FEATURE_SET)
    /* bstack_swap */
    T(test_swap_returns_old_stores_new);
    T(test_swap_len_zero_is_noop);
    T(test_swap_at_start);
    T(test_swap_does_not_change_file_size);
    T(test_swap_exceeds_size_returns_error);
    T(test_swap_persists_across_reopen);

    /* bstack_cas */
    T(test_cas_matching_performs_exchange);
    T(test_cas_mismatch_returns_false_no_change);
    T(test_cas_len_zero_returns_true);
    T(test_cas_does_not_change_file_size);
    T(test_cas_exceeds_size_returns_error);
    T(test_cas_persists_across_reopen);

    /* bstack_process */
    T(test_process_mutates_range);
    T(test_process_middle_range);
    T(test_process_callback_receives_correct_bytes);
    T(test_process_start_end_equal_is_noop);
    T(test_process_does_not_change_file_size);
    T(test_process_end_less_than_start_returns_error);
    T(test_process_end_exceeds_size_returns_error);
    T(test_process_persists_across_reopen);
#endif

    printf("\n%d/%d passed\n", g_passed, g_total);
    return (g_passed == g_total) ? 0 : 1;
}
