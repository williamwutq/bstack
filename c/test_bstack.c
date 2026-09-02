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

/* On Windows, open()/read()/write() default to text mode and translate
 * 0x0A <-> 0x0D 0x0A, which corrupts the binary file images these tests craft
 * and inspect.  O_BINARY suppresses that; it is a no-op (0) on POSIX. */
#ifndef O_BINARY
#  define O_BINARY 0
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

static const uint8_t MAGIC[8]        = {'B','S','T','K', 0, 4, 4, 0};
static const uint8_t MAGIC_PREFIX[6] = {'B','S','T','K', 0, 4};
#define TEST_HEADER_SIZE 32

/* Write-in-progress journal modes (mirror bstack.c). */
#define WIP_SET           ((uint64_t)0)
#define WIP_SPLICE_GROW   (UINT64_MAX - 1)
#define WIP_SPLICE_SHRINK (UINT64_MAX - 2)
#define WIP_REPEAT        (UINT64_MAX - 3)
#define WIP_COPY          (UINT64_MAX - 4)
#define WIP_MULTI         (UINT64_MAX - 5)

/* Write a crafted 0.4.0 file image: 32-byte header (magic, clen, wip_ptr,
 * wip_aux) followed by payload_len raw payload bytes. Used to simulate a file
 * left with an armed journal by a crash mid-write. */
static int write_wip_file(const char *path, uint64_t clen,
                          uint64_t wip_ptr, uint64_t wip_aux,
                          const uint8_t *payload, size_t payload_len)
{
    uint8_t hdr[TEST_HEADER_SIZE];
    memcpy(hdr, MAGIC, 8);
    for (int i = 0; i < 8; i++) hdr[8 + i]  = (uint8_t)(clen    >> (8 * i));
    for (int i = 0; i < 8; i++) hdr[16 + i] = (uint8_t)(wip_ptr >> (8 * i));
    for (int i = 0; i < 8; i++) hdr[24 + i] = (uint8_t)(wip_aux >> (8 * i));
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | O_BINARY, 0666);
    if (fd < 0) return -1;
    if (write(fd, hdr, TEST_HEADER_SIZE) != (ssize_t)TEST_HEADER_SIZE) {
        close(fd); return -1;
    }
    if (payload_len &&
        write(fd, payload, payload_len) != (ssize_t)payload_len) {
        close(fd); return -1;
    }
    close(fd);
    return 0;
}

static int test_new_file_has_valid_header(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    bstack_close(bs);

    int fd = open(tmp, O_RDONLY);
    CHECK(fd >= 0);

    struct stat st; fstat(fd, &st);
    CHECK(st.st_size == TEST_HEADER_SIZE);

    uint8_t hdr[TEST_HEADER_SIZE];
    CHECK(pread(fd, hdr, TEST_HEADER_SIZE, 0) == TEST_HEADER_SIZE);
    CHECK(memcmp(hdr, MAGIC, 8) == 0);
    /* committed_len[8] + wip_ptr[8] + wip_aux[8] all zero on a fresh file. */
    uint8_t zeros[24] = {0};
    CHECK(memcmp(hdr + 8, zeros, 24) == 0);

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

    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC | O_BINARY, 0666);
    CHECK(fd >= 0);
    uint8_t garbage[TEST_HEADER_SIZE];
    memcpy(garbage, "GARBAGE!", 8);
    memset(garbage + 8, 0, sizeof garbage - 8);
    CHECK(write(fd, garbage, sizeof garbage) == (ssize_t)sizeof garbage);
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

    /* Commit "hello" (clen == 5, file == 32+5 == 37 bytes). */
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
        CHECK(ftruncate(fd, TEST_HEADER_SIZE + 5 + 3) == 0);
        /* clen at offset 8 still says 5 — do NOT update it. */
        close(fd);
    }

    /* Reopen: recovery must truncate back to 32+5. */
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

    /* Simulate a pop that truncated the file to 32+5 but crashed before
     * writing the new committed length to the header. */
    {
        int fd = open(tmp, O_WRONLY);
        CHECK(fd >= 0);
        CHECK(ftruncate(fd, TEST_HEADER_SIZE + 5) == 0);
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
 * Deferred replay
 *
 * A genuine mid-write I/O failure is unreachable through the public API, so
 * these tests induce one: the stack's descriptor is swapped for a read-only
 * one on the same file, leaving seeks and size checks working while every
 * write — including the rollback ftruncate — fails.  struct bstack is opaque
 * here, so the descriptor is located by matching (st_dev, st_ino).
 * ====================================================================== */

#ifndef _WIN32

static int find_stack_fd(const char *path, int exclude)
{
    struct stat want;
    if (stat(path, &want) != 0)
        return -1;
    for (int fd = 3; fd < 256; fd++) {
        struct stat st;
        if (fd == exclude || fstat(fd, &st) != 0)
            continue;
        if (st.st_dev == want.st_dev && st.st_ino == want.st_ino)
            return fd;
    }
    return -1;
}

/* Make every write on bs fail, issue one push, then restore the descriptor.
 * Returns 0 only if that push failed, which is what flags the stack. */
static int fail_one_write(bstack_t *bs, const char *path)
{
    int ro = open(path, O_RDONLY | O_BINARY);
    if (ro < 0)
        return -1;
    int fd = find_stack_fd(path, ro);
    if (fd < 0) { close(ro); return -1; }
    int saved = dup(fd);
    if (saved < 0) { close(ro); return -1; }
    if (dup2(ro, fd) < 0) { close(ro); close(saved); return -1; }

    int rc = bstack_push(bs, (const uint8_t *)"x", 1, NULL);

    if (dup2(saved, fd) < 0) { close(ro); close(saved); return -1; }
    close(saved);
    close(ro);
    return (rc == 0) ? -1 : 0;
}

static int test_failed_write_defers_a_replay(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (const uint8_t *)"hello", 5, NULL) == 0);
    CHECK(fail_one_write(bs, tmp) == 0);

    /* The failed write flagged the stack, so reads are refused ... */
    uint64_t len;
    errno = 0;
    CHECK(bstack_len(bs, &len) == -1);
    CHECK(errno == BSTACK_EREPLAY);
    uint8_t buf[8];
    errno = 0;
    CHECK(bstack_get(bs, 0, 5, buf) == -1);
    CHECK(errno == BSTACK_EREPLAY);

    /* ... until the next write replays (a no-op: nothing ever landed). */
    CHECK(bstack_push(bs, (const uint8_t *)"!", 1, NULL) == 0);
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 6);
    CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, "hello!", 6) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_pending_replay_applies_the_journal_on_the_next_write(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    uint8_t a[300]; memset(a, 'a', sizeof a);
    CHECK(bstack_push(bs, a, sizeof a, NULL) == 0);

    /* A set of [0, 300) that staged its backup and armed, then died before
     * writing in place: recovery rolls it forward from the staged tail. */
    {
        uint8_t b[300]; memset(b, 'b', sizeof b);
        uint8_t ptr[8] = { TEST_HEADER_SIZE, 0, 0, 0, 0, 0, 0, 0 };
        int fd = open(tmp, O_WRONLY | O_BINARY);
        CHECK(fd >= 0);
        CHECK(pwrite(fd, b, sizeof b, TEST_HEADER_SIZE + 300) == (ssize_t)sizeof b);
        CHECK(pwrite(fd, ptr, 8, 16) == 8);   /* wip_ptr = 32 (target 0) */
        close(fd);
    }
    CHECK(fail_one_write(bs, tmp) == 0);

    CHECK(bstack_push(bs, (const uint8_t *)"z", 1, NULL) == 0);
    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 301);
    uint8_t got[301], expect[301];
    memset(expect, 'b', 300); expect[300] = 'z';
    CHECK(bstack_peek(bs, 0, got, NULL) == 0);
    CHECK(memcmp(got, expect, sizeof expect) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_pending_replay_leaves_the_locked_prefix_readable(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    uint8_t a[128]; memset(a, 'a', sizeof a);
    CHECK(bstack_push(bs, a, sizeof a, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 64) == 0);
    CHECK(fail_one_write(bs, tmp) == 0);

    /* Locked bytes are immutable and no journal can target them, so the
     * lock-free path still serves them; anything past the prefix is refused. */
    uint8_t buf[65];
    CHECK(bstack_get(bs, 0, 64, buf) == 0);
    CHECK(memcmp(buf, a, 64) == 0);
    errno = 0;
    CHECK(bstack_get(bs, 0, 65, buf) == -1);
    CHECK(errno == BSTACK_EREPLAY);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_recover_applies_a_pending_replay_without_a_write(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    uint8_t a[300]; memset(a, 'a', sizeof a);
    CHECK(bstack_push(bs, a, sizeof a, NULL) == 0);
    {
        uint8_t b[300]; memset(b, 'b', sizeof b);
        uint8_t ptr[8] = { TEST_HEADER_SIZE, 0, 0, 0, 0, 0, 0, 0 };
        int fd = open(tmp, O_WRONLY | O_BINARY);
        CHECK(fd >= 0);
        CHECK(pwrite(fd, b, sizeof b, TEST_HEADER_SIZE + 300) == (ssize_t)sizeof b);
        CHECK(pwrite(fd, ptr, 8, 16) == 8);
        close(fd);
    }
    CHECK(fail_one_write(bs, tmp) == 0);
    uint8_t got[300];
    CHECK(bstack_get(bs, 0, 300, got) == -1);

    /* The point of the call: read again after a failed write, without
     * having to issue another one. */
    int replayed = -1;
    CHECK(bstack_recover(bs, &replayed) == 0);
    CHECK(replayed == 1);
    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 300);
    uint8_t expect[300]; memset(expect, 'b', sizeof expect);
    CHECK(bstack_get(bs, 0, 300, got) == 0);
    CHECK(memcmp(got, expect, sizeof expect) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_recover_on_an_intact_stack_reports_nothing_pending(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (const uint8_t *)"hello", 5, NULL) == 0);

    int replayed = -1;
    CHECK(bstack_recover(bs, &replayed) == 0);
    CHECK(replayed == 0);

    /* A replay runs once: the second call has nothing left to do.  A NULL
     * out_replayed is valid. */
    CHECK(fail_one_write(bs, tmp) == 0);
    CHECK(bstack_recover(bs, &replayed) == 0);
    CHECK(replayed == 1);
    CHECK(bstack_recover(bs, NULL) == 0);
    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 5);

    bstack_close(bs); unlink(tmp);
    return 0;
}

#endif /* !_WIN32 */

/* =========================================================================
 * Write-in-progress journal recovery (armed on open) — recovery is always
 * compiled, so these run in every feature configuration.
 * ====================================================================== */

static int test_recovery_replays_armed_set(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    /* clen=10, target at offset 0; old bytes still 'X', staged new bytes 'N'
     * in the tail. Recovery copies the tail into [0,10). */
    uint8_t payload[20];
    memset(payload, 'X', 10);
    memset(payload + 10, 'N', 10);
    CHECK(write_wip_file(tmp, 10, TEST_HEADER_SIZE, WIP_SET, payload, 20) == 0);

    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 10);
    uint8_t buf[10]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, "NNNNNNNNNN", 10) == 0);
    bstack_close(bs);

    int fd = open(tmp, O_RDONLY | O_BINARY);
    struct stat st; fstat(fd, &st);
    CHECK(st.st_size == TEST_HEADER_SIZE + 10);       /* tail truncated */
    CHECK(raw_read_le64(fd, 16) == 0);                /* wip_ptr disarmed */
    close(fd); unlink(tmp);
    return 0;
}

static int test_recovery_replays_armed_repeat(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    /* clen=12, target at 0; tail is [k=6 | "ab"] → fill [0,12) with "ab"x6. */
    uint8_t payload[22];
    memset(payload, '.', 12);
    uint64_t k = 6;
    for (int i = 0; i < 8; i++) payload[12 + i] = (uint8_t)(k >> (8 * i));
    payload[20] = 'a'; payload[21] = 'b';
    CHECK(write_wip_file(tmp, 12, TEST_HEADER_SIZE, WIP_REPEAT, payload, 22) == 0);

    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 12);
    uint8_t buf[12]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, "abababababab", 12) == 0);
    bstack_close(bs);
    unlink(tmp);
    return 0;
}

static int test_recovery_replays_armed_copy(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    /* clen=20; source [0,10)='S', dest [10,20) old='d'; tail [src=0 | n=10].
     * wip_ptr = HEADER+10 (dest). Recovery copies source into [10,20). */
    uint8_t payload[36];
    memset(payload, 'S', 10);
    memset(payload + 10, 'd', 10);
    uint64_t src = 0, n = 10;
    for (int i = 0; i < 8; i++) payload[20 + i] = (uint8_t)(src >> (8 * i));
    for (int i = 0; i < 8; i++) payload[28 + i] = (uint8_t)(n   >> (8 * i));
    CHECK(write_wip_file(tmp, 20, TEST_HEADER_SIZE + 10, WIP_COPY, payload, 36) == 0);

    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 20);
    uint8_t buf[20]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, "SSSSSSSSSSSSSSSSSSSS", 20) == 0); /* both halves = source */
    bstack_close(bs);
    unlink(tmp);
    return 0;
}

static int test_recovery_rolls_forward_splice_grow(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    /* Replace last 20 of 100 bytes with 50 → clen'=130, a=80, S=130.
     * payload: 80 'A' + 20 'X' + 30 gap + 50 'N' staged at [130,180). */
    uint8_t payload[180];
    memset(payload, 'A', 80);
    memset(payload + 80, 'X', 20);
    memset(payload + 100, 0, 30);
    memset(payload + 130, 'N', 50);
    CHECK(write_wip_file(tmp, 100, TEST_HEADER_SIZE + 80, WIP_SPLICE_GROW,
                         payload, 180) == 0);

    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 130);
    uint8_t buf[130]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    for (int i = 0; i < 80; i++)  CHECK(buf[i] == 'A');
    for (int i = 80; i < 130; i++) CHECK(buf[i] == 'N');
    bstack_close(bs);

    int fd = open(tmp, O_RDONLY | O_BINARY);
    struct stat st; fstat(fd, &st);
    CHECK(st.st_size == TEST_HEADER_SIZE + 130);
    CHECK(raw_read_le64(fd, 16) == 0);
    close(fd); unlink(tmp);
    return 0;
}

static int test_recovery_rolls_forward_splice_shrink(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    /* Replace last 50 of 130 bytes with 20 → clen'=100, a=80, S=130.
     * payload: 80 'A' + 50 'X' + 20 'N' staged at [130,150). */
    uint8_t payload[150];
    memset(payload, 'A', 80);
    memset(payload + 80, 'X', 50);
    memset(payload + 130, 'N', 20);
    CHECK(write_wip_file(tmp, 130, TEST_HEADER_SIZE + 80, WIP_SPLICE_SHRINK,
                         payload, 150) == 0);

    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 100);
    uint8_t buf[100]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    for (int i = 0; i < 80; i++)   CHECK(buf[i] == 'A');
    for (int i = 80; i < 100; i++) CHECK(buf[i] == 'N');
    bstack_close(bs);
    unlink(tmp);
    return 0;
}

static int test_recovery_rolls_back_unknown_mode(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    /* An armed but unrecognized wip_aux rolls back to the committed length. */
    uint8_t payload[20];
    memset(payload, 'A', 10);
    memset(payload + 10, 'Z', 10); /* stale staged bytes to be dropped */
    CHECK(write_wip_file(tmp, 10, TEST_HEADER_SIZE, 0x1234, payload, 20) == 0);

    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 10);
    uint8_t buf[10]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, "AAAAAAAAAA", 10) == 0); /* original preserved */
    bstack_close(bs);

    int fd = open(tmp, O_RDONLY | O_BINARY);
    struct stat st; fstat(fd, &st);
    CHECK(st.st_size == TEST_HEADER_SIZE + 10);
    CHECK(raw_read_le64(fd, 16) == 0);
    close(fd); unlink(tmp);
    return 0;
}

/* =========================================================================
 * Legacy migration
 * ====================================================================== */

static int test_migrate_upgrades_legacy_file(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    /* Craft a legacy 0.1.x file: 16-byte header (magic 0.1.15, clen=5) + payload. */
    {
        uint8_t hdr[16] = {'B','S','T','K', 0, 1, 15, 0};
        uint64_t clen = 5;
        for (int i = 0; i < 8; i++) hdr[8 + i] = (uint8_t)(clen >> (8 * i));
        int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC | O_BINARY, 0666);
        CHECK(fd >= 0);
        CHECK(write(fd, hdr, 16) == 16);
        CHECK(write(fd, "hello", 5) == 5);
        close(fd);
    }
    /* Opening it directly must fail (incompatible magic). */
    CHECK(bstack_open(tmp) == NULL);

    /* Migrate, then open and verify the payload survived. */
    CHECK(bstack_migrate(tmp) == 0);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);
    uint8_t buf[5]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, "hello", 5) == 0);
    bstack_close(bs);

    /* Second migrate must fail: the file is no longer legacy. */
    CHECK(bstack_migrate(tmp) == -1);
    CHECK(errno == EINVAL);

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

/* --- bstack_repeat ----------------------------------------------------- */

static int test_repeat_fills_pattern(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"..........", 10, NULL) == 0);
    /* 3 copies of "xy" over [0,6). */
    CHECK(bstack_repeat(bs, 0, (uint8_t *)"xy", 2, 3) == 0);
    uint8_t buf[10]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, "xyxyxy....", 10) == 0);
    bstack_close(bs);
    unlink(tmp);
    return 0;
}

static int test_repeat_empty_or_zero_count_is_noop(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    CHECK(bstack_repeat(bs, 0, (uint8_t *)"x", 0, 5) == 0); /* empty pattern */
    CHECK(bstack_repeat(bs, 0, (uint8_t *)"x", 1, 0) == 0); /* zero count */
    uint8_t buf[5]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, "hello", 5) == 0);
    bstack_close(bs);
    unlink(tmp);
    return 0;
}

static int test_repeat_journals_large_region_and_reopens_clean(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    /* 400-byte payload; fill [0,300) with "ab"x150 spans a 256B block, so it
     * goes through the repeat journal, not the atomic-block fast path. */
    uint8_t *big = (uint8_t *)malloc(400);
    CHECK(big != NULL);
    memset(big, 'Z', 400);
    CHECK(bstack_push(bs, big, 400, NULL) == 0);
    CHECK(bstack_repeat(bs, 0, (uint8_t *)"ab", 2, 150) == 0);
    uint8_t *out = (uint8_t *)malloc(400);
    CHECK(out != NULL);
    CHECK(bstack_peek(bs, 0, out, NULL) == 0);
    for (int i = 0; i < 300; i++) CHECK(out[i] == (i % 2 == 0 ? 'a' : 'b'));
    for (int i = 300; i < 400; i++) CHECK(out[i] == 'Z');
    bstack_close(bs);

    /* Journal disarmed, tail truncated, value survives reopen. */
    int fd = open(tmp, O_RDONLY | O_BINARY);
    struct stat st; fstat(fd, &st);
    CHECK(st.st_size == TEST_HEADER_SIZE + 400);
    CHECK(raw_read_le64(fd, 16) == 0);
    close(fd);
    bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_peek(bs, 0, out, NULL) == 0);
    for (int i = 0; i < 300; i++) CHECK(out[i] == (i % 2 == 0 ? 'a' : 'b'));
    bstack_close(bs);
    free(big); free(out);
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

/* =========================================================================
 * bstack_extend_sparse / bstack_extend_sparse_batched
 * ====================================================================== */

static int test_extend_sparse_writes_prefix_and_zeros_rest(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"abc", 3, NULL) == 0);
    uint64_t off;
    CHECK(bstack_extend_sparse(bs, (uint8_t *)"XY", 2, 6, &off) == 0);
    CHECK(off == 3);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 9);
    uint8_t buf[9]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(w == 9);
    CHECK(memcmp(buf, "abcXY\x00\x00\x00\x00", 9) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_extend_sparse_empty_buf_is_pure_extend(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"ab", 2, NULL) == 0);
    uint64_t off;
    CHECK(bstack_extend_sparse(bs, NULL, 0, 4, &off) == 0);
    CHECK(off == 2);

    uint8_t buf[6]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "ab\x00\x00\x00\x00", 6) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_extend_sparse_buf_longer_than_length_errors(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    errno = 0;
    CHECK(bstack_extend_sparse(bs, (uint8_t *)"toolong", 7, 3, NULL) == -1);
    CHECK(errno == EINVAL);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_extend_sparse_batched_scatters_buffers(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"..", 2, NULL) == 0);
    bstack_iovec_t writes[2] = {
        { 0, (uint8_t *)"AA", 2 },
        { 5, (uint8_t *)"BB", 2 },
    };
    uint64_t off;
    CHECK(bstack_extend_sparse_batched(bs, writes, 2, 8, &off) == 0);
    CHECK(off == 2);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 10);
    uint8_t buf[10]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "..AA\x00\x00\x00" "BB" "\x00", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_extend_sparse_batched_overlap_errors(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    bstack_iovec_t writes[2] = {
        { 0, (uint8_t *)"aaa", 3 },
        { 2, (uint8_t *)"bb", 2 },
    };
    errno = 0;
    CHECK(bstack_extend_sparse_batched(bs, writes, 2, 8, NULL) == -1);
    CHECK(errno == EINVAL);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_extend_sparse_batched_out_of_range_errors(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    bstack_iovec_t writes[1] = { { 3, (uint8_t *)"zzz", 3 } };
    errno = 0;
    CHECK(bstack_extend_sparse_batched(bs, writes, 1, 5, NULL) == -1);
    CHECK(errno == EINVAL);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_extend_sparse_persists_across_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"hi", 2, NULL) == 0);
        CHECK(bstack_extend_sparse(bs, (uint8_t *)"Z", 1, 4, NULL) == 0);
        bstack_close(bs);
    }
    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        uint8_t buf[6]; size_t w;
        CHECK(bstack_peek(bs, 0, buf, &w) == 0);
        CHECK(memcmp(buf, "hiZ\x00\x00\x00", 6) == 0);
        bstack_close(bs);
    }

    unlink(tmp);
    return 0;
}

/* =========================================================================
 * bstack_resize
 * ====================================================================== */

static int test_resize_grows_with_zeros(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"abc", 3, NULL) == 0);
    uint64_t initial;
    CHECK(bstack_resize(bs, 6, &initial) == 0);
    CHECK(initial == 3);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 6);
    uint8_t buf[6]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "abc\x00\x00\x00", 6) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_resize_shrinks(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    uint64_t initial;
    CHECK(bstack_resize(bs, 5, &initial) == 0);
    CHECK(initial == 10);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);
    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hello", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_resize_same_size_is_noop(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    uint64_t initial;
    CHECK(bstack_resize(bs, 5, &initial) == 0);
    CHECK(initial == 5);

    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hello", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_resize_to_zero_truncates(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    uint64_t initial;
    CHECK(bstack_resize(bs, 0, &initial) == 0);
    CHECK(initial == 5);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_resize_shrink_below_locked_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 5) == 0);
    CHECK(bstack_resize(bs, 3, NULL) == -1);
    CHECK(errno == EINVAL);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 10);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_resize_persists_across_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"hi", 2, NULL) == 0);
        CHECK(bstack_resize(bs, 4, NULL) == 0);
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

/* =========================================================================
 * bstack_ensure
 * ====================================================================== */

static int test_ensure_grows_short_payload_with_zeros(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"abc", 3, NULL) == 0);
    uint64_t initial;
    CHECK(bstack_ensure(bs, 6, &initial) == 0);
    CHECK(initial == 3);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 6);
    uint8_t buf[6]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "abc\x00\x00\x00", 6) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_ensure_noop_when_already_long_enough(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    uint64_t initial;
    CHECK(bstack_ensure(bs, 5, &initial) == 0);
    CHECK(initial == 10);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 10);
    uint8_t buf[10]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "helloworld", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_ensure_persists_across_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"hi", 2, NULL) == 0);
        CHECK(bstack_ensure(bs, 4, NULL) == 0);
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

#ifdef BSTACK_FEATURE_ATOMIC
/* =========================================================================
 * bstack_ensure_with  (compiled only with -DBSTACK_FEATURE_ATOMIC)
 * ====================================================================== */

static int ensure_with_cb_fill_xyz(uint8_t *buf, size_t len, void *ctx)
{
    (void)ctx;
    if (len != 3) return -1;
    memcpy(buf, "XYZ", 3);
    return 0;
}

static int test_ensure_with_grows_and_calls_callback_on_new_region(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"abc", 3, NULL) == 0);
    uint64_t initial;
    CHECK(bstack_ensure_with(bs, 6, ensure_with_cb_fill_xyz, NULL, &initial) == 0);
    CHECK(initial == 3);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 6);
    uint8_t buf[6]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "abcXYZ", 6) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int ensure_with_cb_mark_called(uint8_t *buf, size_t len, void *ctx)
{
    (void)buf; (void)len;
    *(int *)ctx = 1;
    return 0;
}

static int test_ensure_with_skips_callback_when_already_long_enough(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    int called = 0;
    uint64_t initial;
    CHECK(bstack_ensure_with(bs, 5, ensure_with_cb_mark_called, &called, &initial) == 0);
    CHECK(initial == 10);
    CHECK(called == 0);

    uint8_t buf[10]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "helloworld", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int ensure_with_cb_fill_zzz(uint8_t *buf, size_t len, void *ctx)
{
    (void)ctx;
    if (len != 3) return -1;
    memcpy(buf, "ZZZ", 3);
    return 0;
}

static int test_ensure_with_persists_across_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"hi", 2, NULL) == 0);
        CHECK(bstack_ensure_with(bs, 5, ensure_with_cb_fill_zzz, NULL, NULL) == 0);
        bstack_close(bs);
    }
    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        uint8_t buf[5]; size_t w;
        CHECK(bstack_peek(bs, 0, buf, &w) == 0);
        CHECK(memcmp(buf, "hiZZZ", 5) == 0);
        bstack_close(bs);
    }

    unlink(tmp);
    return 0;
}
#endif /* BSTACK_FEATURE_ATOMIC */

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

/* ---- bstack_try_extend_sparse --------------------------------------------- */

static int test_try_extend_sparse_matching_writes(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    int ok = -1;
    CHECK(bstack_try_extend_sparse(bs, 5, (uint8_t *)"XY", 2, 6, &ok) == 0);
    CHECK(ok == 1);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 11);
    uint8_t buf[11]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "helloXY\x00\x00\x00\x00", 11) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_try_extend_sparse_mismatching_returns_false(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    int ok = -1;
    CHECK(bstack_try_extend_sparse(bs, 3, (uint8_t *)"XY", 2, 6, &ok) == 0);
    CHECK(ok == 0);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_try_extend_sparse_malformed_errors_even_on_mismatch(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    /* Size does not match (3 != 5), but the malformed request still errors. */
    errno = 0;
    CHECK(bstack_try_extend_sparse(bs, 3, (uint8_t *)"toolong", 7, 2, NULL) == -1);
    CHECK(errno == EINVAL);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_try_extend_sparse_batched_matching_scatters(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"..", 2, NULL) == 0);
    bstack_iovec_t writes[2] = {
        { 0, (uint8_t *)"AA", 2 },
        { 5, (uint8_t *)"BB", 2 },
    };
    int ok = -1;
    CHECK(bstack_try_extend_sparse_batched(bs, 2, writes, 2, 8, &ok) == 0);
    CHECK(ok == 1);
    uint8_t buf[10]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "..AA\x00\x00\x00" "BB" "\x00", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_try_extend_sparse_batched_mismatching_returns_false(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"..", 2, NULL) == 0);
    bstack_iovec_t writes[1] = { { 0, (uint8_t *)"AA", 2 } };
    int ok = -1;
    CHECK(bstack_try_extend_sparse_batched(bs, 99, writes, 1, 8, &ok) == 0);
    CHECK(ok == 0);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 2);

    bstack_close(bs); unlink(tmp);
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

/* --- splice journal (length-changing tail replace) --------------------- */

static int test_atrunc_splice_journal_grow_and_reopens_clean(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    /* 400 'A'; replace the last 100 with 200 'B' — a length-changing tail
     * replace (m=200 != n=100, both > 0), so it goes through the splice
     * journal. clen' = 300 + 200 = 500. */
    uint8_t *a = (uint8_t *)malloc(400);
    CHECK(a != NULL); memset(a, 'A', 400);
    CHECK(bstack_push(bs, a, 400, NULL) == 0);
    uint8_t *b = (uint8_t *)malloc(200);
    CHECK(b != NULL); memset(b, 'B', 200);
    CHECK(bstack_atrunc(bs, 100, b, 200) == 0);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 500);
    uint8_t *out = (uint8_t *)malloc(500);
    CHECK(out != NULL);
    CHECK(bstack_peek(bs, 0, out, NULL) == 0);
    for (int i = 0; i < 300; i++) CHECK(out[i] == 'A');
    for (int i = 300; i < 500; i++) CHECK(out[i] == 'B');
    bstack_close(bs);

    int fd = open(tmp, O_RDONLY | O_BINARY);
    struct stat st; fstat(fd, &st);
    CHECK(st.st_size == TEST_HEADER_SIZE + 500);
    CHECK(raw_read_le64(fd, 16) == 0);
    close(fd);
    bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_len(bs, &len) == 0); CHECK(len == 500);
    bstack_close(bs);
    free(a); free(b); free(out);
    unlink(tmp);
    return 0;
}

static int test_atrunc_splice_journal_shrink_and_reopens_clean(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    /* 400 'A'; replace the last 200 with 50 'B' — length-changing shrink
     * (m=50 != n=200). clen' = 200 + 50 = 250. */
    uint8_t *a = (uint8_t *)malloc(400);
    CHECK(a != NULL); memset(a, 'A', 400);
    CHECK(bstack_push(bs, a, 400, NULL) == 0);
    uint8_t b[50]; memset(b, 'B', 50);
    CHECK(bstack_atrunc(bs, 200, b, 50) == 0);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 250);
    uint8_t *out = (uint8_t *)malloc(250);
    CHECK(out != NULL);
    CHECK(bstack_peek(bs, 0, out, NULL) == 0);
    for (int i = 0; i < 200; i++)   CHECK(out[i] == 'A');
    for (int i = 200; i < 250; i++) CHECK(out[i] == 'B');
    bstack_close(bs);

    int fd = open(tmp, O_RDONLY | O_BINARY);
    struct stat st; fstat(fd, &st);
    CHECK(st.st_size == TEST_HEADER_SIZE + 250);
    CHECK(raw_read_le64(fd, 16) == 0);
    close(fd);
    free(a); free(out);
    unlink(tmp);
    return 0;
}

static int test_splice_returns_removed_on_length_change(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    /* Replace last 5 with 3 bytes (length change). */
    uint8_t removed[5];
    CHECK(bstack_splice(bs, removed, 5, (uint8_t *)"XYZ", 3) == 0);
    CHECK(memcmp(removed, "world", 5) == 0);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 8);
    uint8_t buf[8]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, "helloXYZ", 8) == 0);
    bstack_close(bs);
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

/* -------------------------------------------------------------------------
 * process_gen  (require BSTACK_FEATURE_ATOMIC and BSTACK_FEATURE_SET)
 * ---------------------------------------------------------------------- */

static uint64_t pg_le64_get(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v |= (uint64_t)p[i] << (8 * i);
    return v;
}

static void pg_le64_put(uint8_t *p, uint64_t v)
{
    for (int i = 0; i < 8; i++) p[i] = (uint8_t)(v >> (8 * i));
}

struct pg_read_then_write_ctx {
    uint8_t buf[5];
    int     step;
};

static int pg_read_then_write_gen(bstack_gen_op_t *out_op, void *userctx)
{
    struct pg_read_then_write_ctx *ctx = userctx;
    switch (ctx->step++) {
    case 0:
        out_op->kind          = BSTACK_GEN_READ;
        out_op->u.read.offset = 0;
        out_op->u.read.buf    = ctx->buf;
        out_op->u.read.len    = sizeof ctx->buf;
        return 1;
    case 1:
        out_op->kind           = BSTACK_GEN_WRITE;
        out_op->u.write.offset = 5;
        out_op->u.write.data   = ctx->buf;
        out_op->u.write.len    = sizeof ctx->buf;
        return 1;
    default:
        return 0;
    }
}

static int test_process_gen_reads_then_writes(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);

    struct pg_read_then_write_ctx ctx = {{0}, 0};
    CHECK(bstack_process_gen(bs, pg_read_then_write_gen, &ctx) == 0);
    CHECK(memcmp(ctx.buf, "hello", 5) == 0);

    uint8_t buf[10]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hellohello", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

struct pg_dependent_ctx {
    uint8_t ptr_buf[8];
    uint8_t node_buf[2];
    int     step;
};

static int pg_dependent_gen(bstack_gen_op_t *out_op, void *userctx)
{
    struct pg_dependent_ctx *ctx = userctx;
    switch (ctx->step++) {
    case 0:
        out_op->kind          = BSTACK_GEN_READ;
        out_op->u.read.offset = 0;
        out_op->u.read.buf    = ctx->ptr_buf;
        out_op->u.read.len    = sizeof ctx->ptr_buf;
        return 1;
    case 1:
        /* The previous read has already filled ptr_buf by the time we're
         * called again. */
        out_op->kind          = BSTACK_GEN_READ;
        out_op->u.read.offset = pg_le64_get(ctx->ptr_buf);
        out_op->u.read.buf    = ctx->node_buf;
        out_op->u.read.len    = sizeof ctx->node_buf;
        return 1;
    default:
        return 0;
    }
}

static int test_process_gen_dependent_reads_inform_next_offset(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    /* Layout: [pointer: u64 LE][node "A "][node "B "] */
    uint8_t payload[12];
    pg_le64_put(payload, 8);
    memcpy(payload + 8, "A ", 2);
    memcpy(payload + 10, "B ", 2);
    CHECK(bstack_push(bs, payload, sizeof payload, NULL) == 0);

    struct pg_dependent_ctx ctx = {{0}, {0}, 0};
    CHECK(bstack_process_gen(bs, pg_dependent_gen, &ctx) == 0);
    CHECK(pg_le64_get(ctx.ptr_buf) == 8);
    CHECK(memcmp(ctx.node_buf, "A ", 2) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int pg_immediate_none_gen(bstack_gen_op_t *out_op, void *userctx)
{
    (void)out_op; (void)userctx;
    return 0;
}

static int test_process_gen_immediate_none_is_noop(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    CHECK(bstack_process_gen(bs, pg_immediate_none_gen, NULL) == 0);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);
    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hello", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int pg_write_ends_sequence_gen(bstack_gen_op_t *out_op, void *userctx)
{
    int *calls = userctx;
    (*calls)++;
    if (*calls == 1) {
        out_op->kind           = BSTACK_GEN_WRITE;
        out_op->u.write.offset = 0;
        out_op->u.write.data   = (const uint8_t *)"HELLO";
        out_op->u.write.len    = 5;
    } else {
        out_op->kind           = BSTACK_GEN_WRITE;
        out_op->u.write.offset = 5;
        out_op->u.write.data   = (const uint8_t *)"WORLD";
        out_op->u.write.len    = 5;
    }
    return 1;
}

static int test_process_gen_write_ends_sequence(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    int calls = 0;
    CHECK(bstack_process_gen(bs, pg_write_ends_sequence_gen, &calls) == 0);
    CHECK(calls == 1);

    uint8_t buf[10]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "HELLOworld", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int pg_swap_ends_sequence_gen(bstack_gen_op_t *out_op, void *userctx)
{
    int *calls = userctx;
    (*calls)++;
    if (*calls == 1) {
        out_op->kind           = BSTACK_GEN_SWAP;
        out_op->u.swap.a_offset = 0;
        out_op->u.swap.b_offset = 5;
        out_op->u.swap.len      = 5;
    } else {
        out_op->kind           = BSTACK_GEN_WRITE;
        out_op->u.write.offset = 0;
        out_op->u.write.data   = (const uint8_t *)"NOPE!";
        out_op->u.write.len    = 5;
    }
    return 1;
}

static int test_process_gen_swap_exchanges_two_regions_and_ends_sequence(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    int calls = 0;
    CHECK(bstack_process_gen(bs, pg_swap_ends_sequence_gen, &calls) == 0);
    CHECK(calls == 1);

    uint8_t buf[10]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "worldhello", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

struct pg_swap_informed_ctx {
    uint8_t ptr_buf[8];
    int     step;
};

static int pg_swap_informed_gen(bstack_gen_op_t *out_op, void *userctx)
{
    struct pg_swap_informed_ctx *ctx = userctx;
    switch (ctx->step++) {
    case 0:
        out_op->kind          = BSTACK_GEN_READ;
        out_op->u.read.offset = 0;
        out_op->u.read.buf    = ctx->ptr_buf;
        out_op->u.read.len    = sizeof ctx->ptr_buf;
        return 1;
    case 1:
        out_op->kind            = BSTACK_GEN_SWAP;
        out_op->u.swap.a_offset = 8;
        out_op->u.swap.b_offset = pg_le64_get(ctx->ptr_buf);
        out_op->u.swap.len      = 8;
        return 1;
    default:
        return 0;
    }
}

static int test_process_gen_swap_target_informed_by_prior_read(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    /* Layout: [pointer: u64 LE][block A: 8 bytes][block B: 8 bytes] */
    uint8_t payload[24];
    pg_le64_put(payload, 16); /* names block B */
    memcpy(payload + 8,  "AAAAAAAA", 8);
    memcpy(payload + 16, "BBBBBBBB", 8);
    CHECK(bstack_push(bs, payload, sizeof payload, NULL) == 0);

    struct pg_swap_informed_ctx ctx = {{0}, 0};
    CHECK(bstack_process_gen(bs, pg_swap_informed_gen, &ctx) == 0);

    uint8_t buf[16]; size_t w;
    CHECK(bstack_peek(bs, 8, buf, &w) == 0);
    CHECK(memcmp(buf, "BBBBBBBBAAAAAAAA", 16) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int pg_swap_overlapping_gen(bstack_gen_op_t *out_op, void *userctx)
{
    (void)userctx;
    out_op->kind            = BSTACK_GEN_SWAP;
    out_op->u.swap.a_offset = 0;
    out_op->u.swap.b_offset = 3;
    out_op->u.swap.len      = 5;
    return 1;
}

static int test_process_gen_swap_overlapping_regions_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_process_gen(bs, pg_swap_overlapping_gen, NULL) == -1);
    CHECK(errno == EINVAL);

    uint8_t buf[10]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "helloworld", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int pg_swap_locked_gen(bstack_gen_op_t *out_op, void *userctx)
{
    (void)userctx;
    out_op->kind            = BSTACK_GEN_SWAP;
    out_op->u.swap.a_offset = 0;
    out_op->u.swap.b_offset = 5;
    out_op->u.swap.len      = 5;
    return 1;
}

static int test_process_gen_swap_in_locked_region_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 5) == 0);
    CHECK(bstack_process_gen(bs, pg_swap_locked_gen, NULL) == -1);
    CHECK(errno == EINVAL);

    uint8_t buf[10];
    CHECK(bstack_get(bs, 0, 10, buf) == 0);
    CHECK(memcmp(buf, "helloworld", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int pg_write_gen(bstack_gen_op_t *out_op, void *userctx)
{
    (void)userctx;
    out_op->kind           = BSTACK_GEN_WRITE;
    out_op->u.write.offset = 0;
    out_op->u.write.data   = (const uint8_t *)"HELLO";
    out_op->u.write.len    = 5;
    return 1;
}

static int test_process_gen_does_not_change_file_size(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_process_gen(bs, pg_write_gen, NULL) == 0);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 10);

    bstack_close(bs); unlink(tmp);
    return 0;
}

struct pg_read_oob_ctx {
    uint8_t buf[10];
    int     called;
};

static int pg_read_oob_gen(bstack_gen_op_t *out_op, void *userctx)
{
    struct pg_read_oob_ctx *ctx = userctx;
    if (ctx->called)
        return 0;
    ctx->called = 1;
    out_op->kind          = BSTACK_GEN_READ;
    out_op->u.read.offset = 0;
    out_op->u.read.buf    = ctx->buf;
    out_op->u.read.len    = sizeof ctx->buf;
    return 1;
}

static int test_process_gen_read_out_of_bounds_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hi", 2, NULL) == 0);
    struct pg_read_oob_ctx ctx = {{0}, 0};
    CHECK(bstack_process_gen(bs, pg_read_oob_gen, &ctx) == -1);
    CHECK(errno == EINVAL);

    uint8_t buf[2]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hi", 2) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int pg_write_oob_gen(bstack_gen_op_t *out_op, void *userctx)
{
    (void)userctx;
    out_op->kind           = BSTACK_GEN_WRITE;
    out_op->u.write.offset = 2;
    out_op->u.write.data   = (const uint8_t *)"abcdefgh";
    out_op->u.write.len    = 8;
    return 1;
}

static int test_process_gen_write_out_of_bounds_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    CHECK(bstack_process_gen(bs, pg_write_oob_gen, NULL) == -1);
    CHECK(errno == EINVAL);

    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hello", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_process_gen_write_in_locked_region_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 5) == 0);
    CHECK(bstack_process_gen(bs, pg_write_gen, NULL) == -1);
    CHECK(errno == EINVAL);

    uint8_t buf[10];
    CHECK(bstack_get(bs, 0, 10, buf) == 0);
    CHECK(memcmp(buf, "helloworld", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

struct pg_read_locked_ctx {
    uint8_t buf[5];
    int     called;
};

static int pg_read_locked_gen(bstack_gen_op_t *out_op, void *userctx)
{
    struct pg_read_locked_ctx *ctx = userctx;
    if (ctx->called)
        return 0;
    ctx->called = 1;
    out_op->kind          = BSTACK_GEN_READ;
    out_op->u.read.offset = 0;
    out_op->u.read.buf    = ctx->buf;
    out_op->u.read.len    = sizeof ctx->buf;
    return 1;
}

static int test_process_gen_read_in_locked_region_succeeds(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 5) == 0);

    struct pg_read_locked_ctx ctx = {{0}, 0};
    CHECK(bstack_process_gen(bs, pg_read_locked_gen, &ctx) == 0);
    CHECK(memcmp(ctx.buf, "hello", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int pg_write_world_gen(bstack_gen_op_t *out_op, void *userctx)
{
    (void)userctx;
    out_op->kind           = BSTACK_GEN_WRITE;
    out_op->u.write.offset = 5;
    out_op->u.write.data   = (const uint8_t *)"WORLD";
    out_op->u.write.len    = 5;
    return 1;
}

static int test_process_gen_persists_across_reopen(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);

    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
        CHECK(bstack_process_gen(bs, pg_write_world_gen, NULL) == 0);
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

static int pg_push_ends_sequence_gen(bstack_gen_op_t *out_op, void *userctx)
{
    int *calls = userctx;
    (*calls)++;
    if (*calls == 1) {
        out_op->kind        = BSTACK_GEN_PUSH;
        out_op->u.push.data = (const uint8_t *)"world";
        out_op->u.push.len  = 5;
    } else {
        out_op->kind        = BSTACK_GEN_PUSH;
        out_op->u.push.data = (const uint8_t *)"NOPE!";
        out_op->u.push.len  = 5;
    }
    return 1;
}

static int test_process_gen_push_appends_and_ends_sequence(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    int calls = 0;
    CHECK(bstack_process_gen(bs, pg_push_ends_sequence_gen, &calls) == 0);
    CHECK(calls == 1);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 10);
    uint8_t buf[10]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "helloworld", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int pg_push_empty_gen(bstack_gen_op_t *out_op, void *userctx)
{
    (void)userctx;
    out_op->kind        = BSTACK_GEN_PUSH;
    out_op->u.push.data = (const uint8_t *)"";
    out_op->u.push.len  = 0;
    return 1;
}

static int test_process_gen_push_empty_data_is_noop_and_ends_sequence(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    CHECK(bstack_process_gen(bs, pg_push_empty_gen, NULL) == 0);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);
    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hello", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

struct pg_pop_ctx {
    uint8_t buf[5];
    int     calls;
};

static int pg_pop_ends_sequence_gen(bstack_gen_op_t *out_op, void *userctx)
{
    struct pg_pop_ctx *ctx = userctx;
    ctx->calls++;
    if (ctx->calls == 1) {
        out_op->kind       = BSTACK_GEN_POP;
        out_op->u.pop.buf  = ctx->buf;
        out_op->u.pop.len  = sizeof ctx->buf;
    } else {
        out_op->kind           = BSTACK_GEN_WRITE;
        out_op->u.write.offset = 0;
        out_op->u.write.data   = (const uint8_t *)"NOPE!";
        out_op->u.write.len    = 5;
    }
    return 1;
}

static int test_process_gen_pop_removes_and_ends_sequence(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    struct pg_pop_ctx ctx = {{0}, 0};
    CHECK(bstack_process_gen(bs, pg_pop_ends_sequence_gen, &ctx) == 0);
    CHECK(ctx.calls == 1);
    CHECK(memcmp(ctx.buf, "world", 5) == 0);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);
    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hello", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int pg_pop_null_buf_gen(bstack_gen_op_t *out_op, void *userctx)
{
    int *calls = userctx;
    (*calls)++;
    if (*calls == 1) {
        /* NULL destination: discard the tail without copying it out. */
        out_op->kind      = BSTACK_GEN_POP;
        out_op->u.pop.buf = NULL;
        out_op->u.pop.len = 5;
    } else {
        out_op->kind           = BSTACK_GEN_WRITE;
        out_op->u.write.offset = 0;
        out_op->u.write.data   = (const uint8_t *)"NOPE!";
        out_op->u.write.len    = 5;
    }
    return 1;
}

static int test_process_gen_pop_null_buf_discards_and_ends_sequence(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    int calls = 0;
    CHECK(bstack_process_gen(bs, pg_pop_null_buf_gen, &calls) == 0);
    CHECK(calls == 1);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);
    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hello", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int pg_pop_zero_gen(bstack_gen_op_t *out_op, void *userctx)
{
    (void)userctx;
    out_op->kind      = BSTACK_GEN_POP;
    out_op->u.pop.buf = NULL;
    out_op->u.pop.len = 0;
    return 1;
}

static int test_process_gen_pop_zero_is_noop_and_ends_sequence(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    CHECK(bstack_process_gen(bs, pg_pop_zero_gen, NULL) == 0);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 5);
    uint8_t buf[5]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "hello", 5) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int pg_pop_oob_gen(bstack_gen_op_t *out_op, void *userctx)
{
    uint8_t *buf = userctx;
    out_op->kind      = BSTACK_GEN_POP;
    out_op->u.pop.buf = buf;
    out_op->u.pop.len = 10;
    return 1;
}

static int test_process_gen_pop_exceeds_payload_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hi", 2, NULL) == 0);
    uint8_t buf[10];
    CHECK(bstack_process_gen(bs, pg_pop_oob_gen, buf) == -1);
    CHECK(errno == EINVAL);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 2);
    uint8_t pbuf[2]; size_t w;
    CHECK(bstack_peek(bs, 0, pbuf, &w) == 0);
    CHECK(memcmp(pbuf, "hi", 2) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int pg_pop_locked_gen(bstack_gen_op_t *out_op, void *userctx)
{
    uint8_t *buf = userctx;
    out_op->kind      = BSTACK_GEN_POP;
    out_op->u.pop.buf = buf;
    out_op->u.pop.len = 5;
    return 1;
}

static int test_process_gen_pop_below_locked_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 8) == 0);
    uint8_t buf[5];
    CHECK(bstack_process_gen(bs, pg_pop_locked_gen, buf) == -1);
    CHECK(errno == EINVAL);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 10);
    uint8_t pbuf[10];
    CHECK(bstack_get(bs, 0, 10, pbuf) == 0);
    CHECK(memcmp(pbuf, "helloworld", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

struct pg_len_ctx {
    uint64_t size;
    int      calls;
};

static int pg_len_gen(bstack_gen_op_t *out_op, void *userctx)
{
    struct pg_len_ctx *ctx = userctx;
    ctx->calls++;
    if (ctx->calls == 1) {
        out_op->kind      = BSTACK_GEN_LEN;
        out_op->u.len.out = &ctx->size;
        return 1;
    }
    return 0;
}

static int test_process_gen_len_reports_current_size_and_continues(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    struct pg_len_ctx ctx = {0, 0};
    CHECK(bstack_process_gen(bs, pg_len_gen, &ctx) == 0);
    CHECK(ctx.calls == 2);
    CHECK(ctx.size == 10);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 10);
    uint8_t buf[10]; size_t w;
    CHECK(bstack_peek(bs, 0, buf, &w) == 0);
    CHECK(memcmp(buf, "helloworld", 10) == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

struct pg_len_pop_ctx {
    uint64_t size;
    uint8_t  buf[5];
    int      step;
};

static int pg_len_informs_pop_gen(bstack_gen_op_t *out_op, void *userctx)
{
    struct pg_len_pop_ctx *ctx = userctx;
    switch (ctx->step++) {
    case 0:
        out_op->kind      = BSTACK_GEN_LEN;
        out_op->u.len.out = &ctx->size;
        return 1;
    case 1:
        out_op->kind      = BSTACK_GEN_POP;
        out_op->u.pop.buf = ctx->buf;
        out_op->u.pop.len = (size_t)(ctx->size - 8);
        return 1;
    default:
        return 0;
    }
}

static int test_process_gen_len_informs_pop_size(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    /* Layout: [count: u64 LE]["world"] — pop the trailing "world" whose
     * length is only known once BSTACK_GEN_LEN has reported the current
     * size. */
    uint8_t payload[13];
    pg_le64_put(payload, 8);
    memcpy(payload + 8, "world", 5);
    CHECK(bstack_push(bs, payload, sizeof payload, NULL) == 0);

    struct pg_len_pop_ctx ctx = {0, {0}, 0};
    CHECK(bstack_process_gen(bs, pg_len_informs_pop_gen, &ctx) == 0);
    CHECK(ctx.size == 13);
    CHECK(memcmp(ctx.buf, "world", 5) == 0);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 8);

    bstack_close(bs); unlink(tmp);
    return 0;
}

/* --- copy: disjoint copy journal --------------------------------------- */

static int test_copy_disjoint_journals_and_reopens_clean(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    /* 800-byte payload; copy 300 disjoint bytes [0,300) -> [400,700). 300 > one
     * aligned block, so it takes the copy-journal path (not the atomic write). */
    uint8_t *data = (uint8_t *)malloc(800);
    CHECK(data != NULL);
    memset(data, 'S', 300);
    memset(data + 300, '.', 500);
    CHECK(bstack_push(bs, data, 800, NULL) == 0);
    CHECK(bstack_copy(bs, 0, 400, 300) == 0);

    uint8_t *out = (uint8_t *)malloc(800);
    CHECK(out != NULL);
    CHECK(bstack_peek(bs, 0, out, NULL) == 0);
    for (int i = 400; i < 700; i++) CHECK(out[i] == 'S'); /* copy landed */
    for (int i = 0; i < 300; i++)   CHECK(out[i] == 'S'); /* source unchanged */
    bstack_close(bs);

    int fd = open(tmp, O_RDONLY | O_BINARY);
    struct stat st; fstat(fd, &st);
    CHECK(st.st_size == TEST_HEADER_SIZE + 800); /* staging tail truncated */
    CHECK(raw_read_le64(fd, 16) == 0);           /* wip disarmed */
    close(fd);
    bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_peek(bs, 0, out, NULL) == 0);
    for (int i = 400; i < 700; i++) CHECK(out[i] == 'S');
    bstack_close(bs);
    free(data); free(out);
    unlink(tmp);
    return 0;
}

static int test_copy_same_location_is_noop(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    uint8_t *data = (uint8_t *)malloc(400);
    CHECK(data != NULL); memset(data, 'Z', 400);
    CHECK(bstack_push(bs, data, 400, NULL) == 0);
    /* from == to with a block-spanning length: short-circuits, no journal. */
    CHECK(bstack_copy(bs, 50, 50, 300) == 0);
    uint8_t *out = (uint8_t *)malloc(400);
    CHECK(out != NULL);
    CHECK(bstack_peek(bs, 0, out, NULL) == 0);
    for (int i = 0; i < 400; i++) CHECK(out[i] == 'Z');
    bstack_close(bs);
    free(data); free(out);
    unlink(tmp);
    return 0;
}

/* --- process_gen: BSTACK_GEN_SPLICE ------------------------------------ */

struct pg_splice_ctx { uint8_t removed[5]; int calls; };

static int pg_splice_gen(bstack_gen_op_t *out_op, void *userctx)
{
    struct pg_splice_ctx *c = userctx;
    c->calls++;
    if (c->calls == 1) {
        out_op->kind              = BSTACK_GEN_SPLICE;
        out_op->u.splice.removed  = c->removed;   /* read popped bytes back */
        out_op->u.splice.n        = 5;
        out_op->u.splice.new_buf  = (const uint8_t *)"THERE!";
        out_op->u.splice.new_len  = 6;
    } else {
        out_op->kind           = BSTACK_GEN_WRITE;
        out_op->u.write.offset = 0;
        out_op->u.write.data   = (const uint8_t *)"NOPE!";
        out_op->u.write.len    = 5;
    }
    return 1;
}

static int test_process_gen_splice_replaces_tail_and_ends_sequence(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);

    struct pg_splice_ctx ctx = {{0}, 0};
    CHECK(bstack_process_gen(bs, pg_splice_gen, &ctx) == 0);
    CHECK(ctx.calls == 1); /* Splice ends the sequence */
    CHECK(memcmp(ctx.removed, "world", 5) == 0);

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 11);
    uint8_t buf[11]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, "helloTHERE!", 11) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int pg_splice_atrunc_gen(bstack_gen_op_t *out_op, void *userctx)
{
    (void)userctx;
    /* NULL removed = the atrunc form (discard the popped bytes). */
    out_op->kind              = BSTACK_GEN_SPLICE;
    out_op->u.splice.removed  = NULL;
    out_op->u.splice.n        = 5;
    out_op->u.splice.new_buf  = (const uint8_t *)"THERE!";
    out_op->u.splice.new_len  = 6;
    return 1;
}

static int test_process_gen_splice_null_removed_acts_like_atrunc(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_process_gen(bs, pg_splice_atrunc_gen, NULL) == 0);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 11);
    uint8_t buf[11]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, "helloTHERE!", 11) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

/* --- process_gen: BSTACK_GEN_SPARSE ------------------------------------ */

static const bstack_iovec_t pg_sparse_writes[2] = {
    { 0, (uint8_t *)"AA", 2 },
    { 5, (uint8_t *)"BB", 2 },
};

static int pg_sparse_gen(bstack_gen_op_t *out_op, void *userctx)
{
    int *calls = userctx;
    (*calls)++;
    if (*calls == 1) {
        out_op->kind            = BSTACK_GEN_SPARSE;
        out_op->u.sparse.writes = pg_sparse_writes;
        out_op->u.sparse.count  = 2;
        out_op->u.sparse.length = 8;
    } else {
        out_op->kind           = BSTACK_GEN_WRITE;
        out_op->u.write.offset = 0;
        out_op->u.write.data   = (const uint8_t *)"NOPE";
        out_op->u.write.len    = 4;
    }
    return 1;
}

static int test_process_gen_sparse_scatters_and_ends_sequence(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"..", 2, NULL) == 0);

    int calls = 0;
    CHECK(bstack_process_gen(bs, pg_sparse_gen, &calls) == 0);
    CHECK(calls == 1); /* Sparse ends the sequence */

    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 10);
    uint8_t buf[10]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, "..AA\x00\x00\x00" "BB" "\x00", 10) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static const bstack_iovec_t pg_sparse_overlap_writes[2] = {
    { 0, (uint8_t *)"aaa", 3 },
    { 2, (uint8_t *)"bb", 2 },
};

static int pg_sparse_overlap_gen(bstack_gen_op_t *out_op, void *userctx)
{
    (void)userctx;
    out_op->kind            = BSTACK_GEN_SPARSE;
    out_op->u.sparse.writes = pg_sparse_overlap_writes;
    out_op->u.sparse.count  = 2;
    out_op->u.sparse.length = 8;
    return 1;
}

static int test_process_gen_sparse_overlap_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"hi", 2, NULL) == 0);

    errno = 0;
    CHECK(bstack_process_gen(bs, pg_sparse_overlap_gen, NULL) == -1);
    CHECK(errno == EINVAL);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 2);
    uint8_t buf[2]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, "hi", 2) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

/* -------------------------------------------------------------------------
 * set_batched / inplace_gen  (multi-write journal)
 * ---------------------------------------------------------------------- */

static int test_set_batched_commits_all_writes_and_reopens_clean(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    uint8_t init[500]; memset(init, '.', sizeof init);
    CHECK(bstack_push(bs, init, sizeof init, NULL) == 0);

    uint8_t x[100], y[100], z[100];
    memset(x, 'X', 100); memset(y, 'Y', 100); memset(z, 'Z', 100);
    bstack_iovec_t writes[3] = {
        { 0,   x, 100 },
        { 400, z, 100 },
        { 200, y, 100 },
    };
    CHECK(bstack_set_batched(bs, writes, 3) == 0);

    uint8_t expect[500];
    memset(expect, '.', 500);
    memset(expect + 0,   'X', 100);
    memset(expect + 200, 'Y', 100);
    memset(expect + 400, 'Z', 100);
    uint8_t buf[500];
    CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, expect, 500) == 0);
    bstack_close(bs);

    /* Staging tail dropped, journal disarmed, value survives reopen. */
    int fd = open(tmp, O_RDONLY | O_BINARY);
    struct stat st; fstat(fd, &st);
    CHECK(st.st_size == TEST_HEADER_SIZE + 500);
    CHECK(raw_read_le64(fd, 16) == 0);   /* wip_ptr disarmed */
    CHECK(raw_read_le64(fd, 24) == 0);   /* wip_aux disarmed */
    close(fd);

    bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, expect, 500) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_set_batched_rejects_overlap(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    uint8_t init[200]; memset(init, '.', sizeof init);
    CHECK(bstack_push(bs, init, sizeof init, NULL) == 0);

    uint8_t a[100], b[100];
    memset(a, 'a', 100); memset(b, 'b', 100);
    bstack_iovec_t writes[2] = { { 0, a, 100 }, { 50, b, 100 } };
    errno = 0;
    CHECK(bstack_set_batched(bs, writes, 2) == -1);
    CHECK(errno == EINVAL);

    /* File untouched. */
    uint8_t buf[200];
    CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, init, 200) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_set_batched_empty_single_and_out_of_range(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    uint8_t init[100]; memset(init, '.', sizeof init);
    CHECK(bstack_push(bs, init, sizeof init, NULL) == 0);

    /* Empty batch: no-op. */
    CHECK(bstack_set_batched(bs, NULL, 0) == 0);

    /* Empty-data entries dropped, leaving a lone effective write. */
    uint8_t q[5]; memset(q, 'q', 5);
    bstack_iovec_t mixed[2] = { { 0, NULL, 0 }, { 10, q, 5 } };
    CHECK(bstack_set_batched(bs, mixed, 2) == 0);
    uint8_t buf[5]; CHECK(bstack_peek(bs, 10, buf, NULL) == 0);
    CHECK(memcmp(buf, "qqqqq", 5) == 0);

    /* Out-of-range write rejected. */
    uint8_t z[20]; memset(z, 'z', 20);
    bstack_iovec_t oob[1] = { { 90, z, 20 } };
    errno = 0;
    CHECK(bstack_set_batched(bs, oob, 1) == -1);
    CHECK(errno == EINVAL);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_recovery_replays_armed_multi_write(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    /* clen=300; two staged blocks [0,100)<-'A' and [200,300)<-'B'. */
    uint8_t payload[300 + 16 + 100 + 16 + 100];
    size_t p = 0;
    memset(payload, '.', 300); p = 300;
    /* block 1: s=0, e=100, data 'A'x100 */
    for (int i = 0; i < 8; i++) payload[p + i]     = (uint8_t)((uint64_t)0   >> (8 * i));
    for (int i = 0; i < 8; i++) payload[p + 8 + i] = (uint8_t)((uint64_t)100 >> (8 * i));
    p += 16; memset(payload + p, 'A', 100); p += 100;
    /* block 2: s=200, e=300, data 'B'x100 */
    for (int i = 0; i < 8; i++) payload[p + i]     = (uint8_t)((uint64_t)200 >> (8 * i));
    for (int i = 0; i < 8; i++) payload[p + 8 + i] = (uint8_t)((uint64_t)300 >> (8 * i));
    p += 16; memset(payload + p, 'B', 100); p += 100;
    CHECK(write_wip_file(tmp, 300, 0, WIP_MULTI, payload, p) == 0);

    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 300);
    uint8_t expect[300];
    memset(expect, '.', 300);
    memset(expect + 0,   'A', 100);
    memset(expect + 200, 'B', 100);
    uint8_t buf[300]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, expect, 300) == 0);
    bstack_close(bs);

    int fd = open(tmp, O_RDONLY | O_BINARY);
    struct stat st; fstat(fd, &st);
    CHECK(st.st_size == TEST_HEADER_SIZE + 300);   /* tail truncated */
    CHECK(raw_read_le64(fd, 24) == 0);             /* wip_aux cleared */
    close(fd); unlink(tmp);
    return 0;
}

static int test_recovery_rolls_back_corrupt_multi_write_tail(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    /* First block valid; second names end 350 > clen 300 -> whole tail corrupt,
     * so nothing is applied. */
    uint8_t payload[300 + 16 + 100 + 16 + 100];
    size_t p = 0;
    memset(payload, '.', 300); p = 300;
    for (int i = 0; i < 8; i++) payload[p + i]     = (uint8_t)((uint64_t)0   >> (8 * i));
    for (int i = 0; i < 8; i++) payload[p + 8 + i] = (uint8_t)((uint64_t)100 >> (8 * i));
    p += 16; memset(payload + p, 'A', 100); p += 100;
    for (int i = 0; i < 8; i++) payload[p + i]     = (uint8_t)((uint64_t)250 >> (8 * i));
    for (int i = 0; i < 8; i++) payload[p + 8 + i] = (uint8_t)((uint64_t)350 >> (8 * i));
    p += 16; memset(payload + p, 'B', 100); p += 100;
    CHECK(write_wip_file(tmp, 300, 0, WIP_MULTI, payload, p) == 0);

    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    uint8_t all_dots[300]; memset(all_dots, '.', 300);
    uint8_t buf[300]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, all_dots, 300) == 0);   /* rolled back, nothing applied */
    bstack_close(bs);

    int fd = open(tmp, O_RDONLY | O_BINARY);
    struct stat st; fstat(fd, &st);
    CHECK(st.st_size == TEST_HEADER_SIZE + 300);
    CHECK(raw_read_le64(fd, 24) == 0);
    close(fd); unlink(tmp);
    return 0;
}

/* ---- inplace_gen ---- */

struct ig_ctx {
    int      step;
    int      prev_status;   /* bstack writes the previous op's status here */
    uint8_t *src;
    uint8_t *rbuf;
};

static int ig_reads_see_pending_gen(bstack_gen_op_t *out_op, void *userctx)
{
    struct ig_ctx *c = userctx;
    switch (c->step++) {
    case 0:
        out_op->kind = BSTACK_GEN_WRITE;
        out_op->u.write.offset = 0;
        out_op->u.write.data   = c->src;   /* "ABCDE" */
        out_op->u.write.len    = 5;
        return 1;
    case 1:
        out_op->kind = BSTACK_GEN_READ;
        out_op->u.read.offset = 0;
        out_op->u.read.buf    = c->rbuf;
        out_op->u.read.len    = 10;
        return 1;
    default:
        return 0;
    }
}

static int test_inplace_gen_reads_see_pending_writes(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);

    uint8_t src[5]; memcpy(src, "ABCDE", 5);
    uint8_t rbuf[10] = {0};
    struct ig_ctx c = { 0, 0, src, rbuf };
    CHECK(bstack_inplace_gen(bs, ig_reads_see_pending_gen, &c, &c.prev_status) == 0);
    /* Read observed batch-so-far: "ABCDE" overlaid on "hello". */
    CHECK(memcmp(rbuf, "ABCDEworld", 10) == 0);
    uint8_t buf[10]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, "ABCDEworld", 10) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

struct ig_two_ctx { int step; uint8_t *first; uint8_t *second; };

static int ig_overlap_gen(bstack_gen_op_t *out_op, void *userctx)
{
    struct ig_two_ctx *c = userctx;
    switch (c->step++) {
    case 0:
        out_op->kind = BSTACK_GEN_WRITE;
        out_op->u.write.offset = 0;
        out_op->u.write.data   = c->first;   /* '1'x6 -> [0,6) */
        out_op->u.write.len    = 6;
        return 1;
    case 1:
        out_op->kind = BSTACK_GEN_WRITE;
        out_op->u.write.offset = 3;
        out_op->u.write.data   = c->second;  /* '2'x6 -> [3,9) */
        out_op->u.write.len    = 6;
        return 1;
    default:
        return 0;
    }
}

static int test_inplace_gen_later_write_overrides_overlap(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    uint8_t init[10]; memset(init, '.', 10);
    CHECK(bstack_push(bs, init, 10, NULL) == 0);

    uint8_t first[6], second[6];
    memset(first, '1', 6); memset(second, '2', 6);
    struct ig_two_ctx c = { 0, first, second };
    CHECK(bstack_inplace_gen(bs, ig_overlap_gen, &c, NULL) == 0);
    /* [0,3)='1', [3,9)='2', [9,10)='.' */
    uint8_t buf[10]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, "111222222.", 10) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

struct ig_enc_ctx { int step; uint8_t *e1; uint8_t *e2; uint8_t *e3; uint8_t *e4; };

static int ig_enclosure_gen(bstack_gen_op_t *out_op, void *userctx)
{
    struct ig_enc_ctx *c = userctx;
    out_op->kind = BSTACK_GEN_WRITE;
    switch (c->step++) {
    case 0: out_op->u.write.offset = 4;  out_op->u.write.data = c->e1; out_op->u.write.len = 8;  return 1;
    case 1: out_op->u.write.offset = 14; out_op->u.write.data = c->e2; out_op->u.write.len = 4;  return 1;
    case 2: out_op->u.write.offset = 6;  out_op->u.write.data = c->e3; out_op->u.write.len = 2;  return 1;
    case 3: out_op->u.write.offset = 2;  out_op->u.write.data = c->e4; out_op->u.write.len = 14; return 1;
    default: return 0;
    }
}

static int test_inplace_gen_overlay_enclosure_and_gaps(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    uint8_t init[20]; memset(init, '.', 20);
    CHECK(bstack_push(bs, init, 20, NULL) == 0);

    uint8_t e1[8], e2[4], e3[2], e4[14];
    memset(e1, '1', 8); memset(e2, '2', 4); memset(e3, '3', 2); memset(e4, '4', 14);
    struct ig_enc_ctx c = { 0, e1, e2, e3, e4 };
    CHECK(bstack_inplace_gen(bs, ig_enclosure_gen, &c, NULL) == 0);
    /* [0,2)='.', [2,16)='4', [16,18)='2', [18,20)='.' */
    uint8_t expect[20];
    memset(expect, '.', 20);
    memset(expect + 2, '4', 14);
    memset(expect + 16, '2', 2);
    uint8_t buf[20]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, expect, 20) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

struct ig_cover_ctx { int step; uint8_t *a; uint8_t *b; uint8_t *rbuf; };

static int ig_fully_covered_gen(bstack_gen_op_t *out_op, void *userctx)
{
    struct ig_cover_ctx *c = userctx;
    switch (c->step++) {
    case 0:
        out_op->kind = BSTACK_GEN_WRITE;
        out_op->u.write.offset = 0; out_op->u.write.data = c->a; out_op->u.write.len = 4;
        return 1;
    case 1:
        out_op->kind = BSTACK_GEN_WRITE;
        out_op->u.write.offset = 4; out_op->u.write.data = c->b; out_op->u.write.len = 4;
        return 1;
    case 2:
        out_op->kind = BSTACK_GEN_READ;
        out_op->u.read.offset = 2; out_op->u.read.buf = c->rbuf; out_op->u.read.len = 6;
        return 1;
    default:
        return 0;
    }
}

static int test_inplace_gen_read_fully_covered_by_overlay(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    uint8_t init[12]; memset(init, '.', 12);
    CHECK(bstack_push(bs, init, 12, NULL) == 0);

    uint8_t a[4], b[4]; memset(a, 'A', 4); memset(b, 'B', 4);
    uint8_t rbuf[6] = {0};
    struct ig_cover_ctx c = { 0, a, b, rbuf };
    CHECK(bstack_inplace_gen(bs, ig_fully_covered_gen, &c, NULL) == 0);
    /* [2,4)='A', [4,8)='B' */
    CHECK(memcmp(rbuf, "AABBBB", 6) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

struct ig_span_ctx { int step; uint8_t *a; uint8_t *b; uint8_t *cc; uint8_t *rbuf; };

static int ig_span_gen(bstack_gen_op_t *out_op, void *userctx)
{
    struct ig_span_ctx *c = userctx;
    switch (c->step++) {
    case 0:
        out_op->kind = BSTACK_GEN_WRITE;
        out_op->u.write.offset = 2;  out_op->u.write.data = c->a;  out_op->u.write.len = 4;
        return 1;
    case 1:
        out_op->kind = BSTACK_GEN_WRITE;
        out_op->u.write.offset = 10; out_op->u.write.data = c->b;  out_op->u.write.len = 4;
        return 1;
    case 2:
        out_op->kind = BSTACK_GEN_WRITE;
        out_op->u.write.offset = 22; out_op->u.write.data = c->cc; out_op->u.write.len = 4;
        return 1;
    case 3:
        out_op->kind = BSTACK_GEN_READ;
        out_op->u.read.offset = 4;  out_op->u.read.buf = c->rbuf; out_op->u.read.len = 20;
        return 1;
    default:
        return 0;
    }
}

static int test_inplace_gen_read_spans_multiple_edits_and_gaps(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    uint8_t init[30]; memset(init, '.', 30);
    CHECK(bstack_push(bs, init, 30, NULL) == 0);

    uint8_t a[4], b[4], cc[4];
    memset(a, 'A', 4); memset(b, 'B', 4); memset(cc, 'C', 4);
    uint8_t rbuf[20] = {0};
    struct ig_span_ctx c = { 0, a, b, cc, rbuf };
    CHECK(bstack_inplace_gen(bs, ig_span_gen, &c, NULL) == 0);
    /* Read [4,24): [4,6)='A', [6,10)='.', [10,14)='B', [14,22)='.', [22,24)='C' */
    uint8_t expect[20];
    memset(expect, '.', 20);
    memset(expect + 0,  'A', 2);
    memset(expect + 6,  'B', 4);
    memset(expect + 18, 'C', 2);
    CHECK(memcmp(rbuf, expect, 20) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

struct ig_reject_ctx {
    int step;
    uint8_t *data;
    const int *prev;   /* points at the caller's prev_status variable */
    int push_prev;     /* prev_status captured after the PUSH op */
    int sparse_prev;   /* prev_status captured after the SPARSE op */
};

static const bstack_iovec_t ig_reject_sparse_writes[1] = { { 0, (uint8_t *)"Z", 1 } };

static int ig_reject_size_ops_gen(bstack_gen_op_t *out_op, void *userctx)
{
    struct ig_reject_ctx *c = userctx;
    switch (c->step++) {
    case 0:
        out_op->kind = BSTACK_GEN_WRITE;
        out_op->u.write.offset = 0; out_op->u.write.data = c->data; out_op->u.write.len = 5;
        return 1;
    case 1:
        out_op->kind = BSTACK_GEN_PUSH;
        out_op->u.push.data = (const uint8_t *)"!!!";
        out_op->u.push.len  = 3;
        return 1;
    case 2:
        c->push_prev = *c->prev;   /* feedback for the rejected PUSH */
        out_op->kind = BSTACK_GEN_SPARSE;
        out_op->u.sparse.writes = ig_reject_sparse_writes;
        out_op->u.sparse.count  = 1;
        out_op->u.sparse.length = 4;
        return 1;
    default:
        c->sparse_prev = *c->prev; /* feedback for the rejected SPARSE */
        return 0;
    }
}

static int test_inplace_gen_rejects_size_ops_but_still_commits(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);

    uint8_t data[5]; memcpy(data, "HELLO", 5);
    int prev = 0;
    struct ig_reject_ctx c;
    c.step = 0; c.data = data; c.prev = &prev;
    c.push_prev = 0; c.sparse_prev = 0;
    CHECK(bstack_inplace_gen(bs, ig_reject_size_ops_gen, &c, &prev) == 0);
    /* Both size-changing ops were rejected as they were yielded. */
    CHECK(c.push_prev == EINVAL);
    CHECK(c.sparse_prev == EINVAL);
    /* Only the valid in-place WRITE committed; the size is unchanged. */
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 10);
    uint8_t buf[10]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, "HELLOworld", 10) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int ig_immediate_none_gen(bstack_gen_op_t *out_op, void *userctx)
{
    (void)out_op; (void)userctx;
    return 0;
}

static int test_inplace_gen_immediate_none_is_noop(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    CHECK(bstack_inplace_gen(bs, ig_immediate_none_gen, NULL, NULL) == 0);
    uint8_t buf[5]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, "hello", 5) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

/* ---- BSTACK_GEN_ABORT ---- */

struct ig_abort_ctx {
    int      step;
    int     *prev;      /* prev_status slot, so gen can inspect it */
    uint8_t  a[4];
    uint8_t  b[4];
    int      rejected;  /* prev_status captured after the out-of-range WRITE */
};

/* Stage two valid writes, then abort: neither may reach the file. */
static int ig_abort_gen(bstack_gen_op_t *out_op, void *userctx)
{
    struct ig_abort_ctx *c = userctx;
    switch (c->step++) {
    case 0:
        out_op->kind = BSTACK_GEN_WRITE;
        out_op->u.write.offset = 0; out_op->u.write.data = c->a; out_op->u.write.len = 4;
        return 1;
    case 1:
        out_op->kind = BSTACK_GEN_WRITE;
        out_op->u.write.offset = 10; out_op->u.write.data = c->b; out_op->u.write.len = 4;
        return 1;
    default:
        out_op->kind = BSTACK_GEN_ABORT;
        out_op->u.abort.status = EIO;
        return 1;
    }
}

static int test_inplace_gen_abort_discards_staged_writes(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    uint8_t dots[20]; memset(dots, '.', sizeof dots);
    CHECK(bstack_push(bs, dots, sizeof dots, NULL) == 0);

    struct ig_abort_ctx c;
    c.step = 0; c.prev = NULL; c.rejected = 0;
    memset(c.a, 'A', 4); memset(c.b, 'B', 4);
    errno = 0;
    CHECK(bstack_inplace_gen(bs, ig_abort_gen, &c, NULL) == -1);
    CHECK(errno == EIO);
    /* Neither staged write committed. */
    uint8_t buf[20]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, dots, sizeof dots) == 0);
    uint64_t len; CHECK(bstack_len(bs, &len) == 0); CHECK(len == 20);
    bstack_close(bs); unlink(tmp);
    return 0;
}

/* A rejected write arrives through prev_status; aborting in response unwinds
 * the write that did land, which returning 0 would have committed. */
static int ig_abort_after_reject_gen(bstack_gen_op_t *out_op, void *userctx)
{
    struct ig_abort_ctx *c = userctx;
    switch (c->step++) {
    case 0:
        out_op->kind = BSTACK_GEN_WRITE;
        out_op->u.write.offset = 0; out_op->u.write.data = c->a; out_op->u.write.len = 4;
        return 1;
    case 1:
        out_op->kind = BSTACK_GEN_WRITE;   /* past the payload: rejected */
        out_op->u.write.offset = 100; out_op->u.write.data = c->b; out_op->u.write.len = 4;
        return 1;
    default:
        c->rejected = *c->prev;
        out_op->kind = BSTACK_GEN_ABORT;
        out_op->u.abort.status = EINVAL;
        return 1;
    }
}

static int test_inplace_gen_abort_unwinds_after_a_rejected_write(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    uint8_t dots[20]; memset(dots, '.', sizeof dots);
    CHECK(bstack_push(bs, dots, sizeof dots, NULL) == 0);

    int prev = 0;
    struct ig_abort_ctx c;
    c.step = 0; c.prev = &prev; c.rejected = 0;
    memset(c.a, 'A', 4); memset(c.b, 'B', 4);
    errno = 0;
    CHECK(bstack_inplace_gen(bs, ig_abort_after_reject_gen, &c, &prev) == -1);
    CHECK(c.rejected == EINVAL);
    CHECK(errno == EINVAL);
    uint8_t buf[20]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, dots, sizeof dots) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

/* A zero-status abort is not a failure: the call succeeds, but the staged write
 * is still dropped — unlike returning 0, which commits it. */
static int ig_abort_ok_gen(bstack_gen_op_t *out_op, void *userctx)
{
    struct ig_abort_ctx *c = userctx;
    if (c->step++ == 0) {
        out_op->kind = BSTACK_GEN_WRITE;
        out_op->u.write.offset = 0; out_op->u.write.data = c->a; out_op->u.write.len = 4;
        return 1;
    }
    out_op->kind = BSTACK_GEN_ABORT;
    out_op->u.abort.status = 0;
    return 1;
}

static int test_inplace_gen_abort_without_a_status_succeeds_and_commits_nothing(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    uint8_t dots[20]; memset(dots, '.', sizeof dots);
    CHECK(bstack_push(bs, dots, sizeof dots, NULL) == 0);

    struct ig_abort_ctx c;
    c.step = 0; c.prev = NULL; c.rejected = 0;
    memset(c.a, 'A', 4); memset(c.b, 'B', 4);
    CHECK(bstack_inplace_gen(bs, ig_abort_ok_gen, &c, NULL) == 0);
    uint8_t buf[20]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, dots, sizeof dots) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

struct pg_abort_ctx { int step; uint8_t rbuf[5]; };

static int pg_abort_gen(bstack_gen_op_t *out_op, void *userctx)
{
    struct pg_abort_ctx *c = userctx;
    if (c->step++ == 0) {
        out_op->kind = BSTACK_GEN_READ;
        out_op->u.read.offset = 0; out_op->u.read.buf = c->rbuf; out_op->u.read.len = 5;
        return 1;
    }
    out_op->kind = BSTACK_GEN_ABORT;
    out_op->u.abort.status = EIO;
    return 1;
}

static int test_process_gen_abort_ends_the_sequence_with_an_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);

    struct pg_abort_ctx c; c.step = 0; memset(c.rbuf, 0, sizeof c.rbuf);
    errno = 0;
    CHECK(bstack_process_gen(bs, pg_abort_gen, &c) == -1);
    CHECK(errno == EIO);
    CHECK(memcmp(c.rbuf, "hello", 5) == 0);   /* the read still happened */
    uint8_t buf[10]; CHECK(bstack_peek(bs, 0, buf, NULL) == 0);
    CHECK(memcmp(buf, "helloworld", 10) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

#endif /* BSTACK_FEATURE_ATOMIC && BSTACK_FEATURE_SET */

/* =========================================================================
 * lock_up_to / locked_len / open_locked_up_to tests
 * ====================================================================== */

static int test_locked_len_is_zero_by_default(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_locked_len(bs) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_lock_up_to_sets_boundary(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 5) == 0);
    CHECK(bstack_locked_len(bs) == 5);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_lock_up_to_monotonic_can_grow(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"0123456789", 10, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 3) == 0);
    CHECK(bstack_lock_up_to(bs, 7) == 0);
    CHECK(bstack_locked_len(bs) == 7);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_lock_up_to_monotonic_cannot_shrink(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 5) == 0);
    CHECK(bstack_lock_up_to(bs, 3) == -1);
    CHECK(errno == EINVAL);
    CHECK(bstack_locked_len(bs) == 5); /* unchanged */
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_lock_up_to_n_equal_locked_is_idempotent(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 5) == 0);
    CHECK(bstack_lock_up_to(bs, 5) == 0); /* same value — no error */
    CHECK(bstack_locked_len(bs) == 5);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_lock_up_to_n_exceeds_len_returns_error(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 10) == -1);
    CHECK(errno == EINVAL);
    CHECK(bstack_locked_len(bs) == 0); /* unchanged */
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_open_locked_up_to_sets_boundary(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    /* Create a file with data first. */
    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"0123456789abcdefghij", 20, NULL) == 0);
        bstack_close(bs);
    }
    bstack_t *bs = bstack_open_locked_up_to(tmp, 10);
    CHECK(bs != NULL);
    CHECK(bstack_locked_len(bs) == 10);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_open_locked_up_to_n_exceeds_len_returns_null(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"hello", 5, NULL) == 0);
        bstack_close(bs);
    }
    bstack_t *bs = bstack_open_locked_up_to(tmp, 100);
    CHECK(bs == NULL);
    CHECK(errno == EINVAL);
    unlink(tmp);
    return 0;
}

/* =========================================================================
 * bstack_open_cached / bstack_open_locked_up_to_cached
 * ====================================================================== */

static int test_cache_get_reads_from_cache(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open_cached(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"abcdefgh", 8, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 8) == 0);
    uint8_t buf[8];
    CHECK(bstack_get(bs, 0, 8, buf) == 0);
    CHECK(memcmp(buf, "abcdefgh", 8) == 0);
    CHECK(bstack_get(bs, 2, 5, buf) == 0);
    CHECK(memcmp(buf, "cde", 3) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_cache_matches_uncached_get(void)
{
    /* Cached and non-cached reads must return identical bytes. */
    char tmp_c[64]; make_tmp(tmp_c, sizeof tmp_c);
    char tmp_u[64]; make_tmp(tmp_u, sizeof tmp_u);
    uint8_t data[256];
    int i;
    for (i = 0; i < 256; i++) data[i] = (uint8_t)i;

    bstack_t *cached = bstack_open_cached(tmp_c);
    CHECK(cached != NULL);
    CHECK(bstack_push(cached, data, 256, NULL) == 0);
    CHECK(bstack_lock_up_to(cached, 256) == 0);

    bstack_t *uncached = bstack_open(tmp_u);
    CHECK(uncached != NULL);
    CHECK(bstack_push(uncached, data, 256, NULL) == 0);
    CHECK(bstack_lock_up_to(uncached, 256) == 0);

    uint8_t bc[256], bu[256];
    CHECK(bstack_get(cached,  0, 256, bc) == 0);
    CHECK(bstack_get(uncached, 0, 256, bu) == 0);
    CHECK(memcmp(bc, bu, 256) == 0);

    CHECK(bstack_get(cached,  10, 210, bc) == 0);
    CHECK(bstack_get(uncached, 10, 210, bu) == 0);
    CHECK(memcmp(bc, bu, 200) == 0);

    bstack_close(cached);  unlink(tmp_c);
    bstack_close(uncached); unlink(tmp_u);
    return 0;
}

static int test_cache_sequential_lock_up_to(void)
{
    /* Two sequential lock_up_to calls — first allocates, second extends. */
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open_cached(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"abcdefghijklmnop", 16, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 8) == 0);
    uint8_t buf[16];
    CHECK(bstack_get(bs, 0, 8, buf) == 0);
    CHECK(memcmp(buf, "abcdefgh", 8) == 0);
    CHECK(bstack_lock_up_to(bs, 16) == 0);
    CHECK(bstack_get(bs, 0, 16, buf) == 0);
    CHECK(memcmp(buf, "abcdefghijklmnop", 16) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_cache_in_place_extend(void)
{
    /* lock_up_to(5) allocates capacity 8; lock_up_to(7) fits in-place. */
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open_cached(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"abcdefghij", 10, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 5) == 0);
    uint8_t buf[10];
    CHECK(bstack_get(bs, 0, 5, buf) == 0);
    CHECK(memcmp(buf, "abcde", 5) == 0);
    CHECK(bstack_lock_up_to(bs, 7) == 0);
    CHECK(bstack_get(bs, 0, 7, buf) == 0);
    CHECK(memcmp(buf, "abcdefg", 7) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_open_locked_up_to_cached_convenience(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    {
        bstack_t *bs = bstack_open(tmp);
        CHECK(bs != NULL);
        CHECK(bstack_push(bs, (uint8_t *)"hello world", 11, NULL) == 0);
        bstack_close(bs);
    }
    bstack_t *bs = bstack_open_locked_up_to_cached(tmp, 11);
    CHECK(bs != NULL);
    CHECK(bstack_locked_len(bs) == 11);
    uint8_t buf[11];
    CHECK(bstack_get(bs, 0, 11, buf) == 0);
    CHECK(memcmp(buf, "hello world", 11) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_uncached_stack_behaviour_unchanged(void)
{
    /* Non-cached stacks must still work identically after cache feature add. */
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"regression", 10, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 10) == 0);
    uint8_t buf[10];
    CHECK(bstack_get(bs, 0, 10, buf) == 0);
    CHECK(memcmp(buf, "regression", 10) == 0);
    CHECK(bstack_locked_len(bs) == 10);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_pop_respects_locked_region(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 5) == 0);
    /* Pop that would shrink below locked length must fail. */
    uint8_t buf[7];
    CHECK(bstack_pop(bs, 7, buf, NULL) == -1);
    CHECK(errno == EINVAL);
    /* Pop that stays at or above locked length must succeed. */
    CHECK(bstack_pop(bs, 3, buf, NULL) == 0);
    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 7);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_discard_respects_locked_region(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"0123456789", 10, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 6) == 0);
    /* Discard that would shrink below locked length must fail. */
    CHECK(bstack_discard(bs, 8) == -1);
    CHECK(errno == EINVAL);
    /* Discard that stays at or above locked length must succeed. */
    CHECK(bstack_discard(bs, 2) == 0);
    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 8);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_get_reads_locked_region_lock_free(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 5) == 0);
    /* Read entirely within locked region (lock-free fast-path). */
    uint8_t buf[5];
    CHECK(bstack_get(bs, 0, 5, buf) == 0);
    CHECK(memcmp(buf, "hello", 5) == 0);
    /* Read crossing the boundary still works via normal path. */
    uint8_t buf2[10];
    CHECK(bstack_get(bs, 0, 10, buf2) == 0);
    CHECK(memcmp(buf2, "helloworld", 10) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

#ifdef BSTACK_FEATURE_SET

static int test_set_respects_locked_region(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 5) == 0);
    /* Write into locked region must fail. */
    CHECK(bstack_set(bs, 0, (uint8_t *)"HELLO", 5) == -1);
    CHECK(errno == EINVAL);
    /* Write outside locked region must succeed. */
    CHECK(bstack_set(bs, 5, (uint8_t *)"WORLD", 5) == 0);
    uint8_t buf[10];
    CHECK(bstack_get(bs, 0, 10, buf) == 0);
    CHECK(memcmp(buf, "helloWORLD", 10) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_zero_respects_locked_region(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 5) == 0);
    /* Zero into locked region must fail. */
    CHECK(bstack_zero(bs, 2, 3) == -1);
    CHECK(errno == EINVAL);
    /* Zero outside locked region must succeed. */
    CHECK(bstack_zero(bs, 5, 5) == 0);
    uint8_t buf[10];
    CHECK(bstack_get(bs, 0, 10, buf) == 0);
    CHECK(memcmp(buf, "hello\0\0\0\0\0", 10) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

#endif /* BSTACK_FEATURE_SET */

#ifdef BSTACK_FEATURE_ATOMIC

static int test_atrunc_respects_locked_region(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 5) == 0);
    /* atrunc that would touch locked region must fail. */
    CHECK(bstack_atrunc(bs, 7, (uint8_t *)"!", 1) == -1);
    CHECK(errno == EINVAL);
    /* atrunc that stays above locked boundary must succeed. */
    CHECK(bstack_atrunc(bs, 3, (uint8_t *)"!", 1) == 0);
    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 8);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_splice_respects_locked_region(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"0123456789", 10, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 6) == 0);
    /* splice that would touch locked region must fail. */
    uint8_t removed[8];
    CHECK(bstack_splice(bs, removed, 8, (uint8_t *)"!!", 2) == -1);
    CHECK(errno == EINVAL);
    /* splice that stays above locked boundary must succeed. */
    CHECK(bstack_splice(bs, removed, 2, (uint8_t *)"!!", 2) == 0);
    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 10);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_try_discard_respects_locked_region(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"0123456789", 10, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 6) == 0);
    int ok = 0;
    /* try_discard that would shrink below locked length must fail. */
    CHECK(bstack_try_discard(bs, 10, 8, &ok) == -1);
    CHECK(errno == EINVAL);
    /* try_discard that stays at or above locked length must succeed. */
    CHECK(bstack_try_discard(bs, 10, 2, &ok) == 0);
    CHECK(ok == 1);
    uint64_t len;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(len == 8);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int cb_replace_x(const uint8_t *old, size_t old_len,
                        uint8_t **new_buf, size_t *new_len, void *ctx)
{
    (void)old; (void)ctx;
    *new_buf = malloc(old_len);
    if (!*new_buf) return -1;
    memset(*new_buf, 'X', old_len);
    *new_len = old_len;
    return 0;
}

static int test_replace_respects_locked_region(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 5) == 0);
    /* replace that would touch locked region must fail. */
    CHECK(bstack_replace(bs, 7, cb_replace_x, NULL) == -1);
    CHECK(errno == EINVAL);
    /* replace that stays above locked boundary must succeed. */
    CHECK(bstack_replace(bs, 3, cb_replace_x, NULL) == 0); /* replaces last 3 bytes: "rld" → "XXX" */
    uint8_t buf[10];
    CHECK(bstack_get(bs, 0, 10, buf) == 0);
    CHECK(memcmp(buf, "hellowoXXX", 10) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

#endif /* BSTACK_FEATURE_ATOMIC */

#if defined(BSTACK_FEATURE_ATOMIC) && defined(BSTACK_FEATURE_SET)

static int test_swap_respects_locked_region(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 5) == 0);
    /* swap into locked region must fail. */
    uint8_t old_buf[5];
    CHECK(bstack_swap(bs, 0, old_buf, (uint8_t *)"HELLO", 5) == -1);
    CHECK(errno == EINVAL);
    /* swap outside locked region must succeed. */
    CHECK(bstack_swap(bs, 5, old_buf, (uint8_t *)"WORLD", 5) == 0);
    CHECK(memcmp(old_buf, "world", 5) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_cas_respects_locked_region(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 5) == 0);
    int ok = 0;
    /* CAS into locked region must fail. */
    CHECK(bstack_cas(bs, 0, (uint8_t *)"hello", (uint8_t *)"HELLO", 5, &ok) == -1);
    CHECK(errno == EINVAL);
    /* CAS outside locked region must succeed. */
    CHECK(bstack_cas(bs, 5, (uint8_t *)"world", (uint8_t *)"WORLD", 5, &ok) == 0);
    CHECK(ok == 1);
    bstack_close(bs); unlink(tmp);
    return 0;
}

static int cb_proc_upper_lock(uint8_t *buf, size_t len, void *ctx)
{
    (void)ctx;
    for (size_t i = 0; i < len; i++)
        buf[i] = (uint8_t)toupper(buf[i]);
    return 0;
}

static int test_process_respects_locked_region(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);
    CHECK(bstack_push(bs, (uint8_t *)"helloworld", 10, NULL) == 0);
    CHECK(bstack_lock_up_to(bs, 5) == 0);
    /* process on locked region must fail. */
    CHECK(bstack_process(bs, 0, 5, cb_proc_upper_lock, NULL) == -1);
    CHECK(errno == EINVAL);
    /* process outside locked region must succeed. */
    CHECK(bstack_process(bs, 5, 10, cb_proc_upper_lock, NULL) == 0);
    uint8_t buf[10];
    CHECK(bstack_get(bs, 0, 10, buf) == 0);
    CHECK(memcmp(buf, "helloWORLD", 10) == 0);
    bstack_close(bs); unlink(tmp);
    return 0;
}

#endif /* BSTACK_FEATURE_ATOMIC && BSTACK_FEATURE_SET (lock tests) */

/* =========================================================================
 * bstack_is_empty tests
 * ====================================================================== */

static int test_is_empty_on_new_file(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    int empty = 99;
    CHECK(bstack_is_empty(bs, &empty) == 0);
    CHECK(empty == 1);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_is_empty_after_push(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hi", 2, NULL) == 0);

    int empty = 99;
    CHECK(bstack_is_empty(bs, &empty) == 0);
    CHECK(empty == 0);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_is_empty_after_pop_to_zero(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"hi", 2, NULL) == 0);

    uint8_t buf[2];
    CHECK(bstack_pop(bs, 2, buf, NULL) == 0);

    int empty = 99;
    CHECK(bstack_is_empty(bs, &empty) == 0);
    CHECK(empty == 1);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_is_empty_after_discard_to_zero(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    CHECK(bstack_push(bs, (uint8_t *)"abc", 3, NULL) == 0);
    CHECK(bstack_discard(bs, 3) == 0);

    int empty = 99;
    CHECK(bstack_is_empty(bs, &empty) == 0);
    CHECK(empty == 1);

    bstack_close(bs); unlink(tmp);
    return 0;
}

static int test_is_empty_consistent_with_len(void)
{
    char tmp[64]; make_tmp(tmp, sizeof tmp);
    bstack_t *bs = bstack_open(tmp);
    CHECK(bs != NULL);

    /* Empty: both agree. */
    uint64_t len; int empty;
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(bstack_is_empty(bs, &empty) == 0);
    CHECK((len == 0) == (empty == 1));

    /* Non-empty: both agree. */
    CHECK(bstack_push(bs, (uint8_t *)"x", 1, NULL) == 0);
    CHECK(bstack_len(bs, &len) == 0);
    CHECK(bstack_is_empty(bs, &empty) == 0);
    CHECK((len == 0) == (empty == 1));

    bstack_close(bs); unlink(tmp);
    return 0;
}

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

    /* bstack_is_empty */
    T(test_is_empty_on_new_file);
    T(test_is_empty_after_push);
    T(test_is_empty_after_pop_to_zero);
    T(test_is_empty_after_discard_to_zero);
    T(test_is_empty_consistent_with_len);

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
#ifndef _WIN32
    T(test_failed_write_defers_a_replay);
    T(test_pending_replay_applies_the_journal_on_the_next_write);
    T(test_pending_replay_leaves_the_locked_prefix_readable);
    T(test_recover_applies_a_pending_replay_without_a_write);
    T(test_recover_on_an_intact_stack_reports_nothing_pending);
#endif

    /* Write-in-progress journal recovery (always compiled) */
    T(test_recovery_replays_armed_set);
    T(test_recovery_replays_armed_repeat);
    T(test_recovery_replays_armed_copy);
    T(test_recovery_rolls_forward_splice_grow);
    T(test_recovery_rolls_forward_splice_shrink);
    T(test_recovery_rolls_back_unknown_mode);

    /* Legacy migration */
    T(test_migrate_upgrades_legacy_file);

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

    /* bstack_extend_sparse / bstack_extend_sparse_batched */
    T(test_extend_sparse_writes_prefix_and_zeros_rest);
    T(test_extend_sparse_empty_buf_is_pure_extend);
    T(test_extend_sparse_buf_longer_than_length_errors);
    T(test_extend_sparse_batched_scatters_buffers);
    T(test_extend_sparse_batched_overlap_errors);
    T(test_extend_sparse_batched_out_of_range_errors);
    T(test_extend_sparse_persists_across_reopen);

    /* bstack_resize */
    T(test_resize_grows_with_zeros);
    T(test_resize_shrinks);
    T(test_resize_same_size_is_noop);
    T(test_resize_to_zero_truncates);
    T(test_resize_shrink_below_locked_returns_error);
    T(test_resize_persists_across_reopen);

    /* bstack_ensure */
    T(test_ensure_grows_short_payload_with_zeros);
    T(test_ensure_noop_when_already_long_enough);
    T(test_ensure_persists_across_reopen);

#ifdef BSTACK_FEATURE_ATOMIC
    /* bstack_ensure_with */
    T(test_ensure_with_grows_and_calls_callback_on_new_region);
    T(test_ensure_with_skips_callback_when_already_long_enough);
    T(test_ensure_with_persists_across_reopen);
#endif

    /* bstack_lock_up_to / bstack_locked_len / bstack_open_locked_up_to */
    T(test_locked_len_is_zero_by_default);
    T(test_lock_up_to_sets_boundary);
    T(test_lock_up_to_monotonic_can_grow);
    T(test_lock_up_to_monotonic_cannot_shrink);
    T(test_lock_up_to_n_equal_locked_is_idempotent);
    T(test_lock_up_to_n_exceeds_len_returns_error);
    T(test_open_locked_up_to_sets_boundary);
    T(test_open_locked_up_to_n_exceeds_len_returns_null);

    /* bstack_open_cached / bstack_open_locked_up_to_cached */
    T(test_cache_get_reads_from_cache);
    T(test_cache_matches_uncached_get);
    T(test_cache_sequential_lock_up_to);
    T(test_cache_in_place_extend);
    T(test_open_locked_up_to_cached_convenience);
    T(test_uncached_stack_behaviour_unchanged);

    T(test_pop_respects_locked_region);
    T(test_discard_respects_locked_region);
    T(test_get_reads_locked_region_lock_free);

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

    /* bstack_repeat */
    T(test_repeat_fills_pattern);
    T(test_repeat_empty_or_zero_count_is_noop);
    T(test_repeat_journals_large_region_and_reopens_clean);

    /* bstack_set / bstack_zero — locked-region protection */
    T(test_set_respects_locked_region);
    T(test_zero_respects_locked_region);
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

    /* splice journal (length-changing tail replace) */
    T(test_atrunc_splice_journal_grow_and_reopens_clean);
    T(test_atrunc_splice_journal_shrink_and_reopens_clean);
    T(test_splice_returns_removed_on_length_change);

    /* bstack_try_extend */
    T(test_try_extend_matching_returns_true);
    T(test_try_extend_mismatching_returns_false);
    T(test_try_extend_empty_buf_matching);
    T(test_try_extend_persists_across_reopen);

    /* bstack_try_extend_sparse / bstack_try_extend_sparse_batched */
    T(test_try_extend_sparse_matching_writes);
    T(test_try_extend_sparse_mismatching_returns_false);
    T(test_try_extend_sparse_malformed_errors_even_on_mismatch);
    T(test_try_extend_sparse_batched_matching_scatters);
    T(test_try_extend_sparse_batched_mismatching_returns_false);

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

    /* bstack_atrunc / bstack_splice / bstack_try_discard / bstack_replace —
     * locked-region protection */
    T(test_atrunc_respects_locked_region);
    T(test_splice_respects_locked_region);
    T(test_try_discard_respects_locked_region);
    T(test_replace_respects_locked_region);
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

    /* bstack_swap / bstack_cas / bstack_process — locked-region protection */
    T(test_swap_respects_locked_region);
    T(test_cas_respects_locked_region);
    T(test_process_respects_locked_region);

    /* bstack_process_gen */
    T(test_process_gen_reads_then_writes);
    T(test_process_gen_dependent_reads_inform_next_offset);
    T(test_process_gen_immediate_none_is_noop);
    T(test_process_gen_write_ends_sequence);
    T(test_process_gen_swap_exchanges_two_regions_and_ends_sequence);
    T(test_process_gen_swap_target_informed_by_prior_read);
    T(test_process_gen_swap_overlapping_regions_returns_error);
    T(test_process_gen_swap_in_locked_region_returns_error);
    T(test_process_gen_does_not_change_file_size);
    T(test_process_gen_read_out_of_bounds_returns_error);
    T(test_process_gen_write_out_of_bounds_returns_error);
    T(test_process_gen_write_in_locked_region_returns_error);
    T(test_process_gen_read_in_locked_region_succeeds);
    T(test_process_gen_persists_across_reopen);
    T(test_process_gen_push_appends_and_ends_sequence);
    T(test_process_gen_push_empty_data_is_noop_and_ends_sequence);
    T(test_process_gen_pop_removes_and_ends_sequence);
    T(test_process_gen_pop_null_buf_discards_and_ends_sequence);
    T(test_process_gen_pop_zero_is_noop_and_ends_sequence);
    T(test_process_gen_pop_exceeds_payload_returns_error);
    T(test_process_gen_pop_below_locked_returns_error);
    T(test_process_gen_len_reports_current_size_and_continues);
    T(test_process_gen_len_informs_pop_size);
    T(test_process_gen_splice_replaces_tail_and_ends_sequence);
    T(test_process_gen_splice_null_removed_acts_like_atrunc);
    T(test_process_gen_sparse_scatters_and_ends_sequence);
    T(test_process_gen_sparse_overlap_returns_error);

    /* bstack_set_batched / bstack_inplace_gen — multi-write journal */
    T(test_set_batched_commits_all_writes_and_reopens_clean);
    T(test_set_batched_rejects_overlap);
    T(test_set_batched_empty_single_and_out_of_range);
    T(test_recovery_replays_armed_multi_write);
    T(test_recovery_rolls_back_corrupt_multi_write_tail);
    T(test_inplace_gen_reads_see_pending_writes);
    T(test_inplace_gen_later_write_overrides_overlap);
    T(test_inplace_gen_overlay_enclosure_and_gaps);
    T(test_inplace_gen_read_fully_covered_by_overlay);
    T(test_inplace_gen_read_spans_multiple_edits_and_gaps);
    T(test_inplace_gen_rejects_size_ops_but_still_commits);
    T(test_inplace_gen_immediate_none_is_noop);
    T(test_inplace_gen_abort_discards_staged_writes);
    T(test_inplace_gen_abort_unwinds_after_a_rejected_write);
    T(test_inplace_gen_abort_without_a_status_succeeds_and_commits_nothing);
    T(test_process_gen_abort_ends_the_sequence_with_an_error);

    /* bstack_copy — disjoint copy journal */
    T(test_copy_disjoint_journals_and_reopens_clean);
    T(test_copy_same_location_is_noop);
#endif

    printf("\n%d/%d passed\n", g_passed, g_total);
    return (g_passed == g_total) ? 0 : 1;
}
