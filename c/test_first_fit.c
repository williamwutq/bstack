#ifndef _WIN32
#  define _DARWIN_C_SOURCE
#  define _DEFAULT_SOURCE
#  define _POSIX_C_SOURCE 200809L
#endif

#include "bstack_alloc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

static int passed = 0, failed = 0;

#define PASS(name) do { printf("PASS  %s\n", name); passed++; } while(0)
#define FAIL(name, msg) do { printf("FAIL  %s: %s\n", name, msg); failed++; } while(0)
#define CHECK(name, cond) do { if (cond) PASS(name); else FAIL(name, #cond); } while(0)

static const char *TMP = "/tmp/test_first_fit.bstack";

static void cleanup(void) { remove(TMP); }

static void test_alloc_small(void)
{
    /* alloc(5) should not fail dealloc — this was broken before the align_len fix */
    bstack_t *bs = bstack_open(TMP);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    assert(a);

    bstack_slice_t s;
    int r = bstack_allocator_alloc((bstack_allocator_t *)a, 5, &s);
    CHECK("alloc_small: alloc(5) succeeds", r == 0);
    CHECK("alloc_small: slice.len == 5", s.len == 5);

    uint8_t data[5] = {1,2,3,4,5};
    bstack_slice_write(s, data, 5);

    r = bstack_allocator_dealloc((bstack_allocator_t *)a, s);
    CHECK("alloc_small: dealloc(5-byte slice) succeeds", r == 0);

    bstack_close(first_fit_bstack_allocator_into_stack(a));
    cleanup();
}

static void test_alloc_write_read(void)
{
    bstack_t *bs = bstack_open(TMP);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    assert(a);

    bstack_slice_t s;
    assert(bstack_allocator_alloc((bstack_allocator_t *)a, 16, &s) == 0);

    uint8_t wbuf[16];
    memset(wbuf, 0xAB, 16);
    bstack_slice_write(s, wbuf, 16);

    uint8_t rbuf[16];
    assert(bstack_slice_read(s, rbuf) == 0);
    CHECK("alloc_write_read: data roundtrip", memcmp(wbuf, rbuf, 16) == 0);

    bstack_close(first_fit_bstack_allocator_into_stack(a));
    cleanup();
}

static void test_realloc_grow(void)
{
    bstack_t *bs = bstack_open(TMP);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    assert(a);

    bstack_slice_t s;
    assert(bstack_allocator_alloc((bstack_allocator_t *)a, 32, &s) == 0);

    uint8_t wbuf[32];
    memset(wbuf, 0xCD, 32);
    bstack_slice_write(s, wbuf, 32);

    /* Grow to 64 bytes */
    bstack_slice_t s2;
    assert(bstack_allocator_realloc((bstack_allocator_t *)a, s, 64, &s2) == 0);
    CHECK("realloc_grow: offset preserved (tail block)", s2.offset == s.offset);
    CHECK("realloc_grow: new len == 64", s2.len == 64);

    uint8_t rbuf[64];
    assert(bstack_slice_read(s2, rbuf) == 0);
    CHECK("realloc_grow: original 32 bytes preserved",
          memcmp(rbuf, wbuf, 32) == 0);
    {
        int zeros_ok = 1;
        int i;
        for (i = 32; i < 64; i++)
            if (rbuf[i] != 0) { zeros_ok = 0; break; }
        CHECK("realloc_grow: new bytes are zero", zeros_ok);
    }

    bstack_close(first_fit_bstack_allocator_into_stack(a));
    cleanup();
}

static void test_slot_reuse(void)
{
    bstack_t *bs = bstack_open(TMP);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    assert(a);

    /* alloc a, alloc b so a is not the tail, dealloc a, alloc c — c reuses a's slot */
    bstack_slice_t sa, sb, sc;
    assert(bstack_allocator_alloc((bstack_allocator_t *)a, 32, &sa) == 0);
    assert(bstack_allocator_alloc((bstack_allocator_t *)a, 32, &sb) == 0);
    uint64_t a_offset = sa.offset;

    assert(bstack_allocator_dealloc((bstack_allocator_t *)a, sa) == 0);
    assert(bstack_allocator_alloc((bstack_allocator_t *)a, 16, &sc) == 0);

    CHECK("slot_reuse: first-fit reuses freed slot", sc.offset == a_offset);

    bstack_allocator_dealloc((bstack_allocator_t *)a, sc);
    bstack_allocator_dealloc((bstack_allocator_t *)a, sb);
    bstack_close(first_fit_bstack_allocator_into_stack(a));
    cleanup();
}

static void test_persist_reopen(void)
{
    uint64_t saved_offset;
    {
        bstack_t *bs = bstack_open(TMP);
        first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
        assert(a);

        bstack_slice_t s;
        assert(bstack_allocator_alloc((bstack_allocator_t *)a, 24, &s) == 0);
        saved_offset = s.offset;

        uint8_t data[24];
        memset(data, 0x77, 24);
        bstack_slice_write(s, data, 24);
        /* Close WITHOUT deallocating — block stays allocated across reopen */
        bstack_close(first_fit_bstack_allocator_into_stack(a));
    }
    {
        bstack_t *bs = bstack_open(TMP);
        first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
        assert(a);

        /* Read directly at the saved offset: data must survive the reopen */
        uint8_t rbuf[24];
        assert(bstack_get(bstack_allocator_stack((bstack_allocator_t *)a),
                          saved_offset, saved_offset + 24, rbuf) == 0);
        {
            int ok = 1, i;
            for (i = 0; i < 24; i++) if (rbuf[i] != 0x77) { ok = 0; break; }
            CHECK("persist_reopen: data survives close/reopen", ok);
        }

        /* Session-1 block is still allocated; new alloc goes to a fresh block */
        bstack_slice_t s;
        assert(bstack_allocator_alloc((bstack_allocator_t *)a, 24, &s) == 0);
        CHECK("persist_reopen: new alloc at different offset than session-1 block",
              s.offset != saved_offset);

        bstack_close(first_fit_bstack_allocator_into_stack(a));
    }
    cleanup();
}

static void test_realloc_small(void)
{
    /* alloc(5), realloc to 10, verify no error — exercises align_len fix in realloc */
    bstack_t *bs = bstack_open(TMP);
    first_fit_bstack_allocator_t *a = first_fit_bstack_allocator_new(bs);
    assert(a);

    bstack_slice_t s;
    assert(bstack_allocator_alloc((bstack_allocator_t *)a, 5, &s) == 0);

    uint8_t wbuf[5] = {10,20,30,40,50};
    bstack_slice_write(s, wbuf, 5);

    bstack_slice_t s2;
    int r = bstack_allocator_realloc((bstack_allocator_t *)a, s, 10, &s2);
    CHECK("realloc_small: realloc(5->10) succeeds", r == 0);
    CHECK("realloc_small: len updated to 10", s2.len == 10);

    uint8_t rbuf[10];
    assert(bstack_slice_read(s2, rbuf) == 0);
    CHECK("realloc_small: first 5 bytes preserved", memcmp(rbuf, wbuf, 5) == 0);
    {
        int zeros_ok = 1, i;
        for (i = 5; i < 10; i++) if (rbuf[i] != 0) { zeros_ok = 0; break; }
        CHECK("realloc_small: new bytes are zero", zeros_ok);
    }

    bstack_allocator_dealloc((bstack_allocator_t *)a, s2);
    bstack_close(first_fit_bstack_allocator_into_stack(a));
    cleanup();
}

int main(void)
{
    test_alloc_small();
    test_alloc_write_read();
    test_realloc_grow();
    test_slot_reuse();
    test_persist_reopen();
    test_realloc_small();

    printf("\n%d/%d passed\n", passed, passed + failed);
    return failed > 0 ? 1 : 0;
}
