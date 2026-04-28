/*
 * Atomic compound operations on a bstack event log.
 *
 * Demonstrates all BSTACK_FEATURE_ATOMIC operations:
 *   bstack_replace, bstack_atrunc, bstack_splice, bstack_try_extend,
 *   bstack_try_discard; and when BSTACK_FEATURE_SET is also defined:
 *   bstack_swap, bstack_cas, bstack_process.
 *
 * All log entries are fixed-width 11-byte ASCII lines ("[ok] xxxxx\n") so
 * the byte counts in each call are obvious by inspection.
 *
 * Build and run:
 *   make -C ../c example-atomic_ops
 */

#ifndef BSTACK_FEATURE_ATOMIC
#  error "atomic_ops.c requires -DBSTACK_FEATURE_ATOMIC"
#endif

#include "../c/bstack.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void show(const char *label, bstack_t *bs)
{
    uint64_t len;
    bstack_len(bs, &len);
    char *buf = malloc((size_t)(len + 1));
    size_t written;
    bstack_peek(bs, 0, (uint8_t *)buf, &written);
    buf[written] = '\0';
    printf("%s: \"%s\"\n", label, buf);
    free(buf);
}

/* replace callback: return an uppercased copy of the old tail bytes. */
static int uppercase_cb(const uint8_t *old, size_t old_len,
                        uint8_t **new_buf, size_t *new_len, void *ctx)
{
    (void)ctx;
    uint8_t *out = malloc(old_len);
    if (!out) return -1;
    for (size_t i = 0; i < old_len; i++) {
        uint8_t c = old[i];
        out[i] = (c >= 'a' && c <= 'z') ? (uint8_t)(c - 32) : c;
    }
    *new_buf = out;
    *new_len = old_len;
    return 0;
}

#ifdef BSTACK_FEATURE_SET
/* process callback: increment the 4-byte LE counter stored at buf[4..8]. */
static int increment_cb(uint8_t *buf, size_t len, void *ctx)
{
    (void)ctx;
    if (len < 8) return 0;
    uint32_t n = (uint32_t)buf[4]
               | ((uint32_t)buf[5] << 8)
               | ((uint32_t)buf[6] << 16)
               | ((uint32_t)buf[7] << 24);
    n++;
    buf[4] = (uint8_t)(n);
    buf[5] = (uint8_t)(n >> 8);
    buf[6] = (uint8_t)(n >> 16);
    buf[7] = (uint8_t)(n >> 24);
    return 0;
}
#endif /* BSTACK_FEATURE_SET */

int main(void)
{
    bstack_t *bs = bstack_open("atomic_ops_example.bstack");
    if (!bs) { perror("bstack_open"); return 1; }

    /* Each log line is exactly 11 bytes: "[ok] xxxxx\n" */
    bstack_push(bs, (const uint8_t *)"[ok] start\n", 11, NULL);
    bstack_push(bs, (const uint8_t *)"[ok] login\n", 11, NULL);
    bstack_push(bs, (const uint8_t *)"[ok] fetch\n", 11, NULL);
    show("initial      ", bs);

    /* replace: read last N bytes, pass to callback, write result back —
     * all under a single write lock. */
    bstack_replace(bs, 11, uppercase_cb, NULL);
    show("after replace", bs);

    /* atrunc: atomically remove last N bytes and append new bytes. */
    bstack_atrunc(bs, 11, (const uint8_t *)"[ok] store\n", 11);
    show("after atrunc ", bs);

    /* splice: like atrunc, but also writes removed bytes into a caller buffer. */
    uint8_t removed[11];
    bstack_splice(bs, removed, 11, (const uint8_t *)"[ok] flush\n", 11);
    printf("splice removed: \"%.*s\"\n", 11, removed);
    show("after splice ", bs);

    /* try_extend: append only when the current size equals the expected value.
     * The second call observes the updated size and is a no-op. */
    uint64_t snap;
    bstack_len(bs, &snap);
    int pushed1, pushed2;
    bstack_try_extend(bs, snap, (const uint8_t *)"[ok] retry\n", 11, &pushed1);
    bstack_try_extend(bs, snap, (const uint8_t *)"[ok] retry\n", 11, &pushed2);
    printf("try_extend: first=%d second=%d\n", pushed1, pushed2);
    show("after t_ext  ", bs);

    /* try_discard: remove N bytes only when the current size matches. */
    bstack_len(bs, &snap);
    int ok1, ok2;
    bstack_try_discard(bs, snap, 11, &ok1);
    bstack_try_discard(bs, snap, 11, &ok2);
    printf("try_discard: first=%d second=%d\n", ok1, ok2);
    show("after t_disc ", bs);

#ifdef BSTACK_FEATURE_SET
    /* Push an 8-byte status record: 4-byte ASCII tag + 4-byte LE counter. */
    uint64_t status_off;
    uint8_t zeros[8] = {0};
    bstack_push(bs, zeros, 8, &status_off);
    printf("status record at offset %llu\n", (unsigned long long)status_off);

    /* swap: atomically read N bytes at offset, overwrite them, return old. */
    uint8_t prev[4];
    bstack_swap(bs, status_off, prev, (const uint8_t *)"RUN\x00", 4);
    printf("swap wrote 'RUN\\0', got back [%02x %02x %02x %02x]\n",
           prev[0], prev[1], prev[2], prev[3]);

    /* cas: compare-and-exchange; writes only when current bytes match old.
     * The second call is a no-op because the tag is now "DONE". */
    int ok_cas1, ok_cas2;
    bstack_cas(bs, status_off,
               (const uint8_t *)"RUN\x00", (const uint8_t *)"DONE", 4, &ok_cas1);
    bstack_cas(bs, status_off,
               (const uint8_t *)"RUN\x00", (const uint8_t *)"FAIL", 4, &ok_cas2);
    printf("cas: first=%d second=%d\n", ok_cas1, ok_cas2);
    uint8_t tag[4];
    bstack_get(bs, status_off, status_off + 4, tag);
    printf("tag now: \"%.4s\"\n", tag);

    /* process: read a range into a buffer, pass to callback for in-place
     * mutation, write back — all under one write lock. */
    bstack_process(bs, status_off, status_off + 8, increment_cb, NULL);
    uint8_t record[8];
    bstack_get(bs, status_off, status_off + 8, record);
    uint32_t counter = (uint32_t)record[4]
                     | ((uint32_t)record[5] << 8)
                     | ((uint32_t)record[6] << 16)
                     | ((uint32_t)record[7] << 24);
    printf("after process: tag=\"%.4s\" counter=%u\n", record, counter);
#endif /* BSTACK_FEATURE_SET */

    bstack_close(bs);
    return 0;
}
