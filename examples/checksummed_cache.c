/*
 * Checksummed block with per-thread in-memory cache protected by CRDS.
 *
 * The bstack_t pointer is shared across threads.  The per-thread cache
 * (cached_checksum, cached_payload) lives in each thread's own
 * checksummed_block_t, constructed inside the thread function.
 *
 * A pthread_barrier forces thread 2 to attempt its write only after thread 1
 * has already committed, guaranteeing that thread 2's empty cache is stale.
 * 
 * Note that XOR checksums are not collision-resistant, so this example is only
 * intended to demonstrate the pattern, not to provide strong integrity guarantees.
 *
 * Requires: -DBSTACK_FEATURE_SET -DBSTACK_FEATURE_ATOMIC
 *
 * Build and run:
 *   make -C ../c example-checksummed_cache
 */

#if !defined(BSTACK_FEATURE_SET) || !defined(BSTACK_FEATURE_ATOMIC)
#  error "checksummed_cache.c requires -DBSTACK_FEATURE_SET -DBSTACK_FEATURE_ATOMIC"
#endif

#include "../c/bstack.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BLOCK_SIZE     64
#define PAYLOAD_OFFSET 8
#define PAYLOAD_SIZE   56

/* ── Little-endian helper ────────────────────────────────────────────────── */

static void write_le64(uint8_t *p, uint64_t v)
{
    p[0] = (uint8_t)(v);       p[1] = (uint8_t)(v >>  8);
    p[2] = (uint8_t)(v >> 16); p[3] = (uint8_t)(v >> 24);
    p[4] = (uint8_t)(v >> 32); p[5] = (uint8_t)(v >> 40);
    p[6] = (uint8_t)(v >> 48); p[7] = (uint8_t)(v >> 56);
}

/* ── ChecksummedBlock ────────────────────────────────────────────────────── */

typedef struct {
    bstack_t *stack;                    /* shared across threads */
    int       has_checksum;
    uint8_t   cached_block[BLOCK_SIZE]; /* [0..8) checksum | [8..64) payload */
} checksummed_block_t;

static uint64_t compute_checksum(const uint8_t *payload, size_t len)
{
    uint64_t cs = 0;
    for (size_t i = 0; i < len; i++)
        cs ^= (uint64_t)payload[i];
    return cs;
}

/*
 * Write new_payload atomically via eq_crds.
 *
 * *committed is set to 1 if the write was committed, or 0 if the cached
 * checksum was stale.  On a stale miss the cache is refreshed via bstack_get
 * so the caller can retry immediately.
 */
static int cb_write(checksummed_block_t *cb,
                    const uint8_t *new_payload, size_t len,
                    int *committed)
{
    uint64_t new_checksum = compute_checksum(new_payload, len);

    uint8_t block[BLOCK_SIZE] = {0};
    write_le64(block, new_checksum);
    memcpy(block + PAYLOAD_OFFSET, new_payload, len);

    /* Use cached_block[0..8] as expected checksum (zeroes if no cache yet).
     * cached_block also serves as b_old_buf: on match, eq_crds fills it with
     * the old on-disk block before writing the new one. */
    static const uint8_t zeroes[8] = {0};
    const uint8_t *expected = cb->has_checksum ? cb->cached_block : zeroes;

    int ok = 0;
    if (bstack_eq_crds(cb->stack,
                       0, expected, 8,
                       0, cb->cached_block, block, BLOCK_SIZE,
                       &ok) < 0)
        return -1;

    if (ok) {
        /* Committed — overwrite cached_block with the new state. */
        memcpy(cb->cached_block, block, BLOCK_SIZE);
        cb->has_checksum = 1;
        *committed = 1;
        return 0;
    }

    /* Stale — read current disk state directly into cached_block; no copy. */
    if (bstack_get(cb->stack, 0, BLOCK_SIZE, cb->cached_block) < 0)
        return -1;
    cb->has_checksum = 1;
    *committed = 0;
    return 0;
}

/* ── Read generator ──────────────────────────────────────────────────────── */

typedef struct {
    checksummed_block_t *cb;
    int     need_checksum;
    uint8_t read_checksum[8];
} read_ctx_t;

static int read_gen(uint64_t *out_offset, uint8_t **out_buf, size_t *out_len,
                    void *userctx)
{
    read_ctx_t          *ctx = userctx;
    checksummed_block_t *cb  = ctx->cb;

    if (ctx->need_checksum) {
        ctx->need_checksum = 0;
        *out_offset = 0;
        *out_buf    = ctx->read_checksum;
        *out_len    = 8;
        return 1;
    }

    /* Cache hit — checksum unchanged, skip payload fetch. */
    if (cb->has_checksum &&
        memcmp(ctx->read_checksum, cb->cached_block, 8) == 0)
        return 0;

    /* Cache miss — update checksum bytes and fetch payload directly into
     * cached_block; no copy needed for the payload. */
    memcpy(cb->cached_block, ctx->read_checksum, 8);
    cb->has_checksum = 1;
    *out_offset = PAYLOAD_OFFSET;
    *out_buf    = cb->cached_block + PAYLOAD_OFFSET;
    *out_len    = PAYLOAD_SIZE;
    return 1;
}

static int cb_read(checksummed_block_t *cb, const uint8_t **out_payload)
{
    read_ctx_t ctx = { .cb = cb, .need_checksum = 1, .read_checksum = {0} };
    if (bstack_get_batched_gen(cb->stack, read_gen, &ctx) < 0)
        return -1;
    *out_payload = cb->cached_block + PAYLOAD_OFFSET;
    return 0;
}

/* ── Helpers ─────────────────────────────────────────────────────────────── */

static void print_trimmed(const uint8_t *payload, size_t len)
{
    size_t trimmed = 0;
    for (size_t i = 0; i < len; i++)
        if (payload[i] != 0) trimmed = i + 1;
    printf("\"%.*s\"", (int)trimmed, (const char *)payload);
}

/* ── Thread functions ────────────────────────────────────────────────────── */

typedef struct {
    bstack_t       *stack;
    pthread_mutex_t mtx;
    pthread_cond_t  cond;
    int             t1_written; /* set to 1 after thread 1 commits its write */
} thread_arg_t;

static void *thread1(void *arg)
{
    thread_arg_t        *a  = arg;
    checksummed_block_t  cb = { .stack = a->stack, .has_checksum = 0, .cached_block = {0} };

    int committed = 0;
    if (cb_write(&cb, (const uint8_t *)"Written by thread 1.", 20, &committed) < 0) {
        perror("[t1] write"); return NULL;
    }
    printf("[t1] write          → committed=%d\n", committed);

    const uint8_t *payload = NULL;
    if (cb_read(&cb, &payload) < 0) { perror("[t1] read"); return NULL; }
    printf("[t1] read           → hit  "); print_trimmed(payload, PAYLOAD_SIZE); putchar('\n');

    /* Signal thread 2 that the write has landed. */
    pthread_mutex_lock(&a->mtx);
    a->t1_written = 1;
    pthread_cond_signal(&a->cond);
    pthread_mutex_unlock(&a->mtx);
    return NULL;
}

static void *thread2(void *arg)
{
    thread_arg_t        *a  = arg;
    checksummed_block_t  cb = { .stack = a->stack, .has_checksum = 0, .cached_block = {0} };

    /* Wait until thread 1 has committed its write; our cache (None) is stale. */
    pthread_mutex_lock(&a->mtx);
    while (!a->t1_written)
        pthread_cond_wait(&a->cond, &a->mtx);
    pthread_mutex_unlock(&a->mtx);

    int committed = 0;
    if (cb_write(&cb, (const uint8_t *)"Written by thread 2.", 20, &committed) < 0) {
        perror("[t2] write (stale)"); return NULL;
    }
    printf("[t2] write (stale)  → committed=%d, cache refreshed\n", committed);

    /* Retry — cache is now current. */
    if (cb_write(&cb, (const uint8_t *)"Written by thread 2.", 20, &committed) < 0) {
        perror("[t2] write (retry)"); return NULL;
    }
    printf("[t2] write (retry)  → committed=%d\n", committed);

    const uint8_t *payload = NULL;
    if (cb_read(&cb, &payload) < 0) { perror("[t2] read"); return NULL; }
    printf("[t2] read           → hit  "); print_trimmed(payload, PAYLOAD_SIZE); putchar('\n');

    return NULL;
}

/* ── Main ────────────────────────────────────────────────────────────────── */

int main(void)
{
    const char *path = "checksummed_cache_example.bstack";
    remove(path);

    bstack_t *stack = bstack_open(path);
    if (!stack) { perror("bstack_open"); return 1; }

    /* Initialise with a zeroed block. */
    uint8_t zeros[BLOCK_SIZE] = {0};
    if (bstack_push(stack, zeros, BLOCK_SIZE, NULL) < 0) {
        perror("bstack_push"); bstack_close(stack); return 1;
    }

    thread_arg_t arg = {
        .stack      = stack,
        .mtx        = PTHREAD_MUTEX_INITIALIZER,
        .cond       = PTHREAD_COND_INITIALIZER,
        .t1_written = 0,
    };

    pthread_t t1, t2;
    pthread_create(&t1, NULL, thread1, &arg);
    pthread_create(&t2, NULL, thread2, &arg);
    pthread_join(t1, NULL);
    pthread_join(t2, NULL);

    pthread_mutex_destroy(&arg.mtx);
    pthread_cond_destroy(&arg.cond);
    bstack_close(stack);
    return 0;
}
