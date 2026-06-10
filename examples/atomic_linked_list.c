/*
 * Free-list push and atomic pop without any allocator-level mutex: a
 * SlabBStackAllocator-style free list maintained using only
 * bstack_cross_exchange to push and bstack_process_gen to pop.
 *
 * ── Push: splice a freed block onto the head with bstack_cross_exchange ──
 *
 * 1. Plant a self-pointer placeholder in the block's "next" slot —
 *    bstack_set(b_addr, &b_addr, 8) — seeding it with an in-bounds value
 *    that no reader ever observes.
 * 2. Atomically swap that slot with free_head —
 *    bstack_cross_exchange(b_addr, FREE_HEAD_OFFSET, 8).  Before the call
 *    b_addr holds b_addr and free_head holds the old head H; after,
 *    free_head holds b_addr (b is the new head) and b_addr holds H (b's
 *    next is the old head) — spliced in under one write lock, in a single
 *    step.
 *
 * ── Pop: one held lock across read, read, write with bstack_process_gen ──
 *
 * 1. Read free_head.
 * 2. If it isn't the sentinel, read that block's next pointer.
 * 3. Write next back into free_head, advancing the list.
 *
 * bstack_process_gen acquires the BStack write lock once, *before* the
 * first read, and holds it — unreleased — through every subsequent read and
 * the terminating write.  The whole read-read-write sequence runs as a
 * single indivisible step, so no other thread can observe or modify
 * free_head or any node's next pointer in between.  This is precisely what
 * closes the ABA race window that a bstack_get_batched_gen (read, release
 * lock) + bstack_cas (re-acquire, compare, write) pairing would leave open:
 * between the release and the re-acquisition, another thread could pop the
 * head, pop the next node, and push the first node back — making free_head
 * cycle back to the exact byte value the first reader saw, even though the
 * list underneath has completely changed.  A subsequent CAS would then
 * "succeed" while handing out a node that is already live elsewhere.
 * Holding one lock across the whole sequence makes that interleaving
 * impossible: every one of those steps needs the same write lock, so they
 * simply block until the sequence — including its terminating write — has
 * finished.
 *
 * ── Layout ─────────────────────────────────────────────────────────────
 *
 * - free_head: u64 (8 bytes, LE) at offset 0 — offset of the first free
 *   block, or UINT64_MAX (SENTINEL) when the list is empty.
 * - block[i]: u64 (8 bytes, LE) — while free, holds the offset of the next
 *   free block (or SENTINEL for the list's tail).
 *
 * Requires: -DBSTACK_FEATURE_SET -DBSTACK_FEATURE_ATOMIC
 *
 * Build and run:
 *   make -C ../c example-atomic_linked_list
 */

#if !defined(BSTACK_FEATURE_SET) || !defined(BSTACK_FEATURE_ATOMIC)
#  error "atomic_linked_list.c requires -DBSTACK_FEATURE_SET -DBSTACK_FEATURE_ATOMIC"
#endif

#include "../c/bstack.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SENTINEL        UINT64_MAX
#define FREE_HEAD_OFFSET 0
#define BLOCK_SIZE      8
#define FIRST_BLOCK     (FREE_HEAD_OFFSET + BLOCK_SIZE)
#define NUM_BLOCKS      6

/* ── Little-endian helpers ───────────────────────────────────────────────── */

static uint64_t read_le64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v |= (uint64_t)p[i] << (8 * i);
    return v;
}

static void write_le64(uint8_t *p, uint64_t v)
{
    for (int i = 0; i < 8; i++) p[i] = (uint8_t)(v >> (8 * i));
}

static uint64_t block_offset(uint64_t i)
{
    return FIRST_BLOCK + i * BLOCK_SIZE;
}

static uint64_t block_index(uint64_t b_addr)
{
    return (b_addr - FIRST_BLOCK) / BLOCK_SIZE;
}

static uint64_t read_u64(bstack_t *stack, uint64_t offset)
{
    uint8_t buf[8];
    if (bstack_get(stack, offset, offset + 8, buf) != 0) {
        perror("bstack_get");
        exit(1);
    }
    return read_le64(buf);
}

/* Render the free list as "head -> B3 -> B1 -> B0 -> sentinel" by walking
 * "next" pointers with bstack_get. */
static void free_list_string(bstack_t *stack, char *out, size_t out_size)
{
    size_t pos = (size_t)snprintf(out, out_size, "head");
    uint64_t offset = read_u64(stack, FREE_HEAD_OFFSET);
    while (offset != SENTINEL && pos < out_size) {
        pos += (size_t)snprintf(out + pos, out_size - pos,
                                 " -> B%llu", (unsigned long long)block_index(offset));
        offset = read_u64(stack, offset);
    }
    if (pos < out_size)
        snprintf(out + pos, out_size - pos, " -> sentinel");
}

/* ── Pop: bstack_process_gen state machine ───────────────────────────────── */

typedef struct {
    int      step;
    uint8_t  head_buf[8];
    uint8_t  next_buf[8];
    uint64_t popped;
} pop_ctx_t;

static int pop_gen(bstack_gen_op_t *out_op, void *userctx)
{
    pop_ctx_t *ctx = userctx;
    switch (ctx->step++) {
    case 0:
        /* Step 0: read the current head pointer. */
        out_op->kind          = BSTACK_GEN_READ;
        out_op->u.read.offset = FREE_HEAD_OFFSET;
        out_op->u.read.buf    = ctx->head_buf;
        out_op->u.read.len    = sizeof ctx->head_buf;
        return 1;
    case 1: {
        /* head_buf now holds free_head.  Sentinel means the list is empty —
         * end the sequence with no write.  Otherwise read the head block's
         * next pointer next. */
        uint64_t head = read_le64(ctx->head_buf);
        if (head == SENTINEL)
            return 0;
        ctx->popped = head;
        out_op->kind          = BSTACK_GEN_READ;
        out_op->u.read.offset = head;
        out_op->u.read.buf    = ctx->next_buf;
        out_op->u.read.len    = sizeof ctx->next_buf;
        return 1;
    }
    case 2:
        /* next_buf now holds the popped block's next.  Writing it into
         * free_head advances the list and ends the sequence — still under
         * the lock acquired for step 0's read. */
        out_op->kind           = BSTACK_GEN_WRITE;
        out_op->u.write.offset = FREE_HEAD_OFFSET;
        out_op->u.write.data   = ctx->next_buf;
        out_op->u.write.len    = sizeof ctx->next_buf;
        return 1;
    default:
        return 0;
    }
}

/* Pop the head block via a single bstack_process_gen sequence: read
 * free_head, read its next, write next back into free_head.  The write lock
 * acquired for the first read is held through the terminating write, so the
 * three steps execute as one atomic, uninterruptible unit.
 *
 * Returns 0 with *out_popped == SENTINEL if the list was empty, 0 with
 * *out_popped set to the popped block's offset otherwise, or -1 on error. */
static int pop(bstack_t *stack, uint64_t *out_popped)
{
    pop_ctx_t ctx = {0, {0}, {0}, SENTINEL};
    if (bstack_process_gen(stack, pop_gen, &ctx) != 0)
        return -1;
    *out_popped = ctx.popped;
    return 0;
}

/* ── Push: bstack_set + bstack_cross_exchange ────────────────────────────── */

/* Free each block in turn via bstack_set + bstack_cross_exchange, printing
 * the list after every splice.  Each push prepends, so freeing 0, 1, 2, ...
 * yields the list in reverse insertion order — a textbook LIFO free list. */
static void push_demo(bstack_t *stack)
{
    printf("=== Push: splice onto the head with cross_exchange ===\n");
    for (uint64_t i = 0; i < NUM_BLOCKS; i++) {
        uint64_t b_addr = block_offset(i);

        /* 1. Plant a self-pointer placeholder — never observed by any
         *    reader; cross_exchange replaces it with the old head in the
         *    same atomic step that publishes b as the new head. */
        uint8_t self_ptr[8];
        write_le64(self_ptr, b_addr);
        if (bstack_set(stack, b_addr, self_ptr, sizeof self_ptr) != 0) {
            perror("bstack_set"); exit(1);
        }

        /* 2. Atomically swap b's slot with free_head under one write lock. */
        if (bstack_cross_exchange(stack, b_addr, FREE_HEAD_OFFSET, BLOCK_SIZE) != 0) {
            perror("bstack_cross_exchange"); exit(1);
        }

        char list[256];
        free_list_string(stack, list, sizeof list);
        printf("  freed block %llu -> %s\n", (unsigned long long)i, list);
    }
}

/* Pop every block via bstack_process_gen, printing each pop and the
 * resulting list, until the sentinel comes back and the sequence reports an
 * empty list. */
static void pop_demo(bstack_t *stack)
{
    printf("\n=== Pop: read, read, write under one lock with process_gen ===\n");
    for (;;) {
        uint64_t popped;
        if (pop(stack, &popped) != 0) {
            perror("bstack_process_gen"); exit(1);
        }
        char list[256];
        free_list_string(stack, list, sizeof list);
        if (popped == SENTINEL) {
            printf("  list empty -> %s\n", list);
            break;
        }
        printf("  popped block %llu -> %s\n",
               (unsigned long long)block_index(popped), list);
    }
}

/* ── Concurrent pop demo ──────────────────────────────────────────────────── */

static void *pop_thread(void *arg)
{
    bstack_t *stack = arg;
    uint64_t *result = malloc(sizeof *result);
    if (!result) { perror("malloc"); exit(1); }
    if (pop(stack, result) != 0) {
        perror("bstack_process_gen"); exit(1);
    }
    return result;
}

/* Rebuild a full free list, then have NUM_BLOCKS threads race to pop it
 * concurrently — with no allocator-level mutex.  bstack_process_gen holds
 * the BStack write lock across each thread's whole read-read-write
 * sequence, so — unlike a bstack_get_batched_gen + bstack_cas pairing — no
 * thread can ever observe a free_head that cycled back to a stale value
 * mid-sequence (the classic ABA scenario).  Every block therefore comes out
 * exactly once. */
static void concurrent_pop_demo(bstack_t *stack)
{
    printf("\n=== Concurrent pop: %d threads race process_gen, no mutex ===\n", NUM_BLOCKS);

    /* Rebuild a fresh list of NUM_BLOCKS blocks (LIFO, as in push_demo). */
    for (uint64_t i = 0; i < NUM_BLOCKS; i++) {
        uint64_t b_addr = block_offset(i);
        uint8_t  self_ptr[8];
        write_le64(self_ptr, b_addr);
        if (bstack_set(stack, b_addr, self_ptr, sizeof self_ptr) != 0) {
            perror("bstack_set"); exit(1);
        }
        if (bstack_cross_exchange(stack, b_addr, FREE_HEAD_OFFSET, BLOCK_SIZE) != 0) {
            perror("bstack_cross_exchange"); exit(1);
        }
    }
    char list[256];
    free_list_string(stack, list, sizeof list);
    printf("  rebuilt list -> %s\n", list);

    pthread_t threads[NUM_BLOCKS];
    for (int i = 0; i < NUM_BLOCKS; i++) {
        int rc = pthread_create(&threads[i], NULL, pop_thread, stack);
        if (rc != 0) { fprintf(stderr, "pthread_create: %s\n", strerror(rc)); exit(1); }
    }
    uint64_t popped[NUM_BLOCKS];
    int      n_popped = 0;
    for (int i = 0; i < NUM_BLOCKS; i++) {
        void *res = NULL;
        int rc = pthread_join(threads[i], &res);
        if (rc != 0) { fprintf(stderr, "pthread_join: %s\n", strerror(rc)); exit(1); }
        uint64_t *b_addr = res;
        if (!b_addr) { fprintf(stderr, "pthread_join returned NULL result\n"); exit(1); }
        if (*b_addr != SENTINEL)
            popped[n_popped++] = *b_addr;
        free(b_addr);
    }

    for (int i = 0; i < n_popped; i++) {
        for (int j = i + 1; j < n_popped; j++) {
            if (popped[i] == popped[j]) {
                fprintf(stderr,
                        "block at offset %llu was popped more than once — ABA corruption!\n",
                        (unsigned long long)popped[i]);
                exit(1);
            }
        }
    }
    printf("  %d threads popped %d distinct blocks — no duplicates, nothing lost\n",
           NUM_BLOCKS, n_popped);

    free_list_string(stack, list, sizeof list);
    printf("  final state -> %s\n", list);
}

/* ── Main ─────────────────────────────────────────────────────────────────── */

int main(void)
{
    const char *path = "atomic_linked_list_example.bstack";
    remove(path);

    bstack_t *stack = bstack_open(path);
    if (!stack) { perror("bstack_open"); return 1; }

    /* Reserve free_head plus NUM_BLOCKS fixed-size slots, all zeroed.  The
     * list starts empty: free_head = SENTINEL. */
    uint8_t payload[FREE_HEAD_OFFSET + BLOCK_SIZE + NUM_BLOCKS * BLOCK_SIZE] = {0};
    write_le64(payload + FREE_HEAD_OFFSET, SENTINEL);
    if (bstack_push(stack, payload, sizeof payload, NULL) != 0) {
        perror("bstack_push"); bstack_close(stack); return 1;
    }
    printf("Reserved %d blocks of %d bytes each; free list starts empty.\n\n",
           NUM_BLOCKS, BLOCK_SIZE);

    push_demo(stack);
    pop_demo(stack);
    concurrent_pop_demo(stack);

    bstack_close(stack);
    return 0;
}
