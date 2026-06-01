/*
 * Traversing a variable-sized linked list stored in bstack using
 * bstack_get_batched_gen.
 *
 * Demonstrates the generator pattern for reading linked lists where nodes have
 * different sizes.  The generator reads all data payloads into a single
 * contiguous buffer under one lock by:
 *   1. Reading a node's header (next_ptr + data_size) into a stack-allocated slot
 *   2. Parsing data_size to determine the payload length
 *   3. Issuing the next read for the data directly into the buffer
 *
 * Headers are never stored in the buffer — only data payloads are accumulated.
 * This avoids an interleaved header/data layout and eliminates the need to skip
 * over headers during reconstruction.
 *
 * Layout (each node):
 *   next_offset : u64 LE  (8 bytes) — offset to next node, or UINT64_MAX
 *   data_size   : u64 LE  (8 bytes) — payload length in bytes
 *   data        : [u8]    (variable) — node payload
 *
 * Build and run:
 *   make -C ../c example-linked_list
 */

#ifndef BSTACK_FEATURE_ATOMIC
#  error "linked_list.c requires -DBSTACK_FEATURE_ATOMIC"
#endif

#include "../c/bstack.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SENTINEL    UINT64_MAX
#define HEADER_SIZE 16  /* next_offset (8) + data_size (8) */
#define MAX_NODES   100

/* ── Little-endian helpers ───────────────────────────────────────────────── */

static uint64_t read_le64(const uint8_t *p)
{
    return (uint64_t)p[0]
        | ((uint64_t)p[1] <<  8)
        | ((uint64_t)p[2] << 16)
        | ((uint64_t)p[3] << 24)
        | ((uint64_t)p[4] << 32)
        | ((uint64_t)p[5] << 40)
        | ((uint64_t)p[6] << 48)
        | ((uint64_t)p[7] << 56);
}

static void write_le64(uint8_t *p, uint64_t v)
{
    p[0] = (uint8_t)(v);
    p[1] = (uint8_t)(v >>  8);
    p[2] = (uint8_t)(v >> 16);
    p[3] = (uint8_t)(v >> 24);
    p[4] = (uint8_t)(v >> 32);
    p[5] = (uint8_t)(v >> 40);
    p[6] = (uint8_t)(v >> 48);
    p[7] = (uint8_t)(v >> 56);
}

/* ── Generator state ─────────────────────────────────────────────────────── */

typedef struct {
    size_t   data_size;
    uint64_t next_offset;
} node_info_t;

typedef struct {
    uint8_t     header[HEADER_SIZE]; /* stack-allocated, reused each iteration */
    uint8_t    *buf;                 /* data payloads only */
    size_t      buf_cap;
    size_t      buf_len;
    /* nodes records (data_size, next_offset) so the post-traversal loop can
     * print each node individually.  In a real application you would consume
     * each payload inside the generator itself and skip this array entirely. */
    node_info_t nodes[MAX_NODES];
    uint64_t    current_offset;      /* file offset of the next node header */
    size_t      current_pos;         /* write cursor into buf */
    int         node_count;
    int         reading_header;      /* 1 = next yield is a header read */
} traversal_ctx_t;

static int ensure_capacity(traversal_ctx_t *ctx, size_t needed)
{
    if (needed <= ctx->buf_cap) return 0;
    size_t cap = ctx->buf_cap ? ctx->buf_cap : 64;
    while (cap < needed) cap *= 2;
    uint8_t *p = realloc(ctx->buf, cap);
    if (!p) return -1;
    ctx->buf     = p;
    ctx->buf_cap = cap;
    return 0;
}

/*
 * Generator callback for bstack_get_batched_gen.
 *
 * On each invocation the buffer slice requested by the previous call has
 * already been filled.  Alternates between yielding the stack-allocated
 * header slot and a data slot in the heap buffer for each node.
 */
static int list_gen(uint64_t *out_offset, uint8_t **out_buf, size_t *out_len,
                    void *userctx)
{
    traversal_ctx_t *ctx = userctx;

    if (ctx->current_offset == SENTINEL || ctx->node_count >= MAX_NODES)
        return 0;

    if (ctx->reading_header) {
        *out_offset = ctx->current_offset;
        *out_buf    = ctx->header;
        *out_len    = HEADER_SIZE;
        ctx->reading_header = 0;
        return 1;
    } else {
        /* Header has been filled; parse it. */
        uint64_t next_offset   = read_le64(ctx->header);
        uint64_t data_size     = read_le64(ctx->header + 8);
        uint64_t data_file_off = ctx->current_offset + HEADER_SIZE;

        if (ensure_capacity(ctx, ctx->current_pos + (size_t)data_size) < 0) return -1;

        ctx->nodes[ctx->node_count].data_size   = (size_t)data_size;
        ctx->nodes[ctx->node_count].next_offset = next_offset;

        *out_offset = data_file_off;
        *out_buf    = ctx->buf + ctx->current_pos;
        *out_len    = (size_t)data_size;

        ctx->current_offset  = next_offset;
        ctx->current_pos    += (size_t)data_size;
        ctx->buf_len         = ctx->current_pos;
        ctx->node_count++;
        ctx->reading_header  = 1;
        return 1;
    }
}

/* ── Main ────────────────────────────────────────────────────────────────── */

int main(void)
{
    const char *path = "linked_list_example.bstack";
    remove(path);

    bstack_t *stack = bstack_open(path);
    if (!stack) { perror("bstack_open"); return 1; }

    /* Build a linked list of text blocks by appending each as the new head.
     * Iterate in reverse so that the final head points to the first block. */
    const char *blocks[] = {
        "This example ",
        "demonstrates ",
        "the usage of ",
        "get_batched_gen ",
        "generator pattern",
    };
    int n_blocks = (int)(sizeof(blocks) / sizeof(blocks[0]));

    uint64_t head_offset = SENTINEL;
    for (int i = n_blocks - 1; i >= 0; i--) {
        size_t   data_len  = strlen(blocks[i]);
        uint64_t data_size = (uint64_t)data_len;
        size_t   node_len  = HEADER_SIZE + data_len;

        uint8_t *node = malloc(node_len);
        if (!node) { perror("malloc"); bstack_close(stack); return 1; }

        write_le64(node,           head_offset); /* next pointer */
        write_le64(node + 8,       data_size);   /* payload size */
        memcpy(node + HEADER_SIZE, blocks[i], data_len);

        if (bstack_push(stack, node, node_len, &head_offset) < 0) {
            perror("bstack_push"); free(node); bstack_close(stack); return 1;
        }
        free(node);
    }

    printf("Built %d-node list; head at offset %llu\n",
           n_blocks, (unsigned long long)head_offset);

    /* Traverse with bstack_get_batched_gen — entire chain under one lock. */
    traversal_ctx_t ctx = {
        .header         = {0},
        .buf            = NULL,
        .buf_cap        = 0,
        .buf_len        = 0,
        .current_offset = head_offset,
        .current_pos    = 0,
        .node_count     = 0,
        .reading_header = 1,
    };

    if (bstack_get_batched_gen(stack, list_gen, &ctx) < 0) {
        perror("bstack_get_batched_gen");
        free(ctx.buf);
        bstack_close(stack);
        return 1;
    }

    printf("\nTraversed %d nodes under a single lock\n", ctx.node_count);
    printf("Total buffer size: %zu bytes\n", ctx.buf_len);

    /* Print each node directly from the data-only buffer — no header-skipping needed. */
    size_t pos = 0;

    for (int i = 0; i < ctx.node_count; i++) {
        size_t   dsize = ctx.nodes[i].data_size;
        uint64_t next  = ctx.nodes[i].next_offset;
        const uint8_t *data = ctx.buf + pos;

        char next_str[32];
        if (next == SENTINEL)
            snprintf(next_str, sizeof(next_str), "null");
        else
            snprintf(next_str, sizeof(next_str), "%llu", (unsigned long long)next);

        printf("  Node %d: size=%zu, text=%.*s, next=%s\n",
               i, dsize, (int)dsize, data, next_str);

        pos += dsize;
    }

    printf("\nReconstructed text: \"%.*s\"\n", (int)ctx.buf_len, ctx.buf);
    printf("Expected: \"This example demonstrates the usage of get_batched_gen generator pattern\"\n");

    free(ctx.buf);
    bstack_close(stack);
    return 0;
}
