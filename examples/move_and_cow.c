/*
 * Move semantics with bstack_cross_exchange and copy-on-write with bstack_copy.
 *
 * Scenario 1: Move semantics with bstack_cross_exchange
 *   A fixed-size message queue where messages are 32-byte slots. When a
 *   consumer "takes" a message, we swap it with a sentinel value (all zeros)
 *   to mark it as consumed, atomically retrieving the old content.
 *
 * Scenario 2: Copy-on-write with bstack_copy
 *   A versioned key-value store where updates create new versions by copying
 *   the old record and appending the new one, leaving the original intact.
 *
 * Build and run:
 *   make -C ../c example-move_and_cow
 */

#if !defined(BSTACK_FEATURE_SET) || !defined(BSTACK_FEATURE_ATOMIC)
#  error "move_and_cow.c requires -DBSTACK_FEATURE_SET -DBSTACK_FEATURE_ATOMIC"
#endif

#include "../c/bstack.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#define MSG_SIZE 32

static void move_semantics_demo(void)
{
    printf("=== Move semantics with cross_exchange ===\n");
    
    bstack_t *stack = bstack_open("move_example.bstack");
    if (!stack) { perror("bstack_open"); exit(1); }

    /* Create a message queue with 4 fixed-size slots. */
    for (int i = 0; i < 4; i++) {
        uint8_t msg[MSG_SIZE];
        memset(msg, 0, MSG_SIZE);
        snprintf((char *)msg, MSG_SIZE, "Message #%d", i + 1);
        bstack_push(stack, msg, MSG_SIZE, NULL);
        printf("Enqueued: %s\n", (char *)msg);
    }

    /* "Take" message #2 (offset 32) by swapping it with zeros. */
    uint8_t sentinel[MSG_SIZE];
    memset(sentinel, 0, MSG_SIZE);
    uint64_t sentinel_offset;
    bstack_push(stack, sentinel, MSG_SIZE, &sentinel_offset);
    
    printf("\nTaking message #2 (offset 32)...\n");
    
    /* bstack_cross_exchange swaps [a, a+n) with [b, b+n). */
    if (bstack_cross_exchange(stack, MSG_SIZE, sentinel_offset, MSG_SIZE) != 0) {
        perror("bstack_cross_exchange");
        bstack_close(stack);
        exit(1);
    }
    
    /* Now slot #2 is zeros, and the sentinel area holds the old message. */
    uint8_t taken[MSG_SIZE];
    bstack_get(stack, sentinel_offset, sentinel_offset + MSG_SIZE, taken);
    printf("Taken: %s\n", (char *)taken);
    
    /* Show the queue state. */
    printf("\nQueue after take:\n");
    for (int i = 0; i < 4; i++) {
        uint8_t msg[MSG_SIZE + 1];
        uint64_t offset = (uint64_t)i * MSG_SIZE;
        bstack_get(stack, offset, offset + MSG_SIZE, msg);
        msg[MSG_SIZE] = '\0';
        
        /* Trim trailing nulls for display. */
        size_t len = strlen((char *)msg);
        if (len == 0) {
            printf("  Slot %d: <empty>\n", i);
        } else {
            printf("  Slot %d: %s\n", i, (char *)msg);
        }
    }

    bstack_close(stack);
}

static void copy_on_write_demo(void)
{
    printf("\n=== Copy-on-write with copy ===\n");
    
    bstack_t *stack = bstack_open("cow_example.bstack");
    if (!stack) { perror("bstack_open"); exit(1); }

    /* Version 1: initial key-value record (16 bytes: 8-byte key + 8-byte value). */
    uint64_t key = 42;
    uint64_t value_v1 = 1000;
    uint8_t record[16];
    memcpy(record, &key, 8);
    memcpy(record + 8, &value_v1, 8);
    
    uint64_t offset_v1;
    bstack_push(stack, record, 16, &offset_v1);
    printf("v1: key=%llu, value=%llu at offset %llu\n", 
           (unsigned long long)key, (unsigned long long)value_v1, 
           (unsigned long long)offset_v1);

    /* Version 2: copy the old record and append an updated one. */
    uint64_t value_v2 = 2000;
    
    /* First, extend the stack to make room for the copy. */
    uint8_t zeros[16] = {0};
    bstack_push(stack, zeros, 16, NULL);
    uint64_t offset_v2 = offset_v1 + 16; /* the new copy location */
    
    /* Now copy v1 to v2. */
    if (bstack_copy(stack, offset_v1, offset_v2, 16) != 0) {
        perror("bstack_copy");
        bstack_close(stack);
        exit(1);
    }
    
    /* Now overwrite just the value field (bytes 8..16) in the new copy. */
    if (bstack_set(stack, offset_v2 + 8, (const uint8_t *)&value_v2, 8) != 0) {
        perror("bstack_set");
        bstack_close(stack);
        exit(1);
    }
    printf("v2: key=%llu, value=%llu at offset %llu (copied from v1)\n", 
           (unsigned long long)key, (unsigned long long)value_v2, 
           (unsigned long long)offset_v2);

    /* Version 3: another update. */
    uint64_t value_v3 = 3000;
    
    /* Extend for v3. */
    bstack_push(stack, zeros, 16, NULL);
    uint64_t offset_v3 = offset_v2 + 16;
    
    /* Copy v2 to v3. */
    if (bstack_copy(stack, offset_v2, offset_v3, 16) != 0) {
        perror("bstack_copy");
        bstack_close(stack);
        exit(1);
    }
    
    if (bstack_set(stack, offset_v3 + 8, (const uint8_t *)&value_v3, 8) != 0) {
        perror("bstack_set");
        bstack_close(stack);
        exit(1);
    }
    printf("v3: key=%llu, value=%llu at offset %llu (copied from v2)\n", 
           (unsigned long long)key, (unsigned long long)value_v3, 
           (unsigned long long)offset_v3);

    /* All versions are preserved. */
    printf("\nAll versions in the stack:\n");
    uint64_t offsets[3] = {offset_v1, offset_v2, offset_v3};
    for (int i = 0; i < 3; i++) {
        uint8_t rec[16];
        bstack_get(stack, offsets[i], offsets[i] + 16, rec);
        
        uint64_t k, v;
        memcpy(&k, rec, 8);
        memcpy(&v, rec + 8, 8);
        
        printf("  v%d: key=%llu, value=%llu\n", 
               i + 1, (unsigned long long)k, (unsigned long long)v);
    }

    bstack_close(stack);
}

int main(void)
{
    move_semantics_demo();
    copy_on_write_demo();
    return 0;
}
