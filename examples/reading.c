#include "../c/bstack.h"

#include <stdio.h>
#include <stdlib.h>

int main(void)
{
    bstack_t *stack = bstack_open("concurrent_read_example.bstack");
    if (!stack) { perror("bstack_open"); return 1; }

    for (int i = 0; i < 10; i++) {
        char buf[16];
        int n = snprintf(buf, sizeof buf, "Entry %d\n", i);
        bstack_push(stack, (const uint8_t *)buf, (size_t)n, NULL);
    }

    uint64_t total;
    bstack_len(stack, &total);
    printf("Stack contains %llu bytes\n", (unsigned long long)total);

    uint8_t *all = malloc((size_t)total);
    size_t written;
    bstack_peek(stack, 0, all, &written);
    printf("All data:\n%.*s\n", (int)written, all);
    free(all);

    /* Read a specific half-open byte range */
    uint8_t first_five[5];
    bstack_get(stack, 0, 5, first_five);
    printf("First 5 bytes: \"%.*s\"\n", 5, first_five);

    /* Read 10 bytes starting at offset 10 */
    uint8_t middle[10];
    bstack_get(stack, 10, 20, middle);
    printf("Bytes 10-20: \"%.*s\"\n", 10, middle);

    /* Read 20 bytes from offset 5 — equivalent of peek_into */
    uint8_t peeked[20];
    bstack_get(stack, 5, 25, peeked);
    printf("Peeked into buffer: \"%.*s\"\n", 20, peeked);

    bstack_close(stack);
    return 0;
}
