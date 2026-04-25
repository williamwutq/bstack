/* Demonstrates that bstack_get and bstack_pop write into caller-supplied
 * buffers — no extra allocation is needed for reads. */

#include "../c/bstack.h"

#include <stdio.h>
#include <stdlib.h>

int main(void)
{
    bstack_t *stack = bstack_open("buffer_reuse_example.bstack");
    if (!stack) { perror("bstack_open"); return 1; }

    bstack_push(stack, (const uint8_t *)"First message\n",  14, NULL);
    bstack_push(stack, (const uint8_t *)"Second message\n", 15, NULL);
    bstack_push(stack, (const uint8_t *)"Third message\n",  14, NULL);

    uint64_t len;
    bstack_len(stack, &len);
    printf("Stack length: %llu bytes\n", (unsigned long long)len);

    /* bstack_get fills a caller-supplied buffer — no allocation */
    uint8_t buf[14];
    bstack_get(stack, 0, 14, buf);
    printf("Read with bstack_get: \"%.*s\"\n", 14, buf);

    /* bstack_pop removes bytes and writes them into a caller-supplied buffer */
    uint8_t pop_buf[14];
    size_t written;
    bstack_pop(stack, 14, pop_buf, &written);   /* removes "Third message\n" */
    printf("Popped with bstack_pop: \"%.*s\"\n", (int)written, pop_buf);

    bstack_len(stack, &len);
    printf("Stack length after pop: %llu bytes\n", (unsigned long long)len);

    uint8_t *remaining = malloc((size_t)len);
    bstack_peek(stack, 0, remaining, &written);
    printf("Remaining data: \"%.*s\"\n", (int)written, remaining);
    free(remaining);

    bstack_close(stack);
    return 0;
}
