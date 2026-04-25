#include "../c/bstack.h"

#include <stdio.h>
#include <stdlib.h>

int main(void)
{
    bstack_t *stack = bstack_open("basic_example.bstack");
    if (!stack) { perror("bstack_open"); return 1; }

    uint64_t len;
    bstack_len(stack, &len);
    printf("Initial stack length: %llu\n", (unsigned long long)len);

    uint64_t offset1, offset2;
    bstack_push(stack, (const uint8_t *)"Hello, ", 7, &offset1);
    printf("Pushed 'Hello, ' at offset %llu\n", (unsigned long long)offset1);

    bstack_push(stack, (const uint8_t *)"world!", 6, &offset2);
    printf("Pushed 'world!' at offset %llu\n", (unsigned long long)offset2);

    bstack_len(stack, &len);
    printf("Stack length after pushes: %llu\n", (unsigned long long)len);

    uint8_t *all = malloc((size_t)len);
    size_t written;
    bstack_peek(stack, 0, all, &written);
    printf("All data: \"%.*s\"\n", (int)written, all);
    free(all);

    uint8_t pop_buf[6];
    bstack_pop(stack, 6, pop_buf, &written);
    printf("Popped: \"%.*s\"\n", (int)written, pop_buf);

    bstack_len(stack, &len);
    printf("Stack length after pop: %llu\n", (unsigned long long)len);

    uint8_t *remaining = malloc((size_t)len);
    bstack_peek(stack, 0, remaining, &written);
    printf("Remaining data: \"%.*s\"\n", (int)written, remaining);
    free(remaining);

    bstack_close(stack);
    return 0;
}
