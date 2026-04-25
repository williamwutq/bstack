#include "../c/bstack.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(void)
{
    bstack_t *stack = bstack_open("journal_example.bstack");
    if (!stack) { perror("bstack_open"); return 1; }

    const char *entries[] = {
        "INFO: Application started",
        "INFO: Connected to database",
        "WARN: High memory usage detected",
        "INFO: User login: alice",
        "ERROR: Failed to process request",
    };
    int n = (int)(sizeof entries / sizeof entries[0]);

    for (int i = 0; i < n; i++) {
        size_t elen = strlen(entries[i]);
        uint8_t *line = malloc(elen + 1); /* +1 for '\n' */
        memcpy(line, entries[i], elen);
        line[elen] = '\n';
        uint64_t offset;
        bstack_push(stack, line, elen + 1, &offset);
        printf("Logged entry at offset %llu: %s\n",
               (unsigned long long)offset, entries[i]);
        free(line);
    }

    uint64_t total;
    bstack_len(stack, &total);
    printf("\nTotal log size: %llu bytes\n", (unsigned long long)total);

    uint8_t *log_data = malloc((size_t)total);
    size_t written;
    bstack_peek(stack, 0, log_data, &written);
    printf("\nFull log contents:\n%.*s\n", (int)written, log_data);
    free(log_data);

    /* Approximate last ~50 bytes */
    uint64_t last_start = total > 50 ? total - 50 : 0;
    size_t last_len = (size_t)(total - last_start);
    uint8_t *last = malloc(last_len);
    bstack_peek(stack, last_start, last, &written);
    printf("Last entries (approx):\n%.*s\n", (int)written, last);
    free(last);

    bstack_close(stack);
    return 0;
}
