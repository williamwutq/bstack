/*
 * Variable-length "vec" records packed into a bstack.
 *
 * Layout per record:
 *   [length: uint32_t LE][data: length bytes][0x00 padding]
 *
 * The sentinel lets a reader skip past unknown data and find the next record
 * boundary without a separate index. The same framing pattern generalises to
 * variable-size nodes in graph or tree structures: prefix each node with its
 * byte length, append a 0x00 sentinel, and a linear scan walks every node
 * without needing an external index or fixed-size slots.
 */

#include "../c/bstack.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef bstack_t vec_store_t;

/* Decode a little-endian uint32 from four bytes. */
static uint32_t le32(const uint8_t b[4])
{
    return (uint32_t)b[0]
         | ((uint32_t)b[1] << 8)
         | ((uint32_t)b[2] << 16)
         | ((uint32_t)b[3] << 24);
}

/* Append a record. Returns its logical start offset, or (uint64_t)-1 on error. */
static uint64_t vs_push(vec_store_t *vs, const uint8_t *data, size_t len)
{
    uint8_t *record = malloc(5 + len);
    if (!record) return (uint64_t)-1;
    uint32_t ulen = (uint32_t)len;
    record[0] = (uint8_t)(ulen);
    record[1] = (uint8_t)(ulen >> 8);
    record[2] = (uint8_t)(ulen >> 16);
    record[3] = (uint8_t)(ulen >> 24);
    memcpy(record + 4, data, len);
    record[4 + len] = 0x00;
    uint64_t offset;
    int r = bstack_push(vs, record, 5 + len, &offset);
    free(record);
    return r == 0 ? offset : (uint64_t)-1;
}

/*
 * Read the record starting exactly at pos.
 * Fills buf with up to buf_size bytes of data; sets *data_len to the
 * record's actual data length. Returns the start offset of the next
 * record, or (uint64_t)-1 on error.
 */
static uint64_t vs_read_at(vec_store_t *vs, uint64_t pos,
                            uint8_t *buf, size_t buf_size, size_t *data_len)
{
    uint8_t lb[4];
    if (bstack_get(vs, pos, pos + 4, lb) != 0) return (uint64_t)-1;
    uint32_t len = le32(lb);
    *data_len = len;
    if (len > 0) {
        size_t copy = len < buf_size ? len : buf_size;
        if (bstack_get(vs, pos + 4, pos + 4 + copy, buf) != 0)
            return (uint64_t)-1;
    }
    return pos + 4 + (uint64_t)len + 1;
}

/*
 * Walk to the nth record (0-indexed) and fill buf with its data.
 * Returns 0 on success, -1 on error.
 */
static int vs_get_nth(vec_store_t *vs, size_t n,
                      uint8_t *buf, size_t buf_size, size_t *data_len)
{
    uint64_t pos = 0;
    for (size_t i = 0; i < n; i++) {
        uint8_t lb[4];
        if (bstack_get(vs, pos, pos + 4, lb) != 0) return -1;
        pos += 4 + (uint64_t)le32(lb) + 1;
    }
    uint64_t next = vs_read_at(vs, pos, buf, buf_size, data_len);
    return next != (uint64_t)-1 ? 0 : -1;
}

/*
 * Scan from the beginning and return the first record whose start offset
 * is >= min_pos. Sets *found_at and fills buf. Returns 0 on success, -1
 * if no such record exists.
 */
static int vs_get_after(vec_store_t *vs, uint64_t min_pos,
                        uint64_t *found_at,
                        uint8_t *buf, size_t buf_size, size_t *data_len)
{
    uint64_t total;
    if (bstack_len(vs, &total) != 0) return -1;
    uint64_t cur = 0;
    while (cur < total) {
        uint8_t lb[4];
        if (bstack_get(vs, cur, cur + 4, lb) != 0) return -1;
        uint32_t len = le32(lb);
        if (cur >= min_pos) {
            *found_at = cur;
            *data_len = len;
            if (len > 0) {
                size_t copy = len < buf_size ? len : buf_size;
                if (bstack_get(vs, cur + 4, cur + 4 + copy, buf) != 0)
                    return -1;
            }
            return 0;
        }
        cur += 4 + (uint64_t)len + 1;
    }
    return -1;
}

int main(void)
{
    vec_store_t *store = bstack_open("vec_store_example.bstack");
    if (!store) { perror("bstack_open"); return 1; }

    const char *records[] = { "alpha", "bb", "ccc", "dddd" };
    uint64_t offsets[4];
    for (int i = 0; i < 4; i++) {
        offsets[i] = vs_push(store,
                             (const uint8_t *)records[i],
                             strlen(records[i]));
    }
    printf("record start offsets: [%llu, %llu, %llu, %llu]\n",
           (unsigned long long)offsets[0], (unsigned long long)offsets[1],
           (unsigned long long)offsets[2], (unsigned long long)offsets[3]);

    uint64_t total;
    bstack_len(store, &total);
    printf("total store size:     %llu bytes\n", (unsigned long long)total);

    /* Read by index */
    printf("\nby index:\n");
    uint8_t buf[64];
    for (int i = 0; i < 4; i++) {
        size_t data_len;
        vs_get_nth(store, (size_t)i, buf, sizeof buf, &data_len);
        printf("  [%d] \"%.*s\"\n", i, (int)data_len, buf);
    }

    /* Sequential scan using vs_read_at */
    printf("\nsequential scan:\n");
    uint64_t pos = 0;
    while (pos < total) {
        size_t data_len;
        uint64_t next = vs_read_at(store, pos, buf, sizeof buf, &data_len);
        printf("  @ %4llu: \"%.*s\"\n",
               (unsigned long long)pos, (int)data_len, buf);
        pos = next;
    }

    /* Find first record at or after a raw file offset */
    uint64_t search_from = offsets[1] + 1;
    uint64_t found_at;
    size_t data_len;
    vs_get_after(store, search_from, &found_at, buf, sizeof buf, &data_len);
    printf("\nfirst record at or after offset %llu: \"%.*s\" (starts at %llu)\n",
           (unsigned long long)search_from,
           (int)data_len, buf,
           (unsigned long long)found_at);

    bstack_close(store);
    return 0;
}
