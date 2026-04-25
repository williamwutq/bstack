/*
 * Persistent key-value hashmap backed by two bstack files.
 *
 *   strings.bstack  — append-only null-terminated "key\0value\0" pairs
 *   index.bstack    — 256 x uint64_t slots (2048 bytes total);
 *                     slot = FNV1a(key) & 0xFF;
 *                     value = byte offset into strings.bstack, or UINT64_MAX
 *                     when empty
 *
 * No collision resolution: a second insert to the same slot overwrites the
 * previous pointer. Lookup verifies the stored key and returns not-found on
 * mismatch.
 *
 * Requires -DBSTACK_FEATURE_SET for bstack_set.
 * Build: make -C ../c example-hashmap
 */

#ifndef BSTACK_FEATURE_SET
#  error "hashmap.c requires -DBSTACK_FEATURE_SET"
#endif

#include "../c/bstack.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PHM_SLOTS      256u
#define PHM_SLOT_SIZE  8u
#define PHM_TABLE_BYTES (PHM_SLOTS * PHM_SLOT_SIZE)   /* 2048 */
#define PHM_EMPTY      UINT64_MAX

typedef struct { bstack_t *strings; bstack_t *index; } phm_t;

static phm_t phm_open(const char *strings_path, const char *index_path)
{
    phm_t m = { NULL, NULL };
    m.strings = bstack_open(strings_path);
    if (!m.strings) return m;
    m.index = bstack_open(index_path);
    if (!m.index) { bstack_close(m.strings); m.strings = NULL; return m; }

    uint64_t idx_len;
    bstack_len(m.index, &idx_len);
    if (idx_len == 0) {
        uint8_t table[PHM_TABLE_BYTES];
        memset(table, 0xFF, sizeof table); /* 0xFF…FF == UINT64_MAX per slot */
        bstack_push(m.index, table, sizeof table, NULL);
    }
    return m;
}

static void phm_close(phm_t *m)
{
    if (m->strings) bstack_close(m->strings);
    if (m->index)   bstack_close(m->index);
}

/* FNV-1a over key bytes, folded to uint8_t. */
static uint8_t phm_hash(const char *key)
{
    uint32_t h = 2166136261u;
    for (const uint8_t *p = (const uint8_t *)key; *p; p++) {
        h ^= *p;
        h *= 16777619u;
    }
    return (uint8_t)(h ^ (h >> 8) ^ (h >> 16) ^ (h >> 24));
}

/* Insert key -> value. Overwrites any previous entry at the same slot. */
static int phm_insert(phm_t *m, const char *key, const char *value)
{
    uint64_t slot = phm_hash(key);
    size_t klen = strlen(key), vlen = strlen(value);

    uint8_t *entry = malloc(klen + vlen + 2);
    if (!entry) return -1;
    memcpy(entry, key, klen);           entry[klen] = 0;
    memcpy(entry + klen + 1, value, vlen); entry[klen + 1 + vlen] = 0;

    uint64_t offset;
    int r = bstack_push(m->strings, entry, klen + vlen + 2, &offset);
    free(entry);
    if (r != 0) return -1;

    uint8_t ob[8];
    for (int i = 0; i < 8; i++) ob[i] = (uint8_t)(offset >> (8 * i));
    return bstack_set(m->index, slot * PHM_SLOT_SIZE, ob, 8);
}

/*
 * Look up key. Copies the value (null-terminated) into buf.
 * Returns 1 if found, 0 if empty slot or key mismatch, -1 on error.
 */
static int phm_get(phm_t *m, const char *key, char *buf, size_t buf_size)
{
    uint64_t slot = phm_hash(key);

    uint8_t ob[8];
    if (bstack_get(m->index,
                   slot * PHM_SLOT_SIZE,
                   slot * PHM_SLOT_SIZE + 8, ob) != 0)
        return -1;

    uint64_t offset = 0;
    for (int i = 0; i < 8; i++) offset |= (uint64_t)ob[i] << (8 * i);
    if (offset == PHM_EMPTY) return 0;

    /* Read from offset to EOF; we only need up to the second null byte. */
    uint64_t total;
    bstack_len(m->strings, &total);
    if (offset >= total) return -1;
    size_t region = (size_t)(total - offset);
    uint8_t *data = malloc(region);
    if (!data) return -1;

    size_t written;
    if (bstack_peek(m->strings, offset, data, &written) != 0) {
        free(data); return -1;
    }

    /* Verify stored key matches */
    size_t klen = strlen(key);
    if (written < klen + 1
            || memcmp(data, key, klen) != 0
            || data[klen] != 0) {
        free(data); return 0; /* hash collision, different key */
    }

    /* Extract value up to its null terminator */
    const uint8_t *val = data + klen + 1;
    size_t max  = written - klen - 1;
    size_t vlen = 0;
    while (vlen < max && val[vlen] != 0) vlen++;
    if (vlen >= buf_size) vlen = buf_size - 1;
    memcpy(buf, val, vlen);
    buf[vlen] = '\0';

    free(data);
    return 1;
}

int main(void)
{
    phm_t map = phm_open("strings.bstack", "index.bstack");
    if (!map.strings || !map.index) { perror("phm_open"); return 1; }

    phm_insert(&map, "name", "Alice");
    phm_insert(&map, "city", "Boston");
    phm_insert(&map, "lang", "Rust");

    const char *keys[] = { "name", "city", "lang", "missing" };
    char val[256];
    for (int i = 0; i < 4; i++) {
        int r = phm_get(&map, keys[i], val, sizeof val);
        if (r > 0)
            printf("%s => \"%s\"\n", keys[i], val);
        else
            printf("%s => (not found)\n", keys[i]);
    }

    uint64_t idx_len, str_len;
    bstack_len(map.index,   &idx_len);
    bstack_len(map.strings, &str_len);
    printf("\nindex size:   %llu bytes (%u slots x 8)\n",
           (unsigned long long)idx_len, PHM_SLOTS);
    printf("strings size: %llu bytes (append-only pool)\n",
           (unsigned long long)str_len);

    phm_close(&map);
    return 0;
}
