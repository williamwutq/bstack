#ifndef BSTACK_H
#define BSTACK_H

#include <stddef.h>
#include <stdint.h>

/*
 * bstack — persistent, fsync-durable binary stack backed by a single file.
 *
 * File format (16-byte header followed by payload):
 *   [0..8)  magic: "BSTK" + major(1) + minor(1) + patch(2) + reserved(1)
 *   [8..16) committed payload length, little-endian uint64
 *   [16..)  payload bytes
 *
 * All logical offsets are 0-based from the start of the payload region.
 *
 * Error handling
 * --------------
 * bstack_open  returns NULL on failure; errno is set by the failing syscall,
 *              or to EINVAL for bad/short headers, or to EWOULDBLOCK when
 *              another process holds the exclusive lock.
 * All other functions return 0 on success, -1 on failure with errno set.
 *
 * Thread safety
 * -------------
 * On Unix a pthread_rwlock protects each handle; on Windows an SRWLOCK is
 * used.  bstack_push / bstack_extend / bstack_pop / bstack_discard /
 * bstack_set / bstack_zero hold a write lock; bstack_peek / bstack_get /
 * bstack_len hold a read lock and may run concurrently with each other on
 * both platforms.
 *
 * Multi-process safety
 * --------------------
 * bstack_open acquires an exclusive advisory lock on the file:
 *   Unix    — flock(LOCK_EX|LOCK_NB)
 *   Windows — LockFileEx(LOCKFILE_EXCLUSIVE_LOCK|LOCKFILE_FAIL_IMMEDIATELY)
 * The lock is released when bstack_close is called (fd / HANDLE is closed).
 *
 * Feature flags
 * -------------
 * Compile with -DBSTACK_FEATURE_SET to enable bstack_set and bstack_zero.
 */

typedef struct bstack bstack_t;

#ifdef __cplusplus
extern "C" {
#endif

/* Open or create a stack file at path.  Returns NULL on failure (errno set). */
bstack_t *bstack_open(const char *path);

/* Close the handle and release all resources (flock, rwlock, fd, memory). */
void bstack_close(bstack_t *bs);

/*
 * Append len bytes from data to the stack.
 * If out_offset is non-NULL it receives the logical byte offset where data
 * begins (i.e. the payload size before the write).
 * An empty slice (len == 0) is valid and returns the current end offset.
 */
int bstack_push(bstack_t *bs, const uint8_t *data, size_t len,
                uint64_t *out_offset);

/*
 * Append n zero bytes to the stack.
 * If out_offset is non-NULL it receives the logical byte offset where the
 * zeros begin (i.e. the payload size before the write).
 * n = 0 is valid and returns the current end offset.
 */
int bstack_extend(bstack_t *bs, size_t n, uint64_t *out_offset);

/*
 * Remove and copy the last n bytes of the stack into buf.
 * The caller must ensure buf has room for n bytes; no overflow check is done.
 * If written is non-NULL it receives n on success.
 * Returns EINVAL if n exceeds the current payload size.
 */
int bstack_pop(bstack_t *bs, size_t n,
               uint8_t *buf, size_t *written);

/*
 * Copy bytes from logical offset to end-of-payload into buf.
 * The caller must ensure buf is large enough; no overflow check is done.
 * If written is non-NULL it receives the number of bytes copied.
 * offset == bstack_len is valid and copies 0 bytes.
 * Returns EINVAL if offset exceeds the payload size.
 */
int bstack_peek(bstack_t *bs, uint64_t offset,
                uint8_t *buf, size_t *written);

/*
 * Copy the half-open logical byte range [start, end) into buf.
 * The caller must ensure buf has room for (end - start) bytes.
 * Returns EINVAL if end < start or end exceeds the payload size.
 */
int bstack_get(bstack_t *bs, uint64_t start, uint64_t end,
               uint8_t *buf);

/*
 * Discard the last n bytes from the stack without copying them into a buffer.
 * Equivalent to bstack_pop but skips the read; n = 0 is a no-op.
 * Returns EINVAL if n exceeds the current payload size.
 */
int bstack_discard(bstack_t *bs, size_t n);

/*
 * Write the current logical payload size (excluding the 16-byte header)
 * into *out_len.  Takes the read lock; concurrent calls are allowed.
 */
int bstack_len(bstack_t *bs, uint64_t *out_len);

#ifdef BSTACK_FEATURE_SET
/*
 * Overwrite len bytes in place starting at logical offset.
 * The file size is never changed.  An empty slice is a valid no-op.
 * Returns EINVAL if offset + len would exceed the payload size or overflow
 * uint64_t.
 *
 * Only available when compiled with -DBSTACK_FEATURE_SET.
 */
int bstack_set(bstack_t *bs, uint64_t offset,
               const uint8_t *data, size_t len);

/*
 * Overwrite n bytes with zeros in place starting at logical offset.
 * The file size is never changed.  n = 0 is a valid no-op.
 * Returns EINVAL if offset + n would exceed the payload size or overflow
 * uint64_t.
 *
 * Only available when compiled with -DBSTACK_FEATURE_SET.
 */
int bstack_zero(bstack_t *bs, uint64_t offset, size_t n);
#endif /* BSTACK_FEATURE_SET */

#ifdef __cplusplus
}
#endif

#endif /* BSTACK_H */
