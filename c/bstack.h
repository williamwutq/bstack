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
 * bstack_set / bstack_zero / bstack_atrunc / bstack_splice /
 * bstack_try_extend / bstack_try_discard(s, n>0) / bstack_swap / bstack_cas /
 * bstack_replace / bstack_process
 * hold a write lock.  bstack_try_discard(s, 0) holds a read lock.
 * bstack_peek / bstack_get / bstack_len hold a read lock and may run
 * concurrently with each other on both platforms.
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
 * Compile with -DBSTACK_FEATURE_SET    to enable bstack_set and bstack_zero.
 * Compile with -DBSTACK_FEATURE_ATOMIC to enable bstack_atrunc, bstack_splice,
 *   bstack_try_extend, bstack_try_discard, and bstack_replace.  Both flags
 *   together also enable bstack_swap, bstack_cas, and bstack_process.
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

#ifdef BSTACK_FEATURE_ATOMIC
/*
 * Atomically cut n bytes off the tail then append buf_len bytes from buf.
 *
 * The write ordering is chosen for crash safety: when buf_len > n (net
 * extension) the file is extended before writing buf so a crash before the
 * committed-length update cleanly rolls back to the original state; when
 * buf_len <= n (net truncation or same size) buf is written first, then the
 * file is truncated, so a crash after truncation is committed by recovery.
 *
 * n = 0 with buf_len = 0 is a valid no-op.
 * Returns EINVAL if n exceeds the current payload size.
 *
 * Only available when compiled with -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_atrunc(bstack_t *bs, size_t n,
                  const uint8_t *buf, size_t buf_len);

/*
 * Atomically pop n bytes from the tail into removed, then append new_len
 * bytes from new_buf.
 *
 * removed must point to at least n bytes of caller-allocated storage;
 * it may be NULL when n == 0.  Uses the same two-path ordering strategy as
 * bstack_atrunc.
 *
 * Returns EINVAL if n exceeds the current payload size.
 *
 * Only available when compiled with -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_splice(bstack_t *bs,
                  uint8_t *removed, size_t n,
                  const uint8_t *new_buf, size_t new_len);

/*
 * Append buf_len bytes from buf only if the current logical payload size
 * equals s.
 *
 * *ok (if non-NULL) is set to 1 when the condition matched and the append was
 * performed, or 0 when the size did not match (no-op).
 * Returns 0 on success (condition-matched or not), -1 on I/O error.
 *
 * Only available when compiled with -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_try_extend(bstack_t *bs, uint64_t s,
                      const uint8_t *buf, size_t buf_len, int *ok);

/*
 * Discard n bytes only if the current logical payload size equals s.
 *
 * *ok (if non-NULL) is set to 1 when the condition matched and n bytes were
 * removed, or 0 when the size did not match (no-op).
 * When n == 0 only the read lock is taken; the file is not modified.
 * Returns EINVAL if n exceeds the current payload size (only checked when
 * the size condition matches).
 *
 * Only available when compiled with -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_try_discard(bstack_t *bs, uint64_t s, size_t n, int *ok);

/*
 * Pop n bytes from the tail, pass them read-only to the callback, then write
 * whatever the callback produces as the new tail.
 *
 * The callback signature is:
 *   int cb(const uint8_t *old, size_t old_len,
 *          uint8_t **new_buf, size_t *new_len, void *ctx)
 *
 * The callback must set *new_buf to a malloc'd buffer (or NULL when
 * *new_len == 0) and *new_len to its byte length, then return 0 on success.
 * bstack calls free(*new_buf) after writing; the caller must not free it.
 * If the callback returns -1 the operation is aborted (errno set by the
 * callback); *new_buf is not freed by bstack in that case.
 *
 * The file may grow or shrink according to *new_len; the same two-path
 * crash-safe ordering as bstack_atrunc is used.  n = 0 is valid (old is
 * NULL and old_len is 0).  Returns EINVAL if n exceeds the payload size.
 *
 * Only available when compiled with -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_replace(bstack_t *bs, size_t n,
                   int (*cb)(const uint8_t *old, size_t old_len,
                              uint8_t **new_buf, size_t *new_len,
                              void *ctx),
                   void *ctx);
#endif /* BSTACK_FEATURE_ATOMIC */

#if defined(BSTACK_FEATURE_ATOMIC) && defined(BSTACK_FEATURE_SET)
/*
 * Atomically read len bytes at logical offset into old_buf and overwrite
 * them with new_buf.  The file size is never changed.
 *
 * old_buf and new_buf must each point to at least len bytes; they may overlap
 * only if old_buf == new_buf (a no-op swap).
 * len == 0 is a valid no-op.
 * Returns EINVAL if offset + len would exceed the payload size or overflow
 * uint64_t.
 *
 * Only available when compiled with both -DBSTACK_FEATURE_SET and
 * -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_swap(bstack_t *bs, uint64_t offset,
                uint8_t *old_buf, const uint8_t *new_buf, size_t len);

/*
 * Compare-and-exchange: read len bytes at logical offset and, if they equal
 * old_buf, overwrite them with new_buf.
 *
 * *ok (if non-NULL) is set to 1 if the exchange was performed, 0 if the
 * bytes at offset differed from old_buf (no write is performed).
 * len == 0 always succeeds with *ok = 1.
 * Returns EINVAL if offset + len would exceed the payload size or overflow
 * uint64_t.
 *
 * Only available when compiled with both -DBSTACK_FEATURE_SET and
 * -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_cas(bstack_t *bs, uint64_t offset,
               const uint8_t *old_buf, const uint8_t *new_buf,
               size_t len, int *ok);

/*
 * Read bytes in the half-open logical range [start, end), pass the mutable
 * buffer to the callback for in-place modification, then write it back.
 *
 * The callback signature is:
 *   int cb(uint8_t *buf, size_t len, void *ctx)
 *
 * The callback receives a writable buffer of length (end - start), mutates
 * it in place, and returns 0 on success or -1 on failure.  The file size is
 * never changed.  start == end is a valid no-op (callback invoked with
 * buf == NULL and len == 0).  Returns EINVAL if end < start or end exceeds
 * the payload size.
 *
 * Only available when compiled with both -DBSTACK_FEATURE_SET and
 * -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_process(bstack_t *bs, uint64_t start, uint64_t end,
                   int (*cb)(uint8_t *buf, size_t len, void *ctx),
                   void *ctx);
#endif /* BSTACK_FEATURE_ATOMIC && BSTACK_FEATURE_SET */

#ifdef __cplusplus
}
#endif

#endif /* BSTACK_H */
