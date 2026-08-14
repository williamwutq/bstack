#ifndef BSTACK_H
#define BSTACK_H

#include <stddef.h>
#include <stdint.h>

/*
 * bstack — persistent, fsync-durable binary stack backed by a single file.
 *
 * File format (32-byte header followed by payload):
 *   [0..8)   magic: "BSTK" + major(0) + minor(4) + patch(1) + reserved(0)
 *   [8..16)  committed payload length (clen), little-endian uint64
 *   [16..24) wip_ptr: write-in-progress journal target (0 when idle), LE uint64
 *   [24..32) wip_aux: write-in-progress journal mode, LE uint64
 *   [32..)   payload bytes
 *
 * wip_ptr / wip_aux hold the write-in-progress journal that makes in-place
 * mutations crash-atomic; both are zero in the steady state.  On open, an
 * interrupted in-place write is replayed or rolled back (see algos/WIP.md).  A
 * batch of several in-place writes (bstack_set_batched / bstack_inplace_gen) uses
 * the multi-write sentinel in wip_aux with wip_ptr left 0, staging the writes as
 * [s | e | data] blocks past clen.
 * Legacy 0.1.x files (16-byte header) are upgraded by bstack_migrate.
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
 * used.  bstack_push / bstack_extend / bstack_resize / bstack_ensure /
 * bstack_pop / bstack_discard /
 * bstack_set / bstack_zero / bstack_repeat / bstack_atrunc / bstack_splice /
 * bstack_try_extend / bstack_try_extend_zeros / bstack_try_discard(s, n>0) /
 * bstack_ensure_with / bstack_swap / bstack_cas / bstack_replace / bstack_process /
 * bstack_process_gen / bstack_set_batched / bstack_inplace_gen /
 * bstack_cross_exchange / bstack_copy /
 * bstack_eq_crds / bstack_ne_crds /
 * bstack_masked_eq_crds / bstack_masked_ne_crds
 * hold a write lock.  bstack_try_discard(s, 0) holds a read lock.
 * bstack_peek / bstack_get / bstack_get_batched / bstack_get_batched_gen /
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
 * Compile with -DBSTACK_FEATURE_SET    to enable bstack_set, bstack_zero, and
 *   bstack_repeat.
 * Compile with -DBSTACK_FEATURE_ATOMIC to enable bstack_atrunc, bstack_splice,
 *   bstack_try_extend, bstack_try_extend_zeros, bstack_try_discard,
 *   bstack_try_extend_sparse, bstack_try_extend_sparse_batched, bstack_ensure_with,
 *   bstack_replace, bstack_get_batched, and bstack_get_batched_gen.  Both
 *   flags together also enable bstack_swap, bstack_cas, bstack_process,
 *   bstack_process_gen, bstack_set_batched, bstack_inplace_gen, bstack_gen_op_t,
 *   bstack_cross_exchange, bstack_copy, bstack_eq_crds, bstack_ne_crds,
 *   bstack_masked_eq_crds, and bstack_masked_ne_crds.
 */

typedef struct bstack bstack_t;

/*
 * Descriptor for one entry in a batched operation: a logical byte offset, a
 * buffer pointer, and a byte count.  Used as a read destination by
 * bstack_get_batched, and as a write source by bstack_set_batched,
 * bstack_extend_sparse_batched, and bstack_try_extend_sparse_batched (where
 * offset is interpreted relative to the current tail).
 */
typedef struct {
    uint64_t offset;
    uint8_t *buf;
    size_t   len;
} bstack_iovec_t;

#ifdef __cplusplus
extern "C" {
#endif

/* Open or create a stack file at path.  Returns NULL on failure (errno set). */
bstack_t *bstack_open(const char *path);

/* Close the handle and release all resources (flock, rwlock, fd, memory). */
void bstack_close(bstack_t *bs);

/*
 * Upgrade a legacy pre-0.4.0 (0.1.x, 16-byte header) file at path to the
 * current 0.4.0 layout (32-byte header), in place.  The file is rewritten into
 * a sibling "<path>.migrating" (a fresh 0.4.0 header followed by the old payload
 * shifted from offset 16 to offset 32) which is then swapped in.  The committed
 * length is preserved (clamped to the bytes actually present).  The caller must
 * not hold the file open elsewhere.  On success path is a valid 0.4.0 file.
 * Returns 0 on success, -1 (errno = EINVAL) if path is not a legacy 0.1.x file
 * (wrong magic or shorter than the 16-byte legacy header), or -1 (errno set) on
 * any I/O error.
 */
int bstack_migrate(const char *path);

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
 * Sparsely grow the payload by length bytes, writing the buf_len bytes at buf at
 * the start of the freshly grown region and leaving the remaining
 * length - buf_len bytes zero.
 *
 * The whole length is realised with a single ftruncate (the OS zero-fills), so
 * the tail past buf costs no write I/O — a cheaper alternative to a bstack_push
 * of a large mostly-zero buffer when only a small prefix carries real data.
 * If out_offset is non-NULL it receives the logical byte offset where the growth
 * begins (the payload size before the call, the anchor buf is written at).
 *
 * length = 0 is valid only when buf_len == 0; it writes nothing and returns the
 * current end offset.  A NULL buf is permitted only when buf_len == 0.  No
 * journal is needed: the grown region sits beyond the committed length, so a
 * crash before the commit rolls back by truncation (like bstack_push).
 * Returns EINVAL if buf_len exceeds length or if the payload size plus length
 * overflows uint64_t; otherwise -1 (errno set) on I/O error.
 */
int bstack_extend_sparse(bstack_t *bs, const uint8_t *buf, size_t buf_len,
                         uint64_t length, uint64_t *out_offset);

/*
 * Sparsely grow the payload by length bytes, scattering count buffers into the
 * freshly grown region and leaving the gaps between them zero.
 *
 * writes is an array of count bstack_iovec_t descriptors; each (offset, buf, len)
 * writes its len bytes at logical offset tail + offset, where tail is the payload
 * size before the growth (the returned offset).  The bytes not covered by any
 * buffer read back as zero.  Empty (len == 0) entries are ignored.  As with
 * bstack_extend_sparse, the whole length is realised with a single ftruncate, so
 * the zero gaps cost no write I/O.
 *
 * The writes must be pairwise non-overlapping and each must fit within
 * [0, length); a violation is rejected.  count = 0 (or writes = NULL with
 * count = 0) extends by length with no data (equivalent to bstack_extend).
 * length = 0 is valid only when every buffer is empty.
 * Returns EINVAL if any offset + len overflows uint64_t or exceeds length, if two
 * writes overlap, or if the payload size plus length overflows uint64_t;
 * otherwise -1 (errno set) on I/O error.
 */
int bstack_extend_sparse_batched(bstack_t *bs,
                                 const bstack_iovec_t *writes, size_t count,
                                 uint64_t length, uint64_t *out_offset);

/*
 * Grow or shrink the payload to exactly target bytes.  Any newly grown region
 * is filled with zeros.  If out_initial_len is non-NULL it receives the
 * payload size before the resize.  target equal to the current payload size
 * is a valid no-op.
 * Returns EINVAL if shrinking would cut into the locked region
 * [0, bstack_locked_len).
 */
int bstack_resize(bstack_t *bs, uint64_t target, uint64_t *out_initial_len);

/*
 * Grow the payload to at least target bytes, filling the new region with
 * zeros.  A no-op if the payload is already target bytes or longer.
 * If out_initial_len is non-NULL it receives the payload size before the
 * call.  The grow-only, unconditional counterpart of bstack_resize.
 */
int bstack_ensure(bstack_t *bs, uint64_t target, uint64_t *out_initial_len);

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
 * Write the current logical payload size (excluding the 32-byte header)
 * into *out_len.  This value is cached in memory, so no syscall is made;
 * it takes the read lock, so it can run concurrently with other readers
 * but blocks while a writer is in progress.
 */
int bstack_len(bstack_t *bs, uint64_t *out_len);

/*
 * Write 1 into *out_empty if the payload is empty (len == 0), else 0.
 * Like bstack_len, this is a cached read under the read lock and makes
 * no syscall.
 */
int bstack_is_empty(bstack_t *bs, int *out_empty);

/*
 * Return the current locked length.  0 means no bytes are locked.
 * The locked region is [0, locked_len).  All bytes within this range are
 * permanently immutable: writes and shrink operations that would touch them
 * return EINVAL, and reads to ranges entirely within it skip the rwlock on
 * Unix and Windows.
 */
uint64_t bstack_locked_len(bstack_t *bs);

/*
 * Extend the locked region to cover [0, n).
 * n must be ≥ the current locked length and ≤ the current payload length.
 * After this call, reads to [0, n) are lock-free on Unix and Windows, and
 * all write and shrink operations that would touch [0, n) return EINVAL.
 * Acquires the exclusive write lock to ensure all in-flight writes to
 * [0, n) have completed before the region is declared immutable.
 * Note: on stacks opened with bstack_open_cached, this call also fills the
 * in-memory cache and is therefore significantly more expensive.
 * Returns EINVAL if n is less than the current locked length (partition can
 * only grow) or if n exceeds the current payload length.
 */
int bstack_lock_up_to(bstack_t *bs, uint64_t n);

/*
 * Open a bstack and immediately lock the first n bytes.
 * Equivalent to bstack_open followed by bstack_lock_up_to, but expressed
 * as a single call for the common pattern where the locked region is known
 * ahead of time (e.g. a fixed-size metadata block whose size is a
 * compile-time or configuration constant).
 * Returns NULL on failure (errno set); EINVAL if n exceeds the payload
 * length of the opened file.
 */
bstack_t *bstack_open_locked_up_to(const char *path, uint64_t n);

/*
 * Open or create a stack file at path with the in-memory locked-region
 * cache enabled.  Behaves identically to bstack_open in all other respects.
 *
 * Once the cache is enabled, each subsequent bstack_lock_up_to(bs, n) call
 * reads the newly locked bytes from disk into a heap buffer so that future
 * bstack_get calls whose range falls entirely within the locked region are
 * served by copying from that buffer with no syscall.
 *
 * Performance: bstack_lock_up_to is significantly more expensive on cached
 * stacks because it must read up to n bytes from disk before returning.
 *
 * Returns NULL on failure (errno set).
 */
bstack_t *bstack_open_cached(const char *path);

/*
 * Open a cached bstack and immediately lock the first n bytes.
 * Equivalent to bstack_open_cached followed by bstack_lock_up_to.
 * Returns NULL on failure (errno set); EINVAL if n exceeds the payload
 * length of the opened file.
 */
bstack_t *bstack_open_locked_up_to_cached(const char *path, uint64_t n);

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
 * uint64_t.  Equivalent to bstack_repeat with the single-byte pattern 0x00.
 *
 * Only available when compiled with -DBSTACK_FEATURE_SET.
 */
int bstack_zero(bstack_t *bs, uint64_t offset, size_t n);

/*
 * Fill count copies of pattern in place starting at logical offset — i.e.
 * overwrite [offset, offset + count * pattern_len) with the pattern repeated
 * back to back.  The file size is never changed.  An empty pattern
 * (pattern_len == 0) or count == 0 is a valid no-op.
 *
 * This is the general form of bstack_zero.  Only the pattern and count are
 * journaled, so a crash-safe fill of a large region costs a fixed-size journal
 * (8 + pattern_len bytes) rather than one proportional to the region.  A fill
 * confined to one aligned block takes the single-block atomic path.
 *
 * Returns EINVAL if count * pattern_len or offset + count * pattern_len would
 * overflow uint64_t, or if the filled region would exceed the payload size.
 *
 * Only available when compiled with -DBSTACK_FEATURE_SET.
 */
int bstack_repeat(bstack_t *bs, uint64_t offset,
                  const uint8_t *pattern, size_t pattern_len, uint64_t count);
#endif /* BSTACK_FEATURE_SET */

#ifdef BSTACK_FEATURE_ATOMIC
/*
 * Atomically cut n bytes off the tail then append buf_len bytes from buf.
 *
 * Crash-atomic: after a crash the tail holds either its old contents or the
 * full replacement, never a mix.  The commit dispatches on the shape of the
 * replacement (see algos/WIP.md): a pure truncation (buf_len == 0) drops the
 * tail; a pure append (n == 0) writes beyond the committed end and commits with
 * the clen write; a same-length replace overwrites in place via the Set journal
 * (or a single-block atomic write); a length change (buf_len != n, both > 0)
 * uses the splice journal, which stages the new tail past the live payload,
 * replays it into place, and commits the new length in one atomic header write.
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
 * it may be NULL when n == 0.  The replacement commits with the same
 * crash-atomic, shape-dispatched strategy as bstack_atrunc.
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
 * The file may grow or shrink according to *new_len; the replacement commits
 * with the same crash-atomic, shape-dispatched strategy as bstack_atrunc.
 * n = 0 is valid (old is NULL and old_len is 0).  Returns EINVAL if n exceeds
 * the payload size.
 *
 * Only available when compiled with -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_replace(bstack_t *bs, size_t n,
                   int (*cb)(const uint8_t *old, size_t old_len,
                              uint8_t **new_buf, size_t *new_len,
                              void *ctx),
                   void *ctx);

/*
 * Append n zero bytes only if the current logical payload size equals s.
 *
 * *ok (if non-NULL) is set to 1 when the condition matched and n zero bytes
 * were appended, or 0 when the size did not match (no-op).
 * n = 0 with the size condition matching sets *ok = 1 without I/O.
 * Returns 0 on success (condition-matched or not), -1 on I/O error.
 *
 * Only available when compiled with -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_try_extend_zeros(bstack_t *bs, uint64_t s, size_t n, int *ok);

/*
 * Sparsely grow the payload by length bytes with buf at the start, only if the
 * current logical payload size equals s.  Size-guarded counterpart of
 * bstack_extend_sparse.
 *
 * *ok (if non-NULL) is set to 1 when the condition matched and the growth was
 * applied (or length == 0 and no I/O was needed), or 0 when the size did not
 * match (no-op).  A malformed request (buf_len exceeding length) is rejected with
 * EINVAL regardless of whether the size matches, so it always surfaces rather
 * than being masked by a size mismatch.
 * Returns EINVAL if buf_len exceeds length or if the payload size plus length
 * overflows uint64_t; otherwise -1 (errno set) on I/O error.
 *
 * Only available when compiled with -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_try_extend_sparse(bstack_t *bs, uint64_t s,
                             const uint8_t *buf, size_t buf_len,
                             uint64_t length, int *ok);

/*
 * Sparsely grow the payload by length bytes, scattering count buffers into the
 * grown region, only if the current logical payload size equals s.  Size-guarded
 * counterpart of bstack_extend_sparse_batched.
 *
 * *ok (if non-NULL) is set to 1 when the condition matched and the growth was
 * applied (or length == 0 and no I/O was needed), or 0 when the size did not
 * match (no-op).  A malformed batch (overlapping writes, or a write past length)
 * is rejected with EINVAL regardless of whether the size matches.
 * Returns EINVAL if any offset + len overflows uint64_t or exceeds length, if two
 * writes overlap, or if the payload size plus length overflows uint64_t;
 * otherwise -1 (errno set) on I/O error.
 *
 * Only available when compiled with -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_try_extend_sparse_batched(bstack_t *bs, uint64_t s,
                                     const bstack_iovec_t *writes, size_t count,
                                     uint64_t length, int *ok);

/*
 * Grow the payload to at least target bytes, only if it is currently
 * shorter, handing the freshly allocated tail to the callback for
 * initialization before it is committed.
 *
 * The callback signature is:
 *   int cb(uint8_t *buf, size_t len, void *ctx)
 *
 * If the payload is already target bytes or longer, cb is not called and
 * nothing changes.  Otherwise cb receives a zero-filled buffer of length
 * target - old_len — exactly the region bstack_ensure would have appended —
 * mutates it in place, and returns 0 on success or -1 on failure (errno set
 * by the callback), which aborts the call before anything is written.
 *
 * If out_initial_len is non-NULL it receives the payload size before the
 * call.  Returns ENOMEM if target - old_len exceeds SIZE_MAX on this
 * platform (only reachable where size_t is narrower than uint64_t).
 *
 * Only available when compiled with -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_ensure_with(bstack_t *bs, uint64_t target,
                       int (*cb)(uint8_t *buf, size_t len, void *ctx),
                       void *ctx, uint64_t *out_initial_len);

/*
 * Read multiple logical ranges into caller-provided buffers in a single
 * lock acquisition.
 *
 * entries is an array of n_entries bstack_iovec_t descriptors.  For each
 * entry, entries[i].len bytes are read from logical offset entries[i].offset
 * into entries[i].buf.  n_entries == 0 is a valid no-op.
 *
 * All reads happen under the same shared read lock, so no write can
 * interleave between them.
 *
 * Returns EINVAL if any entry has offset + len overflowing uint64_t or
 * exceeding the current payload size.
 *
 * Only available when compiled with -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_get_batched(bstack_t *bs,
                       const bstack_iovec_t *entries, size_t n_entries);

/*
 * Read a dependent chain of logical ranges in a single lock acquisition.
 *
 * gen is called repeatedly.  Each call should populate *out_offset,
 * *out_buf, and *out_len to request the next read, then return 1.  When
 * gen is called, the buffer supplied by the previous call (if any) has
 * already been filled with its data.  To stop the chain, gen returns 0.
 * gen may return -1 on error (errno must be set); the operation aborts.
 *
 * The generator callback signature:
 *   int gen(uint64_t *out_offset, uint8_t **out_buf, size_t *out_len,
 *           void *ctx)
 *
 * All reads happen under the same shared read lock.
 *
 * Returns EINVAL if any yielded offset + len overflows uint64_t or exceeds
 * the current payload size.  Returns -1 (errno set) if gen returns -1.
 *
 * Only available when compiled with -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_get_batched_gen(bstack_t *bs,
                           int (*gen)(uint64_t *out_offset, uint8_t **out_buf,
                                      size_t *out_len, void *ctx),
                           void *ctx);
#endif /* BSTACK_FEATURE_ATOMIC */

#if defined(BSTACK_FEATURE_ATOMIC) && defined(BSTACK_FEATURE_SET)
/*
 * Discriminant for bstack_gen_op_t — identifies which member of the union
 * is populated.
 *
 * Only available when compiled with both -DBSTACK_FEATURE_SET and
 * -DBSTACK_FEATURE_ATOMIC.
 */
typedef enum {
    /* Read u.read.len bytes starting at logical u.read.offset into
     * u.read.buf. */
    BSTACK_GEN_READ,
    /* Write u.write.len bytes from u.write.data to logical u.write.offset,
     * ending the sequence. */
    BSTACK_GEN_WRITE,
    /* Atomically exchange u.swap.len bytes at u.swap.a_offset with
     * u.swap.len bytes at u.swap.b_offset, ending the sequence. */
    BSTACK_GEN_SWAP,
    /* Append u.push.len bytes from u.push.data to the end of the file,
     * growing the payload, and ending the sequence. */
    BSTACK_GEN_PUSH,
    /* Remove the last u.pop.len bytes from the end of the file into
     * u.pop.buf, shrinking the payload, and ending the sequence. */
    BSTACK_GEN_POP,
    /* Cut u.splice.n bytes off the tail then append u.splice.new_buf, changing
     * the payload length, and ending the sequence.  If u.splice.removed is
     * non-NULL the popped bytes are read into it first (in-sequence
     * bstack_splice); if NULL they are discarded (in-sequence bstack_atrunc). */
    BSTACK_GEN_SPLICE,
    /* Sparsely grow the payload by u.sparse.length bytes, scattering the
     * u.sparse.count descriptors in u.sparse.writes (each offset relative to the
     * current tail) into the freshly grown region and leaving the gaps zero,
     * then ending the sequence — the in-sequence equivalent of
     * bstack_extend_sparse_batched. */
    BSTACK_GEN_SPARSE,
    /* Write the current logical payload size into *u.len.out, then call gen
     * again — does not end the sequence. */
    BSTACK_GEN_LEN,
} bstack_gen_op_kind_t;

/*
 * A request to read from, or write to, a region of the payload, yielded by
 * the generator callback passed to bstack_process_gen.
 *
 * kind selects which member of u is populated:
 *
 * - BSTACK_GEN_READ: read u.read.len bytes starting at logical u.read.offset
 *   into u.read.buf.
 * - BSTACK_GEN_WRITE: write u.write.len bytes from u.write.data to logical
 *   u.write.offset, ending the sequence.
 * - BSTACK_GEN_SWAP: atomically exchange u.swap.len bytes at u.swap.a_offset
 *   with u.swap.len bytes at u.swap.b_offset, ending the sequence.  The
 *   regions must not overlap.
 * - BSTACK_GEN_PUSH: append u.push.len bytes from u.push.data to the end of
 *   the file, growing the payload, and ending the sequence — the in-sequence
 *   equivalent of bstack_push.
 * - BSTACK_GEN_POP: remove the last u.pop.len bytes from the end of the file
 *   into u.pop.buf, shrinking the payload, and ending the sequence — the
 *   in-sequence equivalent of bstack_pop.  If u.pop.buf is NULL the bytes are
 *   dropped without being copied out — the in-sequence equivalent of
 *   bstack_discard.
 * - BSTACK_GEN_SPLICE: cut u.splice.n bytes off the tail then append
 *   u.splice.new_len bytes from u.splice.new_buf, changing the payload length
 *   by new_len - n, and ending the sequence — the in-sequence equivalent of
 *   bstack_splice.  If u.splice.removed is non-NULL the popped bytes are read
 *   into it first (which must hold n bytes); if NULL they are discarded, which
 *   is the in-sequence equivalent of bstack_atrunc (mirroring how a NULL
 *   u.pop.buf turns a POP into a discard).
 * - BSTACK_GEN_SPARSE: sparsely grow the payload by u.sparse.length bytes,
 *   scattering the u.sparse.count (offset, buf, len) descriptors in
 *   u.sparse.writes into the freshly grown region — each written at logical
 *   offset tail + offset, with the gaps left zero — and ending the sequence, the
 *   in-sequence equivalent of bstack_extend_sparse_batched (a single descriptor
 *   at offset 0 covers bstack_extend_sparse).  The writes must be pairwise
 *   non-overlapping and each must fit within [0, length); empty entries are
 *   ignored.
 * - BSTACK_GEN_LEN: write the current logical payload size into
 *   *u.len.out and call gen again — the in-sequence equivalent of
 *   bstack_len.  Does not end the sequence.
 *
 * BSTACK_GEN_WRITE, BSTACK_GEN_SWAP, BSTACK_GEN_PUSH, BSTACK_GEN_POP,
 * BSTACK_GEN_SPLICE, and BSTACK_GEN_SPARSE are the only mutating kinds — exactly
 * one is permitted per bstack_process_gen call, and any one of them ends the
 * sequence immediately.
 *
 * Only available when compiled with both -DBSTACK_FEATURE_SET and
 * -DBSTACK_FEATURE_ATOMIC.
 */
typedef struct {
    bstack_gen_op_kind_t kind;
    union {
        struct {
            uint64_t offset;
            uint8_t *buf;
            size_t   len;
        } read;
        struct {
            uint64_t offset;
            const uint8_t *data;
            size_t   len;
        } write;
        struct {
            uint64_t a_offset;
            uint64_t b_offset;
            uint64_t len;
        } swap;
        struct {
            const uint8_t *data;
            size_t   len;
        } push;
        struct {
            uint8_t *buf;   /* destination, or NULL to discard the bytes */
            size_t   len;
        } pop;
        struct {
            uint8_t       *removed;  /* dest for the popped tail bytes, or NULL
                                      * to discard them (the atrunc form) */
            size_t         n;        /* number of bytes to cut off the tail */
            const uint8_t *new_buf;  /* bytes to append after the cut */
            size_t         new_len;
        } splice;
        struct {
            const bstack_iovec_t *writes;  /* (relative offset, buf, len) blocks,
                                            * each offset from the current tail */
            size_t                count;
            uint64_t              length;  /* total bytes to grow the payload by */
        } sparse;
        struct {
            uint64_t *out;
        } len;
    } u;
} bstack_gen_op_t;

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

/*
 * Run a sequence of dependent reads, optionally followed by a single write
 * or swap, all under one held write lock.
 *
 * gen is called repeatedly while bstack's write lock is held, with *out_op left
 * for it to populate. gen must not call other bstack_* APIs on the same handle:
 * - Returning 1 with *out_op set to BSTACK_GEN_READ reads the requested range
 *   into u.read.buf and calls gen again.  By the time gen is called again,
 *   the buffer from the *previous* read already holds its data, so each step
 *   can use earlier results to decide the next one — e.g. "read the head
 *   pointer, then read the node it points to".
 * - Returning 1 with *out_op set to BSTACK_GEN_WRITE writes u.write.data to
 *   the requested range and ends the sequence; gen is not called again.
 * - Returning 1 with *out_op set to BSTACK_GEN_SWAP atomically exchanges the
 *   two requested regions and ends the sequence; gen is not called again —
 *   the in-sequence equivalent of bstack_cross_exchange, useful when a swap
 *   target is only known once an earlier read has resolved it (e.g. "read the
 *   free-list head, then splice this block in as the new head").
 * - Returning 1 with *out_op set to BSTACK_GEN_PUSH appends u.push.data to
 *   the end of the file, growing the payload, and ends the sequence; gen is
 *   not called again — the in-sequence equivalent of bstack_push.
 * - Returning 1 with *out_op set to BSTACK_GEN_POP removes the last
 *   u.pop.len bytes from the end of the file into u.pop.buf, shrinking the
 *   payload, and ends the sequence; gen is not called again — the
 *   in-sequence equivalent of bstack_pop.  A NULL u.pop.buf drops the bytes
 *   without copying them out — the in-sequence equivalent of bstack_discard.
 * - Returning 1 with *out_op set to BSTACK_GEN_SPLICE cuts u.splice.n bytes off
 *   the tail then appends u.splice.new_buf, changing the payload length, and
 *   ends the sequence; gen is not called again — the in-sequence equivalent of
 *   bstack_splice.  A NULL u.splice.removed discards the popped bytes instead of
 *   reading them out — the in-sequence equivalent of bstack_atrunc.
 * - Returning 1 with *out_op set to BSTACK_GEN_SPARSE sparsely grows the payload
 *   by u.sparse.length bytes, scattering the u.sparse.count descriptors in
 *   u.sparse.writes into the freshly grown region and leaving the gaps zero, and
 *   ends the sequence; gen is not called again — the in-sequence equivalent of
 *   bstack_extend_sparse_batched.  The writes must be pairwise non-overlapping
 *   and each must fit within [0, length).
 * - Returning 1 with *out_op set to BSTACK_GEN_LEN writes the current logical
 *   payload size into *u.len.out and calls gen again — the in-sequence
 *   equivalent of bstack_len, useful when a later step's offset or length
 *   depends on the current payload size (e.g. "read the size, then read the
 *   last element").
 * - Returning 0 ends the sequence without writing anything — useful when the
 *   reads alone inform a decision, including the decision to change nothing.
 * - Returning -1 aborts the operation; errno must be set by gen.
 *
 * Holding the write lock across every read and the final mutation means no
 * other thread can observe or modify any region of the file in between — the
 * guarantee that bstack_get_batched_gen followed by a separate bstack_cas
 * cannot provide, since the two separate lock acquisitions leave an ABA
 * window.  The mutated region(s) need not overlap any region that was read.
 * BSTACK_GEN_PUSH, BSTACK_GEN_POP, BSTACK_GEN_SPLICE, and BSTACK_GEN_SPARSE are
 * the steps that change the file size.
 *
 * Reads of the locked region [0, bstack_locked_len()) are permitted, matching
 * bstack_get.  Write and swap ranges that touch the locked region are
 * rejected, matching bstack_set and bstack_cross_exchange.  A pop that would
 * shrink the payload below bstack_locked_len() is rejected, matching
 * bstack_pop.
 *
 * Returns EINVAL if any offset + len overflows uint64_t, if a read, write, or
 * swap range exceeds the current payload size, if the two swap regions
 * overlap, if a write or swap range overlaps the locked region
 * [0, bstack_locked_len()), if a pop removes more bytes than the current
 * payload size, or if a pop would shrink the payload below
 * bstack_locked_len().  Returns -1 (errno set) if gen returns -1, or if
 * an I/O error occurs.
 *
 * Only available when compiled with both -DBSTACK_FEATURE_SET and
 * -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_process_gen(bstack_t *bs,
                       int (*gen)(bstack_gen_op_t *out_op, void *ctx),
                       void *ctx);

/*
 * Commit several non-overlapping in-place writes as one crash-atomic unit.
 *
 * writes is an array of count descriptors; each (offset, buf, len) overwrites
 * [offset, offset + len) with the len bytes at buf.  Either every write is
 * applied or none is — after a crash the file holds all of them or all of the
 * old bytes, never a partial subset.  The bstack_iovec_t type is reused for the
 * descriptors (its buf is read as the write source here).  Empty (len == 0)
 * entries are ignored, and the file size is never changed.
 *
 * The writes must be pairwise non-overlapping; an overlapping pair is rejected.
 * (The generator form, bstack_inplace_gen, instead resolves overlap in favour of
 * the later write.)
 *
 * Crash-atomic across the whole batch via the multi-write journal (stage all
 * blocks, arm, replay, disarm), recovered on the next bstack_open.  A batch that
 * reduces to a single non-empty write takes the ordinary single-write path.
 *
 * Returns EINVAL if any offset + len overflows uint64_t or exceeds the current
 * payload size, if any write overlaps the locked region [0, bstack_locked_len()),
 * or if two writes overlap.  Returns -1 (errno set) on an I/O error.
 *
 * Only available when compiled with both -DBSTACK_FEATURE_SET and
 * -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_set_batched(bstack_t *bs, const bstack_iovec_t *writes, size_t count);

/*
 * Run a sequence of dependent reads interleaved with multiple in-place writes,
 * committing every write as one crash-atomic unit when the sequence ends.
 *
 * Like bstack_process_gen, gen is called repeatedly while the write lock is held,
 * populating *out_op and returning 1 to yield an op or 0 to end.  Unlike
 * bstack_process_gen:
 *
 * - Writes accumulate; they do not end the sequence.  A BSTACK_GEN_WRITE records
 *   a pending in-place write and gen is called again.  Every recorded write
 *   commits together when gen returns 0, via the multi-write journal.
 * - Later writes override earlier overlapping ones (overlap is not rejected).
 *   For a<b<c<d, writing a..c then b..d commits a..b from the first and b..d
 *   from the second.
 * - A BSTACK_GEN_READ returns the batch-so-far content: committed bytes overlaid
 *   with the edits recorded so far, not the on-disk bytes.
 *
 * Only in-place ops are permitted: BSTACK_GEN_READ, BSTACK_GEN_WRITE, and
 * BSTACK_GEN_LEN.  The size-changing ops (PUSH/POP/SPLICE/SPARSE) and SWAP are
 * rejected (reported as EINVAL via prev_status; not recorded, and the sequence
 * continues).
 * There is no abort: returning 0 always commits whatever validly accumulated.
 *
 * If prev_status is non-NULL, before each gen call *prev_status is set to the
 * previous op's result — 0 on success, or the errno of a failed op (the first
 * call sees 0).  An erroring op is simply not recorded; gen inspects the status
 * and decides whether to continue, issue a different op, or end.  A common
 * pattern is to stash prev_status inside ctx so gen can read it.
 *
 * Every buffer handed to a BSTACK_GEN_WRITE must outlive the call: pending write
 * data is borrowed until the final commit and consulted by later reads, so it
 * must not be reused or mutated until bstack_inplace_gen returns.
 *
 * Reads of the locked region [0, bstack_locked_len()) are permitted; write ranges
 * touching it are rejected, matching bstack_set.
 *
 * Returns 0 on success (including an empty sequence).  Returns -1 (errno set) on
 * an I/O error during a read or the final commit, or on allocation failure.
 *
 * Only available when compiled with both -DBSTACK_FEATURE_SET and
 * -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_inplace_gen(bstack_t *bs,
                       int (*gen)(bstack_gen_op_t *out_op, void *ctx),
                       void *ctx, int *prev_status);

/*
 * Atomically swap two equal-size, non-overlapping regions within the file.
 *
 * Bytes at [a, a+n) and [b, b+n) are exchanged under a single write lock.
 * The file size is never changed.  n = 0 is a valid no-op (bounds are still
 * checked).
 *
 * Returns EINVAL if either a+n or b+n overflows uint64_t, if the regions
 * overlap, if either region exceeds the payload size, or if either region
 * start lies within the locked prefix.
 *
 * Only available when compiled with both -DBSTACK_FEATURE_SET and
 * -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_cross_exchange(bstack_t *bs, uint64_t a, uint64_t b, uint64_t n);

/*
 * Copy n bytes from [from, from+n) to [to, to+n) under a single write lock.
 *
 * Overlapping regions are handled correctly (source is read into a temporary
 * buffer before writing).  The file size is never changed.  n = 0 is a valid
 * no-op (bounds are still checked).
 *
 * Returns EINVAL if either from+n or to+n overflows uint64_t, if either
 * region exceeds the payload size, or if to lies within the locked prefix.
 *
 * Only available when compiled with both -DBSTACK_FEATURE_SET and
 * -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_copy(bstack_t *bs, uint64_t from, uint64_t to, uint64_t n);

/*
 * Cross-Region Dependent Swap — equal condition.
 *
 * Reads a_len bytes from a_offset and compares them to a_expected.  If they
 * are equal, reads b_len bytes from b_offset into b_old_buf and writes
 * b_new_buf there.  *ok (if non-NULL) is set to 1 when the swap was
 * performed or 0 when the comparison failed.
 *
 * All operations happen under one write lock.  a_len == 0 trivially matches.
 * b_len == 0 skips the B swap when the condition passes (*ok = 1, no I/O).
 *
 * Returns EINVAL if either range overflows uint64_t, exceeds the payload
 * size, or if b_offset lies within the locked prefix (when b_len > 0).
 *
 * Only available when compiled with both -DBSTACK_FEATURE_SET and
 * -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_eq_crds(bstack_t *bs,
                   uint64_t a_offset, const uint8_t *a_expected, size_t a_len,
                   uint64_t b_offset, uint8_t *b_old_buf,
                   const uint8_t *b_new_buf, size_t b_len,
                   int *ok);

/*
 * Cross-Region Dependent Swap — not-equal condition.
 *
 * Like bstack_eq_crds but performs the B swap only when the a_len bytes at
 * a_offset are NOT equal to a_expected.  *ok = 0 if the bytes compare equal
 * (swap suppressed).
 *
 * Only available when compiled with both -DBSTACK_FEATURE_SET and
 * -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_ne_crds(bstack_t *bs,
                   uint64_t a_offset, const uint8_t *a_expected, size_t a_len,
                   uint64_t b_offset, uint8_t *b_old_buf,
                   const uint8_t *b_new_buf, size_t b_len,
                   int *ok);

/*
 * Cross-Region Dependent Swap — masked-equal condition.
 *
 * Like bstack_eq_crds but compares (A[i] & mask[i]) == (a_expected[i] &
 * mask[i]) for every byte i.  mask and a_expected must both have length
 * a_len.
 *
 * Returns EINVAL if mask is NULL when a_len > 0.
 *
 * Only available when compiled with both -DBSTACK_FEATURE_SET and
 * -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_masked_eq_crds(bstack_t *bs,
                          uint64_t a_offset, const uint8_t *mask,
                          const uint8_t *a_expected, size_t a_len,
                          uint64_t b_offset, uint8_t *b_old_buf,
                          const uint8_t *b_new_buf, size_t b_len,
                          int *ok);

/*
 * Cross-Region Dependent Swap — masked-not-equal condition.
 *
 * Like bstack_masked_eq_crds but performs the B swap only when at least one
 * masked byte differs: (A[i] & mask[i]) != (a_expected[i] & mask[i]).
 * *ok = 0 if all masked bytes compare equal (swap suppressed).
 *
 * Only available when compiled with both -DBSTACK_FEATURE_SET and
 * -DBSTACK_FEATURE_ATOMIC.
 */
int bstack_masked_ne_crds(bstack_t *bs,
                          uint64_t a_offset, const uint8_t *mask,
                          const uint8_t *a_expected, size_t a_len,
                          uint64_t b_offset, uint8_t *b_old_buf,
                          const uint8_t *b_new_buf, size_t b_len,
                          int *ok);
#endif /* BSTACK_FEATURE_ATOMIC && BSTACK_FEATURE_SET */

#ifdef __cplusplus
}
#endif

#endif /* BSTACK_H */
