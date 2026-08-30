/* Expose POSIX + BSD extensions on non-Windows platforms.
 * _DARWIN_C_SOURCE is defined unconditionally on non-Windows: on real macOS
 * it overrides _POSIX_C_SOURCE restrictions to keep fdatasync/flock visible;
 * on Linux/glibc it is ignored.  This also handles clang cross-compilation
 * that falls back to macOS SDK headers when no Linux sysroot is available.
 * On Windows (_WIN32) these macros are skipped and Win32 APIs are used
 * instead. */
#ifndef _WIN32
#  define _DARWIN_C_SOURCE
#  define _DEFAULT_SOURCE
#  define _POSIX_C_SOURCE 200809L
#  define _XOPEN_SOURCE 700
#endif

#include "bstack.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#  define WIN32_LEAN_AND_MEAN
#  include <windows.h>
#else
#  include <fcntl.h>
#  include <pthread.h>
#  include <sys/file.h>
#  include <sys/stat.h>
#  include <unistd.h>
#endif

/* C11 stdatomic.h or fallback to compiler intrinsics */
#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L && !defined(__STDC_NO_ATOMICS__)
#  include <stdatomic.h>
#  define ATOMIC_UINT64_T atomic_uint_fast64_t
#  define ATOMIC_LOAD_ACQUIRE(ptr) atomic_load_explicit((ptr), memory_order_acquire)
#  define ATOMIC_STORE_RELEASE(ptr, val) atomic_store_explicit((ptr), (val), memory_order_release)
#  define ATOMIC_INIT(val) ATOMIC_VAR_INIT(val)
#elif defined(_WIN32)
#  define ATOMIC_UINT64_T volatile LONGLONG
#  define ATOMIC_LOAD_ACQUIRE(ptr) (uint64_t)InterlockedCompareExchange64((ptr), 0, 0)
#  define ATOMIC_STORE_RELEASE(ptr, val) InterlockedExchange64((ptr), (LONGLONG)(val))
#  define ATOMIC_INIT(val) (val)
#elif defined(__GNUC__) || defined(__clang__)
#  define ATOMIC_UINT64_T volatile uint64_t
#  define ATOMIC_LOAD_ACQUIRE(ptr) __atomic_load_n((ptr), __ATOMIC_ACQUIRE)
#  define ATOMIC_STORE_RELEASE(ptr, val) __atomic_store_n((ptr), (val), __ATOMIC_RELEASE)
#  define ATOMIC_INIT(val) (val)
#else
#  error "No atomic support available"
#endif

/* -------------------------------------------------------------------------
 * Constants
 * ---------------------------------------------------------------------- */

/* On-disk format 0.4.3. The first 6 bytes (BSTK + major + minor) are the
 * compatibility prefix checked on open; the patch byte is informational. */
static const uint8_t  MAGIC[8]        = {'B','S','T','K', 0, 4, 3, 0};
static const uint8_t  MAGIC_PREFIX[6] = {'B','S','T','K', 0, 4};
static const uint64_t HEADER_SIZE     = 32;

/* Pre-0.4.0 (0.1.x) format that bstack_migrate upgrades from: 16-byte header
 * (magic[8] + committed_len[8]). */
static const uint8_t  LEGACY_MAGIC_PREFIX[6] = {'B','S','T','K', 0, 1};
static const uint64_t LEGACY_HEADER_SIZE     = 16;

/* Write-in-progress journal modes stored in the wip_aux header field (offset
 * 24). Meaningful only while wip_ptr != 0. 0 (WIP_SET, also the disarmed value)
 * is the same-length verbatim replay; the non-zero modes take values near
 * UINT64_MAX, decrementing as modes are added, so the low range stays free for
 * future packed encodings and any unrecognized value is unmistakable (recovery
 * rolls it back). See algos/WIP.md. */
#define WIP_SET          ((uint64_t)0)
#define WIP_SPLICE_GROW  (UINT64_MAX - 1)
#define WIP_SPLICE_SHRINK (UINT64_MAX - 2)
#define WIP_REPEAT       (UINT64_MAX - 3)
#define WIP_COPY         (UINT64_MAX - 4)
#define WIP_MULTI        (UINT64_MAX - 5)

/* Conservative power-fail atomic block size (bytes): a write confined to one
 * 256-byte-aligned region cannot tear across a hardware block on power loss.
 * Upper bound on the streaming buffer for on-disk moves and repeat-fills, so a
 * relocation or fill of any length uses O(1) memory. */
#define ATOMIC_BLOCK ((uint64_t)256)
#define MOVE_CHUNK   ((uint64_t)(4 * 1024))

/* -------------------------------------------------------------------------
 * Platform file handle type
 * ---------------------------------------------------------------------- */

#ifdef _WIN32
typedef HANDLE bstack_fd_t;
#else
typedef int    bstack_fd_t;
#endif

/* -------------------------------------------------------------------------
 * Internal struct
 * ---------------------------------------------------------------------- */

struct bstack {
    bstack_fd_t fd;
#ifdef _WIN32
    SRWLOCK          lock;
    CRITICAL_SECTION cache_mutex;
#else
    pthread_rwlock_t lock;
    pthread_mutex_t  cache_mutex;
#endif
    /* Cached copy of the on-disk header's committed payload length (clen).
     * Seeded from the validated header at construction time (after recovery)
     * and kept in sync by every write-lock-held operation that commits a new
     * clen to the header, via write_committed_len. bstack_len and
     * bstack_is_empty read it under the same lock used for the on-disk
     * state, so no extra synchronisation is needed. */
    uint64_t clen;
    /* Monotonically growing partition boundary. Bytes in [0, locked) are
     * immutable and can be read without the rwlock on supported platforms.
     * Not persisted — resets to 0 on every open. */
    ATOMIC_UINT64_T locked;
    /* In-memory mirror of [0, locked).  Only active when cache_enabled != 0.
     * cache_buf is a malloc'd buffer of cache_cap bytes; the count of valid
     * bytes equals bstack_locked_len().  Protected by cache_mutex.
     * cache_buf/cache_cap are populated before locked is advanced so readers
     * that Acquire-load locked always see a consistent buffer. */
    int      cache_enabled;
    uint8_t *cache_buf;
    uint64_t cache_cap;
};

/* =========================================================================
 * Platform layer — Windows
 * ====================================================================== */

#ifdef _WIN32

/* Map the most recent Windows error to an errno value. */
static void win_set_errno(void)
{
    switch (GetLastError()) {
        case ERROR_ACCESS_DENIED:
        case ERROR_SHARING_VIOLATION:   errno = EACCES;      break;
        case ERROR_FILE_NOT_FOUND:
        case ERROR_PATH_NOT_FOUND:      errno = ENOENT;      break;
        case ERROR_NOT_ENOUGH_MEMORY:
        case ERROR_OUTOFMEMORY:         errno = ENOMEM;      break;
        case ERROR_LOCK_VIOLATION:      errno = EWOULDBLOCK; break;
        case ERROR_INVALID_HANDLE:      errno = EBADF;       break;
        default:                        errno = EIO;         break;
    }
}

/* When BSTACK_TEST_NO_DURABLE_SYNC is defined at compile time, plat_durable_sync
 * becomes a no-op.  This mirrors the Rust crate, whose durable_sync is a no-op in
 * cfg(test)+debug builds (src/io_core.rs): an in-process test or fuzz run tears
 * the store down logically and reopens it in-process, so skipping the physical
 * sync changes neither observable behavior nor the on-disk bytes, yet on macOS
 * F_FULLFSYNC otherwise dominates runtime (minutes → seconds).  UNSAFE for any
 * build that must survive a real crash — test/fuzz builds only, never production. */
static int plat_durable_sync(bstack_fd_t h)
{
#ifdef BSTACK_TEST_NO_DURABLE_SYNC
    (void)h;
    return 0;
#else
    if (!FlushFileBuffers(h)) { win_set_errno(); return -1; }
    return 0;
#endif
}

static int plat_file_size(bstack_fd_t h, uint64_t *out)
{
    LARGE_INTEGER li;
    if (!GetFileSizeEx(h, &li)) { win_set_errno(); return -1; }
    *out = (uint64_t)li.QuadPart;
    return 0;
}

/*
 * Positional write via OVERLAPPED — does not advance the file pointer.
 * The file is extended automatically if offset + count exceeds its size.
 */
static int plat_pwrite(bstack_fd_t h, const void *buf, size_t count,
                       uint64_t offset)
{
    if (count == 0) return 0;
    if (count > (size_t)MAXDWORD) { errno = EINVAL; return -1; }
    OVERLAPPED ov;
    memset(&ov, 0, sizeof ov);
    ov.Offset     = (DWORD)(offset & 0xFFFFFFFFU);
    ov.OffsetHigh = (DWORD)(offset >> 32);
    DWORD nw = 0;
    if (!WriteFile(h, buf, (DWORD)count, &nw, &ov)) { win_set_errno(); return -1; }
    if (nw != (DWORD)count) { errno = EIO; return -1; }
    return 0;
}

/* Positional read via OVERLAPPED — does not advance the file pointer. */
static int plat_pread(bstack_fd_t h, void *buf, size_t count,
                      uint64_t offset)
{
    if (count == 0) return 0;
    if (count > (size_t)MAXDWORD) { errno = EINVAL; return -1; }
    OVERLAPPED ov;
    memset(&ov, 0, sizeof ov);
    ov.Offset     = (DWORD)(offset & 0xFFFFFFFFU);
    ov.OffsetHigh = (DWORD)(offset >> 32);
    DWORD nr = 0;
    if (!ReadFile(h, buf, (DWORD)count, &nr, &ov)) { win_set_errno(); return -1; }
    if (nr != (DWORD)count) { errno = EIO; return -1; }
    return 0;
}

/* Truncate (or extend) the file to exactly `size` bytes. */
static int plat_ftruncate(bstack_fd_t h, uint64_t size)
{
    LARGE_INTEGER li;
    li.QuadPart = (LONGLONG)size;
    if (!SetFilePointerEx(h, li, NULL, FILE_BEGIN)) { win_set_errno(); return -1; }
    if (!SetEndOfFile(h)) { win_set_errno(); return -1; }
    return 0;
}

/* =========================================================================
 * Platform layer — Unix
 * ====================================================================== */

#else /* !_WIN32 */

/* No-op under BSTACK_TEST_NO_DURABLE_SYNC — see the note on the Windows
 * definition above.  Test/fuzz builds only, never production. */
static int plat_durable_sync(bstack_fd_t fd)
{
#ifdef BSTACK_TEST_NO_DURABLE_SYNC
    (void)fd;
    return 0;
#else
#  ifdef __APPLE__
    if (fcntl(fd, F_FULLFSYNC) == 0)
        return 0;
    /* Device does not support F_FULLFSYNC — fall back to fdatasync. */
#  endif
    return fdatasync(fd);
#endif
}

static int plat_file_size(bstack_fd_t fd, uint64_t *out)
{
    struct stat st;
    if (fstat(fd, &st) != 0) return -1;
    *out = (uint64_t)st.st_size;
    return 0;
}

static int plat_pwrite(bstack_fd_t fd, const void *buf, size_t count,
                       uint64_t offset)
{
    if (count == 0) return 0;
    ssize_t r = pwrite(fd, buf, count, (off_t)offset);
    if (r < 0) return -1;
    if ((size_t)r != count) { errno = EIO; return -1; }
    return 0;
}

static int plat_pread(bstack_fd_t fd, void *buf, size_t count,
                      uint64_t offset)
{
    if (count == 0) return 0;
    ssize_t r = pread(fd, buf, count, (off_t)offset);
    if (r < 0) return -1;
    if ((size_t)r != count) { errno = EIO; return -1; }
    return 0;
}

static int plat_ftruncate(bstack_fd_t fd, uint64_t size)
{
    return ftruncate(fd, (off_t)size);
}

#endif /* _WIN32 */

/* -------------------------------------------------------------------------
 * Close helper (releases advisory lock on both platforms)
 * ---------------------------------------------------------------------- */

static void close_fd(bstack_fd_t fd)
{
#ifdef _WIN32
    CloseHandle(fd);
#else
    close(fd);
#endif
}

/* -------------------------------------------------------------------------
 * Lock / unlock macros (reader–writer lock, cross-platform)
 * ---------------------------------------------------------------------- */

#ifdef _WIN32
#  define BS_RDLOCK(bs)    AcquireSRWLockShared(&(bs)->lock)
#  define BS_WRLOCK(bs)    AcquireSRWLockExclusive(&(bs)->lock)
#  define BS_RDUNLOCK(bs)  ReleaseSRWLockShared(&(bs)->lock)
#  define BS_WRUNLOCK(bs)  ReleaseSRWLockExclusive(&(bs)->lock)
#  define CACHE_LOCK(bs)   EnterCriticalSection(&(bs)->cache_mutex)
#  define CACHE_UNLOCK(bs) LeaveCriticalSection(&(bs)->cache_mutex)
#else
#  define BS_RDLOCK(bs)    pthread_rwlock_rdlock(&(bs)->lock)
#  define BS_WRLOCK(bs)    pthread_rwlock_wrlock(&(bs)->lock)
#  define BS_RDUNLOCK(bs)  pthread_rwlock_unlock(&(bs)->lock)
#  define BS_WRUNLOCK(bs)  pthread_rwlock_unlock(&(bs)->lock)
#  define CACHE_LOCK(bs)   pthread_mutex_lock(&(bs)->cache_mutex)
#  define CACHE_UNLOCK(bs) pthread_mutex_unlock(&(bs)->cache_mutex)
#endif

/* -------------------------------------------------------------------------
 * Cache helpers
 * ---------------------------------------------------------------------- */

/* Round n up to the next power of two.
 * Returns 1 for n == 0.
 * Returns 0 on overflow (input > 2^63; next power of two would be 2^64).
 * Callers must treat a 0 return as an error (ENOMEM / EINVAL). */
static uint64_t next_pow2_u64(uint64_t n)
{
    if (n == 0) return 1;
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n |= n >> 32;
    /* If all bits are set the true next power of two is 2^64, which overflows. */
    if (n == UINT64_MAX) return 0;
    return n + 1;
}

/* -------------------------------------------------------------------------
 * Little-endian helpers (positional — no cursor side-effects)
 * ---------------------------------------------------------------------- */

static int write_le64(bstack_fd_t fd, uint64_t file_offset, uint64_t val)
{
    uint8_t buf[8];
    for (int i = 0; i < 8; i++)
        buf[i] = (uint8_t)(val >> (8 * i));
    return plat_pwrite(fd, buf, 8, file_offset);
}

/* -------------------------------------------------------------------------
 * Header helpers
 * ---------------------------------------------------------------------- */

/* Overwrite the committed-length field at file offset 8 and update the
 * in-memory cache (*clen) to match. */
static int write_committed_len(bstack_fd_t fd, uint64_t *clen, uint64_t len)
{
    if (write_le64(fd, 8, len) != 0)
        return -1;
    *clen = len;
    return 0;
}

/* Decode a little-endian u64 from an 8-byte buffer. */
static uint64_t decode_le64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 0; i < 8; i++)
        v |= (uint64_t)p[i] << (8 * i);
    return v;
}

/* Write the 32-byte header into a brand-new (empty) file. */
static int init_header(bstack_fd_t fd)
{
    uint8_t hdr[32];
    memcpy(hdr, MAGIC, 8);
    memset(hdr + 8, 0, 24); /* committed_len + wip_ptr + wip_aux = 0 */
    return plat_pwrite(fd, hdr, 32, 0);
}

/* Validates magic prefix and returns committed payload length via *out_clen and
 * the write-in-progress journal fields via *out_wip_ptr / *out_wip_aux.
 * Sets errno = EINVAL on bad magic or short header read. */
static int read_header(bstack_fd_t fd, uint64_t *out_clen,
                       uint64_t *out_wip_ptr, uint64_t *out_wip_aux)
{
    uint8_t hdr[32];
    if (plat_pread(fd, hdr, 32, 0) != 0) {
        errno = EINVAL;
        return -1;
    }
    if (memcmp(hdr, MAGIC_PREFIX, 6) != 0) {
        errno = EINVAL;
        return -1;
    }
    *out_clen    = decode_le64(hdr + 8);
    *out_wip_ptr = decode_le64(hdr + 16);
    *out_wip_aux = decode_le64(hdr + 24);
    return 0;
}

/* -------------------------------------------------------------------------
 * Write-in-progress journal primitives (see algos/WIP.md)
 *
 * The header fields clen (offset 8), wip_ptr (16), wip_aux (24) all lie in the
 * first 32 bytes — one aligned block — so a write spanning any of them is
 * atomic at the storage level: recovery never observes them out of step.
 * ---------------------------------------------------------------------- */

/* Overwrite wip_ptr (offset 16) and wip_aux (offset 24) in one 16-byte write.
 * wip_ptr == 0 is the disarmed (steady) state. */
#if defined(BSTACK_FEATURE_SET) || defined(BSTACK_FEATURE_ATOMIC)
static int write_wip(bstack_fd_t fd, uint64_t wip_ptr, uint64_t wip_aux)
{
    uint8_t buf[16];
    for (int i = 0; i < 8; i++) buf[i]     = (uint8_t)(wip_ptr >> (8 * i));
    for (int i = 0; i < 8; i++) buf[8 + i] = (uint8_t)(wip_aux >> (8 * i));
    return plat_pwrite(fd, buf, 16, 16);
}
#endif

/* Atomically commit a new committed length AND disarm the journal in one
 * 24-byte header write: clen at offset 8, wip_ptr/wip_aux at 16/24. All three
 * lie in the first block, so the new length and the disarm land together.
 * Does not touch the in-memory cache; the caller updates it. */
static int write_header_commit(bstack_fd_t fd, uint64_t clen,
                               uint64_t wip_ptr, uint64_t wip_aux)
{
    uint8_t buf[24];
    for (int i = 0; i < 8; i++) buf[i]      = (uint8_t)(clen    >> (8 * i));
    for (int i = 0; i < 8; i++) buf[8 + i]  = (uint8_t)(wip_ptr >> (8 * i));
    for (int i = 0; i < 8; i++) buf[16 + i] = (uint8_t)(wip_aux >> (8 * i));
    return plat_pwrite(fd, buf, 24, 8);
}

/* Return 1 if overwriting logical [offset, offset+len) is atomic at the storage
 * level — its physical range [HEADER_SIZE+offset, ...) is confined to a single
 * ATOMIC_BLOCK. An empty write is trivially atomic; an overflowing range is
 * reported non-atomic. Gate for skipping the journal on a single-block write. */
#if defined(BSTACK_FEATURE_SET) || defined(BSTACK_FEATURE_ATOMIC)
static int is_atomic_write(uint64_t offset, uint64_t len)
{
    if (len == 0)
        return 1;
    if (offset > UINT64_MAX - HEADER_SIZE)
        return 0;
    uint64_t start = HEADER_SIZE + offset;
    if (start > UINT64_MAX - (len - 1))
        return 0;
    uint64_t last = start + (len - 1);
    return (start / ATOMIC_BLOCK) == (last / ATOMIC_BLOCK);
}
#endif

/* Copy n bytes from logical src to logical dst within the file using a buffer
 * bounded by MOVE_CHUNK (O(1) memory). Source and destination must not overlap;
 * every caller routes an overlapping move through the disjoint tail region. Only
 * ever performed while the write lock is held, so chunking is safe. */
static int move_chunked(bstack_fd_t fd, uint64_t src, uint64_t dst, uint64_t n)
{
    uint8_t buf[MOVE_CHUNK];
    uint64_t done = 0;
    while (done < n) {
        uint64_t left = n - done;
        size_t take = (left < MOVE_CHUNK) ? (size_t)left : (size_t)MOVE_CHUNK;
        if (plat_pread(fd, buf, take, HEADER_SIZE + src + done) != 0)
            return -1;
        if (plat_pwrite(fd, buf, take, HEADER_SIZE + dst + done) != 0)
            return -1;
        done += take;
    }
    return 0;
}

/* Fill [phys, phys + k*s_len) with k back-to-back copies of s (phys is a
 * physical file offset), through a buffer of whole copies of s bounded by
 * MOVE_CHUNK. Used by the live repeat-fill and by recovery. Callers guarantee
 * k*s_len does not overflow. */
static int write_repeated(bstack_fd_t fd, uint64_t phys,
                          const uint8_t *s, size_t s_len, uint64_t k)
{
    if (s_len == 0 || k == 0)
        return 0;
    uint64_t total = k * (uint64_t)s_len;
    /* Pack as many whole copies of s as fit under MOVE_CHUNK (at least one),
     * capped at k, so every chunk boundary lands on a copy boundary. */
    uint64_t copies = MOVE_CHUNK / (uint64_t)s_len;
    if (copies == 0) copies = 1;
    if (copies > k)  copies = k;
    size_t bufsz = (size_t)(copies * (uint64_t)s_len);
    uint8_t *buf = (uint8_t *)malloc(bufsz);
    if (!buf)
        return -1;
    for (uint64_t i = 0; i < copies; i++)
        memcpy(buf + i * s_len, s, s_len);
    uint64_t done = 0;
    while (done < total) {
        uint64_t left = total - done;
        size_t take = (left < bufsz) ? (size_t)left : bufsz;
        if (plat_pwrite(fd, buf, take, phys + done) != 0) {
            free(buf);
            return -1;
        }
        done += take;
    }
    free(buf);
    return 0;
}

/* Recover an armed in-place journal (wip_ptr != 0) found on open, restoring the
 * at-rest invariant file_size == HEADER_SIZE + clen. Writes the committed length
 * after recovery (unchanged except by a splice) into *out_clen. Every replay is
 * idempotent, so a crash during recovery is safe to re-run. Any unrecognized
 * wip_aux, or an inconsistent splice/copy header, rolls back to committed_len.
 * Not feature-gated: a file armed by a featured build must still recover in a
 * build without that feature. */
static int recover_wip(bstack_fd_t fd, uint64_t committed_len,
                       uint64_t wip_ptr, uint64_t wip_aux,
                       uint64_t raw_size, uint64_t *out_clen)
{
    uint64_t tail_start = HEADER_SIZE + committed_len;
    uint64_t tail_len   = (raw_size > tail_start) ? (raw_size - tail_start) : 0;
    uint64_t final_clen = committed_len; /* only a splice changes it */

    if (wip_aux == WIP_SET) {
        /* Replay the staged tail verbatim into the target, if present and the
         * target lies wholly within the committed payload. */
        if (tail_len > 0 && wip_ptr >= HEADER_SIZE) {
            uint64_t dst = wip_ptr - HEADER_SIZE;
            if (dst <= committed_len && committed_len - dst >= tail_len) {
                if (move_chunked(fd, committed_len, dst, tail_len) != 0)
                    return -1;
                if (plat_durable_sync(fd) != 0)
                    return -1;
            }
        }
    } else if (wip_aux == WIP_REPEAT) {
        /* Tail is [k: u64 LE | s]; replay k copies of s into the target. */
        if (tail_len >= 8) {
            uint8_t kbuf[8];
            if (plat_pread(fd, kbuf, 8, tail_start) != 0)
                return -1;
            uint64_t k = decode_le64(kbuf);
            uint64_t s_len = tail_len - 8;
            if (s_len > 0 && k <= UINT64_MAX / s_len) {
                uint64_t total = k * s_len;
                if (wip_ptr >= HEADER_SIZE &&
                    committed_len >= total &&
                    (wip_ptr - HEADER_SIZE) <= committed_len - total) {
                    uint8_t *s = (uint8_t *)malloc((size_t)s_len);
                    if (!s)
                        return -1;
                    if (plat_pread(fd, s, (size_t)s_len, tail_start + 8) != 0) {
                        free(s);
                        return -1;
                    }
                    if (write_repeated(fd, wip_ptr, s, (size_t)s_len, k) != 0) {
                        free(s);
                        return -1;
                    }
                    free(s);
                    if (plat_durable_sync(fd) != 0)
                        return -1;
                }
            }
        }
    } else if (wip_aux == WIP_COPY) {
        /* Tail is [src: u64 LE | n: u64 LE]; the source is disjoint from the
         * destination (wip_ptr), so replay the copy from the intact source. */
        if (tail_len >= 16 && wip_ptr >= HEADER_SIZE) {
            uint8_t meta[16];
            if (plat_pread(fd, meta, 16, tail_start) != 0)
                return -1;
            uint64_t src = decode_le64(meta);
            uint64_t n   = decode_le64(meta + 8);
            uint64_t dst = wip_ptr - HEADER_SIZE;
            int disjoint = (n == 0) ||
                (dst >= src && dst - src >= n) ||
                (src >= dst && src - dst >= n);
            if (n > 0 && n <= committed_len &&
                src <= committed_len - n && dst <= committed_len - n &&
                disjoint) {
                if (move_chunked(fd, src, dst, n) != 0)
                    return -1;
                if (plat_durable_sync(fd) != 0)
                    return -1;
            }
        }
    } else if (wip_aux == WIP_SPLICE_GROW || wip_aux == WIP_SPLICE_SHRINK) {
        /* The staged new tail sits at [S, S+m), S = max(clen, clen'); replay it
         * into [a, clen'). clen' is derived from the armed file size and the
         * direction. Roll back on any inconsistency. */
        int grow = (wip_aux == WIP_SPLICE_GROW);
        if (wip_ptr >= HEADER_SIZE) {
            uint64_t a = wip_ptr - HEADER_SIZE;
            uint64_t payload_end = raw_size - HEADER_SIZE;
            int ok = 0;
            uint64_t clen_new = 0;
            if (grow) {
                /* payload_end == 2*clen' - a  =>  clen' = (payload_end + a)/2 */
                if (payload_end <= UINT64_MAX - a) {
                    clen_new = (payload_end + a) / 2;
                    ok = 1;
                }
            } else {
                /* payload_end == clen + (clen' - a)  =>  clen' = payload_end + a - clen */
                if (payload_end <= UINT64_MAX - a && payload_end + a >= committed_len) {
                    clen_new = payload_end + a - committed_len;
                    ok = 1;
                }
            }
            if (ok) {
                uint64_t s = (committed_len > clen_new) ? committed_len : clen_new;
                uint64_t m = (clen_new > a) ? (clen_new - a) : 0;
                int dir_ok = grow ? (clen_new > committed_len)
                                  : (clen_new < committed_len);
                /* s + m == payload_end also rejects the odd-sum grow case. */
                if (a <= committed_len && clen_new >= a && dir_ok &&
                    s <= UINT64_MAX - m && (s + m) == payload_end) {
                    if (move_chunked(fd, s, a, m) != 0)
                        return -1;
                    if (plat_durable_sync(fd) != 0)
                        return -1;
                    final_clen = clen_new;
                }
            }
        }
    }
    /* else: unknown mode — roll back to committed_len (no replay). */

    /* Atomically commit the (possibly new) length and disarm, then drop any
     * bytes beyond it — restoring file_size == HEADER_SIZE + final_clen. */
    if (write_header_commit(fd, final_clen, 0, WIP_SET) != 0)
        return -1;
    if (plat_durable_sync(fd) != 0)
        return -1;
    if (plat_ftruncate(fd, HEADER_SIZE + final_clen) != 0)
        return -1;
    if (plat_durable_sync(fd) != 0)
        return -1;
    *out_clen = final_clen;
    return 0;
}

/* Walk the staged multi-write tail [tail_start, raw_size). For each well-formed
 * [s: u64 LE | e: u64 LE | data] block, when apply != 0, replay data into
 * [s, e) via move_chunked (the payload is its own crash backup). Sets *out_valid
 * to 1 iff the whole tail parsed into a clean, contiguous sequence ending exactly
 * at raw_size with every target within [0, committed_len); 0 on any malformation
 * (a truncated or oversized block, a reversed range, an out-of-range target, or
 * trailing bytes). A legitimately-armed tail always validates. Returns 0, or -1
 * (errno set) on an I/O error. Not feature-gated: recovery needs it. */
static int walk_multi_blocks(bstack_fd_t fd, uint64_t committed_len,
                             uint64_t tail_start, uint64_t raw_size,
                             int apply, int *out_valid)
{
    uint64_t cursor = tail_start;
    while (cursor < raw_size) {
        /* Header must be fully present. */
        if (raw_size - cursor < 16) { *out_valid = 0; return 0; }
        uint8_t hdr[16];
        if (plat_pread(fd, hdr, 16, cursor) != 0)
            return -1;
        uint64_t s = decode_le64(hdr);
        uint64_t e = decode_le64(hdr + 8);
        /* Well-formed: forward range, within the committed payload. */
        if (e < s || e > committed_len) { *out_valid = 0; return 0; }
        uint64_t plen = e - s;
        uint64_t payload_phys = cursor + 16; /* <= raw_size (checked above) */
        /* Payload must be fully present in the staged tail. */
        if (plen > raw_size - payload_phys) { *out_valid = 0; return 0; }
        if (apply) {
            /* Source is the payload at logical (payload_phys - HEADER_SIZE);
             * destination is the target [s, e). */
            if (move_chunked(fd, payload_phys - HEADER_SIZE, s, plen) != 0)
                return -1;
        }
        cursor = payload_phys + plen;
    }
    /* A clean walk lands exactly on raw_size (the fits-checks reject any
     * trailing partial block). */
    *out_valid = (cursor == raw_size) ? 1 : 0;
    return 0;
}

/* Recover a crashed multi-write journal found on open (wip_ptr == 0,
 * wip_aux == WIP_MULTI), restoring file_size == HEADER_SIZE + committed_len.
 * Writes committed_len (unchanged — a multi-write never changes the payload
 * size) into *out_clen. Validates the whole block sequence first, then replays
 * every block only if it is clean — making the replay all-or-nothing: a
 * genuinely-armed tail always validates, a corrupt one applies nothing and rolls
 * back. Each replay is idempotent, so a crash during recovery is safe to re-run.
 * Not feature-gated: a file armed by a featured build must still recover in a
 * build without that feature. */
static int recover_multi_write(bstack_fd_t fd, uint64_t committed_len,
                               uint64_t raw_size, uint64_t *out_clen)
{
    uint64_t actual_len = (raw_size >= HEADER_SIZE) ? (raw_size - HEADER_SIZE) : 0;
    if (committed_len > actual_len)
        committed_len = actual_len;
    uint64_t tail_start = HEADER_SIZE + committed_len;
    if (raw_size > tail_start) {
        int valid = 0;
        /* Pass 1: validate without touching the payload. */
        if (walk_multi_blocks(fd, committed_len, tail_start, raw_size, 0,
                              &valid) != 0)
            return -1;
        /* Pass 2: apply, but only if the sequence is clean end-to-end. */
        if (valid) {
            int applied_valid = 0;
            if (walk_multi_blocks(fd, committed_len, tail_start, raw_size, 1,
                                  &applied_valid) != 0)
                return -1;
            if (plat_durable_sync(fd) != 0)
                return -1;
        }
    }
    /* Disarm and drop the staged tail (finalize). clen is unchanged. */
    if (write_header_commit(fd, committed_len, 0, WIP_SET) != 0)
        return -1;
    if (plat_durable_sync(fd) != 0)
        return -1;
    if (plat_ftruncate(fd, tail_start) != 0)
        return -1;
    if (plat_durable_sync(fd) != 0)
        return -1;
    *out_clen = committed_len;
    return 0;
}

/* -------------------------------------------------------------------------
 * File size helper
 * ---------------------------------------------------------------------- */

static int file_size(bstack_fd_t fd, uint64_t *out)
{
    return plat_file_size(fd, out);
}

#ifdef __cplusplus
extern "C" {
#endif

/* -------------------------------------------------------------------------
 * bstack_open
 * ---------------------------------------------------------------------- */

bstack_t *bstack_open(const char *path)
{
#ifdef _WIN32
    HANDLE fd = CreateFileA(path,
                            GENERIC_READ | GENERIC_WRITE,
                            FILE_SHARE_READ | FILE_SHARE_WRITE,
                            NULL,
                            OPEN_ALWAYS,
                            FILE_ATTRIBUTE_NORMAL,
                            NULL);
    if (fd == INVALID_HANDLE_VALUE) {
        win_set_errno();
        return NULL;
    }
    /* Exclusive non-blocking advisory lock over the entire file. */
    {
        OVERLAPPED ov_lock;
        memset(&ov_lock, 0, sizeof ov_lock);
        if (!LockFileEx(fd,
                        LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                        0, MAXDWORD, MAXDWORD, &ov_lock)) {
            DWORD err = GetLastError();
            CloseHandle(fd);
            errno = (err == ERROR_LOCK_VIOLATION) ? EWOULDBLOCK : EIO;
            return NULL;
        }
    }
#else
    int fd = open(path, O_RDWR | O_CREAT, 0666);
    if (fd < 0)
        return NULL;

    /* Exclusive non-blocking advisory lock. */
    if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
        int saved = errno;
        close(fd);
        errno = saved;
        return NULL;
    }
#endif

    uint64_t raw_size;
    if (file_size(fd, &raw_size) != 0) {
        int saved = errno;
        close_fd(fd);
        errno = saved;
        return NULL;
    }

    uint64_t clen = 0;
    if (raw_size == 0) {
        /* New file — write header and sync. */
        if (init_header(fd) != 0 || plat_durable_sync(fd) != 0) {
            int saved = errno;
            close_fd(fd);
            errno = saved;
            return NULL;
        }
    } else if (raw_size < HEADER_SIZE) {
        close_fd(fd);
        errno = EINVAL;
        return NULL;
    } else {
        /* Existing file — validate header and crash-recover if needed. */
        uint64_t committed_len, wip_ptr, wip_aux;
        if (read_header(fd, &committed_len, &wip_ptr, &wip_aux) != 0) {
            int saved = errno;
            close_fd(fd);
            errno = saved;
            return NULL;
        }

        clen = committed_len;
        if (wip_ptr != 0) {
            /* An in-place write was in flight. Replay or roll it back, then
             * restore the at-rest invariant. A splice changes the committed
             * length, so adopt whatever recovery commits. */
            if (recover_wip(fd, committed_len, wip_ptr, wip_aux,
                            raw_size, &clen) != 0) {
                int saved = errno;
                close_fd(fd);
                errno = saved;
                return NULL;
            }
        } else if (wip_aux == WIP_MULTI) {
            /* A multi-write batch was in flight (armed with wip_ptr == 0 and the
             * intent-complete sentinel). All blocks were fully staged before the
             * arm, so replay the sequence and disarm. clen is unchanged. */
            if (recover_multi_write(fd, committed_len, raw_size, &clen) != 0) {
                int saved = errno;
                close_fd(fd);
                errno = saved;
                return NULL;
            }
        } else {
            /* No journal armed: reconcile the committed length against the file
             * size, using whichever is smaller (drops a stale tail from a
             * crashed push/extend or a crashed journal stage). */
            uint64_t actual = raw_size - HEADER_SIZE;
            if (actual != committed_len) {
                uint64_t correct = (committed_len < actual) ? committed_len : actual;
                if (plat_ftruncate(fd, HEADER_SIZE + correct) != 0 ||
                    write_committed_len(fd, &clen, correct) != 0 ||
                    plat_durable_sync(fd) != 0)
                {
                    int saved = errno;
                    close_fd(fd);
                    errno = saved;
                    return NULL;
                }
            }
        }
    }

    bstack_t *bs = malloc(sizeof(bstack_t));
    if (!bs) {
        close_fd(fd);
        return NULL;
    }
    bs->fd            = fd;
    bs->clen          = clen;
    bs->locked        = ATOMIC_INIT(0);
    bs->cache_enabled = 0;
    bs->cache_buf     = NULL;
    bs->cache_cap     = 0;
#ifdef _WIN32
    InitializeSRWLock(&bs->lock);
    InitializeCriticalSection(&bs->cache_mutex);
#else
    if (pthread_rwlock_init(&bs->lock, NULL) != 0) {
        free(bs);
        close(fd);
        errno = ENOMEM;
        return NULL;
    }
    if (pthread_mutex_init(&bs->cache_mutex, NULL) != 0) {
        pthread_rwlock_destroy(&bs->lock);
        free(bs);
        close(fd);
        errno = ENOMEM;
        return NULL;
    }
#endif
    return bs;
}

/* -------------------------------------------------------------------------
 * bstack_close
 * ---------------------------------------------------------------------- */

void bstack_close(bstack_t *bs)
{
    if (!bs)
        return;
    free(bs->cache_buf);
#ifdef _WIN32
    DeleteCriticalSection(&bs->cache_mutex);
#else
    pthread_mutex_destroy(&bs->cache_mutex);
    pthread_rwlock_destroy(&bs->lock);
#endif
    close_fd(bs->fd); /* also releases the advisory lock */
    free(bs);
}

/* -------------------------------------------------------------------------
 * bstack_migrate
 * ---------------------------------------------------------------------- */

int bstack_migrate(const char *path)
{
    bstack_fd_t old_fd;
#ifdef _WIN32
    old_fd = CreateFileA(path, GENERIC_READ, FILE_SHARE_READ, NULL,
                         OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    if (old_fd == INVALID_HANDLE_VALUE) { win_set_errno(); return -1; }
#else
    old_fd = open(path, O_RDONLY);
    if (old_fd < 0)
        return -1;
#endif

    /* Read and validate the legacy 16-byte header. */
    uint64_t old_size;
    if (file_size(old_fd, &old_size) != 0) {
        int s = errno; close_fd(old_fd); errno = s; return -1;
    }
    if (old_size < LEGACY_HEADER_SIZE) {
        close_fd(old_fd); errno = EINVAL; return -1;
    }
    uint8_t hdr[16];
    if (plat_pread(old_fd, hdr, 16, 0) != 0) {
        int s = errno; close_fd(old_fd); errno = s; return -1;
    }
    if (memcmp(hdr, LEGACY_MAGIC_PREFIX, 6) != 0) {
        close_fd(old_fd); errno = EINVAL; return -1;  /* not a legacy file */
    }
    /* Committed length, clamped to the payload actually present. */
    uint64_t stored = decode_le64(hdr + 8);
    uint64_t avail  = old_size - LEGACY_HEADER_SIZE;
    uint64_t clen   = (stored < avail) ? stored : avail;

    /* Sibling path "<path>.migrating", in the same directory so the final
     * rename stays within one filesystem. */
    static const char SUFFIX[] = ".migrating";
    size_t plen = strlen(path);
    char *tmp = (char *)malloc(plen + sizeof SUFFIX);
    if (!tmp) { close_fd(old_fd); errno = ENOMEM; return -1; }
    memcpy(tmp, path, plen);
    memcpy(tmp + plen, SUFFIX, sizeof SUFFIX);

    bstack_fd_t new_fd;
#ifdef _WIN32
    new_fd = CreateFileA(tmp, GENERIC_READ | GENERIC_WRITE, 0, NULL,
                         CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
    if (new_fd == INVALID_HANDLE_VALUE) {
        win_set_errno(); free(tmp); close_fd(old_fd); return -1;
    }
#else
    new_fd = open(tmp, O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (new_fd < 0) {
        int s = errno; free(tmp); close_fd(old_fd); errno = s; return -1;
    }
#endif

    /* Write the new 32-byte 0.4.0 header, then the old payload shifted from
     * offset 16 to offset 32. */
    int failed = 0;
    uint8_t nhdr[32];
    memcpy(nhdr, MAGIC, 8);
    for (int i = 0; i < 8; i++) nhdr[8 + i] = (uint8_t)(clen >> (8 * i));
    memset(nhdr + 16, 0, 16);
    if (plat_pwrite(new_fd, nhdr, 32, 0) != 0)
        failed = 1;

    uint8_t buf[MOVE_CHUNK];
    uint64_t done = 0;
    while (!failed && done < clen) {
        uint64_t left = clen - done;
        size_t take = (left < MOVE_CHUNK) ? (size_t)left : (size_t)MOVE_CHUNK;
        if (plat_pread(old_fd, buf, take, LEGACY_HEADER_SIZE + done) != 0 ||
            plat_pwrite(new_fd, buf, take, HEADER_SIZE + done) != 0) {
            failed = 1;
            break;
        }
        done += take;
    }
    if (!failed && plat_durable_sync(new_fd) != 0)
        failed = 1;

    int saved = errno;
    close_fd(new_fd);
    close_fd(old_fd);
    if (failed) {
        remove(tmp);
        free(tmp);
        errno = saved;
        return -1;
    }

    /* Atomically swap the sibling in for the original: the rename replaces the
     * destination in a single step, so a crash leaves either the intact original
     * or the finished 0.4.0 file at path — never neither. (Removing the original
     * first would open a window where a crash leaves only the sibling.) POSIX
     * rename(2) replaces atomically; the C stdio rename does not on Windows, so
     * MoveFileExA(MOVEFILE_REPLACE_EXISTING) is used there. */
#ifdef _WIN32
    if (!MoveFileExA(tmp, path, MOVEFILE_REPLACE_EXISTING)) {
        win_set_errno();
        int s = errno;
        remove(tmp);
        free(tmp);
        errno = s;
        return -1;
    }
#else
    if (rename(tmp, path) != 0) {
        int s = errno;
        remove(tmp);
        free(tmp);
        errno = s;
        return -1;
    }
#endif
    free(tmp);
    return 0;
}

/* qsort comparator: order iovec descriptors by ascending offset. Shared by the
 * sparse-extend validators and, under SET+ATOMIC, bstack_set_batched /
 * bstack_process_gen. */
static int cmp_iovec_offset(const void *pa, const void *pb)
{
    uint64_t a = ((const bstack_iovec_t *)pa)->offset;
    uint64_t b = ((const bstack_iovec_t *)pb)->offset;
    return (a < b) ? -1 : (a > b) ? 1 : 0;
}

/* -------------------------------------------------------------------------
 * Sparse-extend helpers
 *
 * Shared by bstack_extend_sparse[_batched], their try_ variants, and the
 * BSTACK_GEN_SPARSE arm of bstack_process_gen.
 * ---------------------------------------------------------------------- */

/* Validate and compact a batch of sparse-extend blocks against a declared
 * extension of length bytes. Drops empty (len == 0) entries in place, then
 * rejects any block whose [offset, offset + len) overflows uint64_t or runs past
 * length, sorts the survivors by offset, and rejects any overlapping pair. The
 * compacted, sorted count is written to *out_n on success. Returns 0, or -1 with
 * errno = EINVAL on a validation failure. w is modified in place. */
static int validate_sparse_blocks(bstack_iovec_t *w, size_t count,
                                  uint64_t length, size_t *out_n)
{
    size_t n = 0;
    for (size_t i = 0; i < count; i++) {
        if (w[i].len != 0)
            w[n++] = w[i];
    }
    for (size_t i = 0; i < n; i++) {
        if ((uint64_t)w[i].len > UINT64_MAX - w[i].offset) { errno = EINVAL; return -1; }
        uint64_t end = w[i].offset + (uint64_t)w[i].len;
        if (end > length) { errno = EINVAL; return -1; }
    }
    qsort(w, n, sizeof(bstack_iovec_t), cmp_iovec_offset);
    for (size_t i = 0; i + 1 < n; i++) {
        uint64_t a_end = w[i].offset + (uint64_t)w[i].len;
        if (a_end > w[i + 1].offset) { errno = EINVAL; return -1; }
    }
    *out_n = n;
    return 0;
}

/* Commit a sparse payload growth to new_len (== logical_offset + length): extend
 * the file with one ftruncate (the OS zero-fills the new space), write each block
 * into the grown region, then commit the header length and durable-sync. blocks[]
 * holds (relative offset, source buf, len) with offsets measured from
 * logical_offset; callers guarantee each block fits within [logical_offset,
 * new_len) and blocks do not overlap. raw_size (== HEADER_SIZE + logical_offset)
 * is the pre-op file size, the rollback anchor.
 *
 * No journal is needed: the whole grown region sits beyond clen, so a crash
 * before the header commit rolls back by truncation, exactly like bstack_push. On
 * failure the file is rolled back (best effort) to raw_size and the cache/header
 * reset to logical_offset before returning -1 (the triggering errno preserved). */
static int commit_sparse_extend(bstack_t *bs, uint64_t logical_offset,
                                uint64_t raw_size, uint64_t new_len,
                                const bstack_iovec_t *blocks, size_t n_blocks)
{
    if (plat_ftruncate(bs->fd, HEADER_SIZE + new_len) != 0)
        return -1;
    for (size_t i = 0; i < n_blocks; i++) {
        if (plat_pwrite(bs->fd, blocks[i].buf, blocks[i].len,
                        HEADER_SIZE + logical_offset + blocks[i].offset) != 0) {
            int saved = errno;
            plat_ftruncate(bs->fd, raw_size);
            errno = saved;
            return -1;
        }
    }
    if (write_committed_len(bs->fd, &bs->clen, new_len) != 0 ||
        plat_durable_sync(bs->fd) != 0)
    {
        /* Rollback: truncate away the growth and reset the committed length. The
         * cache is reset up front so it reflects the rolled-back file even if the
         * best-effort header rewrite below fails. */
        int saved = errno;
        plat_ftruncate(bs->fd, raw_size);
        bs->clen = logical_offset;
        write_committed_len(bs->fd, &bs->clen, logical_offset);
        plat_durable_sync(bs->fd);
        errno = saved;
        return -1;
    }
    return 0;
}

/* -------------------------------------------------------------------------
 * bstack_push
 * ---------------------------------------------------------------------- */

int bstack_push(bstack_t *bs, const uint8_t *data, size_t len,
                uint64_t *out_offset)
{
    BS_WRLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t logical_offset = raw_size - HEADER_SIZE;

    if (len == 0) {
        BS_WRUNLOCK(bs);
        if (out_offset)
            *out_offset = logical_offset;
        return 0;
    }

    /* Write payload at end of file. */
    if (plat_pwrite(bs->fd, data, len, raw_size) != 0) {
        /* Best-effort rollback: truncate any partial write. */
        plat_ftruncate(bs->fd, raw_size);
        goto fail_unlock;
    }

    uint64_t new_len = logical_offset + (uint64_t)len;
    if (write_committed_len(bs->fd, &bs->clen, new_len) != 0 ||
        plat_durable_sync(bs->fd) != 0)
    {
        /* Rollback: remove written data and reset committed length. The
         * cache is reset up front so it reflects the rolled-back file even
         * if the best-effort header rewrite below fails. */
        plat_ftruncate(bs->fd, raw_size);
        bs->clen = logical_offset;
        write_committed_len(bs->fd, &bs->clen, logical_offset);
        plat_durable_sync(bs->fd);
        goto fail_unlock;
    }

    BS_WRUNLOCK(bs);
    if (out_offset)
        *out_offset = logical_offset;
    return 0;

fail_unlock:
    {
        int saved = errno;
        BS_WRUNLOCK(bs);
        errno = saved;
    }
    return -1;
}

/* -------------------------------------------------------------------------
 * bstack_extend
 * ---------------------------------------------------------------------- */

int bstack_extend(bstack_t *bs, size_t n, uint64_t *out_offset)
{
    BS_WRLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t logical_offset = raw_size - HEADER_SIZE;

    if (n == 0) {
        BS_WRUNLOCK(bs);
        if (out_offset)
            *out_offset = logical_offset;
        return 0;
    }

    /* Extend the file; the OS will zero-fill the new space. */
    uint64_t new_raw_size = raw_size + (uint64_t)n;
    if (plat_ftruncate(bs->fd, new_raw_size) != 0)
        goto fail_unlock;

    uint64_t new_len = logical_offset + (uint64_t)n;
    if (write_committed_len(bs->fd, &bs->clen, new_len) != 0 ||
        plat_durable_sync(bs->fd) != 0)
    {
        /* Rollback: truncate and reset committed length. The cache is reset
         * up front so it reflects the rolled-back file even if the
         * best-effort header rewrite below fails. */
        plat_ftruncate(bs->fd, raw_size);
        bs->clen = logical_offset;
        write_committed_len(bs->fd, &bs->clen, logical_offset);
        plat_durable_sync(bs->fd);
        goto fail_unlock;
    }

    BS_WRUNLOCK(bs);
    if (out_offset)
        *out_offset = logical_offset;
    return 0;

fail_unlock:
    {
        int saved = errno;
        BS_WRUNLOCK(bs);
        errno = saved;
    }
    return -1;
}

/* -------------------------------------------------------------------------
 * bstack_extend_sparse
 * ---------------------------------------------------------------------- */

int bstack_extend_sparse(bstack_t *bs, const uint8_t *buf, size_t buf_len,
                         uint64_t length, uint64_t *out_offset)
{
    if ((uint64_t)buf_len > length) { errno = EINVAL; return -1; }

    BS_WRLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t logical_offset = raw_size - HEADER_SIZE;

    if (length == 0) {
        BS_WRUNLOCK(bs);
        if (out_offset)
            *out_offset = logical_offset;
        return 0;
    }
    if (length > UINT64_MAX - logical_offset) {
        BS_WRUNLOCK(bs); errno = EINVAL; return -1;
    }
    uint64_t new_len = logical_offset + length;

    /* Single block at the start; empty buf means a pure sparse extend. */
    bstack_iovec_t one;
    size_t n_blocks = 0;
    if (buf_len != 0) {
        one.offset = 0;
        one.buf    = (uint8_t *)(uintptr_t)buf;
        one.len    = buf_len;
        n_blocks   = 1;
    }
    if (commit_sparse_extend(bs, logical_offset, raw_size, new_len,
                             n_blocks ? &one : NULL, n_blocks) != 0)
        goto fail_unlock;

    BS_WRUNLOCK(bs);
    if (out_offset)
        *out_offset = logical_offset;
    return 0;

fail_unlock:
    {
        int saved = errno;
        BS_WRUNLOCK(bs);
        errno = saved;
    }
    return -1;
}

/* -------------------------------------------------------------------------
 * bstack_extend_sparse_batched
 * ---------------------------------------------------------------------- */

int bstack_extend_sparse_batched(bstack_t *bs,
                                 const bstack_iovec_t *writes, size_t count,
                                 uint64_t length, uint64_t *out_offset)
{
    /* Materialise into a working array so it can be compacted, sorted, and
     * validated (before the lock, matching bstack_set_batched). */
    bstack_iovec_t *w = NULL;
    size_t n = 0;
    if (count != 0) {
        if (writes == NULL) { errno = EINVAL; return -1; }
        if (count > SIZE_MAX / sizeof(bstack_iovec_t)) { errno = EINVAL; return -1; }
        w = malloc(count * sizeof(bstack_iovec_t));
        if (!w) return -1;
        memcpy(w, writes, count * sizeof(bstack_iovec_t));
        if (validate_sparse_blocks(w, count, length, &n) != 0) {
            int saved = errno; free(w); errno = saved; return -1;
        }
    }

    BS_WRLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail;

    uint64_t logical_offset = raw_size - HEADER_SIZE;

    if (length == 0) {
        /* Every block was validated to fit within [0, 0), so n == 0. */
        BS_WRUNLOCK(bs);
        free(w);
        if (out_offset)
            *out_offset = logical_offset;
        return 0;
    }
    if (length > UINT64_MAX - logical_offset) {
        BS_WRUNLOCK(bs); free(w); errno = EINVAL; return -1;
    }
    uint64_t new_len = logical_offset + length;

    if (commit_sparse_extend(bs, logical_offset, raw_size, new_len, w, n) != 0)
        goto fail;

    BS_WRUNLOCK(bs);
    free(w);
    if (out_offset)
        *out_offset = logical_offset;
    return 0;

fail:
    {
        int saved = errno;
        BS_WRUNLOCK(bs);
        errno = saved;
    }
    free(w);
    return -1;
}

/* -------------------------------------------------------------------------
 * bstack_resize
 * ---------------------------------------------------------------------- */

int bstack_resize(bstack_t *bs, uint64_t target, uint64_t *out_initial_len)
{
    BS_WRLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;

    if (target == data_size) {
        BS_WRUNLOCK(bs);
        if (out_initial_len) *out_initial_len = data_size;
        return 0;
    }

    if (target < data_size) {
        uint64_t locked = ATOMIC_LOAD_ACQUIRE(&bs->locked);
        if (target < locked) {
            BS_WRUNLOCK(bs);
            errno = EINVAL;
            return -1;
        }

        if (plat_ftruncate(bs->fd, HEADER_SIZE + target) != 0)
            goto fail_unlock;
        /* Truncation is the commit point: update the cache now, before the
         * header write, which can fail and skip it (matching bstack_discard). */
        bs->clen = target;
        if (write_committed_len(bs->fd, &bs->clen, target) != 0 ||
            plat_durable_sync(bs->fd) != 0)
            goto fail_unlock;

        BS_WRUNLOCK(bs);
        if (out_initial_len) *out_initial_len = data_size;
        return 0;
    }

    /* Grow: the OS zero-fills the new space. */
    if (plat_ftruncate(bs->fd, HEADER_SIZE + target) != 0)
        goto fail_unlock;

    if (write_committed_len(bs->fd, &bs->clen, target) != 0 ||
        plat_durable_sync(bs->fd) != 0)
    {
        /* Best-effort rollback. The cache is reset up front so it reflects
         * the rolled-back file even if the header rewrite below fails. */
        plat_ftruncate(bs->fd, raw_size);
        bs->clen = data_size;
        write_committed_len(bs->fd, &bs->clen, data_size);
        plat_durable_sync(bs->fd);
        goto fail_unlock;
    }

    BS_WRUNLOCK(bs);
    if (out_initial_len) *out_initial_len = data_size;
    return 0;

fail_unlock:
    {
        int saved = errno;
        BS_WRUNLOCK(bs);
        errno = saved;
    }
    return -1;
}

/* -------------------------------------------------------------------------
 * bstack_ensure
 * ---------------------------------------------------------------------- */

int bstack_ensure(bstack_t *bs, uint64_t target, uint64_t *out_initial_len)
{
    BS_WRLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;

    if (target <= data_size) {
        BS_WRUNLOCK(bs);
        if (out_initial_len) *out_initial_len = data_size;
        return 0;
    }

    if (plat_ftruncate(bs->fd, HEADER_SIZE + target) != 0)
        goto fail_unlock;

    if (write_committed_len(bs->fd, &bs->clen, target) != 0 ||
        plat_durable_sync(bs->fd) != 0)
    {
        plat_ftruncate(bs->fd, raw_size);
        bs->clen = data_size;
        write_committed_len(bs->fd, &bs->clen, data_size);
        plat_durable_sync(bs->fd);
        goto fail_unlock;
    }

    BS_WRUNLOCK(bs);
    if (out_initial_len) *out_initial_len = data_size;
    return 0;

fail_unlock:
    {
        int saved = errno;
        BS_WRUNLOCK(bs);
        errno = saved;
    }
    return -1;
}

/* -------------------------------------------------------------------------
 * bstack_pop
 * ---------------------------------------------------------------------- */

int bstack_pop(bstack_t *bs, size_t n,
               uint8_t *buf, size_t *written_out)
{
    BS_WRLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;
    if ((uint64_t)n > data_size) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }

    uint64_t new_len = data_size - (uint64_t)n;
    
    /* Check if this would shrink below the locked length. */
    uint64_t locked = ATOMIC_LOAD_ACQUIRE(&bs->locked);
    if (new_len < locked) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }
    
    uint64_t read_offset = HEADER_SIZE + new_len;

    /* Read the bytes to be removed before truncating. */
    if (n > 0) {
        if (plat_pread(bs->fd, buf, n, read_offset) != 0)
            goto fail_unlock;
    }

    if (plat_ftruncate(bs->fd, HEADER_SIZE + new_len) != 0)
        goto fail_unlock;
    /* The truncation is the commit point: the tail bytes are gone and
     * recovery would adopt the smaller file size, so update the cache now —
     * before the header write, which can fail and skip it. */
    bs->clen = new_len;
    if (write_committed_len(bs->fd, &bs->clen, new_len) != 0 ||
        plat_durable_sync(bs->fd) != 0)
        goto fail_unlock;

    BS_WRUNLOCK(bs);
    if (written_out)
        *written_out = n;
    return 0;

fail_unlock:
    {
        int saved = errno;
        BS_WRUNLOCK(bs);
        errno = saved;
    }
    return -1;
}

/* -------------------------------------------------------------------------
 * bstack_peek
 * ---------------------------------------------------------------------- */

int bstack_peek(bstack_t *bs, uint64_t offset,
                uint8_t *buf, size_t *written_out)
{
    BS_RDLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;
    if (offset > data_size) {
        BS_RDUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }

    size_t to_read = (size_t)(data_size - offset);
    if (to_read > 0) {
        if (plat_pread(bs->fd, buf, to_read, HEADER_SIZE + offset) != 0)
            goto fail_unlock;
    }

    BS_RDUNLOCK(bs);
    if (written_out)
        *written_out = to_read;
    return 0;

fail_unlock:
    {
        int saved = errno;
        BS_RDUNLOCK(bs);
        errno = saved;
    }
    return -1;
}

/* -------------------------------------------------------------------------
 * bstack_get
 * ---------------------------------------------------------------------- */

int bstack_get(bstack_t *bs, uint64_t start, uint64_t end,
               uint8_t *buf)
{
    if (end < start) {
        errno = EINVAL;
        return -1;
    }

    /* Fast-path: range lies entirely within the locked region — skip the
     * rwlock.  On cached stacks serve from the in-memory buffer (under
     * cache_mutex); otherwise fall through to a lock-free pread. */
    uint64_t locked = ATOMIC_LOAD_ACQUIRE(&bs->locked);
    if (end <= locked) {
        size_t to_read = (size_t)(end - start);
        if (bs->cache_enabled) {
            if (to_read > 0) {
                CACHE_LOCK(bs);
                memcpy(buf, bs->cache_buf + start, to_read);
                CACHE_UNLOCK(bs);
            }
            return 0;
        }
        if (to_read > 0) {
            if (plat_pread(bs->fd, buf, to_read, HEADER_SIZE + start) != 0)
                return -1;
        }
        return 0;
    }

    BS_RDLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;
    if (end > data_size) {
        BS_RDUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }

    size_t to_read = (size_t)(end - start);
    if (to_read > 0) {
        if (plat_pread(bs->fd, buf, to_read, HEADER_SIZE + start) != 0)
            goto fail_unlock;
    }

    BS_RDUNLOCK(bs);
    return 0;

fail_unlock:
    {
        int saved = errno;
        BS_RDUNLOCK(bs);
        errno = saved;
    }
    return -1;
}

/* -------------------------------------------------------------------------
 * bstack_discard
 * ---------------------------------------------------------------------- */

int bstack_discard(bstack_t *bs, size_t n)
{
    if (n == 0)
        return 0;

    BS_WRLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;
    if ((uint64_t)n > data_size) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }

    uint64_t new_len = data_size - (uint64_t)n;
    
    /* Check if this would shrink below the locked length. */
    uint64_t locked = ATOMIC_LOAD_ACQUIRE(&bs->locked);
    if (new_len < locked) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }

    if (plat_ftruncate(bs->fd, HEADER_SIZE + new_len) != 0)
        goto fail_unlock;
    /* The truncation is the commit point: the tail bytes are gone and
     * recovery would adopt the smaller file size, so update the cache now —
     * before the header write, which can fail and skip it. */
    bs->clen = new_len;
    if (write_committed_len(bs->fd, &bs->clen, new_len) != 0 ||
        plat_durable_sync(bs->fd) != 0)
        goto fail_unlock;

    BS_WRUNLOCK(bs);
    return 0;

fail_unlock:
    {
        int saved = errno;
        BS_WRUNLOCK(bs);
        errno = saved;
    }
    return -1;
}

/* -------------------------------------------------------------------------
 * bstack_len
 * ---------------------------------------------------------------------- */

int bstack_len(bstack_t *bs, uint64_t *out_len)
{
    BS_RDLOCK(bs);
    *out_len = bs->clen;
    BS_RDUNLOCK(bs);
    return 0;
}

/* -------------------------------------------------------------------------
 * bstack_is_empty
 * ---------------------------------------------------------------------- */

int bstack_is_empty(bstack_t *bs, int *out_empty)
{
    BS_RDLOCK(bs);
    *out_empty = (bs->clen == 0) ? 1 : 0;
    BS_RDUNLOCK(bs);
    return 0;
}

/* -------------------------------------------------------------------------
 * bstack_locked_len / bstack_lock_up_to / bstack_open_locked_up_to
 * ---------------------------------------------------------------------- */

uint64_t bstack_locked_len(bstack_t *bs)
{
    return ATOMIC_LOAD_ACQUIRE(&bs->locked);
}

int bstack_lock_up_to(bstack_t *bs, uint64_t n)
{
    /* Acquire the write lock to serialize against any in-flight writers. */
    BS_WRLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0) {
        int saved = errno;
        BS_WRUNLOCK(bs);
        errno = saved;
        return -1;
    }
    uint64_t data_size = raw_size - HEADER_SIZE;

    uint64_t current_locked = ATOMIC_LOAD_ACQUIRE(&bs->locked);
    if (n < current_locked) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }
    if (n > data_size) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }

    /* Populate or extend the in-memory cache before publishing the new
     * boundary.  locked is only advanced after the cache is consistent,
     * so readers always see a coherent view. */
    if (bs->cache_enabled && n > current_locked) {
        uint64_t ol = current_locked;
        uint64_t nl = n;

        CACHE_LOCK(bs);

        if (nl > bs->cache_cap) {
            /* Reallocating: new power-of-2 buffer, copy existing valid bytes,
             * read the new portion from disk.  On failure new_buf is freed;
             * locked is unchanged and old cache remains valid for [0..ol]. */
            uint64_t new_cap = next_pow2_u64(nl);
            uint8_t *new_buf;
            if (new_cap == 0) { /* overflow: nl > 2^63 */
                CACHE_UNLOCK(bs);
                BS_WRUNLOCK(bs);
                errno = ENOMEM;
                return -1;
            }
#if UINT64_MAX > SIZE_MAX
            if (new_cap > (uint64_t)SIZE_MAX) {
                CACHE_UNLOCK(bs);
                BS_WRUNLOCK(bs);
                errno = ENOMEM;
                return -1;
            }
#endif
            new_buf = (uint8_t *)malloc((size_t)new_cap);
            if (!new_buf) {
                int saved = errno;
                CACHE_UNLOCK(bs);
                BS_WRUNLOCK(bs);
                errno = saved ? saved : ENOMEM;
                return -1;
            }
            if (ol > 0)
                memcpy(new_buf, bs->cache_buf, (size_t)ol);
            if (plat_pread(bs->fd, new_buf + ol,
                           (size_t)(nl - ol),
                           HEADER_SIZE + ol) != 0) {
                int saved = errno;
                free(new_buf);
                CACHE_UNLOCK(bs);
                BS_WRUNLOCK(bs);
                errno = saved;
                return -1;
            }
            free(bs->cache_buf);
            bs->cache_buf = new_buf;
            bs->cache_cap = new_cap;
        } else {
            /* In-place extend: cache_cap >= nl — read new bytes directly.
             * On failure the buffer still holds valid [0..ol] data. */
            if (plat_pread(bs->fd, bs->cache_buf + ol,
                           (size_t)(nl - ol),
                           HEADER_SIZE + ol) != 0) {
                int saved = errno;
                CACHE_UNLOCK(bs);
                BS_WRUNLOCK(bs);
                errno = saved;
                return -1;
            }
        }

        CACHE_UNLOCK(bs);
    }

    /* Release store: all writes completed under the write lock above are
     * visible to any thread that subsequently loads locked with Acquire. */
    ATOMIC_STORE_RELEASE(&bs->locked, n);
    BS_WRUNLOCK(bs);
    return 0;
}

bstack_t *bstack_open_locked_up_to(const char *path, uint64_t n)
{
    bstack_t *bs = bstack_open(path);
    if (!bs)
        return NULL;
    if (bstack_lock_up_to(bs, n) != 0) {
        int saved = errno;
        bstack_close(bs);
        errno = saved;
        return NULL;
    }
    return bs;
}

bstack_t *bstack_open_cached(const char *path)
{
    bstack_t *bs = bstack_open(path);
    if (!bs)
        return NULL;
    bs->cache_enabled = 1;
    return bs;
}

bstack_t *bstack_open_locked_up_to_cached(const char *path, uint64_t n)
{
    bstack_t *bs = bstack_open_cached(path);
    if (!bs)
        return NULL;
    if (bstack_lock_up_to(bs, n) != 0) {
        int saved = errno;
        bstack_close(bs);
        errno = saved;
        return NULL;
    }
    return bs;
}

/* =========================================================================
 * Journaled in-place mutators (see algos/WIP.md)
 *
 * These run with the write lock held and route each overwrite through the
 * write-in-progress journal (or the single-block atomic fast path), so a crash
 * leaves either the old bytes or the new bytes, never a mix. Callers must have
 * validated the range and the locked region. Helpers that change the committed
 * length take uint64_t *clen (= &bs->clen) and update it.
 * ====================================================================== */

#if defined(BSTACK_FEATURE_SET) || defined(BSTACK_FEATURE_ATOMIC)
/* Crash-atomically overwrite [offset, offset+len) with data (same length) via
 * the write-in-progress journal. data_size is the current payload length, so
 * HEADER_SIZE + data_size is both the committed end and the current file end. */
static int journaled_set(bstack_fd_t fd, uint64_t data_size,
                         uint64_t offset, const uint8_t *data, size_t len)
{
    /* 1. Stage: append data as a tail backup beyond the committed end. */
    if (plat_pwrite(fd, data, len, HEADER_SIZE + data_size) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 2. Arm: point wip_ptr at the physical target. */
    if (write_wip(fd, HEADER_SIZE + offset, WIP_SET) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 3. Commit in place. */
    if (plat_pwrite(fd, data, len, HEADER_SIZE + offset) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 4. Disarm. */
    if (write_wip(fd, 0, WIP_SET) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 5. Drop the tail backup. */
    return plat_ftruncate(fd, HEADER_SIZE + data_size);
}

/* Durably overwrite the same-length slice [offset, offset+len) with data,
 * picking the cheapest crash-safe strategy: a single-block write is atomic at
 * the storage level (one synced write); anything larger goes through the
 * journal. */
static int set_in_place(bstack_fd_t fd, uint64_t data_size,
                        uint64_t offset, const uint8_t *data, size_t len)
{
    if (is_atomic_write(offset, len)) {
        if (plat_pwrite(fd, data, len, HEADER_SIZE + offset) != 0) return -1;
        return plat_durable_sync(fd);
    }
    return journaled_set(fd, data_size, offset, data, len);
}
#endif /* SET || ATOMIC */

#ifdef BSTACK_FEATURE_SET
/* Crash-atomically fill [offset, offset + k*s_len) with k copies of s through
 * the repeat-fill journal. Only the pattern and count are staged, so the tail
 * is 8 + s_len bytes no matter how large the filled region. */
static int journaled_repeat(bstack_fd_t fd, uint64_t data_size,
                            uint64_t offset, const uint8_t *s, size_t s_len,
                            uint64_t k)
{
    uint64_t tail = HEADER_SIZE + data_size;
    /* 1. Stage [k | s] beyond the committed end. */
    uint8_t kbuf[8];
    for (int i = 0; i < 8; i++) kbuf[i] = (uint8_t)(k >> (8 * i));
    if (plat_pwrite(fd, kbuf, 8, tail) != 0) return -1;
    if (s_len > 0 && plat_pwrite(fd, s, s_len, tail + 8) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 2. Arm the repeat-fill journal at the target. */
    if (write_wip(fd, HEADER_SIZE + offset, WIP_REPEAT) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 3. Fill in place. */
    if (write_repeated(fd, HEADER_SIZE + offset, s, s_len, k) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 4. Disarm. */
    if (write_wip(fd, 0, WIP_SET) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 5. Drop the staged tail. */
    return plat_ftruncate(fd, HEADER_SIZE + data_size);
}

/* Fill [offset, offset + k*s_len) with k copies of s, crash-atomically,
 * choosing the atomic-block fast path or the repeat-fill journal. */
static int repeat_fill(bstack_fd_t fd, uint64_t data_size,
                       uint64_t offset, const uint8_t *s, size_t s_len,
                       uint64_t k)
{
    uint64_t total = k * (uint64_t)s_len;
    if (is_atomic_write(offset, total)) {
        if (write_repeated(fd, HEADER_SIZE + offset, s, s_len, k) != 0) return -1;
        return plat_durable_sync(fd);
    }
    return journaled_repeat(fd, data_size, offset, s, s_len, k);
}
#endif /* SET */

#if defined(BSTACK_FEATURE_SET) && defined(BSTACK_FEATURE_ATOMIC)
/* Crash-atomically copy n bytes from logical src to dst when the two regions
 * are DISJOINT, through the copy journal — O(1) memory and O(1) staging (only
 * the source coordinate [src | n] is journaled, never the bytes). */
static int journaled_copy(bstack_fd_t fd, uint64_t data_size,
                          uint64_t src, uint64_t dst, uint64_t n)
{
    uint8_t meta[16];
    for (int i = 0; i < 8; i++) meta[i]     = (uint8_t)(src >> (8 * i));
    for (int i = 0; i < 8; i++) meta[8 + i] = (uint8_t)(n   >> (8 * i));
    /* 1. Stage [src | n] past the payload so recovery knows the source. */
    if (plat_pwrite(fd, meta, 16, HEADER_SIZE + data_size) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 2. Arm the destination as the replay target. */
    if (write_wip(fd, HEADER_SIZE + dst, WIP_COPY) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 3. Commit: stream source -> destination in place (disjoint -> idempotent). */
    if (move_chunked(fd, src, dst, n) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 4. Disarm. */
    if (write_wip(fd, 0, WIP_SET) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 5. Drop the staged metadata. */
    return plat_ftruncate(fd, HEADER_SIZE + data_size);
}

/* Crash-atomically copy n bytes from logical src to dst (same length) through
 * the write-in-progress journal, O(1) memory. src and dst may overlap: the
 * bytes route through the disjoint tail backup. */
static int journaled_move(bstack_fd_t fd, uint64_t data_size,
                          uint64_t src, uint64_t dst, uint64_t n)
{
    /* 1. Stage the source bytes in the tail. */
    if (move_chunked(fd, src, data_size, n) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 2. Arm the destination as the replay target. */
    if (write_wip(fd, HEADER_SIZE + dst, WIP_SET) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 3. Commit: stream the staged bytes into the destination. */
    if (move_chunked(fd, data_size, dst, n) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 4. Disarm. */
    if (write_wip(fd, 0, WIP_SET) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 5. Drop the tail. */
    return plat_ftruncate(fd, HEADER_SIZE + data_size);
}

/* Crash-atomically exchange the equal-length regions [a, a+n) and [b, b+n):
 * on return A holds B's old bytes and B holds A's old bytes. O(1) memory. Only
 * A's original bytes are staged; the exchange commits at a single wip_ptr flip
 * from A to B, reusing the WIP_SET replay format. */
static int journaled_exchange(bstack_fd_t fd, uint64_t data_size,
                              uint64_t a, uint64_t b, uint64_t n)
{
    /* 1. Stage A's bytes beyond the committed end as the replay backup. */
    if (move_chunked(fd, a, data_size, n) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 2. Arm "replay tail into A" — while armed at A a crash rolls back. */
    if (write_wip(fd, HEADER_SIZE + a, WIP_SET) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 3. A <- B's bytes (streamed straight from B). */
    if (move_chunked(fd, b, a, n) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 4. Flip to "replay tail into B" — the atomic commit point. */
    if (write_wip(fd, HEADER_SIZE + b, WIP_SET) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 5. B <- A's bytes (streamed from the tail). */
    if (move_chunked(fd, data_size, b, n) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 6. Disarm and drop the staged tail. */
    if (write_wip(fd, 0, WIP_SET) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    return plat_ftruncate(fd, HEADER_SIZE + data_size);
}

/* Crash-atomically commit count non-overlapping in-place writes {(offset_i,
 * buf_i, len_i)} as one unit through the multi-write journal, O(1) memory beyond
 * the caller's own buffers. data_size is the current payload length; it is never
 * changed. Callers guarantee every block lies within [0, data_size), no block
 * overlaps the locked prefix, the blocks are pairwise non-overlapping, and
 * count >= 2 (a lone write should take set_in_place, an empty batch is a no-op).
 * The four-barrier protocol (stage -> arm -> replay -> disarm) makes a crash
 * leave either every block's old bytes or every block's new bytes. wip_ptr stays
 * 0 throughout; the WIP_MULTI sentinel is the commit point. */
static int journaled_multi_set(bstack_fd_t fd, uint64_t data_size,
                               const bstack_iovec_t *writes, size_t count)
{
    /* 1. Stage every block [s | e | data] back-to-back beyond the committed
     *    end; the file grows to hold them. */
    uint64_t phys = HEADER_SIZE + data_size;
    for (size_t i = 0; i < count; i++) {
        uint64_t s = writes[i].offset;
        uint64_t e = s + (uint64_t)writes[i].len;
        uint8_t hdr[16];
        for (int j = 0; j < 8; j++) hdr[j]     = (uint8_t)(s >> (8 * j));
        for (int j = 0; j < 8; j++) hdr[8 + j] = (uint8_t)(e >> (8 * j));
        if (plat_pwrite(fd, hdr, 16, phys) != 0) return -1;
        phys += 16;
        if (writes[i].len > 0) {
            if (plat_pwrite(fd, writes[i].buf, writes[i].len, phys) != 0)
                return -1;
            phys += writes[i].len;
        }
    }
    if (plat_durable_sync(fd) != 0) return -1;
    /* 2. Arm the intent-complete sentinel; wip_ptr stays 0 (single header
     *    write), so this can never be confused with a single-region journal. */
    if (write_wip(fd, 0, WIP_MULTI) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 3. Replay: write each block into its target in place. Order is arbitrary
     *    (the ranges are non-overlapping); each block's staged copy is the crash
     *    backup recovery replays from. */
    for (size_t i = 0; i < count; i++) {
        if (writes[i].len > 0) {
            if (plat_pwrite(fd, writes[i].buf, writes[i].len,
                            HEADER_SIZE + writes[i].offset) != 0)
                return -1;
        }
    }
    if (plat_durable_sync(fd) != 0) return -1;
    /* 4. Disarm. */
    if (write_wip(fd, 0, WIP_SET) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 5. Drop the staged tail, restoring file_size == HEADER_SIZE + data_size. */
    return plat_ftruncate(fd, HEADER_SIZE + data_size);
}
#endif /* SET && ATOMIC */

#ifdef BSTACK_FEATURE_ATOMIC
/* Commit a payload shrink to new_len: truncate, update the cache, write the
 * header, and sync. The truncation is the commit point, so the cache is updated
 * before the header write. */
static int commit_shrink(bstack_fd_t fd, uint64_t *clen, uint64_t new_len)
{
    if (plat_ftruncate(fd, HEADER_SIZE + new_len) != 0) return -1;
    *clen = new_len;
    if (write_committed_len(fd, clen, new_len) != 0) return -1;
    return plat_durable_sync(fd);
}

/* Crash-atomically replace the last n_old payload bytes with dn (length m !=
 * n_old, both non-zero), changing the length to a + m where a = *clen - n_old,
 * via the splice journal. O(1) memory beyond dn. Recovery derives clen' from
 * the file size and rolls a crash forward. */
static int journaled_splice(bstack_fd_t fd, uint64_t *clen,
                            uint64_t n_old, const uint8_t *dn, uint64_t m)
{
    uint64_t old_clen = *clen;
    uint64_t a = old_clen - n_old;          /* splice point */
    uint64_t clen_new = a + m;
    uint64_t s = (old_clen > clen_new) ? old_clen : clen_new; /* staging base */

    /* 1. Stage dn at [s, s+m), disjoint from the live payload and the target. */
    if (plat_ftruncate(fd, HEADER_SIZE + s + m) != 0) return -1;
    if (plat_pwrite(fd, dn, (size_t)m, HEADER_SIZE + s) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 2. Arm with the direction; recovery derives clen' from the file size. */
    uint64_t dir = (m > n_old) ? WIP_SPLICE_GROW : WIP_SPLICE_SHRINK;
    if (write_wip(fd, HEADER_SIZE + a, dir) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 3. Replay dn into the target [a, clen'). Disjoint -> idempotent. */
    if (move_chunked(fd, s, a, m) != 0) return -1;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 4. Commit the new length while disarming, in one atomic header write. */
    if (write_header_commit(fd, clen_new, 0, WIP_SET) != 0) return -1;
    *clen = clen_new;
    if (plat_durable_sync(fd) != 0) return -1;
    /* 5. Drop the staged bytes. */
    return plat_ftruncate(fd, HEADER_SIZE + clen_new);
}

/* Commit a tail replacement: replace the last n payload bytes with buf,
 * changing the length to *clen - n + buf_len. new_tail_start is the splice
 * point a == *clen - n; file_end is the pre-op raw file size (rollback anchor
 * for the append path). Dispatches by shape to the cheapest crash-atomic path.
 * Shared by atrunc, splice, and replace. */
static int commit_tail_replace(bstack_fd_t fd, uint64_t *clen,
                               uint64_t new_tail_start, uint64_t n,
                               const uint8_t *buf, uint64_t m,
                               uint64_t file_end)
{
    uint64_t a = new_tail_start; /* == *clen - n */
    if (m == 0) {
        /* Pure truncation (or a no-op when n == 0). */
        return commit_shrink(fd, clen, a);
    } else if (n == 0) {
        /* Pure append: buf lands beyond the committed end, uncommitted until
         * the clen write, so a crash rolls back by truncation. */
        uint64_t final_data_len = a + m;
        if (plat_ftruncate(fd, HEADER_SIZE + final_data_len) != 0) return -1;
        if (plat_pwrite(fd, buf, (size_t)m, HEADER_SIZE + a) != 0) {
            plat_ftruncate(fd, file_end); /* best-effort rollback */
            return -1;
        }
        if (plat_durable_sync(fd) != 0) {
            plat_ftruncate(fd, file_end);
            return -1;
        }
        if (write_committed_len(fd, clen, final_data_len) != 0) return -1;
        return plat_durable_sync(fd);
    } else if (m == n) {
        /* Same-length overwrite of the tail: the Set journal suffices. */
        return set_in_place(fd, *clen, a, buf, (size_t)m);
    } else {
        /* Length-changing tail replace: journal it. */
        return journaled_splice(fd, clen, n, buf, m);
    }
}
#endif /* ATOMIC */

/* -------------------------------------------------------------------------
 * bstack_set  (only compiled with -DBSTACK_FEATURE_SET)
 * ---------------------------------------------------------------------- */

#ifdef BSTACK_FEATURE_SET
int bstack_set(bstack_t *bs, uint64_t offset,
               const uint8_t *data, size_t len)
{
    if (len == 0)
        return 0;

    /* Guard against offset + len wrapping around. */
    if ((uint64_t)len > UINT64_MAX - offset) {
        errno = EINVAL;
        return -1;
    }
    uint64_t end = offset + (uint64_t)len;

    BS_WRLOCK(bs);
    
    /* Load locked under the write lock — otherwise a concurrent
     * lock_up_to could extend the locked region between our check and
     * our write, letting us mutate a now-immutable byte. */
    uint64_t locked = ATOMIC_LOAD_ACQUIRE(&bs->locked);
    if (offset < locked) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;
    if (end > data_size) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }

    /* Crash-atomic same-length overwrite: single-block atomic write, else the
     * write-in-progress journal. */
    if (set_in_place(bs->fd, data_size, offset, data, len) != 0)
        goto fail_unlock;

    BS_WRUNLOCK(bs);
    return 0;

fail_unlock:
    {
        int saved = errno;
        BS_WRUNLOCK(bs);
        errno = saved;
    }
    return -1;
}

int bstack_zero(bstack_t *bs, uint64_t offset, size_t n)
{
    /* Zeroing is a repeat-fill of the single-byte pattern 0x00 n times. */
    static const uint8_t ZERO = 0;
    return bstack_repeat(bs, offset, &ZERO, 1, n);
}

int bstack_repeat(bstack_t *bs, uint64_t offset,
                  const uint8_t *pattern, size_t pattern_len, uint64_t count)
{
    if (pattern_len == 0 || count == 0)
        return 0;

    /* Guard against count * pattern_len and offset + total wrapping around. */
    if (count > UINT64_MAX / (uint64_t)pattern_len) {
        errno = EINVAL;
        return -1;
    }
    uint64_t total = count * (uint64_t)pattern_len;
    if (total > UINT64_MAX - offset) {
        errno = EINVAL;
        return -1;
    }
    uint64_t end = offset + total;

    BS_WRLOCK(bs);

    /* Load locked under the write lock (see bstack_set for rationale). */
    uint64_t locked = ATOMIC_LOAD_ACQUIRE(&bs->locked);
    if (offset < locked) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;
    if (end > data_size) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }

    /* Only the pattern and count are journaled: a fixed 8 + pattern_len tail
     * regardless of how large the filled region is. */
    if (repeat_fill(bs->fd, data_size, offset, pattern, pattern_len, count) != 0)
        goto fail_unlock;

    BS_WRUNLOCK(bs);
    return 0;

fail_unlock:
    {
        int saved = errno;
        BS_WRUNLOCK(bs);
        errno = saved;
    }
    return -1;
}

#endif /* BSTACK_FEATURE_SET */

/* -------------------------------------------------------------------------
 * Atomic compound operations  (only compiled with -DBSTACK_FEATURE_ATOMIC)
 * ---------------------------------------------------------------------- */

#ifdef BSTACK_FEATURE_ATOMIC

int bstack_atrunc(bstack_t *bs, size_t n,
                  const uint8_t *buf, size_t buf_len)
{
    if (n == 0 && buf_len == 0)
        return 0;

    BS_WRLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;
    if ((uint64_t)n > data_size) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }
    
    uint64_t locked = ATOMIC_LOAD_ACQUIRE(&bs->locked);
    uint64_t new_tail_start = data_size - (uint64_t)n;
    if (new_tail_start < locked) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }

    if (commit_tail_replace(bs->fd, &bs->clen, new_tail_start,
                            (uint64_t)n, buf, (uint64_t)buf_len, raw_size) != 0)
        goto fail_unlock;

    BS_WRUNLOCK(bs);
    return 0;

fail_unlock:
    { int s = errno; BS_WRUNLOCK(bs); errno = s; }
    return -1;
}

int bstack_splice(bstack_t *bs,
                  uint8_t *removed, size_t n,
                  const uint8_t *new_buf, size_t new_len)
{
    if (n == 0 && new_len == 0)
        return 0;

    BS_WRLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;
    if ((uint64_t)n > data_size) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }
    
    uint64_t locked = ATOMIC_LOAD_ACQUIRE(&bs->locked);
    uint64_t new_tail_start = data_size - (uint64_t)n;
    if (new_tail_start < locked) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }

    uint64_t tail_offset = HEADER_SIZE + new_tail_start;

    /* Read removed bytes before any mutation. */
    if (n > 0 && removed != NULL) {
        if (plat_pread(bs->fd, removed, n, tail_offset) != 0)
            goto fail_unlock;
    }

    if (commit_tail_replace(bs->fd, &bs->clen, new_tail_start,
                            (uint64_t)n, new_buf, (uint64_t)new_len, raw_size) != 0)
        goto fail_unlock;

    BS_WRUNLOCK(bs);
    return 0;

fail_unlock:
    { int s = errno; BS_WRUNLOCK(bs); errno = s; }
    return -1;
}

int bstack_try_extend(bstack_t *bs, uint64_t s,
                      const uint8_t *buf, size_t buf_len, int *ok)
{
    BS_WRLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;
    if (data_size != s) {
        BS_WRUNLOCK(bs);
        if (ok) *ok = 0;
        return 0;
    }
    if (buf_len == 0) {
        BS_WRUNLOCK(bs);
        if (ok) *ok = 1;
        return 0;
    }

    /* Same sequence as bstack_push. */
    if (plat_pwrite(bs->fd, buf, buf_len, raw_size) != 0) {
        plat_ftruncate(bs->fd, raw_size);
        goto fail_unlock;
    }
    uint64_t new_len = data_size + (uint64_t)buf_len;
    if (write_committed_len(bs->fd, &bs->clen, new_len) != 0 ||
        plat_durable_sync(bs->fd) != 0) {
        /* The cache is reset up front so it reflects the rolled-back file
         * even if the best-effort header rewrite below fails. */
        plat_ftruncate(bs->fd, raw_size);
        bs->clen = data_size;
        write_committed_len(bs->fd, &bs->clen, data_size);
        plat_durable_sync(bs->fd);
        goto fail_unlock;
    }

    BS_WRUNLOCK(bs);
    if (ok) *ok = 1;
    return 0;

fail_unlock:
    { int s = errno; BS_WRUNLOCK(bs); errno = s; }
    return -1;
}

int bstack_try_discard(bstack_t *bs, uint64_t s, size_t n, int *ok)
{
    if (n == 0) {
        /* Read-only path: just check the size. */
        BS_RDLOCK(bs);
        uint64_t raw_size;
        if (file_size(bs->fd, &raw_size) != 0) {
            int saved = errno;
            BS_RDUNLOCK(bs);
            errno = saved;
            return -1;
        }
        BS_RDUNLOCK(bs);
        if (ok) *ok = ((raw_size - HEADER_SIZE) == s) ? 1 : 0;
        return 0;
    }

    BS_WRLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;
    if (data_size != s) {
        BS_WRUNLOCK(bs);
        if (ok) *ok = 0;
        return 0;
    }
    if ((uint64_t)n > data_size) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }

    uint64_t new_len = data_size - (uint64_t)n;
    
    /* Check if this would shrink below the locked length. */
    uint64_t locked = ATOMIC_LOAD_ACQUIRE(&bs->locked);
    if (new_len < locked) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }
    
    if (plat_ftruncate(bs->fd, HEADER_SIZE + new_len) != 0)
        goto fail_unlock;
    /* The truncation is the commit point: the tail bytes are gone and
     * recovery would adopt the smaller file size, so update the cache now —
     * before the header write, which can fail and skip it. */
    bs->clen = new_len;
    if (write_committed_len(bs->fd, &bs->clen, new_len) != 0 ||
        plat_durable_sync(bs->fd) != 0)
        goto fail_unlock;

    BS_WRUNLOCK(bs);
    if (ok) *ok = 1;
    return 0;

fail_unlock:
    { int s = errno; BS_WRUNLOCK(bs); errno = s; }
    return -1;
}

int bstack_replace(bstack_t *bs, size_t n,
                   int (*cb)(const uint8_t *old, size_t old_len,
                              uint8_t **new_buf, size_t *new_len,
                              void *ctx),
                   void *ctx)
{
    BS_WRLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;
    if ((uint64_t)n > data_size) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }
    
    uint64_t locked = ATOMIC_LOAD_ACQUIRE(&bs->locked);
    uint64_t new_tail_start = data_size - (uint64_t)n;
    if (new_tail_start < locked) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }

    uint64_t tail_offset = HEADER_SIZE + new_tail_start;

    /* Read old tail bytes (NULL when n == 0 — callback must check old_len). */
    uint8_t *old_tail = NULL;
    if (n > 0) {
        old_tail = (uint8_t *)malloc(n);
        if (old_tail == NULL)
            goto fail_unlock;
        if (plat_pread(bs->fd, old_tail, n, tail_offset) != 0) {
            free(old_tail);
            goto fail_unlock;
        }
    }

    /* Invoke callback to produce new tail. */
    uint8_t *new_buf = NULL;
    size_t   new_len = 0;
    int cb_ret = cb(old_tail, n, &new_buf, &new_len, ctx);
    free(old_tail);

    if (cb_ret != 0)
        goto fail_unlock;

    /* Skip all I/O when both sides are empty. */
    if (n == 0 && new_len == 0) {
        BS_WRUNLOCK(bs);
        return 0;
    }

    if (commit_tail_replace(bs->fd, &bs->clen, new_tail_start,
                            (uint64_t)n, new_buf, (uint64_t)new_len,
                            raw_size) != 0) {
        free(new_buf);
        goto fail_unlock;
    }
    free(new_buf);

    BS_WRUNLOCK(bs);
    return 0;

fail_unlock:
    { int s = errno; BS_WRUNLOCK(bs); errno = s; }
    return -1;
}

int bstack_try_extend_zeros(bstack_t *bs, uint64_t s, size_t n, int *ok)
{
    BS_WRLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;
    if (data_size != s) {
        BS_WRUNLOCK(bs);
        if (ok) *ok = 0;
        return 0;
    }
    if (n == 0) {
        BS_WRUNLOCK(bs);
        if (ok) *ok = 1;
        return 0;
    }

    uint64_t new_len = data_size + (uint64_t)n;
    if (plat_ftruncate(bs->fd, HEADER_SIZE + new_len) != 0 ||
        write_committed_len(bs->fd, &bs->clen, new_len) != 0 ||
        plat_durable_sync(bs->fd) != 0)
    {
        /* Best-effort rollback. The cache is reset up front so it reflects
         * the rolled-back file even if the header rewrite below fails. */
        plat_ftruncate(bs->fd, raw_size);
        bs->clen = data_size;
        write_committed_len(bs->fd, &bs->clen, data_size);
        plat_durable_sync(bs->fd);
        goto fail_unlock;
    }

    BS_WRUNLOCK(bs);
    if (ok) *ok = 1;
    return 0;

fail_unlock:
    { int sv = errno; BS_WRUNLOCK(bs); errno = sv; }
    return -1;
}

int bstack_try_extend_sparse(bstack_t *bs, uint64_t s,
                             const uint8_t *buf, size_t buf_len,
                             uint64_t length, int *ok)
{
    /* Reject a malformed request before the lock, regardless of the size guard. */
    if ((uint64_t)buf_len > length) { errno = EINVAL; return -1; }

    BS_WRLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;
    if (data_size != s) {
        BS_WRUNLOCK(bs);
        if (ok) *ok = 0;
        return 0;
    }
    if (length == 0) {
        BS_WRUNLOCK(bs);
        if (ok) *ok = 1;
        return 0;
    }
    if (length > UINT64_MAX - data_size) {
        BS_WRUNLOCK(bs); errno = EINVAL; return -1;
    }
    uint64_t new_len = data_size + length;

    bstack_iovec_t one;
    size_t n_blocks = 0;
    if (buf_len != 0) {
        one.offset = 0;
        one.buf    = (uint8_t *)(uintptr_t)buf;
        one.len    = buf_len;
        n_blocks   = 1;
    }
    if (commit_sparse_extend(bs, data_size, raw_size, new_len,
                             n_blocks ? &one : NULL, n_blocks) != 0)
        goto fail_unlock;

    BS_WRUNLOCK(bs);
    if (ok) *ok = 1;
    return 0;

fail_unlock:
    { int sv = errno; BS_WRUNLOCK(bs); errno = sv; }
    return -1;
}

int bstack_try_extend_sparse_batched(bstack_t *bs, uint64_t s,
                                     const bstack_iovec_t *writes, size_t count,
                                     uint64_t length, int *ok)
{
    /* Validate the batch up front (before the lock and the size guard), so a
     * malformed request always surfaces rather than being masked by a mismatch. */
    bstack_iovec_t *w = NULL;
    size_t n = 0;
    if (count != 0) {
        if (writes == NULL) { errno = EINVAL; return -1; }
        if (count > SIZE_MAX / sizeof(bstack_iovec_t)) { errno = EINVAL; return -1; }
        w = malloc(count * sizeof(bstack_iovec_t));
        if (!w) return -1;
        memcpy(w, writes, count * sizeof(bstack_iovec_t));
        if (validate_sparse_blocks(w, count, length, &n) != 0) {
            int saved = errno; free(w); errno = saved; return -1;
        }
    }

    BS_WRLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail;

    uint64_t data_size = raw_size - HEADER_SIZE;
    if (data_size != s) {
        BS_WRUNLOCK(bs); free(w);
        if (ok) *ok = 0;
        return 0;
    }
    if (length == 0) {
        BS_WRUNLOCK(bs); free(w);
        if (ok) *ok = 1;
        return 0;
    }
    if (length > UINT64_MAX - data_size) {
        BS_WRUNLOCK(bs); free(w); errno = EINVAL; return -1;
    }
    uint64_t new_len = data_size + length;

    if (commit_sparse_extend(bs, data_size, raw_size, new_len, w, n) != 0)
        goto fail;

    BS_WRUNLOCK(bs); free(w);
    if (ok) *ok = 1;
    return 0;

fail:
    { int sv = errno; BS_WRUNLOCK(bs); errno = sv; }
    free(w);
    return -1;
}

/* -------------------------------------------------------------------------
 * bstack_ensure_with
 * ---------------------------------------------------------------------- */

int bstack_ensure_with(bstack_t *bs, uint64_t target,
                       int (*cb)(uint8_t *buf, size_t len, void *ctx),
                       void *ctx, uint64_t *out_initial_len)
{
    BS_WRLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;

    if (target <= data_size) {
        BS_WRUNLOCK(bs);
        if (out_initial_len) *out_initial_len = data_size;
        return 0;
    }

    uint64_t growth64 = target - data_size;
#if UINT64_MAX > SIZE_MAX
    if (growth64 > (uint64_t)SIZE_MAX) {
        BS_WRUNLOCK(bs);
        errno = ENOMEM;
        return -1;
    }
#endif
    size_t growth = (size_t)growth64;

    uint8_t *buf = (uint8_t *)calloc(1, growth);
    if (buf == NULL)
        goto fail_unlock;

    if (cb(buf, growth, ctx) != 0) {
        free(buf);
        goto fail_unlock;
    }

    if (plat_pwrite(bs->fd, buf, growth, raw_size) != 0) {
        free(buf);
        plat_ftruncate(bs->fd, raw_size);
        goto fail_unlock;
    }
    free(buf);

    if (write_committed_len(bs->fd, &bs->clen, target) != 0 ||
        plat_durable_sync(bs->fd) != 0)
    {
        /* Best-effort rollback. The cache is reset up front so it reflects
         * the rolled-back file even if the header rewrite below fails. */
        plat_ftruncate(bs->fd, raw_size);
        bs->clen = data_size;
        write_committed_len(bs->fd, &bs->clen, data_size);
        plat_durable_sync(bs->fd);
        goto fail_unlock;
    }

    BS_WRUNLOCK(bs);
    if (out_initial_len) *out_initial_len = data_size;
    return 0;

fail_unlock:
    {
        int saved = errno;
        BS_WRUNLOCK(bs);
        errno = saved;
    }
    return -1;
}

int bstack_get_batched(bstack_t *bs,
                       const bstack_iovec_t *entries, size_t n_entries)
{
    if (n_entries == 0)
        return 0;

    BS_RDLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;

    for (size_t i = 0; i < n_entries; i++) {
        if ((uint64_t)entries[i].len > UINT64_MAX - entries[i].offset) {
            BS_RDUNLOCK(bs);
            errno = EINVAL;
            return -1;
        }
        uint64_t end = entries[i].offset + (uint64_t)entries[i].len;
        if (end > data_size) {
            BS_RDUNLOCK(bs);
            errno = EINVAL;
            return -1;
        }
        if (entries[i].len > 0) {
            if (plat_pread(bs->fd, entries[i].buf, entries[i].len,
                           HEADER_SIZE + entries[i].offset) != 0)
                goto fail_unlock;
        }
    }

    BS_RDUNLOCK(bs);
    return 0;

fail_unlock:
    { int sv = errno; BS_RDUNLOCK(bs); errno = sv; }
    return -1;
}

int bstack_get_batched_gen(bstack_t *bs,
                           int (*gen)(uint64_t *out_offset, uint8_t **out_buf,
                                      size_t *out_len, void *ctx),
                           void *ctx)
{
    BS_RDLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;

    for (;;) {
        uint64_t  offset = 0;
        uint8_t  *buf    = NULL;
        size_t    len    = 0;
        int r = gen(&offset, &buf, &len, ctx);
        if (r == 0)
            break;
        if (r < 0)
            goto fail_unlock;
        if ((uint64_t)len > UINT64_MAX - offset) {
            BS_RDUNLOCK(bs);
            errno = EINVAL;
            return -1;
        }
        uint64_t end = offset + (uint64_t)len;
        if (end > data_size) {
            BS_RDUNLOCK(bs);
            errno = EINVAL;
            return -1;
        }
        if (len > 0) {
            if (plat_pread(bs->fd, buf, len, HEADER_SIZE + offset) != 0)
                goto fail_unlock;
        }
    }

    BS_RDUNLOCK(bs);
    return 0;

fail_unlock:
    { int sv = errno; BS_RDUNLOCK(bs); errno = sv; }
    return -1;
}

#endif /* BSTACK_FEATURE_ATOMIC */

/* -------------------------------------------------------------------------
 * swap / cas  (require both BSTACK_FEATURE_ATOMIC and BSTACK_FEATURE_SET)
 * ---------------------------------------------------------------------- */

#if defined(BSTACK_FEATURE_ATOMIC) && defined(BSTACK_FEATURE_SET)

int bstack_swap(bstack_t *bs, uint64_t offset,
                uint8_t *old_buf, const uint8_t *new_buf, size_t len)
{
    if (len == 0)
        return 0;
    if ((uint64_t)len > UINT64_MAX - offset) {
        errno = EINVAL;
        return -1;
    }
    uint64_t end = offset + (uint64_t)len;

    BS_WRLOCK(bs);
    
    /* Load locked under the write lock (see bstack_set for rationale). */
    uint64_t locked = ATOMIC_LOAD_ACQUIRE(&bs->locked);
    if (offset < locked) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;
    if (end > data_size) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }

    if (plat_pread(bs->fd, old_buf, len, HEADER_SIZE + offset) != 0)
        goto fail_unlock;
    if (set_in_place(bs->fd, data_size, offset, new_buf, len) != 0)
        goto fail_unlock;

    BS_WRUNLOCK(bs);
    return 0;

fail_unlock:
    { int s = errno; BS_WRUNLOCK(bs); errno = s; }
    return -1;
}

int bstack_cas(bstack_t *bs, uint64_t offset,
               const uint8_t *old_buf, const uint8_t *new_buf,
               size_t len, int *ok)
{
    if (len == 0) {
        if (ok) *ok = 1;
        return 0;
    }
    if ((uint64_t)len > UINT64_MAX - offset) {
        errno = EINVAL;
        return -1;
    }
    uint64_t end = offset + (uint64_t)len;

    BS_WRLOCK(bs);
    
    /* Load locked under the write lock (see bstack_set for rationale). */
    uint64_t locked = ATOMIC_LOAD_ACQUIRE(&bs->locked);
    if (offset < locked) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;
    if (end > data_size) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }

    /* Compare in fixed-size stack chunks — no heap allocation. */
    uint8_t chunk[256];
    size_t  remaining = len;
    uint64_t file_off = HEADER_SIZE + offset;
    const uint8_t *cmp = old_buf;
    while (remaining > 0) {
        size_t batch = remaining < sizeof chunk ? remaining : sizeof chunk;
        if (plat_pread(bs->fd, chunk, batch, file_off) != 0)
            goto fail_unlock;
        if (memcmp(chunk, cmp, batch) != 0) {
            BS_WRUNLOCK(bs);
            if (ok) *ok = 0;
            return 0;
        }
        cmp       += batch;
        file_off  += batch;
        remaining -= batch;
    }

    /* All bytes matched — crash-atomically write new_buf. */
    if (set_in_place(bs->fd, data_size, offset, new_buf, len) != 0)
        goto fail_unlock;

    BS_WRUNLOCK(bs);
    if (ok) *ok = 1;
    return 0;

fail_unlock:
    { int s = errno; BS_WRUNLOCK(bs); errno = s; }
    return -1;
}

int bstack_process(bstack_t *bs, uint64_t start, uint64_t end,
                   int (*cb)(uint8_t *buf, size_t len, void *ctx),
                   void *ctx)
{
    if (end < start) {
        errno = EINVAL;
        return -1;
    }
    uint64_t n = end - start;

    BS_WRLOCK(bs);
    
    /* Load locked under the write lock (see bstack_set for rationale). */
    uint64_t locked = ATOMIC_LOAD_ACQUIRE(&bs->locked);
    if (start < locked) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;
    if (end > data_size) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }

    uint8_t *buf = NULL;
    if (n > 0) {
        buf = (uint8_t *)malloc((size_t)n);
        if (buf == NULL)
            goto fail_unlock;
        if (plat_pread(bs->fd, buf, (size_t)n, HEADER_SIZE + start) != 0) {
            free(buf);
            goto fail_unlock;
        }
    }

    if (cb(buf, (size_t)n, ctx) != 0) {
        free(buf);
        goto fail_unlock;
    }

    if (n > 0) {
        if (set_in_place(bs->fd, data_size, start, buf, (size_t)n) != 0) {
            free(buf);
            goto fail_unlock;
        }
        free(buf);
    }

    BS_WRUNLOCK(bs);
    return 0;

fail_unlock:
    { int s = errno; BS_WRUNLOCK(bs); errno = s; }
    return -1;
}

int bstack_process_gen(bstack_t *bs,
                       int (*gen)(bstack_gen_op_t *out_op, void *ctx),
                       void *ctx)
{
    BS_WRLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;
    uint64_t data_size = raw_size - HEADER_SIZE;

    /* Load locked under the write lock (see bstack_set for rationale). */
    uint64_t locked = ATOMIC_LOAD_ACQUIRE(&bs->locked);

    for (;;) {
        bstack_gen_op_t op;
        int r = gen(&op, ctx);
        if (r == 0) {
            BS_WRUNLOCK(bs);
            return 0;
        }
        if (r < 0)
            goto fail_unlock;

        switch (op.kind) {
        case BSTACK_GEN_READ: {
            uint64_t offset = op.u.read.offset;
            size_t   len    = op.u.read.len;
            if ((uint64_t)len > UINT64_MAX - offset) {
                BS_WRUNLOCK(bs); errno = EINVAL; return -1;
            }
            uint64_t end = offset + (uint64_t)len;
            if (end > data_size) {
                BS_WRUNLOCK(bs); errno = EINVAL; return -1;
            }
            if (len > 0) {
                /* Fast path: locked bytes are immutable, so they can be
                 * served from the cache instead of a pread — mirroring how
                 * bstack_get treats reads of the locked region. */
                if (end <= locked && bs->cache_enabled) {
                    CACHE_LOCK(bs);
                    memcpy(op.u.read.buf, bs->cache_buf + offset, len);
                    CACHE_UNLOCK(bs);
                } else if (plat_pread(bs->fd, op.u.read.buf, len,
                                       HEADER_SIZE + offset) != 0) {
                    goto fail_unlock;
                }
            }
            break;
        }
        case BSTACK_GEN_WRITE: {
            uint64_t offset = op.u.write.offset;
            size_t   len    = op.u.write.len;
            if ((uint64_t)len > UINT64_MAX - offset) {
                BS_WRUNLOCK(bs); errno = EINVAL; return -1;
            }
            uint64_t end = offset + (uint64_t)len;
            if (offset < locked) {
                BS_WRUNLOCK(bs); errno = EINVAL; return -1;
            }
            if (end > data_size) {
                BS_WRUNLOCK(bs); errno = EINVAL; return -1;
            }
            if (len > 0) {
                if (set_in_place(bs->fd, data_size, offset,
                                 op.u.write.data, len) != 0)
                    goto fail_unlock;
            }
            BS_WRUNLOCK(bs);
            return 0;
        }
        case BSTACK_GEN_SWAP: {
            uint64_t a_offset = op.u.swap.a_offset;
            uint64_t b_offset = op.u.swap.b_offset;
            uint64_t len      = op.u.swap.len;
            if (len > UINT64_MAX - a_offset || len > UINT64_MAX - b_offset) {
                BS_WRUNLOCK(bs); errno = EINVAL; return -1;
            }
            uint64_t a_end = a_offset + len;
            uint64_t b_end = b_offset + len;

            if (len > 0) {
                uint64_t lo = a_offset < b_offset ? a_offset : b_offset;
                uint64_t hi = a_offset < b_offset ? b_offset : a_offset;
                if (lo + len > hi) {
                    BS_WRUNLOCK(bs); errno = EINVAL; return -1;
                }
            }
            if (a_offset < locked || b_offset < locked) {
                BS_WRUNLOCK(bs); errno = EINVAL; return -1;
            }
            if (a_end > data_size || b_end > data_size) {
                BS_WRUNLOCK(bs); errno = EINVAL; return -1;
            }

            if (len > 0) {
                /* Crash-atomic exchange journal (as bstack_cross_exchange). */
                if (journaled_exchange(bs->fd, data_size,
                                       a_offset, b_offset, len) != 0)
                    goto fail_unlock;
            }
            BS_WRUNLOCK(bs);
            return 0;
        }
        case BSTACK_GEN_PUSH: {
            const uint8_t *data = op.u.push.data;
            size_t   len  = op.u.push.len;
            if (len > 0) {
                uint64_t raw_size_now = HEADER_SIZE + data_size;
                if (plat_pwrite(bs->fd, data, len, raw_size_now) != 0) {
                    plat_ftruncate(bs->fd, raw_size_now);
                    goto fail_unlock;
                }
                uint64_t new_len = data_size + (uint64_t)len;
                if (write_committed_len(bs->fd, &bs->clen, new_len) != 0 ||
                    plat_durable_sync(bs->fd) != 0)
                {
                    /* Rollback: remove written data and reset committed
                     * length. The cache is reset up front so it reflects
                     * the rolled-back file even if the header rewrite below
                     * fails. */
                    plat_ftruncate(bs->fd, raw_size_now);
                    bs->clen = data_size;
                    write_committed_len(bs->fd, &bs->clen, data_size);
                    plat_durable_sync(bs->fd);
                    goto fail_unlock;
                }
            }
            BS_WRUNLOCK(bs);
            return 0;
        }
        case BSTACK_GEN_POP: {
            uint8_t *buf = op.u.pop.buf;
            size_t   len = op.u.pop.len;
            if ((uint64_t)len > data_size) {
                BS_WRUNLOCK(bs); errno = EINVAL; return -1;
            }
            uint64_t new_len = data_size - (uint64_t)len;
            if (new_len < locked) {
                BS_WRUNLOCK(bs); errno = EINVAL; return -1;
            }
            if (len > 0) {
                /* buf == NULL discards the bytes without copying them out —
                 * the in-sequence equivalent of bstack_discard. */
                if (buf != NULL) {
                    uint64_t read_offset = HEADER_SIZE + new_len;
                    if (plat_pread(bs->fd, buf, len, read_offset) != 0)
                        goto fail_unlock;
                }
                if (plat_ftruncate(bs->fd, HEADER_SIZE + new_len) != 0)
                    goto fail_unlock;
                /* The truncation is the commit point: the tail bytes are
                 * gone and recovery would adopt the smaller file size, so
                 * update the cache now — before the header write, which can
                 * fail and skip it. */
                bs->clen = new_len;
                if (write_committed_len(bs->fd, &bs->clen, new_len) != 0 ||
                    plat_durable_sync(bs->fd) != 0)
                    goto fail_unlock;
            }
            BS_WRUNLOCK(bs);
            return 0;
        }
        case BSTACK_GEN_SPLICE: {
            /* Cut op.u.splice.n bytes off the tail then append new_buf. When
             * removed != NULL the popped bytes are read into it first (the
             * splice form); removed == NULL discards them (the atrunc form). */
            uint8_t       *removed = op.u.splice.removed;
            size_t         n       = op.u.splice.n;
            const uint8_t *new_buf = op.u.splice.new_buf;
            size_t         new_len = op.u.splice.new_len;
            if ((uint64_t)n > data_size) {
                BS_WRUNLOCK(bs); errno = EINVAL; return -1;
            }
            uint64_t new_tail_start = data_size - (uint64_t)n;
            if (new_tail_start < locked) {
                BS_WRUNLOCK(bs); errno = EINVAL; return -1;
            }
            if (n != 0 || new_len != 0) {
                if (n > 0 && removed != NULL) {
                    if (plat_pread(bs->fd, removed, n,
                                   HEADER_SIZE + new_tail_start) != 0)
                        goto fail_unlock;
                }
                if (commit_tail_replace(bs->fd, &bs->clen, new_tail_start,
                                        (uint64_t)n, new_buf, (uint64_t)new_len,
                                        HEADER_SIZE + data_size) != 0)
                    goto fail_unlock;
            }
            BS_WRUNLOCK(bs);
            return 0;
        }
        case BSTACK_GEN_SPARSE: {
            const bstack_iovec_t *writes = op.u.sparse.writes;
            size_t   count  = op.u.sparse.count;
            uint64_t length = op.u.sparse.length;

            /* Copy the borrowed blocks into a local array to compact, sort, and
             * validate them against length. */
            bstack_iovec_t *w = NULL;
            size_t n = 0;
            if (count != 0) {
                if (writes == NULL) {
                    BS_WRUNLOCK(bs); errno = EINVAL; return -1;
                }
                if (count > SIZE_MAX / sizeof(bstack_iovec_t)) {
                    BS_WRUNLOCK(bs); errno = EINVAL; return -1;
                }
                w = malloc(count * sizeof(bstack_iovec_t));
                if (!w)
                    goto fail_unlock;
                memcpy(w, writes, count * sizeof(bstack_iovec_t));
                if (validate_sparse_blocks(w, count, length, &n) != 0) {
                    int saved = errno;
                    free(w);
                    BS_WRUNLOCK(bs); errno = saved; return -1;
                }
            }
            if (length != 0) {
                if (length > UINT64_MAX - data_size) {
                    free(w);
                    BS_WRUNLOCK(bs); errno = EINVAL; return -1;
                }
                uint64_t new_len = data_size + length;
                if (commit_sparse_extend(bs, data_size, raw_size, new_len,
                                         w, n) != 0) {
                    int saved = errno;
                    free(w);
                    errno = saved;
                    goto fail_unlock;
                }
            }
            free(w);
            BS_WRUNLOCK(bs);
            return 0;
        }
        case BSTACK_GEN_LEN: {
            *op.u.len.out = data_size;
            break;
        }
        case BSTACK_GEN_ABORT: {
            /* Nothing has been mutated: every mutating kind returns out of this
             * loop, so reaching here means only reads have run.  A zero status
             * ends the sequence successfully. */
            int sv = op.u.abort.status;
            BS_WRUNLOCK(bs);
            if (sv == 0)
                return 0;
            errno = sv;
            return -1;
        }
        default:
            BS_WRUNLOCK(bs);
            errno = EINVAL;
            return -1;
        }
    }

fail_unlock:
    { int sv = errno; BS_WRUNLOCK(bs); errno = sv; }
    return -1;
}

int bstack_set_batched(bstack_t *bs, const bstack_iovec_t *writes, size_t count)
{
    if (writes == NULL || count == 0)
        return 0;

    /* Materialise the non-empty writes into a working array we can sort
     * (empty writes touch nothing and are dropped). */
    if (count > SIZE_MAX / sizeof(bstack_iovec_t)) { errno = EINVAL; return -1; }
    bstack_iovec_t *w = malloc(count * sizeof(bstack_iovec_t));
    if (!w) return -1;
    size_t n = 0;
    for (size_t i = 0; i < count; i++) {
        if (writes[i].len != 0)
            w[n++] = writes[i];
    }
    if (n == 0) { free(w); return 0; }

    BS_WRLOCK(bs);

    /* Load locked under the write lock (see bstack_set for rationale). */
    uint64_t locked = ATOMIC_LOAD_ACQUIRE(&bs->locked);
    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail;
    uint64_t data_size = raw_size - HEADER_SIZE;

    /* Validate each block against the payload size and the locked prefix. */
    for (size_t i = 0; i < n; i++) {
        if ((uint64_t)w[i].len > UINT64_MAX - w[i].offset) goto fail_einval;
        uint64_t end = w[i].offset + (uint64_t)w[i].len;
        if (w[i].offset < locked)  goto fail_einval;
        if (end > data_size)       goto fail_einval;
    }

    /* A lone write cannot overlap anything and is already atomic on its own, so
     * skip the overlap scan and the multi-write journal. */
    if (n == 1) {
        if (set_in_place(bs->fd, data_size, w[0].offset, w[0].buf, w[0].len) != 0)
            goto fail;
        BS_WRUNLOCK(bs);
        free(w);
        return 0;
    }

    /* Reject overlap: sort by offset, then check each block ends at or before
     * the next one begins. */
    qsort(w, n, sizeof(bstack_iovec_t), cmp_iovec_offset);
    for (size_t i = 0; i + 1 < n; i++) {
        uint64_t a_end = w[i].offset + (uint64_t)w[i].len;
        if (a_end > w[i + 1].offset) goto fail_einval;
    }

    if (journaled_multi_set(bs->fd, data_size, w, n) != 0)
        goto fail;

    BS_WRUNLOCK(bs);
    free(w);
    return 0;

fail_einval:
    BS_WRUNLOCK(bs);
    free(w);
    errno = EINVAL;
    return -1;
fail:
    { int sv = errno; BS_WRUNLOCK(bs); errno = sv; }
    free(w);
    return -1;
}

/* ---- inplace_gen overlay ------------------------------------------------
 * The pending edits are kept as a sorted, pairwise-non-overlapping array of
 * bstack_iovec_t (offset, buf = borrowed caller data, len). Because it is sorted
 * by start and non-overlapping, the ends are sorted too, so the edits touching
 * any range form one contiguous run that binary search can locate. */

/* First index i with arr[i].offset + arr[i].len > off (== n if none). */
static size_t overlay_lower(const bstack_iovec_t *arr, size_t n, uint64_t off)
{
    size_t lo = 0, hi = n;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (arr[mid].offset + (uint64_t)arr[mid].len <= off) lo = mid + 1;
        else hi = mid;
    }
    return lo;
}

/* First index i with arr[i].offset >= end (== n if none). */
static size_t overlay_upper(const bstack_iovec_t *arr, size_t n, uint64_t end)
{
    size_t lo = 0, hi = n;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (arr[mid].offset < end) lo = mid + 1;
        else hi = mid;
    }
    return lo;
}

/* Insert the edit [off, off+len) into the overlay, resolving overlap in favour
 * of the newer write. The touched run [lo, hi) is replaced by at most three
 * entries: the surviving prefix of the run's first edit, the new edit, and the
 * surviving suffix of the run's last edit (all containment cases fall out of
 * this). Returns 0, or -1 on allocation failure. */
static int overlay_insert(bstack_iovec_t **parr, size_t *pcount, size_t *pcap,
                          uint64_t off, uint8_t *data, size_t len)
{
    bstack_iovec_t *arr = *parr;
    size_t count = *pcount;
    uint64_t end = off + (uint64_t)len;
    size_t lo = overlay_lower(arr, count, off);
    size_t hi = overlay_upper(arr, count, end);

    /* Build the replacement (value copies; buf points to caller data, never into
     * arr, so a realloc below cannot invalidate them). */
    bstack_iovec_t repl[3];
    size_t rn = 0;
    if (lo < hi && arr[lo].offset < off) {
        repl[rn].offset = arr[lo].offset;
        repl[rn].buf    = arr[lo].buf;
        repl[rn].len    = (size_t)(off - arr[lo].offset);
        rn++;
    }
    repl[rn].offset = off; repl[rn].buf = data; repl[rn].len = len; rn++;
    if (lo < hi) {
        bstack_iovec_t last = arr[hi - 1];
        uint64_t last_end = last.offset + (uint64_t)last.len;
        if (last_end > end) {
            repl[rn].offset = end;
            repl[rn].buf    = last.buf + (size_t)(end - last.offset);
            repl[rn].len    = (size_t)(last_end - end);
            rn++;
        }
    }

    size_t new_count = count - (hi - lo) + rn;
    if (new_count > *pcap) {
        size_t newcap = (*pcap == 0) ? 4 : *pcap;
        while (newcap < new_count) newcap *= 2;
        bstack_iovec_t *na = realloc(arr, newcap * sizeof(bstack_iovec_t));
        if (!na) return -1;
        arr = na; *parr = na; *pcap = newcap;
    }
    /* Splice arr[lo..hi) -> repl[0..rn): shift the tail, then drop in repl. */
    memmove(arr + lo + rn, arr + hi, (count - hi) * sizeof(bstack_iovec_t));
    memcpy(arr + lo, repl, rn * sizeof(bstack_iovec_t));
    *pcount = new_count;
    return 0;
}

/* Fill buf with the batch-so-far content for [offset, offset+len): the overlay
 * edits overlaid on committed bytes. The disk read is skipped when the edits
 * already cover the whole range. Returns 0, or -1 on I/O error (errno set). */
static int overlay_read(bstack_fd_t fd, const bstack_iovec_t *arr, size_t count,
                        uint64_t offset, uint8_t *buf, size_t len)
{
    uint64_t end = offset + (uint64_t)len;
    size_t start = overlay_lower(arr, count, offset);
    /* First pass: find the run end and whether any committed bytes show through. */
    size_t run_end = start;
    uint64_t covered_to = offset;
    int has_gap = 0;
    for (size_t i = start; i < count; i++) {
        if (arr[i].offset >= end) break;
        if (arr[i].offset > covered_to) has_gap = 1;
        covered_to = arr[i].offset + (uint64_t)arr[i].len;
        run_end = i + 1;
    }
    if (covered_to < end) has_gap = 1;
    /* Only touch the disk when some committed bytes are actually visible. */
    if (has_gap) {
        if (plat_pread(fd, buf, len, HEADER_SIZE + offset) != 0)
            return -1;
    }
    /* Second pass: overlay each edit in the run (fills buf completely when there
     * was no gap, so the skipped read left nothing uninitialised). */
    for (size_t i = start; i < run_end; i++) {
        uint64_t s = arr[i].offset;
        uint64_t e = s + (uint64_t)arr[i].len;
        uint64_t l = (s > offset) ? s : offset;
        uint64_t h = (e < end)    ? e : end;
        memcpy(buf + (size_t)(l - offset), arr[i].buf + (size_t)(l - s),
               (size_t)(h - l));
    }
    return 0;
}

int bstack_inplace_gen(bstack_t *bs,
                       int (*gen)(bstack_gen_op_t *out_op, void *ctx),
                       void *ctx, int *prev_status)
{
    BS_WRLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;
    uint64_t data_size = raw_size - HEADER_SIZE;
    uint64_t locked = ATOMIC_LOAD_ACQUIRE(&bs->locked);

    /* Sorted, pairwise-non-overlapping pending edits, each borrowing the
     * caller's Write data for the duration of the call. */
    bstack_iovec_t *overlay = NULL;
    size_t ov_count = 0, ov_cap = 0;
    int prev = 0; /* previous op's status: 0 = ok, else errno */

    for (;;) {
        if (prev_status) *prev_status = prev;
        bstack_gen_op_t op;
        int r = gen(&op, ctx);
        if (r == 0)
            break; /* commit the accumulated edits */

        switch (op.kind) {
        case BSTACK_GEN_READ: {
            uint64_t offset = op.u.read.offset;
            size_t   len    = op.u.read.len;
            if ((uint64_t)len > UINT64_MAX - offset ||
                offset + (uint64_t)len > data_size) {
                prev = EINVAL;
                break;
            }
            if (len > 0) {
                if (overlay_read(bs->fd, overlay, ov_count, offset,
                                 op.u.read.buf, len) != 0) {
                    prev = errno ? errno : EIO;
                    break;
                }
            }
            prev = 0;
            break;
        }
        case BSTACK_GEN_WRITE: {
            uint64_t offset = op.u.write.offset;
            size_t   len    = op.u.write.len;
            if (len == 0) { prev = 0; break; } /* no-op, nothing recorded */
            if ((uint64_t)len > UINT64_MAX - offset ||
                offset < locked ||
                offset + (uint64_t)len > data_size) {
                prev = EINVAL;
                break;
            }
            /* op.u.write.data is const; the overlay only reads it and hands it
             * back to the journal as a source, so the cast is sound. */
            if (overlay_insert(&overlay, &ov_count, &ov_cap, offset,
                               (uint8_t *)(uintptr_t)op.u.write.data, len) != 0) {
                prev = errno ? errno : ENOMEM;
                goto fail_free; /* allocation failure is fatal */
            }
            prev = 0;
            break;
        }
        case BSTACK_GEN_LEN: {
            *op.u.len.out = data_size;
            prev = 0;
            break;
        }
        case BSTACK_GEN_ABORT: {
            /* Discard the overlay without committing: the pending writes only
             * ever existed in memory, so the file is untouched.  A zero status
             * ends the sequence successfully, still committing nothing. */
            if (op.u.abort.status == 0) {
                BS_WRUNLOCK(bs);
                free(overlay);
                return 0;
            }
            errno = op.u.abort.status;
            goto fail_free;
        }
        default:
            /* SWAP / PUSH / POP / SPLICE / SPARSE and any unknown kind are not
             * permitted (in-place Read/Write/Len only); report and continue. */
            prev = EINVAL;
            break;
        }
    }

    /* Commit the accumulated edits: 0 -> nothing; 1 -> the ordinary single-write
     * path; many -> the multi-write journal. */
    {
        int rc = 0;
        if (ov_count == 1) {
            rc = set_in_place(bs->fd, data_size, overlay[0].offset,
                              overlay[0].buf, overlay[0].len);
        } else if (ov_count > 1) {
            rc = journaled_multi_set(bs->fd, data_size, overlay, ov_count);
        }
        if (rc != 0)
            goto fail_free;
    }
    BS_WRUNLOCK(bs);
    free(overlay);
    return 0;

fail_free:
    { int sv = errno; BS_WRUNLOCK(bs); free(overlay); errno = sv; }
    return -1;
fail_unlock:
    { int sv = errno; BS_WRUNLOCK(bs); errno = sv; }
    return -1;
}

int bstack_cross_exchange(bstack_t *bs, uint64_t a, uint64_t b, uint64_t n)
{
    if (n > UINT64_MAX - a) { errno = EINVAL; return -1; }
    if (n > UINT64_MAX - b) { errno = EINVAL; return -1; }
    uint64_t a_end = a + n;
    uint64_t b_end = b + n;

    /* Overlap check (only meaningful when n > 0). */
    if (n > 0) {
        uint64_t lo = a < b ? a : b;
        uint64_t hi = a < b ? b : a;
        if (lo + n > hi) {
            errno = EINVAL;
            return -1;
        }
    }

    BS_WRLOCK(bs);

    uint64_t locked = ATOMIC_LOAD_ACQUIRE(&bs->locked);
    if (a < locked) { BS_WRUNLOCK(bs); errno = EINVAL; return -1; }
    if (b < locked) { BS_WRUNLOCK(bs); errno = EINVAL; return -1; }

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;
    if (a_end > data_size) { BS_WRUNLOCK(bs); errno = EINVAL; return -1; }
    if (b_end > data_size) { BS_WRUNLOCK(bs); errno = EINVAL; return -1; }

    if (n == 0) {
        BS_WRUNLOCK(bs);
        return 0;
    }

    /* Crash-atomic exchange through the write-in-progress journal (O(1)
     * memory): stage A's bytes, then commit at a single wip_ptr flip A -> B. */
    if (journaled_exchange(bs->fd, data_size, a, b, n) != 0)
        goto fail_unlock;

    BS_WRUNLOCK(bs);
    return 0;

fail_unlock:
    { int sv = errno; BS_WRUNLOCK(bs); errno = sv; }
    return -1;
}

int bstack_copy(bstack_t *bs, uint64_t from, uint64_t to, uint64_t n)
{
    if (n > UINT64_MAX - from) { errno = EINVAL; return -1; }
    if (n > UINT64_MAX - to)   { errno = EINVAL; return -1; }
    uint64_t from_end = from + n;
    uint64_t to_end   = to   + n;

    BS_WRLOCK(bs);

    uint64_t locked = ATOMIC_LOAD_ACQUIRE(&bs->locked);
    if (to < locked) { BS_WRUNLOCK(bs); errno = EINVAL; return -1; }

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;
    if (from_end > data_size) { BS_WRUNLOCK(bs); errno = EINVAL; return -1; }
    if (to_end   > data_size) { BS_WRUNLOCK(bs); errno = EINVAL; return -1; }

    if (n == 0) {
        BS_WRUNLOCK(bs);
        return 0;
    }
    /* A copy onto its own location leaves every byte unchanged. */
    if (from == to) {
        BS_WRUNLOCK(bs);
        return 0;
    }

    /* Write-strategy hierarchy (see algos/WIP.md):
     *  - destination within one aligned block -> single-block atomic write;
     *  - overlapping source/destination       -> route through the tail backup
     *    (journaled_move), so a replay never reads clobbered source bytes;
     *  - disjoint source/destination          -> copy journal (journaled_copy),
     *    staging only the source coordinate, O(1) staging. */
    if (is_atomic_write(to, n)) {
#if UINT64_MAX > SIZE_MAX
        if (n > (uint64_t)SIZE_MAX) { BS_WRUNLOCK(bs); errno = EINVAL; return -1; }
#endif
        uint8_t *buf = (uint8_t *)malloc((size_t)n);
        if (!buf) goto fail_unlock;
        if (plat_pread(bs->fd,  buf, (size_t)n, HEADER_SIZE + from) != 0 ||
            plat_pwrite(bs->fd, buf, (size_t)n, HEADER_SIZE + to)   != 0 ||
            plat_durable_sync(bs->fd) != 0)
        {
            free(buf);
            goto fail_unlock;
        }
        free(buf);
    } else if (from < to_end && to < from_end) {
        if (journaled_move(bs->fd, data_size, from, to, n) != 0)
            goto fail_unlock;
    } else {
        if (journaled_copy(bs->fd, data_size, from, to, n) != 0)
            goto fail_unlock;
    }

    BS_WRUNLOCK(bs);
    return 0;

fail_unlock:
    { int sv = errno; BS_WRUNLOCK(bs); errno = sv; }
    return -1;
}

/* Shared helper: compare a_len bytes at a_offset against a_expected using a
 * 256-byte stack buffer.  Returns 1 if all bytes match, 0 if any differ, -1
 * on I/O error (errno set).  Caller holds the write lock. */
static int crds_compare_a(bstack_fd_t fd,
                           uint64_t a_offset, const uint8_t *a_expected,
                           size_t a_len)
{
    if (a_len == 0)
        return 1;
    uint8_t chunk[256];
    size_t remaining = a_len;
    uint64_t file_off = HEADER_SIZE + a_offset;
    const uint8_t *exp = a_expected;
    while (remaining > 0) {
        size_t batch = remaining < sizeof chunk ? remaining : sizeof chunk;
        if (plat_pread(fd, chunk, batch, file_off) != 0)
            return -1;
        if (memcmp(chunk, exp, batch) != 0)
            return 0;
        exp      += batch;
        file_off += batch;
        remaining -= batch;
    }
    return 1;
}

/* Shared helper: like crds_compare_a but applies a bitwise mask to both sides
 * before comparing: (A[i] & mask[i]) == (expected[i] & mask[i]).
 * Returns 1 if all masked bytes match, 0 if any differ, -1 on I/O error. */
static int crds_compare_a_masked(bstack_fd_t fd,
                                  uint64_t a_offset, const uint8_t *mask,
                                  const uint8_t *a_expected, size_t a_len)
{
    if (a_len == 0)
        return 1;
    uint8_t chunk[256];
    size_t remaining = a_len;
    uint64_t file_off = HEADER_SIZE + a_offset;
    const uint8_t *exp = a_expected;
    const uint8_t *msk = mask;
    while (remaining > 0) {
        size_t batch = remaining < sizeof chunk ? remaining : sizeof chunk;
        if (plat_pread(fd, chunk, batch, file_off) != 0)
            return -1;
        for (size_t j = 0; j < batch; j++) {
            if ((chunk[j] & msk[j]) != (exp[j] & msk[j]))
                return 0;
        }
        exp      += batch;
        msk      += batch;
        file_off += batch;
        remaining -= batch;
    }
    return 1;
}

/* Shared body for CRDS functions after the comparison result is known.
 * When matched == 1, reads b_len bytes at b_offset into b_old_buf, writes
 * b_new_buf, and syncs.  Caller holds the write lock.
 * Returns 0 on success, -1 on I/O error. */
static int crds_do_swap(bstack_fd_t fd, uint64_t data_size,
                        uint64_t b_offset, uint8_t *b_old_buf,
                        const uint8_t *b_new_buf, size_t b_len)
{
    if (b_len == 0)
        return 0;
    if (plat_pread(fd, b_old_buf, b_len, HEADER_SIZE + b_offset) != 0)
        return -1;
    return set_in_place(fd, data_size, b_offset, b_new_buf, b_len);
}

/* Common setup for all CRDS operations: validates bounds and locked region,
 * computes a_end / b_end, and acquires the write lock.
 * Returns -1 (errno set) on validation failure; lock is NOT held on -1.
 * On success the write lock is held and data_size is filled. */
static int crds_setup(bstack_t *bs,
                      uint64_t a_offset, size_t a_len,
                      uint64_t b_offset, size_t b_len,
                      uint64_t *out_a_end, uint64_t *out_b_end,
                      uint64_t *out_data_size)
{
    if ((uint64_t)a_len > UINT64_MAX - a_offset ||
        (uint64_t)b_len > UINT64_MAX - b_offset) {
        errno = EINVAL;
        return -1;
    }
    *out_a_end = a_offset + (uint64_t)a_len;
    *out_b_end = b_offset + (uint64_t)b_len;

    BS_WRLOCK(bs);

    uint64_t locked = ATOMIC_LOAD_ACQUIRE(&bs->locked);
    if (b_len > 0 && b_offset < locked) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0) {
        int sv = errno;
        BS_WRUNLOCK(bs);
        errno = sv;
        return -1;
    }
    *out_data_size = raw_size - HEADER_SIZE;

    if (a_len > 0 && *out_a_end > *out_data_size) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }
    if (b_len > 0 && *out_b_end > *out_data_size) {
        BS_WRUNLOCK(bs);
        errno = EINVAL;
        return -1;
    }
    return 0;
}

int bstack_eq_crds(bstack_t *bs,
                   uint64_t a_offset, const uint8_t *a_expected, size_t a_len,
                   uint64_t b_offset, uint8_t *b_old_buf,
                   const uint8_t *b_new_buf, size_t b_len,
                   int *ok)
{
    uint64_t a_end, b_end, data_size;
    if (crds_setup(bs, a_offset, a_len, b_offset, b_len,
                   &a_end, &b_end, &data_size) != 0)
        return -1;
    (void)a_end; (void)b_end;

    int cmp = crds_compare_a(bs->fd, a_offset, a_expected, a_len);
    if (cmp < 0)  goto fail_unlock;
    if (cmp == 0) { BS_WRUNLOCK(bs); if (ok) *ok = 0; return 0; }

    if (crds_do_swap(bs->fd, data_size, b_offset, b_old_buf, b_new_buf, b_len) != 0)
        goto fail_unlock;

    BS_WRUNLOCK(bs);
    if (ok) *ok = 1;
    return 0;

fail_unlock:
    { int sv = errno; BS_WRUNLOCK(bs); errno = sv; }
    return -1;
}

int bstack_ne_crds(bstack_t *bs,
                   uint64_t a_offset, const uint8_t *a_expected, size_t a_len,
                   uint64_t b_offset, uint8_t *b_old_buf,
                   const uint8_t *b_new_buf, size_t b_len,
                   int *ok)
{
    uint64_t a_end, b_end, data_size;
    if (crds_setup(bs, a_offset, a_len, b_offset, b_len,
                   &a_end, &b_end, &data_size) != 0)
        return -1;
    (void)a_end; (void)b_end;

    int cmp = crds_compare_a(bs->fd, a_offset, a_expected, a_len);
    if (cmp < 0)  goto fail_unlock;
    if (cmp == 1) { BS_WRUNLOCK(bs); if (ok) *ok = 0; return 0; }

    if (crds_do_swap(bs->fd, data_size, b_offset, b_old_buf, b_new_buf, b_len) != 0)
        goto fail_unlock;

    BS_WRUNLOCK(bs);
    if (ok) *ok = 1;
    return 0;

fail_unlock:
    { int sv = errno; BS_WRUNLOCK(bs); errno = sv; }
    return -1;
}

int bstack_masked_eq_crds(bstack_t *bs,
                          uint64_t a_offset, const uint8_t *mask,
                          const uint8_t *a_expected, size_t a_len,
                          uint64_t b_offset, uint8_t *b_old_buf,
                          const uint8_t *b_new_buf, size_t b_len,
                          int *ok)
{
    uint64_t a_end, b_end, data_size;
    if (crds_setup(bs, a_offset, a_len, b_offset, b_len,
                   &a_end, &b_end, &data_size) != 0)
        return -1;
    (void)a_end; (void)b_end;

    int cmp = crds_compare_a_masked(bs->fd, a_offset, mask, a_expected, a_len);
    if (cmp < 0)  goto fail_unlock;
    if (cmp == 0) { BS_WRUNLOCK(bs); if (ok) *ok = 0; return 0; }

    if (crds_do_swap(bs->fd, data_size, b_offset, b_old_buf, b_new_buf, b_len) != 0)
        goto fail_unlock;

    BS_WRUNLOCK(bs);
    if (ok) *ok = 1;
    return 0;

fail_unlock:
    { int sv = errno; BS_WRUNLOCK(bs); errno = sv; }
    return -1;
}

int bstack_masked_ne_crds(bstack_t *bs,
                          uint64_t a_offset, const uint8_t *mask,
                          const uint8_t *a_expected, size_t a_len,
                          uint64_t b_offset, uint8_t *b_old_buf,
                          const uint8_t *b_new_buf, size_t b_len,
                          int *ok)
{
    uint64_t a_end, b_end, data_size;
    if (crds_setup(bs, a_offset, a_len, b_offset, b_len,
                   &a_end, &b_end, &data_size) != 0)
        return -1;
    (void)a_end; (void)b_end;

    int cmp = crds_compare_a_masked(bs->fd, a_offset, mask, a_expected, a_len);
    if (cmp < 0)  goto fail_unlock;
    if (cmp == 1) { BS_WRUNLOCK(bs); if (ok) *ok = 0; return 0; }

    if (crds_do_swap(bs->fd, data_size, b_offset, b_old_buf, b_new_buf, b_len) != 0)
        goto fail_unlock;

    BS_WRUNLOCK(bs);
    if (ok) *ok = 1;
    return 0;

fail_unlock:
    { int sv = errno; BS_WRUNLOCK(bs); errno = sv; }
    return -1;
}

#endif /* BSTACK_FEATURE_ATOMIC && BSTACK_FEATURE_SET */

#ifdef __cplusplus
}
#endif
