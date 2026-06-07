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

static const uint8_t  MAGIC[8]        = {'B','S','T','K', 0, 1, 14, 0};
static const uint8_t  MAGIC_PREFIX[6] = {'B','S','T','K', 0, 1};
static const uint64_t HEADER_SIZE     = 16;

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

static int plat_durable_sync(bstack_fd_t h)
{
    if (!FlushFileBuffers(h)) { win_set_errno(); return -1; }
    return 0;
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

static int plat_durable_sync(bstack_fd_t fd)
{
#  ifdef __APPLE__
    if (fcntl(fd, F_FULLFSYNC) == 0)
        return 0;
    /* Device does not support F_FULLFSYNC — fall back to fdatasync. */
#  endif
    return fdatasync(fd);
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

static int write_committed_len(bstack_fd_t fd, uint64_t len)
{
    return write_le64(fd, 8, len);
}

static int init_header(bstack_fd_t fd)
{
    uint8_t hdr[16];
    memcpy(hdr, MAGIC, 8);
    memset(hdr + 8, 0, 8);
    return plat_pwrite(fd, hdr, 16, 0);
}

/* Validates magic prefix and returns committed payload length via *out_clen.
 * Sets errno = EINVAL on bad magic or short header read. */
static int read_header(bstack_fd_t fd, uint64_t *out_clen)
{
    uint8_t hdr[16];
    if (plat_pread(fd, hdr, 16, 0) != 0) {
        errno = EINVAL;
        return -1;
    }
    if (memcmp(hdr, MAGIC_PREFIX, 6) != 0) {
        errno = EINVAL;
        return -1;
    }
    uint64_t clen = 0;
    for (int i = 0; i < 8; i++)
        clen |= (uint64_t)hdr[8 + i] << (8 * i);
    *out_clen = clen;
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
        uint64_t clen;
        if (read_header(fd, &clen) != 0) {
            int saved = errno;
            close_fd(fd);
            errno = saved;
            return NULL;
        }

        uint64_t actual = raw_size - HEADER_SIZE;
        if (actual != clen) {
            uint64_t correct = (clen < actual) ? clen : actual;
            if (plat_ftruncate(fd, HEADER_SIZE + correct) != 0 ||
                write_committed_len(fd, correct) != 0 ||
                plat_durable_sync(fd) != 0)
            {
                int saved = errno;
                close_fd(fd);
                errno = saved;
                return NULL;
            }
        }
    }

    bstack_t *bs = malloc(sizeof(bstack_t));
    if (!bs) {
        close_fd(fd);
        return NULL;
    }
    bs->fd            = fd;
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
    if (write_committed_len(bs->fd, new_len) != 0 ||
        plat_durable_sync(bs->fd) != 0)
    {
        /* Rollback: remove written data and reset committed length. */
        plat_ftruncate(bs->fd, raw_size);
        write_committed_len(bs->fd, logical_offset);
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
    if (write_committed_len(bs->fd, new_len) != 0 ||
        plat_durable_sync(bs->fd) != 0)
    {
        /* Rollback: truncate and reset committed length. */
        plat_ftruncate(bs->fd, raw_size);
        write_committed_len(bs->fd, logical_offset);
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

    if (plat_ftruncate(bs->fd, HEADER_SIZE + new_len) != 0 ||
        write_committed_len(bs->fd, new_len) != 0 ||
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

    if (plat_ftruncate(bs->fd, HEADER_SIZE + new_len) != 0 ||
        write_committed_len(bs->fd, new_len) != 0 ||
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

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0) {
        int saved = errno;
        BS_RDUNLOCK(bs);
        errno = saved;
        return -1;
    }

    BS_RDUNLOCK(bs);
    *out_len = raw_size - HEADER_SIZE;
    return 0;
}

/* -------------------------------------------------------------------------
 * bstack_is_empty
 * ---------------------------------------------------------------------- */

int bstack_is_empty(bstack_t *bs, int *out_empty)
{
    BS_RDLOCK(bs);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0) {
        int saved = errno;
        BS_RDUNLOCK(bs);
        errno = saved;
        return -1;
    }

    BS_RDUNLOCK(bs);
    *out_empty = (raw_size == HEADER_SIZE) ? 1 : 0;
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

    if (plat_pwrite(bs->fd, data, len, HEADER_SIZE + offset) != 0)
        goto fail_unlock;

    if (plat_durable_sync(bs->fd) != 0)
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
    if (n == 0)
        return 0;

    /* Guard against offset + n wrapping around. */
    if ((uint64_t)n > UINT64_MAX - offset) {
        errno = EINVAL;
        return -1;
    }
    uint64_t end = offset + (uint64_t)n;

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

    /* Allocate a buffer of zeros and write it. */
    uint8_t *zeros = calloc(n, 1);
    if (!zeros) {
        BS_WRUNLOCK(bs);
        errno = ENOMEM;
        return -1;
    }

    if (plat_pwrite(bs->fd, zeros, n, HEADER_SIZE + offset) != 0) {
        free(zeros);
        goto fail_unlock;
    }

    free(zeros);

    if (plat_durable_sync(bs->fd) != 0)
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

/* Shared body for atrunc and splice (after the removed bytes are read).
 * Caller already holds the write lock.  raw_size / data_size / tail_offset /
 * final_data_len are pre-computed by the caller. */
static int atomic_write_tail(bstack_fd_t fd,
                              uint64_t raw_size,
                              uint64_t tail_offset,
                              uint64_t final_data_len,
                              const uint8_t *buf, size_t buf_len,
                              size_t n)
{
    if (buf_len > n) {
        /* Net extension: extend first so crashes roll back cleanly, then
         * write buf over the old tail + the new space, sync, commit clen. */
        if (plat_ftruncate(fd, HEADER_SIZE + final_data_len) != 0)
            return -1;
        if (buf_len > 0 &&
            plat_pwrite(fd, buf, buf_len, tail_offset) != 0) {
            plat_ftruncate(fd, raw_size); /* best-effort rollback */
            return -1;
        }
        if (plat_durable_sync(fd) != 0) {
            plat_ftruncate(fd, raw_size); /* best-effort rollback */
            return -1;
        }
        return write_committed_len(fd, final_data_len);
    } else {
        /* Net truncation or same size: write buf into old tail, truncate,
         * sync, commit clen.  A crash after truncate is committed by
         * recovery (file_size - 16 < clen → clen = file_size - 16). */
        if (buf_len > 0 &&
            plat_pwrite(fd, buf, buf_len, tail_offset) != 0)
            return -1;
        if (plat_ftruncate(fd, HEADER_SIZE + final_data_len) != 0)
            return -1;
        if (plat_durable_sync(fd) != 0)
            return -1;
        return write_committed_len(fd, final_data_len);
    }
}

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

    uint64_t tail_offset    = HEADER_SIZE + new_tail_start;
    uint64_t final_data_len = new_tail_start + (uint64_t)buf_len;

    if (atomic_write_tail(bs->fd, raw_size, tail_offset,
                          final_data_len, buf, buf_len, n) != 0)
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

    uint64_t tail_offset    = HEADER_SIZE + new_tail_start;
    uint64_t final_data_len = new_tail_start + (uint64_t)new_len;

    /* Read removed bytes before any mutation. */
    if (n > 0 && removed != NULL) {
        if (plat_pread(bs->fd, removed, n, tail_offset) != 0)
            goto fail_unlock;
    }

    if (atomic_write_tail(bs->fd, raw_size, tail_offset,
                          final_data_len, new_buf, new_len, n) != 0)
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
    if (write_committed_len(bs->fd, new_len) != 0 ||
        plat_durable_sync(bs->fd) != 0) {
        plat_ftruncate(bs->fd, raw_size);
        write_committed_len(bs->fd, data_size);
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
    
    if (plat_ftruncate(bs->fd, HEADER_SIZE + new_len) != 0 ||
        write_committed_len(bs->fd, new_len) != 0 ||
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

    uint64_t final_data_len = new_tail_start + (uint64_t)new_len;

    if (atomic_write_tail(bs->fd, raw_size, tail_offset,
                          final_data_len, new_buf, new_len, n) != 0) {
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
        write_committed_len(bs->fd, new_len) != 0 ||
        plat_durable_sync(bs->fd) != 0)
    {
        /* Best-effort rollback. */
        plat_ftruncate(bs->fd, raw_size);
        write_committed_len(bs->fd, data_size);
        goto fail_unlock;
    }

    BS_WRUNLOCK(bs);
    if (ok) *ok = 1;
    return 0;

fail_unlock:
    { int sv = errno; BS_WRUNLOCK(bs); errno = sv; }
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

    if (plat_pread(bs->fd,  old_buf, len, HEADER_SIZE + offset) != 0)
        goto fail_unlock;
    if (plat_pwrite(bs->fd, new_buf, len, HEADER_SIZE + offset) != 0)
        goto fail_unlock;
    if (plat_durable_sync(bs->fd) != 0)
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

    /* All bytes matched — write new_buf and sync. */
    if (plat_pwrite(bs->fd, new_buf, len, HEADER_SIZE + offset) != 0)
        goto fail_unlock;
    if (plat_durable_sync(bs->fd) != 0)
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
        if (plat_pwrite(bs->fd, buf, (size_t)n, HEADER_SIZE + start) != 0) {
            free(buf);
            goto fail_unlock;
        }
        if (plat_durable_sync(bs->fd) != 0) {
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

#if UINT64_MAX > SIZE_MAX
    if (n > (uint64_t)SIZE_MAX) { BS_WRUNLOCK(bs); errno = EINVAL; return -1; }
#endif
    uint8_t *buf_a = (uint8_t *)malloc((size_t)n);
    if (!buf_a) goto fail_unlock;
    uint8_t *buf_b = (uint8_t *)malloc((size_t)n);
    if (!buf_b) { free(buf_a); goto fail_unlock; }

    if (plat_pread(bs->fd,  buf_a, (size_t)n, HEADER_SIZE + a) != 0 ||
        plat_pread(bs->fd,  buf_b, (size_t)n, HEADER_SIZE + b) != 0 ||
        plat_pwrite(bs->fd, buf_b, (size_t)n, HEADER_SIZE + a) != 0 ||
        plat_pwrite(bs->fd, buf_a, (size_t)n, HEADER_SIZE + b) != 0 ||
        plat_durable_sync(bs->fd) != 0)
    {
        free(buf_a);
        free(buf_b);
        goto fail_unlock;
    }
    free(buf_a);
    free(buf_b);

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
static int crds_do_swap(bstack_fd_t fd,
                        uint64_t b_offset, uint8_t *b_old_buf,
                        const uint8_t *b_new_buf, size_t b_len)
{
    if (b_len == 0)
        return 0;
    if (plat_pread(fd,  b_old_buf, b_len, HEADER_SIZE + b_offset) != 0)
        return -1;
    if (plat_pwrite(fd, b_new_buf, b_len, HEADER_SIZE + b_offset) != 0)
        return -1;
    return plat_durable_sync(fd);
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
    (void)a_end; (void)b_end; (void)data_size;

    int cmp = crds_compare_a(bs->fd, a_offset, a_expected, a_len);
    if (cmp < 0)  goto fail_unlock;
    if (cmp == 0) { BS_WRUNLOCK(bs); if (ok) *ok = 0; return 0; }

    if (crds_do_swap(bs->fd, b_offset, b_old_buf, b_new_buf, b_len) != 0)
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
    (void)a_end; (void)b_end; (void)data_size;

    int cmp = crds_compare_a(bs->fd, a_offset, a_expected, a_len);
    if (cmp < 0)  goto fail_unlock;
    if (cmp == 1) { BS_WRUNLOCK(bs); if (ok) *ok = 0; return 0; }

    if (crds_do_swap(bs->fd, b_offset, b_old_buf, b_new_buf, b_len) != 0)
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
    (void)a_end; (void)b_end; (void)data_size;

    int cmp = crds_compare_a_masked(bs->fd, a_offset, mask, a_expected, a_len);
    if (cmp < 0)  goto fail_unlock;
    if (cmp == 0) { BS_WRUNLOCK(bs); if (ok) *ok = 0; return 0; }

    if (crds_do_swap(bs->fd, b_offset, b_old_buf, b_new_buf, b_len) != 0)
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
    (void)a_end; (void)b_end; (void)data_size;

    int cmp = crds_compare_a_masked(bs->fd, a_offset, mask, a_expected, a_len);
    if (cmp < 0)  goto fail_unlock;
    if (cmp == 1) { BS_WRUNLOCK(bs); if (ok) *ok = 0; return 0; }

    if (crds_do_swap(bs->fd, b_offset, b_old_buf, b_new_buf, b_len) != 0)
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
