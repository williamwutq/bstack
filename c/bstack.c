/* Expose POSIX + BSD extensions on all supported platforms.
 * _DARWIN_C_SOURCE is defined unconditionally: on real macOS it overrides
 * _POSIX_C_SOURCE restrictions to keep fdatasync/flock visible; on Linux/glibc
 * it is ignored.  This also handles clang cross-compilation that falls back to
 * macOS SDK headers when no Linux sysroot is available. */
#define _DARWIN_C_SOURCE
#define _DEFAULT_SOURCE
#define _POSIX_C_SOURCE 200809L
#define _XOPEN_SOURCE 700

#include "bstack.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

/* -------------------------------------------------------------------------
 * Constants
 * ---------------------------------------------------------------------- */

static const uint8_t  MAGIC[8]        = {'B','S','T','K', 0, 1, 1, 0};
static const uint8_t  MAGIC_PREFIX[6] = {'B','S','T','K', 0, 1};
static const uint64_t HEADER_SIZE     = 16;

/* -------------------------------------------------------------------------
 * Internal struct
 * ---------------------------------------------------------------------- */

struct bstack {
    int              fd;
    pthread_rwlock_t lock;
};

/* -------------------------------------------------------------------------
 * Platform durability
 * ---------------------------------------------------------------------- */

static int durable_sync(int fd)
{
#ifdef __APPLE__
    if (fcntl(fd, F_FULLFSYNC) == 0)
        return 0;
    /* Device does not support F_FULLFSYNC — fall back to fdatasync. */
#endif
    return fdatasync(fd);
}

/* -------------------------------------------------------------------------
 * Little-endian helpers (using pread/pwrite — no cursor side-effects)
 * ---------------------------------------------------------------------- */

static int write_le64(int fd, uint64_t file_offset, uint64_t val)
{
    uint8_t buf[8];
    for (int i = 0; i < 8; i++)
        buf[i] = (uint8_t)(val >> (8 * i));
    ssize_t r = pwrite(fd, buf, 8, (off_t)file_offset);
    return (r == 8) ? 0 : -1;
}

/* -------------------------------------------------------------------------
 * Header helpers
 * ---------------------------------------------------------------------- */

static int write_committed_len(int fd, uint64_t len)
{
    return write_le64(fd, 8, len);
}

static int init_header(int fd)
{
    uint8_t hdr[16];
    memcpy(hdr, MAGIC, 8);
    memset(hdr + 8, 0, 8);
    ssize_t r = pwrite(fd, hdr, 16, 0);
    return (r == 16) ? 0 : -1;
}

/* Validates magic prefix and returns committed payload length via *out_clen.
 * Sets errno = EINVAL on bad magic or short header read. */
static int read_header(int fd, uint64_t *out_clen)
{
    uint8_t hdr[16];
    ssize_t r = pread(fd, hdr, 16, 0);
    if (r != 16) {
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

static int file_size(int fd, uint64_t *out)
{
    struct stat st;
    if (fstat(fd, &st) != 0)
        return -1;
    *out = (uint64_t)st.st_size;
    return 0;
}

#ifdef __cplusplus
extern "C" {
#endif

/* -------------------------------------------------------------------------
 * bstack_open
 * ---------------------------------------------------------------------- */

bstack_t *bstack_open(const char *path)
{
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

    uint64_t raw_size;
    if (file_size(fd, &raw_size) != 0) {
        int saved = errno;
        close(fd);
        errno = saved;
        return NULL;
    }

    if (raw_size == 0) {
        /* New file — write header and sync. */
        if (init_header(fd) != 0 || durable_sync(fd) != 0) {
            int saved = errno;
            close(fd);
            errno = saved;
            return NULL;
        }
    } else if (raw_size < HEADER_SIZE) {
        close(fd);
        errno = EINVAL;
        return NULL;
    } else {
        /* Existing file — validate header and crash-recover if needed. */
        uint64_t clen;
        if (read_header(fd, &clen) != 0) {
            int saved = errno;
            close(fd);
            errno = saved;
            return NULL;
        }

        uint64_t actual = raw_size - HEADER_SIZE;
        if (actual != clen) {
            uint64_t correct = (clen < actual) ? clen : actual;
            if (ftruncate(fd, (off_t)(HEADER_SIZE + correct)) != 0 ||
                write_committed_len(fd, correct) != 0 ||
                durable_sync(fd) != 0)
            {
                int saved = errno;
                close(fd);
                errno = saved;
                return NULL;
            }
        }
    }

    bstack_t *bs = malloc(sizeof(bstack_t));
    if (!bs) {
        close(fd);
        return NULL;
    }
    bs->fd = fd;
    if (pthread_rwlock_init(&bs->lock, NULL) != 0) {
        free(bs);
        close(fd);
        errno = ENOMEM;
        return NULL;
    }
    return bs;
}

/* -------------------------------------------------------------------------
 * bstack_close
 * ---------------------------------------------------------------------- */

void bstack_close(bstack_t *bs)
{
    if (!bs)
        return;
    pthread_rwlock_destroy(&bs->lock);
    close(bs->fd); /* also releases the advisory flock */
    free(bs);
}

/* -------------------------------------------------------------------------
 * bstack_push
 * ---------------------------------------------------------------------- */

int bstack_push(bstack_t *bs, const uint8_t *data, size_t len,
                uint64_t *out_offset)
{
    pthread_rwlock_wrlock(&bs->lock);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t logical_offset = raw_size - HEADER_SIZE;

    if (len == 0) {
        pthread_rwlock_unlock(&bs->lock);
        if (out_offset)
            *out_offset = logical_offset;
        return 0;
    }

    /* Write payload at end of file. */
    ssize_t written = pwrite(bs->fd, data, len, (off_t)raw_size);
    if (written < 0 || (size_t)written != len) {
        /* Best-effort rollback: truncate any partial write. */
        ftruncate(bs->fd, (off_t)raw_size);
        goto fail_unlock;
    }

    uint64_t new_len = logical_offset + (uint64_t)len;
    if (write_committed_len(bs->fd, new_len) != 0 ||
        durable_sync(bs->fd) != 0)
    {
        /* Rollback: remove written data and reset committed length. */
        ftruncate(bs->fd, (off_t)raw_size);
        write_committed_len(bs->fd, logical_offset);
        goto fail_unlock;
    }

    pthread_rwlock_unlock(&bs->lock);
    if (out_offset)
        *out_offset = logical_offset;
    return 0;

fail_unlock:
    {
        int saved = errno;
        pthread_rwlock_unlock(&bs->lock);
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
    pthread_rwlock_wrlock(&bs->lock);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;
    if ((uint64_t)n > data_size) {
        pthread_rwlock_unlock(&bs->lock);
        errno = EINVAL;
        return -1;
    }

    uint64_t new_len = data_size - (uint64_t)n;
    uint64_t read_offset = HEADER_SIZE + new_len;

    /* Read the bytes to be removed before truncating. */
    if (n > 0) {
        ssize_t r = pread(bs->fd, buf, n, (off_t)read_offset);
        if (r < 0 || (size_t)r != n)
            goto fail_unlock;
    }

    if (ftruncate(bs->fd, (off_t)(HEADER_SIZE + new_len)) != 0 ||
        write_committed_len(bs->fd, new_len) != 0 ||
        durable_sync(bs->fd) != 0)
        goto fail_unlock;

    pthread_rwlock_unlock(&bs->lock);
    if (written_out)
        *written_out = n;
    return 0;

fail_unlock:
    {
        int saved = errno;
        pthread_rwlock_unlock(&bs->lock);
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
    pthread_rwlock_rdlock(&bs->lock);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;
    if (offset > data_size) {
        pthread_rwlock_unlock(&bs->lock);
        errno = EINVAL;
        return -1;
    }

    size_t to_read = (size_t)(data_size - offset);
    if (to_read > 0) {
        ssize_t r = pread(bs->fd, buf, to_read,
                          (off_t)(HEADER_SIZE + offset));
        if (r < 0 || (size_t)r != to_read)
            goto fail_unlock;
    }

    pthread_rwlock_unlock(&bs->lock);
    if (written_out)
        *written_out = to_read;
    return 0;

fail_unlock:
    {
        int saved = errno;
        pthread_rwlock_unlock(&bs->lock);
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

    pthread_rwlock_rdlock(&bs->lock);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;
    if (end > data_size) {
        pthread_rwlock_unlock(&bs->lock);
        errno = EINVAL;
        return -1;
    }

    size_t to_read = (size_t)(end - start);
    if (to_read > 0) {
        ssize_t r = pread(bs->fd, buf, to_read,
                          (off_t)(HEADER_SIZE + start));
        if (r < 0 || (size_t)r != to_read)
            goto fail_unlock;
    }

    pthread_rwlock_unlock(&bs->lock);
    return 0;

fail_unlock:
    {
        int saved = errno;
        pthread_rwlock_unlock(&bs->lock);
        errno = saved;
    }
    return -1;
}

/* -------------------------------------------------------------------------
 * bstack_len
 * ---------------------------------------------------------------------- */

int bstack_len(bstack_t *bs, uint64_t *out_len)
{
    pthread_rwlock_rdlock(&bs->lock);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0) {
        int saved = errno;
        pthread_rwlock_unlock(&bs->lock);
        errno = saved;
        return -1;
    }

    pthread_rwlock_unlock(&bs->lock);
    *out_len = raw_size - HEADER_SIZE;
    return 0;
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

    pthread_rwlock_wrlock(&bs->lock);

    uint64_t raw_size;
    if (file_size(bs->fd, &raw_size) != 0)
        goto fail_unlock;

    uint64_t data_size = raw_size - HEADER_SIZE;
    if (end > data_size) {
        pthread_rwlock_unlock(&bs->lock);
        errno = EINVAL;
        return -1;
    }

    ssize_t r = pwrite(bs->fd, data, len, (off_t)(HEADER_SIZE + offset));
    if (r < 0 || (size_t)r != len)
        goto fail_unlock;

    if (durable_sync(bs->fd) != 0)
        goto fail_unlock;

    pthread_rwlock_unlock(&bs->lock);
    return 0;

fail_unlock:
    {
        int saved = errno;
        pthread_rwlock_unlock(&bs->lock);
        errno = saved;
    }
    return -1;
}

#endif /* BSTACK_FEATURE_SET */

#ifdef __cplusplus
}
#endif
