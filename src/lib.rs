//! A persistent, fsync-durable binary stack backed by a single file.
//!
//! # Overview
//!
//! [`BStack`] treats a file as a flat byte buffer that grows and shrinks from
//! the tail.  Every mutating operation — [`push`](BStack::push),
//! [`pop`](BStack::pop), and (with the `set` feature) [`set`](BStack::set) —
//! calls a *durable sync* before returning, so the data survives a process
//! crash or an unclean system shutdown.  Read-only operations —
//! [`peek`](BStack::peek) and [`get`](BStack::get) — never modify the file
//! and on Unix and Windows can run concurrently with each other.
//!
//! The crate depends on **`libc`** (Unix) and **`windows-sys`** (Windows) for
//! platform-specific syscalls, and uses **no `unsafe` code beyond the required
//! FFI calls**.
//!
//! # File format
//!
//! Every file begins with a fixed 16-byte header:
//!
//! ```text
//! ┌────────────────────────┬──────────────┬──────────────┐
//! │      header (16 B)     │  payload 0   │  payload 1   │  ...
//! │  magic[8] | clen[8 LE] │              │              │
//! └────────────────────────┴──────────────┴──────────────┘
//! ^                        ^              ^              ^
//! file offset 0         offset 16      16+n0          EOF
//! ```
//!
//! * **`magic`** — 8 bytes: `BSTK` + major(1 B) + minor(1 B) + patch(1 B) + reserved(1 B).
//!   This version writes `BSTK\x00\x01\x02\x00` (0.1.2).  [`open`](BStack::open)
//!   accepts any file whose first 6 bytes match `BSTK\x00\x01` (any 0.1.x) and
//!   rejects anything with a different major or minor.
//! * **`clen`** — little-endian `u64` recording the *committed* payload length.
//!   It is updated atomically with each [`push`](BStack::push) or
//!   [`pop`](BStack::pop) and is used for crash recovery on the next
//!   [`open`](BStack::open).
//!
//! All user-visible offsets are **logical** (0-based from the start of the
//! payload region, i.e. from file byte 16).
//!
//! # Crash recovery
//!
//! On [`open`](BStack::open), the header's committed length is compared against
//! the actual file size:
//!
//! | Condition | Cause | Recovery |
//! |-----------|-------|----------|
//! | `file_size − 16 > clen` | partial tail write (push crashed before header update) | truncate to `16 + clen` |
//! | `file_size − 16 < clen` | partial truncation (pop crashed before header update) | set `clen = file_size − 16` |
//!
//! After recovery a `durable_sync` ensures the repaired state is on stable
//! storage before any caller can observe or modify the file.
//!
//! # Durability
//!
//! | Operation | Syscall sequence |
//! |-----------|-----------------|
//! | `push` | `lseek(END)` → `write(data)` → `lseek(8)` → `write(clen)` → `durable_sync` |
//! | `pop`  | `lseek` → `read` → `ftruncate` → `lseek(8)` → `write(clen)` → `durable_sync` |
//! | `set` *(feature)* | `lseek(offset)` → `write(data)` → `durable_sync` |
//! | `peek`, `get` | `pread(2)` on Unix; `ReadFile`+`OVERLAPPED` on Windows; `lseek` → `read` elsewhere (no sync — read-only) |
//!
//! **`durable_sync` on macOS** issues `fcntl(F_FULLFSYNC)`, which flushes the
//! drive's hardware write cache.  Plain `fdatasync` is not sufficient on macOS
//! because the kernel may acknowledge it before the drive controller has
//! committed the data.  If `F_FULLFSYNC` is not supported by the device the
//! implementation falls back to `sync_data` (`fdatasync`).
//!
//! **`durable_sync` on other Unix** calls `sync_data` (`fdatasync`), which is
//! sufficient on Linux and BSD.
//!
//! **`durable_sync` on Windows** calls `sync_data`, which maps to
//! `FlushFileBuffers`.  This flushes the kernel write-back cache and waits for
//! the drive to acknowledge, providing equivalent durability to `fdatasync`.
//!
//! # Multi-process safety
//!
//! On Unix, [`open`](BStack::open) acquires an **exclusive advisory `flock`**
//! on the file (`LOCK_EX | LOCK_NB`).  If another process already holds the
//! lock, `open` returns immediately with [`io::ErrorKind::WouldBlock`] rather
//! than blocking indefinitely.  The lock is released automatically when the
//! [`BStack`] is dropped (the underlying file descriptor is closed).
//!
//! On Windows, [`open`](BStack::open) acquires an **exclusive `LockFileEx`**
//! lock (`LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY`) covering the
//! entire file range.  If another process already holds the lock, `open`
//! returns immediately with [`io::ErrorKind::WouldBlock`]
//! (`ERROR_LOCK_VIOLATION`).  The lock is released when the [`BStack`] is
//! dropped (the underlying file handle is closed).
//!
//! > **Note:** Both `flock` (Unix) and `LockFileEx` (Windows) are advisory
//! > and per-process.  They prevent well-behaved concurrent opens across
//! > processes but do not protect against processes that bypass the lock or
//! > against raw writes to the file.
//!
//! # Correct usage
//!
//! bstack files must only be opened through this crate or a compatible
//! implementation that understands the file format, the header protocol, and
//! the locking semantics.  Reading or writing the underlying file with raw
//! tools or syscalls while a [`BStack`] instance is live — or manually editing
//! the header fields — can silently corrupt the committed-length sentinel or
//! bypass the advisory lock.
//!
//! **The authors make no guarantees about the behaviour of this crate —
//! including freedom from data loss or logical corruption — when the file has
//! been accessed outside of this crate's controlled interface.**
//!
//! # Thread safety
//!
//! `BStack` wraps the file in a [`std::sync::RwLock`].
//!
//! | Operation | Lock (Unix / Windows) | Lock (other) |
//! |-----------|-----------------------|--------------|
//! | `push`, `pop` | write | write |
//! | `set` *(feature)* | write | write |
//! | `peek`, `get` | **read** | write |
//! | `len` | read | read |
//!
//! On Unix and Windows, `peek` and `get` use a cursor-safe positional read
//! (`pread(2)` on Unix; `ReadFile` with `OVERLAPPED` on Windows) that does
//! not modify the file-position cursor.  This allows multiple concurrent
//! `peek`/`get`/`len` calls to run in parallel while any ongoing `push` or
//! `pop` still serialises all writers via the write lock.
//!
//! On other platforms a seek is required, so `peek` and `get` fall back to
//! the write lock and all reads serialise.
//!
//! # Feature flags
//!
//! | Feature | Description |
//! |---------|-------------|
//! | `set`   | Enables [`BStack::set`] — in-place overwrite of existing payload bytes without changing the file size. |
//!
//! Enable with:
//!
//! ```toml
//! [dependencies]
//! bstack = { version = "0.1", features = ["set"] }
//! ```
//!
//! # Examples
//!
//! ```no_run
//! use bstack::BStack;
//!
//! # fn main() -> std::io::Result<()> {
//! let stack = BStack::open("log.bin")?;
//!
//! // push returns the logical byte offset where the payload starts.
//! let off0 = stack.push(b"hello")?;  // 0
//! let off1 = stack.push(b"world")?;  // 5
//!
//! assert_eq!(stack.len()?, 10);
//!
//! // peek reads from a logical offset to the end without removing anything.
//! assert_eq!(stack.peek(off1)?, b"world");
//!
//! // get reads an arbitrary half-open logical byte range.
//! assert_eq!(stack.get(3, 8)?, b"lowor");
//!
//! // pop removes bytes from the tail and returns them.
//! assert_eq!(stack.pop(5)?, b"world");
//! assert_eq!(stack.len()?, 5);
//! # Ok(())
//! # }
//! ```

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::RwLock;

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;

#[cfg(windows)]
use std::os::windows::fs::FileExt as WindowsFileExt;
#[cfg(windows)]
use std::os::windows::io::AsRawHandle;
#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::{
    LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY, LockFileEx,
};
#[cfg(windows)]
use windows_sys::Win32::System::IO::OVERLAPPED;

/// Full magic for files written by this version (`BSTK` + major 0 + minor 1 + patch 2 + 0).
const MAGIC: [u8; 8] = *b"BSTK\x00\x01\x02\x00";

/// Compatibility prefix checked on open: `BSTK` + major 0 + minor 1.
/// Any file whose first 6 bytes match is considered a compatible 0.1.x file.
const MAGIC_PREFIX: [u8; 6] = *b"BSTK\x00\x01";

/// Bytes occupied by the file header (magic[8] + committed_len[8]).
const HEADER_SIZE: u64 = 16;

/// Flush all in-flight writes to stable storage.
///
/// On macOS this uses `F_FULLFSYNC` to flush the drive's hardware write cache,
/// which `fdatasync` alone does not guarantee.  Falls back to `sync_data` if
/// `F_FULLFSYNC` returns an error (e.g. the device doesn't support it).
/// On all other platforms this delegates to `sync_data` (`fdatasync`).
fn durable_sync(file: &File) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        let ret = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) };
        if ret != -1 {
            return Ok(());
        }
        // Device does not support F_FULLFSYNC; fall back to fdatasync.
    }
    file.sync_data()
}

/// Acquire an exclusive, non-blocking advisory flock on `file`.
///
/// Returns `Err(WouldBlock)` if another process already holds the lock.
#[cfg(unix)]
fn flock_exclusive(file: &File) -> io::Result<()> {
    let ret = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if ret == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Acquire an exclusive, non-blocking `LockFileEx` lock on `file`.
///
/// Locks the entire file range (offset 0, length `u64::MAX`).
/// Returns `Err(WouldBlock)` if another process already holds the lock
/// (`ERROR_LOCK_VIOLATION` maps to `io::ErrorKind::WouldBlock` in Rust).
#[cfg(windows)]
fn lock_file_exclusive(file: &File) -> io::Result<()> {
    let handle = file.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE;
    // OVERLAPPED is required by LockFileEx even for synchronous handles.
    // Offset fields (0, 0) anchor the lock at byte 0 of the file.
    let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
    let ret = unsafe {
        LockFileEx(
            handle,
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,        // reserved, must be zero
            u32::MAX, // nNumberOfBytesToLockLow  ─┐ lock entire
            u32::MAX, // nNumberOfBytesToLockHigh ─┘ file space
            &mut overlapped,
        )
    };
    if ret != 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Write the 16-byte header into a brand-new (empty) file.
fn init_header(file: &mut File) -> io::Result<()> {
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&MAGIC)?;
    file.write_all(&0u64.to_le_bytes())
}

/// Overwrite the committed-length field at file offset 8.
fn write_committed_len(file: &mut File, len: u64) -> io::Result<()> {
    file.seek(SeekFrom::Start(8))?;
    file.write_all(&len.to_le_bytes())
}

/// Read `len` bytes from absolute file position `offset` without modifying
/// the file-position cursor, so the caller only needs a shared (read) lock.
///
/// On Unix this uses `pread(2)` via `read_exact_at`.
/// On Windows this uses `ReadFile` with an `OVERLAPPED` offset (via
/// `seek_read`), which is also cursor-safe on synchronous handles.
#[cfg(unix)]
fn pread_exact(file: &File, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    file.read_exact_at(&mut buf, offset)?;
    Ok(buf)
}

/// Windows counterpart of `pread_exact` — see the shared doc comment above.
#[cfg(windows)]
fn pread_exact(file: &File, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    let mut filled = 0usize;
    while filled < len {
        let n = file.seek_read(&mut buf[filled..], offset + filled as u64)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "pread_exact: unexpected EOF",
            ));
        }
        filled += n;
    }
    Ok(buf)
}

/// Read and validate the header; return the committed payload length.
fn read_header(file: &mut File) -> io::Result<u64> {
    file.seek(SeekFrom::Start(0))?;
    let mut hdr = [0u8; 16];
    file.read_exact(&mut hdr)?;
    if hdr[0..6] != MAGIC_PREFIX {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bstack: bad magic number — not a bstack file or incompatible version",
        ));
    }
    Ok(u64::from_le_bytes(hdr[8..16].try_into().unwrap()))
}

// ---------------------------------------------------------------------------

/// A persistent, fsync-durable binary stack backed by a single file.
///
/// See the [crate-level documentation](crate) for the file format, durability
/// guarantees, crash recovery, multi-process safety, and thread-safety model.
pub struct BStack {
    lock: RwLock<File>,
}

impl BStack {
    /// Open or create a stack file at `path`.
    ///
    /// On a **new** file the 16-byte header is written and durably synced
    /// before returning.
    ///
    /// On an **existing** file the header is validated and, if a previous crash
    /// left the file in an inconsistent state, the file is repaired and durably
    /// synced before returning (see *Crash recovery* in the crate docs).
    ///
    /// On Unix an **exclusive advisory `flock`** is acquired; if another
    /// process already holds the lock this function returns immediately with
    /// [`io::ErrorKind::WouldBlock`].
    ///
    /// # Errors
    ///
    /// * [`io::ErrorKind::WouldBlock`] — another process holds the exclusive
    ///   lock (Unix only).
    /// * [`io::ErrorKind::InvalidData`] — the file exists but its header magic
    ///   is wrong (not a bstack file, or created by an incompatible version),
    ///   or the file is too short to contain a valid header.
    /// * Any [`io::Error`] from [`OpenOptions::open`], `read`, `write`, or
    ///   `durable_sync`.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        #[cfg(unix)]
        flock_exclusive(&file)?;

        #[cfg(windows)]
        lock_file_exclusive(&file)?;

        let raw_size = file.metadata()?.len();

        if raw_size == 0 {
            init_header(&mut file)?;
            durable_sync(&file)?;
        } else if raw_size < HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "bstack: file is {raw_size} bytes — too small to contain the 16-byte header"
                ),
            ));
        } else {
            let committed_len = read_header(&mut file)?;
            let actual_data_len = raw_size - HEADER_SIZE;
            if actual_data_len != committed_len {
                // Recover: use whichever length is smaller (the committed
                // value is the last successfully synced boundary).
                let correct_len = committed_len.min(actual_data_len);
                file.set_len(HEADER_SIZE + correct_len)?;
                write_committed_len(&mut file, correct_len)?;
                durable_sync(&file)?;
            }
        }

        Ok(BStack {
            lock: RwLock::new(file),
        })
    }

    /// Append `data` to the end of the file.
    ///
    /// Returns the **logical** byte offset at which `data` begins — i.e. the
    /// payload size immediately before the write.  An empty slice is valid; it
    /// writes nothing and returns the current end offset.
    ///
    /// # Atomicity
    ///
    /// Either the full payload is written, the header committed-length is
    /// updated, and the whole thing is durably synced, or the file is
    /// left unchanged (best-effort rollback via `ftruncate` + header reset).
    ///
    /// # Errors
    ///
    /// Returns any [`io::Error`] from `write_all`, `durable_sync`, or the
    /// fallback `set_len`.
    pub fn push(&self, data: &[u8]) -> io::Result<u64> {
        let mut file = self.lock.write().unwrap();
        let file_end = file.seek(SeekFrom::End(0))?;
        let logical_offset = file_end - HEADER_SIZE;

        if data.is_empty() {
            return Ok(logical_offset);
        }

        if let Err(e) = file.write_all(data) {
            let _ = file.set_len(file_end);
            return Err(e);
        }

        let new_len = logical_offset + data.len() as u64;
        if let Err(e) = write_committed_len(&mut file, new_len).and_then(|_| durable_sync(&file)) {
            // Roll back: truncate data and reset header.
            let _ = file.set_len(file_end);
            let _ = write_committed_len(&mut file, logical_offset);
            return Err(e);
        }

        Ok(logical_offset)
    }

    /// Remove and return the last `n` bytes of the file.
    ///
    /// `n = 0` is valid: no bytes are removed and an empty `Vec` is returned.
    /// `n` may span across multiple previous [`push`](Self::push) boundaries.
    ///
    /// # Atomicity
    ///
    /// The bytes are read before the file is truncated.  The committed-length
    /// in the header is updated and durably synced after the truncation.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `n` exceeds the current
    /// payload size.  Also propagates any I/O error from `read_exact`,
    /// `set_len`, `write_all`, or `durable_sync`.
    pub fn pop(&self, n: u64) -> io::Result<Vec<u8>> {
        let mut file = self.lock.write().unwrap();
        let raw_size = file.seek(SeekFrom::End(0))?;
        let data_size = raw_size - HEADER_SIZE;
        if n > data_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("pop({n}) exceeds payload size ({data_size})"),
            ));
        }
        let new_data_len = data_size - n;
        file.seek(SeekFrom::Start(HEADER_SIZE + new_data_len))?;
        let mut buf = vec![0u8; n as usize];
        file.read_exact(&mut buf)?;
        file.set_len(HEADER_SIZE + new_data_len)?;
        write_committed_len(&mut file, new_data_len)?;
        durable_sync(&file)?;
        Ok(buf)
    }

    /// Return a copy of every payload byte from `offset` to the end of the
    /// file.
    ///
    /// `offset` is a **logical** offset (as returned by [`push`](Self::push)).
    /// `offset == len()` is valid and returns an empty `Vec`.  The file is not
    /// modified.
    ///
    /// # Concurrency
    ///
    /// On Unix and Windows this uses a cursor-safe positional read (`pread(2)`
    /// on Unix; `ReadFile`+`OVERLAPPED` on Windows), so the method takes only
    /// the **read lock**, allowing multiple concurrent `peek` and `get` calls
    /// to run in parallel.
    ///
    /// On other platforms a seek is required; the method falls back to the
    /// write lock and concurrent reads serialise.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `offset` exceeds the current
    /// payload size.
    pub fn peek(&self, offset: u64) -> io::Result<Vec<u8>> {
        #[cfg(any(unix, windows))]
        {
            let file = self.lock.read().unwrap();
            let data_size = file.metadata()?.len().saturating_sub(HEADER_SIZE);
            if offset > data_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("peek offset ({offset}) exceeds payload size ({data_size})"),
                ));
            }
            pread_exact(&file, HEADER_SIZE + offset, (data_size - offset) as usize)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let mut file = self.lock.write().unwrap();
            let raw_size = file.seek(SeekFrom::End(0))?;
            let data_size = raw_size.saturating_sub(HEADER_SIZE);
            if offset > data_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("peek offset ({offset}) exceeds payload size ({data_size})"),
                ));
            }
            file.seek(SeekFrom::Start(HEADER_SIZE + offset))?;
            let mut buf = vec![0u8; (data_size - offset) as usize];
            file.read_exact(&mut buf)?;
            Ok(buf)
        }
    }

    /// Return a copy of the bytes in the half-open logical range `[start, end)`.
    ///
    /// `start == end` is valid and returns an empty `Vec`.  The file is not
    /// modified.
    ///
    /// # Concurrency
    ///
    /// Same as [`peek`](Self::peek): on Unix and Windows the read lock is
    /// taken and concurrent `get`/`peek`/`len` calls may run in parallel.  On
    /// other platforms the write lock is taken and reads serialise.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `end < start` or if `end`
    /// exceeds the current payload size.
    pub fn get(&self, start: u64, end: u64) -> io::Result<Vec<u8>> {
        if end < start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("get: end ({end}) < start ({start})"),
            ));
        }
        #[cfg(any(unix, windows))]
        {
            let file = self.lock.read().unwrap();
            let data_size = file.metadata()?.len().saturating_sub(HEADER_SIZE);
            if end > data_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("get: end ({end}) exceeds payload size ({data_size})"),
                ));
            }
            pread_exact(&file, HEADER_SIZE + start, (end - start) as usize)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let mut file = self.lock.write().unwrap();
            let raw_size = file.seek(SeekFrom::End(0))?;
            let data_size = raw_size.saturating_sub(HEADER_SIZE);
            if end > data_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("get: end ({end}) exceeds payload size ({data_size})"),
                ));
            }
            file.seek(SeekFrom::Start(HEADER_SIZE + start))?;
            let mut buf = vec![0u8; (end - start) as usize];
            file.read_exact(&mut buf)?;
            Ok(buf)
        }
    }

    /// Overwrite `data` bytes in place starting at logical `offset`.
    ///
    /// The file size is never changed: if `offset + data.len()` would exceed
    /// the current payload size the call is rejected.  An empty slice is a
    /// valid no-op.
    ///
    /// # Feature flag
    ///
    /// Only available when the `set` Cargo feature is enabled.
    ///
    /// # Durability
    ///
    /// Equivalent to `push`/`pop`: the overwritten bytes are durably synced
    /// before the call returns.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `offset + data.len()`
    /// exceeds the current payload size, or if the addition overflows `u64`.
    /// Propagates any I/O error from `write_all` or `durable_sync`.
    #[cfg(feature = "set")]
    pub fn set(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let end = offset.checked_add(data.len() as u64).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "set: offset + len overflows u64",
            )
        })?;
        let mut file = self.lock.write().unwrap();
        let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
        if end > data_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("set: write end ({end}) exceeds payload size ({data_size})"),
            ));
        }
        file.seek(SeekFrom::Start(HEADER_SIZE + offset))?;
        file.write_all(data)?;
        durable_sync(&file)
    }

    /// Return the current **logical** payload size in bytes (excludes the
    /// 16-byte header).
    ///
    /// Takes the read lock, so it can run concurrently with other `len` calls
    /// but blocks while any write-lock operation is in progress.  The returned
    /// value always reflects a clean operation boundary.
    ///
    /// # Errors
    ///
    /// Propagates any [`io::Error`] from [`File::metadata`].
    pub fn len(&self) -> io::Result<u64> {
        let file = self.lock.read().unwrap();
        Ok(file.metadata()?.len().saturating_sub(HEADER_SIZE))
    }

    /// Return `true` if the stack contains no payload bytes.
    ///
    /// # Errors
    ///
    /// Propagates any [`io::Error`] from [`File::metadata`].
    pub fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;

    fn mk_stack() -> (BStack, std::path::PathBuf) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("bstack_test_{pid}_{id}.bin"));
        let stack = BStack::open(&path).unwrap();
        (stack, path)
    }

    struct Guard(std::path::PathBuf);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    // -------------------------------------------------------------------------
    // Original functional tests (unchanged behaviour)
    // -------------------------------------------------------------------------

    #[test]
    fn push_returns_correct_offsets() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        let off0 = s.push(b"hello").unwrap();
        let off1 = s.push(b"world").unwrap();
        let off2 = s.push(b"!").unwrap();

        assert_eq!(off0, 0);
        assert_eq!(off1, 5);
        assert_eq!(off2, 10);
        assert_eq!(s.len().unwrap(), 11);
    }

    #[test]
    fn pop_returns_correct_bytes_and_shrinks() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abcde").unwrap();
        s.push(b"fghij").unwrap();
        assert_eq!(s.len().unwrap(), 10);

        let bytes = s.pop(5).unwrap();
        assert_eq!(bytes, b"fghij");
        assert_eq!(s.len().unwrap(), 5);

        let bytes = s.pop(5).unwrap();
        assert_eq!(bytes, b"abcde");
        assert_eq!(s.len().unwrap(), 0);
    }

    #[test]
    fn pop_across_push_boundary() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"12345").unwrap();
        s.push(b"67890").unwrap();

        let bytes = s.pop(7).unwrap();
        assert_eq!(bytes, b"4567890");
        assert_eq!(s.len().unwrap(), 3);
    }

    #[test]
    fn pop_on_empty_file_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        let err = s.pop(1).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn pop_n_exceeds_file_size_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abc").unwrap();
        let err = s.pop(10).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 3);
    }

    #[test]
    fn peek_reads_from_offset_to_end() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        s.push(b"world").unwrap();

        assert_eq!(s.peek(0).unwrap(), b"helloworld");
        assert_eq!(s.peek(5).unwrap(), b"world");
        assert_eq!(s.peek(7).unwrap(), b"rld");
        assert_eq!(s.peek(10).unwrap(), b"");
    }

    #[test]
    fn peek_offset_exceeds_size_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abc").unwrap();
        let err = s.peek(10).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 3);
    }

    #[test]
    fn get_reads_half_open_range() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        s.push(b"world").unwrap();

        assert_eq!(s.get(0, 5).unwrap(), b"hello");
        assert_eq!(s.get(5, 10).unwrap(), b"world");
        assert_eq!(s.get(3, 8).unwrap(), b"lowor");
        assert_eq!(s.get(4, 4).unwrap(), b"");
    }

    #[test]
    fn get_end_exceeds_size_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abc").unwrap();
        let err = s.get(0, 10).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn get_end_less_than_start_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abcde").unwrap();
        let err = s.get(4, 2).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn get_does_not_modify_file() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        s.push(b"world").unwrap();
        let _ = s.get(2, 8).unwrap();
        assert_eq!(s.len().unwrap(), 10);
        let off = s.push(b"!").unwrap();
        assert_eq!(off, 10);
    }

    #[test]
    fn interleaved_push_pop_correct_state() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        let o0 = s.push(b"AAA").unwrap();
        assert_eq!(o0, 0);
        let o1 = s.push(b"BB").unwrap();
        assert_eq!(o1, 3);
        let popped = s.pop(2).unwrap();
        assert_eq!(popped, b"BB");
        let o2 = s.push(b"CCCC").unwrap();
        assert_eq!(o2, 3);
        assert_eq!(s.len().unwrap(), 7);
        let all = s.pop(7).unwrap();
        assert_eq!(all, b"AAACCCC");
        assert_eq!(s.len().unwrap(), 0);
    }

    // ---- persistence / reopen -----------------------------------------------

    #[test]
    fn reopen_reads_back_correct_data() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        s.push(b"hello").unwrap();
        s.push(b"world").unwrap();
        drop(s);

        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.len().unwrap(), 10);
        assert_eq!(s2.peek(0).unwrap(), b"helloworld");
    }

    #[test]
    fn reopen_and_continue_pushing() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        let off0 = s.push(b"first").unwrap();
        assert_eq!(off0, 0);
        drop(s);

        let s2 = BStack::open(&p).unwrap();
        let off1 = s2.push(b"second").unwrap();
        assert_eq!(off1, 5);
        assert_eq!(s2.len().unwrap(), 11);
        assert_eq!(s2.peek(0).unwrap(), b"firstsecond");
    }

    #[test]
    fn reopen_after_pop_sees_truncated_file() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        s.push(b"hello").unwrap();
        s.push(b"world").unwrap();
        s.pop(5).unwrap();
        drop(s);

        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.len().unwrap(), 5);
        assert_eq!(s2.peek(0).unwrap(), b"hello");
    }

    // ---- zero / boundary ----------------------------------------------------

    #[test]
    fn push_empty_slice() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        let off0 = s.push(b"abc").unwrap();
        let off1 = s.push(&[]).unwrap();
        let off2 = s.push(b"def").unwrap();

        assert_eq!(off0, 0);
        assert_eq!(off1, 3);
        assert_eq!(off2, 3);
        assert_eq!(s.len().unwrap(), 6);
        assert_eq!(s.peek(0).unwrap(), b"abcdef");
    }

    #[test]
    fn pop_zero_bytes() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abc").unwrap();
        let bytes = s.pop(0).unwrap();
        assert_eq!(bytes, b"");
        assert_eq!(s.len().unwrap(), 3);
        let off = s.push(b"d").unwrap();
        assert_eq!(off, 3);
    }

    #[test]
    fn peek_zero_offset_on_empty_file() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        assert_eq!(s.peek(0).unwrap(), b"");
    }

    #[test]
    fn get_zero_range_on_empty_file() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        assert_eq!(s.get(0, 0).unwrap(), b"");
    }

    #[test]
    fn drain_to_zero_then_push_starts_at_offset_zero() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        s.pop(5).unwrap();
        assert_eq!(s.len().unwrap(), 0);

        let off = s.push(b"world").unwrap();
        assert_eq!(off, 0);
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"world");
    }

    // ---- data integrity -----------------------------------------------------

    #[test]
    fn peek_does_not_modify_file() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        s.push(b"world").unwrap();
        let _ = s.peek(3).unwrap();
        assert_eq!(s.len().unwrap(), 10);
        let off = s.push(b"!").unwrap();
        assert_eq!(off, 10);
    }

    #[test]
    fn binary_roundtrip_all_byte_values() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        let data: Vec<u8> = (0u16..512).map(|i| (i % 256) as u8).collect();
        s.push(&data).unwrap();
        let got = s.pop(data.len() as u64).unwrap();
        assert_eq!(got, data);
        assert_eq!(s.len().unwrap(), 0);
    }

    #[test]
    fn large_payload_roundtrip() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        let payload: Vec<u8> = (0..1024 * 1024)
            .map(|i: usize| (i.wrapping_mul(7).wrapping_add(13)) as u8)
            .collect();
        s.push(&payload).unwrap();
        let got = s.get(0, payload.len() as u64).unwrap();
        assert_eq!(got, payload);
        assert_eq!(s.len().unwrap(), payload.len() as u64);
    }

    // ---- header / magic / format --------------------------------------------

    #[test]
    fn new_file_has_valid_header() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        drop(s); // close BStack so we can read the raw file

        let raw = std::fs::read(&p).unwrap();
        assert_eq!(
            raw.len(),
            HEADER_SIZE as usize,
            "new file should be exactly 16 bytes"
        );
        assert_eq!(&raw[0..8], &MAGIC, "magic mismatch");
        let clen = u64::from_le_bytes(raw[8..16].try_into().unwrap());
        assert_eq!(clen, 0, "committed length should be 0 for empty stack");
    }

    #[test]
    fn header_committed_len_matches_after_pushes() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        s.push(b"hello").unwrap(); // 5 bytes
        s.push(b"world").unwrap(); // 5 bytes
        drop(s);

        let raw = std::fs::read(&p).unwrap();
        let clen = u64::from_le_bytes(raw[8..16].try_into().unwrap());
        assert_eq!(clen, 10);
        assert_eq!(raw.len() as u64, HEADER_SIZE + 10);
    }

    #[test]
    fn header_committed_len_matches_after_pop() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        s.push(b"hello").unwrap();
        s.push(b"world").unwrap();
        s.pop(5).unwrap();
        drop(s);

        let raw = std::fs::read(&p).unwrap();
        let clen = u64::from_le_bytes(raw[8..16].try_into().unwrap());
        assert_eq!(clen, 5);
        assert_eq!(raw.len() as u64, HEADER_SIZE + 5);
    }

    #[test]
    fn open_rejects_bad_magic() {
        let path = {
            use std::sync::atomic::{AtomicU64, Ordering};
            static C: AtomicU64 = AtomicU64::new(0);
            let id = C.fetch_add(1, Ordering::Relaxed);
            std::env::temp_dir().join(format!("bstack_badmagic_{}.bin", id))
        };
        let _g = Guard(path.clone());

        // Write 16 bytes with wrong magic.
        let mut bad: Vec<u8> = b"WRONGHDR".to_vec();
        bad.extend_from_slice(&0u64.to_le_bytes());
        std::fs::write(&path, &bad).unwrap();

        let err = BStack::open(&path).err().unwrap();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert!(err.to_string().contains("magic"));
    }

    #[test]
    fn open_rejects_truncated_header() {
        let path = {
            use std::sync::atomic::{AtomicU64, Ordering};
            static C: AtomicU64 = AtomicU64::new(0);
            let id = C.fetch_add(1, Ordering::Relaxed);
            std::env::temp_dir().join(format!("bstack_smallfile_{}.bin", id))
        };
        let _g = Guard(path.clone());

        // Only 8 bytes — too short for a valid header.
        std::fs::write(&path, b"tooshort").unwrap();

        let err = BStack::open(&path).err().unwrap();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn recovery_truncates_partial_push() {
        // Simulate a push that wrote data but crashed before updating clen.
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        s.push(b"committed").unwrap(); // 9 bytes, clen = 9
        drop(s);

        // Directly append 5 "phantom" bytes to the file (clen still says 9).
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&p).unwrap();
            f.write_all(b"ghost").unwrap();
            // Do NOT update the header — simulating a crash after write but
            // before the header update + fsync.
        }

        // Verify the raw file has 16 + 9 + 5 = 30 bytes but clen = 9.
        let raw = std::fs::read(&p).unwrap();
        assert_eq!(raw.len(), (HEADER_SIZE + 9 + 5) as usize);
        let clen_before = u64::from_le_bytes(raw[8..16].try_into().unwrap());
        assert_eq!(clen_before, 9);

        // Reopen: recovery should truncate the phantom 5 bytes.
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.len().unwrap(), 9);
        assert_eq!(s2.peek(0).unwrap(), b"committed");
        drop(s2);

        // Raw file should now be exactly 16 + 9 = 25 bytes.
        let raw2 = std::fs::read(&p).unwrap();
        assert_eq!(raw2.len(), (HEADER_SIZE + 9) as usize);
    }

    #[test]
    fn recovery_repairs_header_after_partial_pop() {
        // Simulate a pop that truncated the file but crashed before updating clen.
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        s.push(b"hello").unwrap(); // 5 bytes
        s.push(b"world").unwrap(); // 5 bytes
        drop(s);

        // Manually truncate the file to remove "world" (back to 16 + 5 = 21),
        // but leave clen at 10 — simulating a crash after ftruncate but before
        // the header write + fsync.
        {
            let f = OpenOptions::new().write(true).open(&p).unwrap();
            f.set_len(HEADER_SIZE + 5).unwrap();
            // Header still says clen = 10.
        }

        let raw = std::fs::read(&p).unwrap();
        assert_eq!(raw.len(), (HEADER_SIZE + 5) as usize);
        let clen_before = u64::from_le_bytes(raw[8..16].try_into().unwrap());
        assert_eq!(
            clen_before, 10,
            "header should still claim 10 before recovery"
        );

        // Reopen: recovery should set clen = 5 to match actual file size.
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.len().unwrap(), 5);
        assert_eq!(s2.peek(0).unwrap(), b"hello");
        drop(s2);

        let raw2 = std::fs::read(&p).unwrap();
        let clen_after = u64::from_le_bytes(raw2[8..16].try_into().unwrap());
        assert_eq!(clen_after, 5, "clen should be repaired to 5 after recovery");
    }

    // ---- set (feature-gated) ------------------------------------------------

    #[cfg(feature = "set")]
    #[test]
    fn set_overwrites_middle_bytes() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"helloworld").unwrap();
        s.set(5, b"WORLD").unwrap();
        assert_eq!(s.peek(0).unwrap(), b"helloWORLD");
        assert_eq!(s.len().unwrap(), 10);
    }

    #[cfg(feature = "set")]
    #[test]
    fn set_at_start() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"helloworld").unwrap();
        s.set(0, b"HELLO").unwrap();
        assert_eq!(s.peek(0).unwrap(), b"HELLOworld");
    }

    #[cfg(feature = "set")]
    #[test]
    fn set_at_exact_end_boundary() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        s.set(3, b"LO").unwrap();
        assert_eq!(s.peek(0).unwrap(), b"helLO");
    }

    #[cfg(feature = "set")]
    #[test]
    fn set_empty_slice_is_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        s.set(2, b"").unwrap();
        assert_eq!(s.peek(0).unwrap(), b"hello");
        assert_eq!(s.len().unwrap(), 5);
    }

    #[cfg(feature = "set")]
    #[test]
    fn set_does_not_change_file_size() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abcde").unwrap();
        s.set(1, b"XYZ").unwrap();
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"aXYZe");
    }

    #[cfg(feature = "set")]
    #[test]
    fn set_rejects_write_past_end() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        let err = s.set(3, b"TOOLONG").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        // File must be unchanged.
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(feature = "set")]
    #[test]
    fn set_rejects_offset_past_end() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        let err = s.set(10, b"x").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[cfg(feature = "set")]
    #[test]
    fn set_persists_across_reopen() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        s.push(b"helloworld").unwrap();
        s.set(5, b"WORLD").unwrap();
        drop(s);

        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), b"helloWORLD");
    }

    // ---- concurrency --------------------------------------------------------

    #[cfg(any(unix, windows))]
    #[test]
    fn concurrent_reads_do_not_serialise() {
        // On Unix and Windows, peek and get use a cursor-safe positional read
        // (pread(2) on Unix; ReadFile+OVERLAPPED on Windows) and hold only the
        // read lock, so they must be able to run simultaneously. We verify this
        // by spinning up many reader threads on a pre-populated stack and
        // confirming that they all finish with correct data — no deadlock, no
        // torn reads.
        use std::sync::Arc;
        use std::thread;

        let (s, p) = mk_stack();
        let _g = Guard(p);

        // Write 8 fixed-size records of 16 bytes each.
        const RECORDS: usize = 8;
        const RSIZE: u64 = 16;
        for i in 0..RECORDS {
            let mut rec = [0u8; RSIZE as usize];
            rec[0] = i as u8;
            s.push(&rec).unwrap();
        }

        let s = Arc::new(s);

        // Spawn 32 reader threads; each reads every record via both peek and get.
        let handles: Vec<_> = (0..32)
            .map(|_| {
                let s = Arc::clone(&s);
                thread::spawn(move || {
                    for i in 0..RECORDS {
                        let off = i as u64 * RSIZE;
                        let via_get = s.get(off, off + RSIZE).unwrap();
                        assert_eq!(via_get[0], i as u8);

                        // peek from this record's offset; the first byte of
                        // the returned slice must still be `i`.
                        let via_peek = s.peek(off).unwrap();
                        assert_eq!(via_peek[0], i as u8);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn concurrent_pushes_non_overlapping() {
        use std::collections::HashSet;
        use std::sync::Arc;
        use std::thread;

        let (s, p) = mk_stack();
        let _g = Guard(p);
        let s = Arc::new(s);

        const THREADS: usize = 8;
        const PER_THREAD: usize = 100;
        const ITEM: usize = 16;

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let s = Arc::clone(&s);
                thread::spawn(move || {
                    (0..PER_THREAD)
                        .map(|i| {
                            let mut data = [0u8; ITEM];
                            data[0] = t as u8;
                            data[1..9].copy_from_slice(&(i as u64).to_le_bytes());
                            let off = s.push(&data).unwrap();
                            (off, t, i)
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();

        let results: Vec<_> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();

        for &(off, _, _) in &results {
            assert_eq!(off % ITEM as u64, 0, "offset {off} is not aligned to ITEM");
        }

        let mut seen: HashSet<u64> = HashSet::new();
        for &(off, _, _) in &results {
            assert!(seen.insert(off), "duplicate offset {off}");
        }

        assert_eq!(s.len().unwrap(), (THREADS * PER_THREAD * ITEM) as u64);

        for &(off, t, i) in &results {
            let slot = s.get(off, off + ITEM as u64).unwrap();
            assert_eq!(slot[0], t as u8, "thread id mismatch at offset {off}");
            let idx = u64::from_le_bytes(slot[1..9].try_into().unwrap());
            assert_eq!(idx, i as u64, "item index mismatch at offset {off}");
        }
    }

    #[test]
    fn concurrent_len_is_multiple_of_item_size() {
        use std::sync::Arc;
        use std::thread;

        let (s, p) = mk_stack();
        let _g = Guard(p);
        let s = Arc::new(s);

        const ITEM: u64 = 8;
        const PUSH_THREADS: usize = 4;
        const PUSHES_PER_THREAD: usize = 200;

        let push_handles: Vec<_> = (0..PUSH_THREADS)
            .map(|_| {
                let s = Arc::clone(&s);
                thread::spawn(move || {
                    for _ in 0..PUSHES_PER_THREAD {
                        s.push(&[0xBEu8; ITEM as usize]).unwrap();
                    }
                })
            })
            .collect();

        let len_handle = {
            let s = Arc::clone(&s);
            thread::spawn(move || {
                for _ in 0..2000 {
                    let size = s.len().unwrap();
                    assert_eq!(
                        size % ITEM,
                        0,
                        "torn write: size {size} is not a multiple of {ITEM}"
                    );
                }
            })
        };

        for h in push_handles {
            h.join().unwrap();
        }
        len_handle.join().unwrap();

        assert_eq!(
            s.len().unwrap(),
            (PUSH_THREADS * PUSHES_PER_THREAD) as u64 * ITEM
        );
    }
}
