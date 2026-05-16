//! A persistent, fsync-durable binary stack backed by a single file.
//!
//! # Overview
//!
//! [`BStack`] treats a file as a flat byte buffer that grows and shrinks from
//! the tail.  Every mutating operation — [`push`](BStack::push),
//! [`extend`](BStack::extend), [`pop`](BStack::pop), [`discard`](BStack::discard), (with the `set`
//! feature) [`set`](BStack::set) and [`zero`](BStack::zero), (with the `atomic` feature)
//! [`replace`](BStack::replace), and (with both `set` and `atomic`)
//! [`process`](BStack::process) — calls a *durable sync* before returning,
//! so the data survives a process crash or an unclean system shutdown.
//! Read-only operations — [`peek`](BStack::peek),
//! [`peek_into`](BStack::peek_into), [`get`](BStack::get), and
//! [`get_into`](BStack::get_into) — never modify the file and on Unix and
//! Windows can run concurrently with each other.
//! [`pop_into`](BStack::pop_into) is the buffer-passing counterpart of `pop`,
//! carrying the same durability and atomicity guarantees.
//! [`discard`](BStack::discard) is like `pop` but discards the removed bytes
//! without reading or returning them, avoiding any allocation or copy.
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
//!   This version writes `BSTK\x00\x01\x0a\x00` (0.1.10).  [`open`](BStack::open)
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
//! | `extend` | `lseek(END)` → `set_len(new_end)` → `lseek(8)` → `write(clen)` → `durable_sync` |
//! | `pop`, `pop_into` | `lseek` → `read` → `ftruncate` → `lseek(8)` → `write(clen)` → `durable_sync` |
//! | `discard` | `ftruncate` → `lseek(8)` → `write(clen)` → `durable_sync` |
//! | `set` *(feature)* | `lseek(offset)` → `write(data)` → `durable_sync` |
//! | `zero` *(feature)* | `lseek(offset)` → `write(zeros)` → `durable_sync` |
//! | `atrunc` *(feature: atomic, net extension)* | `set_len(new_end)` → `lseek(tail)` → `write(buf)` → `durable_sync` → `lseek(8)` → `write(clen)` |
//! | `atrunc` *(feature: atomic, net truncation)* | `lseek(tail)` → `write(buf)` → `set_len(new_end)` → `durable_sync` → `lseek(8)` → `write(clen)` |
//! | `splice`, `splice_into` *(feature: atomic)* | `lseek(tail)` → `read(n)` → *(then as `atrunc`)* |
//! | `try_extend` *(feature: atomic)* | `lseek(END)` — conditional `push` sequence if size matches |
//! | `try_discard` *(feature: atomic)* | `lseek(END)` — conditional `discard` sequence if size matches |
//! | `swap`, `swap_into` *(features: set+atomic)* | `lseek(offset)` → `read` → `lseek(offset)` → `write(buf)` → `durable_sync` |
//! | `cas` *(features: set+atomic)* | `lseek(offset)` → `read` → compare — conditional `lseek(offset)` → `write(new)` → `durable_sync` |
//! | `process` *(features: set+atomic)* | `lseek(start)` → `read(end−start)` → *(callback)* → `lseek(start)` → `write(buf)` → `durable_sync` |
//! | `replace` *(feature: atomic)* | `lseek(tail)` → `read(n)` → *(callback)* → *(then as `atrunc`)* |
//! | `peek`, `peek_into`, `get`, `get_into` | `pread(2)` on Unix; `ReadFile`+`OVERLAPPED` on Windows; `lseek` → `read` elsewhere (no sync — read-only) |
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
//! | `push`, `extend`, `pop`, `pop_into`, `discard` | write | write |
//! | `set`, `zero` *(feature)* | write | write |
//! | `atrunc`, `splice`, `splice_into`, `try_extend` *(feature: atomic)* | write | write |
//! | `try_discard(s, n > 0)` *(feature: atomic)* | write | write |
//! | `try_discard(s, 0)` *(feature: atomic)* | **read** | **read** |
//! | `swap`, `swap_into`, `cas` *(features: set+atomic)* | write | write |
//! | `process` *(features: set+atomic)* | write | write |
//! | `replace` *(feature: atomic)* | write | write |
//! | `peek`, `peek_into`, `get`, `get_into` | **read** (or **none** for ranges entirely within the locked region) | write |
//! | `len` | read | read |
//!
//! On Unix and Windows, `peek`, `peek_into`, `get`, and `get_into` use a
//! cursor-safe positional read (`pread(2)` on Unix; `ReadFile` with
//! `OVERLAPPED` on Windows) that does not modify the file-position cursor.
//! This allows multiple concurrent calls to any of these methods to run in
//! parallel while any ongoing `push`, `pop`, or `pop_into` still serialises
//! all writers via the write lock.  When a read range lies entirely within
//! the [locked region](#locked-region-lock_up_to), the rwlock is bypassed
//! altogether — see that section for the concurrency model.
//!
//! On other platforms a seek is required, so `peek`, `peek_into`, `get`, and
//! `get_into` fall back to the write lock and all reads serialise.
//!
//! # Locked region (`lock_up_to`)
//!
//! [`BStack`] maintains an in-memory **monotonically growing partition
//! boundary** named the *locked region*.  Bytes in `[0, locked_len())` are
//! declared permanently immutable for the lifetime of the open file.
//!
//! The locked length starts at `0` on every [`open`](BStack::open) and is
//! **not persisted to disk** — the file format is unchanged.  Callers extend
//! the boundary by calling [`lock_up_to`](BStack::lock_up_to) (or open and
//! lock in one step with [`open_locked_up_to`](BStack::open_locked_up_to)).
//! It can only grow; attempts to shrink it return
//! [`io::ErrorKind::InvalidInput`].
//!
//! ## Effects
//!
//! * **Lock-free reads on Unix and Windows.**  When [`get`](BStack::get),
//!   [`get_into`](BStack::get_into), or [`peek_into`](BStack::peek_into) are
//!   called with a range that lies entirely within the locked region, the
//!   rwlock is bypassed and the read is served directly by `pread(2)`
//!   (Unix) or `ReadFile` + `OVERLAPPED` (Windows).  The `fstat` size check
//!   is skipped too — the locked length is a sufficient upper bound.
//!
//! * **Write protection.**  [`set`](BStack::set), [`zero`](BStack::zero),
//!   [`swap`](BStack::swap), [`swap_into`](BStack::swap_into),
//!   [`cas`](BStack::cas), and [`process`](BStack::process) return
//!   [`io::ErrorKind::InvalidInput`] when their target range overlaps the
//!   locked region.  [`atrunc`](BStack::atrunc), [`splice`](BStack::splice),
//!   [`splice_into`](BStack::splice_into), and [`replace`](BStack::replace)
//!   return the same error when the operation would modify bytes inside it.
//!
//! * **Shrink protection.**  [`pop`](BStack::pop),
//!   [`pop_into`](BStack::pop_into), [`discard`](BStack::discard), and
//!   [`try_discard`](BStack::try_discard) return
//!   [`io::ErrorKind::InvalidInput`] when they would shrink the payload
//!   below the locked length.
//!
//! Callers that never invoke `lock_up_to` see no behavioural change — every
//! read and write path adds only a single uncontended `AtomicU64::load` and
//! a comparison.
//!
//! ## Concurrency model
//!
//! `lock_up_to(n)` acquires the exclusive write lock before publishing the
//! new boundary with a `Release` store.  Lock-free readers `Acquire`-load
//! `locked` before each call.  Two consequences follow:
//!
//! * A stale load is always safe.  If a reader sees an older (smaller)
//!   `locked` value, it falls through to the rwlock path; if it sees a
//!   newer value, the entire range it now reads is by definition immutable.
//!
//! * Locked-region checks on writers are evaluated **under the write lock**,
//!   so they cannot race against a concurrent `lock_up_to` extending the
//!   boundary across the write target.
//!
//! ## Typical use
//!
//! ```no_run
//! use bstack::BStack;
//!
//! # fn main() -> std::io::Result<()> {
//! // A fixed 64-byte metadata block at the head of the file, read by many
//! // threads but never modified after first write.
//! let stack = BStack::open_locked_up_to("meta.bin", 64)?;
//! assert_eq!(stack.locked_len(), 64);
//!
//! // Reads of the metadata bypass the rwlock on Unix and Windows.
//! let header = stack.get(0, 64)?;
//! # let _ = header;
//! # Ok(())
//! # }
//! ```
//!
//! On platforms other than Unix and Windows the boundary still enforces
//! immutability through the rwlock path; only the lock-free read fast path
//! is platform-gated.
//!
//! # Standard I/O adapters
//!
//! ## Writing
//!
//! `BStack` implements [`std::io::Write`] (and so does `&BStack`, mirroring
//! [`std::io::Write` for `&File`]).  Each call to `write` is forwarded to
//! [`push`](BStack::push), so every write is atomically appended and durably
//! synced before returning.  `flush` is a no-op.
//!
//! ```no_run
//! use std::io::Write;
//! use bstack::BStack;
//!
//! # fn main() -> std::io::Result<()> {
//! let mut stack = BStack::open("log.bin")?;
//! stack.write_all(b"hello")?;
//! stack.write_all(b"world")?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Reading
//!
//! [`BStackReader`] wraps a `&BStack` with a cursor and implements
//! [`std::io::Read`] and [`std::io::Seek`].  Use [`BStack::reader`] or
//! [`BStack::reader_at`] to construct one.
//!
//! ```no_run
//! use std::io::{Read, Seek, SeekFrom};
//! use bstack::BStack;
//!
//! # fn main() -> std::io::Result<()> {
//! let stack = BStack::open("log.bin")?;
//! stack.push(b"hello world")?;
//!
//! let mut reader = stack.reader();
//! let mut buf = [0u8; 5];
//! reader.read_exact(&mut buf)?;  // b"hello"
//! reader.seek(SeekFrom::Start(6))?;
//! reader.read_exact(&mut buf)?;  // b"world"
//! # Ok(())
//! # }
//! ```
//!
//! # Trait implementations
//!
//! ## `BStack`
//!
//! | Trait | Semantics |
//! |-------|-----------|
//! | `Debug` | Shows `version` (semver string from the magic header, e.g. `"0.1.6"`) and `len` (`Option<u64>`, `None` on I/O failure). |
//! | `PartialEq` / `Eq` | **Pointer identity.** Two values are equal iff they are the same instance. No two distinct `BStack` values in one process can refer to the same file. |
//! | `Hash` | Hashes the instance address — consistent with pointer-identity `PartialEq`. |
//!
//! ## `BStackReader`
//!
//! | Trait | Semantics |
//! |-------|-----------|
//! | `PartialEq` / `Eq` | Equal when both the `BStack` pointer (identity) and the cursor `offset` match. |
//! | `Hash` | Hashes `(BStack pointer, offset)` — consistent with `PartialEq`. |
//! | `PartialOrd` / `Ord` | Ordered by `BStack` instance address, then by cursor `offset`. Groups all readers over the same stack and within that group orders by position. |
//!
//! # Feature flags
//!
//! | Feature | Description |
//! |---------|-------------|
//! | `set`   | Enables [`BStack::set`] and [`BStack::zero`] — in-place overwrite of existing payload bytes (or with zeros) without changing the file size. |
//! | `alloc` | Enables [`BStackAllocator`], [`BStackBulkAllocator`], [`BStackSlice`], [`BStackSliceReader`], and [`LinearBStackAllocator`] — region-based allocation over a `BStack` payload. |
//! | `atomic` | Enables [`BStack::atrunc`], [`BStack::splice`], [`BStack::splice_into`], [`BStack::try_extend`], [`BStack::try_discard`], and [`BStack::replace`] — compound read-modify-write operations that hold the write lock across what would otherwise be separate calls. Combined with `set`, also enables [`BStack::swap`], [`BStack::swap_into`], [`BStack::cas`], and [`BStack::process`]. |
//!
//! Enable with:
//!
//! ```toml
//! [dependencies]
//! bstack = { version = "0.1", features = ["set"] }
//! # or
//! bstack = { version = "0.1", features = ["alloc"] }
//! # or both
//! bstack = { version = "0.1", features = ["alloc", "set"] }
//! ```
//!
//! # Allocator (`alloc` feature)
//!
//! The `alloc` feature adds a region-management layer on top of [`BStack`].
//!
//! ## Key types
//!
//! * [`BStackAllocator`] — trait for types that own a [`BStack`] and manage
//!   contiguous byte regions within its payload.  Requires `stack()`,
//!   `into_stack()`, `alloc()`, and `realloc()`; provides a default no-op
//!   `dealloc()` and delegation helpers `len()` / `is_empty()`.
//!
//! * [`BStackBulkAllocator`] — extension trait for [`BStackAllocator`] that
//!   adds atomic bulk operations.  Both methods are required with no default; on error
//!   the backing store is left unchanged unless a crash occur.
//!
//! * [`BStackSlice`]`<'a, A>` — lightweight `Copy` handle (allocator reference +
//!   offset + length) to a contiguous region.  Exposes `read`, `read_into`,
//!   `read_range_into`, `subslice`, `subslice_range`, `reader`, `reader_at`,
//!   and (with the `set` feature) `write`, `write_range`, `zero`, `zero_range`.
//!
//! * [`BStackSliceReader`]`<'a, A>` — cursor-based reader over a
//!   [`BStackSlice`], implementing [`io::Read`] and [`io::Seek`] in the
//!   slice's coordinate space.
//!
//! * [`LinearBStackAllocator`] — reference bump allocator that appends regions
//!   sequentially.  `realloc` is O(1) for the tail allocation and returns
//!   `Unsupported` for non-tail slices.  `dealloc` reclaims the tail via
//!   [`BStack::discard`]; non-tail deallocations are a no-op.  Every operation
//!   maps to exactly one [`BStack`] call and is crash-safe by inheritance.
//!   Implements [`BStackAllocator`] and [`BStackBulkAllocator`].
//!
//! * [`FirstFitBStackAllocator`] — Experimental: a persistent first-fit free-list allocator
//!   that reuses freed regions to prevent unbounded file growth.  Requires both
//!   `alloc` and `set` features.
//!
//! * [`GhostTreeBstackAllocator`] — A pure-AVL general-purpose allocator with
//!   zero-overhead live allocations.  Free blocks store their AVL node inline,
//!   and the tree is keyed on `(size, address)` for best-fit allocation.
//!   Provides O(log n) allocation and deallocation with crash recovery through
//!   tree rebalancing on mount.
//!
//! ## Lifetime model
//!
//! `BStackSlice<'a, A>` borrows the **allocator** for `'a`, not the
//! [`BStack`] directly.  As a result the borrow checker statically prevents
//! calling [`BStackAllocator::into_stack`] — which consumes the allocator by
//! value — while any slice is still in scope.
//!
//! ## Quick example
//!
//! ```skip
//! use bstack::{BStack, BStackAllocator, LinearBStackAllocator};
//!
//! # fn main() -> std::io::Result<()> {
//! let alloc = LinearBStackAllocator::new(BStack::open("data.bstack")?);
//!
//! let slice = alloc.alloc(128)?;          // reserve 128 zero bytes
//! let data  = slice.read()?;              // read them back
//! alloc.dealloc(slice)?;                  // release (tail, so O(1))
//!
//! let stack = alloc.into_stack();         // reclaim the BStack
//! # Ok(())
//! # }
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

#[cfg(all(test, feature = "alloc", feature = "set"))]
mod alloc_fuzz_tests;
mod test;

#[cfg(feature = "alloc")]
mod alloc;
#[cfg(feature = "alloc")]
pub use alloc::{
    BStackAllocator, BStackBulkAllocator, BStackSlice, BStackSliceAllocator, BStackSliceReader,
    DebugCheckingAllocator, DebugHandle, LinearBStackAllocator, ManualAllocator,
};
#[cfg(all(feature = "alloc", feature = "set"))]
pub use alloc::{BStackSliceWriter, FirstFitBStackAllocator, GhostTreeBstackAllocator};

#[cfg(all(feature = "guarded", feature = "atomic"))]
pub use alloc::{BStackAtomicGuardedSlice, BStackAtomicGuardedSliceSubview};
#[cfg(feature = "guarded")]
pub use alloc::{BStackGuardedSlice, BStackGuardedSliceSubview};

use std::fmt;
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
#[cfg(unix)]
use std::os::unix::io::RawFd;

#[cfg(windows)]
use std::os::windows::fs::FileExt as WindowsFileExt;
#[cfg(windows)]
use std::os::windows::io::AsRawHandle;
#[cfg(windows)]
use windows_sys::Win32::Foundation::HANDLE;
#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::{
    LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY, LockFileEx, ReadFile,
};
#[cfg(windows)]
use windows_sys::Win32::System::IO::OVERLAPPED;

/// Full magic for files written by this version (`BSTK` + major 0 + minor 1 + patch 10 + 0).
const MAGIC: [u8; 8] = *b"BSTK\x00\x01\x0a\x00";

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

/// Fill `buf` from absolute file position `offset` without modifying the
/// file-position cursor.  Unix uses `pread(2)` via `read_exact_at`;
/// Windows uses `ReadFile` with an `OVERLAPPED` offset via `seek_read`.
#[cfg(unix)]
fn pread_exact_into(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<()> {
    file.read_exact_at(buf, offset)
}

/// Windows counterpart of `pread_exact_into`.
#[cfg(windows)]
fn pread_exact_into(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<()> {
    let len = buf.len();
    let mut filled = 0usize;
    while filled < len {
        let n = file.seek_read(&mut buf[filled..], offset + filled as u64)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "pread_exact_into: unexpected EOF",
            ));
        }
        filled += n;
    }
    Ok(())
}

/// Lock-free positional read using a raw file descriptor (Unix).
///
/// Calls `pread(2)` directly, bypassing the `RwLock<File>`.  Safe only when
/// the target range is within the immutable locked region, ensuring no
/// concurrent writer can touch those bytes.
#[cfg(unix)]
fn pread_exact_raw(fd: RawFd, offset: u64, buf: &mut [u8]) -> io::Result<()> {
    let mut filled = 0usize;
    while filled < buf.len() {
        let n = unsafe {
            libc::pread(
                fd,
                buf[filled..].as_mut_ptr() as *mut libc::c_void,
                buf.len() - filled,
                (offset + filled as u64) as libc::off_t,
            )
        };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "locked pread: unexpected EOF",
            ));
        }
        filled += n as usize;
    }
    Ok(())
}

/// Lock-free positional read using a raw Windows HANDLE.
///
/// Calls `ReadFile` with an `OVERLAPPED` offset, bypassing the `RwLock<File>`.
/// Safe under the same invariant as `pread_exact_raw`.
#[cfg(windows)]
fn pread_exact_raw_handle(handle: isize, offset: u64, buf: &mut [u8]) -> io::Result<()> {
    let handle = handle as HANDLE;
    let mut filled = 0usize;
    let len = buf.len();
    while filled < len {
        let current_offset = offset + filled as u64;
        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        // SAFETY: the Anonymous/Anonymous path exists in the windows-sys OVERLAPPED layout.
        overlapped.Anonymous.Anonymous.Offset = current_offset as u32;
        overlapped.Anonymous.Anonymous.OffsetHigh = (current_offset >> 32) as u32;
        let mut bytes_read: u32 = 0;
        let ret = unsafe {
            ReadFile(
                handle,
                buf[filled..].as_mut_ptr(),
                (len - filled) as u32,
                &mut bytes_read,
                &mut overlapped,
            )
        };
        if ret == 0 {
            return Err(io::Error::last_os_error());
        }
        if bytes_read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "locked ReadFile: unexpected EOF",
            ));
        }
        filled += bytes_read as usize;
    }
    Ok(())
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
    /// Monotonically growing partition boundary.  Bytes in `[0, locked)` are
    /// immutable and can be read without the rwlock on supported platforms.
    /// Not persisted — resets to 0 on every open.
    locked: AtomicU64,
    /// Copy of the raw file descriptor used for lock-free positional reads
    /// on the locked region.  The `File` inside `lock` retains ownership and
    /// will close the descriptor when `BStack` is dropped.
    #[cfg(unix)]
    fd: RawFd,
    /// Copy of the Windows HANDLE stored as `isize` so the field is
    /// `Send + Sync`.  Same lifetime guarantee as `fd` above.
    #[cfg(windows)]
    handle: isize,
}

// `BStack` is auto-`Send + Sync` on every platform: all fields
// (`RwLock<File>`, `AtomicU64`, and the `RawFd` / `isize` handle) already
// implement both traits.  The lock-free `pread` / `ReadFile`+`OVERLAPPED`
// paths are cursor-independent and safe to call from any thread, and the raw
// fd / handle remains valid for as long as `BStack` owns the `File`.

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

        #[cfg(unix)]
        let fd = file.as_raw_fd();
        #[cfg(windows)]
        let handle = file.as_raw_handle() as isize;

        Ok(BStack {
            #[cfg(unix)]
            fd,
            #[cfg(windows)]
            handle,
            lock: RwLock::new(file),
            locked: AtomicU64::new(0),
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
    pub fn push(&self, data: impl AsRef<[u8]>) -> io::Result<u64> {
        let data = data.as_ref();
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

    /// Append `n` zero bytes to the end of the file.
    ///
    /// Returns the **logical** byte offset at which the zeros begin — i.e. the
    /// payload size immediately before the write.  `n = 0` is valid; it writes
    /// nothing and returns the current end offset.
    ///
    /// # Atomicity
    ///
    /// Either the file is extended, the header committed-length is updated,
    /// and the whole thing is durably synced, or the file is left unchanged
    /// (best-effort rollback via `ftruncate` + header reset).
    ///
    /// # Errors
    ///
    /// Returns any [`io::Error`] from `set_len`, `durable_sync`, or the
    /// fallback `set_len`.
    pub fn extend(&self, n: u64) -> io::Result<u64> {
        let mut file = self.lock.write().unwrap();
        let file_end = file.seek(SeekFrom::End(0))?;
        let logical_offset = file_end - HEADER_SIZE;

        if n == 0 {
            return Ok(logical_offset);
        }

        let new_file_end = file_end + n;
        file.set_len(new_file_end)?;

        let new_len = logical_offset + n;
        if let Err(e) = write_committed_len(&mut file, new_len).and_then(|_| durable_sync(&file)) {
            // Roll back: truncate and reset header.
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
        let locked = self.locked.load(Ordering::Acquire);
        if new_data_len < locked {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("pop({n}) would shrink payload below locked length ({locked})"),
            ));
        }
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
        // Fast-path: if the range lies entirely within the locked region,
        // skip the rwlock — locked bytes are immutable so no lock is needed.
        #[cfg(unix)]
        {
            let locked = self.locked.load(Ordering::Acquire);
            if end <= locked {
                let mut buf = vec![0u8; (end - start) as usize];
                pread_exact_raw(self.fd, HEADER_SIZE + start, &mut buf)?;
                return Ok(buf);
            }
        }
        #[cfg(windows)]
        {
            let locked = self.locked.load(Ordering::Acquire);
            if end <= locked {
                let mut buf = vec![0u8; (end - start) as usize];
                pread_exact_raw_handle(self.handle, HEADER_SIZE + start, &mut buf)?;
                return Ok(buf);
            }
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

    /// Fill `buf` with bytes from logical `offset` to `offset + buf.len()`.
    ///
    /// Reads exactly `buf.len()` bytes from `offset` into the caller-supplied
    /// buffer.  An empty buffer is a valid no-op.  The file is not modified.
    ///
    /// Use this instead of [`peek`](Self::peek) when the destination buffer is
    /// already allocated and you want to avoid the extra heap allocation.
    ///
    /// # Concurrency
    ///
    /// Same as [`peek`](Self::peek): on Unix and Windows only the read lock is
    /// taken; on other platforms the write lock serialises all reads.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `offset + buf.len()` overflows
    /// `u64` or exceeds the current payload size.
    pub fn peek_into(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let len = buf.len() as u64;
        let end = offset.checked_add(len).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "peek_into: offset + len overflows u64",
            )
        })?;
        // Fast-path: locked region is immutable, skip the rwlock.
        #[cfg(unix)]
        {
            let locked = self.locked.load(Ordering::Acquire);
            if end <= locked {
                return pread_exact_raw(self.fd, HEADER_SIZE + offset, buf);
            }
        }
        #[cfg(windows)]
        {
            let locked = self.locked.load(Ordering::Acquire);
            if end <= locked {
                return pread_exact_raw_handle(self.handle, HEADER_SIZE + offset, buf);
            }
        }
        #[cfg(any(unix, windows))]
        {
            let file = self.lock.read().unwrap();
            let data_size = file.metadata()?.len().saturating_sub(HEADER_SIZE);
            if end > data_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "peek_into: range [{offset}, {end}) exceeds payload size ({data_size})"
                    ),
                ));
            }
            pread_exact_into(&file, HEADER_SIZE + offset, buf)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let mut file = self.lock.write().unwrap();
            let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
            if end > data_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "peek_into: range [{offset}, {end}) exceeds payload size ({data_size})"
                    ),
                ));
            }
            file.seek(SeekFrom::Start(HEADER_SIZE + offset))?;
            file.read_exact(buf)
        }
    }

    /// Fill `buf` with bytes from the half-open logical range
    /// `[start, start + buf.len())`.
    ///
    /// An empty buffer is a valid no-op.  The file is not modified.
    ///
    /// Use this instead of [`get`](Self::get) when the destination buffer is
    /// already allocated and you want to avoid the extra heap allocation.
    ///
    /// # Concurrency
    ///
    /// Same as [`get`](Self::get): on Unix and Windows only the read lock is
    /// taken; on other platforms the write lock serialises all reads.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `start + buf.len()` overflows
    /// `u64` or exceeds the current payload size.
    pub fn get_into(&self, start: u64, buf: &mut [u8]) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let len = buf.len() as u64;
        let end = start.checked_add(len).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "get_into: start + len overflows u64",
            )
        })?;
        // Fast-path: locked region is immutable, skip the rwlock.
        #[cfg(unix)]
        {
            let locked = self.locked.load(Ordering::Acquire);
            if end <= locked {
                return pread_exact_raw(self.fd, HEADER_SIZE + start, buf);
            }
        }
        #[cfg(windows)]
        {
            let locked = self.locked.load(Ordering::Acquire);
            if end <= locked {
                return pread_exact_raw_handle(self.handle, HEADER_SIZE + start, buf);
            }
        }
        #[cfg(any(unix, windows))]
        {
            let file = self.lock.read().unwrap();
            let data_size = file.metadata()?.len().saturating_sub(HEADER_SIZE);
            if end > data_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("get_into: end ({end}) exceeds payload size ({data_size})"),
                ));
            }
            pread_exact_into(&file, HEADER_SIZE + start, buf)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let mut file = self.lock.write().unwrap();
            let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
            if end > data_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("get_into: end ({end}) exceeds payload size ({data_size})"),
                ));
            }
            file.seek(SeekFrom::Start(HEADER_SIZE + start))?;
            file.read_exact(buf)
        }
    }

    /// Remove the last `buf.len()` bytes from the file and write them into `buf`.
    ///
    /// An empty buffer is a valid no-op: no bytes are removed.
    ///
    /// Use this instead of [`pop`](Self::pop) when the destination buffer is
    /// already allocated and you want to avoid the extra heap allocation.
    ///
    /// # Atomicity
    ///
    /// Same guarantees as [`pop`](Self::pop).
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `buf.len()` exceeds the
    /// current payload size.  Also propagates any I/O error from `read_exact`,
    /// `set_len`, `write_all`, or `durable_sync`.
    pub fn pop_into(&self, buf: &mut [u8]) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let n = buf.len() as u64;
        let mut file = self.lock.write().unwrap();
        let raw_size = file.seek(SeekFrom::End(0))?;
        let data_size = raw_size - HEADER_SIZE;
        if n > data_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("pop_into({n}) exceeds payload size ({data_size})"),
            ));
        }
        let new_data_len = data_size - n;
        let locked = self.locked.load(Ordering::Acquire);
        if new_data_len < locked {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("pop_into({n}) would shrink payload below locked length ({locked})"),
            ));
        }
        file.seek(SeekFrom::Start(HEADER_SIZE + new_data_len))?;
        file.read_exact(buf)?;
        file.set_len(HEADER_SIZE + new_data_len)?;
        write_committed_len(&mut file, new_data_len)?;
        durable_sync(&file)?;
        Ok(())
    }

    /// Remove (discard) the last `n` bytes from the file without returning them.
    ///
    /// Equivalent to [`pop`](Self::pop) but avoids allocating a buffer for the
    /// removed bytes.  `n = 0` is valid and is a no-op.
    ///
    /// # Atomicity
    ///
    /// Same guarantees as [`pop`](Self::pop).
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `n` exceeds the current
    /// payload size.  Also propagates any I/O error from `set_len`,
    /// `write_all`, or `durable_sync`.
    pub fn discard(&self, n: u64) -> io::Result<()> {
        if n == 0 {
            return Ok(());
        }
        let mut file = self.lock.write().unwrap();
        let raw_size = file.seek(SeekFrom::End(0))?;
        let data_size = raw_size - HEADER_SIZE;
        if n > data_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("discard({n}) exceeds payload size ({data_size})"),
            ));
        }
        let new_data_len = data_size - n;
        let locked = self.locked.load(Ordering::Acquire);
        if new_data_len < locked {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("discard({n}) would shrink payload below locked length ({locked})"),
            ));
        }
        file.set_len(HEADER_SIZE + new_data_len)?;
        write_committed_len(&mut file, new_data_len)?;
        durable_sync(&file)?;
        Ok(())
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
    pub fn set(&self, offset: u64, data: impl AsRef<[u8]>) -> io::Result<()> {
        let data = data.as_ref();
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
        // Load `locked` under the write lock — otherwise a concurrent
        // `lock_up_to` could extend the locked region between our check and
        // our write, letting us mutate a now-immutable byte.
        let locked = self.locked.load(Ordering::Acquire);
        if offset < locked {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("set: range [{offset}, {end}) overlaps locked region [0, {locked})"),
            ));
        }
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

    /// Overwrite `n` bytes with zeros in place starting at logical `offset`.
    ///
    /// The file size is never changed: if `offset + n` would exceed
    /// the current payload size the call is rejected.  `n = 0` is a
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
    /// Returns [`io::ErrorKind::InvalidInput`] if `offset + n`
    /// exceeds the current payload size, or if the addition overflows `u64`.
    /// Propagates any I/O error from `write_all` or `durable_sync`.
    #[cfg(feature = "set")]
    pub fn zero(&self, offset: u64, n: u64) -> io::Result<()> {
        if n == 0 {
            return Ok(());
        }
        let end = offset.checked_add(n).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "zero: offset + n overflows u64",
            )
        })?;
        let mut file = self.lock.write().unwrap();
        // Load `locked` under the write lock (see `set` for rationale).
        let locked = self.locked.load(Ordering::Acquire);
        if offset < locked {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("zero: range [{offset}, {end}) overlaps locked region [0, {locked})"),
            ));
        }
        let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
        if end > data_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("zero: write end ({end}) exceeds payload size ({data_size})"),
            ));
        }
        file.seek(SeekFrom::Start(HEADER_SIZE + offset))?;
        let zeros = vec![0u8; n as usize];
        file.write_all(&zeros)?;
        durable_sync(&file)
    }
}

// ---------------------------------------------------------------------------
// Atomic compound operations

#[cfg(feature = "atomic")]
impl BStack {
    /// Cut `n` bytes off the tail then append `buf` as a single atomic operation.
    ///
    /// The operation ordering is chosen based on the net size change to maximise
    /// crash-recovery safety (see *Durability* in the crate docs):
    ///
    /// * **Net extension** (`buf.len() > n`): the file is extended first, `buf`
    ///   is written into the freed tail region plus the new space, then a
    ///   `durable_sync` commits the data before the header committed-length is
    ///   updated.  On crash before the header update, recovery truncates back to
    ///   the original committed length — a clean rollback.
    ///
    /// * **Net truncation or same size** (`buf.len() ≤ n`): `buf` is written
    ///   into the tail first, then the file is truncated, then `durable_sync`
    ///   commits the result before the header is updated.  On crash after
    ///   truncation, recovery sets the committed length to the (smaller) file
    ///   size, committing the final state.
    ///
    /// `n = 0` with an empty `buf` is a valid no-op.
    ///
    /// # Feature flag
    ///
    /// Only available when the `atomic` Cargo feature is enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `n` exceeds the current
    /// payload size.  Propagates any I/O error from `set_len`, `write_all`,
    /// or `durable_sync`.
    #[cfg(feature = "atomic")]
    pub fn atrunc(&self, n: u64, buf: impl AsRef<[u8]>) -> io::Result<()> {
        let buf = buf.as_ref();
        let buf_len = buf.len() as u64;
        if n == 0 && buf_len == 0 {
            return Ok(());
        }
        let mut file = self.lock.write().unwrap();
        let file_end = file.seek(SeekFrom::End(0))?;
        let data_size = file_end - HEADER_SIZE;
        if n > data_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("atrunc: n ({n}) exceeds payload size ({data_size})"),
            ));
        }
        let locked = self.locked.load(Ordering::Acquire);
        let new_tail_start = data_size - n;
        if new_tail_start < locked {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("atrunc: operation would modify locked region [0, {locked})"),
            ));
        }
        let tail_offset = HEADER_SIZE + new_tail_start;
        let final_data_len = new_tail_start + buf_len;

        if buf_len > n {
            // Net extension: extend first so data is never lost, then write buf,
            // sync the data, then commit the new length.
            let new_file_end = HEADER_SIZE + final_data_len;
            file.set_len(new_file_end)?;
            file.seek(SeekFrom::Start(tail_offset))?;
            if let Err(e) = file.write_all(buf) {
                let _ = file.set_len(file_end);
                return Err(e);
            }
            if let Err(e) = durable_sync(&file) {
                let _ = file.set_len(file_end);
                return Err(e);
            }
            write_committed_len(&mut file, final_data_len)?;
        } else {
            // Net truncation or same size: write buf into the old tail first,
            // truncate, sync, then commit the new length.
            if !buf.is_empty() {
                file.seek(SeekFrom::Start(tail_offset))?;
                file.write_all(buf)?;
            }
            file.set_len(HEADER_SIZE + final_data_len)?;
            durable_sync(&file)?;
            write_committed_len(&mut file, final_data_len)?;
        }
        Ok(())
    }

    /// Pop `n` bytes off the tail then append `buf`, returning the removed bytes.
    ///
    /// The bytes are read before any mutation, so they are always available in
    /// the returned `Vec` even if the subsequent write fails.  The same
    /// ordering strategy as [`atrunc`](Self::atrunc) is used.
    ///
    /// `n = 0` with an empty `buf` is a valid no-op and returns an empty `Vec`.
    ///
    /// # Feature flag
    ///
    /// Only available when the `atomic` Cargo feature is enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `n` exceeds the current
    /// payload size.  Propagates any I/O error from `read_exact`, `set_len`,
    /// `write_all`, or `durable_sync`.
    #[cfg(feature = "atomic")]
    pub fn splice(&self, n: u64, buf: impl AsRef<[u8]>) -> io::Result<Vec<u8>> {
        let buf = buf.as_ref();
        let buf_len = buf.len() as u64;
        if n == 0 && buf_len == 0 {
            return Ok(Vec::new());
        }
        let mut file = self.lock.write().unwrap();
        let file_end = file.seek(SeekFrom::End(0))?;
        let data_size = file_end - HEADER_SIZE;
        if n > data_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("splice: n ({n}) exceeds payload size ({data_size})"),
            ));
        }
        let locked = self.locked.load(Ordering::Acquire);
        let new_tail_start = data_size - n;
        if new_tail_start < locked {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("splice: operation would modify locked region [0, {locked})"),
            ));
        }
        let tail_offset = HEADER_SIZE + new_tail_start;
        let final_data_len = new_tail_start + buf_len;

        // Read the bytes to remove before any mutation.
        file.seek(SeekFrom::Start(tail_offset))?;
        let mut removed = vec![0u8; n as usize];
        file.read_exact(&mut removed)?;

        if buf_len > n {
            // Net extension: extend first, write buf, sync, commit.
            let new_file_end = HEADER_SIZE + final_data_len;
            file.set_len(new_file_end)?;
            file.seek(SeekFrom::Start(tail_offset))?;
            if let Err(e) = file.write_all(buf) {
                let _ = file.set_len(file_end);
                return Err(e);
            }
            if let Err(e) = durable_sync(&file) {
                let _ = file.set_len(file_end);
                return Err(e);
            }
            write_committed_len(&mut file, final_data_len)?;
        } else {
            // Net truncation or same size: write buf, truncate, sync, commit.
            if !buf.is_empty() {
                file.seek(SeekFrom::Start(tail_offset))?;
                file.write_all(buf)?;
            }
            file.set_len(HEADER_SIZE + final_data_len)?;
            durable_sync(&file)?;
            write_committed_len(&mut file, final_data_len)?;
        }

        Ok(removed)
    }

    /// Pop `old.len()` bytes off the tail into `old`, then append `new`.
    ///
    /// Buffer-reuse counterpart of [`splice`](Self::splice): avoids allocating
    /// a `Vec` for the removed bytes by writing them into the caller-supplied
    /// `old` slice.  The same ordering strategy as [`atrunc`](Self::atrunc) is
    /// used for the write/truncation side.
    ///
    /// An empty `old` with an empty `new` is a valid no-op.
    ///
    /// # Feature flag
    ///
    /// Only available when the `atomic` Cargo feature is enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `old.len()` exceeds the
    /// current payload size.  Propagates any I/O error from `read_exact`,
    /// `set_len`, `write_all`, or `durable_sync`.
    #[cfg(feature = "atomic")]
    pub fn splice_into(&self, old: &mut [u8], new: impl AsRef<[u8]>) -> io::Result<()> {
        let new = new.as_ref();
        let n = old.len() as u64;
        let new_len = new.len() as u64;
        if n == 0 && new_len == 0 {
            return Ok(());
        }
        let mut file = self.lock.write().unwrap();
        let file_end = file.seek(SeekFrom::End(0))?;
        let data_size = file_end - HEADER_SIZE;
        if n > data_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("splice_into: n ({n}) exceeds payload size ({data_size})"),
            ));
        }
        let locked = self.locked.load(Ordering::Acquire);
        let new_tail_start = data_size - n;
        if new_tail_start < locked {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("splice_into: operation would modify locked region [0, {locked})"),
            ));
        }
        let tail_offset = HEADER_SIZE + new_tail_start;
        let final_data_len = new_tail_start + new_len;

        // Read the bytes to remove before any mutation.
        file.seek(SeekFrom::Start(tail_offset))?;
        file.read_exact(old)?;

        if new_len > n {
            // Net extension: extend first, write new, sync, commit.
            let new_file_end = HEADER_SIZE + final_data_len;
            file.set_len(new_file_end)?;
            file.seek(SeekFrom::Start(tail_offset))?;
            if let Err(e) = file.write_all(new) {
                let _ = file.set_len(file_end);
                return Err(e);
            }
            if let Err(e) = durable_sync(&file) {
                let _ = file.set_len(file_end);
                return Err(e);
            }
            write_committed_len(&mut file, final_data_len)?;
        } else {
            // Net truncation or same size: write new, truncate, sync, commit.
            if !new.is_empty() {
                file.seek(SeekFrom::Start(tail_offset))?;
                file.write_all(new)?;
            }
            file.set_len(HEADER_SIZE + final_data_len)?;
            durable_sync(&file)?;
            write_committed_len(&mut file, final_data_len)?;
        }
        Ok(())
    }

    /// Append `buf` only if the current logical payload size equals `s`.
    ///
    /// Returns `Ok(true)` if the size matched and `buf` was appended (or `buf`
    /// is empty and no I/O was needed).  Returns `Ok(false)` without modifying
    /// the file if the size does not match.
    ///
    /// # Feature flag
    ///
    /// Only available when the `atomic` Cargo feature is enabled.
    ///
    /// # Errors
    ///
    /// Propagates any I/O error from `write_all`, `write_committed_len`, or
    /// `durable_sync`.
    #[cfg(feature = "atomic")]
    pub fn try_extend(&self, s: u64, buf: impl AsRef<[u8]>) -> io::Result<bool> {
        let buf = buf.as_ref();
        let mut file = self.lock.write().unwrap();
        let file_end = file.seek(SeekFrom::End(0))?;
        let data_size = file_end - HEADER_SIZE;
        if data_size != s {
            return Ok(false);
        }
        if buf.is_empty() {
            return Ok(true);
        }
        if let Err(e) = file.write_all(buf) {
            let _ = file.set_len(file_end);
            return Err(e);
        }
        let new_len = data_size + buf.len() as u64;
        if let Err(e) = write_committed_len(&mut file, new_len).and_then(|_| durable_sync(&file)) {
            let _ = file.set_len(file_end);
            let _ = write_committed_len(&mut file, data_size);
            return Err(e);
        }
        Ok(true)
    }

    /// Discard `n` bytes only if the current logical payload size equals `s`.
    ///
    /// Returns `Ok(true)` if the size matched and `n` bytes were removed (or
    /// `n = 0` and the size check passed without I/O).  Returns `Ok(false)`
    /// without modifying the file if the size does not match.
    ///
    /// When `n = 0` only the read lock is taken (no file mutation occurs).
    ///
    /// # Feature flag
    ///
    /// Only available when the `atomic` Cargo feature is enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `n` exceeds the current
    /// payload size.  Propagates any I/O error from `set_len`,
    /// `write_committed_len`, or `durable_sync`.
    #[cfg(feature = "atomic")]
    pub fn try_discard(&self, s: u64, n: u64) -> io::Result<bool> {
        if n == 0 {
            let file = self.lock.read().unwrap();
            let data_size = file.metadata()?.len().saturating_sub(HEADER_SIZE);
            return Ok(data_size == s);
        }
        let mut file = self.lock.write().unwrap();
        let raw_size = file.seek(SeekFrom::End(0))?;
        let data_size = raw_size - HEADER_SIZE;
        if data_size != s {
            return Ok(false);
        }
        if n > data_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("try_discard: n ({n}) exceeds payload size ({data_size})"),
            ));
        }
        let new_data_len = data_size - n;
        let locked = self.locked.load(Ordering::Acquire);
        if new_data_len < locked {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("try_discard: would shrink payload below locked length ({locked})"),
            ));
        }
        file.set_len(HEADER_SIZE + new_data_len)?;
        write_committed_len(&mut file, new_data_len)?;
        durable_sync(&file)?;
        Ok(true)
    }

    /// Pop `n` bytes off the tail, pass them read-only to a callback that
    /// returns the new tail bytes, then write the new tail.
    ///
    /// The read, callback invocation, and write all happen under the same write
    /// lock, so no other thread can observe the state between the pop and the
    /// push.  The callback may return a [`Vec<u8>`] of any length — the file
    /// will grow or shrink accordingly using the same crash-safe ordering
    /// strategy as [`atrunc`](Self::atrunc).
    ///
    /// `n = 0` is valid: the callback receives an empty slice and whatever it
    /// returns is appended.
    ///
    /// # Feature flag
    ///
    /// Only available when the `atomic` Cargo feature is enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `n` exceeds the current
    /// payload size.  Propagates any I/O error from `read_exact`, `set_len`,
    /// `write_all`, or `durable_sync`.
    #[cfg(feature = "atomic")]
    pub fn replace<F>(&self, n: u64, f: F) -> io::Result<()>
    where
        F: FnOnce(&[u8]) -> Vec<u8>,
    {
        let mut file = self.lock.write().unwrap();
        let file_end = file.seek(SeekFrom::End(0))?;
        let data_size = file_end - HEADER_SIZE;
        if n > data_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("replace: n ({n}) exceeds payload size ({data_size})"),
            ));
        }
        let locked = self.locked.load(Ordering::Acquire);
        let new_tail_start = data_size - n;
        if new_tail_start < locked {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("replace: operation would modify locked region [0, {locked})"),
            ));
        }
        let tail_offset = HEADER_SIZE + new_tail_start;
        file.seek(SeekFrom::Start(tail_offset))?;
        let mut old_tail = vec![0u8; n as usize];
        file.read_exact(&mut old_tail)?;
        let new_tail = f(&old_tail);
        let new_tail_len = new_tail.len() as u64;
        let final_data_len = new_tail_start + new_tail_len;

        if new_tail_len > n {
            // Net extension: extend first, write new tail, sync, commit.
            let new_file_end = HEADER_SIZE + final_data_len;
            file.set_len(new_file_end)?;
            file.seek(SeekFrom::Start(tail_offset))?;
            if let Err(e) = file.write_all(&new_tail) {
                let _ = file.set_len(file_end);
                return Err(e);
            }
            if let Err(e) = durable_sync(&file) {
                let _ = file.set_len(file_end);
                return Err(e);
            }
            write_committed_len(&mut file, final_data_len)?;
        } else {
            // Net truncation or same size: write new tail, truncate, sync, commit.
            if !new_tail.is_empty() {
                file.seek(SeekFrom::Start(tail_offset))?;
                file.write_all(&new_tail)?;
            }
            file.set_len(HEADER_SIZE + final_data_len)?;
            durable_sync(&file)?;
            write_committed_len(&mut file, final_data_len)?;
        }
        Ok(())
    }
}

#[cfg(all(feature = "set", feature = "atomic"))]
impl BStack {
    /// Atomically read `buf.len()` bytes at `offset` and overwrite them with
    /// `buf`, returning the old contents.
    ///
    /// Both the read and the write happen under the same write lock, so no
    /// other thread can observe either the pre-swap or mid-swap state.  The
    /// file size is never changed.
    ///
    /// An empty `buf` is a valid no-op and returns an empty `Vec`.
    ///
    /// # Feature flags
    ///
    /// Only available when both the `set` and `atomic` Cargo features are
    /// enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `offset + buf.len()`
    /// overflows `u64` or exceeds the current payload size.  Propagates any
    /// I/O error from `read_exact`, `write_all`, or `durable_sync`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn swap(&self, offset: u64, buf: impl AsRef<[u8]>) -> io::Result<Vec<u8>> {
        let buf = buf.as_ref();
        if buf.is_empty() {
            return Ok(Vec::new());
        }
        let end = offset.checked_add(buf.len() as u64).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "swap: offset + len overflows u64",
            )
        })?;
        let mut file = self.lock.write().unwrap();
        // Load `locked` under the write lock (see `set` for rationale).
        let locked = self.locked.load(Ordering::Acquire);
        if offset < locked {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("swap: range [{offset}, {end}) overlaps locked region [0, {locked})"),
            ));
        }
        let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
        if end > data_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("swap: range [{offset}, {end}) exceeds payload size ({data_size})"),
            ));
        }
        file.seek(SeekFrom::Start(HEADER_SIZE + offset))?;
        let mut old = vec![0u8; buf.len()];
        file.read_exact(&mut old)?;
        file.seek(SeekFrom::Start(HEADER_SIZE + offset))?;
        file.write_all(buf)?;
        durable_sync(&file)?;
        Ok(old)
    }

    /// Atomically read `buf.len()` bytes at `offset` into `buf` while writing
    /// the original contents of `buf` into that position.
    ///
    /// On return, `buf` contains the bytes that were previously at `offset`,
    /// and the file contains what `buf` held on entry.  Buffer-reuse
    /// counterpart of [`swap`](Self::swap).
    ///
    /// An empty `buf` is a valid no-op.
    ///
    /// # Feature flags
    ///
    /// Only available when both the `set` and `atomic` Cargo features are
    /// enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `offset + buf.len()`
    /// overflows `u64` or exceeds the current payload size.  Propagates any
    /// I/O error from `read_exact`, `write_all`, or `durable_sync`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn swap_into(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let end = offset.checked_add(buf.len() as u64).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "swap_into: offset + len overflows u64",
            )
        })?;
        let mut file = self.lock.write().unwrap();
        // Load `locked` under the write lock (see `set` for rationale).
        let locked = self.locked.load(Ordering::Acquire);
        if offset < locked {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("swap_into: range [{offset}, {end}) overlaps locked region [0, {locked})"),
            ));
        }
        let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
        if end > data_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("swap_into: range [{offset}, {end}) exceeds payload size ({data_size})"),
            ));
        }
        file.seek(SeekFrom::Start(HEADER_SIZE + offset))?;
        let mut tmp = vec![0u8; buf.len()];
        file.read_exact(&mut tmp)?;
        file.seek(SeekFrom::Start(HEADER_SIZE + offset))?;
        file.write_all(buf)?;
        durable_sync(&file)?;
        buf.copy_from_slice(&tmp);
        Ok(())
    }

    /// Compare-and-exchange: read `old.len()` bytes at `offset` and, if they
    /// equal `old`, overwrite them with `new`.
    ///
    /// Returns `Ok(true)` if the comparison succeeded and the exchange was
    /// performed.  Returns `Ok(false)` without modifying the file if
    /// `old.len() != new.len()` or if the current bytes do not match `old`.
    ///
    /// Both the compare and the exchange happen under the same write lock.
    ///
    /// # Feature flags
    ///
    /// Only available when both the `set` and `atomic` Cargo features are
    /// enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `offset + old.len()`
    /// overflows `u64` or exceeds the current payload size.  Propagates any
    /// I/O error from `read_exact`, `write_all`, or `durable_sync`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn cas(
        &self,
        offset: u64,
        old: impl AsRef<[u8]>,
        new: impl AsRef<[u8]>,
    ) -> io::Result<bool> {
        let old = old.as_ref();
        let new = new.as_ref();
        if old.len() != new.len() {
            return Ok(false);
        }
        if old.is_empty() {
            return Ok(true);
        }
        let end = offset.checked_add(old.len() as u64).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "cas: offset + len overflows u64",
            )
        })?;
        let mut file = self.lock.write().unwrap();
        // Load `locked` under the write lock (see `set` for rationale).
        let locked = self.locked.load(Ordering::Acquire);
        if offset < locked {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("cas: range [{offset}, {end}) overlaps locked region [0, {locked})"),
            ));
        }
        let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
        if end > data_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("cas: range [{offset}, {end}) exceeds payload size ({data_size})"),
            ));
        }
        file.seek(SeekFrom::Start(HEADER_SIZE + offset))?;
        let mut current = vec![0u8; old.len()];
        file.read_exact(&mut current)?;
        if current != old {
            return Ok(false);
        }
        file.seek(SeekFrom::Start(HEADER_SIZE + offset))?;
        file.write_all(new)?;
        durable_sync(&file)?;
        Ok(true)
    }

    /// Read bytes in the half-open logical range `[start, end)`, pass them to
    /// a callback that may mutate them in place, then write the modified bytes
    /// back.
    ///
    /// The read, callback invocation, and write all happen under the same write
    /// lock, so no other thread can observe an intermediate state.  The file
    /// size is never changed.
    ///
    /// `start == end` is a valid no-op: `f` is called with an empty slice and
    /// no I/O is performed beyond the initial size check.
    ///
    /// # Feature flags
    ///
    /// Only available when both the `set` and `atomic` Cargo features are
    /// enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `end < start` or if `end`
    /// exceeds the current payload size.  Propagates any I/O error from
    /// `read_exact`, `write_all`, or `durable_sync`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn process<F>(&self, start: u64, end: u64, f: F) -> io::Result<()>
    where
        F: FnOnce(&mut [u8]),
    {
        if end < start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("process: end ({end}) < start ({start})"),
            ));
        }
        let n = end - start;
        let mut file = self.lock.write().unwrap();
        let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
        if end > data_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("process: end ({end}) exceeds payload size ({data_size})"),
            ));
        }
        let locked = self.locked.load(Ordering::Acquire);
        if start < locked {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("process: range [{start}, {end}) overlaps locked region [0, {locked})"),
            ));
        }
        let mut buf = vec![0u8; n as usize];
        if n > 0 {
            file.seek(SeekFrom::Start(HEADER_SIZE + start))?;
            file.read_exact(&mut buf)?;
        }
        f(&mut buf);
        if n > 0 {
            file.seek(SeekFrom::Start(HEADER_SIZE + start))?;
            file.write_all(&buf)?;
            durable_sync(&file)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------

impl BStack {
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

    /// Returns the current locked length.  `0` means no bytes are locked.
    ///
    /// The locked region is `[0, locked_len())`.  All bytes within this range
    /// are permanently immutable: writes and shrink operations that would
    /// touch them return [`io::ErrorKind::InvalidInput`], and reads to ranges
    /// entirely within it skip the rwlock on Unix and Windows.
    pub fn locked_len(&self) -> u64 {
        self.locked.load(Ordering::Acquire)
    }

    /// Extend the locked region to cover `[0, n)`.
    ///
    /// `n` must be ≥ the current locked length and ≤ the current payload
    /// length.  After this call, reads to `[0, n)` are lock-free on Unix and
    /// Windows, and all write and shrink operations that would touch `[0, n)`
    /// return [`io::ErrorKind::InvalidInput`].
    ///
    /// Acquires the exclusive write lock to ensure all in-flight writes to
    /// `[0, n)` have completed before the region is declared immutable.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `n` is less than the current
    /// locked length (partition can only grow) or if `n` exceeds the current
    /// payload length.
    pub fn lock_up_to(&self, n: u64) -> io::Result<()> {
        // Acquire the write lock to serialise against any in-flight writers.
        let file = self.lock.write().unwrap();
        let data_size = file.metadata()?.len().saturating_sub(HEADER_SIZE);
        let current_locked = self.locked.load(Ordering::Relaxed);
        if n < current_locked {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "lock_up_to: n ({n}) is less than the current locked length ({current_locked})"
                ),
            ));
        }
        if n > data_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("lock_up_to: n ({n}) exceeds payload size ({data_size})"),
            ));
        }
        // Release store: all writes completed under the write lock above are
        // visible to any thread that subsequently loads `locked` with Acquire.
        self.locked.store(n, Ordering::Release);
        drop(file);
        Ok(())
    }

    /// Open a `BStack` and immediately lock the first `n` bytes.
    ///
    /// Equivalent to [`BStack::open`] followed by [`lock_up_to`](Self::lock_up_to),
    /// but expressed as a single call for the common pattern where the locked
    /// region is known ahead of time (e.g. a fixed-size metadata block whose
    /// size is a compile-time or configuration constant).
    ///
    /// # Errors
    ///
    /// Propagates all errors from [`open`](Self::open).  Returns
    /// [`io::ErrorKind::InvalidInput`] if `n` exceeds the payload length of
    /// the opened file.
    pub fn open_locked_up_to(path: impl AsRef<Path>, n: u64) -> io::Result<Self> {
        let stack = Self::open(path)?;
        stack.lock_up_to(n)?;
        Ok(stack)
    }
}

// ---------------------------------------------------------------------------
// io::Write

/// Appends bytes to the stack.
///
/// Each call to [`write`](io::Write::write) is equivalent to [`push`](BStack::push):
/// all bytes are written atomically and durably synced before returning.
/// Calling `write_all` or chaining multiple `write` calls therefore issues
/// one `durable_sync` per call — callers that need to batch many small writes
/// without per-write syncs should accumulate data and call `push` directly.
///
/// [`flush`](io::Write::flush) is a no-op because every `write` is already
/// durable.
impl io::Write for BStack {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.push(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Shared-reference counterpart of `impl Write for BStack`.
///
/// Because [`push`](BStack::push) takes `&self` (interior mutability via
/// `RwLock`), the `Write` implementation is also available on `&BStack`,
/// mirroring the standard library's `impl Write for &File`.
impl io::Write for &BStack {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.push(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl fmt::Debug for BStack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BStack")
            .field(
                "version",
                &format!("{}.{}.{}", MAGIC[4], MAGIC[5], MAGIC[6]),
            )
            .field("len", &self.len().ok())
            .finish_non_exhaustive()
    }
}

impl Eq for BStack {}

/// Two `BStack` instances are equal iff they are the **same instance** in memory.
///
/// Because [`BStack::open`] acquires an exclusive advisory lock, no two
/// `BStack` values within one process can refer to the same file at the same
/// time.  Pointer identity is therefore the only meaningful equality: a stack
/// is equal to itself and to nothing else.
impl PartialEq for BStack {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

/// Hashes the instance address, consistent with the pointer-identity [`PartialEq`].
impl Hash for BStack {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self as *const BStack).hash(state);
    }
}

/// A cursor-based reader over a [`BStack`] payload.
///
/// `BStackReader` implements [`io::Read`] and [`io::Seek`], allowing the
/// stack's payload to be consumed through any interface that expects a
/// readable, seekable byte stream.
///
/// # Construction
///
/// ```no_run
/// use bstack::BStack;
///
/// # fn main() -> std::io::Result<()> {
/// let stack = BStack::open("log.bin")?;
/// stack.push(b"hello world")?;
///
/// // Start reading from the beginning.
/// let mut reader = stack.reader();
///
/// // Or start from an arbitrary offset.
/// let mut mid = stack.reader_at(6);
/// # Ok(())
/// # }
/// ```
///
/// # Concurrency
///
/// `BStackReader` borrows the stack immutably, so multiple readers can coexist
/// and run concurrently with each other and with [`peek`](BStack::peek) /
/// [`get`](BStack::get) calls.  Concurrent [`push`](BStack::push) or
/// [`pop`](BStack::pop) operations are not blocked by an active reader, but
/// reading interleaved with writes may observe different snapshots of the
/// payload across calls — callers are responsible for synchronisation when
/// that matters.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct BStackReader<'a> {
    stack: &'a BStack,
    offset: u64,
}

impl BStack {
    /// Create a [`BStackReader`] positioned at the start of the payload.
    pub fn reader(&self) -> BStackReader<'_> {
        BStackReader {
            stack: self,
            offset: 0,
        }
    }

    /// Create a [`BStackReader`] positioned at `offset` bytes into the payload.
    ///
    /// Seeking past the current end is allowed; [`read`](io::Read::read) will
    /// return `Ok(0)` until new data is pushed past that point.
    pub fn reader_at(&self, offset: u64) -> BStackReader<'_> {
        BStackReader {
            stack: self,
            offset,
        }
    }
}

impl<'a> BStackReader<'a> {
    /// Return the current logical read offset within the payload.
    pub fn position(&self) -> u64 {
        self.offset
    }
}

impl<'a> From<&'a BStack> for BStackReader<'a> {
    fn from(stack: &'a BStack) -> Self {
        stack.reader()
    }
}

impl<'a> From<BStackReader<'a>> for &'a BStack {
    fn from(val: BStackReader<'a>) -> Self {
        val.stack
    }
}

impl<'a> PartialOrd for BStackReader<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Ordered by `BStack` instance address, then by cursor `offset`.
///
/// The address component groups all readers over the same stack together,
/// and within that group the natural read order (smaller offset first) applies.
/// This ordering is consistent with the pointer-identity [`PartialEq`].
impl<'a> Ord for BStackReader<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let self_ptr = self.stack as *const BStack as usize;
        let other_ptr = other.stack as *const BStack as usize;
        self_ptr
            .cmp(&other_ptr)
            .then(self.offset.cmp(&other.offset))
    }
}

impl<'a> io::Read for BStackReader<'a> {
    /// Read bytes from the current position into `buf`.
    ///
    /// Returns the number of bytes read, which may be less than `buf.len()` if
    /// the end of the payload is reached.  Returns `Ok(0)` at EOF.
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let data_size = self.stack.len()?;
        if self.offset >= data_size {
            return Ok(0);
        }
        let available = (data_size - self.offset) as usize;
        let n = buf.len().min(available);
        self.stack.get_into(self.offset, &mut buf[..n])?;
        self.offset += n as u64;
        Ok(n)
    }
}

impl<'a> io::Seek for BStackReader<'a> {
    /// Move the read cursor.
    ///
    /// [`SeekFrom::Start`] and [`SeekFrom::Current`] with a non-negative delta
    /// may advance the cursor past the current end of the payload; subsequent
    /// [`read`](io::Read::read) calls will return `Ok(0)` until the payload
    /// grows past that point.  Seeking before the start of the payload returns
    /// [`io::ErrorKind::InvalidInput`].
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let data_size = self.stack.len()? as i128;
        let new_offset = match pos {
            SeekFrom::Start(n) => n as i128,
            SeekFrom::End(n) => data_size + n as i128,
            SeekFrom::Current(n) => self.offset as i128 + n as i128,
        };
        if new_offset < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek before beginning of payload",
            ));
        }
        self.offset = new_offset as u64;
        Ok(self.offset)
    }
}
