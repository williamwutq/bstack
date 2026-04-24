# bstack

A persistent, fsync-durable binary stack backed by a single file.

`push` and `pop` perform a *durable sync* before returning, so data survives a
process crash or unclean shutdown.  On **macOS**, `fcntl(F_FULLFSYNC)` is used
instead of `fdatasync` to flush the drive's hardware write cache, which plain
`fdatasync` does not guarantee.

[![Crates.io](https://img.shields.io/crates/v/bstack)](https://crates.io/crates/bstack)
[![Docs.rs](https://img.shields.io/docsrs/bstack)](https://docs.rs/bstack)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

A 16-byte file header stores a **magic number** and a **committed-length
sentinel**.  On reopen, any mismatch between the header and the actual file
size is repaired automatically — no user intervention required.

On **Unix**, `open` acquires an **exclusive advisory `flock`**; on
**Windows**, `LockFileEx` is used instead.  Both prevent two processes from
concurrently corrupting the same stack file.

**Minimal dependencies (`libc` on Unix, `windows-sys` on Windows).  No `unsafe` beyond required FFI calls.**

> **Warning:** bstack files must only be opened through this crate or a
> compatible implementation that understands the file format, header protocol,
> and locking semantics.  Reading or writing the file with raw tools (`dd`,
> `xxd`, custom `open(2)` calls, etc.) while a `BStack` instance is live, or
> manually editing the header fields, can silently corrupt the committed-length
> sentinel or bypass the advisory lock.  **The authors make no guarantees about
> the behaviour of the crate — including freedom from data loss or logical
> corruption — when the file has been accessed outside of this crate's
> controlled interface.**

---

## Quick start

```rust
use bstack::BStack;

let stack = BStack::open("log.bin")?;

// push appends bytes and returns the starting logical offset.
let off0 = stack.push(b"hello")?;  // 0
let off1 = stack.push(b"world")?;  // 5

assert_eq!(stack.len()?, 10);

// peek reads from a logical offset to the end.
assert_eq!(stack.peek(off1)?, b"world");

// get reads an arbitrary half-open logical byte range.
assert_eq!(stack.get(3, 8)?, b"lowor");

// pop removes bytes from the tail and returns them.
assert_eq!(stack.pop(5)?, b"world");
assert_eq!(stack.len()?, 5);
```

---

## API

```rust
impl BStack {
    /// Open or create a stack file at `path`.
    /// Acquires an exclusive flock on Unix, or LockFileEx on Windows.
    /// Validates the header and performs crash recovery on existing files.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self>;

    /// Append `data` and durable-sync.  Returns the starting logical offset.
    /// An empty slice is valid and a no-op on disk.
    pub fn push(&self, data: &[u8]) -> io::Result<u64>;

    /// Remove and return the last `n` bytes, then durable-sync.
    /// `n = 0` is valid.  Errors if `n` exceeds the current payload size.
    pub fn pop(&self, n: u64) -> io::Result<Vec<u8>>;

    /// Remove the last `buf.len()` bytes and write them into `buf`, then durable-sync.
    /// An empty buffer is a valid no-op.  Errors if `buf.len()` exceeds the current payload size.
    /// Prefer this over `pop` when a buffer is already available to avoid an extra allocation.
    pub fn pop_into(&self, buf: &mut [u8]) -> io::Result<()>;

    /// Overwrite `data` bytes in place starting at logical `offset`.
    /// Never changes the file size; errors if the write would exceed the
    /// current payload.  Requires the `set` feature.
    #[cfg(feature = "set")]
    pub fn set(&self, offset: u64, data: &[u8]) -> io::Result<()>;

    /// Copy all bytes from `offset` to the end of the payload.
    /// `offset == len()` returns an empty Vec.
    pub fn peek(&self, offset: u64) -> io::Result<Vec<u8>>;

    /// Fill `buf` with exactly `buf.len()` bytes starting at logical `offset`.
    /// An empty buffer is a valid no-op.
    /// Prefer this over `peek` when a buffer is already available to avoid an extra allocation.
    pub fn peek_into(&self, offset: u64, buf: &mut [u8]) -> io::Result<()>;

    /// Copy bytes in the half-open logical range `[start, end)`.
    /// `start == end` returns an empty Vec.
    pub fn get(&self, start: u64, end: u64) -> io::Result<Vec<u8>>;

    /// Fill `buf` with bytes from the half-open logical range `[start, start + buf.len())`.
    /// An empty buffer is a valid no-op.
    /// Prefer this over `get` when a buffer is already available to avoid an extra allocation.
    pub fn get_into(&self, start: u64, buf: &mut [u8]) -> io::Result<()>;

    /// Current payload size in bytes (excludes the 16-byte header).
    pub fn len(&self) -> io::Result<u64>;

    /// Create a `BStackReader` positioned at the start of the payload.
    pub fn reader(&self) -> BStackReader<'_>;

    /// Create a `BStackReader` positioned at `offset` bytes into the payload.
    pub fn reader_at(&self, offset: u64) -> BStackReader<'_>;
}

// BStack and &BStack both implement std::io::Write (each write = one push + durable_sync).
// BStackReader implements std::io::Read + std::io::Seek + From<&BStack>.
```

---

## Standard I/O adapters

### Writing — `impl Write for BStack` / `impl Write for &BStack`

`BStack` implements [`std::io::Write`].  Each call to `write` is forwarded to
`push`, so every write is atomically appended and durably synced before
returning.  `flush` is a no-op.

`&BStack` also implements `Write` (mirroring `impl Write for &File`), which
lets you pass a shared reference wherever a writer is expected.

```rust
use std::io::Write;
use bstack::BStack;

let mut stack = BStack::open("log.bin")?;

// write / write_all forward to push.
stack.write_all(b"hello")?;
stack.write_all(b"world")?;
assert_eq!(stack.len()?, 10);

// io::copy works out of the box.
let mut src = std::io::Cursor::new(b"more data");
std::io::copy(&mut src, &mut stack)?;
```

Wrapping in `BufWriter` batches small writes into fewer `push` calls (and
fewer `durable_sync` calls):

```rust
use std::io::{BufWriter, Write};
use bstack::BStack;

let stack = BStack::open("log.bin")?;
let mut bw = BufWriter::new(&stack);
for chunk in chunks {
    bw.write_all(chunk)?;
}
bw.flush()?; // one push + one durable_sync for the whole batch
```

> **Note:** Each raw `write` call issues one `durable_sync`.  If you call
> `write` or `write_all` in a tight loop, prefer `push` directly or use
> `BufWriter` to batch.

### Reading — `BStackReader`

[`BStackReader`] wraps a `&BStack` with a cursor and implements
[`std::io::Read`] and [`std::io::Seek`].

```rust
use std::io::{Read, Seek, SeekFrom};
use bstack::{BStack, BStackReader};

let stack = BStack::open("log.bin")?;
stack.push(b"hello world")?;

// From the beginning:
let mut reader = stack.reader();

// From an arbitrary offset:
let mut mid = stack.reader_at(6);

// From<&BStack> is also implemented:
let mut r = BStackReader::from(&stack);

let mut buf = [0u8; 5];
reader.read_exact(&mut buf)?;  // b"hello"

reader.seek(SeekFrom::Start(6))?;
reader.read_exact(&mut buf)?;  // b"world"

// read_to_end, BufReader, etc. all work.
let mut out = Vec::new();
stack.reader().read_to_end(&mut out)?;
```

`BStackReader` borrows the stack immutably, so multiple readers can coexist
and run concurrently with each other and with `peek`/`get` calls.

---

## Feature flags

### `set`

`BStack::set(offset, data)` — in-place overwrite of existing payload bytes
without changing the file size or the committed-length header.

```toml
[dependencies]
bstack = { version = "0.1", features = ["set"] }
```

---

## File format

```
┌────────────────────────┬──────────────┬──────────────┐
│      header (16 B)     │  payload 0   │  payload 1   │  ...
│  magic[8] | clen[8 LE] │              │              │
└────────────────────────┴──────────────┴──────────────┘
^                        ^              ^              ^
file offset 0        offset 16       16+n0          EOF
```

* **`magic`** — 8 bytes: `BSTK` + major(1 B) + minor(1 B) + patch(1 B) + reserved(1 B).
  This version writes `BSTK\x00\x01\x02\x00` (0.1.2).  `open` accepts any
  0.1.x file (first 6 bytes `BSTK\x00\x01`) and rejects a different major or
  minor as incompatible.
* **`clen`** — little-endian `u64` recording the last successfully committed
  payload length.  Updated on every `push` and `pop` before the durable sync.

All user-visible offsets (returned by `push`, accepted by `peek`/`get`) are
**logical** — 0-based from the start of the payload region (file byte 16).

---

## Durability

| Operation                              | Sequence                                                                           |
|----------------------------------------|------------------------------------------------------------------------------------|
| `push`                                 | `lseek(END)` → `write(data)` → `lseek(8)` → `write(clen)` → sync                   |
| `pop`, `pop_into`                      | `lseek` → `read` → `ftruncate` → `lseek(8)` → `write(clen)` → sync                 |
| `set` *(feature)*                      | `lseek(offset)` → `write(data)` → sync                                             |
| `peek`, `peek_into`, `get`, `get_into` | `pread(2)` on Unix; `ReadFile`+`OVERLAPPED` on Windows; `lseek` → `read` elsewhere |

**`durable_sync` on macOS** issues `fcntl(F_FULLFSYNC)`.  Unlike `fdatasync`,
this flushes the drive controller's write cache, providing the same "barrier
to stable media" guarantee that `fsync` gives on Linux.  Falls back to
`sync_data` if the device does not support `F_FULLFSYNC`.

**`durable_sync` on Linux / other Unix** calls `sync_data` (`fdatasync`).

**`durable_sync` on Windows** calls `sync_data`, which maps to
`FlushFileBuffers`.  This flushes the kernel write-back cache and waits for
the drive to acknowledge, providing equivalent durability to `fdatasync`.

**Push rollback:** if the write or sync fails, a best-effort `ftruncate` and
header reset restore the pre-push state.

---

## Crash recovery

The committed-length sentinel in the header ensures automatic recovery on the
next `open`:

| Condition               | Cause                                             | Recovery                                  |
|-------------------------|---------------------------------------------------|-------------------------------------------|
| `file_size − 16 > clen` | partial tail write (crashed before header update) | truncate to `16 + clen`, durable-sync     |
| `file_size − 16 < clen` | partial truncation (crashed before header update) | set `clen = file_size − 16`, durable-sync |

No caller action is required; recovery is transparent.

---

## Multi-process safety

On **Unix**, `open` calls `flock(LOCK_EX | LOCK_NB)` on the file.  If another
process already holds the lock, `open` returns immediately with
`io::ErrorKind::WouldBlock`.  The lock is released when the `BStack` is
dropped.

On **Windows**, `open` calls `LockFileEx` with
`LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY` covering the entire
file range.  The same `WouldBlock` semantics apply (`ERROR_LOCK_VIOLATION`
maps to `io::ErrorKind::WouldBlock` in Rust).  The lock is released when the
`BStack` is dropped.

> Both `flock` (Unix) and `LockFileEx` (Windows) are advisory and per-process.
> They protect against concurrent `BStack::open` calls across well-behaved
> processes, not against raw file access.

---

## Thread safety

`BStack` wraps the file in a `RwLock<File>`.

| Operation                              | Lock (Unix / Windows) | Lock (other) |
|----------------------------------------|-----------------------|--------------|
| `push`, `pop`, `pop_into`              | write                 | write        |
| `set` *(feature)*                      | write                 | write        |
| `peek`, `peek_into`, `get`, `get_into` | **read**              | write        |
| `len`                                  | read                  | read         |

On Unix and Windows, `peek`, `peek_into`, `get`, and `get_into` use a
cursor-safe positional read (`pread(2)` / `read_exact_at` on Unix; `ReadFile`
with `OVERLAPPED` via `seek_read` on Windows) that does not modify the shared
file-position cursor.  Multiple concurrent calls to any of these methods can
therefore run in parallel.  Any in-progress `push`, `pop`, or `pop_into` still
blocks all readers via the write lock, so readers always observe a consistent,
committed state.

On other platforms a seek is required; `peek`, `peek_into`, `get`, and
`get_into` fall back to the write lock and reads serialise.

---

## Known limitations

- **No record framing.** The file stores raw bytes; the caller must track how
  many bytes each logical record occupies.
- **Push rollback is best-effort.** A failure during rollback is silently
  swallowed; crash recovery on the next `open` will repair the state.
- **No `O_DIRECT`.** Writes go through the page cache; durability relies on
  `durable_sync`, not cache bypass.
- **Single file only.** There is no WAL, manifest, or secondary index.
- **Multi-process lock is advisory.** `flock` (Unix) and `LockFileEx` (Windows) protect well-behaved processes but not raw file access.

---

## Why async (Tokio) integration is not planned

`bstack` is deliberately synchronous, and async support would add real cost for
no meaningful gain.  The reasons below are structural, not incidental.

### 1. Durability is an inherently blocking syscall

The entire value proposition of `bstack` is that `push` and `pop` do not return
until the data is on stable storage.  That contract is fulfilled by
`fcntl(F_FULLFSYNC)` on macOS, `fdatasync` on other Unix, and
`FlushFileBuffers` on Windows.  All three are blocking syscalls that park the
calling thread until the drive acknowledges the write.

In an async runtime, blocking syscalls must be offloaded to a thread pool via
`spawn_blocking`.  So an "async `push`" would simply be:

```rust
tokio::task::spawn_blocking(|| stack.push(data)).await
```

That is not necessary to be added as an async method on `BStack` itself, since the blocking nature of the operation is already clear from the API and documentation. The above pattern is idiomatic for using blocking operations in a Tokio application.

### 2. No I/O concurrency to exploit

Async I/O improves throughput when multiple independent operations can be
in-flight simultaneously.  `bstack` cannot do this:

- **Writes are ordered.** Each `push` extends the file at the tail and updates
  the committed-length header.  Reordering or interleaving writes would corrupt
  the header or produce a torn committed length.
- **Every write ends with a barrier.** `durable_sync` must complete before the
  next operation starts; there is nothing to pipeline.
- **Operations are serialised by a `RwLock`.** Concurrent writes already block
  on each other.  Wrapping that in `async` would only add overhead.

### 3. The file lock is blocking and must not run on an async thread

`open` calls `flock(LOCK_EX | LOCK_NB)` on Unix or `LockFileEx` on Windows.
Blocking on a mutex inside an async executor stalls the thread and starves
other tasks on the same worker.  Moving the lock acquisition to
`spawn_blocking` is again just synchronous I/O on a thread pool.

### 4. Tokio would break the minimal-dependencies guarantee

`bstack` currently depends only on `libc` (Unix) and `windows-sys` (Windows).
Pulling in `tokio` — even as an optional dependency — would introduce a large
transitive dependency tree that affects every user of the crate, including the
many users who do not use an async runtime at all.

### What to do in async code

If you are using `bstack` from inside a Tokio application, the idiomatic
approach is:

```rust
let result = tokio::task::spawn_blocking(move || {
    stack.push(&data)
}).await?;
```
