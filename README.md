# bstack

A persistent, fsync-durable binary stack backed by a single file.

`push` and `pop` perform a *durable sync* before returning, so data survives a
process crash or unclean shutdown.  On **macOS**, `fcntl(F_FULLFSYNC)` is used
instead of `fdatasync` to flush the drive's hardware write cache, which plain
`fdatasync` does not guarantee.

A 16-byte file header stores a **magic number** and a **committed-length
sentinel**.  On reopen, any mismatch between the header and the actual file
size is repaired automatically — no user intervention required.

On **Unix**, `open` acquires an **exclusive advisory `flock`**, so two
processes cannot concurrently corrupt the same stack file.

**Minimal dependencies (`libc` on Unix only).  No `unsafe` beyond required FFI calls.**

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
    /// Acquires an exclusive flock on Unix.
    /// Validates the header and performs crash recovery on existing files.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self>;

    /// Append `data` and durable-sync.  Returns the starting logical offset.
    /// An empty slice is valid and a no-op on disk.
    pub fn push(&self, data: &[u8]) -> io::Result<u64>;

    /// Remove and return the last `n` bytes, then durable-sync.
    /// `n = 0` is valid.  Errors if `n` exceeds the current payload size.
    pub fn pop(&self, n: u64) -> io::Result<Vec<u8>>;

    /// Overwrite `data` bytes in place starting at logical `offset`.
    /// Never changes the file size; errors if the write would exceed the
    /// current payload.  Requires the `set` feature.
    #[cfg(feature = "set")]
    pub fn set(&self, offset: u64, data: &[u8]) -> io::Result<()>;

    /// Copy all bytes from `offset` to the end of the payload.
    /// `offset == len()` returns an empty Vec.
    pub fn peek(&self, offset: u64) -> io::Result<Vec<u8>>;

    /// Copy bytes in the half-open logical range `[start, end)`.
    /// `start == end` returns an empty Vec.
    pub fn get(&self, start: u64, end: u64) -> io::Result<Vec<u8>>;

    /// Current payload size in bytes (excludes the 16-byte header).
    pub fn len(&self) -> io::Result<u64>;
}
```

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
  This version writes `BSTK\x00\x01\x01\x00` (0.1.1).  `open` accepts any
  0.1.x file (first 6 bytes `BSTK\x00\x01`) and rejects a different major or
  minor as incompatible.
* **`clen`** — little-endian `u64` recording the last successfully committed
  payload length.  Updated on every `push` and `pop` before the durable sync.

All user-visible offsets (returned by `push`, accepted by `peek`/`get`) are
**logical** — 0-based from the start of the payload region (file byte 16).

---

## Durability

| Operation           | Sequence                                                             |
|---------------------|----------------------------------------------------------------------|
| `push`              | `lseek(END)` → `write(data)` → `lseek(8)` → `write(clen)` → sync     |
| `pop`               | `lseek` → `read` → `ftruncate` → `lseek(8)` → `write(clen)` → sync   |
| `set` *(feature)*   | `lseek(offset)` → `write(data)` → sync                               |
| `peek`, `get`       | `pread(2)` on Unix; `lseek` → `read` elsewhere (no sync — read-only) |

**`durable_sync` on macOS** issues `fcntl(F_FULLFSYNC)`.  Unlike `fdatasync`,
this flushes the drive controller's write cache, providing the same "barrier
to stable media" guarantee that `fsync` gives on Linux.  Falls back to
`sync_data` if the device does not support `F_FULLFSYNC`.

**`durable_sync` on Linux / other Unix** calls `sync_data` (`fdatasync`).

**Push rollback:** if the write or sync fails, a best-effort `ftruncate` and
header reset restore the pre-push state.

---

## Crash recovery

The committed-length sentinel in the header ensures automatic recovery on the
next `open`:

| Condition | Cause | Recovery |
|-----------|-------|----------|
| `file_size − 16 > clen` | partial tail write (crashed before header update) | truncate to `16 + clen`, durable-sync |
| `file_size − 16 < clen` | partial truncation (crashed before header update) | set `clen = file_size − 16`, durable-sync |

No caller action is required; recovery is transparent.

---

## Multi-process safety

On Unix, `open` calls `flock(LOCK_EX | LOCK_NB)` on the file.  If another
process already holds the lock, `open` returns immediately with
`io::ErrorKind::WouldBlock`.  The lock is released when the `BStack` is
dropped.

> `flock` is advisory.  It protects against concurrent `BStack::open` calls
> across processes, not against raw file access.

---

## Thread safety

`BStack` wraps the file in a `RwLock<File>`.

| Operation             | Lock (Unix)    | Lock (non-Unix) |
|-----------------------|----------------|-----------------|
| `push`, `pop`         | write          | write           |
| `set` *(feature)*     | write          | write           |
| `peek`, `get`         | **read**       | write           |
| `len`                 | read           | read            |

On Unix, `peek` and `get` use `pread(2)` (`read_exact_at` from
`std::os::unix::fs::FileExt`), which reads at an absolute file offset without
touching the shared file-position cursor.  Multiple concurrent `peek`, `get`,
and `len` calls can therefore run in parallel.  Any in-progress `push` or
`pop` still blocks all readers via the write lock, so readers always observe a
consistent, committed state.

On non-Unix platforms a seek is required; `peek` and `get` fall back to the
write lock and reads serialise.

---

## Known limitations

- **No record framing.** The file stores raw bytes; the caller must track how
  many bytes each logical record occupies.
- **Push rollback is best-effort.** A failure during rollback is silently
  swallowed; crash recovery on the next `open` will repair the state.
- **No `O_DIRECT`.** Writes go through the page cache; durability relies on
  `durable_sync`, not cache bypass.
- **Single file only.** There is no WAL, manifest, or secondary index.
- **Multi-process lock is Unix-only.** No equivalent is implemented on Windows.
