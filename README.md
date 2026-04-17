# bstack

A persistent, fsync-durable binary stack backed by a single file.

`push` and `pop` call `fdatasync` before returning, so data survives a process
crash or unclean shutdown.  The file format is raw bytes with no framing or
checksums — trivially inspectable with `xxd` or `hexdump`.

**No external dependencies.  No `unsafe` code.**

---

## Quick start

```rust
use bstack::BStack;

let stack = BStack::open("log.bin")?;

// push appends bytes and returns the starting offset.
let off0 = stack.push(b"hello")?;  // 0
let off1 = stack.push(b"world")?;  // 5

assert_eq!(stack.len()?, 10);

// peek reads from an offset to the end without removing anything.
assert_eq!(stack.peek(off1)?, b"world");

// get reads an arbitrary half-open byte range.
assert_eq!(stack.get(3, 8)?, b"lowor");

// pop removes bytes from the tail and returns them.
assert_eq!(stack.pop(5)?, b"world");
assert_eq!(stack.len()?, 5);
```

---

## API

```rust
impl BStack {
    /// Open or create a stack file at `path`.  Existing data is preserved.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self>;

    /// Append `data` and fdatasync.  Returns the starting byte offset.
    /// An empty slice is valid and a no-op on disk.
    pub fn push(&self, data: &[u8]) -> io::Result<u64>;

    /// Remove and return the last `n` bytes, then fdatasync.
    /// `n = 0` is valid.  Errors if `n` exceeds the current file size.
    pub fn pop(&self, n: u64) -> io::Result<Vec<u8>>;

    /// Copy all bytes from `offset` to the end of the file.
    /// `offset == len()` returns an empty Vec.
    pub fn peek(&self, offset: u64) -> io::Result<Vec<u8>>;

    /// Copy bytes in the half-open range `[start, end)`.
    /// `start == end` returns an empty Vec.
    pub fn get(&self, start: u64, end: u64) -> io::Result<Vec<u8>>;

    /// Current file size in bytes.
    pub fn len(&self) -> io::Result<u64>;
}
```

---

## File format

```
┌──────────────┬──────────────┬──────────────┐
│  payload 0   │  payload 1   │  payload 2   │  ...
└──────────────┴──────────────┴──────────────┘
^              ^              ^              ^
offset 0    offset n0      offset n0+n1   EOF
```

Payloads are written back-to-back with no length prefixes, checksums, or
separators.  The caller owns the framing.  One common pattern is to store a
fixed-size header as the first push and use the returned offsets as an
application-level index.

---

## Durability

| Operation     | Sequence                                     |
|---------------|----------------------------------------------|
| `push`        | `lseek(END)` → `write` → `fdatasync`         |
| `pop`         | `lseek` → `read` → `ftruncate` → `fdatasync` |
| `peek`, `get` | `lseek` → `read` (no sync — read-only)       |

`fdatasync` is used instead of `fsync` because inode metadata (mtime, ctime,
block count) is not needed for crash-recovery correctness.  On most
journalling filesystems this halves the number of journal flushes per
operation.

**Push rollback:** if `write` succeeds but `fdatasync` fails, a best-effort
`ftruncate` restores the pre-push length.  If that truncation also fails the
error is swallowed and the file may contain a partial tail write; the caller
is responsible for detecting and trimming it on the next open (e.g. via a
stored length sentinel).

---

## Thread safety

`BStack` wraps the file in a `RwLock<File>`.

| Operation                    | Lock       |
|------------------------------|------------|
| `push`, `pop`, `peek`, `get` | write lock |
| `len`                        | read lock  |

`peek` and `get` take the write lock because [`Seek`] requires `&mut File`.
`len` uses `File::metadata` (no seek) and takes the read lock, so multiple
`len` calls can proceed concurrently.  All callers block while a write-lock
operation is in progress, so `len` always observes a size at a clean
operation boundary.

---

## Known limitations

- **No record framing.** The file stores raw bytes; the caller must track
  how many bytes each logical record occupies.
- **Push rollback is best-effort.** A failure during rollback is silently
  swallowed (see *Durability* above).
- **No `O_DIRECT`.** Writes go through the page cache; durability relies on
  `fdatasync`, not cache bypass.
- **Single file only.** There is no WAL, manifest, or secondary index.
