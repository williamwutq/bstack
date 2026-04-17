# bstack

A persistent, fsync-durable binary stack backed by a single file.

## Features

- **Single file** — no sidecar or manifest files
- **Durable** — every `push` and `pop` calls `sync_data` before returning
- **Thread-safe** — internal `RwLock<File>` allows concurrent reads of `len`
  while serialising mutating operations
- **No external dependencies** — pure `std`

## Usage

```rust
use bstack::BStack;

fn main() -> std::io::Result<()> {
    let stack = BStack::open("my.stack")?;

    // Push two payloads; returns the byte offset each one starts at.
    let off0 = stack.push(b"hello")?;  // off0 == 0
    let off1 = stack.push(b"world")?;  // off1 == 5

    println!("file is {} bytes", stack.len()?); // 10

    // Pop the last 5 bytes (most-recently pushed item).
    let top = stack.pop(5)?;
    assert_eq!(top, b"world");

    println!("file is now {} bytes", stack.len()?); // 5

    // Pop a chunk that spans two previously pushed payloads.
    let rest = stack.pop(3)?;
    assert_eq!(rest, b"llo");

    Ok(())
}
```

## API

```rust
impl BStack {
    /// Open or create a stack file at `path`.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self>;

    /// Append `data` and fsync. Returns the starting byte offset.
    pub fn push(&self, data: &[u8]) -> io::Result<u64>;

    /// Read and remove the last `n` bytes, then fsync.
    /// Errors if `n` exceeds the current file size.
    pub fn pop(&self, n: u64) -> io::Result<Vec<u8>>;

    /// Current file size in bytes.
    pub fn len(&self) -> io::Result<u64>;
}
```

## Durability guarantees

| Operation | What is fsynced |
|-----------|-----------------|
| `push`    | `sync_data` after `write_all` — data pages only, not metadata |
| `pop`     | `sync_data` after `set_len` (ftruncate) |

`sync_data` is used instead of `sync_all` because mtime/ctime updates are not
needed for crash-recovery correctness and skipping them reduces latency.

If `push` fails partway through, the implementation attempts a best-effort
truncation back to the pre-write length before returning the error.
