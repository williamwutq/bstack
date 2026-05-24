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
    pub fn push(&self, data: impl AsRef<[u8]>) -> io::Result<u64>;

    /// Append `n` zero bytes and durable-sync.  Returns the starting logical offset.
    /// `n = 0` is valid and a no-op on disk.
    pub fn extend(&self, n: u64) -> io::Result<u64>;

    /// Remove and return the last `n` bytes, then durable-sync.
    /// `n = 0` is valid.  Errors if `n` exceeds the current payload size.
    pub fn pop(&self, n: u64) -> io::Result<Vec<u8>>;

    /// Remove the last `buf.len()` bytes and write them into `buf`, then durable-sync.
    /// An empty buffer is a valid no-op.  Errors if `buf.len()` exceeds the current payload size.
    /// Prefer this over `pop` when a buffer is already available to avoid an extra allocation.
    pub fn pop_into(&self, buf: &mut [u8]) -> io::Result<()>;

    /// Discard the last `n` bytes without reading or returning them, then durable-sync.
    /// `n = 0` is valid and is a no-op.  Errors if `n` exceeds the current payload size.
    /// Prefer this over `pop` when the removed bytes are not needed, to avoid any allocation or copy.
    pub fn discard(&self, n: u64) -> io::Result<()>;

    /// Overwrite `data` bytes in place starting at logical `offset`.
    /// Never changes the file size; errors if the write would exceed the
    /// current payload.  Requires the `set` feature.
    #[cfg(feature = "set")]
    pub fn set(&self, offset: u64, data: impl AsRef<[u8]>) -> io::Result<()>;

    /// Overwrite `n` bytes with zeros in place starting at logical `offset`.
    /// Never changes the file size; errors if the write would exceed the
    /// current payload.  `n = 0` is a no-op.  Requires the `set` feature.
    #[cfg(feature = "set")]
    pub fn zero(&self, offset: u64, n: u64) -> io::Result<()>;

    /// Atomically cut `n` bytes off the tail then append `buf`.
    /// Combines discard + push under a single write lock.  Requires the `atomic` feature.
    #[cfg(feature = "atomic")]
    pub fn atrunc(&self, n: u64, buf: impl AsRef<[u8]>) -> io::Result<()>;

    /// Pop `n` bytes off the tail then append `buf`; returns the removed bytes.
    /// Requires the `atomic` feature.
    #[cfg(feature = "atomic")]
    pub fn splice(&self, n: u64, buf: impl AsRef<[u8]>) -> io::Result<Vec<u8>>;

    /// Pop `old.len()` bytes into `old` then append `new`.
    /// Buffer-reuse variant of `splice`.  Requires the `atomic` feature.
    #[cfg(feature = "atomic")]
    pub fn splice_into(&self, old: &mut [u8], new: impl AsRef<[u8]>) -> io::Result<()>;

    /// Append `buf` only if the current payload size equals `s`; returns whether it did.
    /// Requires the `atomic` feature.
    #[cfg(feature = "atomic")]
    pub fn try_extend(&self, s: u64, buf: impl AsRef<[u8]>) -> io::Result<bool>;

    /// Discard `n` bytes only if the current payload size equals `s`; returns whether it did.
    /// Requires the `atomic` feature.
    #[cfg(feature = "atomic")]
    pub fn try_discard(&self, s: u64, n: u64) -> io::Result<bool>;

    /// Atomically read `buf.len()` bytes at `offset` and overwrite them with `buf`;
    /// returns the old contents.  Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn swap(&self, offset: u64, buf: impl AsRef<[u8]>) -> io::Result<Vec<u8>>;

    /// Atomic swap via a caller-supplied buffer: on return `buf` holds the old bytes.
    /// Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn swap_into(&self, offset: u64, buf: &mut [u8]) -> io::Result<()>;

    /// Compare-and-exchange: if the bytes at `offset` match `old`, overwrite with `new`.
    /// Returns `true` if the exchange was performed.  Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn cas(&self, offset: u64, old: impl AsRef<[u8]>, new: impl AsRef<[u8]>) -> io::Result<bool>;

    /// Read the tail `n` bytes, pass them to `f`, write back whatever `f` returns as the new tail.
    /// The file may grow or shrink.  `n = 0` is valid.  Requires the `atomic` feature.
    #[cfg(feature = "atomic")]
    pub fn replace<F>(&self, n: u64, f: F) -> io::Result<()>
    where F: FnOnce(&[u8]) -> Vec<u8>;

    /// Read `[start, end)`, pass the bytes to `f` for in-place mutation, write them back.
    /// File size never changes.  `start == end` is a valid no-op.
    /// Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn process<F>(&self, start: u64, end: u64, f: F) -> io::Result<()>
    where F: FnOnce(&mut [u8]);

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

    /// Current locked length.  `0` means no bytes are locked.
    /// Bytes in `[0, locked_len())` are permanently immutable for the lifetime of this open file.
    pub fn locked_len(&self) -> u64;

    /// Extend the locked region to cover `[0, n)`.  Monotonically growing: `n` must be
    /// ≥ the current locked length and ≤ the current payload length.  After this call,
    /// reads to `[0, n)` are lock-free on Unix and Windows, and writes/shrinks that
    /// would touch `[0, n)` return `InvalidInput`.
    pub fn lock_up_to(&self, n: u64) -> io::Result<()>;

    /// Open a `BStack` and immediately lock the first `n` bytes.
    /// Convenience for the common pattern where the locked region is known up front.
    pub fn open_locked_up_to(path: impl AsRef<Path>, n: u64) -> io::Result<Self>;

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

## Locked region (`lock_up_to`)

`BStack` maintains an in-memory **monotonically growing partition boundary**
called the *locked region*.  Bytes in `[0, locked_len())` are declared
permanently immutable for the lifetime of the open file.

The locked length starts at `0` on every `open` and is **not persisted to
disk** — the file format is unchanged.  Callers extend the boundary by
calling `lock_up_to` (or open and lock in one step with
`open_locked_up_to`).  It can only grow; attempts to shrink it return
`InvalidInput`.

Opening with `open_cached` (or `open_locked_up_to_cached`) enables an
in-memory mirror of the locked region: each `lock_up_to` call reads the
newly locked bytes from disk into a heap buffer, and subsequent reads whose
range falls entirely within the cached region are served with no syscall.
The trade-off is that `lock_up_to` becomes significantly more expensive on
cached stacks (it must read up to `n` bytes from disk before returning).

### What changes when bytes are locked

* **`get`/`get_into` fast-path reads.**  Calls whose range lies entirely
  within the locked region bypass the internal `RwLock`.
  - On **non-cached** stacks (Unix/Windows), the read is lock-free and uses
    `pread(2)` (Unix) or `ReadFile` + `OVERLAPPED` (Windows).
  - On **cached** stacks (all platforms), the read is served from the
    in-memory buffer under a `Mutex` (so RwLock-free, but not lock-free).
  The locked length remains a sufficient upper bound, so no extra payload-size
  check is needed on this path.
* **Write protection.**  `set`, `zero`, `swap`, `swap_into`, `cas`,
  `process`, `atrunc`, `splice`, `splice_into`, and `replace` return
  `InvalidInput` if their target overlaps the locked region.
* **Shrink protection.**  `pop`, `pop_into`, `discard`, and `try_discard`
  return `InvalidInput` if they would shrink the payload below the locked
  length.

Callers that never call `lock_up_to` see no behavioural change — every
read and write path adds only an uncontended atomic load and a comparison.

### Example

```rust
use bstack::BStack;

// 64-byte metadata header read by many threads, never modified after first write.
let stack = BStack::open_locked_up_to("meta.bin", 64)?;
assert_eq!(stack.locked_len(), 64);

// Reads of the metadata bypass the rwlock on Unix and Windows.
let header = stack.get(0, 64)?;

// Writes into the locked region are rejected.
assert!(stack.pop(stack.len()? - 60).is_err()); // would shrink below locked
```

### Concurrency

`lock_up_to(n)` takes the write lock and publishes the new boundary with a `Release` store. Locked-region fast-path readers `Acquire`-load the boundary; a stale load safely falls through to the rwlock path. Writers re-check under the write lock and cannot race against an in-flight `lock_up_to`. On cached stacks this fast path is available on all platforms via the cache `Mutex`; on non-cached stacks the lock-free path is Unix/Windows-only.

---

## Trait implementations

### `BStack`

| Trait              | Semantics                                                                                                                                             |
|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| `PartialEq` / `Eq` | **Pointer identity.** Two values are equal iff they are the same instance. No two distinct `BStack` values in one process can refer to the same file. |
| `Hash`             | Hashes the instance address — consistent with pointer-identity equality.                                                                              |

### `BStackReader`

| Trait | Semantics |
|-------|-----------|
| `PartialEq` / `Eq` | Equal when both the `BStack` pointer and the cursor offset match. |
| `Hash` | Hashes `(BStack pointer, offset)`. |
| `PartialOrd` / `Ord` | Ordered by `BStack` instance address, then by cursor offset. |

### `BStackSlice` (`alloc` feature)

| Trait                            | Semantics                                                                                               |
|----------------------------------|---------------------------------------------------------------------------------------------------------|
| `PartialEq` / `Eq`               | Compares `(offset, len)`. The allocator reference is **not** compared.                                  |
| `Hash`                           | Hashes `(offset, len)`.                                                                                 |
| `PartialOrd` / `Ord`             | Ordered by `offset`, then `len`.                                                                        |
| `From<BStackSlice> for [u8; 16]` | Serialises to `[offset_le8 ‖ len_le8]` for on-disk storage. Reconstruct with `unsafe { BStackSlice::from_bytes(...) }` — caller must ensure the bytes encode a valid range within the payload. |

### `BStackSliceReader` and `BStackSliceWriter` (`alloc` / `alloc + set` features)

| Trait                | Semantics                                                                            |
|----------------------|--------------------------------------------------------------------------------------|
| `PartialEq` / `Eq`   | Equal when the underlying slice (`offset` + `len`) and cursor position both match.   |
| `Hash`               | Hashes `(slice, cursor)`.                                                            |
| `PartialOrd` / `Ord` | Ordered by absolute payload position (`slice.start() + cursor`), then `slice.len()`. |

Reader and writer are also **cross-comparable**: `PartialEq` and `PartialOrd` are defined between
`BStackSliceReader` and `BStackSliceWriter` using the same `(abs_pos, len)` key, so the two cursor
types can be mixed in sorted collections. Both also implement `PartialEq<BStackSlice>` (cursor
position is ignored for that comparison).

---

## Feature flags

### `atomic`

Enables compound read-modify-write operations that hold the write lock across
what would otherwise be separate calls, providing thread-level atomicity and
crash-safe ordering.

```toml
[dependencies]
bstack = { version = "0.2", features = ["atomic"] }
# Combined set + atomic unlocks swap, swap_into, and cas:
bstack = { version = "0.2", features = ["set", "atomic"] }
```

- **`atrunc(n, buf)`** — Cut `n` bytes off the tail, then append `buf`. Equivalent to `discard(n)` + `push(buf)` with no intermediate visible state.
- **`splice(n, buf)`** — Remove and return the last `n` bytes, then append `buf`. Equivalent to `pop(n)` + `push(buf)` but atomically.
- **`splice_into(old, new)`** — Like `splice` but reads removed bytes into a caller-supplied buffer instead of allocating a `Vec` (`n = old.len()`).
- **`try_extend(s, buf)`** — Append `buf` only if the current payload size equals `s`. Returns `true` on success. Useful for optimistic-append protocols.
- **`try_discard(s, n)`** — Discard `n` bytes only if the current payload size equals `s`. Returns `true` on success.
- **`replace(n, f)`** — Pop `n` bytes, pass them read-only to callback `f`, write back the returned `Vec` as the new tail. File may grow or shrink.
- **`swap(offset, buf)`** *(requires `set`)* — Read `buf.len()` bytes at `offset`, overwrite with `buf`, return old bytes. File size never changes.
- **`swap_into(offset, buf)`** *(requires `set`)* — Like `swap` but exchanges in-place: on entry `buf` holds new bytes, on return holds old bytes.
- **`cas(offset, old, new)`** *(requires `set`)* — Overwrite bytes at `offset` with `new` only if they currently equal `old`. Returns `true` if exchanged. File size never changes.
- **`process(start, end, f)`** *(requires `set`)* — Read `[start, end)`, pass to `f` as `&mut [u8]` for in-place mutation, write back. File size never changes. `start == end` is a no-op.

---

### `set`

Enables `BStack::set(offset, data)` (in-place overwrite) and `BStack::zero(offset, n)` (zero-fill in place). Neither changes the file size or the committed-length header.

```toml
[dependencies]
bstack = { version = "0.2", features = ["set"] }
```

### `alloc`

Enables the region-management layer on top of `BStack`: `BStackAllocator`, `BStackBulkAllocator`, `BStackSlice`, `BStackSliceReader`, `LinearBStackAllocator`, `FirstFitBStackAllocator`, `GhostTreeBstackAllocator`, `ManualAllocator`, and the `BStackSliceAllocator` supertrait.  Combined with `set`, also enables `BStackSliceWriter`, `BStackByteVec`, and `BStackByteVecIter`.

```toml
[dependencies]
bstack = { version = "0.2", features = ["alloc"] }
# In-place slice writes (BStackSliceWriter) also need `set`:
bstack = { version = "0.2", features = ["alloc", "set"] }
```

---

## Allocator (`alloc` feature)

The `alloc` feature adds typed region management over a `BStack` payload.

### `BStackAllocator` trait

A trait for types that own a `BStack` and manage contiguous byte regions
within its payload.  Implementors must provide:

```rust
pub trait BStackAllocator: Sized {
    // All allocators in this library set Error = io::Error.
    type Error: fmt::Debug + fmt::Display;

    fn stack(&self) -> &BStack;
    fn into_stack(self) -> BStack;
    fn alloc(&self, len: u64) -> Result<BStackSlice<'_, Self>, Self::Error>;
    fn realloc<'a>(&'a self, slice: BStackSlice<'a, Self>, new_len: u64)
        -> Result<BStackSlice<'a, Self>, Self::Error>;

    // Default no-op; override for free-list allocators:
    fn dealloc(&self, slice: BStackSlice<'_, Self>) -> Result<(), Self::Error> { Ok(()) }

    // Delegation helpers:
    fn len(&self) -> io::Result<u64>;
    fn is_empty(&self) -> io::Result<bool>;
}
```

For batch allocation and deallocation, see [`BStackBulkAllocator`](#bstackbulkallocator-trait).

### `BStackBulkAllocator` trait

An extension trait for `BStackAllocator` that adds two atomic bulk methods. "Atomic" means either the operation succeeds completely or the backing store is left entirely unchanged. After a crash, the on-disk state is always consistent or recovery succeeds before the next user operation.

```rust
pub trait BStackBulkAllocator: BStackAllocator {
    /// Allocate one slice per entry in `lengths` in a single atomic operation.
    fn alloc_bulk(&self, lengths: impl AsRef<[u64]>)
        -> io::Result<Vec<Self::Allocated<'_>>>;

    /// Deallocate all supplied slices in a single atomic operation.
    fn dealloc_bulk<'a>(&'a self, slices: impl AsRef<[Self::Allocated<'a>]>)
        -> io::Result<()>;
}
```

### `BStackSlice<'a, A>`

A lightweight `Copy` handle — one `&'a A` reference plus two `u64` fields
(`offset`, `len`) — to a contiguous region of the allocator's `BStack`.
Produced by `BStackAllocator::alloc`; consumed by `realloc` and `dealloc`.

> **Slice origin requirement.** `realloc` and `dealloc` are only guaranteed to
> work correctly with a slice returned directly by `alloc` or a prior `realloc`
> on the **same allocator instance**.  Passing a sub-slice (`subslice`,
> `subslice_range`) or a manually reconstructed slice may silently corrupt
> allocator metadata.  To preserve a handle across a file reopen, serialise the
> raw `(start, len)` fields and reconstruct via
> `unsafe { BStackSlice::from_raw_parts(...) }` for read/write I/O only —
> never pass a reconstructed slice to `realloc` or `dealloc`.

Key methods:

| Method                                       | Description                                    |
|----------------------------------------------|------------------------------------------------|
| `read()`                                     | Read the entire region into a new `Vec<u8>`    |
| `read_into(buf)`                             | Read into a caller-supplied buffer             |
| `read_range(start, end)`                     | Read a sub-range into a new `Vec<u8>`          |
| `read_range_into(start, buf)`                | Read a sub-range into a caller-supplied buffer |
| `subslice(start, end)`                       | Narrow to a sub-range (relative offsets)       |
| `subslice_range(range)`                      | Narrow to a sub-range using a `Range<u64>`     |
| `reader()`                                   | Cursor-based `BStackSliceReader` at position 0 |
| `reader_at(offset)`                          | Cursor-based `BStackSliceReader` at `offset`   |
| `write(data)` *(feature `set`)*              | Overwrite the beginning of the region in place |
| `write_range(start, data)` *(feature `set`)* | Overwrite a sub-range in place                 |
| `zero()` *(feature `set`)*                   | Zero the entire region in place                |
| `zero_range(start, n)` *(feature `set`)*     | Zero a sub-range in place                      |

### `BStackSliceReader<'a, A>`

A cursor-based reader over a `BStackSlice`.  Implements `io::Read` and
`io::Seek` within the slice's coordinate space (position 0 = `slice.start()`).
Constructed via `BStackSlice::reader()` or `BStackSlice::reader_at(offset)`.

### `LinearBStackAllocator`

The reference bump allocator.  Regions are appended sequentially to the tail.

| Operation              | Without `atomic`  | With `atomic`          | Crash-safe |
|------------------------|-------------------|------------------------|------------|
| `alloc`                | `BStack::extend`  | `BStack::extend`       | yes        |
| `alloc_bulk`           | `BStack::extend`  | `BStack::extend`       | yes        |
| `realloc` grow         | `BStack::extend`  | `BStack::try_extend`   | yes        |
| `realloc` shrink       | `BStack::discard` | `BStack::try_discard`  | yes        |
| `dealloc` (tail)       | `BStack::discard` | `BStack::try_discard`  | yes        |
| `dealloc` (non-tail)   | no-op             | no-op                  | yes        |
| `dealloc_bulk` (tail)  | `BStack::discard` | `BStack::try_discard`  | yes        |

`realloc` returns `io::ErrorKind::Unsupported` for non-tail slices.  With the
`atomic` feature, `realloc` also returns `Unsupported` when another thread
races to move the tail first — identical semantics to the non-tail case.

#### Thread safety

Without the `atomic` feature, `LinearBStackAllocator` is **`Send`** but **not
`Sync`**: `realloc` and `dealloc` read the tail length and modify it in two
separate steps, creating a TOCTOU race under concurrent `&self` access.

With the `atomic` feature, `LinearBStackAllocator` is **`Send + Sync`**.
`alloc` and `alloc_bulk` use `extend`, which is serialized by `BStack`'s write
lock and returns a distinct region to each caller.  `realloc` uses
`try_extend`/`try_discard` and `dealloc`/`dealloc_bulk` use `try_discard`:
each fuses the tail-length check and the modification into a single locked
step, so concurrent calls cannot corrupt each other.

### Experimental `FirstFitBStackAllocator` (`alloc + set` features)

Experimental: A persistent first-fit free-list allocator.  Freed regions are tracked on disk
in a doubly-linked intrusive free list and reused for future allocations, so
the file does not grow without bound.

```toml
[dependencies]
bstack = { version = "0.2", features = ["alloc", "set"] }
```

#### On-disk layout

The allocator occupies the entire `BStack` payload.  The first 48 payload
bytes are a header region, followed immediately by the block arena:

```
┌──────────────────────┬───────────────────────────────────────────────────┐
│  reserved (16 B)     │ allocator header (32 B)                           │
│  (custom use)        │ magic[8] | flags[4] | _reserved[4] | free_head[8] │
└──────────────────────┴───────────────────────────────────────────────────┘
^                      ^                                                   ^
payload offset 0       offset 16                                       offset 48
                                                                     (arena start)
```

Every allocation in the arena is:

```
[ BlockHeader 16 B | payload (size bytes) | BlockFooter 8 B ]
```

* **BlockHeader** — `size: u64`, `flags: u32` (bit 0 = `is_free`), `_reserved: u32`.
* **BlockFooter** — `size: u64` (mirrors the header, used for leftward coalescing).
* **Free blocks** additionally store `next_free: u64` and `prev_free: u64` in the
  first 16 bytes of their payload, forming an intrusive doubly-linked list.

The minimum allocation size is 16 bytes; all sizes are rounded up to a multiple of 8.

#### Allocation policy

`alloc` walks the free list from the head and takes the first block whose size
≥ the aligned request (**first-fit**).  If the found block is large enough to
yield a remainder of at least 16 bytes after splitting, the remainder is left
as a new free block; the allocated portion is carved from the back.  When no
free block fits, the arena is extended by pushing a new block onto the stack.

#### Coalescing

`dealloc` merges the freed block with adjacent free neighbours (right then
left).  If the merged block reaches the stack tail it is discarded immediately.
A cascade check removes any further free blocks newly exposed at the tail,
maintaining the invariant that the tail block is always allocated.

#### Crash consistency

Multi-step operations set a `recovery_needed` flag in the allocator header
before mutating the free list and clear it after all writes complete.  On the
next `FirstFitBStackAllocator::new`, if `recovery_needed` is set, a single
linear scan of the arena rebuilds the free list from the `is_free` flags in
block headers — stored pointer values are not trusted.  Any partial tail block
is also truncated.

#### Thread safety

`FirstFitBStackAllocator` is **`Send`** but **not `Sync`**.  Ownership can be
transferred to another thread, but concurrent `&self` access from multiple
threads would race on the on-disk free list without any allocator-level lock.
Each instance must be used from at most one thread at a time.

#### Example

```rust
use bstack::{BStack, BStackAllocator, FirstFitBStackAllocator};

let alloc = FirstFitBStackAllocator::new(BStack::open("data.bstack")?)?;

let a = alloc.alloc(64)?;
let b = alloc.alloc(64)?;
a.write(b"hello world")?;

alloc.dealloc(a)?;        // freed; slot available for reuse

let c = alloc.alloc(64)?; // reuses a's slot
assert_eq!(c.start(), a.start());

let stack = alloc.into_stack();
```

### `BStackByteVec<'a, A>` (`alloc + set` features)

A growable byte (`u8`) vector backed by a `BStack` allocation, mirroring the core `Vec<u8>` API.

For a general typed vector over arbitrary `Copy` types, see `PLANNED.md` — the general case requires a sound POD/byte-castable bound and is planned for a future release.

```toml
[dependencies]
bstack = { version = "0.2", features = ["alloc", "set"] }
```

#### Memory layout

```
┌──────────────────────┬──────────────────────┬────────────────────────┐
│   len  (8 B, LE u64) │   cap  (8 B, LE u64) │   elements: [u8; cap]  │
└──────────────────────┴──────────────────────┴────────────────────────┘
  byte 0                 byte 8                  byte 16
```

The header is re-read from disk on every call, so the `(len, cap)` metadata is
recoverable after a crash by reconstructing the handle from the raw block via
`BStackByteVec::from_raw_block`.

#### Key behaviour

- **Growth**: `push` reallocates to `max(cap × 2, 4)` bytes when `len == cap`. New space is zero-initialised by `BStack::extend`.
- **Readback helper**: `read_bytes` loads all logical bytes into a Rust `Vec<u8>`.
- **Zeroing on removal**: `pop` decrements `len` before zeroing the vacated slot; `truncate` writes the new `len` before zeroing removed slots in a single `BStackSlice::zero_range` call. Deallocation zeroing is delegated to the allocator.
- **Iterator**: `BStackByteVecIter` borrows the vec immutably for its lifetime (preventing concurrent mutation) and yields `io::Result<u8>` per byte, reading from disk on demand.

#### Example

```rust
use bstack::{BStack, BStackByteVec, LinearBStackAllocator};

let alloc = LinearBStackAllocator::new(BStack::open("buf.bstack")?);

let mut v: BStackByteVec<_> = BStackByteVec::new(&alloc)?;
v.push(b'A')?;
v.push(b'B')?;
v.push(b'C')?;

assert_eq!(v.len()?, 3);
assert_eq!(v.get(1)?, Some(b'B'));
assert_eq!(v.pop()?, Some(b'C'));

let all = v.read_bytes()?;
println!("{}", String::from_utf8_lossy(&all));

alloc.dealloc(v.into_raw_block())?;
```

---

### Lifetime model

`BStackSlice<'a, A>` borrows the **allocator** for `'a`, not the `BStack`
directly.  This lets the borrow checker statically prevent calling
`into_stack()` — which consumes the allocator — while any slice is still alive.

### Example

```rust
use bstack::{BStack, BStackAllocator, LinearBStackAllocator};

let alloc = LinearBStackAllocator::new(BStack::open("data.bstack")?);

let slice = alloc.alloc(128)?;     // reserve 128 zero bytes
let data  = slice.read()?;         // read them back
alloc.dealloc(slice)?;             // release (tail → O(1) discard)

let stack = alloc.into_stack();    // reclaim the BStack
```

### `GhostTreeBstackAllocator` (`alloc` feature)

A pure-AVL general-purpose allocator built on top of a [`BStack`]. Free blocks
store their AVL node inline at offset 0 within the block — live allocations
carry **zero** overhead (no headers, no footers). The tree is keyed on
`(size, address)` for a strict total order. All memory is kept zeroed: the
BStack zeroes on extension, and the allocator zeroes on free.

Implements both `BStackAllocator` and `BStackBulkAllocator`.

```toml
[dependencies]
bstack = { version = "0.2", features = ["alloc"] }
```

| Operation               | Strategy                                         | Crash-safe  |
|-------------------------|--------------------------------------------------|-------------|
| `alloc`                 | best-fit from AVL tree, or `extend`              | multi-call  |
| `alloc_bulk`            | one block for the combined size, then split      | single-call |
| `realloc` same block    | in-place length update; zero gap on shrink       | multi-call  |
| `realloc` shrink (tail) | zero gap, `discard` freed tail                   | multi-call  |
| `realloc` shrink        | zero gap + freed tail, AVL insert                | multi-call  |
| `realloc` grow (tail)   | `extend` in-place — no copy                      | single-call |
| `realloc` grow          | alloc new, copy, dealloc old                     | multi-call  |
| `dealloc` (tail)        | `discard` — O(1), no AVL insert                  | single-call |
| `dealloc`               | zero block, AVL insert                           | multi-call  |
| `dealloc_bulk`          | merge adjacent slices, then tail-truncate/insert | multi-call  |

#### On-disk layout

```
┌─────────────────────────────┐  payload offset 0
│   User-reserved (32 bytes)  │
├─────────────────────────────┤  offset 32
│   Magic number (8 bytes)    │  "ALGT\x00\x01\x00\x00"
├─────────────────────────────┤  offset 40
│   AVL root pointer (8 B)    │  absolute payload offset of the root node
├─────────────────────────────┤  offset 48  ← arena start (32-byte aligned)
│   ... heap grows upward ... │
└─────────────────────────────┘
```

#### Allocation policy

`alloc` searches the AVL tree for the smallest free block that can satisfy the
request (best-fit). If no suitable block exists, the arena is extended.

`alloc_bulk` rounds each requested length up to 32 bytes individually, sums
them, and allocates one contiguous block (one AVL remove or one `extend`). The
block is sliced into per-request regions. When all slices are returned together
to `dealloc_bulk`, adjacent slices are merged and freed as a single operation
— typically one `discard` if they are at the tail.

#### Crash consistency

No write-ahead log, no checksums. A crash during `dealloc` before the AVL
insert permanently loses that block. A crash during rotation leaves the tree
imbalanced — corrected on the next `GhostTreeBstackAllocator::new`.

#### Thread safety

`GhostTreeBstackAllocator` is **`Send`** but **not `Sync`**.  Ownership can be
transferred to another thread, but concurrent `&self` access from multiple
threads would race on the on-disk AVL tree without any allocator-level lock.
Each instance must be used from at most one thread at a time.

#### Example

```rust
use bstack::{BStack, BStackAllocator, BStackBulkAllocator, GhostTreeBstackAllocator};

let alloc = GhostTreeBstackAllocator::new(BStack::open("data.bstack")?)?;

let a = alloc.alloc(64)?;
let b = alloc.alloc(64)?;
a.write(b"hello world")?;

alloc.dealloc(a)?;        // freed; slot available for reuse

let c = alloc.alloc(64)?; // may reuse a's slot

// Bulk: one block allocation, one block free when returned together.
let slices = alloc.alloc_bulk(&[32, 64, 128])?;
alloc.dealloc_bulk(&slices)?;

let stack = alloc.into_stack();
```

### `DebugCheckingAllocator` (`alloc` feature)

A debug/test wrapper around any `BStackAllocator`.  It tracks allocated and freed
regions in memory and panics immediately if it detects:

- **Overlapping allocations** — the inner allocator returned a region that overlaps
  an existing live allocation.
- **Double-frees** — a region is freed twice within the same session.
- **Partial or spanning frees** — a deallocation does not exactly match one recorded
  allocation.

The tracking state is **in-memory only** and is reset on process restart, so it
complements but does not replace the allocator's own crash-safety guarantees.

> **Warning:** Not for production use. The in-memory tracking state adds significant overhead, so this wrapper is intended only for debugging and testing. You should not rely on it to detect double-frees or overlapping allocations in production.

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
  This version writes `BSTK\x00\x01\x0b\x00` (0.1.11).  `open` accepts any
  0.1.x file (first 6 bytes `BSTK\x00\x01`) and rejects a different major or
  minor as incompatible.
* **`clen`** — little-endian `u64` recording the last successfully committed
  payload length.  Updated on every `push` and `pop` before the durable sync.

All user-visible offsets (returned by `push`, accepted by `peek`/`get`) are
**logical** — 0-based from the start of the payload region (file byte 16).

---

## Durability

| Operation                              | Sequence                                                                                  |
|----------------------------------------|-------------------------------------------------------------------------------------------|
| `push`                                 | `lseek(END)` → `write(data)` → `lseek(8)` → `write(clen)` → sync                          |
| `extend`                               | `lseek(END)` → `set_len(new_end)` → `lseek(8)` → `write(clen)` → sync                     |
| `pop`, `pop_into`                      | `lseek` → `read` → `ftruncate` → `lseek(8)` → `write(clen)` → sync                        |
| `discard`                              | `ftruncate` → `lseek(8)` → `write(clen)` → sync                                           |
| `set` *(feature)*                      | `lseek(offset)` → `write(data)` → sync                                                    |
| `zero` *(feature)*                     | `lseek(offset)` → `write(zeros)` → sync                                                   |
| `atrunc` *(atomic, net extension)*     | `set_len(new_end)` → `lseek(tail)` → `write(buf)` → sync → `write(clen)`                  |
| `atrunc` *(atomic, net truncation)*    | `lseek(tail)` → `write(buf)` → `set_len(new_end)` → sync → `write(clen)`                  |
| `splice`, `splice_into` *(atomic)*     | `lseek(tail)` → `read(n)` → *(then as `atrunc`)*                                          |
| `try_extend` *(atomic)*                | size check → conditional `push` sequence                                                  |
| `try_discard` *(atomic)*               | size check → conditional `discard` sequence                                               |
| `swap`, `swap_into` *(set+atomic)*     | `lseek(offset)` → `read` → `lseek(offset)` → `write(buf)` → sync                          |
| `cas` *(set+atomic)*                   | `lseek(offset)` → `read` → compare → conditional `write(new)` → sync                      |
| `process` *(set+atomic)*               | `lseek(start)` → `read(end−start)` → *(callback)* → `lseek(start)` → `write(buf)` → sync  |
| `replace` *(atomic)*                   | `lseek(tail)` → `read(n)` → *(callback)* → *(then as `atrunc`)*                           |
| `peek`, `peek_into`, `get`, `get_into` | `pread(2)` on Unix; `ReadFile`+`OVERLAPPED` on Windows; `lseek` → `read` elsewhere        |

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

| Operation                                                    | Lock (Unix / Windows) | Lock (other) |
|--------------------------------------------------------------|-----------------------|--------------|
| `push`, `extend`, `pop`, `pop_into`, `discard`               | write                 | write        |
| `set`, `zero` *(feature)*                                    | write                 | write        |
| `atrunc`, `splice`, `splice_into`, `try_extend` *(atomic)*   | write                 | write        |
| `try_discard(s, n > 0)` *(atomic)*                           | write                 | write        |
| `try_discard(s, 0)` *(atomic)*                               | **read**              | **read**     |
| `swap`, `swap_into`, `cas` *(set+atomic)*                    | write                 | write        |
| `process` *(set+atomic)*                                     | write                 | write        |
| `replace` *(atomic)*                                         | write                 | write        |
| `peek`, `peek_into`, `get`, `get_into`                       | **read**              | write        |
| `len`                                                        | read                  | read         |

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

## Why async is not planned

Durability requires `fcntl(F_FULLFSYNC)` (macOS), `fdatasync` (Linux), or `FlushFileBuffers` (Windows) — all blocking syscalls that must complete before returning. All writes are tail-ordered with a mandatory `durable_sync` barrier; there is nothing to pipeline, and a `RwLock` serialises concurrent writers regardless. Adding Tokio even as an optional dependency would break the minimal-dependency guarantee. The idiomatic approach from async code is:

```rust
let result = tokio::task::spawn_blocking(move || {
    stack.push(&data)
}).await?;
```
