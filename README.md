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
    pub fn set(&self, offset: u64, data: &[u8]) -> io::Result<()>;

    /// Overwrite `n` bytes with zeros in place starting at logical `offset`.
    /// Never changes the file size; errors if the write would exceed the
    /// current payload.  `n = 0` is a no-op.  Requires the `set` feature.
    #[cfg(feature = "set")]
    pub fn zero(&self, offset: u64, n: u64) -> io::Result<()>;

    /// Atomically cut `n` bytes off the tail then append `buf`.
    /// Combines discard + push under a single write lock.  Requires the `atomic` feature.
    #[cfg(feature = "atomic")]
    pub fn atrunc(&self, n: u64, buf: &[u8]) -> io::Result<()>;

    /// Pop `n` bytes off the tail then append `buf`; returns the removed bytes.
    /// Requires the `atomic` feature.
    #[cfg(feature = "atomic")]
    pub fn splice(&self, n: u64, buf: &[u8]) -> io::Result<Vec<u8>>;

    /// Pop `old.len()` bytes into `old` then append `new`.
    /// Buffer-reuse variant of `splice`.  Requires the `atomic` feature.
    #[cfg(feature = "atomic")]
    pub fn splice_into(&self, old: &mut [u8], new: &[u8]) -> io::Result<()>;

    /// Append `buf` only if the current payload size equals `s`; returns whether it did.
    /// Requires the `atomic` feature.
    #[cfg(feature = "atomic")]
    pub fn try_extend(&self, s: u64, buf: &[u8]) -> io::Result<bool>;

    /// Discard `n` bytes only if the current payload size equals `s`; returns whether it did.
    /// Requires the `atomic` feature.
    #[cfg(feature = "atomic")]
    pub fn try_discard(&self, s: u64, n: u64) -> io::Result<bool>;

    /// Atomically read `buf.len()` bytes at `offset` and overwrite them with `buf`;
    /// returns the old contents.  Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn swap(&self, offset: u64, buf: &[u8]) -> io::Result<Vec<u8>>;

    /// Atomic swap via a caller-supplied buffer: on return `buf` holds the old bytes.
    /// Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn swap_into(&self, offset: u64, buf: &mut [u8]) -> io::Result<()>;

    /// Compare-and-exchange: if the bytes at `offset` match `old`, overwrite with `new`.
    /// Returns `true` if the exchange was performed.  Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn cas(&self, offset: u64, old: &[u8], new: &[u8]) -> io::Result<bool>;

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
| `From<BStackSlice> for [u8; 16]` | Serialises to `[offset_le8 ‖ len_le8]` for on-disk storage. Reconstruct with `BStackSlice::from_bytes`. |

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
bstack = { version = "0.1", features = ["atomic"] }
# Combined set + atomic unlocks swap, swap_into, and cas:
bstack = { version = "0.1", features = ["set", "atomic"] }
```

#### `atrunc(n, buf)` — truncate then append

Cut `n` bytes off the tail then append `buf` in one locked operation.  
Equivalent to `discard(n)` + `push(buf)` but with no intermediate visible state.

```rust
stack.push(b"hello world")?;
stack.atrunc(6, b"Rust")?; // remove " world", append "Rust"
assert_eq!(stack.peek(0)?, b"helloRust");
```

#### `splice(n, buf) -> Vec<u8>` — pop then append, returning removed bytes

Remove and return the last `n` bytes, then append `buf`.  
Equivalent to `pop(n)` + `push(buf)` but atomically.

```rust
stack.push(b"hello world")?;
let removed = stack.splice(5, b"Rust")?;
assert_eq!(removed, b"world");
assert_eq!(stack.peek(0)?, b"hello Rust");
```

#### `splice_into(old, new)` — pop into buffer then append

Same as `splice` but reads the removed bytes into a caller-supplied `old`
slice instead of allocating a `Vec`, where `n = old.len()`.

```rust
stack.push(b"hello world")?;
let mut buf = [0u8; 5];
stack.splice_into(&mut buf, b"Rust")?;
assert_eq!(&buf, b"world");
```

#### `try_extend(s, buf) -> bool` — conditional append

Append `buf` only if the current logical payload size equals `s`.  Returns
`true` on success, `false` if the size did not match (no-op).  Useful for
optimistic, lock-free–style append protocols.

```rust
let len = stack.len()?;
if stack.try_extend(len, b"new entry\n")? {
    // appended
} else {
    // someone else wrote first; retry
}
```

#### `try_discard(s, n) -> bool` — conditional discard

Discard `n` bytes only if the current logical payload size equals `s`.  Returns
`true` on success, `false` if the size did not match.

```rust
let len = stack.len()?;
if stack.try_discard(len, 4)? {
    // last 4 bytes removed
}
```

---

#### `swap(offset, buf) -> Vec<u8>` — atomic read-then-overwrite *(requires `set`)*

Read `buf.len()` bytes at `offset`, overwrite them with `buf`, and return the
old contents.  The file size never changes.

```rust
stack.push(b"helloworld")?;
let old = stack.swap(5, b"WORLD")?;
assert_eq!(old, b"world");
assert_eq!(stack.peek(0)?, b"helloWORLD");
```

#### `swap_into(offset, buf)` — atomic read-then-overwrite into buffer *(requires `set`)*

Same as `swap` but exchanges in-place through a caller-supplied buffer: on
entry `buf` holds the new bytes; on return `buf` holds the old bytes.

```rust
let mut buf = *b"WORLD";
stack.swap_into(5, &mut buf)?;
// buf now holds the old bytes at offset 5
```

#### `cas(offset, old, new) -> bool` — compare-and-exchange *(requires `set`)*

Read `old.len()` bytes at `offset` and, if they match `old`, overwrite them
with `new`.  Returns `true` if the exchange was performed, `false` if the
comparison failed or the lengths differ.  The file size never changes.

```rust
stack.push(b"helloworld")?;
let swapped = stack.cas(5, b"world", b"WORLD")?;
assert!(swapped);
assert_eq!(stack.peek(0)?, b"helloWORLD");
```

---

### `set`

`BStack::set(offset, data)` — in-place overwrite of existing payload bytes
without changing the file size or the committed-length header.

`BStack::zero(offset, n)` — in-place overwrite of `n` bytes with zeros,
without changing the file size or the committed-length header.  Equivalent to
`set` with a zero-filled buffer but avoids a caller-supplied allocation.

```toml
[dependencies]
bstack = { version = "0.1", features = ["set"] }
```

### `alloc`

Enables the region-management layer on top of `BStack`:
`BStackAllocator`, `BStackSlice`, `BStackSliceReader`, and
`LinearBStackAllocator`.

```toml
[dependencies]
bstack = { version = "0.1", features = ["alloc"] }
# In-place slice writes (BStackSliceWriter) also need `set`:
bstack = { version = "0.1", features = ["alloc", "set"] }
# Experimental FirstFitBStackAllocator requires both alloc and set:
bstack = { version = "0.1", features = ["alloc", "set"] }
```

---

## Allocator (`alloc` feature)

The `alloc` feature adds typed region management over a `BStack` payload.

### `BStackAllocator` trait

A trait for types that own a `BStack` and manage contiguous byte regions
within its payload.  Implementors must provide:

```rust
pub trait BStackAllocator: Sized {
    fn stack(&self) -> &BStack;
    fn into_stack(self) -> BStack;
    fn alloc(&self, len: u64) -> io::Result<BStackSlice<'_, Self>>;
    fn realloc<'a>(&'a self, slice: BStackSlice<'a, Self>, new_len: u64)
        -> io::Result<BStackSlice<'a, Self>>;

    // Default no-op; override for free-list allocators:
    fn dealloc(&self, slice: BStackSlice<'_, Self>) -> io::Result<()> { Ok(()) }

    // Delegation helpers:
    fn len(&self) -> io::Result<u64>;
    fn is_empty(&self) -> io::Result<bool>;
}
```

### `BStackSlice<'a, A>`

A lightweight `Copy` handle — one `&'a A` reference plus two `u64` fields
(`offset`, `len`) — to a contiguous region of the allocator's `BStack`.
Produced by `BStackAllocator::alloc`; consumed by `realloc` and `dealloc`.

> **Slice origin requirement.** `realloc` and `dealloc` are only guaranteed to
> work correctly with a slice that was returned directly by `alloc` or by a
> prior call to `realloc` on the **same allocator instance**.  Passing an
> arbitrary sub-slice obtained via `subslice`, `subslice_range`, or a manually
> constructed `BStackSlice::new` is not supported and may silently corrupt the
> allocator's internal state.  If you need to preserve a slice handle across a
> file reopen, serialise the raw `(start, len)` fields and reconstruct the
> slice via `BStackSlice::new` only for read/write I/O — never pass a
> reconstructed slice back to `realloc` or `dealloc`.

Key methods:

| Method                                       | Description                                    |
|----------------------------------------------|------------------------------------------------|
| `read()`                                     | Read the entire region into a new `Vec<u8>`    |
| `read_into(buf)`                             | Read into a caller-supplied buffer             |
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

| Operation            | Underlying call   | Crash-safe |
|----------------------|-------------------|------------|
| `alloc`              | `BStack::extend`  | yes        |
| `realloc` grow       | `BStack::extend`  | yes        |
| `realloc` shrink     | `BStack::discard` | yes        |
| `dealloc` (tail)     | `BStack::discard` | yes        |
| `dealloc` (non-tail) | no-op             | yes        |

`realloc` returns `io::ErrorKind::Unsupported` for non-tail slices.

### Experimental `FirstFitBStackAllocator` (`alloc + set` features)

Experimental: A persistent first-fit free-list allocator.  Freed regions are tracked on disk
in a doubly-linked intrusive free list and reused for future allocations, so
the file does not grow without bound.

```toml
[dependencies]
bstack = { version = "0.1", features = ["alloc", "set"] }
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
  This version writes `BSTK\x00\x01\x06\x00` (0.1.6).  `open` accepts any
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
| `extend`                               | `lseek(END)` → `set_len(new_end)` → `lseek(8)` → `write(clen)` → sync              |
| `pop`, `pop_into`                      | `lseek` → `read` → `ftruncate` → `lseek(8)` → `write(clen)` → sync                 |
| `discard`                              | `ftruncate` → `lseek(8)` → `write(clen)` → sync                                    |
| `set` *(feature)*                      | `lseek(offset)` → `write(data)` → sync                                             |
| `zero` *(feature)*                     | `lseek(offset)` → `write(zeros)` → sync                                            |
| `atrunc` *(atomic, net extension)*     | `set_len(new_end)` → `lseek(tail)` → `write(buf)` → sync → `write(clen)`           |
| `atrunc` *(atomic, net truncation)*    | `lseek(tail)` → `write(buf)` → `set_len(new_end)` → sync → `write(clen)`           |
| `splice`, `splice_into` *(atomic)*     | `lseek(tail)` → `read(n)` → *(then as `atrunc`)*                                   |
| `try_extend` *(atomic)*                | size check → conditional `push` sequence                                           |
| `try_discard` *(atomic)*               | size check → conditional `discard` sequence                                        |
| `swap`, `swap_into` *(set+atomic)*     | `lseek(offset)` → `read` → `lseek(offset)` → `write(buf)` → sync                  |
| `cas` *(set+atomic)*                   | `lseek(offset)` → `read` → compare → conditional `write(new)` → sync              |
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

| Operation                                                    | Lock (Unix / Windows) | Lock (other) |
|--------------------------------------------------------------|-----------------------|--------------|
| `push`, `extend`, `pop`, `pop_into`, `discard`               | write                 | write        |
| `set`, `zero` *(feature)*                                    | write                 | write        |
| `atrunc`, `splice`, `splice_into`, `try_extend` *(atomic)*   | write                 | write        |
| `try_discard(s, n > 0)` *(atomic)*                           | write                 | write        |
| `try_discard(s, 0)` *(atomic)*                               | **read**              | **read**     |
| `swap`, `swap_into`, `cas` *(set+atomic)*                    | write                 | write        |
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
