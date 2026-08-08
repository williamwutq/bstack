# bstack

A persistent, fsync-durable binary stack backed by a single file.

Every write — `push`, `pop`, and the optional `set`/`atomic` operations —
performs a *durable sync* before returning, so data survives a process crash
or unclean shutdown.  On **macOS**, `fcntl(F_FULLFSYNC)` is used instead of
`fdatasync` to flush the drive's hardware write cache, which plain
`fdatasync` does not guarantee.

[![Crates.io](https://img.shields.io/crates/v/bstack)](https://crates.io/crates/bstack)
[![Docs.rs](https://img.shields.io/docsrs/bstack)](https://docs.rs/bstack)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

A 32-byte file header stores a **magic number**, a **committed-length
sentinel**, and a **write-in-progress journal**.  On reopen, an interrupted
in-place write is replayed or rolled back and any header/size mismatch is
repaired automatically — no user intervention required.

On **Unix**, `open` acquires an **exclusive advisory `flock`**; on
**Windows**, `LockFileEx` is used instead.  Both prevent two processes from
concurrently corrupting the same stack file.

The optional `atomic` feature adds compound read-modify-write and
compare-and-swap operations, including a generator-driven `get_batched_gen` for
multi-step reads, `process_gen` for multi-step read and writes, and `set_batched`
/ `inplace_gen` for committing several in-place writes as one crash-atomic unit.
Independent of 
features, `lock_up_to` lets a prefix of the file be marked permanently immutable
for lock-free reads, and an optional in-memory cache (`open_cached`) can
mirror that region for even faster, syscall-free access.

Other optional Cargo features layer on more: `set` adds in-place writes to
existing bytes, and `alloc` adds typed sub-allocators (linear, first-fit,
slab, etc.) over the payload.

**Minimal dependencies (`libc` on Unix, `windows-sys` on Windows).

> Upgrading from 0.2.x? See [docs/MIGRATION_0.4.0.md](docs/MIGRATION_0.4.0.md) for a step-by-step migration guide.

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

    /// Sparsely grow the payload by `length` bytes, writing `buf` at the start of the
    /// new region and leaving the rest zero (realised with one `set_len`, so the zero
    /// tail costs no write I/O).  Returns the starting logical offset.  Errors if
    /// `buf.len()` exceeds `length`.
    pub fn extend_sparse(&self, buf: impl AsRef<[u8]>, length: u64) -> io::Result<u64>;

    /// Sparsely grow the payload by `length` bytes, scattering `(relative_offset, data)`
    /// buffers (relative to the current tail) into the new region and leaving the gaps
    /// zero.  Returns the starting logical offset.  Writes must be pairwise non-overlapping
    /// and fit within `[0, length)`; empty buffers are ignored.
    pub fn extend_sparse_batched<I, D>(&self, writes: I, length: u64) -> io::Result<u64>
    where I: IntoIterator<Item = (u64, D)>, D: AsRef<[u8]>;

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

    /// Fill `count` copies of `pattern` in place starting at logical `offset`
    /// (the general form of `zero`).  Never changes the file size; errors if the
    /// fill would exceed the current payload.  An empty `pattern` or `count = 0`
    /// is a no-op.  Requires the `set` feature.
    #[cfg(feature = "set")]
    pub fn repeat(&self, offset: u64, pattern: impl AsRef<[u8]>, count: u64) -> io::Result<()>;

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

    /// Append `n` zero bytes only if the current payload size equals `s`; returns whether it did.
    /// Requires the `atomic` feature.
    #[cfg(feature = "atomic")]
    pub fn try_extend_zeros(&self, s: u64, n: u64) -> io::Result<bool>;

    /// Sparse `extend_sparse`/`extend_sparse_batched` gated on a size check: apply the
    /// growth only if the current payload size equals `s`; returns whether it did.
    /// A malformed request still errors regardless of the size match.  Requires the `atomic` feature.
    #[cfg(feature = "atomic")]
    pub fn try_extend_sparse(&self, s: u64, buf: impl AsRef<[u8]>, length: u64) -> io::Result<bool>;
    #[cfg(feature = "atomic")]
    pub fn try_extend_sparse_batched<I, D>(&self, s: u64, writes: I, length: u64) -> io::Result<bool>
    where I: IntoIterator<Item = (u64, D)>, D: AsRef<[u8]>;

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

    /// Atomically swap two non-overlapping byte regions of length `n` under one write lock.
    /// Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn cross_exchange(&self, a: u64, b: u64, n: u64) -> io::Result<()>;

    /// Run a sequence of dependent reads, optionally followed by a single write, under
    /// one held write lock. `f` is called in a loop and drives the sequence through
    /// `BStackGenOp::{Read, Len, Write, Swap, Push, Pop, Discard, Atrunc, Splice, Sparse}`; at
    /// most one of `Write`/`Swap`/`Push`/`Pop`/`Discard`/`Atrunc`/`Splice`/`Sparse` is permitted
    /// and ends the sequence.
    /// Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn process_gen<'a, F>(&self, f: F) -> io::Result<()>
    where F: FnMut() -> Option<BStackGenOp<'a>>;

    /// Commit several non-overlapping in-place writes as one crash-atomic unit.
    /// Each `(offset, data)` overwrites `[offset, offset + data.len())`; either all
    /// apply or none do. Empty `data` is ignored, overlapping writes are rejected,
    /// and the file size never changes.
    /// Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn set_batched<I, D>(&self, writes: I) -> io::Result<()>
    where I: IntoIterator<Item = (u64, D)>, D: AsRef<[u8]>;

    /// Run dependent reads interleaved with multiple in-place writes, committing
    /// every write together at the end via the multi-write journal. `f` is called
    /// in a loop over `BStackGenOp::{Read, Write, Len}` (size-changing ops and
    /// `Swap` are rejected); `Write`s accumulate rather than ending the sequence,
    /// later writes override earlier overlapping ones, and `Read`s see the
    /// batch-so-far content. `f` receives the previous op's `io::Result` (an
    /// erroring op is simply not recorded); `None` commits and ends.
    /// Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn inplace_gen<'a, F>(&self, f: F) -> io::Result<()>
    where F: FnMut(io::Result<()>) -> Option<BStackGenOp<'a>>;

    /// Copy `n` bytes from `from` to `to` under one write lock.  Regions may overlap.
    /// Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn copy(&self, from: u64, to: u64, n: u64) -> io::Result<()>;

    /// Write `b_buf` at `b_offset` only if bytes at `a_offset` equal `a_expected`.
    /// Returns `Some(old_b)` on success, `None` if the condition was not met.
    /// Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn eq_crds(&self, a_offset: u64, a_expected: impl AsRef<[u8]>,
                   b_offset: u64, b_buf: impl AsRef<[u8]>) -> io::Result<Option<Vec<u8>>>;

    /// Like `eq_crds` but writes when region A does NOT match `a_expected`.
    /// Returns `Some(old_b)` on success, `None` if the condition was not met.
    /// Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn ne_crds(&self, a_offset: u64, a_expected: impl AsRef<[u8]>,
                   b_offset: u64, b_buf: impl AsRef<[u8]>) -> io::Result<Option<Vec<u8>>>;

    /// Like `eq_crds` with a bitmask applied to the comparison of region A.
    /// Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn masked_eq_crds(&self, a_offset: u64, mask: impl AsRef<[u8]>,
                          a_expected: impl AsRef<[u8]>, b_offset: u64,
                          b_buf: impl AsRef<[u8]>) -> io::Result<Option<Vec<u8>>>;

    /// Like `ne_crds` with a bitmask applied to the comparison of region A.
    /// Requires the `set` and `atomic` features.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn masked_ne_crds(&self, a_offset: u64, mask: impl AsRef<[u8]>,
                          a_expected: impl AsRef<[u8]>, b_offset: u64,
                          b_buf: impl AsRef<[u8]>) -> io::Result<Option<Vec<u8>>>;

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

    /// Read multiple byte ranges under a single read lock; ranges may overlap.
    /// Requires the `atomic` feature.
    #[cfg(feature = "atomic")]
    pub fn get_batched<I>(&self, ranges: I) -> io::Result<Vec<Vec<u8>>>
    where I: IntoIterator<Item = std::ops::Range<u64>>;

    /// Like `get_batched` but reads into caller-supplied `(offset, buf)` pairs.
    /// Requires the `atomic` feature.
    #[cfg(feature = "atomic")]
    pub fn get_batched_into<'a, I>(&self, bufs: I) -> io::Result<()>
    where I: IntoIterator<Item = (u64, &'a mut [u8])>;

    /// Like `get_batched_into` but the caller supplies a generator closure yielding
    /// `(offset, buf)` pairs; `None` ends the batch.  Requires the `atomic` feature.
    #[cfg(feature = "atomic")]
    pub fn get_batched_gen<'a, F>(&self, f: F) -> io::Result<()>
    where F: FnMut() -> Option<(u64, &'a mut [u8])>;

    /// Current payload size in bytes (excludes the 32-byte header).
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

`BStack` and `&BStack` implement [`std::io::Write`]; each `write` call
forwards to `push` (atomic append + durable sync).  `flush` is a no-op.
Wrap in `BufWriter` to batch many small writes into a single `push`.

```rust
use std::io::Write;
let mut stack = BStack::open("log.bin")?;
stack.write_all(b"hello")?;
std::io::copy(&mut std::io::Cursor::new(b"world"), &mut stack)?;
```

### Reading — `BStackReader`

[`BStackReader`] wraps a `&BStack` with a cursor and implements
[`std::io::Read`] and [`std::io::Seek`].  Multiple readers can coexist
concurrently with each other and with `peek`/`get` calls.

```rust
use std::io::{Read, SeekFrom, Seek};
use bstack::{BStack, BStackReader};
let stack = BStack::open("log.bin")?;
let mut reader = stack.reader();        // from the start
let mut mid    = stack.reader_at(6);   // from offset 6
let mut buf = [0u8; 5];
reader.read_exact(&mut buf)?;
```

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
* **Write protection.**  `set`, `zero`, `repeat`, `swap`, `swap_into`, `cas`,
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

| Trait                | Semantics                                                         |
|----------------------|-------------------------------------------------------------------|
| `PartialEq` / `Eq`   | Equal when both the `BStack` pointer and the cursor offset match. |
| `Hash`               | Hashes `(BStack pointer, offset)`.                                |
| `PartialOrd` / `Ord` | Ordered by `BStack` instance address, then by cursor offset.      |

### Region handle types (`alloc` feature)

**`BStackRange`** — raw `(offset, len)` coordinate pair, no backing reference.

| Trait                                                               | Semantics                                                             |
|---------------------------------------------------------------------|-----------------------------------------------------------------------|
| `PartialEq` / `Eq`                                                  | Compares `(offset, len)`.                                             |
| `Hash`                                                              | Hashes `(offset, len)`.                                               |
| `PartialOrd` / `Ord`                                                | Ordered by `offset`, then `len`.                                      |
| `From<[u8; 16]> for BStackRange` / `From<BStackRange> for [u8; 16]` | Serialises/deserialises `[offset_le8 ‖ len_le8]` for on-disk storage. |

**`BStackOwnedSlice<'a, A>`** — ownership handle carrying `&'a A`. Non-`Copy`, non-`Clone`.

| Trait                | Semantics                                                              |
|----------------------|------------------------------------------------------------------------|
| `PartialEq` / `Eq`   | Compares `(offset, len)`. The allocator reference is **not** compared. |
| `Hash`               | Hashes `(offset, len)`.                                                |
| `PartialOrd` / `Ord` | Ordered by `offset`, then `len`.                                       |

**`BStackSlice<'a>`** — borrowed I/O view carrying `&'a BStack`. Non-`Copy`, `Clone`.

| Trait                | Semantics                                                          |
|----------------------|--------------------------------------------------------------------|
| `PartialEq` / `Eq`   | Compares `(offset, len)`. The stack reference is **not** compared. |
| `Hash`               | Hashes `(offset, len)`.                                            |
| `PartialOrd` / `Ord` | Ordered by `offset`, then `len`.                                   |

`BStackRange`, `BStackOwnedSlice`, and `BStackSlice` are also **cross-comparable**: `PartialEq` and
`PartialOrd` are defined between every pair of the three (both directions), all keyed on the same
`(offset, len)`, so a raw token, an allocation handle, and a borrowed view can be compared or sorted
together directly without an explicit conversion. See [Slice Location Equality](#slice-location-equality)
below for what this comparison does and does not mean.

**`BStackChunk<'a>`** — fixed-stride view carrying `&'a BStack`. Non-`Copy`, `Clone`.

| Trait                | Semantics                                                                                   |
|----------------------|---------------------------------------------------------------------------------------------|
| `PartialEq` / `Eq`   | Compares `(chunk_len, aligned region)` — same stride *and* same underlying `(offset, len)`. |
| `Hash`               | Hashes `(chunk_len, aligned region)`.                                                       |
| `PartialOrd` / `Ord` | Ordered by `chunk_len` first, then by the aligned region's own `Ord`.                       |

Deliberately **not** cross-comparable with `BStackSlice`/`BStackOwnedSlice`/`BStackRange` — a chunk view's stride is part of its identity, and comparing it directly against a bare slice would silently discard that.

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
bstack = { version = "0.4", features = ["atomic"] }
# Combined set + atomic unlocks swap, swap_into, and cas:
bstack = { version = "0.4", features = ["set", "atomic"] }
```

- **`atrunc`**, **`splice`**, **`splice_into`** — atomic discard+push / pop+push tail replacement.
- **`try_extend`**, **`try_discard`**, **`try_extend_zeros`** — size-checked, optimistic append/discard.
- **`try_extend_sparse`**, **`try_extend_sparse_batched`** — size-checked, optimistic sparse tail growth.
- **`replace(n, f)`** — pop `n` bytes, pass to `f`, push back the returned tail.
- **`get_batched`**, **`get_batched_into`**, **`get_batched_gen`** — read multiple (possibly dependent) ranges under one read lock.
- **`swap`**, **`swap_into`**, **`cas`** *(requires `set`)* — atomic read-modify-write / compare-and-swap of a single region.
- **`process`**, **`process_gen`** *(requires `set`)* — in-place mutation, or a dependent read/write sequence ending in at most one `Write`/`Swap`/`Push`/`Pop`/`Discard`/`Atrunc`/`Splice`/`Sparse`.
- **`set_batched`**, **`inplace_gen`** *(requires `set`)* — commit several non-overlapping in-place writes as one crash-atomic unit (a batch, or a generator that also reads the batch-so-far state).
- **`cross_exchange`**, **`copy`** *(requires `set`)* — swap or copy two regions under one write lock.
- **`eq_crds`**, **`ne_crds`**, **`masked_eq_crds`**, **`masked_ne_crds`** *(requires `set`)* — cross-region compare-and-swap, with `==`/`!=`/masked variants.

See [API](#api) for full signatures and details.

---

### `set`

Enables `BStack::set(offset, data)` (in-place overwrite), `BStack::zero(offset, n)` (zero-fill in place), and `BStack::repeat(offset, pattern, count)` (fill with a repeated pattern — the general form of `zero`). None changes the file size or the committed-length header.

```toml
[dependencies]
bstack = { version = "0.4", features = ["set"] }
```

### `alloc`

Enables the region-management layer on top of `BStack`: `BStackAllocator`, `BStackBulkAllocator`, `BStackUninitAllocator`, `BStackOwnedSliceAllocator`, `BStackAllocError`, `BStackBulkAllocError`, `BStackRange`, `BStackOwnedSlice`, `BStackSlice`, `BStackSliceReader`, `LinearBStackAllocator`, `GhostTreeBstackAllocator`, and `DebugCheckingAllocator`.  Combined with `set`, also enables `BStackSliceWriter`, `FirstFitBStackAllocator`, `SlabBStackAllocator`, `CheckedSlabBStackAllocator`, `BStackByteVec`, and `BStackByteVecIter`.

```toml
[dependencies]
bstack = { version = "0.4", features = ["alloc"] }
# In-place slice writes (BStackSliceWriter) also need `set`:
bstack = { version = "0.4", features = ["alloc", "set"] }
```

---

## File format

A fixed 32-byte header precedes the payload:

```text
  bytes      field
  ─────      ─────
   0 ..  8   magic[8]
   8 .. 16   clen      — committed payload length (u64 LE)
  16 .. 24   wip_ptr   — write-in-progress journal target (u64 LE)
  24 .. 32   wip_aux   — write-in-progress journal mode (u64 LE)
  32 ..      payload   — push 0, push 1, … concatenated
```

* **`magic`** — 8 bytes: `BSTK` + major(1 B) + minor(1 B) + patch(1 B) + reserved(1 B).
  This version writes `BSTK\x00\x04\x01\x00` (0.4.1).  `open` accepts any
  0.4.x file (first 6 bytes `BSTK\x00\x04`) and rejects a different major or
  minor as incompatible.  Legacy `0.1.x` files can be upgraded in place with
  `BStack::migrate`.
* **`clen`** — little-endian `u64` recording the last successfully committed
  payload length.  Updated on every `push` and `pop` before the durable sync.
* **`wip_ptr` / `wip_aux`** — the write-in-progress journal that makes in-place
  mutations crash-atomic.  `wip_ptr` is the physical offset a crashed in-place
  write is replayed into (`0` when idle); `wip_aux` names the mode (verbatim
  replay, repeated pattern, a disjoint copy replayed from its still-intact source,
  or a length-changing tail replace whose new committed length recovery derives
  from the file size and direction).  A batch of several in-place writes
  (`set_batched`/`inplace_gen`) uses the `MultiWrite` sentinel in `wip_aux` with
  `wip_ptr` left `0`, staging the writes as `[s | e | data]` blocks past `clen`.
  Interpreted by recovery on `open`.

All user-visible offsets (returned by `push`, accepted by `peek`/`get`) are
**logical** — 0-based from the start of the payload region (file byte 32).

---

## Durability

**In-place same-length writes** — `set`, `zero`, `repeat`, `swap`, `swap_into`,
`cas`, `copy`, `cross_exchange`, `process`, `set_batched`, `inplace_gen`, and the
`crds` family — never change the payload length and are each **crash-atomic**,
committing by one of three strategies (recovered on the next `open`):

* **Aligned-block write** — a target within one power-fail-atomic block is
  committed by a single `write` + sync; no journal is armed.
* **Write-in-progress journal** — otherwise: stage a backup past `clen` → sync →
  arm `wip_ptr` → sync → write in place → sync → clear `wip_ptr` → sync →
  `ftruncate`.  `zero`/`repeat` stage only `[count | pattern]`; `cross_exchange`
  stages one region and commits at a single atomic `wip_ptr` flip; moves and
  fills stream through a bounded buffer.
* **Multi-write journal** — `set_batched` and `inplace_gen` commit several
  non-overlapping in-place writes as one unit: stage every `[s | e | data]` block
  past `clen` → sync → arm the `MultiWrite` sentinel (`wip_ptr` stays `0`, so it
  never collides with a single-region journal) → sync → replay each block in place
  → sync → disarm → `ftruncate`. A batch of one write falls back to the
  single-write strategies above.

Below, *commit* denotes whichever strategy applies to the bytes written; anything
before it is read/compare/callback work under the lock.

| Operation                              | Sequence                                                                                  |
|----------------------------------------|-------------------------------------------------------------------------------------------|
| `push`                                 | `lseek(END)` → `write(data)` → `lseek(8)` → `write(clen)` → sync                          |
| `extend`                               | `lseek(END)` → `set_len(new_end)` → `lseek(8)` → `write(clen)` → sync                     |
| `extend_sparse`, `extend_sparse_batched` | `set_len(new_end)` → `write` each buffer into the grown region (gaps left zero) → `lseek(8)` → `write(clen)` → sync |
| `pop`, `pop_into`                      | `lseek` → `read` → `ftruncate` → `lseek(8)` → `write(clen)` → sync                        |
| `discard`                              | `ftruncate` → `lseek(8)` → `write(clen)` → sync                                           |
| `set` *(feature)*                      | *commit* `data`                                                                           |
| `zero`, `repeat` *(feature)*           | *commit* the repeated pattern (the journal stages only `[count \| pattern]`)              |
| `atrunc` *(atomic)*                    | dispatch on shape: truncation → `ftruncate` → *commit* `clen`; append → `set_len(new_end)` → `write(buf)` → sync → *commit* `clen`; same-length → *commit* `buf` in place; length change → **splice journal** (stage new tail past the payload → arm `SpliceGrow`/`SpliceShrink` → replay into place → atomically commit `clen'` + disarm → truncate, sync at each barrier) |
| `splice`, `splice_into` *(atomic)*     | `lseek(tail)` → `read(n)` → *(then as `atrunc`)*                                          |
| `try_extend` *(atomic)*                | size check → conditional `push` sequence                                                  |
| `try_discard` *(atomic)*               | size check → conditional `discard` sequence                                               |
| `try_extend_zeros` *(atomic)*          | size check → conditional `extend(n)` sequence                                             |
| `try_extend_sparse`, `try_extend_sparse_batched` *(atomic)* | size check → conditional `extend_sparse` / `extend_sparse_batched` sequence               |
| `swap`, `swap_into` *(set+atomic)*     | `read` old bytes → *commit* `buf`                                                         |
| `cas` *(set+atomic)*                   | `read` → compare → conditional *commit* of `new`                                          |
| `process` *(set+atomic)*               | `read(start..end)` → *(callback)* → *commit* the buffer                                    |
| `process_gen` *(set+atomic)*           | closure-driven reads (and `Len` queries), ending in at most one mutating step — `Write` *commits*; `Swap` uses the exchange journal (as `cross_exchange`); `Push`/`Pop`/`Discard`/`Atrunc`/`Splice`/`Sparse` behave as their standalone forms |
| `set_batched` *(set+atomic)*           | validate + reject overlap → **multi-write journal**: stage every `[s \| e \| data]` block past `clen` → arm the `MultiWrite` sentinel (`wip_ptr` stays `0`) → replay each block in place → disarm → `ftruncate` (sync at each barrier); a lone effective write takes the ordinary single-write *commit* |
| `inplace_gen` *(set+atomic)*           | closure-driven reads (each overlaid with the batch-so-far edits) interleaved with accumulated `Write`s (later overrides earlier on overlap); on `None` the pending edits commit together via the multi-write journal (as `set_batched`) |
| `replace` *(atomic)*                   | `lseek(tail)` → `read(n)` → *(callback)* → *(then as `atrunc`)*                           |
| `cross_exchange` *(set+atomic)*        | `read(a)`, `read(b)` → exchange journal: stage `a` → arm at `a` → write `b`→`a` → flip `wip_ptr` to `b` → write `a`→`b` → disarm → `ftruncate` (sync at each barrier) |
| `copy` *(set+atomic)*                  | same-location → no-op; single-block dest → *commit*; overlapping → stream source→tail→dest (`Set` journal); disjoint → copy journal (stage only `[src \| n]`, arm `Copy`, stream source→dest; recovery replays from the untouched source) |
| `eq_crds`, `ne_crds` *(set+atomic)*    | `read(a)` → compare → conditional *commit* of `b_buf`                                      |
| `masked_eq_crds`, `masked_ne_crds` *(set+atomic)* | `read(a)` → mask+compare → conditional *commit* of `b_buf`                     |
| `peek`, `peek_into`, `get`, `get_into`, `get_batched`, `get_batched_into`, `get_batched_gen` | `pread(2)` on Unix; `ReadFile`+`OVERLAPPED` on Windows; `lseek` → `read` elsewhere |

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

On the next `open`, recovery first checks the write-in-progress journal
(`wip_ptr`); if disarmed, it reconciles the committed length against the file
size:

| Condition                                      | Cause                                                        | Recovery                                                      |
|------------------------------------------------|--------------------------------------------------------------|--------------------------------------------------------------|
| `wip_ptr != 0`, `wip_aux = Set`                | in-place `set`/`swap`/`cas`/`copy`/`cross_exchange` crashed mid-commit | replay the staged tail into `[wip_ptr, …)`, disarm, truncate |
| `wip_ptr != 0`, `wip_aux = Repeat`             | `zero`/`repeat` crashed mid-fill                             | write `count` copies of the staged pattern, disarm, truncate |
| `wip_ptr != 0`, `wip_aux = Copy`               | disjoint `copy` crashed mid-copy                            | replay `move_chunked(src → wip_ptr)` from the untouched source (tail stages only `[src \| n]`), disarm, truncate |
| `wip_ptr != 0`, `wip_aux = SpliceGrow`/`SpliceShrink` | length-changing `atrunc`/`splice`/`splice_into`/`replace` crashed mid-replace | derive `clen'` from the file size and direction, replay the staged new tail, commit `clen'` while disarming, truncate |
| `wip_ptr != 0`, `wip_aux` unrecognized         | mode armed by a newer build                                 | roll back: disarm, truncate to `32 + clen`                   |
| `wip_ptr == 0`, `wip_aux = MultiWrite`         | `set_batched`/`inplace_gen` batch crashed after all blocks were staged | replay each staged `[s \| e \| data]` block into `[s, e)`, disarm, truncate (a corrupt tail rolls back, applying nothing) |
| `wip_ptr == 0`, `file_size − 32 > clen`        | partial tail write (crashed before header update)           | truncate to `32 + clen`, durable-sync                        |
| `wip_ptr == 0`, `file_size − 32 < clen`        | partial truncation (crashed before header update)           | set `clen = file_size − 32`, durable-sync                    |

Each replay is idempotent (the staged tail is immutable), so a crash during
recovery is safe to re-run.  No caller action is required; recovery is transparent.

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

`BStack` wraps the file in a `RwLock`. The committed payload length is also
cached in memory and kept in sync with the on-disk header by every
write-lock-held operation, so `len`/`is_empty` can be answered under the read
lock without any `File::metadata` syscall.

| Operation                                                       | Lock (Unix / Windows) | Lock (other) |
|-----------------------------------------------------------------|-----------------------|--------------|
| `push`, `extend`, `pop`, `pop_into`, `discard`                  | write                 | write        |
| `extend_sparse`, `extend_sparse_batched`                        | write                 | write        |
| `set`, `zero`, `repeat` *(feature)*                             | write                 | write        |
| `atrunc`, `splice`, `splice_into`, `try_extend` *(atomic)*      | write                 | write        |
| `try_discard(s, n > 0)` *(atomic)*                              | write                 | write        |
| `try_discard(s, 0)` *(atomic)*                                  | **read**              | **read**     |
| `try_extend_zeros` *(atomic)*                                   | write                 | write        |
| `try_extend_sparse`, `try_extend_sparse_batched` *(atomic)*     | write                 | write        |
| `swap`, `swap_into`, `cas` *(set+atomic)*                       | write                 | write        |
| `process`, `process_gen` *(set+atomic)*                         | write                 | write        |
| `set_batched`, `inplace_gen` *(set+atomic)*                     | write                 | write        |
| `replace` *(atomic)*                                            | write                 | write        |
| `cross_exchange`, `copy` *(set+atomic)*                         | write                 | write        |
| `eq_crds`, `ne_crds` *(set+atomic)*                             | write                 | write        |
| `masked_eq_crds`, `masked_ne_crds` *(set+atomic)*               | write                 | write        |
| `peek`, `peek_into`, `get`, `get_into`                          | **read**              | write        |
| `get_batched`, `get_batched_into`, `get_batched_gen` *(atomic)* | **read**              | write        |
| `len`                                                           | read                  | read         |

On Unix and Windows, `peek`, `peek_into`, `get`, and `get_into` use a
cursor-safe positional read (`pread(2)` / `read_exact_at` on Unix; `ReadFile`
with `OVERLAPPED` via `seek_read` on Windows) that does not modify the shared
file-position cursor.  Multiple concurrent calls to any of these methods can
therefore run in parallel.  Any in-progress `push`, `pop`, or `pop_into` still
blocks all readers via the write lock, so readers always observe a consistent,
committed state.

On other platforms a seek is required; `peek`, `peek_into`, `get`, and
`get_into` fall back to the write lock and reads serialise.

Unlike `get_batched_gen`, which only ever takes the **read** lock, `process_gen`
and `inplace_gen` *always* take the **write** lock — even for sequences that turn
out to be read-only and end in `None` — because the closure may decide, only
after seeing earlier reads, to mutate; the lock must therefore be acquired before
the first read so the whole sequence runs as one indivisible step.

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

## Fault injection (`fault-injection` feature, dev/test only)

The `fault-injection` feature lets tests make `BStack` I/O fail on demand, to exercise error-handling and rollback paths that a successful `push`/`pop`/`realloc`/`dealloc` sequence can never reach. Implement `FaultPolicy` — `fn next_fault(&self, op: &'static str, seq: u64) -> Option<io::Error>` — and arm it with `BStack::with_fault_policy(policy)` (at construction) or `set_fault_policy(Some(policy))` / `fault_policy()` (arm, re-arm, or disarm mid-test). Every I/O method then consults the policy once, **after** validating its arguments, so validation errors always take precedence over an injected fault; under `atomic`, concurrent operations share one per-stack sequence counter for reproducible, seedable schedules.

The whole mechanism is gated on `all(debug_assertions, feature = "fault-injection")`: it is off by default, and a `--release` build carries none of it — no struct field, no per-call branch — so release performance is unaffected.

---

## Allocators (`alloc` feature)

The `alloc` feature adds typed region management over a `BStack` payload.

### Region handle design

The `alloc` feature provides four distinct handle types for different roles:

| Type                      | Carries      | Copy | I/O      | Alloc ops |
|---------------------------|--------------|------|----------|-----------|
| `BStackRange`             | nothing      | yes  | no       | no        |
| `BStackOwnedSlice<'a, A>` | `&'a A`      | no   | via view | yes       |
| `BStackSlice<'a>`         | `&'a BStack` | no   | yes      | no        |
| `BStackChunk<'a>`         | `&'a BStack` | no   | yes      | no        |

`BStackOwnedSlice` is non-`Copy` and non-`Clone`: an allocation has exactly one owner.  Obtaining an I/O view via `as_slice()` or `as_slice_mut()` ties the view's lifetime to the borrow of the owned slice, preventing it from outliving the handle.  `BStackSlice` is non-`Copy` so that `write*(&mut self)` provides single-writer exclusivity; it is `Clone` for explicit second views.

`BStackChunk` sits at the same semantic position as `BStackSlice` — same `Carries`/`Copy`/`I/O`/`Alloc ops` columns, same non-`Copy`-but-`Clone` rationale — it is simply a `BStackSlice` with a fixed stride layered on top (see "`BStackChunk<'a>` — fixed-stride chunked view" below). It is not an iterator itself and has no allocator operations of its own.

### `BStackAllocator` trait

A trait for types that own a `BStack` and manage contiguous byte regions
within its payload.  Implementors must provide:

```rust
pub trait BStackAllocator: Sized {
    type Error: fmt::Debug + fmt::Display;
    // All built-in allocators set Allocated<'a> = BStackOwnedSlice<'a, Self>.
    // Custom allocators may use a richer type that implements Into<BStackOwnedSlice<'a, Self>>.
    type Allocated<'a>: Into<BStackOwnedSlice<'a, Self>> where Self: 'a;

    fn stack(&self) -> &BStack;
    fn into_stack(self) -> BStack;
    fn alloc(&self, len: u64) -> Result<Self::Allocated<'_>, Self::Error>;

    // On failure, realloc/dealloc return a BStackAllocError carrying the
    // surviving allocation (see below), so a failed operation never leaks it.
    fn realloc<'a>(&'a self, handle: Self::Allocated<'a>, new_len: u64)
        -> Result<Self::Allocated<'a>, BStackAllocError<'a, Self>>;

    // Default no-op; override for free-list allocators:
    fn dealloc<'a>(&'a self, handle: Self::Allocated<'a>)
        -> Result<(), BStackAllocError<'a, Self>> { Ok(()) }

    // Delegation helpers:
    fn len(&self) -> io::Result<u64>;
    fn is_empty(&self) -> io::Result<bool>;
}
```

#### `BStackAllocError<'a, A>` — the failing handle is returned, not leaked

`realloc` and `dealloc` consume the handle by value, but a failed resize or free
almost always leaves a valid allocation behind — either the original region is
untouched, or a new region is fully committed. Because `BStackOwnedSlice`'s
`Drop` is a no-op, dropping that handle on the error path would silently leak the
region, so both methods return it instead:

```rust
pub struct BStackAllocError<'a, A: BStackAllocator> {
    pub source: A::Error,
    /// `Some` — the region survived and is owned by the caller again
    /// (implementations return this whenever possible); `None` — the allocation
    /// was genuinely lost mid-operation (recoverable only via crash recovery).
    pub handle: Option<A::Allocated<'a>>,
}
```

It implements `Display` (delegating to `source`) and `std::error::Error`, so `?`
works within functions that return it. Converting *out* to a bare `Self::Error`
discards the recovered handle, so that step is deliberately explicit — decide
whether to retry, fall back, or free the allocation first:

```rust
// Give up and surface just the error (drops the recovered handle):
let resized = alloc.realloc(handle, new_len).map_err(|e| e.source)?;

// Or recover the handle and retry / fall back:
let resized = match alloc.realloc(handle, new_len) {
    Ok(h) => h,
    Err(e) => {
        let original = e.handle.expect("region survived the failed realloc");
        // ... retry with a different size, or alloc.dealloc(original), etc.
    }
};
```

### `BStackOwnedSliceAllocator` supertrait

A convenience bound for the common case of a `BStackAllocator` whose handle type is `BStackOwnedSlice` and whose error type is `io::Error`:

```rust
// Compact form:
A: BStackOwnedSliceAllocator

// Equivalent verbose form:
A: 'static + BStackAllocator<Error = io::Error>,
for<'a> A: BStackAllocator<Allocated<'a> = BStackOwnedSlice<'a, A>>,
```

All built-in allocators implement `BStackOwnedSliceAllocator`.

### `BStackBulkAllocator` trait

An extension trait for `BStackAllocator` that adds two atomic bulk methods. "Atomic" means either the operation succeeds completely or the backing store is left entirely unchanged.

```rust
pub trait BStackBulkAllocator: BStackAllocator {
    fn alloc_bulk(&self, lengths: impl AsRef<[u64]>)
        -> Result<Vec<Self::Allocated<'_>>, Self::Error>;

    // On failure, returns the handles it did not free (every handle, for an
    // atomic implementation) rather than leaking them.
    fn dealloc_bulk<'a>(&'a self, handles: impl IntoIterator<Item = Self::Allocated<'a>>)
        -> Result<(), BStackBulkAllocError<'a, Self>>;
}
```

`BStackBulkAllocError<'a, A>` is the bulk analogue of `BStackAllocError`: it
carries `source: A::Error` plus `handles: Vec<A::Allocated<'a>>`, the handles
still owned by the caller after a failed bulk free.

### `BStackUninitAllocator` trait

An opt-in extension trait for `BStackAllocator` that adds uninitialised
variants of `alloc` and `realloc`. `alloc` guarantees a zero-initialised region
and `realloc` zero-fills newly added bytes when growing; that guarantee costs a
write, since a region pulled from a free list may hold leftover bytes that the
allocator must scrub first. Callers that immediately overwrite the whole region
(for example, `write`-ing a serialized record right after `alloc`) have no use
for the zero-fill. These methods let them skip it.

```rust
pub trait BStackUninitAllocator: BStackAllocator {
    fn alloc_uninit(&self, len: u64) -> Result<Self::Allocated<'_>, Self::Error>;

    fn realloc_uninit<'a>(&'a self, handle: Self::Allocated<'a>, new_len: u64)
        -> Result<Self::Allocated<'a>, BStackAllocError<'a, Self>>;
}
```

The bytes in a region returned by `alloc_uninit`, or in the newly added portion
of one returned by `realloc_uninit`, are **unspecified**: they may be zero, or
may be leftover bytes from a previous allocation that occupied the same on-disk
space. They are always valid to read — no undefined behavior, unlike
`MaybeUninit<u8>` in memory — but callers must not rely on their value until
they have written to the region themselves. Existing bytes are preserved exactly
as `realloc`; only *newly added* bytes are left unspecified.

Implementing the trait is optional and signals that the allocator actually has a
cheaper uninitialised path. The savings are concentrated in the free-list-reuse
path, where a previously-occupied block is handed back without being scrubbed.
Allocators for which zero-fill is already free — an always-extend bump allocator
(the freshly extended tail is already zero via `set_len` on a sparse file), or
one that scrubs blocks eagerly on free — gain nothing and may either implement
the trait as a thin wrapper around `alloc`/`realloc` or not implement it at all.

### `BStackOwnedSlice<'a, A>`

The ownership handle for one allocation. Returned by `alloc`, consumed by `realloc` and `dealloc`. Non-`Copy`, non-`Clone` — exactly one owner per region.

Key methods on `BStackOwnedSlice`:

| Method                                              | Description                                  |
|-----------------------------------------------------|----------------------------------------------|
| `start()` / `end()` / `len()`                       | Coordinate accessors                         |
| `as_slice<'s>(&'s self) -> BStackSlice<'s>`         | Shared read view (lifetime tied to `&self`)  |
| `as_slice_mut<'s>(&'s mut self) -> BStackSlice<'s>` | Exclusive write view                         |
| `to_range()`                                        | Convert to a `BStackRange` for serialisation |

### `BStackSlice<'a>`

A borrowed I/O view carrying `&'a BStack` directly. Obtained from `BStackOwnedSlice::as_slice[_mut]()` or constructed via `unsafe { BStackSlice::from_raw_parts(stack, offset, len) }`.

Key methods on `BStackSlice`:

| Method                                                               | Description                                             |
|----------------------------------------------------------------------|---------------------------------------------------------|
| `read()`                                                             | Read the entire region into a new `Vec<u8>`             |
| `read_into(buf)`                                                     | Read into a caller-supplied buffer                      |
| `read_range(start, end)`                                             | Read a sub-range                                        |
| `subslice(start, end)`                                               | Narrow to a sub-range                                   |
| `head(n)` / `tail(n)`                                                | Sub-view of the first/last `n` bytes (capped to length) |
| `split_at(mid)` / `split_at_mut(mid)`                                | Split into two independent sub-views                    |
| `get(index)`                                                         | Read a single byte, or `None` if out of bounds          |
| `contains(byte)`                                                     | Whether the slice contains a byte                       |
| `starts_with(prefix)` / `ends_with(suffix)`                          | Whether the slice starts/ends with a byte pattern       |
| `find(byte)` / `rfind(byte)`                                         | Index of the first/last occurrence of a byte            |
| `position(pred)` / `rposition(pred)`                                 | Index of the first/last byte matching a predicate       |
| `reader()` / `reader_at(offset)`                                     | Cursor-based `BStackSliceReader`                        |
| `write(data)` *(feature `set`)*                                      | Overwrite the beginning of the region                   |
| `write_range(start, data)` *(feature `set`)*                         | Overwrite a sub-range                                   |
| `zero()` / `zero_range(start, n)` *(feature `set`)*                  | Zero the region or a sub-range                          |
| `fill(value)` *(feature `set`)*                                      | Overwrite the entire slice with one byte value          |
| `fill_with(f)` *(feature `set`)*                                     | Overwrite the entire slice, generating each byte        |
| `copy_from_slice(src)` *(feature `set`)*                             | Overwrite from a matching-length `&[u8]`                |
| `copy_from_bstack_slice(src)` *(features `set` + `atomic`)*          | Overwrite from a matching-length `BStackSlice`          |
| `copy_within(range, dest)` *(features `set` + `atomic`)*             | Copy a sub-range to another offset, in place            |
| `swap(other)` *(features `set` + `atomic`)*                          | Exchange contents with another same-length slice        |
| `reverse()` *(features `set` + `atomic`)*                            | Reverse the byte order in place                         |
| `rotate_left(mid)` / `rotate_right(k)` *(features `set` + `atomic`)* | Rotate the slice in place                               |

Every write method above is a single crash-atomic call. `BStackOwnedSlice` mirrors all of these (delegating through `as_slice()`/`as_slice_mut()`).

### `BStackRange`

A raw `(offset, len)` coordinate pair with no backing reference. `Copy`, serializable to/from `[u8; 16]` via `to_bytes()`/`from_bytes()`. Used for on-disk token storage.

### `BStackSliceReader`

A cursor-based reader over a `BStackSlice`. Implements `io::Read` and `io::Seek`.

### `BStackChunk<'a>` — fixed-stride chunked view

A "slice with a stride": divides a region into `chunk_len`-byte records. Sits at the same semantic position as `BStackSlice` — carries `&'a BStack` directly, no allocator operations — but is a **view**, not an iterator; it does not implement `Iterator`. Non-`Copy`, `Clone`, same rationale as `BStackSlice`.

Obtained from `BStackSlice::chunks(chunk_len)` / `BStackSlice::rchunks(chunk_len)` (mirrored on `BStackOwnedSlice`), each returning **`(BStackChunk<'a>, BStackSlice<'a>)`**: the aligned chunk view, plus whatever bytes are left over if `chunk_len` doesn't evenly divide the source length. `chunks` aligns from the start (leftover at the tail); `rchunks` aligns from the end (leftover at the head). No I/O — pure offset arithmetic, as cheap as `subslice`.

| Method                                                                              | Description                                                                                                              |
|-------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| `chunk_len()` / `chunk_count()` / `len()` / `is_empty()`                            | Stride, chunk count, and total aligned byte length                                                                       |
| `get(index)`                                                                        | The chunk at `index` as a `BStackSlice`, or `None` — O(1), no I/O                                                        |
| `as_slice()` / `into_slice()`                                                       | The whole aligned region as a plain `BStackSlice` — by clone, or by consuming `self`                                     |
| `with_stride(new_stride)`                                                           | Consume `self`, re-dividing the aligned region with a different stride — `(BStackChunk, BStackSlice)`, same as `chunks`  |
| `iter()` / `IntoIterator`                                                           | A lazy `BStackChunkIter` (see below); usable directly in a `for` loop, by value or `&view`                               |
| `binary_search_by(cmp)` / `binary_search_by_key(target, key)`                       | Binary search over already-ordered chunks — O(log n) chunk reads, never the whole region.                                |
| `sort_by(cmp)` / `sort_by_key(key)` *(features `set` + `atomic`)*                   | Stable sort of whole chunks by their bytes/a key. One `BStack::process` call                                             |
| `select_nth_by(n, cmp)` / `select_nth_by_key(n, key)` *(features `set` + `atomic`)* | Partition so chunk `n` lands where a full sort would place it (`[T]::select_nth_unstable_by`); single-transaction atomic |

**`BStackChunkIter`** — the lazy iterator returned by `iter()`/`IntoIterator`. `Item = BStackSlice<'a>`. Each `next()`/`next_back()` (it's `DoubleEndedIterator` + `ExactSizeIterator` + `FusedIterator`) is pure offset arithmetic and performs **no I/O**; actual bytes are only read when the caller calls `.read()` on an individual yielded chunk, one at a time — the chunked region is never materialized into memory as a whole by the iterator itself, regardless of size. (`sort_by`/`select_nth_by` are the exception: they intentionally read the whole aligned region at once, to commit as one crash-atomic transaction — a different, opt-in tradeoff from plain iteration.)

**`PartialEq`/`Eq`/`Hash`/`PartialOrd`/`Ord` for `BStackChunk`** — location equality/ordering over `(chunk_len, aligned_region)`: equal only when both the stride *and* the underlying region match; ordered first by stride, then by the region's own `Ord`. Unlike `BStackSlice`/`BStackOwnedSlice`/`BStackRange`, there is deliberately **no** cross-type comparison against a bare `BStackSlice` — a chunk view's stride is part of its identity, and comparing it directly to a slice would silently discard that.

### Slice Location Equality

`BStackSlice`, `BStackOwnedSlice`, and `BStackRange` implement `PartialEq` **and** `PartialOrd` against each other — every pairing, both directions (`BStackSlice` ↔ `BStackSlice`, `BStackOwnedSlice` ↔ `BStackOwnedSlice`, `BStackSlice` ↔ `BStackOwnedSlice`, `BStackRange` ↔ `BStackSlice`, `BStackRange` ↔ `BStackOwnedSlice`).

This is **location equality/ordering**: it compares coordinates (`offset`, `len`), not the bytes stored there. Two slices over disjoint regions that happen to hold identical bytes compare unequal; two handles over the exact same region compare equal even before anything has been written. `<`/`>` order by `offset`, then `len`, matching each type's own `Ord`. The comparison is synchronous and infallible — no I/O is performed.

To compare *contents* instead, read both sides (`read()`/`read_into()`) and compare the resulting `Vec<u8>`/`[u8]` directly. `BStackByteVec` deliberately implements **neither** trait against any of these types, since a meaningful comparison for a vec would require reading its header to resolve `len` first — an I/O operation `==`/`<` should not perform silently.

### Lifetime model

`BStackOwnedSlice<'a, A>` borrows the allocator for `'a`.  Views obtained via `as_slice[_mut]()` have a shorter lifetime tied to the borrow of the owned slice, preventing them from outliving the handle that owns the region.

### Example

```rust
use bstack::{BStack, BStackAllocator, LinearBStackAllocator};

let alloc = LinearBStackAllocator::new(BStack::open("data.bstack")?);

let mut slice = alloc.alloc(128)?;      // reserve 128 zero bytes
let data = slice.as_slice().read()?;   // read them back
// dealloc returns the handle inside its error on failure; `.map_err(|e| e.source)`
// surfaces just the io::Error.
alloc.dealloc(slice).map_err(|e| e.source)?;  // release (tail → O(1) discard)

let stack = alloc.into_stack();        // reclaim the BStack
```

For detailed on-disk layouts, allocation policies, coalescing rules, crash
consistency guarantees, and thread safety analysis for each allocator, see
[algos/ALLOCATOR.md](algos/ALLOCATOR.md).

### `LinearBStackAllocator`

Bump allocator — regions appended sequentially to the tail.  `dealloc` on a
non-tail slice is a no-op; `realloc` returns `Unsupported` for non-tail slices.
Without `atomic`: `Send` only.  With `atomic`: `Send + Sync` via
`try_extend`/`try_discard`.

### `FirstFitBStackAllocator` (`alloc + set`)

Doubly-linked intrusive free list with first-fit placement and immediate
coalescing.  On-disk header flags trigger a linear recovery scan on the next
open after an unclean shutdown.  Without `atomic`: `Send` only.  With
`atomic`: `Send + Sync` via an internal `Mutex` serialising free-list mutations.

### `GhostTreeBstackAllocator` (`alloc`)

AVL tree keyed on `(size, address)`; best-fit; zero per-allocation overhead.
Also implements `BStackBulkAllocator`.  Without `atomic`: `Send` only.  With
`atomic`: `Send + Sync` via an internal `Mutex`.

### `SlabBStackAllocator` (`alloc + set`)

Fixed `block_size` slab; singly-linked free list; zero per-block overhead.
Constructors: `new(stack, block_size)` for a fresh stack, `open(stack)` to
reattach.  Without `atomic`: `Send` only.  With `atomic`: `Send + Sync` with
no allocator-level lock (uses `BStack::process_gen` / `cross_exchange`).

### `CheckedSlabBStackAllocator` (`alloc + set`)

Like `SlabBStackAllocator` but each block has an 8-byte overhead tag that
catches double-frees immediately and allows full recovery after a crash.
Constructor takes `data_size` (usable bytes per block; physical = `data_size + 8`).
`open` runs `recover()` automatically.  Without `atomic`: `Send` only.  With
`atomic`: `Send + Sync` (same lock-free strategy as `SlabBStackAllocator`).

### `DebugCheckingAllocator` (`alloc`)

Transparent debug wrapper that can be placed around any allocator.  Tracks
allocated and freed regions in memory and panics on overlapping allocations,
double-frees, partial-frees, and multi-span frees.  When the inner allocator
reports a lost handle (`handle: None`), the region is removed from tracking
entirely.  Intended for tests and debugging only — the O(n) overlap checks add
significant per-operation overhead.

```rust
use bstack::{BStack, BStackAllocator, DebugCheckingAllocator, LinearBStackAllocator};

let inner = LinearBStackAllocator::new(BStack::open("debug.bstack")?);
let alloc = DebugCheckingAllocator::new(inner);
let handle = alloc.alloc(256)?;
// alloc/realloc/dealloc are validated against the tracking sets
```

### Benchmarks (`alloc + atomic`)

`benches/alloc.rs` measures cross-allocator throughput at several thread counts.
It requires the `atomic` feature for `Sync` allocators. Use `-- <allocator>` to select one of the built-in allocators:

> **Note:** Benchmarks are only available when building from source (e.g. after cloning this repository). They are not included in the crate published to [crates.io](https://crates.io).
> If you have the source, consider running the benchmarks to identify the best allocator for your workload.
> For the most accurate results, edit `benches/alloc.rs` to model your actual allocation patterns (sizes, op mix, thread count) before running.

```sh
cargo bench --bench alloc --features "alloc set atomic" -- "first_fit"
```

As a general guideline (based on `benches/alloc.rs` mixed-workload results):
- **`ghost_tree`** is the best default: fastest overall and its latency stays flat from 1 to 16 threads, making it the safest choice under unknown or high contention. However, long term use of `ghost_tree` may lead to fragmentation and wasted space since it does not support coalescing. If fragmentation is a concern, consider using `first_fit` or `checked_slab` instead.
- **`checked_slab`** (sized to match your typical allocation) is usually faster than plain `slab` at the same block size, in addition to catching double-frees and supporting crash recovery — prefer it over `slab` in most cases, especially at smaller block sizes.
- **`slab`** can still win for single-threaded, fixed-size workloads at some block sizes (e.g. `slab_32`), but is inconsistent across sizes and degrades at others (e.g. `slab_64`) — benchmark your specific `block_size` before choosing it over `checked_slab`.

**Configuration** — all knobs are environment variables read once at startup:

| Variable                 | Meaning                                                      | Default    |
|--------------------------|--------------------------------------------------------------|------------|
| `BSTACK_BENCH_OP`        | op mix: preset name or `alloc,realloc,dealloc` weight triple | `mixed`    |
| `BSTACK_BENCH_SIZE`      | size distribution preset                                     | `uniform`  |
| `BSTACK_BENCH_MAX`       | maximum allocation length drawn                              | `1024`     |
| `BSTACK_BENCH_THREADS`   | comma-separated thread counts                                | `1,2,4,16` |
| `BSTACK_BENCH_PRE_ALLOC` | live allocations pre-populated per benchmark                 | `256`      |
| `BSTACK_BENCH_SEED`      | seed for the decision stream                                 | `48`       |

Op-mix presets: `mixed`, `alloc-only`, `alloc-heavy`, `realloc-heavy`, `churn`.  
Size presets: `uniform`, `fixed`, `gamma[:k:theta_frac]`, `bimodal[:small:p_large]`.

Example — alloc-only workload, gamma-distributed sizes up to 4096 bytes, single thread for `FirstFitBStackAllocator`:

```sh
BSTACK_BENCH_OP=alloc-only \
BSTACK_BENCH_SIZE=gamma \
BSTACK_BENCH_MAX=4096 \
BSTACK_BENCH_THREADS=1 \
cargo bench --bench alloc --features "alloc set atomic" -- "first_fit"
```

## `BStackByteVec<'a, A>` (`alloc + set` features)

A growable byte (`u8`) vector backed by a `BStack` allocation, mirroring the core `Vec<u8>` API.

A general typed vector over arbitrary `Copy` types requires a sound POD/byte-castable bound and is planned for a future release.

```toml
[dependencies]
bstack = { version = "0.4", features = ["alloc", "set"] }
```

### Memory layout

```
┌──────────────────────┬──────────────────────┬────────────────────────┐
│   len  (8 B, LE u64) │   cap  (8 B, LE u64) │   elements: [u8; cap]  │
└──────────────────────┴──────────────────────┴────────────────────────┘
  byte 0                 byte 8                  byte 16
```

The header is re-read from disk on every call, so the `(len, cap)` metadata is
recoverable after a crash by reconstructing the handle from the raw block via
`BStackByteVec::from_raw_block`.

### Key behaviour

- **Growth**: `push` reallocates to `max(cap × 2, 4)` bytes when `len == cap`. New space is zero-initialised by `BStack::extend`.
- **Bulk append**: `extend_from_slice` appends a whole `&[u8]` at once, reserving the needed capacity in a single reallocation and writing all bytes with one durable `set` before committing `len` — cheaper than a `push` per byte. A crash before the `len` write leaves the appended bytes invisible (beyond `len`); re-running with the same data recovers.
- **In-place edits**: `set(index, value)` overwrites one slot; `fill(value)` overwrites the whole populated region with a single `BStack::repeat` (fixed-size journal regardless of `len`). Both are single crash-atomic writes.
- **Out-of-bounds convention**: the index/range-taking methods (`set`, `insert`, `remove`, `swap_remove`, `extend_from_within`, `copy_into_bstack_slice`, `move_tail_into`) return `io::Result<Option<_>>` and yield `Ok(None)` for an out-of-range index/length or a `u64` overflow (like `get`), leaving the vec untouched. `Err` is reserved for I/O failures; passing a slice from a *different* `BStack` to a cross-slice method is an `Err` (a misuse, not an out-of-range request).
- **Capacity control**: `reserve_exact` grows to exactly `len + additional` (no amortised over-allocation); `shrink_to(min)` / `shrink_to_fit()` reallocate the block down to `max(len, min)` / `len`, releasing spare capacity.
- **Atomic byte movers** (`atomic` feature): built on the crash-atomic `BStack::copy` and `BStack::cross_exchange` primitives, so no method shifts bytes one at a time.
  - Append-only, benign crash model (like `push`): `extend_from_within(start, count)` appends a copy of an existing range; `extend_from_bstack_slice(&src)` appends an on-disk slice from the same `BStack`; `append_from_owned(owned)` appends a `BStackOwnedSlice`'s bytes then frees it (a move).
  - In-place, torn-but-valid on crash: `insert(index, value)` / `remove(index)` shift the tail via `copy`; `swap_remove(index)` swaps the hole with the last byte via `cross_exchange`; `move_tail_into(&mut dest)` swaps the vec's tail into a `BStackOwnedSlice` and shrinks. `copy_into_bstack_slice(start, &mut dst)` copies vec bytes out into a same-`BStack` slice (a single atomic `copy`).
- **Readback helper**: `read_bytes` loads all logical bytes into a Rust `Vec<u8>`.
- **Zeroing on removal**: `pop` decrements `len` before zeroing the vacated slot; `truncate` writes the new `len` before zeroing removed slots in a single `BStackSlice::zero_range` call. Deallocation zeroing is delegated to the allocator.
- **Iterator**: `BStackByteVecIter` borrows the vec immutably for its lifetime (preventing concurrent mutation) and yields `io::Result<u8>` per byte, reading from disk on demand.

### Example

```rust
use bstack::{BStack, BStackByteVec, LinearBStackAllocator};

let alloc = LinearBStackAllocator::new(BStack::open("buf.bstack")?);

let mut v: BStackByteVec<_> = BStackByteVec::new(&alloc)?;
v.push(b'A')?;
v.extend_from_slice(b"BC")?; // bulk append

assert_eq!(v.len()?, 3);
assert_eq!(v.get(1)?, Some(b'B'));
assert_eq!(v.pop()?, Some(b'C'));

let all = v.read_bytes()?;
println!("{}", String::from_utf8_lossy(&all));

alloc.dealloc(v.into_raw_block()).map_err(|e| e.source)?;
```

