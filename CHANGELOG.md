# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.7] - 2026-04-27

### Added

- **`atomic` feature**: Enables compound read-modify-write operations that hold the write lock across what would otherwise be separate calls, providing thread-level atomicity and crash-safe sequencing.
  - **`BStack::atrunc(n, buf)`**: Cut `n` bytes off the tail then append `buf` as a single locked operation. Operation ordering is chosen based on the net file-size change: for a net extension the file is extended before the write (so a crash before the committed-length update cleanly rolls back to the original state); for a net truncation the new bytes are written first then the file is truncated (so a crash after truncation is correctly committed by recovery).
  - **`BStack::splice(n, buf) -> Vec<u8>`**: Pop `n` bytes from the tail (returning them) then append `buf`. The removed bytes are read before any mutation. Uses the same two-path ordering strategy as `atrunc`.
  - **`BStack::splice_into(old, new)`**: Buffer-reuse counterpart of `splice`: reads the removed bytes into the caller-supplied `old` slice (`n = old.len()`) then appends `new`, avoiding a heap allocation.
  - **`BStack::try_extend(s, buf) -> bool`**: Append `buf` only if the current logical payload size equals `s`; returns `true` if the append was performed, `false` (no-op) otherwise. Enables optimistic check-then-append patterns.
  - **`BStack::try_discard(s, n) -> bool`**: Discard `n` bytes only if the current logical payload size equals `s`; returns `true` if the discard was performed. When `n = 0` only the read lock is taken.
  - **`BStack::swap(offset, buf) -> Vec<u8>`** *(requires `set` + `atomic`)*: Atomically read `buf.len()` bytes at `offset` and overwrite them with `buf`; returns the old contents. File size is never changed.
  - **`BStack::swap_into(offset, buf)`** *(requires `set` + `atomic`)*: Same atomic swap but exchanges in-place through a caller-supplied buffer: on entry `buf` holds the new bytes; on return `buf` holds the old bytes.
  - **`BStack::cas(offset, old, new) -> bool`** *(requires `set` + `atomic`)*: Compare-and-exchange. Reads `old.len()` bytes at `offset`, compares them to `old`, and if equal writes `new` in their place. Returns `true` if the exchange was performed. Returns `false` (no-op) if the byte comparison fails or if `old.len() != new.len()`.

## [0.1.6] - 2026-04-26

### Added

- **`FirstFitBStackAllocator::realloc` — in-place grow by merging the next free block**: when the block immediately following the current allocation is free and large enough, `realloc` now absorbs it without copying any data. The merged region is then split if the result is significantly larger than the requested size, with the surplus returned to the free list as a new free block. This avoids the copy-and-move path for the common case of growing an allocation that has adjacent free space.
- **Recovery: partial-split detection and repair**: after a crash between the block-data write and the header-size update of a split operation, the recovered header still reports the pre-split (oversized) length while the inner footer and the second sub-block's header form a consistent signature. Recovery now detects this three-point mismatch — outer footer value `F`, inner footer at `H − F − OVERHEAD` equal to `H − F − OVERHEAD`, second sub-block header equal to `F` — and rewrites the corrupted header to its correct value so both sub-blocks are visible and navigated correctly.

### Fixed

- **`FirstFitBStackAllocator::realloc` — incorrect merged block size**: the in-place merge computed `merged_size = block_size + next_block_size`, omitting the 24-byte `BLOCK_OVERHEAD_SIZE` that sits between the two original blocks. This caused the header to advertise a smaller extent than where the footer was actually written, making any subsequent free-and-coalesce operation navigate to the wrong position. Fixed to `block_size + BLOCK_OVERHEAD_SIZE + next_block_size`.
- **`FirstFitBStackAllocator::realloc` — split threshold too loose**: the split condition used `merged_size > aligned_new_len + BLOCK_FOOTER_SIZE + MIN_BLOCK_PAYLOAD_SIZE` (strict `>`). Because `BLOCK_FOOTER_SIZE + MIN_BLOCK_PAYLOAD_SIZE = BLOCK_OVERHEAD_SIZE = 24` and all sizes are multiples of 8, the minimum triggering case was `merged_size = aligned_new_len + 32`, producing a remainder of 8 bytes — below `MIN_BLOCK_PAYLOAD_SIZE` (16) and too small to hold the free block's `next_free`/`prev_free` pointers. Fixed to `merged_size >= aligned_new_len + BLOCK_OVERHEAD_SIZE + MIN_BLOCK_PAYLOAD_SIZE`, guaranteeing remainder ≥ 16 bytes.
- **`FirstFitBStackAllocator::alloc` and `realloc` — split/no-split detection inconsistent with `unlink_block`**: `alloc` and `realloc` computed the new payload location using `found_size > aligned_len + BLOCK_FOOTER_SIZE + MIN_BLOCK_PAYLOAD_SIZE` to decide whether `unlink_block` would split, but `unlink_block` itself uses `found_size >= aligned_len + BLOCK_OVERHEAD_SIZE + MIN_BLOCK_PAYLOAD_SIZE`. When `found_size` fell in the gap between those two thresholds (i.e., `aligned_len + 32 ≤ found_size < aligned_len + 40`), the caller assumed a split occurred and returned a slice pointing to the back of the found block, while `unlink_block` had in fact written the user data to the front. Every read and write via the returned slice then accessed the wrong memory region, silently corrupting or discarding user data. Fixed by aligning both conditions to `>= aligned_len + BLOCK_OVERHEAD_SIZE + MIN_BLOCK_PAYLOAD_SIZE`.
- **`FirstFitBStackAllocator::realloc` — stale bytes exposed on in-place grow**: when `realloc` grew a slice without moving it (block already large enough, tail-extend, or in-place merge), bytes between the old `slice.len()` and the new `len` could contain stale data from a previous larger allocation. The affected paths now zero exactly `[slice.len(), new_len)` before returning, matching the zero-initialisation contract of `alloc` and `LinearBStackAllocator::realloc`. The copy-and-move paths are fixed by limiting the copy to `slice.len()` bytes (not `aligned_current_len`), leaving the rest of the destination buffer zero-initialised.

## [0.1.5] - 2026-04-26 [YANKED]

* Yanked due to critical bugs in the new `FirstFitBStackAllocator` implementation. See fixes in [0.1.6].

### Added

- **`BStack::Debug`**: Shows `version` (semver string derived from the magic header, e.g. `"0.1.x"`) and `len` (current payload size as `Option<u64>`, `None` on I/O failure).
- **`BStack` equality and hashing**: `PartialEq`/`Eq` use pointer identity — two distinct instances are never equal. Because `open` holds an exclusive advisory lock, no two `BStack` values in one process can refer to the same file simultaneously, making pointer identity the only meaningful equality. `Hash` hashes the instance address, consistent with `PartialEq`.
- **`BStackReader` equality, hashing, and ordering**: `PartialEq`/`Eq` compare `(BStack pointer, offset)`; `Hash` is consistent; `PartialOrd`/`Ord` order by `BStack` instance address then by cursor offset.
- **`alloc` feature**: Adds region-based allocation over a `BStack` payload.
  - **`BStackAllocator` trait**: Standard interface for types that own a `BStack` and manage contiguous byte regions within its payload. Requires `stack()`, `into_stack()`, `alloc()`, and `realloc()`; provides a default no-op `dealloc()`, and delegation helpers `len()` / `is_empty()`. Includes `Debug`, `From<BStack>`, and `From<LinearBStackAllocator> for BStack` on `LinearBStackAllocator`.
  - **`BStackSlice<'a, A>`**: Lightweight `Copy` handle (allocator reference + `offset` + `len`) to a contiguous region. Exposes `read`, `read_into`, `read_range_into`, `subslice`, `subslice_range`, `reader`, `reader_at`, `to_bytes`, `from_bytes`; and (with `set`) `write`, `write_range`, `zero`, `zero_range`, `writer`, `writer_at`. Trait impls: `PartialEq`/`Eq`/`Hash` by `(offset, len)`; `PartialOrd`/`Ord` by `(offset, len)`; `From<BStackSlice> for [u8; 16]`.
  - **`BStackSliceReader<'a, A>`**: Cursor-based reader over a `BStackSlice`, implementing `io::Read` and `io::Seek` in the slice's coordinate space. Trait impls: `PartialEq`/`Eq`/`Hash` by `(slice, cursor)`; `PartialOrd`/`Ord` by absolute payload position `slice.start() + cursor`, then `slice.len()`.
  - **`BStackSliceWriter<'a, A>`** (requires `alloc` + `set`): Cursor-based writer over a `BStackSlice`, implementing `io::Write` and `io::Seek`. Every `write` call delegates to `BStack::set` and is durably synced. Same trait impls as `BStackSliceReader`.
  - **Cross-type comparisons**: `PartialEq` and `PartialOrd` are defined between `BStackSliceReader` and `BStackSliceWriter` using the same `(abs_pos, len)` key (requires `set`). Both cursor types also implement `PartialEq<BStackSlice>` (cursor position ignored).
  - **`From` conversions**: `BStackSlice` ↔ `BStackSliceReader`, `BStackSlice` ↔ `BStackSliceWriter`, `BStackSliceReader` ↔ `BStackSliceWriter`.
  - **`LinearBStackAllocator`**: Reference bump allocator that appends regions sequentially. `realloc` is O(1) for the tail allocation and returns `Unsupported` for non-tail slices. `dealloc` reclaims the tail via `BStack::discard`; non-tail deallocations are a no-op. Every operation maps to exactly one `BStack` call and is crash-safe by inheritance.
  - **`FirstFitBStackAllocator`** (requires `alloc` + `set`): Persistent first-fit free-list allocator. Freed regions are tracked on disk in a doubly-linked intrusive free list and reused for future allocations so the file does not grow without bound.
    - **On-disk layout**: the first 48 payload bytes are an allocator header (`ALFF` magic + flags + `free_head`); the arena follows immediately. Each block is `[BlockHeader 16 B | payload | BlockFooter 8 B]`; free blocks store `next_free`/`prev_free` in the first 16 bytes of their payload. Minimum payload size is 16 bytes; all sizes are 8-byte aligned.
    - **Allocation**: first-fit walk of the free list; splits found blocks from the back when the remainder would be ≥ 16 bytes; extends the stack when no free block fits.
    - **Coalescing**: `dealloc` merges adjacent free neighbours (right then left). Merged blocks that reach the stack tail are discarded. A cascade check removes any further free blocks newly exposed at the tail, maintaining the invariant that the tail block is always allocated.
    - **Crash consistency**: multi-step operations bracket free-list mutations with a `recovery_needed` flag. On `new`, if `recovery_needed` is set, a linear O(n) scan rebuilds the free list from `is_free` header flags (stored pointers are not trusted) and truncates any partial tail block.
    - **`realloc`**: O(1) in-place grow/shrink for the tail block; copy-and-move for non-tail blocks using an existing free block or a new stack extension; same-block optimisation when the existing block already fits.

## [0.1.4] - 2026-04-25

### Added
- **`extend` method (Rust) / `bstack_extend` (C)**: Append `n` zero bytes to the tail and durable-sync. Returns the starting logical offset. `n = 0` is a no-op. Useful for reserving space in the payload without a caller-supplied buffer.
- **`zero` method (Rust, `set` feature) / `bstack_zero` (C, `BSTACK_FEATURE_SET`)**: Overwrite `n` bytes with zeros in place at a logical offset and durable-sync, without changing the file size. `n = 0` is a no-op; errors if `offset + n` exceeds the payload size.
- **`discard` method (Rust) / `bstack_discard` (C)**: Remove the last `n` bytes from the tail and durable-sync, without reading or returning the removed bytes. Equivalent to `pop`/`bstack_pop` but skips the buffer read, avoiding any allocation or copy. `n = 0` is a no-op; exceeding the payload size returns an error.

## [0.1.3] - 2026-04-20

### Added
- **`peek_into` method**: Fill a caller-supplied `&mut [u8]` from a logical offset, avoiding the `Vec` allocation of `peek`
- **`get_into` method**: Fill a caller-supplied `&mut [u8]` from a half-open logical range, avoiding the `Vec` allocation of `get`
- **`pop_into` method**: Pop bytes from the tail directly into a caller-supplied `&mut [u8]`, avoiding the `Vec` allocation of `pop`
- **`impl std::io::Write for BStack`**: Each `write` call forwards to `push` — atomically appended and durably synced; `flush` is a no-op
- **`impl std::io::Write for &BStack`**: Shared-reference counterpart, mirroring `impl Write for &File`; enables `BufWriter<&BStack>` for batched writes
- **`BStackReader` type**: Cursor-based reader over `&BStack` implementing `std::io::Read`, `std::io::Seek`, and `From<&BStack>`; multiple readers can coexist and run concurrently
- **`BStack::reader()`**: Construct a `BStackReader` positioned at the start of the payload
- **`BStack::reader_at(offset)`**: Construct a `BStackReader` at an arbitrary logical offset

### Changed
- Moved tests to `src/test.rs` for better organization and to avoid cluttering the main library file

## [0.1.2] - 2026-04-18

### Added
- **Windows support**: Full first-class Windows support with `LockFileEx` for exclusive file locking and `ReadFile` with `OVERLAPPED` for cursor-safe positional reads
- **Concurrent reads on Windows**: `peek` and `get` operations now use the read lock on Windows, enabling concurrent readers just like on Unix
- **Cross-platform durability**: `FlushFileBuffers` on Windows provides equivalent durability guarantees to `fdatasync` on Unix

### Changed
- Updated thread-safety documentation to reflect Windows support alongside Unix
- Updated multi-process safety documentation to cover both `flock` (Unix) and `LockFileEx` (Windows)
- Extended concurrent reads test to run on both Unix and Windows platforms

### Dependencies
- Added `windows-sys` crate for Windows platform support

## [0.1.1] - 2026-04-17

### Added
- **`get` method**: Read arbitrary half-open byte ranges `[start, end)` from logical offsets
- **Concurrent reads on Unix**: `peek` and `get` operations now use `pread(2)` and take only the read lock, allowing multiple concurrent readers
- **Enhanced durability on macOS**: `durable_sync` now uses `F_FULLFSYNC` to flush the drive's hardware write cache, providing stronger guarantees than plain `fdatasync`

### Changed
- Updated thread-safety model documentation to reflect read-lock usage for `peek`/`get` on Unix

## [0.1.0] - 2026-04-16

### Added
- Initial release of `bstack`: A persistent, fsync-durable binary stack backed by a single file
- Core operations: `push`, `pop`, `peek`, `len`
- Crash recovery with committed-length sentinel
- Multi-process safety via advisory `flock` on Unix
- File format with 16-byte header containing magic number and committed length
- Durability guarantees with `durable_sync` (fdatasync on Unix)
- Optional `set` feature for in-place payload mutation