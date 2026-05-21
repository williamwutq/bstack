# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **In-memory caching of the locked region (`open_cached`, `open_locked_up_to_cached`)**: Opening a `BStack` with [`open_cached`](BStack::open_cached) enables an opt-in in-memory mirror of the locked region. Each subsequent `lock_up_to(n)` call reads the newly locked bytes from disk into a heap-allocated `Vec<u8>` with power-of-two capacity growth. Reads whose range falls entirely within the locked region are then served by copying from the buffer with no syscall. The trade-off is that `lock_up_to` becomes significantly more expensive on cached stacks (it must read up to `n` bytes from disk before returning). [`open_locked_up_to_cached`](BStack::open_locked_up_to_cached) combines `open_cached` and `lock_up_to` into a single call for the common pattern of locking a known prefix at open time. Stacks opened with [`open`](BStack::open) or [`open_locked_up_to`](BStack::open_locked_up_to) are unaffected.

- **`BStackByteVec<'a, A: BStackSliceAllocator>`** (`alloc` + `set` features): A growable byte (`u8`) vector backed by a `BStack` allocation, mirroring the core `Vec<u8>` API. The 16-byte block header stores `len` and `capacity` as little-endian `u64` values; bytes follow immediately. The header is re-read from disk on every call, so the metadata is recoverable after a crash by reconstructing the handle via `BStackByteVec::from_raw_block`. A general typed vector (`BStackVec<T>`) is planned; the general case requires a sound POD/byte-castable bound and will be added in a future release. Key design points:
  - **Growth**: when `push` would exceed capacity, the block is reallocated to `max(cap * 2, 4)` bytes via the allocator's `realloc`. New space is zero-initialised by `BStack::extend`.
  - **Zeroing on removal**: `pop` decrements `len` before zeroing the vacated slot; `truncate` writes the new `len` before zeroing removed slots in a single `BStackSlice::zero_range` call. Deallocation zeroing is delegated to the allocator.
  - **API**: `new`, `with_capacity`, `from_slice`, `unsafe from_raw_block`, `len`, `capacity`, `is_empty`, `get`, `read_bytes`, `as_slice`, `push`, `pop`, `truncate`, `clear`, `reserve`, `resize`, `iter`, `unsafe raw_block`, `into_raw_block`, `dealloc`.
  - **Iterator**: `BStackByteVecIter<'b, 'a, A>` borrows the vec immutably for its lifetime (preventing concurrent mutation), snapshots `len` at construction, and yields `io::Result<u8>` per byte read from disk on demand.

- **`BStackSlice::empty(allocator)` / `bstack_slice_empty`** (`alloc` feature): Constructs a zero-length slice anchored at offset 0.

### Changed

- **`FirstFitBStackAllocator` internal reads converted from `get` to `get_into` with stack-allocated buffers** (`alloc` + `set` features): Three internal reads — the 32-byte allocator header on `new`, the 8-byte `free_head` field at the start of `find_large_enough_block`, and the 8-byte block size during `realloc`'s same-block fast path — now use fixed-size stack arrays and `get_into` instead of `get`. This eliminates three small heap allocations per call to those paths, with no change to observable behaviour.

## [0.2.1] - 2026-05-20

### Fixed

- **`FirstFitBStackAllocator::dealloc` — double-free** (`alloc` + `set` features): `dealloc` now returns `InvalidInput` if the block header's `is_free` flag is already set. Previously a double-free wrote a self-referential free-list entry; because `recovery_needed` was cleared normally, the corruption survived reopen and made every subsequent allocation fail.
- **`FirstFitBStackAllocator::dealloc` — tail fast path missing `recovery_needed` guard** (`alloc` + `set` features): `dealloc` now sets `recovery_needed` before `BStack::discard` in the tail-block path and clears it after `cascade_discard_free_tail`. A crash between the two steps could leave a free block at the new tail, violating the invariant that the tail is always allocated, with no recovery triggered on reopen.
- **`FirstFitBStackAllocator` recovery — mid-arena corrupt block truncated all following data** (`alloc` + `set` features): Recovery now returns `InvalidData` when a block header contains an invalid size (below minimum or not 8-aligned), instead of discarding everything from that offset to the tail. Truncation is still performed for a valid-size block that merely extends past the stack end (the intended partial-tail-write case).
- **`FirstFitBStackAllocator::find_large_enough_block` — free-list cycle hangs indefinitely** (`alloc` + `set` features): The walk now returns `InvalidData` if iteration count exceeds `arena_size / min_block_size + 1`, bounding the loop against self-referential entries introduced by corruption.
- **`GhostTreeBstackAllocator::alloc_bulk` — found-block remainder silently discarded** (`alloc` feature): When best-fit returned a block larger than the requested total, the surplus was consumed without being re-inserted into the AVL tree. `alloc_bulk` now mirrors `alloc`'s split logic: if the remainder is ≥ 32 bytes it is recycled as a new free block.
- **`GhostTreeBstackAllocator::coalesce_and_rebalance` — duplicate node visits widened corruption** (`alloc` feature): A partial rotation crash can leave one node reachable from two parents; the in-order walk visited it twice, and the rebuild pass clobbered the child pointers written by the first visit. The collected block list is now deduplicated by address before coalescing.
- **`GhostTreeBstackAllocator` — AVL recursion unbounded on cyclic corrupted tree** (`alloc` feature): All five recursive AVL functions (`walk_inorder`, `insert_rec`, `remove_rec`, `find_best_fit_and_remove_rec`, `min`) now accept a depth counter capped at 128 and return `InvalidData` when it reaches zero, preventing a stack overflow when a stale pointer forms a cycle.
- **`GhostTreeBstackAllocator::dealloc` — double-free undetectable by design** (`alloc` feature): GhostTree stores no per-block `is_free` flag and no block headers for live allocations, so there is no reliable way to distinguish a free block's AVL size field from user data that happens to hold the same value. Double-free is not detected; callers are responsible for not freeing a handle twice.

### Changed

- **`FirstFitBStackAllocator` version bumped to 0.1.2** (`alloc` + `set` features): Magic number updated from `ALFF\x00\x01\x01\x00` to `ALFF\x00\x01\x02\x00`. This is to reflect the bug fixes in 0.1.6, which are critical for data integrity. Existing 0.1.x files remain fully compatible (only the first 6 bytes are checked on open).
- **`GhostTreeBstackAllocator` version bumped to 0.1.1** (`alloc` feature): Magic number updated from `ALGT\x00\x01\x00\x00` to `ALGT\x00\x01\x01\x00`. This is to reflect the bug fixes in 0.1.6, which are critical for data integrity. Existing 0.1.x files remain fully compatible.

- **`LinearBStackAllocator::realloc` error message clarified** (`alloc` feature): The `Unsupported` error returned for non-tail slice reallocation now explicitly states that deallocating the old slice is also a no-op and will leak the region, so callers understand both steps of the copy-and-dealloc workaround are their own responsibility.
- **`LinearBStackAllocator::alloc_bulk` documents zero-length entry aliasing** (`alloc` feature): Zero-length entries produce slices with the same offset as the immediately following non-zero slice (adding zero to the running offset leaves it unchanged). Multiple consecutive zero-length entries therefore compare equal by `(offset, len)`. This was always true; it is now documented so callers are not surprised.
- **`LinearBStackAllocator::dealloc_bulk` documents duplicate/overlap coalescing** (`alloc` feature): Duplicate or overlapping slice handles are silently coalesced rather than causing an error — only the bytes collectively covered are discarded once. This matches single-item `dealloc` semantics and is now documented explicitly.

- **`BStackReader`, `BStackSliceReader`, and `BStackSliceWriter` now implement `Copy`**: All cursor types can now be copied by value without an explicit `.clone()` call, making them more ergonomic to use in iterator-like patterns and when passing to functions that don't need to move the original cursor. (`BStackSliceReader` and `BStackSliceWriter` require the `alloc` feature.)

### Added

- **`DebugCheckingAllocator`** (`alloc` feature): A wrapper allocator that tracks allocated regions in memory and validates all operations against them, providing strong guarantees that all allocations and deallocations are well-formed and correctly paired. Intended for testing and debugging; the tracking data structure is not optimised for performance or memory usage. See `debug_checking.rs` for details.

## [0.2.0] - 2026-05-15

### Changed

- **`BStackSlice::from_bytes` is now `unsafe`** (`alloc` feature): The method signature has changed to `pub unsafe fn from_bytes(allocator: &'a A, bytes: [u8; 16]) -> Self`. **Migration:** Callers must ensure the encoded offset and length lie within the bounds of the underlying allocator's payload. Each existing call site must be wrapped in an `unsafe` block.
- **`BStackAllocator` gains an associated error type** (`alloc` feature): The trait now requires `type Error: fmt::Debug + fmt::Display;`, and `alloc`, `realloc`, `dealloc`, `alloc_bulk`, and `dealloc_bulk` return `Result<_, Self::Error>` instead of `io::Result<_>`. All allocators provided by this library (`LinearBStackAllocator`, `FirstFitBStackAllocator`, `GhostTreeBstackAllocator`) set `type Error = io::Error`, preserving existing behaviour. **Migration:** Third-party `impl BStackAllocator` blocks must add `type Error = io::Error;` (or a custom type implementing `Debug + Display`) to compile.
- **`BStackAllocator` gains a `type Allocated<'a>` GAT** (`alloc` feature): The trait now requires `type Allocated<'a>: Copy + TryInto<BStackSlice<'a, Self>> where Self: 'a;`, and `alloc`, `realloc`, and `dealloc` traffic in `Self::Allocated<'_>` instead of `BStackSlice` directly. `BStackBulkAllocator` is updated in the same way: `alloc_bulk` returns `Vec<Self::Allocated<'_>>` and `dealloc_bulk` accepts `impl AsRef<[Self::Allocated<'a>]>`. All allocators provided by this library set `type Allocated<'a> = BStackSlice<'a, Self>`, so existing call sites are unaffected. **Migration:** Third-party `impl BStackAllocator` blocks must add `type Allocated<'a> = BStackSlice<'a, Self>;` to compile.

### Deprecated

- **`BStackSlice::new`** (`alloc` feature): Replaced by the explicitly-unsafe [`BStackSlice::from_raw_parts`] (see *Added* below). The old name hid that constructing an arbitrary `(offset, len)` slice can corrupt allocator metadata when the slice is later passed to `realloc` or `dealloc`. **Migration:** replace every call `BStackSlice::new(allocator, offset, len)` with `unsafe { BStackSlice::from_raw_parts(allocator, offset, len) }` and ensure the safety contract of `from_raw_parts` is upheld at that call site.

### Added

- **`BStackSlice::from_raw_parts(allocator, offset, len)` — `unsafe` constructor** (`alloc` feature): Explicit-unsafe replacement for the deprecated `BStackSlice::new`. The body is identical but the `unsafe fn` signature makes the caller's responsibility visible. The safety contract requires that `offset + len` does not overflow `u64`, and — critically — that **if the slice will be passed to `realloc` or `dealloc`** the `(offset, len)` pair must describe an allocation that was returned by `alloc` or a prior `realloc` on the same allocator instance; passing a sub-slice (from `subslice`/`subslice_range`) or a manually-forged offset may silently corrupt the allocator's persistent metadata.

- **`BStack::lock_up_to(n)` / `locked_len()` / `open_locked_up_to(path, n)`**: Declares the first `n` payload bytes permanently immutable for the lifetime of this open file. The boundary is monotonically growing, kept in memory only (not persisted — resets to `0` on every reopen), and never changes the on-disk format. Reads of ranges entirely within the locked region (`get`, `get_into`, `peek_into`) bypass the internal `RwLock` and serve the read directly with `pread(2)` (Unix) or `ReadFile` + `OVERLAPPED` (Windows). Writes (`set`, `zero`, `swap`, `swap_into`, `cas`, `process`, `atrunc`, `splice`, `splice_into`, `replace`) and shrink operations (`pop`, `pop_into`, `discard`, `try_discard`) return `InvalidInput` when their target overlaps the locked region. Callers that never call `lock_up_to` see only an uncontended atomic load and comparison added to each path.

- **`ManualAllocator`** (`alloc` feature): A singleton allocator with no backing [`BStack`]. `alloc` and `realloc` always return `Err(Unsupported)`; `dealloc` is a no-op. Intended for code that wants the `BStackSlice` type as a typed `(offset, len)` coordinate — serialised, compared, stored — while managing positions on the [`BStack`] directly rather than through an allocator. Obtain via `ManualAllocator::get()` (direct construction is prevented). Calling `stack()` or `into_stack()` panics; calling `read`/`write` on slices backed by this allocator will also panic. Implements `BStackSliceAllocator` and benefits from the same migration path.
- **`BStackSliceAllocator` convenience supertrait** (`alloc` feature): A supertrait that bundles `BStackAllocator<Error = io::Error>` and `for<'a> BStackAllocator<Allocated<'a> = BStackSlice<'a, Self>>` into a single bound, covering the common case where no custom handle or error type is needed. All allocators provided by this library implement it automatically via a blanket `impl`. Requires `'static` (implied by the `for<'a>` HRTB) — all provided allocators satisfy this. **Migration:** replacing `A: BStackAllocator` with `A: BStackSliceAllocator` in generic bounds is sufficient to satisfy all of the breaking `BStackAllocator` changes above in one step.

## [0.1.9] - 2026-05-07

### Changed

- **Restructured allocators into multiple files**: `alloc` now contain three files — `mod.rs` for the public API, `linear.rs` for `LinearBStackAllocator`, and `first_fit.rs` for `FirstFitBStackAllocator` — to improve organization and readability as the allocator codebase grows. The main `lib.rs` re-exports the public API from `alloc/mod.rs` when the `alloc` feature is enabled, so no API changes are required for users of the allocators.
- **`FirstFitBStackAllocator` allocation methods now optimize for zero-length slices**: `alloc(0)` returns a valid zero-length slice at offset 0; `realloc` can grow or shrink to/from zero length; `dealloc` of a zero-length slice is a no-op. This allows users to treat zero-length allocations as normal slices without actually allocating any space in the file, which is a common pattern for representing empty values or optional data. The change is backward-compatible.

### Added

- **`BStackSlice::read_range`** (`alloc` feature): Reads the relative range `[start, end)` into a freshly allocated `Vec<u8>`.
- **`BStackBulkAllocator` trait** (`alloc` feature): Extension trait for [`BStackAllocator`] that adds two required, atomic bulk methods — alloc_bulk` and `dealloc_bulk`.  Both methods must either succeed completely or leave the backing store unchanged; partial state is never permitted. 
- **Implement `BStackBulkAllocator` for `LinearBStackAllocator`**. `alloc_bulk` reduces all allocations to a single `BStack::extend` call (one durable
  sync for the whole batch) and `dealloc_bulk` reclaims the largest contiguous tail region covered by the supplied slices with a single `BStack::discard` call.
- **`BStackGuardedSlice` trait** (`guarded` feature): Lifecycle-hook slice abstraction for transparent I/O interception. Provides four hook methods (`pre_read`, `post_read`, `pre_write`, `post_write`) to intercept and transform read/write operations. Includes support for narrowed subviews and atomic marker traits (`BStackAtomicGuardedSlice`, `BStackAtomicGuardedSliceSubview`) with crash-safety guarantees.
- **`GhostTreeBstackAllocator`** (`alloc` feature): A pure-AVL general-purpose allocator with zero-overhead live allocations. Free blocks store their AVL node inline, and the tree is keyed on `(size, address)` for best-fit allocation. Provides O(log n) allocation and deallocation with crash recovery through tree rebalancing on mount.
- **`BStackBulkAllocator` for `GhostTreeBstackAllocator`**: `alloc_bulk` rounds each requested length up to 32 bytes individually, sums them into a single contiguous block allocation (one AVL remove or one `extend` — crash-safe by construction), then slices it into per-request regions. `dealloc_bulk` sorts slices by address, merges contiguous ones (so slices from the same `alloc_bulk` call collapse into one block), and frees each merged group via tail-truncate (`discard`) when at the tail, or zero + AVL insert otherwise.
- **Tail truncation optimisation for `GhostTreeBstackAllocator`**: `dealloc` and `realloc` shrink now detect when the freed region is at the BStack tail and call `BStack::discard` directly instead of zeroing and inserting into the AVL tree. `realloc` grow at the tail calls `BStack::extend` in-place — no copy, no free, O(1).

## [0.1.8] - 2026-05-01

### Changed

- **Immutable buffer parameters widened from `&[u8]` to `impl AsRef<[u8]>`**: `push`, `set`, `atrunc`, `splice`, `try_extend`, `swap`, and `cas` (`old` and `new`) now accept any type that implements `AsRef<[u8]>` — including `Vec<u8>`, `Box<[u8]>`, `[u8; N]`, and `String` — without requiring an explicit `as_slice()` or `&buf[..]` conversion. The `new` parameter of `splice_into` is also widened. All existing callers passing `&[u8]` or byte-string literals continue to compile unchanged.
- **`BStackSlice::write` and `BStackSlice::write_range` widened from `&[u8]` to `impl AsRef<[u8]>`** (`alloc + set` features): same widening applied to the slice write methods for consistency.
- **FirstFitBStackAllocator version bumped to 0.1.1**: Updated magic number from `ALFF\x00\x01\x00\x00` to `ALFF\x00\x01\x01\x00`. This is still compatible with the previous version, so existing files can be read and written by both versions, but the new magic allows the allocator to detect whether the fixes in 0.1.6 are present.

### Fixed

- **`FirstFitBStackAllocator::dealloc` and `realloc` — validation rejected valid slices with user-visible length < 16**: both methods validated the input slice using the raw user-visible `slice.len()`, passing it directly to `is_impossible_block_size` and `is_impossible_block_end`. Because `align_len` guarantees the underlying block is always at least 16 bytes, a slice allocated with `len < 16` (e.g. `alloc(5)`) is perfectly valid, but the check `5 < MIN_BLOCK_PAYLOAD_SIZE` returned "impossible" and the call returned `InvalidInput`. Fixed by computing `aligned_len = align_len(slice.len())` first and using that for all size and end-offset checks.
- **`FirstFitBStackAllocator::dealloc` and `realloc` — wrong block boundary for small slices**: the tail-block detection condition used `slice.end().next_multiple_of(8)`, where `slice.end() = slice.start() + slice.len()`. For a 5-byte slice the rounded end (8) does not equal the actual block boundary (`slice.start() + 16`), so the fast tail-discard path was never taken and the block was incorrectly routed through `add_to_free_list` instead. The `discard` call on that path also used `slice.len().next_multiple_of(8)` (8 bytes) instead of `align_len(slice.len())` (16 bytes), leaving 8 bytes of garbage at the stack tail. Fixed to use `slice.start() + aligned_len` for both the condition and the discard amount.
- **`FirstFitBStackAllocator::find_large_enough_block` — end-of-list treated as corrupt pointer**: after advancing `head = next_free`, the function immediately called `is_impossible_block_start(head)` before the `while head != 0` loop guard could re-check. A `next_free` value of `0` (normal end-of-list sentinel) was therefore flagged as an invalid offset and the function returned `InvalidData`, causing any allocation attempt to fail when the free list contained at least one block that was too small. Fixed by guarding the check with `if head != 0 && is_impossible_block_start(head)`.

## [0.1.7] - 2026-04-28

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
  - **`BStack::replace(n, f)`** *(requires `atomic`)*: Pop `n` bytes off the tail, pass them read-only to a callback `f: FnOnce(&[u8]) -> Vec<u8>`, then write whatever `f` returns as the new tail. The entire read-callback-write sequence holds the write lock, so no other thread can observe the intermediate state. The file grows or shrinks according to the returned `Vec` length, using the same crash-safe two-path ordering as `atrunc`. `n = 0` is valid: the callback receives an empty slice.
  - **`BStack::process(start, end, f)`** *(requires `set` + `atomic`)*: Read bytes in the half-open logical range `[start, end)`, pass them to a callback `f: FnOnce(&mut [u8])` that mutates them in place, then write the modified bytes back. The entire read-callback-write sequence holds the write lock, so no other thread can observe the intermediate state. The file size is never changed. `start == end` is a valid no-op.

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
