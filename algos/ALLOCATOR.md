# Allocator implementations

Detailed algorithm and on-disk format documentation for each `BStackAllocator`
implementation provided by this crate.

---

## `LinearBStackAllocator`

The reference bump allocator.  Regions are appended sequentially to the tail.

| Operation             | Without `atomic`  | With `atomic`         | Crash-safe |
|-----------------------|-------------------|-----------------------|------------|
| `alloc`               | `BStack::extend`  | `BStack::extend`      | yes        |
| `alloc_bulk`          | `BStack::extend`  | `BStack::extend`      | yes        |
| `realloc` grow        | `BStack::extend`  | `BStack::try_extend`  | yes        |
| `realloc` shrink      | `BStack::discard` | `BStack::try_discard` | yes        |
| `dealloc` (tail)      | `BStack::discard` | `BStack::try_discard` | yes        |
| `dealloc` (non-tail)  | no-op             | no-op                 | yes        |
| `dealloc_bulk` (tail) | `BStack::discard` | `BStack::try_discard` | yes        |

`realloc` returns `io::ErrorKind::Unsupported` for non-tail slices.  With the
`atomic` feature, `realloc` also returns `Unsupported` when another thread
races to move the tail first — identical semantics to the non-tail case.

### Thread safety

Without the `atomic` feature, `LinearBStackAllocator` is **`Send`** but **not
`Sync`**: `realloc` and `dealloc` read the tail length and modify it in two
separate steps, creating a TOCTOU race under concurrent `&self` access.

With the `atomic` feature, `LinearBStackAllocator` is **`Send + Sync`**.
`alloc` and `alloc_bulk` use `extend`, which is serialized by `BStack`'s write
lock and returns a distinct region to each caller.  `realloc` uses
`try_extend`/`try_discard` and `dealloc`/`dealloc_bulk` use `try_discard`:
each fuses the tail-length check and the modification into a single locked
step, so concurrent calls cannot corrupt each other.

---

## `FirstFitBStackAllocator` (`alloc + set` features)

A persistent first-fit free-list allocator.  Freed regions are tracked on disk
in a doubly-linked intrusive free list and reused for future allocations, so
the file does not grow without bound.

### On-disk layout

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

### Allocation policy

`alloc` walks the free list from the head and takes the first block whose size
≥ the aligned request (**first-fit**).  If the found block is large enough to
yield a remainder of at least 16 bytes after splitting, the remainder is left
as a new free block; the allocated portion is carved from the back.  When no
free block fits, the arena is extended by pushing a new block onto the stack.

### Coalescing

`dealloc` merges the freed block with adjacent free neighbours (right then
left).  If the merged block reaches the stack tail it is discarded immediately.
A cascade check removes any further free blocks newly exposed at the tail,
maintaining the invariant that the tail block is always allocated.

### Crash consistency

Multi-step operations set a `recovery_needed` flag in the allocator header
before mutating the free list and clear it after all writes complete.  On the
next `FirstFitBStackAllocator::new`, if `recovery_needed` is set, a single
linear scan of the arena rebuilds the free list from the `is_free` flags in
block headers — stored pointer values are not trusted.  Any partial tail block
is also truncated.

### Thread safety

`FirstFitBStackAllocator` is always **`Send`** — ownership can be transferred to
another thread.

Without the `atomic` feature it is **not `Sync`**: operations take `&self` and
mutate the on-disk free list in several steps, so concurrent `&self` access from
multiple threads would race on that state.  Use one instance from at most one
thread at a time.

With the `atomic` feature it is **`Send + Sync`**.  An internal
`std::sync::Mutex` serializes the two compound operations that `BStack`'s own
per-call write lock does not already make atomic: mutating the free list and
extending/discarding the stack tail.  Operations that touch only caller-owned
bytes inside an already-allocated block — growing in place within the existing
block, or zeroing within the same alignment bucket — run without the lock.  The
`recovery_needed` flag is updated with a compare-and-swap (no extra cost over the
disk write it must perform anyway) and additionally rejects operating on a stack
left in a needs-recovery state.

Unlike `LinearBStackAllocator`, which uses optimistic `try_extend`/`try_discard`
and reports a lost tail race as `Unsupported`, a contended `FirstFit` operation
*blocks* on the mutex and proceeds once the lock is free.

---

## `GhostTreeBstackAllocator` (`alloc` feature)

A pure-AVL general-purpose allocator built on top of a `BStack`. Free blocks
store their AVL node inline at offset 0 within the block — live allocations
carry **zero** overhead (no headers, no footers). The tree is keyed on
`(size, address)` for a strict total order. All memory is kept zeroed: the
BStack zeroes on extension, and the allocator zeroes on free.

Implements both `BStackAllocator` and `BStackBulkAllocator`.

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

### On-disk layout

```
┌─────────────────────────────┐  payload offset 0
│   User-reserved (32 bytes)  │
├─────────────────────────────┤  offset 32
│   Magic number (8 bytes)    │  "ALGT\x00\x01\x02\x00"
├─────────────────────────────┤  offset 40
│   AVL root pointer (8 B)    │  absolute payload offset of the root node
├─────────────────────────────┤  offset 48  ← arena start (32-byte aligned)
│   ... heap grows upward ... │
└─────────────────────────────┘
```

### Allocation policy

`alloc` searches the AVL tree for the smallest free block that can satisfy the
request (best-fit). If no suitable block exists, the arena is extended.

`alloc_bulk` rounds each requested length up to 32 bytes individually, sums
them, and allocates one contiguous block (one AVL remove or one `extend`). The
block is sliced into per-request regions. When all slices are returned together
to `dealloc_bulk`, adjacent slices are merged and freed as a single operation
— typically one `discard` if they are at the tail.

### Crash consistency

No write-ahead log, no checksums. A crash during `dealloc` before the AVL
insert permanently loses that block. A crash during rotation leaves the tree
imbalanced — corrected on the next `GhostTreeBstackAllocator::new`.

### Thread safety

`GhostTreeBstackAllocator` is always **`Send`** — ownership can be transferred
to another thread.

Without the `atomic` feature it is **not `Sync`**: all allocator operations
take `&self` and mutate the on-disk AVL tree, so concurrent shared access from
multiple threads would race on that state.  Each instance must be used from at
most one thread at a time.

With the `atomic` feature it is **`Send + Sync`**.  An internal `Mutex`
serialises all AVL tree mutations; tail operations use
`BStack::try_discard` / `BStack::try_extend_zeros`, which check-and-act
atomically under `BStack`'s own write lock without holding the allocator
mutex.

---

## `SlabBStackAllocator` (`alloc + set` features) *(Experimental)*

A fixed-block slab allocator.  All blocks in the arena are exactly `block_size`
bytes (must be ≥ 8) with **no** per-block header or footer.  Freed blocks form
an intrusive singly-linked free list stored in the first 8 bytes of each free
block; live allocations carry zero metadata overhead.

### On-disk layout

```
[ reserved(24) | magic[8] | block_size[8] | free_head[8] | arena ... ]
  ^               ^
  offset 0        offset 24 (allocator header start)
  user data       offset 48 (arena start)
```

* **`magic`** — `"ALSL\x00\x01\x01\x00"` (version 0.1.1).
* **`block_size`** — fixed size of every arena block, little-endian `u64`.
* **`free_head`** — payload offset of the first free block's first byte, or `0` (sentinel).
* **Free block** — first 8 bytes hold the payload offset of the next free block (`u64` LE, `0` = end of list); remaining bytes belong to the caller when live.

### Allocation policy

| Request            | Strategy                                                 |
|--------------------|----------------------------------------------------------|
| `len == 0`         | Null handle (offset 0, len 0)                            |
| `len ≤ block_size` | Pop from free list; extend tail by `block_size` if empty |
| `len > block_size` | Extend tail by `⌈len / block_size⌉ × block_size`         |

The returned slice covers exactly `len` bytes; backing blocks have no visible overhead.

### Deallocation policy

| Case                    | Strategy                                                    |
|-------------------------|-------------------------------------------------------------|
| Oversized block at tail | `BStack::discard` (single call; crash-safe)                 |
| All other blocks        | Segment into `block_size` chunks; prepend each to free list |

Slab blocks at the tail are added to the free list (not discarded) so they can be reused without searching.

### Crash consistency

Each free-list mutation is two `BStack` calls: write the next-pointer into the block, then update `free_head` in the header.  A crash between the two calls leaks the block being added or removed but leaves the rest of the free list intact.  No recovery scan is required on reopen.

### Thread safety

`SlabBStackAllocator` is always **`Send`** — ownership can be transferred to another thread.

Without the `atomic` feature it is **not `Sync`**: free-list mutations require a read then a write of `free_head` as separate `BStack` calls — a TOCTOU race under concurrent `&self` access that can result in two callers receiving the same block.

With the `atomic` feature it **is `Sync`** with no allocator-level lock at all. Free-list pop drives a single `BStack::process_gen` sequence that holds `BStack`'s write lock across the read of `free_head`, the read of the popped block's `next` pointer, and the write that advances `free_head` — closing the ABA window a `get`/`cas` pair would leave open. Free-list push splices a single block (or a whole freed run) onto the head with one `BStack::cross_exchange`. Tail grow/shrink use `try_extend_zeros` / `try_discard`, which check-and-act atomically under `BStack`'s own write lock. Every concurrent `&self` operation is therefore safe through `BStack`'s interior mutability alone — no `Mutex`.

### Constructors

| Constructor                                   | Stack         | Effect                                                                                                            |
|-----------------------------------------------|---------------|-------------------------------------------------------------------------------------------------------------------|
| `SlabBStackAllocator::new(stack, block_size)` | **empty**     | Writes the 48-byte allocator header; fails with `InvalidInput` if the stack already has data.                     |
| `SlabBStackAllocator::open(stack)`            | **non-empty** | Reads and validates the stored header; fails with `InvalidInput` if the stack is empty or has a mismatched magic. |

---

## `CheckedSlabBStackAllocator` (`alloc + set` features) *(Experimental)*

A crash-recoverable fixed-block slab allocator. Like `SlabBStackAllocator`, every block is the same physical size on disk. Unlike it, each block carries an 8-byte **overhead** prefix that records whether the block is free or in use, making leaked blocks recoverable by a linear scan after a crash and catching double-frees at runtime before the free list can be corrupted.

The constructor takes `data_size` — the number of usable bytes per block (must be ≥ 8). The physical block size stored on disk is `data_size + 8`.

### On-disk layout

```
[ reserved(24) | magic[8] | block_size[8] | free_head[8] | arena ... ]
  ^               ^
  offset 0        offset 24 (allocator header start)
  user data       offset 48 (arena start)
```

* **`magic`** — `"ALCK\x00\x01\x01\x00"` (version 0.1.1).
* **`block_size`** — `data_size + 8`, little-endian `u64`.
* **`free_head`** — block start offset of the first free block, or `0` (sentinel; no valid block starts at offset 0).

Every block in the arena has the shape:

```
[ overhead(8) | data(data_size) ]
```

The overhead field encodes block state:

| Value                   | Meaning                                                                                     |
|-------------------------|---------------------------------------------------------------------------------------------|
| `0x0000_0000_0000_0000` | Block is free. `data[0..8]` holds the next-free block offset (`u64` LE, `0` = end of list). |
| `0x8NNN_NNNN_NNNN_NNNN` | Block is in use; low 63 bits are the allocation's block count; high bit is always 1.        |

A multi-block allocation stores the overhead **only in the first block**; the remaining `data_size` bytes of the first block and all subsequent blocks form one contiguous data region. A linear crash-recovery scan advances by the block count at live allocations and by one block at free blocks.

### Allocation policy

| Request           | Strategy                                                     |
|-------------------|--------------------------------------------------------------|
| `len == 0`        | Null handle (offset 0, len 0)                                |
| `len ≤ data_size` | Pop from free list; extend tail by one `block_size` if empty |
| `len > data_size` | Extend tail by `⌈(len + 8) / block_size⌉ × block_size` bytes |

Multi-block requests always extend the tail — the free list holds single blocks only.

### Deallocation policy

| Case                                   | Strategy                                            |
|----------------------------------------|-----------------------------------------------------|
| Already free (overhead high bit clear) | Return `InvalidInput` double-free error immediately |
| Multi-block allocation at tail         | `BStack::discard` (single call; crash-safe)         |
| All other cases                        | Each block becomes a free-list node                 |

### Crash consistency

Free-list mutations write block payloads before updating `free_head`. The overhead high bit is flipped to "in use" only after the block's data is clean, so a crash at any intermediate point leaks at most one block without corrupting the free list. A linear arena scan can reconstruct a valid free list from scratch if needed.

### Thread safety

`CheckedSlabBStackAllocator` is always **`Send`** — ownership can be transferred to another thread.

Without the `atomic` feature it is **not `Sync`**: free-list mutations read then write `free_head` as separate `BStack` calls — a TOCTOU race under concurrent `&self` access.

With the `atomic` feature it **is `Sync`**. `alloc` / `dealloc` / `realloc` take no allocator-level lock: free-list pop uses a single `BStack::process_gen` sequence, free-list push uses `BStack::cross_exchange`, and tail grow/shrink use `try_extend_zeros` / `try_discard` — all check-and-act atomically under `BStack`'s own write lock (the shrink path writes the overhead before the tail check, since the overhead must be committed before discarding). The one retained `Mutex` is held only by `recover`, to keep recovery single-flight (two concurrent runs could otherwise reclaim the same leaked block twice); the recovery scan itself is serialised against alloc/dealloc/realloc by the `BStack` write lock it holds across one `process_gen` sequence, not by the `Mutex`.

### Constructors

| Constructor                                         | Stack         | Effect                                                                                                                                                                                                                                                           |
|-----------------------------------------------------|---------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `CheckedSlabBStackAllocator::new(stack, data_size)` | **empty**     | Writes the 48-byte allocator header; fails with `InvalidInput` if the stack already has data or `data_size < 8`.                                                                                                                                                 |
| `CheckedSlabBStackAllocator::open(stack)`           | **non-empty** | Reads and validates the stored header, then runs `recover()` automatically. Fails with `InvalidData` on magic mismatch, invalid block size, or misaligned arena.                                                                                                 |
| `CheckedSlabBStackAllocator::recover()`             | any           | Reclaims leaked blocks and discards orphaned tails left by an unclean shutdown. Returns the count of blocks that could not be classified with certainty (`0` = fully accounted for). Called automatically by `open`; exposed for explicit inspection or re-runs. |
