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

`realloc_inplace` (via `BStackInPlaceResizeAllocator`) delegates to `realloc`
when `prepend == 0` and returns `Unsupported` for any `prepend != 0`: a bump
allocator cannot move the front edge without relocating the whole payload.

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

### In-place resize (`realloc_inplace`)

`BStackInPlaceResizeAllocator::realloc_inplace(handle, prepend, append)` moves
either edge without relocating the retained bytes; the returned range is exactly
`(start - prepend, end + append)`.

An **empty handle** (`len == 0`) anchors no on-disk region, so it has no position
for that guarantee to honor: every `(prepend, append)` on it — the no-op
included — is `Unsupported`, uniformly across all allocators. Growing from empty
is a fresh `alloc`, not a resize. This is a pre-mutation rejection, so the handle
is always returned intact.

| `(prepend, append)`            | Path                                                                  |
|--------------------------------|-----------------------------------------------------------------------|
| `append <= 0`, `prepend == 0`  | narrow the visible length within the block (no I/O)                    |
| `append > 0`, `prepend == 0`   | same-block if it already fits, else tail-extend, else merge the following free block (shared with `realloc`), else `Unsupported` |
| `prepend < 0`, `append <= 0`   | carve the front into a free block (add-to-free-list), narrow the back  |
| `prepend > 0`, `append == 0`   | grow the front from a free left neighbour (shrink it, or absorb it)    |
| mixed grow/shrink across edges | `Unsupported`                                                          |

Front shrink requires a trim `pf ≥ BLOCK_OVERHEAD_SIZE + MIN_BLOCK_PAYLOAD_SIZE`
(40) that is 8-aligned, so the carved-off front `[start, start + pf)` is a valid
free block (payload `pf − 24`); a smaller or misaligned trim is `Unsupported`.
The carve writes the inner footer + retained header, then the retained footer,
then shrinks the front header **last**, reproducing the split shape that
`unlink_block` produces — so a torn write is repaired by recovery's partial-split
detection.

Front grow requires `pg ≥ BLOCK_OVERHEAD_SIZE` (24), 8-aligned, and a free left
neighbour found via its footer tag that either keeps a valid remainder (`pg ≤
lsize − MIN_BLOCK_PAYLOAD_SIZE`) or is fully absorbed (`pg == lsize + 24`). The
shrink-remainder path uses the mirror three-write shape (neighbour footer + our
header, our footer, shrink neighbour header last) with the free block on the
left; the `pg ≥ 24` floor keeps those writes clear of the old boundary tags, so
a torn write walks back to the original pair or forward to the merged result.
The absorb path unlinks the neighbour then overwrites its header; a crash before
that leaves it marked free for recovery to relink.

Once mutation begins the original handle can no longer be returned: a mid-op I/O
failure yields `handle: None` and leaves `recovery_needed` set (reopen to
recover). Negative resulting length returns `InvalidInput` (not a panic) with the
handle intact.

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

## `GhostTreeBstackAllocator` (`alloc + set` features)

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

### In-place resize (`realloc_inplace`)

**Shrinking** either or both edges (`prepend ≤ 0`, `append ≤ 0`) and the
identity are supported; any **grow** returns `Unsupported`. The zero-overhead
layout is why: a block has no header, so its size is derived from the handle
length (`align_up_len`), and there is no boundary tag to find a neighbour to grow
into — a grow would have to move.

A shrink carves the block into up to three pieces: the front residue `[start,
start + pf)`, the retained window `[start + pf, start + pf + align_up_len(new_len))`,
and the back residue after it. `pf` (the front trim) must be `MIN_ALLOC`-aligned;
since it, `align_up_len`, and the block size are all `MIN_ALLOC` multiples, each
residue is `0` or `≥ MIN_ALLOC` — never a sub-block sliver. Each nonzero residue
is zeroed then AVL-inserted as its own free block (the same per-region insert
`dealloc`/`dealloc_bulk` use); `pf == 0` is a pure back shrink.

Crash safety mirrors the non-tail tail-shrink: both residues are zeroed first (a
fault there keeps the original, `handle: Some`), then inserted under the lock.
Once the first insert begins a torn insert is `handle: None` (the block lost,
same contract as `dealloc`); a crash between the two inserts frees one residue
and leaks the other, never touching the retained window's bytes.

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

## `SlabBStackAllocator` (`alloc + set` features)

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

### Bulk operations (`atomic` feature)

With `atomic`, implements `BStackBulkAllocator`; each request yields its own freeable handle (not one sliced block).

| Operation      | Strategy                                                                                                                             |
|----------------|-------------------------------------------------------------------------------------------------------------------------------------|
| `alloc_bulk`   | Pop single-block requests from the free list in one `process_gen` chase; serve oversized runs and any overflow from one `extend`; scrub recycled blocks in one streamed `inplace_gen` (shared buffer) |
| `dealloc_bulk` | Thread the whole batch into one chain with one `set_batched`, then splice onto the free list with one `cross_exchange` (freed runs go to the list, never discarded) |

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

## `CheckedSlabBStackAllocator` (`alloc + set` features)

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

### Bulk operations (`atomic` feature)

With `atomic`, implements `BStackBulkAllocator`; each request yields its own freeable handle.

| Operation      | Strategy                                                                                                                                       |
|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| `alloc_bulk`   | Pop single-block requests from the free list in one `process_gen` chase; serve oversized runs and any overflow from one `extend`; write every overhead tag (and scrub recycled blocks) in one streamed `inplace_gen` |
| `dealloc_bulk` | Validate/reject double-frees first (including a block repeated within the batch); clear every freed overhead and thread the chain in one `set_batched`, then splice with one `cross_exchange`. A crash leaves only `recover`-reclaimable zero-overhead leaks |

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

---

## `SegregatedBStackAllocator` (**experimental**, `alloc + set` features)

> **Experimental.** The on-disk format and API are not yet stable, a few resize
> paths behave differently between the `atomic` and non-`atomic` builds, and the
> background coalescer and deep in-use-leak reclamation are not yet implemented.

[`CheckedSlabBStackAllocator`] generalised from one block size to **33 size
classes** sharing one arena. Each class is an independent intrusive free list;
the class is derived from the request with register arithmetic (no tables), so
classed alloc/dealloc are O(1). Every block carries the same 8-byte overhead tag
as the checked slab.

### Size-class scheme

Fixed at compile time (encoded by the magic version), quantum 16:

* **16 linear classes** — 16, 32, …, 256 (step 16).
* **16 geometric classes** — octaves `[256, 4096)`, 4 subclasses each: 320, 384,
  448, 512; 640, 768, 896, 1024; 1280, 1536, 1792, 2048; 2560, 3072, 3584, 4096.
* **1 shared oversized bucket** — above 4096 B.

Sizes are physical block sizes (payload + 8) and multiples of 16; **33 free-list
heads** total (`NUM_CLASSES`), the last oversized. Helpers: `phys_need(len) =
round_up(len + 8, 16)`; `class_blocksize(need)` snaps up to the enclosing class;
`largest_class_le(v)` snaps down (for the carve).

### On-disk layout

```text
offset  0  reserved (user)                24 B
offset 24  magic  "ALSG\x00\x02\x00\x00"   8 B
offset 32  _reserved                       8 B
offset 40  free_head[33] : u64           264 B   # last entry = oversized list
offset 304 arena start (16-B aligned)
```

Each block is `[ overhead(8) | data(block − 8) ]`; the caller pointer is the data
start (`block_start + 8`, always `16n + 8`). The overhead is one tagged word
carrying the **physical block size `>> 4`** (also the class tag) in its low 63
bits under **both** tags: **high bit set ⇒ in use**, **high bit clear ⇒ free**.
The caller's visible length is *not* stored on disk — it lives in the returned
handle — so a live block may be physically larger than its request needs. A free
block stores `next_free` inline at the data start; live blocks carry no overhead
beyond the word.

### Allocation policy

1. Compute `class_blocksize(phys_need(len))` and the class index.
2. **Classed** (`≤ 4096`): pop the class head, claiming the block by writing
   overhead and any copied prefix as one buffer; on a miss, grow a zero-filled
   block at the tail with one sparse `extend`.
3. **Oversized** (`> 4096`): pop the head if its stored size is `≥ need`. If the
   excess (`actual − block`) is below `SPLIT_MIN`, claim the **whole** popped
   block in one write, recording its full size; otherwise claim `block` bytes and
   carve the excess into ≤ 3 class free blocks. On a miss, extend.

`SPLIT_MIN` (minimum excess worth reclaiming vs. retaining as slack) is a tuning
dial, not a format constraint — a block records its own size either way. Initially
`LINEAR_MAX` = 256 B.

### Deallocation policy

Read the overhead at `ptr − 8`; reject a clear high bit (double-free). The block's
physical size comes straight from the word; the handle's length is trusted
(rejecting only a length too large to have fit the block). An oversized block at
the tail is `discard`ed back to the stack; every other block is spliced onto its
class head.

### Realloc

The caller's length lives in the returned handle, not on disk, so a resize that
fits the current block — or a shrink whose excess is retained — touches no
metadata at all.

| Case                                | Strategy                                                                 |
|-------------------------------------|--------------------------------------------------------------------------|
| Fits the block (`phys_need ≤ size`) | retain; no write (zero the new tail on grow)                             |
| Grow past block, at tail            | extend in place, record new size                                         |
| Shrink, excess `≥ SPLIT_MIN`        | `atomic`: drop excess (`Atrunc` or carve), one txn; non-`atomic`: retain |
| Shrink, excess `< SPLIT_MIN`        | retain; no write                                                         |
| Non-tail grow                       | *move*: alloc new class, copy prefix, free old                           |

The **greedy carve** takes the largest class `≤` the remainder, repeated: a region
`> 4096` becomes one oversized block; a classed region splits into ≤ 3
distinct-class pieces. Fixed stack buffers, no heap.

### Crash consistency

Every path either commits atomically or leaves an orphaned-but-recoverable block.

* **With `atomic`**: pops use `BStack::process_gen`; pushes and the in-place
  non-tail carve use `BStack::inplace_gen` (multiple writes, one journalled
  commit); tail grow/shrink use `try_extend_zeros` / `try_discard`.
* **Without `atomic`**: plain read-then-write / `len`-check-then-`extend`
  (each one durable write). A shrink's freed tail overlaps still-live caller
  bytes and cannot be dropped fault-safely without an atomic operation, so a shrink
  simply **retains** the excess in place (zero writes); the oversized-reuse carve
  (over free excess only) writes all freed pieces first and the claiming header
  last.

`recover()` rebuilds every free list with one linear scan of the overhead words:
a live block is strided by its **recorded physical size** (no length-to-class
derivation); a free block is relinked onto the head of the largest class `≤ size`
(`classify(largest_class_le(size))`, or the oversized head above 4096), which
reclaims leaked blocks and degrades a malformed non-class size to a leak rather
than a head that would overrun the block; a fully-zeroed tail is discarded. The rebuilt head table is published as one crash-atomic contiguous
write. The scan trusts only the overhead words and is idempotent. In-use orphans
(e.g. the old block of a crashed *move*) are left live; a deep GC to reclaim them
is future work. `recover` requires a quiescent allocator and is `unsafe`; `new`
runs it before the handle escapes.

### Thread safety

Always **`Send`**. With `atomic`, also **`Send + Sync`** with no allocator-level
lock — operations drive `process_gen`/`inplace_gen` under `BStack`'s write lock;
no retained `Mutex`. Without `atomic`, **not `Sync`** (pops/pushes read a head
then write without holding a lock across the pair).

### Constructors

| Constructor                                   | Stack         | Effect                                                                                                                                                                                                                                                                     |
|-----------------------------------------------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SegregatedBStackAllocator::new(stack)`       | **empty**     | Writes the header (magic + 33 zeroed heads) with one sparse extend to the arena start.                                                                                                                                                                                     |
| `SegregatedBStackAllocator::new(stack)`       | **non-empty** | Validates the magic prefix and arena alignment, then runs `recover()` automatically before returning. Fails with `InvalidData`/`UnexpectedEof` on mismatch or misalignment.                                                                                                |
| `unsafe SegregatedBStackAllocator::recover()` | any           | Reclaims leaks and discards orphaned tails. Returns the count of blocks that could not be classified with certainty (`0` = fully accounted for). `unsafe`: the caller must guarantee the allocator is quiescent (no concurrent operations). Called automatically by `new`. |
