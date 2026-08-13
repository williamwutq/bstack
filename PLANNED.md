# Planned Features

This document outlines upcoming features planned for the `bstack` crate. These enhancements aim to improve usability, performance, and integration while maintaining the core principles of durability, crash-safety, and simplicity. Changes aim to be backward-compatible. New features are suggested to be added as optional features under feature flags and new traits, instead of modifying existing ones, to avoid breaking changes. All features aim to follow [Rust's API design guidelines](https://rust-lang.github.io/api-guidelines/) and BStack's design principles.

---

## NOT PLANNED

### Deprecating `BStackGuardedSlice::as_slice` in favor of read-only access

Reasons:

`BStackGuardedSlice::as_slice` is not actually unsafe as data corruption through misuse doesn't violate memory safety or compromise allocator structure, so marking it unsafe would be misleading per Rust conventions. Documentation and API design already encourage using `read()` and `write()`. Callers using `as_slice()` are expected to understand the implications. In addition, the unsafe `raw_block()` method already exists for cases where hook bypass is needed, and its safety contract documents that hooks must be manually called. If this function is deprecated, it would break existing code, and callers who correctly use `as_slice()` for read-only purposes would need to migrate. Therefore, `as_slice()` as a safe function that returns a `BStackSlice` is sufficient, and the safety contract can be clearly documented without making it `unsafe fn`.

### Replacing `std::sync::RwLock` with `parking_lot::RwLock` or `usync::RwLock`

Reasons:

There are insufficient evidences that changing the implementation of RwLock will bring sufficient performance improvements to `BStack`. `std::sync::RwLock` remains the default. On Linux, `std` wins 10/14 benchmarks, often by 20–30% on write-heavy operations which dominate real workloads. The wins for `parking_lot` and `usync` are limited to fast read-path operations (`peek`, `get`, `len`) which are not the bottleneck.

For context, accepted optimizations like caching and locked region have demonstrated **~2-10× improvements**. The gains shown here do not meet that bar and introduce an additional dependency without a compelling cross-platform case.

Reference: https://github.com/williamwutq/bstack/pull/3

### Adding a singly-linked `BStackSList<T>`

Reasons:

A singly-linked variant saves 8 bytes per node by omitting `prev_ptr`, but any workload that reaches for a disk-backed linked list is already paying the cost of allocator round-trips and random-access I/O per element. An 8-byte savings per node is negligible against those overheads, and the restriction to forward-only traversal eliminates `push_front`-with-O(1)-`pop_back`, bidirectional cursors, and `split_off` without a full scan. `BStackList<T>` covers all singly-linked use cases with no meaningful extra cost, so a separate singly-linked type adds complexity without benefit.

### Making `BStackAllocator::realloc` and `dealloc` `unsafe fn`

Reasons:

While `realloc` and `dealloc` have a slice-origin requirement, the hazard window is now significantly narrowed because constructing a bad handle already requires `unsafe` (via `BStackSlice::from_raw_parts`). A well-reviewed `unsafe` block that constructs a `BStackSlice::from_raw_parts` can be expected to read the safety contract and comply with the origin requirement. The remaining concern is sub-slices: `subslice` and `subslice_range` are *safe* functions that produce slices with an origin different from any allocator-returned handle. However, marking `realloc` and `dealloc` as `unsafe fn` would force all call sites into an `unsafe` block, which may be unnecessarily burdensome given the already narrow hazard window. The best conventions use `unsafe` for operations that can cause undefined behavior and overuse of `unsafe` can lead to desensitization and misuse. As of `BStack` 0.2, the allocator interfaces are already mature and breaking changes should be avoided. Since the origin requirement is a safety contract that can be documented and enforced through careful API design, it may be sufficient to keep `realloc` and `dealloc` as safe functions while clearly documenting the requirements and risks.

Furthermore, `alloc`, `realloc`, and `dealloc` in the `BStackAllocator` trait do not need to operate on `BStackSlice` directly — they operate on an associated handle type. Custom allocator implementations can define handle types that do not support sub-slicing at all, eliminating the origin problem at the type level for those allocators. The sub-slice concern is therefore a consequence of a specific design choice (using `BStackSlice` itself as the raw handle) rather than an inherent flaw in the trait. The recommended approach is for allocators to use a handle type distinct from `BStackSlice`, where converting from handle to `BStackSlice` is straightforward but the reverse is not possible — making the origin requirement a type-level guarantee. The default allocators in this crate currently do not follow this recommendation, but that is a separate concern addressed in the planned features below.

### Adding `BStackVec<T>` for typed vector storage

Reasons:

A typed vector is a data structure, not an I/O mechanism, and `bstack`'s role is to abstract crash-safe atomic file I/O — the stack, allocators, and slices — not to provide collection types. Such structures compose on top of those mechanisms and belong downstream. `BStackByteVec` already covers the byte-buffer case, and end push/pop is already crash-atomic via a single `clen` write inherited from the stack, so a generic `BStackVec<T>` would mainly add a `bytemuck`/`zerocopy` dependency and POD-soundness surface for a generalization downstream consumers can build themselves. Keeping it out preserves the lean core and avoids freezing API surface ahead of 1.0.

### Adding `BStackList<T>` for typed doubly-linked list storage

Reasons:

As with `BStackVec<T>`, a linked list is a data-structure policy that belongs downstream rather than in the I/O core. The downstream `bllist` crate already provides on-disk doubly-linked lists built on `bstack`, with a no-corruption, recoverable-leak model: every block write is atomic, so a crash degrades to an orphaned node reclaimed on reopen rather than data corruption. The only capability that would justify a first-party type is crash-atomic multi-block structural mutation (`append`, `split_off`), which depends on the write-in-progress journaling primitive planned for 0.5.0 — and once that primitive is public, downstream crates can consume it to achieve the same guarantee. A `bstack`-native list would therefore never hold an exclusive capability. `bstack` should instead ship the mechanisms such structures need (allocators and, later, the public transaction primitive) and leave the structures themselves to downstream.

### Narrowing the `FirstFitBStackAllocator` mutex to guard only the free list

Reasons:

The mutex in `FirstFitBStackAllocator` serialises free-list mutation and tail extension/retraction. The size-conditional `try_extend`/`try_discard` primitives could pull the tail operations out from under it, leaving it to guard only the free list — but the win is small. Tail operations are not the common case, and a thread doing one still contends on the free-list mutex for the surrounding read-modify-write. While the free list remains a single mutex-guarded doubly-linked list (lock-free traversal is not possible yet), removing tail-op contention alone does not move the bottleneck — and it trades an obviously-correct design for an optimistic protocol resting on per-path ABA arguments (`try_*` checks payload *size*, not block identity).

### Adding a `Copy { from, to, len }` op to `BStackGenOp`

Reasons:

A single-shot `copy` is well-defined because there is exactly one source read and one destination write against a fixed state, so overlap can be rejected up front. Inside a generator (`process_gen`/`inplace_gen`) that guarantee dissolves. Chaining copies makes the source of a later copy depend on the destination of an earlier one — `Copy a→b` then `Copy b→a` has no single answer (does the second copy read `b`'s original bytes or the bytes the first copy just wrote?), and the choice silently changes the result. Worse, a sequence of individually non-overlapping copies can compose into an *effective* overlap that no per-op bounds check can catch, so the "regions must not overlap" invariant that makes standalone `copy` crash-atomic cannot be enforced across the batch. Resolving this would require either snapshotting the whole payload or defining a read-vs-write ordering that the crash-atomic commit model has no way to represent. Callers who need copy-like behaviour in a generator can express it explicitly with `Read` into a caller-owned buffer followed by `Write`, which makes the intended source state unambiguous.

### Adopting `extend_sparse` in the slab allocators

Reasons:

`SlabBStackAllocator::realloc` (grow-non-tail) and `CheckedSlabBStackAllocator::alloc` (tail-extend) each materialise a mostly-zero region — a `push` of a full zeroed buffer, and an `extend` followed by a separate overhead `set` — that `extend_sparse` could collapse into a single call (also making `CheckedSlab::alloc` crash-atomic, closing its extended-but-untagged orphan-tail window). Benchmarking showed the performance benefit is insignificant: tail growth is not the bottleneck and the elided zero-byte writes are cheap next to the surrounding I/O. The gain does not justify churning both allocators' on-disk writer version (a magic bump), and the minor crash-window improvement for `CheckedSlab::alloc` is not, on its own, compelling enough to pursue.

Reference: https://github.com/williamwutq/bstack/pull/32

### Implementing `PartialEq`, or any other trait, whose only implementation would perform I/O

Reasons:

Trait methods that callers invoke implicitly — `PartialEq::eq`, `Hash::hash`, `Ord::cmp` — are conventionally assumed cheap and infallible: no blocking I/O, no `Result`, no panicking on a failed read. That assumption is baked into `assert_eq!`, hash-map keys, `sort`/`dedup`, derived impls on a containing struct, and generic trait bounds. `BStack` keeps I/O explicit (`read()`/`get()`/`set()` and friends), so a trait whose only possible implementation must touch the file goes against that, whichever trait it is. `BStackByteVec` is the concrete case: it cannot compare content without first reading its header to resolve `len`, so a content-based `PartialEq` would silently issue disk I/O wherever `==` appears. Types that compare for free stay fair game — `BStackSlice`, `BStackOwnedSlice`, and `BStackRange` implement `PartialEq` against each other since `(offset, len)` comparison needs no I/O. 

Reference: https://github.com/williamwutq/bstack/pull/37

---

## `BStackInPlaceGuard` — ambient atomic-block guard over in-place writes

**Feature flag:** `set` + `atomic` (same gate as `inplace_gen`/`process_gen`)
**Breaking change:** No — purely additive.

### Motivation

`inplace_gen`/`process_gen` batch in-place reads/writes into one crash-atomic multi-write journal, but only for callers willing to speak `BStackGenOp`. Code already written against the ordinary `&BStack` API — including third-party code with no notion of an atomic block — can't be folded into one `inplace_gen` call without a rewrite.

`BStackInPlaceGuard` fixes this: acquire once, then call ordinary `BStack` methods (`set`, `get`, `swap`, `process`, …) from ordinary `&BStack`-taking functions. While held, those calls are transparently redirected into `inplace_gen`'s overlay/journal machinery, so an atomic block can be built around code that wasn't written for one.

### Design

- **Acquisition.** `BStack::inplace_guard(&self) -> BStackInPlaceGuard<'_>` takes the write lock, then, still holding it: sets a `thread_local!` marker (this thread holds a guard on this instance), populates overlay storage on `BStack` itself (`Mutex<Option<Overlay>>` — needed because intercepted methods see only `&self`, never the guard value; the `Mutex` is for `Sync`, not contention, since no other thread can be mid-method while the write lock is held), and sets an `AtomicBool` (`guard_acquired`) used solely by `is_inplace_guarded()`, not by routing. `BStackInPlaceGuard` holds the `RwLockWriteGuard` and is `!Send`.

- **Routing is thread-local, no cross-thread identity needed.** Two threads can never both hold the write lock, so a different thread's unmodified `self.lock.write()`/`.read()` already blocks correctly — no check required on that path. The only failure mode is the *same* thread re-entering its own guard (deadlock on the non-reentrant `RwLock`), which the `thread_local!` marker answers with no synchronization:
  - **Unset:** proceed via the existing lock path, unchanged.
  - **Set:** served from the overlay, per `inplace_gen`'s model:
    - Reads (`get`/`peek`/`len`, read half of `swap`/`cas`/`process`/`cross_exchange`/`copy`/`eq_crds`/`ne_crds`/…) resolve against committed bytes overlaid with pending writes (`inplace_overlay_read`); no read lock, write lock already suffices.
    - In-place writes (`set`/`zero`/`repeat`, write half of the above) stage into the overlay (`inplace_overlay_insert`), later overlapping writes winning, and return without touching disk.
    - Length-changing calls (`push`, `extend`, `pop`, `pop_into`, `discard`, `atrunc`, `splice`, `splice_into`, `try_extend*`, `try_discard(_, n>0)`, `replace`) return `Err(InvalidInput)` immediately, matching `inplace_gen`'s existing rejection, and discard the overlay — poisoning the transaction. An ordinary `Err` needs no special plumbing: `?` unwinds into `Drop`.
  - **Nested `inplace_guard`/`inplace_gen`/`process_gen`, same thread:** join the outer transaction — reads/writes stage into the same overlay, and the nested call's own end-of-sequence does *not* commit. Persistence defers to the outermost guard. `Ok(())` from a nested `inplace_gen` therefore does not imply persisted — the main implicit/sharp edge of this design.

- **Commit.** `BStackInPlaceGuard::commit(self) -> io::Result<()>` consumes the guard, runs the same reduction `inplace_gen` uses (0 writes → no-op, 1 → single-write path, many → `journaled_multi_set`), and suppresses the subsequent `Drop` (e.g. via `ManuallyDrop`/`mem::forget`) so it isn't run twice. `Drop::drop`, reached only if `commit` was never called, runs the identical reduction and `.unwrap()`s it — panics on I/O failure. A poisoned transaction commits/drops an empty overlay (no-op, `Ok(())`). The write lock releases only once this runs.

- **Query API.** `BStack::is_inplace_guarded(&self) -> bool` — `Acquire` load of `guard_acquired`; whether *any* thread holds a guard, independent of routing.

### Open questions

- **Thread-local keying.** One thread may hold guards on two different `BStack` instances at once (joining applies only within one instance), so the marker must be keyed per-instance (e.g. by address), not a single flag. Representation (thread-local set/small-map of instance pointers) is an implementation detail, not a soundness question.
- **Overlay ownership.** `inplace_gen`'s overlay borrows (`Vec<(u64, &'a [u8])>`) within one call's lifetime. A guard's overlay spans many independent calls with no shared lifetime (e.g. `swap`'s written data is a just-read value, not a caller buffer), so it likely needs to own its bytes (`Vec<(u64, Vec<u8>)>`) — a real memory/copy cost versus `inplace_gen`.
- **Overlay-aware `swap`/`cas`/`process`/`cross_exchange`/`copy`/`eq_crds`/`ne_crds`/`masked_eq_crds`/`masked_ne_crds`.** Each has its own direct read-then-write shape today; each needs a guarded variant reading/writing through the overlay. `cas`/`eq_crds`/`ne_crds` also need their comparison step to read through the overlay, or an earlier guarded write in the same transaction is invisible to it.
- **Recovery format.** Whether commit reuses the existing multi-write journal (`wip_aux = MultiWrite`) unchanged — the guard still reduces to a flat non-overlapping `[offset, data]` set — or needs its own `wip_aux` mode.
- **Naming.** `BStackInPlaceGuard`/`inplace_guard()`/`commit()`/`is_inplace_guarded()` are working names.

## External-merge-sort strategy and partial sort for `BStackChunk`

**Feature flag:** `alloc` + `set` + `atomic`, same as the `BStackChunk::sort_by`/`select_nth_by` family it extends.
**Breaking change:** No — purely additive; the current single-transaction `sort_by`/`sort_by_key`/`select_nth_by`/`select_nth_by_key` keep their existing behavior and signatures.

### Motivation

`BStackChunk::sort_by`/`sort_by_key`/`select_nth_by`/`select_nth_by_key` commit via a single `BStack::process` call: the aligned region is read whole, permuted in place (cycle-following, O(1) scratch chunks), and written back atomically. This is bounded by available memory — a region too large for one `Vec<u8>` can't be sorted this way. The staged small/medium/large strategy below would lift that bound; it was cut from the initial implementation as extra scope, not for lack of an atomicity primitive — `BStack::set_batched`/`inplace_gen` (`set` + `atomic`) already commit an arbitrary batch of non-overlapping writes as one crash-atomic multi-write-journal transaction, and are the natural fit for committing a multi-section sort's final output in one shot.

### Design

- **Section sort + merge**, applied recursively (external sort is section-sort-then-merge at every scale — "medium" and "large" from the original sketch are the same algorithm, just more sections and more merge passes; "small," i.e. what's shipped today, is the degenerate one-section case):
  1. Split the aligned region into sections that individually fit in memory.
  2. Sort each section in place (today's `sort_by`/`sort_by_key`, applied per-section).
  3. Merge adjacent sorted sections, repeating merge passes until one fully-sorted region remains.

  Section size and the small/medium/large threshold are implementation details (fixed constant, fraction of available memory, or caller-configurable), not part of the public contract.

- **Whole-sort atomicity across sections, via `BStack::set_batched`.** The final merge pass streams `(new_offset, chunk_bytes)` pairs — produced lazily while scanning the sorted sections, not all held in memory at once — into one `set_batched` call, so the whole multi-section sort commits as a single crash-atomic transaction: a crash leaves either the pre-sort order or the fully-sorted order, never an intermediate state. Only the final section-to-final-position write needs batching this way; each section's own in-place sort (step 2) stays on the existing per-section `process` call.

- **`sort_partial_by`/`sort_partial_by_key`** — best-effort: pushes through as many sections/merge passes as it can rather than stopping early by choice, and returns `Err` only for a genuine I/O failure, never merely for running out of passes or budget. "Partial" only ever means *not fully ordered*, never corrupted or lost data: every completed step (section sort, merge pass) is itself committed atomically, so the visible state on return is always some valid permutation of the original records, however far the sort got.

- **`select_nth_by`/`select_nth_by_key` for out-of-core data.** Quickselect's whole point is avoiding a full sort, so the section-and-merge strategy above doesn't directly apply — it needs its own out-of-core partitioning approach (e.g. section-local partitioning plus a pivot-selection pass across sections) rather than assuming in-memory partitioning over the whole range.

### Open questions

- **Unstable sort.** Whether to also provide `sort_unstable_by`/`sort_unstable_by_key`, mirroring `std`'s stable/unstable split — unstable sort does fewer chunk moves at the cost of stability, which may matter more once movement is spread across multiple sections/passes.
- **Threshold sizing.** How the small/medium/large boundary and section size are chosen, and whether it should be caller-configurable given the memory/atomicity tradeoff is now explicit.
- **Batch staging cost.** `set_batched` still stages every block's bytes into the multi-write journal before committing, so the final pass's staging cost scales with the number and size of the (new_offset, chunk_bytes) pairs it's fed — worth measuring against the current single-`process` cost before committing to this as the merge-commit strategy.

## `SegregatedBStackAllocator` (`alloc` + `set`)

**Feature flag:** `alloc` + `set`; `Send + Sync` only under `atomic` (see Thread safety).
**Breaking change:** No — new allocator type behind a new magic (`ALSG`); no existing allocator is modified.

### Motivation

`GhostTreeBstackAllocator` is the fastest *general-purpose* allocator in the crate, but it has structural cost - best-fit is a root-to-leaf AVL descent, issuing O(log N) dependent node reads on the critical path and additional rotation writes on mutation. Although PR #39 already squeezed that path for ~27% lower per-op latency, it did not remove its fundamental limitation. In addition, under `atomic`, the whole descent runs under the allocator `Mutex`, which means the chain also sets the length of the serialized critical section, capping threaded throughput.

Results from the benchmark requires an alternative for higher throughput. From the benchmark, we have a few observations to follow. First, on-hot-path coalescing introduces latency: `first_fit` is the only allocator that merges adjacent frees per-`dealloc`, and it is both the slowest general-purpose allocator and the one with pathological tails under contention.Second, a slab allocator configured to the **correct size** is the fastest thing measured (`slab_16/uniform`), because its pop is a single fixed-offset, fixed-shape fused critical section. a **wrong-sized** slab is the slowest (`slab_128`, `slab_24`).

The conclusion dictates the design of the modern `bstack` on disk allocator. It points out that the solution is to generalize `slab` from one size class to N. By keeping slab's superior performance and minimal critical section while allowing better size fitting for allocation requests, an allocator can process allocation and deallocation of arbitrary sizes at the same class of efficiency as `slab_16`.

### Design

- **Segregated (binned) free lists.** The header holds an array of `num_classes` free-list heads. Each is a singly linked list of free blocks, where the pointer `head == 0` meaning empty, and non-empty pointers should point to the next block. As the mimimum physical block size is 16, free blocks has enough space to store their `next_free` offset inline in their own payload, allowing for zero disk space overhead for live allocation (only the 8-byte state overhead, below). Each class is effectively an independent slab sharing one arena.

- **Hot path is fixed-shape.** `alloc` computes the class from the requested size with register arithmetic, then reads exactly one head at the computed offset `head_base + 8*class` and pops. On an exact-class miss (`head == 0`) it `try_extend`s one fresh block of that class's size and returns it. This is the `pop, else extend` rule slabs already use. It deliberately does **not** search neighbouring classes on a miss, as a data-dependent search would lengthen the critical section under contention, leading to suboptimal concurrency performance. Not only that, it also could introduced the need for new locks in memory, further degrading performance. Class drift/fragmentation is the background coalescer's job, off the hot path.

- **Size classes are physical block sizes, all multiples of 16.** On disk, we store a class size that includes the 8-byte overhead, so usable `data = class − 8`. Every class size is a multiple of 16. Defining classes on the physical size rather than data size allows splitting blocks to yield blocks that are exact class blocks, and coalescing two adjacent class blocks yields another multiple of 16, letting the overhead to be absorbed inside each piece. Therefore, the boundaries of blocks will stay clean over time, reducing internal fragmentation of the allocator.

  - **Linear floor:** block sizes 16, 32, 48, ..., up to `linear_max`, step 16. Pure power-of-two would waste too much space; the 16-step floor caps small-size waste at < 16 B. Index is direct: `class = (block >> 4) − 1`.
  - **Geometric ceiling** (blocks above `linear_max`): each octave `[2^k, 2^{k+1})` is split into `2^subclass_bits` evenly-spaced subclasses, each itself a multiple of 16. `subclass_bits` is a **compile-time constant** (not a stored, caller-tunable field) — it sets *how many* subclasses each power-of-two octave is divided into (`subclass_bits = 2` → 4 per octave), which bounds worst-case internal waste to `2^{−subclass_bits}` of the request (≤ 25% at 2, ≤ 12.5% at 3) in exchange for more classes (larger header). Index via `clz` (octave) + the top `subclass_bits` of the remainder (subclass) — a formula, never a stored table.

| block-size (class) region | class sizes (bytes, incl. 8 B overhead) | usable data        |
|---------------------------|-----------------------------------------|--------------------|
| linear (step 16)          | 16, 32, 48, … `linear_max`              | 8, 24, 40, …       |
| octave (256, 512] ÷4      | 320, 384, 448, 512                      | 312, 376, 440, 504 |
| octave (512, 1024] ÷4     | 640, 768, 896, 1024                     | …                  |
| …                         | … up to `max_class`                     | …                  |
| > `max_class`             | oversized (raw multi-16 path)           | —                  |

  Alloc rounds the *physical* need up to a class: `class = round_up(len + 8, 16)` then snap into the geometric region if above `linear_max`. Example: `len = 200` → need 208 B → linear class **208** (= 16×13, index 12), 200 B usable, exact fit. `len = 500` → need 508 → octave `(256,512]` → class **512**, 504 B usable. Minimum block is 16 (8 data), matching slab's `≥ 8` data floor.

- **On-disk layout.** The size→class scheme is a fixed compile-time policy, not per-file state — by the same logic that keeps `subclass_bits` out of the header, `quantum` (16), `linear_max`, and the resulting `NUM_CLASSES` are all constants baked into the build and *encoded by the magic version*, so a format change bumps the magic rather than reinterpreting a stored field. The header therefore carries only the magic, the recovery flag, and the head array; `open` validates the magic and arena alignment.

  ```
  offset  0  reserved (user)                24 B
  offset 24  magic  "ALSG\x00\x01\x00\x00"   8 B   # version encodes the fixed class scheme
  offset 32  flags                           4 B   # bit0 = recovery_needed (CAS-updated)
  offset 36  _reserved                       4 B
  offset 40  free_head[NUM_CLASSES] : u64 each     # the only per-op-mutated region;
             (pad to 32-B alignment)               #   last entry is the shared oversized list
  arena start (32-B aligned)
  ```

  `NUM_CLASSES` = (linear classes) + (geometric classes) + 1 for the oversized bucket, all fixed at compile time. Each `free_head[c]` holds the block-start offset of that class's first free block, `0` = empty.

  Every arena block is `[ overhead(8) | data(block − 8) ]`; the returned pointer `ptr` is the data start (`block + 8`). The 8-byte overhead is a single tagged word:

| bit(s) | in_use = 1 (high bit set)                                | in_use = 0 (high bit clear)          |
|--------|----------------------------------------------------------|--------------------------------------|
| 63     | `1` — block is live                                      | `0` — block is free                  |
| 0–62   | **slice length** — the exact bytes the caller was handed | **physical block size >> 4** — bytes |

  One word, two readings, tag in the top bit — the same high-bit-set convention `CheckedSlab` uses for "in use", so a raw scan reads the sign bit and knows immediately. The choices behind it:

  - **Live blocks store the caller's slice length, not the block size.** The block size is always recoverable as `classify_blocksize(len)` (a formula, no IO), so storing `len` is strictly more information: it lets `recover()` reconstruct exact `(ptr, len)` handles, lets `realloc` know the true occupancy of the current block, and still lets the scan stride by deriving the block size. This allows for smaller range to zero during realloc, if the zeroing strategy is zero-on-allocation and zero-on-grow, and also prevents bugs introduced by the caller by refusing to deallocate a partial slice or a slice originated from erroneous integer math. The physical size of the block can easily be obtained from the stored `len` (round up to the next class).
  - **Free blocks store their actual physical size, divided by 16**, which doubles as the class tag: `classify(size)` gives the free-list bucket with no separate class field, and the recovery scan strides directly by it. This is why there is **no oversized bit** — "oversized" is simply what `classify()` returns for any `size > max_class`, collapsing every large size onto one shared free-list head instead of minting a class (and a header slot) for `2^56`-style sizes that would bloat `NUM_CLASSES`. A huge free block records its true size right here in its own overhead; nothing extra is stored out of line. Since every physical block is a multiple of 16, the stored valus is the size shifted right by 4, which fits in the 63 bits available.
  - **`next_free` lives at `ptr` (data `[0..8]`).** This is different than the design of `CheckedSlabBStackAllocator` because the minimum block is 16 B (8 overhead + 8 data), every free block has at least 8 data bytes to hold the link, so the overhead word is free to keep the size/class tag *while* the block is free — the two never contend. Head slots and `next_free` both store block-start offsets; `0` is the empty/end sentinel (offset 0 is the header, never a block).

  So `dealloc` needs no stored class: read overhead → derive `size = classify_blocksize(len)` → `classify(size)` picks the head. And double-free is caught up front — if the high bit is already `0`, the block is already free → `InvalidInput` before any list write.

- **Operations.**

| operation | strategy |
|---|---|
| `alloc` (`len ≤ max_class`) | `c = classify(len)`; pop `head[c]`, else `try_extend` one class-`c` block; write overhead `in_use\|len` |
| `alloc` (`len > max_class`) | oversized: pop the oversized head if a block fits, else `try_extend` `round_up(len+8, 16)`; write overhead `in_use\|len` |
| `dealloc` | high bit clear → double-free `InvalidInput`; else `size = classify_blocksize(len)`, write overhead `free\|size` + `next_free` + splice to `head[classify(size)]` (one batch) |
| `dealloc` (oversized, at tail) | `try_discard` the whole block (single call) |
| `dealloc` (oversized, mid-arena) | push to oversized head — see Open questions (carve vs mark) |
| `realloc` (same class) | `len` still maps to this class → rewrite overhead `len` only; no list touch |
| `realloc` (grow at tail) | `try_extend_zeros` in place, rewrite `len` |
| `realloc` (cross-class-shrink) | rewrite overhead `len`, split maximum unused tail into free blocks of the same class (this is done to reduce the number of lists touched), push new free blocks onto `head[classify(size)]` (multi-transaction; `recovery_needed`-bracketed) |
| `realloc` (cross-class-grow) | alloc new class, copy, dealloc old (multi-transaction; `recovery_needed`-bracketed) |

- **Atomicity mapping (`atomic`).** Reuses slab's primitives, so there is **no allocator-level lock on the alloc/dealloc/realloc paths**. The key lever: `BStack::set_batched` / `inplace_gen` (`set` + `atomic`) commit an arbitrary batch of *non-overlapping* writes as one crash-atomic multi-write-journal transaction. A free-list mutation's three writes — the overhead word (block `[0..8]`, `free\|size` ⇄ `in_use\|len`), `next_free` (data `[0..8]`), and `head[class]` (header) — are non-overlapping, so they land in a **single** transaction; there is no ordered "write A then write B" and thus no window between them. Pop is one `process_gen` holding `BStack`'s write lock across read-`head` → read-`next` → the batched write (closing the ABA window a `get`/`cas` pair would open); push is one `cross_exchange` (the specialized 2-write splice) or an `inplace_gen` batch when the overhead flip is bundled in. Tail grow/shrink use `try_extend_zeros` / `try_discard`, check-and-act under `BStack`'s own write lock. `recovery_needed` is a CAS on the flag word. The only retained `Mutex` is held by `recover()` alone, to keep recovery single-flight; the scan itself serializes against live ops through the `BStack` write lock it holds across one `process_gen` sequence, not through the `Mutex`.

- **Crash consistency.** Under `atomic`, a single-block alloc/dealloc commits its overhead word, `next_free`, and `head[class]` as one crash-atomic `set_batched`/`inplace_gen` transaction, so a crash leaves the block either fully in-use or fully free-and-linked — **no torn state, no leak window**, and a double-free cannot be observed mid-op. Without `atomic` the same mutation is separate `set` calls (write `next_free`, then `head`), so a crash between them leaks ≤ 1 block without corrupting a list. Oversized tail discard is a single crash-atomic `discard`. The leaks `recover()` must reclaim therefore come only from (a) the non-atomic build and (b) genuinely multi-transaction ops — `realloc` grow-by-copy and background coalescing — which bracket their work with the `recovery_needed` flag. No recovery scan is *required* to reopen a cleanly-closed stack.

- **Recovery.** `recover()` (auto-run by `open`, like `CheckedSlab`) linearly scans the arena, reading each overhead word: a live block (high bit set) strides by `classify_blocksize(len)`; a free block (high bit clear) strides by its stored `size` and is relinked onto `head[classify(size)]` from scratch (stored `next_free` pointers are rebuilt, not trusted). A partial tail block is truncated. Returns the count of blocks it could not classify with certainty (`0` = fully accounted for).

- **Thread safety.** Always `Send`. Without `atomic`, **not `Sync`**: `head[class]` is read then written as separate `BStack` calls (TOCTOU, two callers could pop the same block). With `atomic`, **`Send + Sync`** via the fused `process_gen`/`cross_exchange` sequences above — no allocator mutex except the recovery single-flight guard.

### Open questions

- **Coalescer.** Should the background coalescer be a separate thread, caller triggered (e.g. `alloc::coalesce()`), periodically run by `alloc`/`dealloc`, or only involved in heavy ops (e.g. oversized pop miss)?
- **Oversized mid-arena free.** Carve the span into class blocks (immediately reusable, a few extra IO on a rare path) vs mark-and-leave for the coalescer (cheaper free, slower reclaim). Leaning carve, but it depends on how oversized-heavy the real workload is.
- **Class scheme constants.** `linear_max`, `subclass_bits`, `max_class` are chosen at build time (the quantum is fixed at 16), trading fit-density against `NUM_CLASSES`/header size. Open: the specific values, and whether more than one scheme (i.e. more than one magic version) is worth shipping. If per-call cost turns out size-sensitive at the top end (the `slab_128` slowdown hints it might), a larger `subclass_bits` beats wider octaves.
- **Oversized reuse.** The shared oversized free list holds variable-size blocks, so reuse isn't a fixed-offset pop: pop-if-head-fits-else-extend is O(1) but wastes non-head fits; a bounded first-fit walk reclaims more at some cold-path cost. Which, given how rare oversized is in the real workload.
- **Header bound.** `NUM_CLASSES` is a compile-time constant but still sets header size (e.g. 64 classes → 512 B of heads); pick `linear_max`/`max_class` so the head array and the oversized cutover stay sane for the workload's large tail.
