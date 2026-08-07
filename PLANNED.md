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

## GhostTree allocator: multithreaded performance improvement

**Feature flag:** `alloc` (optionally `atomic` for the `Sync` path)
**Breaking change:** No — internal implementation only.

### Motivation

`benches/alloc.rs` result shows `GhostTreeBstackAllocator` is already the fastest general-purpose allocator in the suite, but its scaling under concurrency has received less attention than its single-threaded design: throughput rises from 1t to 4t and then flattens through 16t. This is consistent with `GhostTreeBstackAllocator::lock` — the single mutex serializing all non-tail `alloc`/`dealloc`/`realloc` — capping throughput once contention saturates it. As the crate's best-performing allocator, it is also the one most likely to be used under concurrent load, so its scaling behavior warrants continued performance work, independent of any specific defect.

The mutex's scope and implementation are not in question here (see the `NOT PLANNED` entry on `FirstFitBStackAllocator`'s mutex), and no tree-sharding or other data-structure redesign is intended — that would be a different allocator. The improvement surface is reducing the amount of work done per operation while the mutex is held. Two examples found by code inspection, illustrative rather than exhaustive:

- `avl_insert`, `avl_find_best_fit_and_remove`, and `avl_remove_min` each allocate a `Vec::with_capacity(MAX_AVL_DEPTH)` path buffer under the mutex on every call, despite `MAX_AVL_DEPTH` being a fixed compile-time bound that a stack array could cover instead.
- In the up-pass of `avl_insert` and `avl_find_best_fit_and_remove`, `avl_write_and_update` calls `avl_height` on a child subtree whose height was already computed and written in the previous loop iteration, issuing an avoidable `BStack::get_into` (lock + syscall) to re-fetch it.

### Open questions

- **Validation.** Whether `mixed/uniform` workload is sufficient to measure improvement, or whether a contention-specific microbenchmark (concurrent non-tail alloc/dealloc only) is needed to isolate the critical-section-size effect.

## Additional slice and vector APIs for ergonomics and compatibility

**Feature flag:** `set` (for write operations) or no flag (for read operations)  
**Breaking change:** No — purely additive.

### Motivation

`BStackSlice` and `BStackByteVec` currently provide fundamental I/O operations (`read`, `write`, `zero`, `subslice`) and basic vector operations (`push`, `pop`, `extend_from_slice`), but lack many ergonomic methods present in Rust's standard slice and vector types. While the existing API is sufficient for all operations, callers must manually compose primitives for common patterns, reducing productivity and readability. Adding methods that mirror `std` slice/vector APIs improves ergonomics, clarifies intent, and eases migration for code already familiar with Rust's conventions.

### Proposed additions

All methods follow crash-safety principles: read operations are side-effect-free, and write operations sync durably before returning. All write operations are **crash-atomic** — either fully persisted or not started, per bstack's core guarantee.

#### `BStackSlice` — read-only query methods (no feature flag)

- **`get(&self, index: u64) -> io::Result<Option<u8>>`** — Read the byte at `index`, or `None` if out of bounds. (Note: `BStackByteVec` already has this.)
- **`head(&self, n: u64) -> BStackSlice<'a>`** — Return a sub-slice of the first `n` bytes. Returns a slice of length `min(n, self.len())`. Convenient for `slice.head(100)` instead of `slice.subslice(0, n.min(slice.len()))`.
- **`tail(&self, n: u64) -> BStackSlice<'a>`** — Return a sub-slice of the last `n` bytes. Returns a slice of length `min(n, self.len())`, starting at `max(0, self.len() - n)`.
- **`contains(&self, needle: u8) -> io::Result<bool>`** — Returns `true` if the slice contains `needle`.
- **`starts_with(&self, prefix: &[u8]) -> io::Result<bool>`** — Returns `true` if the slice begins with `prefix`.
- **`ends_with(&self, suffix: &[u8]) -> io::Result<bool>`** — Returns `true` if the slice ends with `suffix`.
- **`find(&self, needle: u8) -> io::Result<Option<u64>>`** — Returns the index of the first occurrence of `needle`, or `None` if not found.
- **`rfind(&self, needle: u8) -> io::Result<Option<u64>>`** — Returns the index of the last occurrence of `needle`, or `None` if not found.
- **`position(&self, predicate: impl Fn(u8) -> bool) -> io::Result<Option<u64>>`** — Returns the index of the first byte satisfying `predicate`, or `None`.
- **`rposition(&self, predicate: impl Fn(u8) -> bool) -> io::Result<Option<u64>>`** — Reverse variant of `position`.

#### `BStackSlice` — write methods (requires `set` feature)

- **`fill(&mut self, value: u8) -> io::Result<()>`** — Fill the entire slice with `value`. (Note: `BStackByteVec` already has this.)
- **`fill_with(&mut self, f: impl FnMut() -> u8) -> io::Result<()>`** — Fill the slice by calling `f` repeatedly.
- **`copy_from_slice(&mut self, src: &[u8]) -> io::Result<()>`** — Copy `src` into this slice. Panics if `src.len() != self.len()`.
- **`copy_from_bstack_slice(&mut self, src: &BStackSlice<'_>) -> io::Result<()>`** — Copy from another `BStackSlice` into this slice. Panics if lengths differ. Requires `atomic` feature. Complements `BStackByteVec::copy_into_bstack_slice` and `extend_from_bstack_slice`.
- **`copy_within(&mut self, src_range: Range<u64>, dest: u64) -> io::Result<()>`** — Copy `src_range` within this slice to `dest`. Requires `atomic` feature.
- **`swap(&mut self, other: &mut BStackSlice<'_>) -> io::Result<()>`** — Swap the contents of this slice with `other`. Panics if lengths differ. Requires `atomic` feature.
- **`reverse(&mut self) -> io::Result<()>`** — Reverse the byte order in place. Requires `atomic` feature.
- **`rotate_left(&mut self, mid: u64) -> io::Result<()>`** — Rotate bytes left by `mid` positions. Requires `atomic` feature.
- **`rotate_right(&mut self, k: u64) -> io::Result<()>`** — Rotate bytes right by `k` positions. Requires `atomic` feature.
- **`split_at(&self, mid: u64) -> (BStackSlice<'a>, BStackSlice<'a>)`** — Split into two sub-slices at `mid`. Already achievable via `subslice`, but this follows `std` naming. Panics if `mid > len`.
- **`split_at_mut(&mut self, mid: u64) -> (BStackSlice<'a>, BStackSlice<'a>)`** — Mutable variant. Returns two independent sub-slices. Panics if `mid > len`.

### Implementation notes

- **Crash consistency:** All write operations are crash-atomic. Single-write operations (`fill`, `copy_from_slice`) use direct primitives. Multi-step operations (`swap`, `reverse`, `rotate`) require the `atomic` feature to ensure atomicity via `BStack::copy`, `inplace_gen`, or other crash-atomic primitives.
- **`BStackOwnedSlice`:** Methods for `BStackOwnedSlice` should be thin wrappers around the corresponding `BStackSlice` methods via `as_slice()` and `as_slice_mut()`, since owned slices should not perform I/O directly.
- **`BStackByteVec` unchanged:** `BStackByteVec` already has a comprehensive API. No changes proposed.

### Open questions

- **Iterator API.** Whether `BStackSlice::iter()` is worth adding, or whether `read()` followed by in-memory iteration is sufficient.
- **Windows.** Whether to add a `windows` iterator to enable sliding-window patterns.
- **Comparison operators.** Whether to implement `PartialEq<[u8]>` for `BStackSlice` and `BStackByteVec`.

Chunking and sorting are addressed separately below.

## Chunked slice view, sorting, search, and selection

**Feature flag:** `set` (mutating chunk operations: sort/select) or no flag (read-only chunking, search); chunk movement additionally requires `atomic`, per the `BStackSlice::swap`/`rotate_*`/`copy_within` primitives proposed above.
**Breaking change:** No — purely additive; introduces new types rather than modifying existing ones.

### Motivation

Data stored in a `BStackSlice`/`BStackByteVec` is rarely a flat stream meant to be sorted byte-by-byte — it's much more commonly fixed-width records (keys, struct-like entries) packed contiguously, where the caller wants to reorder whole records while leaving each record's internal bytes untouched. Byte-level `sort` is therefore not the useful primitive; the useful primitive is `std`'s `chunks_exact` pattern — dividing a slice into fixed-size units — with sorting, searching, and selection defined over those units. This was flagged as an open question in "Additional slice and vector APIs" and is developed here as its own feature, since it needs a new type rather than a method added to `BStackSlice` directly.

### Design

- **`BStackSlice::chunks_exact(&self, chunk_len: u64) -> BStackChunks<'a>`** — divides the slice into `chunk_len`-byte records, mirroring `[T]::chunks_exact`. Only the exact form is offered — a variable-length final chunk (`[T]::chunks`) has no natural place in a record-oriented view, since a short trailing chunk isn't a record of the type being sorted/searched/selected. A short remainder is exposed via `.remainder() -> BStackSlice<'a>`, matching `[T]::chunks_exact`; iterating `BStackChunks` yields `BStackSlice` sub-slices. Read-only construction and iteration need no feature flag.

- **Sorting**, gated on `set` (+ `atomic` for movement): `BStackChunks::sort_by(&mut self, cmp: impl FnMut(&[u8], &[u8]) -> Ordering)` and `sort_by_key(&mut self, key: impl FnMut(&[u8]) -> K) where K: Ord`. No comparator-free `sort()` on raw chunk bytes is proposed — sorting undifferentiated byte records by their own byte order is rarely the caller's intent. Movement is always whole-chunk (via `swap`/`copy_within`-style primitives), so a record's bytes are moved as a unit and never split.

  Internally, the implementation picks a strategy by data size — the caller sees only `sort_by`/`sort_by_key`:
  - **Small** (chunk data fits comfortably in memory): read every chunk into an in-memory buffer, sort there with ordinary in-memory sorting, then write the result back.
  - **Medium**: split into sections that individually fit in memory, sort each section in place, then merge adjacent sorted sections.
  - **Large**: classic external-merge-sort — same sort-section-then-merge idea as medium, generalized to multiple sections and multiple merge passes (k-way or repeated pairwise merge) so no single pass needs more than one section resident at once.

  The medium/large split is the same algorithm at different scale (external sort is section-sort-then-merge applied recursively); "small" is just the degenerate one-section case. Thresholds are an implementation detail, not part of the public contract.

  **Whole-sort atomicity.** `sort_by`/`sort_by_key` commit as a single crash-atomic transaction (journaled multi-write, extending the `inplace_gen` machinery to a batch computed ahead of time rather than issued live): a crash during sort leaves either the pre-sort order or the fully-sorted order, never an intermediate permutation. This is necessary because the small/medium/large strategies above write back in a different shape than a simple swap sequence (e.g. a full rewrite for "small"), so per-swap atomicity alone wouldn't give a caller a stable story across strategies.

  **`sort_partial_by`/`sort_partial_by_key`** — a variant that is explicitly allowed to stop early and leave the data only partially sorted (e.g. after some bounded number of merge passes or sections, for a large slice where a full sort is prohibitively expensive but an approximately-sorted result is acceptable). "Partial" only ever means *not fully ordered*, never corrupted or lost data: every completed step (section sort, merge pass) is itself committed atomically, so the visible state after a partial sort is always some valid permutation of the original records.

- **Binary search.** `BStackChunks::binary_search_by(&self, cmp: impl FnMut(&[u8]) -> Ordering) -> Result<u64, u64>` and `binary_search_by_key` — standard binary search over chunks, requiring the chunks already be ordered by the same key/comparator (caller's responsibility, as in `std`). Read-only, no feature flag.

- **Quickselect.** `BStackChunks::select_nth_by(&mut self, n: u64, cmp: impl FnMut(&[u8], &[u8]) -> Ordering)` / `select_nth_by_key`, mirroring `[T]::select_nth_unstable_by(_key)`: partitions chunks so the `n`th is in its sorted position, with unspecified order on either side. Gated on `set` (+ `atomic`), same whole-operation atomicity guarantee as `sort_by` — a crash leaves either the original order or a valid completed partition, never a half-applied one.

### Open questions

- **Unstable variants.** Whether to also provide `sort_unstable_by`/`sort_unstable_by_key`, mirroring `std`'s stable/unstable split, given unstable sort does fewer chunk moves at the cost of stability.
- **Section/threshold sizing.** How the small/medium/large boundary and section size are chosen — fixed constant, fraction of available memory, or caller-configurable — and how that interacts with `select_nth_by`'s partitioning for very large slices (quickselect also needs random access across the full range, so it may need its own out-of-core strategy rather than assuming in-memory partitioning).
- **`chunks_mut`.** Whether mutable, non-sort chunk iteration (for per-chunk in-place editing without a full sort) is worth exposing alongside the sort/select-specific API.
- **Naming.** `BStackChunks` is a working name.
