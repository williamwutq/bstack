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

---

## Restoring `DebugCheckingAllocator` (post-0.4.0)

**Feature flag:** `alloc` (as before)
**Breaking change:** No — additive; the type was withdrawn in 0.4.0 and returns as new API surface.

### Motivation

`DebugCheckingAllocator<A>` was a wrapping allocator that validated an inner allocator's behaviour at runtime: it tracked the set of live and freed regions, detected overlapping allocations, caught double-frees and use-after-free, and flagged any invariant violation — a drop-in checker for both the built-in allocators and third-party implementations. It was temporarily removed in 0.4.0 because its implementation predated the three-type handle redesign and the `BStackAllocError` / `BStackBulkAllocError` return types, and porting it under the pressure of a large breaking release would have shipped a messy, under-reviewed version. Rather than block the release, it was withdrawn with the intent to reintroduce it cleanly.

The value it provides is hard to replicate ad hoc: allocator bugs — overlap, double-free, leaks, metadata corruption — are exactly the class that ordinary unit tests miss and that only surface under randomized or adversarial workloads. A maintained checking wrapper is the natural place to assert those invariants once and reuse them across every allocator and fuzz test.

### Design

Reintroduce `DebugCheckingAllocator<A: BStackAllocator>` as a transparent wrapper:

- Wraps an inner `A`, forwards `alloc` / `realloc` / `dealloc` / bulk to it, and records the resulting `(offset, len)` regions in an in-memory tracking structure.
- On each operation validates: no returned region overlaps a live region; no `dealloc` / `realloc` targets a region that is not currently live (double-free / use-after-free); bulk operations are all-or-nothing against the tracked set.
- Its handle type carries the inner handle plus enough identity to map back to the tracking entry, and converts `Into<BStackOwnedSlice>` like any other handle.
- Must thread through the new error types: on a failed `realloc` / `dealloc` the inner allocator now returns the surviving handle inside `BStackAllocError`, so the wrapper must **re-wrap that handle and update its tracking** rather than dropping it — otherwise the checker itself would introduce the very leak the 0.4.0 change set out to prevent. `dealloc_bulk` likewise re-wraps the un-freed handles from `BStackBulkAllocError`.

### Open questions

- **Panic vs. error on violation.** Panicking gives loud, immediate test failures but is unusable in `Result`-based property tests that want to *assert* an error was produced; a configurable mode (panic / return `Self::Error`) may be needed.
- **Tracking under `None`-handle errors.** When an inner operation fails and reports `handle: None` (allocation genuinely lost — e.g. `ghost_tree`'s torn AVL insert), the wrapper must decide whether to keep the region "live," mark it "lost," or drop it from tracking; each choice changes which subsequent operations the checker flags.
- **Overhead and gating.** Whether it lives behind a dedicated feature or is always available under `alloc`; the tracking set adds allocation and per-op cost that should not leak into production builds.

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

## C allocator API: signal whether the original survived a failed `realloc`/`dealloc`

**Feature flag:** `alloc` (C: `BSTACK_FEATURE_ALLOC`)
**Breaking change:** C only — an API break; the return-code variant below keeps it ABI-compatible.

### Motivation

Rust's `BStackAllocator::realloc`/`dealloc` return `BStackAllocError { handle: Option<A::Allocated> }`: on failure `Some` hands back a still-usable region (the untouched original, or a fully-committed new region) and `None` means the allocation was genuinely lost mid-operation, recoverable only through crash recovery. The C vtable equivalents (`gt_vt_realloc`, `..._dealloc`, …) return only `0`/`-1` with `errno` and write nothing meaningful to `*out` on `-1`, so a C caller cannot tell after a failed resize/free whether its original handle is still valid to reuse or must be dropped.

This is not hypothetical — the GhostTree tail-shrink fix showed several failure paths legitimately invalidate the original: a non-`atomic` shrink that has already committed part-way, a grow-that-moved-then-failed-to-free-the-old-block, or any path that returns the new region in place of the old. A C caller that keeps using its original handle after such a failure can double-free or read past a shrunk region. Today the only safe C policy is "on `-1`, treat the handle as lost and rely on crash recovery," which needlessly leaks the common case where the original is in fact intact.

### Design

Give the C `realloc`/`dealloc` a survivor signal mirroring Rust's `handle`. Two shapes are on the table; both let a failure-agnostic caller keep working unchanged.

- **Distinct return code (preferred — API-only break, ABI-preserving).** Keep the existing signatures and split the failure return: `-1` = failed, original **survived** (its handle is written to the existing `*out`); `-2` = failed, allocation **lost** (do not reuse the handle). Because every existing caller tests `ret < 0` (or `ret != 0`) for failure, both codes are still seen as errors and old code keeps working untouched — only callers that want the distinction check for `-2`. This refines the contract (what the negative codes and `*out`-on-failure mean) without touching any signature or the vtable layout, so it stays binary-compatible.
- **Out-parameter (API + ABI break).** `..._realloc(alloc, slice, new_len, bstack_slice_t *out, int *survived)` — on failure `*survived == 1` means `*out` is a valid handle the caller still owns and `*survived == 0` means the region was lost; `survived` may be `NULL` to opt into the current "treat as lost on any failure" policy. More explicit than a magic return code, but changes the function-pointer signatures, so it breaks the ABI.

### Open questions

- **Return code vs out-parameter vs result struct.** The `-2` return code is the least intrusive but relies on a magic value and on `*out` being read on failure; an explicit `int *survived` or a by-value `bstack_alloc_result_t { bstack_slice_t handle; int survived; }` (closer to Rust's `BStackAllocError`) trade ABI compatibility for clarity. Whether the extra explicitness is worth the break.
- **Vtable impact.** The out-parameter/result-struct variants change the vtable function-pointer signatures, so every built-in and third-party C allocator must update. The `-2` return-code variant leaves the signatures untouched — each allocator only refines its own failure-return logic — which is the main reason to prefer it.
- **Bulk `dealloc`.** The analogue of `BStackBulkAllocError.handles` — reporting *which* handles survived a partial bulk free — needs its own out-array shape regardless of which single-handle variant is chosen.
