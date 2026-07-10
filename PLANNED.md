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

## `FaultInjectingBStack` for deterministic I/O-failure testing (post-0.4.0)

**Feature flag:** dedicated (working name `fault-injection`), test/dev-oriented
**Breaking change:** No — additive.

### Motivation

The 0.4.0 error-handling change makes `realloc` / `dealloc` return the surviving allocation in `BStackAllocError` (and `dealloc_bulk` the un-freed handles in `BStackBulkAllocError`), under a delicate contract: `Some` when the region survives, `None` only when it is genuinely lost, plus best-effort rollback of a freshly-allocated region when a mid-operation copy fails. Almost none of this is exercised by the current test suite — the fuzzers drive only *successful* `alloc` / `realloc` / `dealloc` sequences, so the failure paths (which handle is returned, whether it points at valid data, whether a rollback actually frees the orphaned region) are validated only by a couple of synchronous validation-error asserts. The behaviour that most needs testing is precisely the behaviour under I/O faults, which the happy path can never reach.

A `BStack` that fails I/O on demand closes that gap: a test allocates, arms a fault at a chosen operation (or a seeded random schedule), performs the `realloc` / `dealloc`, and asserts on the returned error — that `handle` is `Some` and reads back the correct bytes on the in-place and copy paths, that it is `None` exactly where a partial free makes retry unsafe (`ghost_tree`'s torn AVL insert, the slab free-list paths), and that best-effort rollback left no leaked region. The same tool exercises crash recovery (`open` / `recover`) and the write-in-progress journal by cutting I/O mid-commit.

### Design

A fault source that surfaces `io::Error`s at controlled points in `BStack`'s I/O, without changing allocator code:

- **Fault policy.** Configurable by: fail the Nth I/O op; fail every op matching a predicate (kind, offset range); or a seeded PRNG with a failure probability — deterministic and reproducible from the seed. Policies compose (arm once, arm repeatedly, disarm).
- **Injected error shape.** Returns representative `io::ErrorKind`s (`Other`, `StorageFull`, `Interrupted`), and optionally injects *after* the bytes reach the file but before `sync` (to simulate a crash between write and durability) versus *before* the write (a clean failure that changed nothing).
- **Integration.** Because the allocators hold a concrete `BStack` by value, the cleanest shape is an opt-in, feature-gated fault hook *inside* `BStack` — a policy object consulted before each syscall — rather than a separate wrapper type the allocators could not accept. `FaultInjectingBStack` would then be a thin constructor/newtype that builds a `BStack` with a policy installed, or the capability may be exposed directly on `BStack` under the feature. This mirrors how the `guarded` feature already hooks slice access.

### Open questions

- **Wrapper vs. built-in hook.** A standalone `FaultInjectingBStack` type would require the allocators to be generic over their backing store — a large, breaking change — so a feature-gated hook on `BStack` is likely the only drop-in option, at the cost of putting (flag-gated) test scaffolding in the core type.
- **Compile-time type substitution.** Alternatively, keep `FaultInjectingBStack` a fully separate type that mirrors `BStack`'s API, and under the feature (or `#[cfg(debug_assertions)]`) alias it into the allocators crate-wide — e.g. `#[cfg(feature = "fault-injection")] use fault::FaultInjectingBStack as BStack;` — so every allocator compiles against the faulting type with **no code change and no genericity**, and release builds contain none of the fault machinery. Costs: the alias must be an internal name the allocators reference (not the public `BStack`), the faulting type has to track `BStack`'s entire allocator-facing surface (including internal methods, keeping it in lockstep as `BStack` evolves), and a single build cannot mix real and faulting stacks — it is all-or-nothing per compilation. Weigh this against the runtime hook, which needs no API mirroring but does bake a (disabled) branch into the shipped type.
- **Granularity.** Whether faults are injected per public `BStack` method or per underlying syscall (`read_exact` / `write_all` / `sync`); the latter is needed to test partial-write and write-vs-sync crash windows, but couples the policy to internal I/O structure.
- **Determinism across threads.** Under `atomic`, concurrent operations share one fault schedule; a global counter/PRNG must be synchronized, and reproducibility across thread interleavings is limited.
- **Scope.** Whether this ships as public API (useful for downstream allocator authors testing their own implementations) or stays `pub(crate)` / `#[doc(hidden)]` for the crate's own test suite.
