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

## Region handle redesign: owned, borrowed, and raw region types (0.4.0)

**Breaking change:** Yes — touches every use of `BStackSlice` and the `BStackAllocator` associated handle type.

### Motivation

The default allocators use `BStackSlice` as both the allocation handle (passed to `realloc`/`dealloc`) and the I/O view (used for reads and writes). Conflating these two roles causes three problems, none of which documentation alone can fix:

1. **Sub-slices can be deallocated.** `BStackSlice` exposes safe `subslice`/`subslice_range`. A sub-range has an origin no allocator ever returned, yet it can be passed to `realloc`/`dealloc` with no `unsafe` at the call site, silently corrupting allocator metadata.
2. **Handles outlive the allocation they name.** `BStackSlice` is `Copy`. After `realloc` moves a region or `dealloc` frees it, the old value remains a usable handle pointing at a stale or freed offset — a use-after-free / use-after-realloc that the type system currently permits.
3. **Mutation is invisible.** Every write method takes `&self` (because `BStack` is interior-mutable via `RwLock<File>`), so a shared or copied slice can mutate the file from any call site with no `&mut` at the point of mutation.

Splitting the single type into three — an **owned** handle, a **borrowed** view, and a **raw** range — resolves all three at the type level. Each type carries exactly one role, and the capability that causes each problem is simply absent from the type that must not have it. This entry supersedes the two former 0.4.0 entries ("newtype handle for default allocators" and "requiring `&mut BStackSlice` for mutation"); those were two facets of this one redesign and are specified together here.

### Design

Three region types replace today's single `BStackSlice<'a, A>`.

#### `BStackOwnedSlice<'a, A>` — the owned allocation handle

Represents ownership of one allocation. Returned by `alloc`; consumed by `realloc` and `dealloc`.

- **Carries `&'a A`** (the allocator), an offset, and a length.
- **Not `Copy`, not `Clone`.** An allocation has exactly one owner. Consuming the handle in `realloc`/`dealloc` makes use-after-free and use-after-realloc *compile errors*: there is no surviving copy left to misuse. This — not the sub-slice gap — is the headline win of the redesign.
- **No I/O.** It cannot read or write. Allocation identity and region access are separate operations; to touch bytes you first convert to a `BStackSlice`.
- **No subslicing.** A sub-range is not an allocation, so the owned handle cannot produce one. This closes the sub-slice-dealloc gap structurally.
- **Convertible to a borrowed `BStackSlice` for I/O.** A by-value conversion (working name `as_slice<'s>(&'s self) -> BStackSlice<'s>`) derives `&'s BStack` from the stored `&'a A` via `BStackAllocator::stack()`. **The returned view's lifetime is tied to the `&self` borrow (`'s ≤ 'a`), not to `'a`.** This is load-bearing: it makes the view a genuine borrow *of the handle*, so a view cannot outlive a `dealloc`/`realloc` (both move the handle by value and are blocked while any view is live). Returning `BStackSlice<'a>` instead — decoupled from the handle — would let a view survive its own deallocation, and that hole exists whether or not the view is `Copy`, so tying the lifetime is the actual fix, not merely making the view non-`Copy`. A literal `Deref` impl is *not* possible: `Deref` must return a reference into stored data, but the handle stores `&A`, not a `BStackSlice` — so this is an explicit by-value method, not the `Deref` trait.
- **`Drop` is a no-op.** Allocations persist on disk beyond the process, so dropping the handle leaks (recoverably) rather than freeing. Freeing is always the explicit `dealloc`/`realloc` call. This is inherent to persistence, not a RAII feature.
- **Unsafe reconstruction.** An `unsafe` constructor takes `(&'a A, BStackRange)`, for rehydrating a serialized offset/length against a live allocator on reopen. The caller upholds the origin invariant.

#### `BStackSlice<'a>` — the borrowed region view

The read/write view of a region. A view, not a handle; carries no allocation identity — but, unlike `&[u8]`, it is a unique borrow rather than a freely copied one (see below).

- **Carries `&'a BStack`**, an offset, and a length. **The `A` type parameter is gone** — the slice reaches the file only through `BStack` (today's `BStackSlice` already routes all I/O through `self.allocator.stack()`), so it never needs the allocator type. When derived from an owned handle, `'a` is instantiated to the handle-borrow lifetime `'s`, so the view borrows the handle.
- **Not `Copy`.** A `Copy` view could be duplicated out of a `&mut` borrow, which would defeat the write-exclusivity below and let a mutable view be aliased. Dropping `Copy` makes `&mut self` writes *genuinely* single-writer within safe code, not merely advisory. (It does **not** need to drop `Copy` to be dealloc-safe — that is handled by the lifetime tie on the conversion above; non-`Copy` is about write-exclusivity.) It stays `Clone`-able if a caller genuinely needs a second independent view, which is an explicit act rather than an implicit copy.
- **Subsliceable.** `subslice`/`subslice_range` produce sub-*views*, never handles, so there is no origin hazard. Because the view is non-`Copy`, the *mutable* subslice path needs a `split_at_mut`-style API (consume/reborrow into disjoint ranges) rather than a plain `&self` split; immutable subslicing off `&self` is unrestricted.
- **Reads take `&self`, writes take `&mut self`.** `write`, `write_range`, `zero`, `zero_range`, `writer`, and `writer_at` change from `&self` to `&mut self`. With the view non-`Copy`, `&mut self` is a real exclusive-write borrow within safe code — no aliased mutable view can exist through safe code.
- **`Drop` is a no-op.** Same persistence reasoning as the owned handle.
- **Unsafe construction.** An `unsafe` constructor takes `(&'a BStack, BStackRange)` — the successor to today's `from_raw_parts`.

The same `&self` → `&mut self` write-receiver change applies to `BStackGuardedSlice` and any future slice type.

#### `BStackRange` — the raw region

An offset/length pair, analogous to `Range<u64>`. No pointer, no I/O, no allocation participation.

- **`Copy`, carries no reference.** This is the serialization / persistence representation: what you store on disk and reload.
- Cannot read, write, `dealloc`, or `realloc`. To do anything it must be `unsafe`-cast into a `BStackOwnedSlice` (supplying `&A`) or a `BStackSlice` (supplying `&BStack`). No validation is performed on the cast: serialized bytes are just data, per-handle validation is both infeasible and falsely reassuring, and the `unsafe` cast makes the caller's obligation explicit.

#### `BStackAllocator` trait changes

- The associated handle bound changes from `Copy + TryInto<BStackSlice<'a, Self>>` to `Into<BStackOwnedSlice<'a, Self>>`, and the handle is **no longer required to be `Copy`**. Moving from fallible `TryInto` to infallible `Into` follows from dropping per-handle validation. All library allocators set `type Allocated<'a> = BStackOwnedSlice<'a, Self>`, for which the `Into` is the identity.
- `Into<BStackOwnedSlice>` is an ownership-normalization / interop bound only. It is *consuming*, and therefore is **not** the I/O path — generic code reads and writes by converting the owned handle to a `BStackSlice`. It is also one-way: a custom `Allocated` normalized to `BStackOwnedSlice` cannot be fed back to that allocator's `dealloc` (which wants `Self::Allocated`), which is moot for the library's own allocators where the two coincide.
- `realloc` and `dealloc` continue to take `Self::Allocated` by value, but with a non-`Copy`/non-`Clone` handle this now has teeth: the caller cannot retain a usable handle across the call.

Within safe code the guarantees are real: the non-`Copy` owned handle makes use-after-free/realloc a compile error, the lifetime-tied conversion prevents a view outliving its allocation, and the non-`Copy` view makes `&mut self` writes genuinely single-writer. **At the process boundary, however, they remain advisory:** interior-mutable `RwLock<File>` plus the `unsafe` `BStackRange` casts can still mint two owned handles — or two views — for one region. The types prevent *accidental* aliasing through safe code, not aliasing outright, and the documentation must claim exactly that.

As a 0.4.0 breaking release, backward compatibility is not a constraint; every use of `BStackSlice<'a, A>` migrates to one of the three new types.

### Open questions

- **Names.** `BStackOwnedSlice` / `BStackSlice` / `BStackRange` are working names. `BStackOwnedSlice` still contains "Slice" despite doing no I/O; alternatives (`BStackOwned`, `BStackBlock`, `BStackAlloc`) drop that but lose the parallel with `BStackSlice`. `BStackRange` should not be confused with the `Range<u64>` used elsewhere in the API.
- **Conversion API shape.** Owned → borrowed is a by-value method (`as_slice`, `slice`, `to_slice`?). Now that `BStackSlice` is non-`Copy`, a single `as_slice(&self)` yields a shared (read-only in practice) view; a separate `as_slice_mut(&mut self)` is needed to obtain a view usable for writes, since a `&self`-derived view cannot be reborrowed mutably. Confirm the pair `as_slice`/`as_slice_mut` mirroring `&`/`&mut`.
- **`writer`/`writer_at` receiver.** These construct a `BStackSliceWriter` rather than mutating. Requiring `&mut self` is consistent but may feel strict; and `BStackSliceWriter` is currently `Copy` — should it stay `Copy`, or become non-`Copy` to reflect exclusive-write intent?

---

## `BStackUninitAllocator` extension trait for uninitialised allocation

**Feature flag:** None (additive API surface)
**Breaking change:** No

### Motivation

`BStackAllocator::alloc` guarantees that the returned region is zero-initialised, and `realloc` zero-fills any newly added bytes when growing. This guarantee costs a write: a fresh region pulled from the free list may hold leftover bytes from a previous allocation, and the allocator must zero it explicitly (e.g. `first_fit`'s `alloc` builds a zeroed buffer of the full block size before writing it).

Many callers immediately overwrite the entire region with `write` right after `alloc` — for example, a caller that allocates a block solely to `write` a serialized record into it has no use for the zero-fill, since every byte will be overwritten before being read. For these callers, the zero-fill is pure overhead: an extra full-region write (and, depending on the allocator, a larger I/O than the eventual payload write).

An opt-in `alloc_uninit`/`realloc_uninit` pair lets such callers skip the zero-fill. The returned region's bytes are **unspecified** — they may be zero, or may be leftover bytes from a previous allocation that occupied the same on-disk space — but they are always valid `u8` values, so reading them before writing is safe (no undefined behavior, unlike `MaybeUninit<u8>` in memory). This mirrors `Vec::with_capacity` followed by `set_len`, except no analog of `set_len` is needed since the bytes are always valid to read, just unspecified in value.

### Design

```rust
/// Extension trait for allocators that can skip zero-initialisation of newly
/// allocated or grown regions.
///
/// The bytes in a region returned by [`alloc_uninit`](Self::alloc_uninit), or
/// in the newly added portion of a region returned by
/// [`realloc_uninit`](Self::realloc_uninit), are **unspecified**: they may be
/// zero, or may be leftover bytes from a previous allocation that occupied the
/// same on-disk space. They are always valid to read — no undefined behavior
/// results — but callers must not rely on their value until they have written
/// to the region themselves.
pub trait BStackUninitAllocator: BStackAllocator {
    /// Allocate `len` bytes without zero-initialising them.
    ///
    /// Equivalent to [`alloc`](BStackAllocator::alloc) except that the
    /// returned region's contents are unspecified rather than guaranteed
    /// zero. `len = 0` is valid.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` on failure.
    fn alloc_uninit(&self, len: u64) -> Result<Self::Allocated<'_>, Self::Error>;

    /// Resize the region described by `handle` to `new_len` bytes without
    /// zero-initialising any newly added bytes.
    ///
    /// Equivalent to [`realloc`](BStackAllocator::realloc) except that, when
    /// `new_len` is larger than the current length, the contents of the added
    /// bytes are unspecified rather than guaranteed zero. Shrinking is
    /// unaffected, as no new bytes are introduced.
    ///
    /// # Slice origin requirement
    ///
    /// Same requirement as [`realloc`](BStackAllocator::realloc): `handle`
    /// must have been returned by [`alloc`](BStackAllocator::alloc),
    /// [`alloc_uninit`](Self::alloc_uninit), [`realloc`](BStackAllocator::realloc),
    /// or [`realloc_uninit`](Self::realloc_uninit) on this same allocator
    /// instance.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` on failure, including when the implementation
    /// does not support reallocation.
    fn realloc_uninit<'a>(
        &'a self,
        handle: Self::Allocated<'a>,
        new_len: u64,
    ) -> Result<Self::Allocated<'a>, Self::Error>;
}
```

Allocators implement this trait by reusing their existing free-list/extension logic for `alloc`/`realloc`, but skipping the zero-fill step before returning the handle. For an allocator whose growth always goes through `BStack::extend`, the newly extended tail is already zero (via `set_len` on a sparse file), so `alloc_uninit` may be no cheaper than `alloc` in that path — the savings are concentrated in the free-list-reuse path, where a previously-occupied block is handed back without being scrubbed first.

Implementing this trait is optional. Allocators for which zero-fill is already free (e.g. always-extend bump allocators) may implement it as a thin wrapper around `alloc`/`realloc` with no savings, or simply not implement it at all.

### Open questions

- **Default implementations.** Could `BStackAllocator` provide default `alloc_uninit`/`realloc_uninit` implementations that simply delegate to `alloc`/`realloc` (i.e., always zero), with `BStackUninitAllocator` only needed as a marker for allocators that actually skip the zero-fill? This would let generic code call `alloc_uninit` without a separate trait bound, at the cost of the trait no longer signalling "this allocator supports the fast path."
- **New Type** Should the unspecification contract be encoded in the type system, allowing the return to be `Self::UninitAllocated<'a>` instead of `Self::Allocated<'a>`? This would make it impossible to accidentally treat an uninitialised allocation as a normal one, but it would also require more boilerplate for callers who want to use the fast path, as they would need to convert from `UninitAllocated` to `Allocated` after writing to the region, or an allocator could simply set `Self::UninitAllocated<'a>` to `Self::Allocated<'a>` if no such distinction is needed.

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
