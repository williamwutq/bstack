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

### Replacing the locked-region cache `Mutex` with an `RwLock`

Reasons:

On a cached stack, reads whose range lies entirely within the locked region copy out of the in-memory cache (`self.cache`) under a `Mutex`, so concurrent cache reads serialise. An `RwLock` — or publishing the cache as an immutable `Box<[u8]>` behind the existing `locked` atomic — would let those reads run in parallel. But the cache is only ever *written* by `lock_up_to` (rare, monotonic growth), and the hot read is a single `copy_from_slice` out of it: a short critical section with no steady-state contention against writers. The lock-free, non-cached `pread` path already exists for callers who want maximum read parallelism, and the cache path is chosen precisely to trade a syscall for a memcpy; adding a second lock type for a memcpy-length critical section is unlikely to move a real workload. Not worth the extra complexity ahead of evidence.

### Backporting the 0.4.4 deferred-replay / `InterruptedWrite` machinery

Reasons:

The 0.4.x line guards against a follow-up write running against an inconsistent file — the window after a failed write whose best-effort rollback *also* failed — with an in-memory `replay_needed` flag, a new public `InterruptedWrite` error returned from *reads*, and a `recover()` entry point. On the 0.2 line the same hazard is addressed more cheaply by **Treat the committed length as the sole source of truth for logical size** (below): bounding every read and in-place write against `clen`, and appending at `HEADER_SIZE + clen` rather than the physical end, means a stale un-rolled-back tail can never be seen as valid payload — with no new error type and no on-disk change. The deferred-replay design also makes `len`/`is_empty` fallible (a `len().unwrap()` after a failed write would then panic), a borderline-breaking behavioural change not justified on a stable 0.2 line. Revisit only if the `clen`-authoritative change proves insufficient.

---

## A `ByteRange` type for `(offset, len)` parameters

**Feature flag:** None (additive API surface)
**Breaking change:** No via companion methods; yes if the existing `(offset, len)` signatures are changed in place.

### Motivation

The `(offset: u64, len: u64)` pair recurs across `get_range`, `zero_range`, and similar APIs. Both fields are `u64`, so there is no type-level distinction between "an offset into this stack" and "a length", so callers must remember the order at every call site.

A named `ByteRange` type would make these call sites self-documenting and let the compiler reject transposed arguments.

The other recurring I/O patterns already have named forms, so this is the only one left: the read/write regions `(offset, buf)` / `(offset, data)` are `BStackGenOp::Read` / `Write`, and the cross-region pair `(a, b, len)` is `BStackGenOp::Swap` (and `cross_exchange`).

### Design (sketch)

A lightweight `Copy` wrapper over a range:

```rust
pub struct ByteRange(Range<u64>);
```

Retrofitting is not free: Rust has no overloading, and the target methods take two positional `u64`s. Three routes, with different costs:

- **Companion methods** (e.g. a new `get_byte_range` beside `get_range`) taking `ByteRange` — non-breaking, but doubles the surface for every range method.
- **Generalise an already-single-argument method** (e.g. `subslice_range`) to `impl Into<ByteRange>`, with `ByteRange: From<Range<u64>>` so current callers still compile — non-breaking, but only where the argument is already a single value.
- **Change the `(offset, len)` methods in place** to one `ByteRange` argument — collapses two args to one, so every existing call site breaks.

The `impl Into<ByteRange>` "accept anything" trick only helps the single-argument case; it cannot fold two positional `u64`s into one without that breaking arity change.

### Open questions

- **Is the benefit real?** The pair already has named parameters in the Rust signatures (`offset`, `len`), so transposition is not silent. The main gain is readability at call sites, which is a matter of taste.
- **Proliferation cost.** A new public type adds documentation surface, appears in error messages, and must be maintained indefinitely.
- **Naming.** `ByteRange` is illustrative. Alternatives: `Span`, `Region`, `Segment` (avoid `Slice`, which collides with `BStackSlice`). The name should signal an I/O coordinate, not a data container.

---

## Treat the committed length as the sole source of truth for logical size

**Feature flag:** None (internal change; no API or on-disk change)
**Breaking change:** No

### Motivation

Two size sources coexist. `len`/`is_empty` and crash recovery use the cached committed length `clen` (the `.1` of `RwLock<(File, u64)>`), but the read paths (`peek`/`get`/`peek_into`/`get_into`/`get_batched*`) bound-check against `File::metadata().len()` (an `fstat`), and the in-place write paths (`set`/`zero`/`repeat`/`swap`/`cas`/`cross_exchange`/`copy`/`process`/`process_gen`, the `*_crds` family) derive the payload size from `lseek(SEEK_END)`. In the steady state these agree, so those syscalls are pure overhead on the hot path.

They diverge in exactly one window: after a write fails **and its best-effort rollback also fails** (e.g. the rollback `set_len` errors and that error is swallowed), the physical file is larger than `clen`. A subsequent `set`/`swap` then accepts a range in `(clen, file_size − 16]` and writes into a region recovery will discard on the next `open` (silent loss); a subsequent `push` — which appends at the physical end — folds the orphaned tail into the committed payload. This is the hazard the 0.4.4 line fixed with deferred replay (see the NOT PLANNED note on that machinery, which this supersedes on the 0.2 line).

### Design (sketch)

Make `clen` authoritative everywhere:

- Reads bound-check against `guard.1` under the read lock instead of `metadata()`. `clen` is stable under the read lock and exactly equals the payload size, so no `fstat` is needed — `len()` already reads it this way.
- In-place write ops bound-check against `guard.1` under the write lock instead of `lseek(SEEK_END)`.
- Appends (`push`, `extend`, `extend_sparse*`, the `Push` gen op) position at `HEADER_SIZE + clen` rather than the physical file end, overwriting any orphaned tail a failed rollback left behind.

This removes one syscall from every read and every in-place write **and** closes the failed-rollback hazard, with no new error type and no on-disk change. Master still uses `metadata()`/`seek(END)` in these paths, so this is a fresh optimization on both lines, not a backport.

### Open questions

- Is there any op that legitimately relies on the physical size differing from `clen`? (Reviewed: none — `clen` is committed in lockstep with every size change; the only divergence is the failure window this change neutralises.)
- The non-Unix/Windows fallback paths also use `seek(SEEK_END)`; fold them in too.

---

## Bounded-memory in-place fills and copies

**Feature flag:** None (internal change)
**Breaking change:** No

### Motivation

`repeat` materialises the whole `count * pattern.len()` region in a heap `Vec` before a single `write_all`; `zero` allocates `vec![0u8; n]`; `copy` reads all `n` bytes into a `Vec` before writing. Each is O(region) memory for what can be a large region (e.g. an allocator block move). None of this changes crash behaviour — these ops are not crash-atomic on the 0.2 line either way — so it is purely a memory-footprint concern.

### Design (sketch)

- **`zero` / `repeat`:** a shared streaming-fill helper stages one bounded chunk (a whole number of patterns, capped at ~64 KiB), fills it once with the repeated pattern, then writes it at successive offsets until the region is covered, ending in one `durable_sync`. Because the region length and every write length are multiples of `pattern.len()`, the tiling stays phase-aligned across chunks. `zero` becomes `repeat` of the single byte `0x00` (or shares the helper). Memory drops to O(chunk); regions below the chunk size keep today's single-write path. Master's `repeat`/`write_repeated` already streams this way.
- **`copy`:** for a disjoint source and destination, stream through a bounded buffer (read chunk → write chunk), O(chunk) memory. For overlapping regions, either keep the current full-buffer read (simplest — full buffering is what makes today's overlap handling correct) or add a direction-aware chunked move (copy backwards when `dest > src`). Master splits these into disjoint vs. overlapping paths.

### Open questions

- **Study item.** Chunk size (64 KiB? 1 MiB?) and the small-region threshold below which the single-allocation path is kept — needs a quick benchmark against realistic allocator block sizes.
- For `copy`, is the added overlap-detection branch worth it, or keep overlap on the full-buffer path? (`copy` currently handles overlap only by virtue of full buffering; it does not detect it.)
- `zero` on Linux could alternatively use `fallocate(FALLOC_FL_ZERO_RANGE)`, but that is platform-specific and out of step with the portable-core design; the bounded-buffer loop is the recommended form.

---

## Debug feature flag to skip durable sync for faster fault-injection testing

**Feature flag:** new debug-only flag (`debug-no-sync`), off by default and not for production use.
**Breaking change:** No — purely additive, gated behind an opt-in flag.

### Motivation

Downstream crash-safety and fault-injection tests spend most of their wall-clock time paying for a real durable sync on every write, even when the test only cares about post-crash state correctness, not actual durability. A feature-gated flag that skips the durable sync (writes still happen, just without the sync) lets those harnesses iterate much faster, at the cost of no durability guarantee — so it must be scoped to debug/testing use only.

On this line the gap is also a **C/Rust parity mismatch**: the C port already honours `BSTACK_TEST_NO_DURABLE_SYNC` at compile time (`c/bstack.c`), but the Rust `durable_sync` always issues the sync. The 0.4.x line shipped the Rust `debug-no-sync` feature; backporting it restores parity and speeds up this line's own fsync-heavy suites.

*(Adapted from the 0.4.x line's planned entry for this feature.)*

### Open questions

- Gate on `all(debug_assertions, feature = "debug-no-sync")` (release builds always sync), matching the 0.4.x implementation and the C `-D` flag's intent.

---

## Lifetime ergonomics for `process_gen` (local-buffer ops)

**Feature flag:** none for the macro path (same feature surface as `process_gen`).
**Breaking change:** No — additive.

### Motivation

`BStack::process_gen` takes a closure returning `Option<BStackGenOp<'a>>`, where `'a` is chosen by the caller and outlives the whole call. A call site that hands an op a short-lived scratch buffer (e.g. an 8-byte head/next pointer) captured by the closure must today write an open-coded `transmute` to reattach the longer `'a`:

```rust
buf: unsafe { core::mem::transmute::<&mut [u8], &mut [u8]>(&mut head_buf[..]) },
```

The transmute is sound — the buffer is a stack local outside the `process_gen` call and outlives every invocation — but the borrow checker cannot see it (`error[E0521]: borrowed data escapes outside of closure`). The same pattern is rejected under Polonius Alpha and is outside the stated scope of Full Polonius, so it will not fix itself.

### Design (sketch)

Introduce a small declarative macro whose name contains `unsafe`, so each use is visibly unsafe without the caller writing an `unsafe` block:

```rust
bstack_unsafe_reborrow_mut!(head_buf[..] as &mut [u8])
bstack_unsafe_reborrow!(next_buf[..] as &[u8])
```

It expands to the lifetime-only reborrow (via `transmute`, or `ptr::from_mut` + cast), preserving the referent type by construction. Documented invariant: the referent must outlive the whole `process_gen` call; the macro cannot check this and each call site still owes a `// SAFETY:` note. This is a readability/auditability win — the same sound reborrow, written once and greppable.

*(Adapted from the 0.4.x line's planned entry, which also covered `inplace_gen` and a safe out-parameter API; on the 0.2 line only `process_gen` exists. The 0.4.x line shipped the `bstack_unsafe_reborrow!` / `_mut!` macros in a `reborrow` module.)*

### Open questions

- Exact spelling (`bstack_unsafe_reborrow_mut!` vs `bstack_reborrow_mut_unchecked!`).
- Whether to also add a safe out-parameter variant (`process_gen_with` taking `&mut Option<BStackGenOp<'a>>`), as the 0.4.x plan proposed, or keep only the macro.

---

## `BStackGenOp::Abort` — end a `process_gen` sequence with an error

**Feature flag:** `set` + `atomic`.
**Breaking change:** No — `BStackGenOp` is `#[non_exhaustive]`.

### Motivation

`process_gen`'s only ways to end a sequence are a mutating op or `None`, and `None` returns `Ok(())`. A generator that decides, after seeing earlier reads, that it must not proceed can only end with `None` — it cannot signal *why* it stopped. An `Abort { source: io::Error }` op would end the sequence without writing (like `None`) but return that error, letting a generator propagate a decision-driven failure.

On this line `process_gen`'s per-op validation errors are **returned** directly (unlike the 0.4.x `inplace_gen`, whose rejected ops are *reported* to the next callback and silently swallowed on `None`), so `Abort`'s role here is narrower than in 0.4.x: it is an early-exit-with-error, not a batch-discard. It still closes the "end early and fail" gap and keeps parity with the 0.4.x `BStackGenOp::Abort`.

*(Adapted from the 0.4.x line's planned entry, whose motivation centred on `inplace_gen`'s in-memory overlay — absent on the 0.2 line.)*

### Open questions

- Whether the value over returning the error from the code surrounding the closure is worth a new variant on this line, given `process_gen` already returns op errors. It is cheap and forward-compatible, but the 0.4.x-specific motivation (silent overlay commit) does not apply here.

---

## `fault-injection` module for deterministic I/O-failure testing

**Feature flag:** dedicated (`fault-injection`), test/dev-oriented; active only with `debug_assertions`.
**Breaking change:** No — additive; release builds contain none of the machinery.

### Motivation

The failure branches of the API — which surviving handle a failed operation returns, whether it reads back valid bytes, whether best-effort rollback frees an orphaned region — are hard to reach from the happy path, so they are barely exercised. A `BStack` that fails I/O on demand closes that gap: a test arms a fault at a chosen operation (or a seeded, reproducible schedule), drives the operation, and asserts on the outcome, including crash-window behaviour (a fault injected after the write reaches the file but before `sync`).

### Design (sketch)

A feature-gated fault hook consulted inside `BStack`'s I/O, after argument validation, returning a policy-supplied `io::Error` in place of the I/O. Configurable to fail the Nth op, fail ops matching a predicate, or fail with a seeded probability; deterministic from the seed. Because the allocators hold a concrete `BStack` by value, an in-type hook (as the `guarded` feature already does for slice access) is cleaner than a wrapper type. The 0.4.x line shipped this as a `fault` module gated on `all(debug_assertions, feature = "fault-injection")`.

*(Lower priority — test infrastructure. Adapted and trimmed from the 0.4.x line's planned entry, which was framed around the allocator error contract.)*

### Open questions

- Whether faults are injected per public method or per underlying syscall (`read`/`write`/`sync`) — the latter is needed for partial-write and write-vs-sync crash windows.
- Public API (useful for downstream allocator authors) vs. `pub(crate)` for this crate's own suite.
