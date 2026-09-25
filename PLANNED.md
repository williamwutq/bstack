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

### Adding a `ByteRange` type for `(offset, len)` parameters

Reasons:

The gain is small. The parameters already have names (`offset`, `len`), so the main benefit is call-site readability, which is a matter of taste. The cost is large. Rust has no overloading, so retrofitting means either adding a companion method beside every range method, which doubles the surface, or changing the `(offset, len)` signatures in place, which breaks every call site. Either way the crate takes on a new public type to document and maintain indefinitely, and a breaking change is not acceptable on the stable 0.2 line.

### Adding a `fault-injection` module for deterministic I/O-failure testing

Reasons:

On the 0.2 line, none of the `atrunc` or batched write operations are atomic, so a crash or I/O failure partway through can leave a partial state. 0.4.x closed this with journalling (along with deferred replay and `InterruptedWrite`), which is too invasive to backport. Fault-injection testing on this line would mostly confirm failure modes that are already known and left unfixed on purpose. Shipping it would also suggest that 0.2's failure handling is hardened when it is not, which gives users a false sense of safety. Users who depend on correct behavior under I/O failure should upgrade to the 0.4.x line, which already ships this module.

### Adding a `debug-no-sync` feature flag to skip durable sync in tests

Reasons:

The flag exists to speed up crash-safety and fault-injection harnesses. With fault injection not planned for this line (see above), there is no such harness to speed up. The C port's `BSTACK_TEST_NO_DURABLE_SYNC` stays as it is, and the parity gap is accepted. Users who need fast fault-injection testing should upgrade to the 0.4.x line, which ships both.

---

## Treat the committed length as the sole source of truth for logical size

**Feature flag:** None (internal change; no API or on-disk change)
**Breaking change:** No

### Motivation

Reads and in-place writes already bound-check against the cached committed length `clen` (a4b9b16, backport of #94). Appends still position at the physical file end via `seek(SeekFrom::End(0))` (`file_size()` in C).

The two diverge after a write fails **and its best-effort rollback also fails**, leaving the physical file larger than `clen`. A subsequent `push` then appends after the orphaned tail and commits it as payload. This is the hazard the 0.4.4 line fixed with deferred replay (see the NOT PLANNED note on that machinery, which this supersedes on the 0.2 line).

### Design (sketch)

Appends (`push`, `extend`, `extend_sparse`, `extend_sparse_batched`, the `Push` gen op, and their C counterparts) position at `HEADER_SIZE + clen` instead of the physical end, overwriting any orphaned tail. This drops one `lseek` per append and closes the hazard, with no new error type and no on-disk change.

### Open questions

- The size-changing ops (`pop`, `pop_into`, `discard`, `resize`, `ensure`, `ensure_with`, `atrunc`, `splice`, `splice_into`, `replace`, `try_extend*`, `try_discard`) also read the physical size. Should they switch to `clen` in the same change?

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
