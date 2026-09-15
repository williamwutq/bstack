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

### Adding `BStackInPlaceGuard`, an ambient atomic-block guard over in-place writes

The proposal: `BStack::inplace_guard(&self)` takes the write lock and, while held, transparently redirects ordinary `BStack` method calls on the same thread (via a per-instance `thread_local!` marker) into `inplace_gen`'s overlay/journal machinery, committing on explicit `commit(self)` or `Drop`.

Reasons:

The transparency that is the selling point is also the hazard. `BStack` keeps I/O explicit (see the trait rationale above); an ambient guard makes durability implicitly deferred, silently revoking documented contracts for exactly the code that cannot know about it — `Ok(())` from a nested `inplace_gen` no longer implies persisted, and code that catches a length-changing rejection (which poisons the transaction) and continues operates on undefined state. Documentation cannot close this gap.

The mechanism also has real holes. Routing adds a check ahead of every lock acquisition in all ~45 lock-taking public methods — a permanent tax on every future one. `lock_up_to` must be rejected too (unintercepted it same-thread-deadlocks; allowed, it advances the immutable boundary over pending overlay writes and lets commit mutate "immutable" bytes under lock-free readers). Commit-on-`Drop` durably commits a half-built transaction on panic unwind and aborts on I/O failure, which drop-equals-rollback conventionally avoids. The overlay must own its bytes, forking `inplace_overlay_insert`'s borrow-re-slicing core. Runtime overhead is notably the weakest objection — the cost is complexity and implicitness, not speed.

The underlying need — straight-line atomic blocks instead of the generator protocol — is real but modest: downstream callers already unroll recursion to drive the generators, as performance and atomicity demand that anyway. It is better served by an explicit transaction object (`stack.transaction()` exposing the mirrored in-place API over an overlay; explicit `commit`, drop = discard) reusing the same journal — worthwhile future work alongside the public journaling primitive planned for 0.5.0. Nor is ambient interception a stepping stone to cross-crate atomicity, which multiplies the same implicit-durability problems; the conventional answers remain caller-side buffering or per-method atomicity as `bstack` already provides.

### Adding `.dealloc() -> io::Result<()>` and `.realloc(new_len) -> io::Result<Self>` to `BStackOwnedSlice`, generic over any allocator

Reasons:

`BStackOwnedSlice` cannot generically call back into an arbitrary `BStackAllocator`. The conversion from an allocator's handle to `BStackOwnedSlice` is one-directional by trait design: `BStackAllocator::Allocated<'a>` need only satisfy `Into<BStackOwnedSlice<'a, Self>>`, and a custom allocator may embed extra metadata in a newtype handle that `BStackOwnedSlice` alone cannot reconstruct — so `dealloc`/`realloc`, which take `Self::Allocated<'a>`, cannot be driven from a bare `BStackOwnedSlice`. Separately, `BStackAllocator::Error` is an associated type, not necessarily `io::Error` — third-party allocators are free to use a richer error type. Fixing the return type to `io::Result<_>` would only be correct for allocators that happen to set `Error = io::Error`. Both conditions together require `BStackOwnedSliceAllocator` (the convenience supertrait that already fixes `Error = io::Error` and `Allocated<'a> = BStackOwnedSlice<'a, Self>`), so the method cannot apply to the general `BStackAllocator` case. Reaching for `unsafe` transmutes or a bespoke trait just to paper over this asymmetry is unnecessary — the caller already holds the allocator reference used to obtain the handle and can call `allocator.dealloc(handle)` / `allocator.realloc(handle, new_len)` directly.

### Implementing `RangeBounds<u64>` for `BStackRange`

Reasons:

`RangeBounds::end_bound` must return `Bound<&u64>` — a reference into `self`. `BStackRange` stores `offset` and `len`, so the end (`offset + len`) is a computed temporary with no field to borrow, and only `start_bound` could be satisfied. Storing `end` instead of `len` would make both bounds borrowable but is invasive: it touches every `len`-reading method and the 16-byte on-disk `(offset, len)` layout. The need is already met by the existing `range()` accessor, which returns `Range<u64>` (itself `RangeBounds<u64>`) at zero cost.

## Range access control on `BStack` and `BStackOwnedSlice`

**Feature flag:** `expensive-slice-access-control` (implies `alloc` + `set`). Off by default.
**Breaking change:** No. A build without the flag compiles to exactly today's code.

### Motivation

`bstack` has one enforcement mechanism for "these bytes must not change": `lock_up_to`. It is shaped for the stack case — a consumer whose bottom `n` bytes are settled and whose later pushes build on them — and is right there. For anything else it is a prefix, it is all-or-nothing, and it conflates writing with truncating.

What nothing encodes is *who is asking*. Ownership and borrowing settle aliasing, but aliasing is not authority: two callers holding the same range are indistinguishable, and nothing can say that one may write it and the other may not. It is the axis an OS gives every page, where `rwx` belongs to the mapping rather than to any pointer into it. Three things follow:

- **Allocator metadata is protected only as far as the slice API.** No handle spans a block header, but an allocator hands out its stack, and `allocator.stack().set(..)` reaches any byte in the arena. The rule being broken — *only the allocator may write here* — is about the caller, not about aliasing.
- **Truncating cannot be separated from writing.** A tail region may be freely writable yet must not be discarded.
- **Reads cannot be denied.** Atomicity guarantees a read is never torn, not that the bytes are still *yours*: a slice held across a `dealloc` reads whatever the block was reused for.

None of this is a correction — used as documented, the APIs keep a stack intact. Access control is an **additional layer** for callers who would rather have an invariant checked at runtime.

### Design

#### Modes

```rust
pub enum BStackAccess { All, Rw, RwStrict, Prot, RwProt, Alloc, ReadOnly, Locked }
```

| Mode       | Read      | Write     | Truncate           |
|------------|-----------|-----------|--------------------|
| `All`      | any       | any       | any                |
| `Rw`       | any       | any       | allocator or guard |
| `RwStrict` | any       | any       | none               |
| `Prot`     | guard     | guard     | guard              |
| `RwProt`   | guard     | guard     | none               |
| `Alloc`    | allocator | allocator | allocator          |
| `ReadOnly` | any       | none      | none               |
| `Locked`   | none      | none      | none               |

Each cell lists the authorities that satisfy it; `any` means no token is needed. The two tokens are **incomparable** — neither outranks the other — so `Prot` and `Alloc` are each private to their own holder on all three axes: a range marked `Alloc` cannot be read, written, or truncated by a guard holder, which is what makes metadata inviolable even to the policy owner. `All` is the default everywhere and is what an unprotected stack reports.

#### Authority

Two capability tokens, `BStackProtection<'a>` and `BStackAllocAuthority<'a>`, neither `Clone` nor `Copy`, each minted at most once per handle (`take_protection() -> Option<_>`, `None` thereafter). One-shot minting is what makes them mean anything. Allocator constructors claim the second naturally, since they already consume a `BStack` exclusively. Checked entry points gain a token-carrying sibling, `set_as(&self, auth, offset, data)`.

#### The point table

A sorted `Vec<(u64, BStackAccess)>` of change points: `(16, Alloc)` means `Alloc` from offset 16 until the next point, with an absent leading point implying `All` from 0. Adjacent equal modes coalesce, so the table is proportional to the number of distinct regions and one protected header is two entries. Deliberately **not** a `BTreeMap`: the table is read far more often than written, a read is a `partition_point` over a contiguous array.

- **Point lookup** — `partition_point(|p| p.0 <= off) - 1`.
- **Range check** for `[a, b)` — one `partition_point`, then a forward scan while `points[j].0 < b`, folding to the most restrictive mode. The common case spans one point and the scan does not run.
- **Setting** `[a, b)` to `M` — record the mode in effect at `b` as a point at `b`, insert `(a, M)`, drop points strictly inside, coalesce.

The table takes its own `RwLock`, separate from the stack lock, because the locked-region read fast path bypasses the stack lock and must still reject a `Locked` read. Mutation takes both, stack lock first, so in-flight writers drain before the new policy is published — the ordering and the reasoning of `lock_up_to`. An `AtomicBool` short-circuits every check on a stack that has never been protected.

#### Checks

Writes check their target range, truncations `[new_len, old_len)`, reads their read range. Batched paths check every block before the journal is armed. Points beyond `len` are retained rather than trimmed, so a range can be armed before its bytes arrive. Denials return `PermissionDenied`.

The locked prefix is checked first and stays out of the table, which can only further restrict it; folding the two would cost `lock_up_to` its lock-free read path. Protection is set through an owned handle — `BStackOwnedSlice::protect(mode)`, forwarded to the stack's table — never through `BStack` with an arbitrary range. A caller may set any range whose current mode already admits its token; one without a token may only tighten a range currently at `All`. Nothing is persisted, so reopening clears the table.

#### Cost

The flag is named for it. On a protected stack every checked call pays a relaxed load, an `RwLock` read acquisition, and a binary search before any I/O — a real fraction of a small `set`, whose fast path is one write and one sync. Batched ops pay per block, and every checked entry point grows a token-carrying sibling.

### Open questions

- **Named modes or an axis triple.** A `{ read, write, truncate }` triple of authorities is more expressive and no larger, at the cost of admitting nonsense (`read: none, write: any`). The enum is proposed because the curated eight are what callers want and a one-byte discriminant keeps the table compact.
- **What an allocator may do inside a `Prot` range.** Incomparability settles one direction — a guard holder cannot reach an `Alloc` range — but not the other. A caller may mark its own allocation `Prot` and then free it, leaving a mode over bytes the allocator is about to hand to someone else. Either `dealloc` resets the reclaimed range to `All`, which means an allocator overriding a mode it otherwise cannot touch, or the protection outlives the allocation and poisons the block for its next owner.
