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

---

## Enabling the `atomic` feature by default

**Feature flag:** N/A — this changes which features are enabled by default, not a new one.
**Breaking change:** Yes (0.5.0) — a plain `bstack = "0.5"` dependency (anything without `default-features = false`) now compiles in the `atomic`-gated API surface: `atrunc`, `splice`/`splice_into`, `try_extend`/`try_extend_zeros`/`try_extend_sparse`/`try_extend_sparse_batched`, `try_discard`, and `get_batched_gen`. The larger set gated on `set` *and* `atomic` together — `cross_exchange`, `copy`, `process_gen`, `set_batched`/`inplace_gen`, `swap`/`swap_into`/`cas` — only newly compiles in for consumers who *also* already enable `set`, since `set` itself stays opt-in (see Design below). Consumers who already pin an explicit `features = [...]` list without `atomic` are unaffected; `default-features = false` still builds the bare push/pop/get/peek stack with none of it.

### Motivation

- **No on-disk format change.** The write-in-progress journal that `atomic`'s compound ops use — `wip_ptr`/`wip_aux` in the 32-byte header, the splice/exchange/multi-write/copy journal modes documented in `lib.rs`'s format table and implemented by `io_core.rs`'s `journaled_*` helpers — is already unconditional: every `bstack` file carries it regardless of which features are compiled in. Enabling `atomic` by default changes no format byte and persists no new state; it only exposes machinery that is already there.
- **Does not compromise plain-stack use.** `atomic` only adds methods; it changes no existing `push`/`pop`/`get`/`peek` behavior. `alloc`, by contrast, adds real state — sub-allocator bookkeeping, handle types, `BStackSlice` provenance — which is why it stays opt-in even for a caller who only wants compound atomicity. `atomic` carries none of that cost.
- **Already required by most of what ships.** `alloc`, `set`, and `atomic` are gated together across `guarded`'s hook-based slices, the external-merge-sort strategy above, `BStackInPlaceResizeAllocator`, and the public journal primitive planned for 0.5.0 that `bllist` depends on. 7 of the 12 examples already require `atomic`. A caller reaching for any of these already hand-adds the flag; defaulting it removes that step.
- **Matches how the crate is already documented.** README's dependency snippets already list `atomic` in nearly every example (`features = ["atomic"]`, `features = ["set", "atomic"]`, …) — it's optional in name only.

### Design

- Add `default = ["atomic"]` to `[features]` in `Cargo.toml`.
- `set` and `alloc` stay opt-in — both add real new semantics (in-place byte-mutation history, sub-allocation) that a stack-only consumer may not want, unlike `atomic`.
- `docs.rs` already builds with `all-features = true`, so its output is unaffected.
- The 0.5.0 migration guide should note this briefly: it affects only consumers on default features who don't want `atomic` — an uncommon but real case (e.g. minimizing compiled surface, or auditing exactly which journal codepaths are reachable) — who now need `default-features = false` plus an explicit `features = [...]` list to keep it out.

### Open questions

- Whether to also drop the now-redundant `atomic` entry from example `required-features` lists that pair it with `set`/`alloc` — cosmetic only, since `required-features` checks additively against whatever is enabled regardless of default status, so leaving them as-is is also fine.

---

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

---

## `.dealloc()`/`.realloc(new_len)` on `BStackOwnedSlice<'a, A: BStackOwnedSliceAllocator>`

**Feature flag:** `alloc`.
**Breaking change:** No — new inherent methods, gated behind the `BStackOwnedSliceAllocator` bound.

### Motivation

Freeing or resizing an owned handle today requires the caller to separately hold the allocator and call `allocator.dealloc(handle)`/`allocator.realloc(handle, new_len)`. This is unavoidable in general (see the NOT PLANNED entry above), but under `BStackOwnedSliceAllocator` — the bound `try_clone`/`try_clone_uninit` already use — the handle carries `A::Error = io::Error` and its own `allocator()`, so `dealloc`/`realloc` can be forwarded as inherent methods on the handle itself.

### Design

```rust
impl<'a, A: BStackOwnedSliceAllocator> BStackOwnedSlice<'a, A> {
    pub fn dealloc(self) -> Result<(), BStackAllocError<'a, A>> {
        self.allocator().dealloc(self)
    }

    pub fn realloc(self, new_len: u64) -> Result<Self, BStackAllocError<'a, A>> {
        let allocator = self.allocator();
        allocator.realloc(self, new_len)
    }
}
```

Both are pure forwarding: `allocator()` returns `&'a A`, independent of `&self`, so it can be read before `self` is consumed by the call. No new behavior, error type, or crash-consistency class is introduced — each method has exactly the semantics of `BStackAllocator::dealloc`/`realloc` on the handle's own allocator.

### Open questions

- **Necessity.** The allocator is already at hand wherever a `BStackOwnedSlice` was obtained (it was needed to call `alloc`/`realloc` in the first place), so the caller can always reach `allocator.dealloc(handle)` directly. Whether the convenience of dropping that extra reference at call sites justifies the added inherent-method surface on `BStackOwnedSlice` is not yet decided.

---

## Segregated allocator: record the block's **physical size** in the in-use overhead word

**Feature flag:** `alloc` + `set`, same as `SegregatedBStackAllocator` itself.
**Breaking change:** On-disk only, and only for the **experimental** allocator — the `ALSG` magic bumps to `ALSG\x00\x02`, so existing `ALSG\x00\x01` files fail `new` with `InvalidData`. No crate API, trait, or type signature changes, and no other allocator's format is touched; the bump falls under the allocator's standing "format is not yet stable" caveat (`README.md` and `algos/ALLOCATOR.md`, §`SegregatedBStackAllocator`).

### Motivation

A live block's overhead word stores the **caller's visible length** (`IN_USE_BIT | len`); a free block stores its **physical size** (`size >> 4`, which doubles as the class tag) — `algos/ALLOCATOR.md` §"On-disk layout". `recover` therefore cannot read a live block's extent and must derive it as `class_blocksize(phys_need(len))`. That derivation is sound only under a strict invariant: **a live block's physical size equals the class size implied by its stored length**. Maintaining that invariant is what makes the expensive paths expensive:

- **Oversized non-exact reuse.** `pop_oversized` returns `actual ≥ need`, and the excess cannot be retained, so every inexact large `alloc` runs `commit_carve` — a greedy split into ≤ `MAX_CARVE_PIECES` (3) blocks, committed as one `inplace_gen` that reads and rewrites up to three free-list heads.
- **Non-tail shrink.** Must carve the freed tail (`atomic`) or, with no journal to fuse the length commit with the carve, take a **full move**: alloc, copy, free.
- **Tail shrink.** Must replace the block via `Len` + `Atrunc`, with no fault-safe non-`atomic` form (it moves instead) — the same coupling behind the recovery bug fixed under `[Unreleased]` in the CHANGELOG, where a committed length whose class stride disagreed with the block's physical extent let `recover` read live payload as an overhead word.
- **In-block resize.** A resize that changes nothing physical still costs one durable `set`, purely to keep the recorded length exact.

Recording the physical size severs the derivation. The extent stops being a function of the visible length, so a live block may be **larger than its request needs**. The visible length stays where it already is — in the caller's `BStackOwnedSlice` — and `realloc`/`dealloc` trust the length handed back on the same footing as the slice-origin obligation they already document.

### Design

#### Encoding

Both candidates are equally breaking on disk, so the choice is internal:

1. **`IN_USE_BIT | (size >> 4)`** — *recommended*: identical low-63 semantics to the free encoding.
2. **`IN_USE_BIT | size`** — raw size; live and free words then carry different units.

Prefer **1**. `recover` decodes one way for both tags (`size = (word & !IN_USE_BIT) << 4`) and consults the tag only to decide whether to relink; the claim and `push` paths share one word builder; and a misread tag still yields the correct stride, where **2** yields one wrong by 16×. The zeroed-tail sentinel (`word == 0` ⇒ crashed `extend`) holds under either: a live word has the high bit set, a free word has `size >> 4 ≥ 1`.

#### Changes to code paths

| Path                          | Today                                                                 | Under the new ABI                                                                                            |
|-------------------------------|-----------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------|
| `alloc`, oversized non-exact  | claim `block` + `commit_carve` the excess (≤ 3-piece `inplace_gen`)   | claim the **whole** popped block in one `set`; carve only above the split threshold                          |
| `realloc`, fits current block | rewrite the overhead word (one durable `set`)                         | **no metadata write** — only the `init` zero-fill of newly exposed bytes                                     |
| `realloc`, grow at tail       | `try_extend_zeros`, then rewrite the length                           | same shape, rewriting the size instead; still leak-preferring                                                |
| `realloc`, shrink at tail     | `Len` + `Atrunc` transaction (`atomic`); **move** without `atomic`    | `Len` + `Atrunc` as today, recording the new size (`atomic`); retained in place, not moved, without `atomic` |
| `realloc`, non-tail shrink    | carve transaction (`atomic`); **move** without `atomic`               | retained in place; carve only above the split threshold, in either build                                     |
| `dealloc`                     | recompute `size` from the stored length, then splice or `try_discard` | read `size` from the word; splice or `try_discard` unchanged                                                 |
| `recover`                     | stride live blocks by `class_blocksize(phys_need(len))`               | stride by the recorded size; the derivation and its overflow branch disappear                                |

The most significant improvements will be in-block resize dropping to **zero `BStack` calls**, oversized reuse losing its carve transaction, and the non-`atomic` build no longer moving on a shrink. Retention becomes a policy dial (`SPLIT_MIN` — retain excess below it, carve above) rather than a format constraint, since the block records its own extent either way.

#### Invariant that must survive

Classed free lists are **size-homogeneous**: `pop_class` claims the class size sight-unseen, and `classify` rounds *up* (`classify(336)` returns the 384-byte head), so a non-class-sized block linked into the classed range would hand out 384 bytes from a 336-byte block and overrun its neighbour. Retention must never create a classed-range block whose size is not an exact class size — and it does not: an uncarved oversized block stays oversized (that bucket is heterogeneous by design and `pop_oversized` checks `size ≥ need`), a retained-after-shrink block keeps the class size it already had, and carve pieces come from `largest_class_le`. Worth hardening regardless: have `recover` relink by `largest_class_le(size)` rather than `classify(size)`, so a malformed size degrades to a leak instead of an overlap.

#### Changes to slice validation

`dealloc`/`realloc` lose the `word & !IN_USE_BIT != len` comparison ("cannot free a partial or mismatched slice"): a retained block is deliberately larger than its request needs, so exact equality no longer holds. The handle's length is trusted from there on — the same trust `SlabBStackAllocator` and `FirstFitBStackAllocator` already place in it when they derive their zero and copy ranges from `slice.len()`. What remains is `ensure_own_handle`, `block_start_of` (pointer `≡ OVERHEAD (mod QUANTUM)` and `≥ ARENA_START + OVERHEAD`), exact double-free detection from the in-use bit, and `phys_need(len) ≤ size` in place of the equality — a bound that holds under retention and still rejects a length the block could never have held.

### Open questions

- **Split threshold.** Where `SPLIT_MIN` sits — retain always, carve above `LINEAR_MAX` (256 B), or carve once the excess exceeds a fraction of the block. Retention trades I/O against internal fragmentation, worst in the oversized bucket where the size spread is unbounded; `benches/alloc.rs` should grow a fragmentation-versus-calls measurement before the constant is fixed. The format constrains none of it, so a conservative initial value can be tuned later without another magic bump.
