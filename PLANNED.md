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

## `BStackTransaction` — a buffered, crash-atomic transaction object (0.5.0)

**Feature flag:** `transaction` (implies `set` + `atomic`) for the public API. The recovery path in `io_core` is ungated, like all recovery.
**Breaking change:** Yes (0.5.0) — the on-disk magic bumps. The header layout is unchanged and there is no data migration; the only change is a new `wip_aux` mode. A 0.4 file and a 0.5 file differ in their first eight bytes and nothing else. See *Format and versioning*.

### Motivation

The only way to run several dependent operations under one held lock today is the generator protocol (`process_gen`, `inplace_gen`). It has three limits.

- **The caller must write a state machine.** `SegregatedBStackAllocator::carve_and_free` contains the same algorithm twice: a straight-line sequence of `set` calls for builds without `atomic`, and, for the atomic path, a dispatch over `step < k`, `step == k`, `step < k + 1 + 2 * k` where each arm recomputes which piece it is handling. The generator's shape requires this.
- **A sequence cannot span functions or crates.** Every op carries one caller-chosen `'a` that must outlive the whole call, so every buffer must be a local declared before the call and passed through `bstack_unsafe_reborrow!`. A callee cannot yield an op holding its own stack data, so a sequence cannot be built by recursion, by a helper, or by a downstream crate.
- **Size cannot change.** `inplace_gen` rejects all size-changing ops: the multi-write journal uses `clen` as the staging base and `file_size` as its end. Allocate-a-block-then-link-it — what `bllist` needs for crash-atomic `append`/`split_off` — cannot be expressed.

`algos/ATOMICLIST.md` requires a pointer-structure mutation to keep its reads and its dependent write in one critical section. The generator is currently the only construct that provides one.

The NOT PLANNED entry on `BStackInPlaceGuard` rejected ambient interception and named the alternative: an explicit transaction object with a mirrored API over a buffered image, an explicit `commit`, and drop-discards. This entry is that object, plus size changes.

### Design

#### Shape

A guard that holds the write lock for its lifetime and mirrors `BStack`'s surface:

```rust
impl BStack {
    pub fn transaction(&self) -> io::Result<BStackTransaction<'_>>;
    pub fn try_transaction(&self) -> io::Result<Option<BStackTransaction<'_>>>;
}

impl<'a> BStackTransaction<'a> {
    // Mirrors BStack, modulo Rust-level type differences: len, get, get_into,
    // peek, peek_into, set, zero, repeat, push, pop, pop_into, discard, extend,
    // extend_sparse, resize, ensure, atrunc, splice, splice_into, copy, swap,
    // swap_into, cas, cross_exchange, process, ...
    pub fn commit(self) -> io::Result<()>;
    pub fn abort(self);          // == drop; named for intent
}
```

Methods take data by short borrow and return owned results. Nothing escapes a call, so the transactional path needs no reborrow macros, and a `&mut BStackTransaction<'_>` can be passed through recursion and across crate boundaries.

`Drop` discards; `commit` is explicit and consuming. Same reasoning as the `BStackInPlaceGuard` entry: commit-on-drop would durably commit a partial transaction during panic unwind, and has no way to report an I/O error. Drop-discards also means `?` inside a transaction body rolls back.

`BStackTransaction` holds the `RwLockWriteGuard`, so it is `!Send`.

#### Ops are not journaled; the image is

The transaction keeps no op log. It maintains one flat logical image of the payload — content plus length — and every method mutates that image in memory. Commit journals the difference between the committed state and the final image, not the sequence that produced it.

Consequences:

- `push(A); pop(n); push(B)`, where the pop cuts past the original committed end, makes the second push overwrite originally-committed bytes rather than append. Whether a dirty range is an in-place edit or an append is decided at commit, by comparing it against `min(base_len, final_len)`. An op log cannot represent this; an image can.
- Bytes written and then popped, or written and then overwritten, cost no I/O.
- `copy(a, b)` then `copy(b, a)` has one answer: the second reads the image the first left, which is what the two statements mean in ordinary code.
- Reads see the buffered image, and a read fully covered by buffered content does no I/O. Later writes win on overlapping bytes. Both rules come from `inplace_gen`.

An op that fails validation is not recorded and does not poison the transaction; the caller gets the `io::Result` and decides. This matches `inplace_gen`, and differs from a database transaction, where a failed statement usually aborts.

This does not reverse the NOT PLANNED entry on `BStackGenOp::Copy`. That entry rejects a journal-level op whose source is read at replay time, after earlier copies have landed — chained copies then have no single answer, and individually non-overlapping copies can compose into an effective overlap. A transaction resolves `copy` at issue time against the image, and the journal only ever sees literal staged bytes.

#### The buffer

The image is a chunk list over `[0, cur_len)`; gaps are untouched committed bytes. Variants:

| Variant                     | Holds               | Purpose                                   |
|-----------------------------|---------------------|-------------------------------------------|
| `Arena(Range<usize>)`       | arena range         | literal bytes, copied in at issue time    |
| `Repeat { pattern, count }` | pattern, count      | pattern fill; memory is O(pattern)        |
| `Copy { src, len }`         | committed reference | in-file copy, no arena bytes              |
| `Move { src, len }`         | committed reference | in-file move; source vacated, may overlap |
| `Extend { len }`            | length              | sparse zero growth via one `set_len`      |
| `Discard { len }`           | length              | tail removal                              |

`zero` is `Repeat` of one zero byte, and a transaction whose only dirty range is a `Repeat` commits through the existing O(1)-staging `Repeat` mode. `Move` has `memmove` semantics without materialising the region. `Extend`'s zeros cost no write I/O, and `Discard` is recorded as a length change rather than a rewrite.

`Copy` and `Move` references need no dependency tracking. A later buffered write to the referenced source never reaches the disk, so the committed bytes still hold the issue-time content at commit; and the reference is materialised during staging, which runs before any destructive in-place write.

`Repeat`, `Extend`, `Copy`, and `Move` keep the common cases out of memory: growing by a gigabyte of zeros costs a few bytes, and moving a gigabyte within the file costs none. Literal pushed bytes are what remains buffered.

Arena garbage is the cost. Bytes superseded by a later write stay in the arena, so peak memory is the sum of all writes rather than the merged footprint, and a loop rewriting one offset grows without bound. Mitigations: reuse in place when the superseded chunk is the arena's tail, and compact above a live/total ratio.

#### Commit planner

Commit reduces the image to a set of dirty ranges plus a final length, then picks the cheapest journal mode that expresses it. Exactly one journal arm per transaction — the arm is the commit point in every existing mode.

| Final state                                         | Plan                                                    |
|-----------------------------------------------------|---------------------------------------------------------|
| no dirty ranges, length unchanged                   | no-op; no write, no sync                                |
| length unchanged, one dirty range                   | single-write path (derived atomicity, or `Set`)         |
| length unchanged, one pattern or disjoint reference | `Repeat` / `Copy` — O(1) staging                        |
| length unchanged, several dirty ranges              | `MultiWrite`                                            |
| length changed, suffix rewrite from `a` acceptable  | one `SpliceGrow` / `SpliceShrink` over `[a, final_len)` |
| length changed, otherwise                           | `MultiAtrunc` (new; see below)                          |

The planner can recognise `Repeat` and `Copy` from the chunk variants. Through the direct API those modes are reached only when the caller names the matching method.

The fifth row is the problem case. `a` is the lowest dirty offset, so a single splice rewrites everything from `a` to the end. In the motivating case — allocate a node at the tail, then update the previous tail's `next` pointer — `a` is wherever the previous node happens to sit, so a list whose last node is near offset zero of a large file would rewrite the file on every append. `MultiAtrunc` exists to remove that case.

#### New journal mode: `MultiAtrunc`

A general transaction is an `atrunc` fused with a multi-write, and there is no existing encoding for that. `MultiWrite` cannot be stretched to cover it: recovery pins block targets to `e <= committed_len` and always finalises with `clen` unchanged.

Sketch, following the conventions of the existing modes:

- `wip_aux = u64::MAX - 6`, continuing the decrementing sequence. `wip_ptr = 32 + clen'`, non-zero — unambiguous against `MultiWrite` (always armed with `wip_ptr == 0`) and against the single-region modes (each keyed by its own aux value).
- Staging base `S = max(clen, clen')`, as the splice modes already use, so staging is disjoint from both the old payload and the new one.
- The staged tail is the existing back-to-back `[s | e | data]` block sequence from `32 + S` to `file_size`, with validation relaxed from `e <= clen` to `e <= clen'`. On a grow, the new tail content is one more block, whose target sits above the old `clen`.
- Protocol: `set_len` to fit staging → stage → sync → arm → sync → replay in tail order → sync → `write_header_commit(clen', 0, Set)` → sync → truncate to `32 + clen'`.
- Recovery reads `clen'` from `wip_ptr` instead of deriving it from the file size. That is what allows an arbitrary block count; the splice modes can derive `clen'` only because they stage exactly one region.

The mode overwrites committed bytes before its commit point, so WIP.md's Rule 2 applies: a reader that does not recognise the aux value rolls it back and leaves a torn region. That is the reason for the format change.

#### Format and versioning

The magic bumps, `0.5.x` becomes the current line, and `0.4.x` is maintained with backports, as `0.2.x` already is for allocator fixes. There is no data migration: the header stays 32 bytes, no field moves, no existing mode changes, and only a new mode is added.

The upgrade mechanism is the one already in the crate. `FORMAT_MINOR` goes 4 → 5, so `MAGIC_PREFIX` changes and `open` rejects a 0.4 file loudly rather than silently upgrading it; `LEGACY_MAGIC_PREFIX` becomes the 0.4 prefix, and `BStack::migrate` converts a file explicitly, exactly as it does today for 0.1.x → 0.4. The conversion is smaller than the one already shipped: since the header layout does not change, `migrate` rewrites the eight magic bytes instead of rebuilding a 16-byte header into a 32-byte one.

The alternative — gate the new mode's recovery behind the feature flag and keep the magic — does not work. A file armed by a build with the feature must still recover when reopened by a build without it, which is why `recover_multi_write` is already ungated. Gating recovery would make the on-disk format depend on which features a consumer compiled.

#### Feature flag

`transaction`, implying `set` + `atomic`. The machinery is large enough that it should be possible not to compile it. `cas`, `process`, `set_batched`, `process_gen`, and `inplace_gen` already cover sequences that fit in one closure, which is most of them; the transaction is for sequences that cross a function or crate boundary.

Only the API is gated. `MultiAtrunc` recovery is ungated in `io_core`, and the magic bump is unconditional — an on-disk format that varies with compiled features would defeat the purpose.

#### Hazards to document

- **Re-entrancy deadlocks.** A callee holding `&BStack` that calls any method while a transaction is open blocks on the non-reentrant `RwLock`, and mirroring `BStack`'s full API makes such a callee more likely. A debug-only owning-`ThreadId` assertion at lock acquisition catches it in tests at no release cost. `lock_up_to` has the same problem, and would additionally move the immutable boundary over buffered content.
- **Lock hold time is caller-controlled.** A long transaction blocks all writers and all non-locked-region readers. `try_transaction` is for callers that must not block. Holding a transaction across an `await` or across user input is misuse.
- **Memory.** The chunk variants cover the common cases, but literal pushed bytes stay buffered until commit.

### Open questions

- **Consuming the `BStack` instead of borrowing it.** `into_transaction(self)`, with `commit` and `abort` handing the stack back, would make the re-entrancy deadlock a compile error rather than a debug-only assertion — no `&BStack` exists for a callee to call through. Exclusive ownership also removes the reason to hold the write lock for the transaction's lifetime, which would make the transaction `Send`. Both endings must return the stack unconditionally rather than an `io::Result<BStack>`: a failed commit hands back a recovered `BStack` (or the raw `File`) paired with the outcome, never nothing at all. It needs sole ownership, so an `Arc<BStack>` is out, and it does not compose with `BStackAllocator`, whose `&self` methods cannot move their owned `BStack` out — so probably a second constructor alongside the borrowing one.
- **Nesting and joining.** A callee handed a transaction that wants to begin one must join instead, and an inner commit must not commit. Savepoints with partial rollback need an undo log or a per-savepoint image clone, and are likely out of scope for a first version.
- **`guarded` interaction.** `BStackGuardedSlice` hooks fire on write, and a transactional write defers the write. Whether hooks fire at issue time, at commit time, or whether the transaction is simply not hook-integrated in the first version, has to be decided.
- **State after a failed commit.** A commit that fails mid-journal leaves the file for recovery on the next open, and the in-memory handle in an unclear state. `journaled_multi_set` has the same property today; a transaction makes it easier to hit.
- **Inspectable plans.** Exposing the chosen plan, publicly or in debug builds, would make the planner testable, and would let a caller see that a transaction is about to rewrite a suffix rather than a few blocks.
- **Spilling.** Staging into the file tail as writes arrive would make transaction size independent of memory. It is harder here than in an in-place-only design, because a later grow can invalidate a staging base chosen earlier.
- **C parity.** Required by the checklist. An opaque `bstack_txn_t` with `begin`/`read`/`write`/`commit`/`abort` fits C better than the generator callback.

---

## `SegregatedBStackAllocator` background coalescer

**Feature flag:** `alloc` + `set`; the merge splices require `atomic`.
**Breaking change:** No API break. Experimental, like the rest of this allocator.

### Motivation

The module doc lists the coalescer as the single pending item, and it is one of the two reasons the allocator is still marked experimental. Freed blocks are only ever returned to their own class list and adjacent free blocks are never merged, so a workload that frees many small blocks can hold a large contiguous free run that no oversized request can use.

### Design

The scan already exists: `recover()` walks every block by its recorded physical size, which is exactly the walk needed to spot adjacency. The coalescer is that walk plus a merge, not new machinery.

- An explicit `coalesce()` entry point rather than a thread — the crate takes no runtime dependencies and spawns no threads; "background" means "out of the allocation path", not "on its own thread".
- On a run of two or more adjacent free blocks: unlink each from its class list, write one merged free block, push it onto the class for the merged size. Each merge is a bounded set of free-list splices plus one overhead write.
- Crash safety: every intermediate state must parse as a valid arena for the existing `recover` scan. Leak-preferring ordering gives this — unlinking before the merged overhead word lands leaves reachable-but-unlinked blocks, which `recover` already reclaims — and makes the whole pass restartable.

### Open questions

- **Concurrency.** Under `atomic` the allocator is lock-free with no allocator-level mutex, so a coalescer racing an `alloc` that pops the very block being merged is the hard part. Options are a quiescence requirement (as `recover` already imposes) or a per-block claim protocol.
- **Bounded work.** Whether a call must be incremental — a cursor plus a work budget — rather than scanning the whole arena, so it can be driven from an idle hook.
- **Tail handling.** Whether a coalesced run that reaches the tail should be `try_discard`ed back to the file instead of merged into a free block.
- **Relationship to the deep in-use-leak GC.** The other unimplemented item shares the same linear scan; whether the two ship together or separately is undecided.

---

## Batch `FirstFitBStackAllocator`'s free-list writes

**Feature flag:** `alloc` + `set` + `atomic`.
**Breaking change:** No

### Motivation

`FirstFitBStackAllocator` uses no batching primitive at all: no `set_batched`, `process_gen`, `inplace_gen`, or `cross_exchange` across 49 write sites. Every free-list mutation is a sequence of individually committed `BStack::set` calls, each a separate `durable_sync`.

Small writes take `set_in_place`'s derived-atomicity fast path (one write, one sync) rather than the 4-sync journal, so the cost is one sync per `set`. `add_to_free_list` issues six — mark-free (4 B), header size (8 B), footer size (8 B), flags + reserved + next_free + prev_free (24 B), `free_head` (8 B), and the old head's back-link (8 B) — for **60 bytes of metadata**. Each coalesce adds two more through `unlink_from_free_list`, and the whole operation is bracketed by `set_recovery_needed` / `clear_recovery_needed`, one sync each.

| Path                                              | Syncs today |
|---------------------------------------------------|-------------|
| `dealloc`, no coalesce                            | 8           |
| `dealloc`, both neighbours coalesce               | 12          |
| `alloc` from the free list (`unlink_block` carve) | up to 9     |

`journaled_multi_set` — what `set_batched` commits through — costs exactly 4 syncs regardless of block count, so break-even is 4 writes and every path above clears it.

The throughput gain is real but secondary. The primary gain is that these writes become **one crash-atomic unit** instead of six to twelve independent commit points.

### Design

Only the commit grouping changes. The read-and-decide logic — the coalesce probes, the size cross-checks, the free-list walk — stays exactly as written.

**Phase 1: batch the writes, keep the recovery bracket.** Commit each operation's writes as one journal arm. Every write qualifies: all are in-place, none changes the file size (tail reclamation is a separate `cascade_discard_free_tail` call), and `dealloc` already holds the allocator mutex across the whole sequence, so the concurrency model is untouched. `set_recovery_needed` / `clear_recovery_needed` stay in place. Syncs go 8 → 6 and 12 → 6.

Use **both** primitives, chosen per call site — they commit through the same `journaled_multi_set`, so this is a call-shape choice, not a semantic one:

- **`inplace_gen`** where reads and writes interleave. `add_to_free_list` probes a neighbour, unlinks it, reads `free_head`, then writes; the generator protocol expresses that directly under one held lock, and is what `SegregatedBStackAllocator` uses for the same reason.
- **`set_batched`** where every write is known once the reads are done, so a one-shot list is simpler than a generator.

Two mechanical details:

- **One pair overlaps.** The mark-free write (4 B at `block_header_start + 8`) and the flags/links write (24 B at `result_start - BLOCK_HEADER_SIZE + 8`) target the same offset when no left-coalesce occurred, and both primitives reject overlapping ranges. In that case the mark-free write is subsumed and should be dropped from the batch; when a left-coalesce *did* occur the two offsets differ and both belong in the batch as disjoint blocks. Which case applies is known before the batch is built.
- **Two are adjacent.** The header-size write (8 B at `result_header_start`) and the flags/links write (24 B at `+8`) are contiguous and should be merged into one 32-byte block rather than passed as two.

**Phase 2 (separate change, contingent on Phase 1): drop the recovery bracket on the now-atomic paths.** Once an operation commits as a single journal arm there is no partial state for `recovery_needed` to announce, so the two bracketing writes become dead weight. Syncs go 6 → 4. This is deliberately *not* part of Phase 1: it changes the allocator's crash-recovery contract rather than just its write grouping.

**Non-`atomic` builds keep the current sequence unchanged.** Both primitives require `set` + `atomic` while this allocator requires only `set`, so the batched commit is `atomic`-gated — the same two-path shape `SegregatedBStackAllocator` already uses.

### Why this is safe to do to old code

`FirstFitBStackAllocator` is 2863 lines and among the oldest code in the crate, so the argument for each step has to be that it *narrows* behaviour rather than reshaping it.

- **Recovery does not have to change.** Recovery is a linear arena scan that rebuilds the free list from the `is_free` flags in block headers and trusts no stored pointer values. It does not depend on write ordering, so removing intermediate states cannot invalidate it.
- **Batching removes crash states, it never adds one.** Today recovery may observe the state after any prefix of the six writes. Batched, it observes either none of them or all of them — a strict subset. The one genuinely new state is "multi-write journal armed", which is a `BStack`-level condition recovered by `recover_multi_write`; that path is ungated, already shipped, and already exercised by `set_batched` and `inplace_gen`.
- **The `is_free` flag still lands.** It is carried in the 24-byte flags write, so a batch that drops the redundant early marker still leaves recovery a coherent flag.
- **Scope it to one call site first.** `add_to_free_list` alone, measured against `benches/alloc.rs` and `benches/alloc_non_atomic.rs`, before touching `unlink_block`. The carve path is the more intricate of the two and should follow only if Phase 1 measures well and fuzzes clean.
- **Fault coverage before merge.** `alloc_fuzz_tests.rs` and `alloc_fault_tests.rs` currently drive only `alloc`/`realloc`/`dealloc`; the new commit path needs torn-write injection at the batch boundary, since that is what surfaced the two recovery bugs fixed in 0.4.3.

### Open questions

- **Left-coalesce flag hygiene.** When a left-coalesce merges the block into its predecessor, the original block's header becomes interior to the merged block and its stale `is_free` flag is never read by the scan, which strides by recorded size. Worth confirming against the recovery walk rather than assuming, since dropping the early marker is the one semantic change in Phase 1.

---

## `guarded` semantics under `BStackTransaction` (0.5.0)

**Feature flag:** `guarded`, in combination with `transaction`.
**Breaking change:** Undetermined. Not in 0.5.0 itself; a deprecation would be, in a later release.

### Motivation

`guarded` exists to solve one specific problem: **communicating atomicity across a crate boundary**. Code that interposes on reads and writes — transforming bytes on the way through — does not own the I/O and cannot establish its own crash-safety guarantee, so the boundary itself has to carry the contract. That is what the hook traits are for, and what `BStackAtomicGuardedSlice` states as an `unsafe trait`: the implementor promises that each pre-hook, I/O, post-hook sequence is one uninterruptible, crash-safe unit.

`BStackTransaction` addresses exactly that problem, and addresses it more directly. A `&mut BStackTransaction<'_>` can be passed through recursion and across crate boundaries, and everything issued through it commits as a single atomic unit — so a caller on the far side of a boundary obtains atomicity by being *handed* a transaction, rather than by implementing a trait whose unsafe contract it must uphold by hand. The transaction entry lists "a sequence cannot span functions or crates" as one of the three limits it exists to remove; guarded's reason for existing is a special case of that same limit.

**There is therefore potential to deprecate `guarded` altogether.** If the transaction covers the cross-boundary atomicity case, what remains of guarded is byte transformation on read and write, which is a narrower job than the trait's current shape implies and may not warrant a feature of its own.

The interaction question is real regardless of which way that goes, because the hooks are written against an assumption the transaction breaks: that a write call *is* the I/O, so `pre_write` runs immediately before bytes reach the disk and `post_write` immediately after they are durable. A transactional write defers both — bytes land in the buffered image at issue time and reach the disk at commit, possibly coalesced with later writes to the same range, possibly never written at all if the transaction is dropped.

Deciding this early is cheap. The module is four traits and four derived methods, with no shipped implementation, no tests, and no examples, so it has more design freedom now than it will ever have again — and far more than it will once the transaction has shipped against it.

### Open questions

- **Deprecate, rework, or integrate.** Whether the transaction fully subsumes the cross-boundary atomicity case, and if it does, whether anything worth keeping remains. A deprecation would need a migration path for the transformation use, which the transaction does not currently offer.
- **When hooks fire,** if they survive. At issue time, at commit time, or not integrated with transactions at all in the first version. Issue time preserves `pre_write`'s transform semantics but lets a hook observe a write that never commits; commit time is honest about durability but loses the per-call pairing that `post_write`'s `(offset, len)` arguments assume.
- **Coalescing.** The image merges overlapping writes to the same range, so a hook firing per call at issue time may transform bytes that are later partly overwritten. A transforming hook may not survive that at all.
- **What the atomic markers mean inside a transaction.** Their contract is per write call; under a transaction the atomic unit is the whole commit. Either the marker means something different there, or it does not apply — and if the transaction is the answer to cross-boundary atomicity, the markers may have no remaining purpose.
- **Relationship to the transaction entry.** This supersedes that entry's `guarded` interaction open question; the two should not be resolved independently.

