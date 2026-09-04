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

---

## Stabilising `SegregatedBStackAllocator` (0.4.5)

**Feature flag:** `alloc` + `set` (the allocator's existing gates; `atomic` for the lock-free/`Sync` build). No new flag.
**Breaking change:** No.

### Motivation

The allocator landed experimental in 0.4.2 and was hardened through 0.4.3 (physical-size overhead word, tail-shrink recovery-desync fix) and 0.4.4 (`alloc_bulk`/`dealloc_bulk`, `coalesce`, `alloc_uninit`/`realloc_uninit`). Its caveat named four items: format instability, API instability, the atomic/non-atomic resize divergence, and a missing coalescer and in-use-leak GC. The coalescer shipped in 0.4.4, the divergence is settled behaviour, and the leak GC needs no format change — so the label now overstates the risk and forces callers onto an experimental format for no gain.

### Format and API freeze

- **Format.** The current roadmap does not require adding an on-disk field. The header (magic + 33 free-list heads) and one 8-byte tagged overhead word per block (physical size `>> 4`, high bit = in-use) are the complete input to `recover`'s linear-scan rebuild. The visible length lives in the handle and the class scheme is a compile-time magic-encoded constant; neither is stored. The in-use-leak GC reads the same overhead words and adds nothing, like `coalesce`. No pending work grows the header or the block, so the format can freeze.
- **API.** The inherent surface (`new`/`open`, `unsafe recover`, `coalesce`), the trait impls (`BStackAllocator`, `BStackInPlaceResizeAllocator`, `BStackUninitAllocator`, `BStackBulkAllocator` under `atomic`), and the C `segregated_bstack_allocator_*` surface freeze. Additions stay allowed; removals and signature changes become breaking.

### Retained behaviour, not instability

- **Resize divergence.** A shrink reclaims freed excess (tail `Atrunc` or in-place carve) only under `atomic`; without it the excess is retained in place. Both satisfy `realloc`'s contract (a retain or a move is permitted, and the visible length is carried in the handle). Documented as stable behaviour.
- **In-use-leak GC.** `recover` reclaims free leaks and discards orphaned tails, but leaves in-use orphans (e.g. a crashed *move*'s old block) live — as every built-in allocator does. No data loss; deep reclamation stays future work.

### Performance tuning

Freezing the format leaves the heuristics free. A block self-describes its physical size in its overhead word, so any build reads any file regardless of the policy that carved it. The runtime dials are decoupled from the on-disk layout and stay tunable across patch releases without a magic bump. Stabilisation should benchmark and, where warranted, revise:

- **`SPLIT_MIN`** (currently `LINEAR_MAX`, 256 B), the excess above which a shrink or oversized-reuse carves rather than retains. A higher value retains more slack and writes less; a lower value reclaims more and fragments more. This is pure policy, deciding when to split; the split block records its own size regardless.
- **Oversized matching**: `alloc_bulk`'s largest-first chase and the greedy carve into up to 3 class pieces. The order and stopping rule stay open to change, since the resulting blocks are self-describing.
- **Tail grow/shrink thresholds**: when a back grow extends the tail versus moves, and when a shrink's excess reaches the tail-`Atrunc` path.

The class scheme is the exception. `QUANTUM`, `LINEAR_MAX`, `SUBCLASS_BITS`, `MAX_CLASS`, and the resulting 33 heads are magic-encoded, so retuning the class boundaries is a format change rather than a heuristic one. Freezing them costs little, because the current scheme already covers the size distribution well: the quantum-16 linear classes keep internal fragmentation under 16 B for every allocation up to 256 B, where small requests concentrate; the four-per-octave geometric classes bound waste to roughly a quarter of the request across `[256, 4096)`; and the shared oversized bucket absorbs the long tail, where relative waste is already small and a dedicated class would earn little. This matches the size-class shape proven in general-purpose allocators such as jemalloc and tcmalloc, so a compelling case to move a boundary is unlikely to arise. The freeze fixes the boundaries and leaves the fill policy within them free.

### Mechanics

- Drop the **Experimental** markers from the README allocator section, `algos/ALLOCATOR.md`, the [segregated.rs](src/alloc/segregated.rs) module doc, and the C header ([c/bstack_alloc.h](c/bstack_alloc.h) ~1355/1436). Recast the resize and leak-GC notes as documented behaviour.
- No format write: the magic is unchanged, so a 0.4.4 file opens unchanged under 0.4.5.
- CHANGELOG entry under *Changed*, no magic bump.

### Open questions

- **Patch-byte bump.** Whether to advance `\x01` → `\x02` for writer attribution. Cosmetic (no format change); `\x01` keeps 0.4.4/0.4.5 files byte-identical.
- **GC before freeze.** Ship a bounded in-use-leak reclaimer first, or freeze now and document the limitation.

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

---

## `CycleSwap` — a journal mode for cyclic multi-region swaps (0.5.0)

**Feature flag:** `set` + `atomic`.
**Breaking change:** Yes (0.5.0) — a new `wip_aux` mode, so the magic bumps.

### Motivation

`cross_exchange` swaps two regions atomically. A three-way rotation — `a → b → c → a`, as a sort or a free-list reorder produces — has no single-call form today, and running it as three `cross_exchange` calls exposes intermediate states to a crash. The generator cannot help: `process_gen` allows at most one mutating step.

Rotation is the right primitive because it is the only one needed. Any rearrangement of `k` regions is a map `s` on slot indices — the content in slot `i` ends up in slot `s(i)` — and it is a bijection, since every slot ends up holding exactly one region's content. Pick any slot `i` and follow it: `i`, `s(i)`, `s(s(i))`, … There are finitely many slots, so some value repeats, and the first repeat must be `i` itself — if `s^a(i) = s^b(i)` with `a < b`, injectivity cancels `s^a` and gives `s^(b-a)(i) = i`. That orbit is therefore a cycle, and `s` restricted to it is a rotation. Orbits partition the slots (following `s` forward or backward from a shared member reaches the same set), so `s` is a set of disjoint rotations, and disjoint slots mean disjoint bytes. A 2-cycle is a plain swap and a 1-cycle is a no-op, so `cross_exchange` and "leave it alone" are the two smallest cases of this mode.

The decomposition is per cycle, not per permutation: each rotation is atomic on its own, and a caller that needs a whole multi-cycle permutation to land as one unit needs `BStackTransaction`.

### Design

```rust
pub fn cross_rotate(&self, offsets: &[u64], n: u64) -> io::Result<()>;
```

Rotates `k` disjoint equal-length regions: the content of `offsets[i]` moves to `offsets[i + 1]`, and the last to `offsets[0]`. `k == 2` is a plain swap and delegates to the existing exchange journal; `k < 2` is a no-op.

- `wip_aux = u64::MAX - 7`, continuing the decrementing sequence (`u64::MAX - 6` if `MultiAtrunc` does not land first).
- Staging: `[n | k | offsets… | snapshot of offsets[k - 1]]` appended at `32 + clen` — one region of bytes, not `k`.
- Progress is a step counter `s`, carried in the header as `wip_ptr = 32 + offsets[s]`: the destination of the next unperformed copy, non-zero, and matching the convention of the existing modes. `cross_exchange` already advances `wip_ptr` mid-journal for the same reason. Keeping the counter in the header rather than in the staged tail is what makes it non-tearing (WIP.md's first-256-bytes argument) and costs one 8-byte write per step either way.
- Protocol: stage → sync → arm at `s = k - 1` → sync → then, for `s` from `k - 1` down to `1`: copy `offsets[s - 1]` → `offsets[s]` → sync → write `wip_ptr = 32 + offsets[s - 1]` → sync. The final step copies the staged snapshot to `offsets[0]`. Then disarm → sync → truncate to `32 + clen`.
- Each step is a **commit point**: the file is at a fully-defined intermediate state of the rotation, and the counter names which step is next. A crash replays exactly one step. Replaying a step is idempotent because its source, `offsets[s - 1]`, is not written until the following step — so the snapshot only ever has to cover `offsets[k - 1]`, the one region overwritten before it is read.
- Recovery validates `tail_len == 16 + 8 * k + n`, `n > 0`, `k >= 2`, `wip_ptr - 32` present in the staged offset list, every `offset + n <= clen`, and pairwise disjointness, then resumes at `s`. `clen` never changes, so the mode overwrites committed bytes before its commit point and WIP.md's Rule 2 applies — which is why the format changes.

Cost is `n` staged bytes and `2k` syncs. Staging is independent of `k`, so rotating a hundred megabyte-sized regions stages one megabyte rather than a hundred. The mode defines exactly one protocol and one recovery path; a caller that would rather stage every region and land them in one shot is describing a `MultiWrite`, which already exists, and that choice belongs in the `BStack` method, not in the journal.

### Open questions

- **A `BStackGenOp` variant.** `process_gen` already ends in at most one mutating step, and `Swap` there is the two-region case of this mode, so a `Rotate { offsets, n }` variant would fit the existing shape — a generator could read the regions it wants to reorder and end by rotating them under the same lock. Whether the borrow of an offset slice works inside `BStackGenOp`'s single caller-chosen `'a` is the open part.
- **Arbitrary permutations.** A general permutation decomposes into disjoint cycles, so the mode could take a full mapping and rotate each cycle. Whether that belongs here or in `BStackTransaction`, which can already express it, is undecided.

---

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

---

## `BStackGuardedUnit` and `BStackGuardedBuilder` — composable transform units for `guarded` (0.5.0)

**Feature flag:** `guarded`.
**Breaking change:** No

### Motivation

The whole-block codec model (`decode`/`encode`) already makes layering transforms — compress-then-encrypt, checksum-then-encode — a plain function composition: `decode = inner.decode ∘ outer.decode`, `encode = outer.encode ∘ inner.encode`. It works today with no new API: an implementor writes a guard whose `decode`/`encode` chain the layers by hand. That manual pattern is enough — this entry is convenience, not a missing capability.

What it removes is repetition. Every downstream that stacks transforms hand-rolls a bespoke guard, re-deriving the composition order each time and, in the naive form, allocating a fresh buffer at every layer.

A "middle" layer is not a guard: it binds no storage (`len`/`raw_block`) and has no side hooks — it is only a transform. Composition therefore operates on the transform half alone, over one shared storage binding. Making that first-class means naming the transform as its own unit and giving a builder that stacks units and attaches the result to a `BStackSlice`.

### Design

A **unit** is a single storage-agnostic transform — an `encode`/`decode` pair, with no `len`/`raw_block`/hooks:

```rust
pub trait BStackGuardedUnit {
    /// inner (toward storage) bytes -> outer (toward caller) bytes
    fn decode<'d>(&self, data: &'d [u8]) -> io::Result<Cow<'d, [u8]>>;
    /// outer bytes -> inner bytes
    fn encode<'d>(&self, data: &'d [u8]) -> io::Result<Cow<'d, [u8]>>;
}
```

A **builder** accumulates units from storage outward and binds them to a slice. It is monadic — each `then` returns a new builder carrying one more layer — so a pipeline reads in storage-to-caller order and the composition order stays implicit:

```rust
let guard = BStackGuardedBuilder::over(slice) // innermost = closest to storage
    .then(Encrypt::new(key))                  // raw <-> compressed
    .then(Compress::default())                // compressed <-> plaintext
    .build();                                 // : impl BStackGuardedSlice
// guard.decode = Compress.decode ∘ Encrypt.decode
// guard.encode = Encrypt.encode ∘ Compress.encode
```

`build()` yields a `BStackGuardedSlice` whose `len`/`raw_block` come from the bound slice, whose `decode`/`encode` fold the units in the two opposite orders, and whose `on_read`/`on_write` fire once for the whole stack.

### Open questions

- **Allocation between layers.** The naive fold allocates a `Vec` per unit (each `Cow::Owned`). Users that care run the layers through a reused scratch/ring buffer instead. The builder should offer a buffer-reusing fold — a scratch pair the fold ping-pongs between, or an in-place unit variant — without forcing every unit author to manage buffers. A length-changing unit (compression) complicates a fixed scratch.
- **Static vs dynamic stacks.** A tuple/HList builder monomorphizes and inlines the fold with zero per-layer dispatch but fixes the layer count at the type level; a `Vec<Box<dyn BStackGuardedUnit>>` allows runtime-assembled pipelines at the cost of a virtual call and a heap indirection per layer. Pick the static form as the default and let a `Box<dyn>` unit holding a `Vec` cover the dynamic case, or offer both.
- **Length bookkeeping.** For a length-changing stack `len()` (apparent) must be derived, and the atomic in-place methods (`write_range`/`process`/…) do not apply — they require `encode` to preserve the raw block length. The builder should surface whether the composed stack is length-preserving, so those methods are available exactly when every unit is.
- **Relationship to the deprecation question.** If `BStackTransaction` subsumes cross-boundary atomicity and `guarded` is reduced to byte transformation (see "`guarded` semantics under `BStackTransaction`"), the unit/builder *is* that reduced core — the transform surface without the storage/atomicity trait machinery. These entries should be resolved together.
