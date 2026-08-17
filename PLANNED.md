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

---

## Trimming redundant zero-fill writes on the initialised allocator paths

**Feature flag:** `alloc` + `set`
**Breaking change:** No — no observable behaviour change, no on-disk format change.

### Motivation

Implementing `BStackUninitAllocator` for the slab and ghost-tree allocators surfaced places
where the *initialised* path pays for zeroes it is already guaranteed. None is a
correctness problem, and none was changed as part of that work, since each
touches a path with its own documented crash-consistency reasoning.

### Candidate

1. **`LinearBStackAllocator::realloc` materialises a zero buffer to grow under `atomic`.**
   The grow branch allocates `vec![0u8; delta]` and calls `try_extend(end, zeros)`,
   writing `delta` bytes. `try_extend_zeros(end, delta)` has the identical guard
   and result but realises the growth with one `set_len`, so the zeroes cost no
   write I/O — the same primitive the other allocators' tail-grow paths already
   use. The non-`atomic` branch already takes the cheap route via `extend`. This is
   the only write standing between `LinearBStackAllocator` and "zero-fill is
   entirely free", which is the stated reason it does not implement
   `BStackUninitAllocator`.

More candidates of this kind exist in the allocators that have not adopted
`BStackUninitAllocator` yet, and belong with that work rather than here.

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

## Lifetime ergonomics for `process_gen` / `inplace_gen` (local-buffer ops)

**Feature flag:** none required for the macro path; the parallel API is available on the same feature surface as today's `process_gen` / `inplace_gen` (core + whatever gates the underlying journal / atomic primitives).
**Breaking change:** No for the additive paths (macro + parallel API). The existing `process_gen` / `inplace_gen` signatures remain; deprecation of the old form is opt-in and deferred until the parallel API is stablized and widely adopted.

### Motivation

`BStack::process_gen` and `BStack::inplace_gen` take a higher-order closure that returns `Option<BStackGenOp<'a>>`:

```rust
pub fn process_gen<'a, F>(&self, mut f: F) -> io::Result<()>
where
    F: FnMut() -> Option<BStackGenOp<'a>>;

fn inplace_gen<'a, F>(&self, mut f: F) -> io::Result<()>
where
    F: FnMut(io::Result<()>) -> Option<BStackGenOp<'a>>;
```

Call sites that need to hand the generated op a short-lived scratch buffer (e.g. an 8-byte head/next pointer) must today write:

```rust
buf: unsafe { core::mem::transmute::<&mut [u8], &mut [u8]>(&mut head_buf[..]) },
```

The transmute is sound — the buffers are stack-allocated outside the `process_gen` / `inplace_gen` call and therefore outlive every invocation of the closure and every use of the returned `BStackGenOp` — but the current borrow checker cannot see it. The same pattern is rejected under Polonius Alpha (`-Zpolonius` / `-Zpolonius=next` on current nightly) and is outside the stated scope of Full Polonius (which targets additional flow-sensitivity for linked-list / cursor patterns, not the `FnMut` + named-lifetime escape rule).

Relevant references:

- NLL / Polonius background and the limits of Alpha: https://blog.rust-lang.org/2026/08/04/enabling-polonius-alpha-on-nightly/
- Project goal “Stabilize and model Polonius Alpha”: https://rust-lang.github.io/goals/2026/polonius.html
- “The Borrow Checker Within” roadmap (Full Polonius = more flow-sensitivity for linked-list reborrowing; not this closure-escape case): https://rust-lang.github.io/rust-project-goals/2026/roadmap-borrow-checker-within.html
- Classic diagnostic: “captured variable cannot escape `FnMut` closure body”

The three options below standardise the sound workaround, offer a long-term safe API, and (optionally) provide a heavier macro that can prove the common “stack buffer outlives the call” pattern without an explicit transmute at the call site.

### Design

#### 1. Standardised unsafe macro (immediate, zero-cost)

Introduce a small declarative macro whose name contains `unsafe` so that every use is visibly unsafe, yet the caller does not need to write an `unsafe` block themselves:

```rust
// Suggested name (bikeshed-friendly)
bstack_unsafe_reborrow_mut!(head_buf[..] as &mut [u8])
bstack_unsafe_reborrow!(next_buf[..] as &[u8])
```

- Accepts an expression and a target type (or infers the target from context).
- Expands to the `transmute` (or an equivalent `ptr::from_ref` / `from_mut` + cast) that strips the short local lifetime and reattaches the longer `'a` demanded by `BStackGenOp<'a>`.
- Documented invariant: the referent must outlive the entire enclosing `process_gen` / `inplace_gen` call; the macro does not and cannot check this.
- Applies identically to both `process_gen` and `inplace_gen` call sites.

This is purely a readability / auditability win: the same sound transmute, written once, greppable, and clearly marked.

#### 2. Parallel “out-parameter” API (safe, preferred long-term)

Add a second pair of methods that invert the data flow:

```rust
pub fn process_gen_with<'a, F>(&self, mut f: F) -> io::Result<()>
where
    F: for<'b> FnMut(Option<&'b mut BStackGenOp<'a>>) -> ControlFlow<()>;
// or a simpler form that always passes a slot:
// F: FnMut(&mut Option<BStackGenOp<'a>>) -> bool  // true = continue
```

(and the analogous `inplace_gen_with`).

- The closure receives a mutable slot (`&mut Option<BStackGenOp<'a>>` or `Option<&mut BStackGenOp<'a>>`).
- It fills the slot with the next op (or leaves it `None` to terminate).
- Because the `BStackGenOp` value is constructed *inside* the callee’s stack frame for the duration of one iteration, the short-lived buffers can be borrowed with ordinary reborrows; no named lifetime needs to escape the closure.
- The old `process_gen` / `inplace_gen` remain supported. After at least one minor-version cycle of the new API being available, the old forms are soft-deprecated with a migration note pointing at `*_with` and at the unsafe macro for callers that cannot yet switch.

This solves the root cause for new code while keeping the existing surface stable.

#### 3. Analysing macro (`bstack_process_gen_expr!` / `bstack_inplace_gen_expr!`)

A heavier macro that accepts a block containing both the buffer allocations and the generator body:

```rust
bstack_process_gen_expr! {
    let mut head_buf = [0u8; 8];
    let mut next_buf = [0u8; 8];
    let mut step = 0usize;
    // process_gen call is implicit
    || {
        let op = match step { /* ... */ };
        step += 1;
        op   // may borrow head_buf / next_buf
    }
}
```

- The macro parses the leading `let` bindings that are simple array / slice allocations (or `Vec` with a known capacity that stays on the stack for the duration of the expansion).
- It rewrites the body so that the generated `BStackGenOp`s use the standardised unsafe reborrow (option 1) under the hood, or, when the pattern is simple enough, emits the out-parameter form (option 2) instead.
- Static assertions (or a small const-eval helper) document the intended invariant; the macro never claims to be a full borrow-checker replacement.
- The same shape is provided for `inplace_gen` (`bstack_inplace_gen_expr!`).

This is the highest-ergonomics option for the common “scratch buffers + step-machine generator” pattern, at the cost of macro complexity and the usual hygiene / diagnostics trade-offs.

### Open questions

- Exact spelling of the unsafe macro (`bstack_unsafe_reborrow_mut!` vs `bstack_reborrow_mut_unchecked!` etc.).
- Whether the out-parameter API uses `ControlFlow`, a `bool` continue flag, or an iterator-style `next`-like trait object for maximum flexibility.
- How aggressively the analysing macro should accept `Vec` / `SmallVec` scratch space versus restricting itself to fixed-size arrays.
