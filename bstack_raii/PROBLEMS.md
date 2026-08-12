# `bstack_raii` audit — problem list

A breadth-first audit (2026-08-11), grouped by the requested categories. Items
are observations to triage, not verified defects unless marked; each is brief and
shallow by design (flagged on suspicion, not deeply investigated). Solutions are
omitted except where trivial.

## 1. Missing / incomplete features

- **Deep clone / teardown never use bulk alloc/free.** `alloc_many`/`free_many`
  are wired **only** into the 2-block `(rc, weak)` constructor. `ClonePlan`
  allocates each new block via sequential `alloc_raw`, and teardown frees
  sequentially — the "prefer `alloc_bulk`/`dealloc_bulk` when the allocator
  supports it" design is unrealized for exactly the N-alloc / N-free paths it was
  meant for.
- **`Foreign` ↔ `bstack_move` / `bstack_cast` semantics incomplete.** Moving a
  struct with a `Foreign` field, and casting for the wide-pointer relationship,
  were flagged as still-open in the project notes; needs confirmation that
  `bstack_move!` yields the right typed value at the foreign location.
- **Registry lazy-init not implemented** — only explicit `registry::init(path)`;
  the intended "init on first live attach" path was left unresolved (no registry
  path source).
- **`ForeignHost` lacks batched/generator ops**, so a cross-file clone's home
  commit and foreign side cannot be one atomic unit (best-effort only — see §3).

## 2. Code quality

- [FIXED] **Crate-wide `#![allow(dead_code, unused_imports, unused_variables)]`**
  ([lib.rs:45](src/lib.rs#L45)) suppresses three whole warning classes across the
  entire crate — it hides real dead code (below) and would mask unused-variable
  bugs. Should be removed and warnings addressed per-item.
- [FIXED] **Dead / superseded public helpers.** `init_rc` and `alloc_control`
  ([construct.rs](src/construct.rs)) are unused by codegen (the batched
  constructor path replaced them) yet still `pub use`d. `alloc_control` is also
  **non-atomic** (allocates + writes payload, then a separate `set` of the data
  block's back-pointer) — a public primitive whose behavior contradicts the atomic
  path that superseded it. `init_rc` removed (truly dead); `alloc_control` removed
  from `construct.rs` and replaced in `tests.rs` with a test-local equivalent that
  commits the control payload and the data block's back-pointer in one
  `set_batched`, matching the atomic path.
- **Pervasive `.to_le_bytes().to_vec()`** — every counter/pointer write allocates
  a fresh 8-byte heap `Vec<u8>` to feed the `set_batched` / `ClonePlan::write`
  batch APIs (dozens per operation in `list`/`deque`/`map`). A small-buffer /
  inline representation for batch entries would remove most of these allocations.
- [FIXED] **Stale crate-level docs** ([lib.rs](src/lib.rs) module comment): "Status:
  method bodies marked `todo!()` are the work ahead" and "procedural macros come
  after the runtime is filled in" describe a half-built crate; it is now
  feature-complete. The module-map table also omits `construct`, `vec`, `wal`,
  `registry`, `foreign`, `stdlib`, `bulk`, `cast`, `replace`. Status rewritten to
  reflect feature-completeness (naming the one real open gap, `Foreign<T>`
  cross-file teardown/deep-clone); module-map table now lists all 18 modules.

## 3. Atomicity / crash safety

Core paths are sound (constructors commit via one `write_range` / `set_batched`;
deep clone is two-phase allocate-then-atomic-commit; owned teardown is WAL-backed).
Residual points, all *leak-only* (permitted) but worth recording:

- **`BStackWeak::upgrade`** ([shared.rs:238](src/shared.rs#L238)) increments the
  strong count, then reads the data forward-pointer; if that read fails the strong
  increment is orphaned (over-count → the block can never reach zero). Same class
  as the weak-setter leak just fixed; could reuse the release-on-failure idea.
- **`BStackRc::try_move`** ([shared.rs:162](src/shared.rs#L162)): after the CAS
  `strong 1→0`, a failure inside `T::bstack_move` leaves the block unwrapped with
  the shell possibly unfreed — an error-path leak.
- **Cross-file teardown frees are not WAL-protected on the *target* file.** The
  home WAL logs `(foreign_id, range)` and `free_recorded` replays them via the
  registry — but only if the foreign file is *attached at recovery time*. A crash
  where the foreign file isn't re-attached on the next open loses those frees
  (leak). Worth documenting as a recovery precondition.
- **`alloc_control`** (public, non-codegen): transient half-wired window where the
  data block's `ctrl == 0` between its two writes (see §2).

## 4. General bugs

- **Fragile lifetime `transmute`** ([teardown.rs:111](src/teardown.rs#L111)):
  `transmute::<&[u8], _>(&flip[..])` launders the lifetime of a 1-byte stack local
  so the `inplace_gen` closure can capture it; sound only because `flip` outlives
  the call. A refactor that moves/reorders `flip` would silently make it UB — it
  relies on an invariant the compiler no longer checks.
- No concrete logic defect surfaced in the sampled runtime paths; the atomic
  counter ops (`refcount.rs`) and the map/list/deque algorithms look correct.

## 5. Semantics violations (safe code → UB)

- No path found where a *safe* public API leads to UB (the risky constructors are
  all `unsafe fn from_raw` / `from_range`, correctly marked).
- **21 lifetime-laundering `transmute::<&[u8], _>` / `<&mut [u8], _>`** calls
  across `teardown`, `clone`, and the stdlib collections (the `inplace_gen`
  buffers-outlive-the-call pattern) are the crate's main UB exposure: each is sound
  only while its buffer provably outlives the generator call. They should be
  funneled through a single audited helper rather than open-coded 21 times (see §9).

## 6. Missing documentation

- **The entire `stdlib` collection suite is absent from the README** (0 mentions):
  `BStackHashMap`, `BStackBTreeMap`, `BStackHashSet`, `BStackBTreeSet`,
  `BStackDeque`, `BStackLinkedList`, `BStackBinaryHeap`, `BStackBox`, `BStackCow`,
  `BStackString`, `BStackCountingBloomFilter` and their iterators — a large,
  user-facing feature with no README presence.
- **Large WAL surface exported with unclear audience**: `AllocReq`, `Reduced`,
  `WalEntry`, `WalHeader`, `WalLog`, `WalOp`, `WalStatus`, `finish`, `persist_at`,
  `reduce`, `STD_WAL_ANCHOR` are all `pub use`d at the crate root. If they are
  internal machinery they should be `pub(crate)`; if public, they need docs on how
  a user is meant to use them.
- README does not mention `alloc_many` / `free_many` or the `foreign_*` runtime
  helpers (acceptable if intentionally internal, but they are publicly exported).

## 7. Bad use experience

- **Field reads always require an explicit `stack` / allocator argument**
  (`h.get_field(alloc.stack())`). Callers almost always hold the allocator, so
  `.stack()` is constant boilerplate; accessor forms taking `&A` directly would cut
  it.
- **`BStackRc` / `BStackWeak` have no `Deref`** (only `BStackOwned` does), so a
  shared handle needs `rc.handle().get_field(...)` while an owned one allows
  `owned.get_field(...)` — inconsistent ergonomics for the same operation.
- **`Foreign::with` returns `Option`** (None conflates "null pointer" and "target
  file not attached"); a `Result` (or distinct sentinel) would let callers tell a
  missing file from a genuinely null `Foreign`.

## 8. Performance potentials

- Thousands of tiny `Vec<u8>` allocations for 8-byte writes (§2) — the single most
  pervasive avoidable allocation.
- No bulk alloc/free in clone/teardown (§1) even when the concrete allocator
  implements `BStackBulkAllocator`.
- **Double read per strong child in clone**: `ClonePlan::bump_strong` calls
  `strong_parts` (a read to find the control offset) during planning, then the
  commit's `inplace_gen` reads the same counter again.
- Reads are buffer-copy based (no mmap zero-copy) — documented inherent limitation.

## 9. Duplicated code

- **The `inplace_gen` commit pattern is open-coded repeatedly** — buffers hoisted
  to outlive the call, a phased read→compute→write generator, and the lifetime
  `transmute`s — in `teardown::wal_free_all`, `clone::commit_inner`, and each
  stdlib collection's commit path. A single `batched_commit` helper would remove
  the duplication *and* shrink the unsafe surface in §5.
- **`(offset, value.to_le_bytes().to_vec())` write-tuple construction** is repeated
  hundreds of times across `stdlib/*` and the codegen; a tiny constructor helper
  (`w8(off, val)`) would compress it.
- Every stdlib collection repeats a "read `OnDisk` header → mutate counters →
  `set_batched`" shape; some of it could share a helper (this is runtime code, not
  the struct-vs-enum codegen that was explicitly excluded).

## 10. Feature interactions

Cross-feature combinations, both the undesirable and the confirmed-sound (recorded
so they are not re-flagged).

### Undesirable / risky

- **Raw `ForeignPtr` bypasses cross-file ownership.** `Foreign<T>` is deliberately
  **not `Pod`** (only `Copy`), so it is correctly rejected from every `T: Pod`
  container (`BStackBox<Foreign>`, `BStackVec<Foreign>`, a `Foreign` map/set/heap
  key all fail to compile — good). But `ForeignPtr` **is** `Pod` and is publicly
  re-exported, so `BStackBox<ForeignPtr>` / `BStackVec<ForeignPtr>` / a `ForeignPtr`
  Pod-key compile and store a cross-file pointer as opaque bytes with **no** owning
  dispatch → the target is leaked on teardown and aliased on clone. The macro's
  "a `Foreign` field must carry an annotation" guard has no analogue here. Low
  severity (raw form), but it is a hole in the "a foreign pointer always carries
  ownership dispatch" invariant.
- **Stdlib collections composed into blocks are entirely untested.** No test puts a
  collection in a `#[bstack_block]` field (`#[bstack_owned] d: BStackDeque<Leaf>`,
  `Option<BStackHashMap<..>>`), an enum variant, a `Vec`/array element, or a
  `Foreign<Collection>` target. The bounds line up (collections are
  `BStackBlock + TryCloneIn` and override the nested `__bstack_*` hooks, so it
  *should* work), but deep-clone/teardown of these compositions is unverified — a
  large, plausible-but-unexercised surface.
- **Generic struct owning a generic collection may not infer bounds.** For
  `struct S<T: BStackBlock> { #[bstack_owned] d: BStackDeque<T> }`, the macro's
  generic-bound inference is built around direct type params and `Foreign<T>`
  targets; whether it propagates the needed `T`-bounds for a field typed
  `BStackDeque<T>` (an arbitrary generic block type) is unverified and may surface a
  confusing trait-bound error.
- **`#[embed]` of a collection is semantically dubious.** `#[embed] BStackDeque<T>`
  would inline only the fixed descriptor while the ring/nodes stay out-of-line;
  whether embed teardown/move handle a block whose `OnDisk` is a descriptor (not a
  self-contained payload) is unverified — embed was designed for self-contained
  blocks.
- **`#[bstack_mut]` is silently ignored on Vec / array / tuple / `Foreign` fields.**
  The mutator injection ([block.rs:2501](derive/src/block.rs#L2501)) runs *after*
  those field branches `continue` (e.g. the `Foreign` branch at
  [block.rs:649](derive/src/block.rs#L649)), so no `set_`/`replace_`/`raw_<field>_slice`
  is generated and **no error or warning** is emitted — only `#[embed]` errors. A
  user marking such a field `#[bstack_mut]` gets a silent no-op. (When the container
  mutator gap is eventually filled, a `Foreign` `replace_` must also free/repoint
  the *old cross-file target*, or it leaks — like the scalar owned `replace_`.)
- **`Foreign(SELF)` resolution trusts the caller's `local` with no check.**
  `Foreign::with(local, f)` resolves a `FileId::SELF` pointer against `local.stack()`
  unconditionally ([foreign.rs:148](src/foreign.rs#L148)) — nothing verifies `local`
  is the file the block actually lives in. A SELF `Foreign` read out of a
  foreign-resident block and resolved with the *home* allocator silently reads the
  wrong file. Relatedly, byte-copying a SELF `Foreign` across files (a plain clone
  of a field holding one) rebinds it to the destination file — a position-dependent
  pointer that silently changes meaning when it moves.
- **`bstack_cast!(foreign as BStackRef<T>)` yields an offset-only ref** valid only
  in the target's *own* file; resolving it against the local stack reads garbage
  (documented internally, not UB, but an easy silent-wrong-data footgun).
- **No explicit guard against `#[embed]` of an `(rc)` / `(rc, weak)` block.** Embed
  folds the child's data inline and frees its shell; an `(rc, weak)` child's
  *separate* control block would then keep a stale forward pointer to the freed data
  offset → corruption. It is currently prevented only incidentally (an rc block
  yields `BStackRc`, not the `BStackOwned<Child>` that embed's `new` requires), not
  by an explicit rejection.

### Limitation (by construction)

- **A collection cannot be shared (`#[bstack_strong]`/`#[bstack_weak]`).**
  Collections aren't `(rc)`/`(rc, weak)` blocks, so they don't implement
  `BStackShared`/`BStackWeakable`; two structs cannot share one collection the way
  they share an rc block. The only path is hand-rolling an rc wrapper block around
  it. Worth documenting so users don't expect a shared collection.
- **`bstack_move!` works only on `BStackBox`, not the other collections.** Only
  `BStackBox` implements `BStackMove` ([boxed.rs:169](src/stdlib/boxed.rs#L169));
  `map`/`deque`/`list`/`set`/`tree`/`string` do not, so `bstack_move!(collection)`
  won't compile. Probably intended (a map has no meaningful field-destructure), but
  it is an undocumented asymmetry. (It does *not* block a collection from being a
  moved-out `#[bstack_owned]` field — that path needs only `BStackBlock`.)
- **stdlib grow/realloc multi-block atomicity unverified.** `hashmap`/`deque`/`tree`
  growth allocates a fresh backing block, copies into it, flips the descriptor, and
  frees the old block. Whether each is a single atomic descriptor flip (leak-only on
  crash) or has a torn window was not checked — a category to verify, likely
  leak-only.

### Confirmed sound (do not re-flag)

- **Nested collections work** (`BStackHashMap<K, BStackDeque<V>>`, etc.): every
  collection overrides `__bstack_clone_into` / `__bstack_drop_children`, so when a
  collection is a value inside another block/collection, deep clone and teardown
  recurse correctly instead of byte-copy-aliasing the descriptor.
- **A block value with a `Foreign` field, stored in a collection**, dispatches the
  cross-file clone/free correctly — the map/deque/list clone/drop each value through
  the value block's generated `__bstack_*`, which include the `Foreign` handling.
- **rc/weak blocks can't be smuggled into collections as owned values**: `insert`
  et al. take `BStackOwned<V>`, which an `(rc)` block cannot produce.
