# `bstack_raii` audit — problem list

A breadth-first audit (2026-08-11), grouped by the requested categories. Items
are observations to triage, not verified defects unless marked; each is brief and
shallow by design (flagged on suspicion, not deeply investigated). Solutions are
omitted except where trivial.

## Feature interactions

Cross-feature combinations, both the undesirable and the confirmed-sound (recorded
so they are not re-flagged).

### Undesirable / risky

- **`bstack_move!` of an `#[bstack_owned] Foreign` hands back a non-RAII pointer.**
  Moving an owned *in-file* field out yields a `BStackOwned<Child>` (a typed owning
  handle with `.bstack_drop`); moving an owned *foreign* field out yields a bare
  `Foreign<T>` — a `Copy` wide pointer with no owning-drop method — so the caller must
  re-store it or free it via the `unsafe foreign_drop_*` helpers, and simply dropping
  it leaks the target. Consistent with `Foreign` being a non-RAII pointer (and with
  `BStackOwned` also freeing nothing on `Drop`), but an ergonomic asymmetry; there is
  no `BStackOwned`-equivalent RAII wrapper for a moved-out cross-file owner.
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
