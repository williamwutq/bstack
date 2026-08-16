# `bstack_raii` audit — problem list

A breadth-first audit (2026-08-11), grouped by the requested categories. Items
are observations to triage, not verified defects unless marked; each is brief and
shallow by design (flagged on suspicion, not deeply investigated). Solutions are
omitted except where trivial.

## Feature interactions

Cross-feature combinations, both the undesirable and the confirmed-sound (recorded
so they are not re-flagged).

### Undesirable / risky

- [CONFIRMED-DEFECT] **`#[embed]` of a collection is semantically dubious.** `#[embed] BStackDeque<T>`
  would inline only the fixed descriptor while the ring/nodes stay out-of-line;
  whether embed teardown/move handle a block whose `OnDisk` is a descriptor (not a
  self-contained payload) is unverified — embed was designed for self-contained
  blocks. Tests (`embed_collection_build_read_teardown`, `embed_collection_move_rehomes`,
  `embed_collection_clone_is_independent`) show build/read/teardown and `bstack_move!`
  re-home are **correct** (the descriptor's absolute ring/element offsets survive), but
  deep clone is **broken**: the collections' hand-written impls do not override
  `__bstack_clone_children_inplace`, whose default returns the descriptor *verbatim*, so a
  cloned embedded deque aliases the source's out-of-line ring and elements — tearing the
  clone down frees the source's children (double-free). The fix is per-collection
  `__bstack_clone_children_inplace` = `__bstack_clone_into` without the handle allocation.
- **`#[bstack_mut]` is silently ignored on Vec / array / tuple / `Foreign` fields.**
  The mutator injection ([block.rs:2501](derive/src/block.rs#L2501)) runs *after*
  those field branches `continue` (e.g. the `Foreign` branch at
  [block.rs:649](derive/src/block.rs#L649)), so no `set_`/`replace_`/`raw_<field>_slice`
  is generated and **no error or warning** is emitted — only `#[embed]` errors. A
  user marking such a field `#[bstack_mut]` gets a silent no-op. (When the container
  mutator gap is eventually filled, a `Foreign` `replace_` must also free/repoint
  the *old cross-file target*, or it leaks — like the scalar owned `replace_`.)
- **No explicit guard against `#[embed]` of an `(rc)` / `(rc, weak)` block.** Embed
  folds the child's data inline and frees its shell; an `(rc, weak)` child's
  *separate* control block would then keep a stale forward pointer to the freed data
  offset → corruption. Verified current behavior: there is **no guard at expansion
  time** — `#[bstack_block] struct X { #[embed] r: RcChild, .. }` compiles cleanly, the
  derive never inspects the child's ownership mode. The only obstruction is
  incidental and surfaces at the constructor call site as a raw type mismatch:
  ```
  error[E0308]: mismatched types
     |     let _ = X::new(&alloc, c, ..);
     |             -------        ^^ expected `BStackOwned<RcChild>`, found `BStackRc<'_, RcChild, ...>`
  ```
  because an rc block's `new` yields `BStackRc`, not the `BStackOwned<Child>` the
  generated embed constructor requires. Not a trait-bounds message, but also not a
  directed diagnostic, and the derive admits the composition, so the guard is
  bypassable via `unsafe { BStackOwned::from_raw(RcChild::from_range(..)) }` — after
  which the stale-forward-pointer corruption would occur. A proper fix is an explicit
  rejection in the derive (error on `#[embed]` of a block whose mode is not plain).
