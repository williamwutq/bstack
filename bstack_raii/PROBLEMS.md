# `bstack_raii` audit — problem list

A breadth-first audit (2026-08-11), grouped by the requested categories. Items
are observations to triage, not verified defects unless marked; each is brief and
shallow by design (flagged on suspicion, not deeply investigated). Solutions are
omitted except where trivial.

## Feature interactions

Cross-feature combinations, both the undesirable and the confirmed-sound (recorded
so they are not re-flagged).

### Undesirable / risky

- **[RESOLVED (scalar) 2026-08-15] `bstack_move!` of an owning `Foreign` now yields a
  RAII handle.** Added `ForeignOwned<'a,T>` / `ForeignRc<'a,T>` / `ForeignWeak<'a,T>` —
  the cross-file duals of `BStackOwned` / `BStackRc` / `BStackWeak`. Moving out a
  `#[bstack_owned/strong/weak] Foreign<T>` **scalar** field (or `Option<..>`) now returns
  the matching handle; `#[bstack_ref]` still returns a plain `Foreign` (owns nothing —
  correct). Each handle is **non-`Copy`** (an owner is used once), carries
  `bstack_drop(&home)` (registry-resolved cross-file free, same dispatch as the field
  teardown — the safe replacement for the `unsafe foreign_drop_*` helpers) and
  `into_foreign()` (relinquish → re-store into another owning field). Like `BStackOwned`,
  they don't free on `Drop`, so a forgotten handle still leaks (identical to in-file).
  **Residual:** only *scalar* foreign fields are wired — an owning `Foreign` inside a
  `Vec` / array / tuple / enum variant still moves out as a bare `Foreign` (the `Vec`
  case is partly covered because the moved-out `BStackVec<ForeignRepr>` handle itself
  owns the block; per-*element* owning handles for containers are a follow-up).
- **[MITIGATED 2026-08-15] Raw wire pointer bypasses cross-file ownership.**
  `Foreign<T>` is deliberately **not `Pod`** (only `Copy`), so it is correctly rejected
  from every `T: Pod` container. The old public `Pod` `ForeignPtr` was a hole (it could
  be stored in a `BStackBox`/`BStackVec` as opaque bytes with no owning dispatch). It has
  been renamed to `ForeignRepr`, made `#[doc(hidden)]`, and dropped from the public
  prelude, and it now carries **no resolution API** — there is no safe way to turn a
  stored `ForeignRepr` back into a `Foreign` (only the `unsafe fn from_repr`, called by
  generated code). So `BStackVec<ForeignRepr>` still compiles (it is `pub` for macro
  output) but stores inert bytes that reach no target without an explicit `unsafe`,
  which is the correct boundary. User code never names the type.
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
- **[MOSTLY CLOSED 2026-08-15] `Foreign(SELF)` position-dependence.** `Foreign<T>` is
  now `Foreign<'a, T>`, and a field accessor binds `'a` to the `&'a BStack` it read
  through (an enum variant to the read/move borrow). A SELF pointer is therefore
  **borrow-bound to its home file** and can no longer be *stored into another file* — the
  byte-copy-rebind footgun is closed for the safe (generated-accessor) path, and an
  explicit pointer resolves through the registry regardless. Two residuals remain, both
  now behind `unsafe` or a documented caveat: (a) the raw `unsafe fn Foreign::new` can
  fabricate a `'static` SELF whose deref against a wrong-but-co-live `local` reads the
  wrong file (the lifetime pins *scope*, not value identity — its safety contract says
  "resolve only against its home"); (b) a persisted SELF is still position-dependent on
  disk (allowed by design — persisted SELF is legal), so migrating the *containing*
  block to another file rebinds it. Both are `unsafe`/contract-level, not safe-API holes.
- **`bstack_cast!(foreign as BStackRef<T>)` yields an offset-only ref** valid only
  in the target's *own* file; resolving it against the local stack reads garbage
  (documented internally, not UB, but an easy silent-wrong-data footgun).
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
