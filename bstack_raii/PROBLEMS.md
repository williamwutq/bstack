# `bstack_raii` audit — problem list

A breadth-first audit (2026-08-11), grouped by the requested categories. Items
are observations to triage, not verified defects unless marked; each is brief and
shallow by design (flagged on suspicion, not deeply investigated). Solutions are
omitted except where trivial.

## Feature interactions

Cross-feature combinations, both the undesirable and the confirmed-sound (recorded
so they are not re-flagged).

### Undesirable / risky

- **`#[bstack_mut]` is silently ignored on Vec / array / tuple / `Foreign` fields.**
  The mutator injection ([block.rs:2501](derive/src/block.rs#L2501)) runs *after*
  those field branches `continue` (e.g. the `Foreign` branch at
  [block.rs:649](derive/src/block.rs#L649)), so no `set_`/`replace_`/`raw_<field>_slice`
  is generated and **no error or warning** is emitted — only `#[embed]` errors. A
  user marking such a field `#[bstack_mut]` gets a silent no-op. (When the container
  mutator gap is eventually filled, a `Foreign` `replace_` must also free/repoint
  the *old cross-file target*, or it leaks — like the scalar owned `replace_`.)
