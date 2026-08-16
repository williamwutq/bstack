//! The `bstack_raii` feature **hypercube** — end-to-end (integration) tests of the
//! generated public API.
//!
//! The feature space is a sparse hypercube: a *container* (block / enum, in some
//! rc-mode) holds *slots* (fields / variant payloads); each slot has an ownership
//! *kind* (pod / owned / strong / weak / ref / embed) and a *shape* (`T`, `Vec<T>`,
//! `[T; N]`, tuple, `Foreign<T>`, …) over an *element* type. The crate supports a
//! fixed set of *operations* (construct / read / mutate / teardown / clone / move /
//! cast / refcount / foreign-resolve / atomicity / wal).
//!
//! Rather than one bespoke fixture per cell (the old `src/tests.rs` had ~140), we
//! declare the axes **once** as reusable "sink" fixtures in [`fixtures`], and
//! organize the tests **by operation** — one module (file) per concern, each
//! iterating the shared sinks. Shared harness lives in [`common`].
//!
//! Integration tests see only the *public* API; refcount-counting and cross-file
//! foreign resolution (which need crate internals / a scoped registry) stay as unit
//! tests in `src/tests.rs`.

mod common;
mod fixtures;

mod atomicity;
mod cast;
mod clone;
mod compile_fail;
mod construct;
mod foreign;
mod move_;
mod mutate;
mod refcount;
mod reference;
mod teardown;
