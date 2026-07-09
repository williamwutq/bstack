# bstack_raii

A typed, RAII-style ownership, lifetime, and on-disk-layout layer over the
[`bstack`](https://github.com/williamwutq/bstack) allocation primitives
(`BStackRange`, `BStackSlice`, `BStackOwnedSlice`). It decouples disk-level
destruction (`BStackDrop`) from Rust's process-scoped `Drop`, giving persistent
storage the ergonomics of C++ `unique_ptr` / `shared_ptr` / `weak_ptr`.

The full design is in [`RAII.md`](../RAII.md) at the repository root. This crate
is its implementation.

## Why a separate crate (not a `bstack` feature)

`bstack` keeps stable features ABI-stable. The RAII layer introduces a large,
not-yet-stable ABI surface (block layouts, control blocks, refcounting), so it
lives outside the `bstack` package until it settles.

## Layout

```
bstack_raii/            # runtime: traits + on-disk header + handle types
  src/lib.rs
  derive/               # proc-macro crate: #[bstack_block], bstack_move!, bstack_cast!
    src/lib.rs
```

`bstack_raii` re-exports the macros, so downstream code depends only on
`bstack_raii`. It is a **self-contained cargo workspace**: the outer `bstack`
repo has no `[workspace]`, so root-level `cargo` commands never build this crate,
and `bstack`'s CI is configured to ignore `bstack_raii/**`.

## Status

Scaffold. Traits, on-disk header, typed-reference and child-handle types, and
the three proc-macro entry points are stubbed with their final signatures and
generation contracts; the bodies (marked `todo!()` / `TODO`) are the work ahead.

## Dependency on bstack

`bstack` 0.4.0 is feature-complete but not yet on crates.io, so this crate
depends on the GitHub source:

```toml
bstack = { git = "https://github.com/williamwutq/bstack", features = ["alloc", "set", "atomic"] }
```

Pin a `branch`/`rev`/`tag` (or switch to `path = ".."` for local iteration) once
the RAII primitives land on a stable ref.
