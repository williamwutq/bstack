# Planned Features

This document outlines upcoming features planned for the `bstack` crate. These enhancements aim to improve usability, performance, and integration while maintaining the core principles of durability, crash-safety, and simplicity. Changes aim to be backward-compatible. New features are suggested to be added as optional features under feature flags and new traits, instead of modifying existing ones, to avoid breaking changes. All features aim to follow [Rust's API design guidelines](https://rust-lang.github.io/api-guidelines/) and BStack's design principles.

---

## Making `BStackAllocator::realloc` and `dealloc` `unsafe fn`

**Feature flag:** N/A (core trait)
**Breaking change:** Yes — all callers and all `impl BStackAllocator` blocks must change

### Motivation

Both `realloc` and `dealloc` carry a *slice-origin requirement* that the
type system cannot enforce: the supplied handle must have been returned by
`alloc` or a prior `realloc` on the **same allocator instance**.  Passing an
arbitrary slice — constructed via `BStackSlice::from_raw_parts` or derived
via `subslice` / `subslice_range` — may silently corrupt the allocator's
persistent on-disk metadata (free-list pointers, AVL node fields, block
headers/footers) in a way that is difficult or impossible to recover from.

This is structurally identical to why `std::alloc::GlobalAlloc::dealloc` and
`realloc` are `unsafe fn`: the contract cannot be expressed as a type
invariant and violation causes severe, irreversible state corruption.

### Design

Change the trait signatures:

```rust
// Before
fn realloc<'a>(&'a self, handle: Self::Allocated<'a>, new_len: u64)
    -> Result<Self::Allocated<'a>, Self::Error>;
fn dealloc(&self, handle: Self::Allocated<'_>) -> Result<(), Self::Error>;

// After
unsafe fn realloc<'a>(&'a self, handle: Self::Allocated<'a>, new_len: u64)
    -> Result<Self::Allocated<'a>, Self::Error>;
unsafe fn dealloc(&self, handle: Self::Allocated<'_>) -> Result<(), Self::Error>;
```

All internal implementations (`LinearBStackAllocator`, `FirstFitBStackAllocator`,
`GhostTreeBstackAllocator`) would be updated to `unsafe fn` as well.

### Open questions

**Is this change actually necessary given `BStackSlice::from_raw_parts` is
already `unsafe`?**

Since `BStackSlice::new` is now deprecated and `from_raw_parts` is `unsafe`,
a caller cannot construct an allocator-corrupting slice *without already being
in an `unsafe` block*.  A well-reviewed `unsafe` block that constructs a
`BStackSlice::from_raw_parts` can be expected to read the safety contract and
comply with the origin requirement.

The remaining concern is sub-slices: `subslice` and `subslice_range` are *safe*
functions that produce slices with an origin different from any allocator-returned
handle.  A caller could accidentally (or intentionally) pass such a sub-slice to
`dealloc` without any `unsafe` at the call site.  Making `dealloc` / `realloc`
`unsafe fn` would force that call site into an `unsafe` block and require the
caller to explicitly reason about validity.

Counter-argument: the pain of a breaking change across every `impl BStackAllocator`
and every call site is high, and the hazard window is now significantly narrowed
because constructing a bad handle already requires `unsafe`.  A simpler alternative
is to mark `subslice` and `subslice_range` with a prominent warning that the
returned slices must not be passed to `realloc` or `dealloc`.

---

## Optimizing `FirstFitBStackAllocator` with atomic feature

**Feature flag:** `atomic`
**Breaking change:** No (if added as optional)

### Motivation

The `FirstFitBStackAllocator` could benefit from atomic operations to improve performance and thread-safety in concurrent environments. Atomic operations can reduce contention and allow for lock-free or reduced-lock implementations in certain scenarios. It also allows for better crash resilience by ensuring that metadata updates are atomic, reducing the risk of corruption in the event of a crash.

### Design

[To be determined — implementation details would involve using atomic primitives for metadata updates and allocation tracking.]

### Open questions

- Should this optimization be added as an optional feature flag, or required for all users? If added, we end up maintaining two implementations of `FirstFitBStackAllocator`; if required, all users need the atomic flag.

