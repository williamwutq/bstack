# Planned Features

This document outlines upcoming features planned for the `bstack` crate. These enhancements aim to improve usability, performance, and integration while maintaining the core principles of durability, crash-safety, and simplicity. Changes aim to be backward-compatible. New features are suggested to be added as optional features under feature flags and new traits, instead of modifying existing ones, to avoid breaking changes. All features aim to follow [Rust's API design guidelines](https://rust-lang.github.io/api-guidelines/) and BStack's design principles.

---

## Make from_bytes of `BStackSlice` unsafe

**Breaking change:** Yes — mechanical (one line per existing call to `BStackSlice::from_bytes`)

### Motivation

`BStackSlice::from_bytes` currently takes a byte array and decodes it into a slice. This operation is inherently unsafe because the byte array may not represent a valid slice (e.g., it could have an offset and length that point outside the bounds of the underlying data). Making this method `unsafe` signals to users that they must ensure the validity of the input bytes, and it encourages safer usage patterns.

### Design

Change the signature of `from_bytes` to:

```rust
pub unsafe fn from_bytes(allocator: &'a A, bytes: [u8; 16]) -> Self
```

---

## `type Error` — associated error type for `BStackAllocator`

**Breaking change:** Yes — mechanical (one line per existing `impl BStackAllocator`)

This is a breaking but insignificant change to the `BStackAllocator` API, adding an associated error type for the `io::Result` returned by `alloc`, `realloc`, and `dealloc`.

### Motivation

Currently, `BStackAllocator` methods return `io::Result`, which is a generic error type that may not provide sufficient context for allocator-specific errors. By introducing an associated error type, we can allow allocators to define their own error types that carry more specific information about allocation failures, such as out-of-memory conditions or fragmentation issues.

### Design

Add an associated type to `BStackAllocator`:

```rust
pub trait BStackAllocator: Sized {
    type Error;
}

```

Then, change the return types of `alloc`, `realloc`, and `dealloc` to use this associated error type.

---

## `type Allocated` — allocator-native handle type

**Breaking change:** Yes — mechanical (one line per existing `impl BStackAllocator`)

This is a breaking but insignificant change to the `BStackAllocator` API, adding an associated type for the handle returned by `alloc` and accepted by `realloc` and `dealloc`. Users who implement `BStackAllocator` are not as common as users who call it, so the break is limited to a small number of implementors and is fully mechanical to fix. The change is justified by the significant ergonomic and performance benefits for implementors.

### Motivation

`BStackAllocator::alloc`, `realloc`, and `dealloc` currently traffic in `BStackSlice`, which only carries `(offset, len)`. Allocators like `FirstFitBStackAllocator` must re-derive block-level metadata (aligned size, block boundaries) on every `realloc` and `dealloc` call because that information was discarded when the handle was returned to the caller. An associated handle type lets allocators round-trip metadata that they already know at allocation time.

### Design

Add a GAT to `BStackAllocator`:

```rust
pub trait BStackAllocator: Sized {
    /// The handle type returned by `alloc`/`realloc` and accepted by
    /// `realloc`/`dealloc`. Must be `Copy` (cheap to pass by value) and
    /// convertible to `BStackSlice` for generic and I/O use.
    ///
    /// Simple allocators set `type Allocated<'a> = BStackSlice<'a, Self>`.
    /// Richer allocators embed additional metadata in a newtype.
    type Allocated<'a>: Copy + Into<BStackSlice<'a, Self>>
    where
        Self: 'a;

    fn alloc(&self, len: u64) -> io::Result<Self::Allocated<'_>>;
    fn realloc<'a>(
        &'a self,
        slice: Self::Allocated<'a>,
        new_len: u64,
    ) -> io::Result<Self::Allocated<'a>>;
    fn dealloc(&self, slice: Self::Allocated<'_>) -> io::Result<()> {
        Ok(())
    }
    // stack, into_stack, len, is_empty — unchanged
}
```

`LinearBStackAllocator` adds `type Allocated<'a> = BStackSlice<'a, Self>` — behaviour unchanged. `FirstFitBStackAllocator` defines:

```rust
#[derive(Copy, Clone)]
pub struct FirstFitAllocated<'a> {
    inner: BStackSlice<'a, FirstFitBStackAllocator>,
    block_size: u64,
}

impl<'a> Into<BStackSlice<'a, FirstFitBStackAllocator>> for FirstFitAllocated<'a> {
    fn into(self) -> BStackSlice<'a, FirstFitBStackAllocator> { self.inner }
}
```

`dealloc` and `realloc` receive `block_size` directly rather than re-computing `align_len(slice.len())` on every call.

### Open questions

- Associated type defaults (`type Allocated<'a> = BStackSlice<'a, Self>`) require `#![feature(associated_type_defaults)]`, which is not yet stable. Until stabilisation, all existing `impl BStackAllocator` blocks must add the line explicitly — a breaking but fully mechanical migration.
- Whether to require `From<BStackSlice<'a, Self>>` as an additional bound (allowing generic code to construct a handle from a plain slice via re-computation) is left open. It is not required for correctness and can be added later. Semantics and must be documented as an explicit guarantee, not left ambiguous.

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

