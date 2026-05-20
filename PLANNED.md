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

---

## Deprecating `BStackGuardedSlice::as_slice` in favor of read-only access

**Feature flag:** `guarded`
**Breaking change:** Yes — callers using `as_slice` would need to migrate to alternative APIs

### Motivation

The `as_slice()` method on `BStackGuardedSlice` returns a `Result<BStackSlice<'a, A>, io::Error>`, exposing the underlying slice directly. This creates a potential hazard: callers can use the returned slice to bypass the guard's hook system entirely, writing directly to the underlying `BStack` and potentially corrupting data that the guard was meant to protect (e.g., encrypted data written as plaintext, compressed data written uncompressed).

While such misuse does not compromise allocator structure or create memory-safety issues (hence `as_slice` is not `unsafe`), it can lead to logical data corruption that violates the guard's invariants. For example:

- An encryption guard expects all writes to go through `pre_write` to encrypt data
- A caller obtains the slice via `as_slice()` and writes directly, bypassing encryption
- The underlying data is now partially plaintext, violating the encryption guarantee

### Design options

#### Option: Deprecate and replace with reader-only access

Deprecate `as_slice()` and introduce a new method that returns a read-only cursor or reader type:

```rust
fn as_reader(&self) -> BStackSliceReader<'a, A>;
```

This prevents accidental writes while still allowing inspection of the underlying slice for debugging or metadata purposes.

### Arguments in favor of deprecation

1. **Safety by design** — Making it harder to accidentally bypass hooks aligns with Rust's principle of making the safe path the easy path
2. **Clear intent** — If a caller truly needs to bypass hooks, they can still use `unsafe { raw_block() }`, making the intent explicit
3. **Consistency** — The recommended API (subslicing, `read()`, `write()`) already doesn't use `as_slice()`

### Arguments against deprecation

1. **Not actually unsafe** — Data corruption through misuse doesn't violate memory safety or compromise allocator structure, so marking it unsafe would be misleading per Rust conventions
2. **Breaking change burden** — This would break existing code, and callers who correctly use `as_slice()` for read-only purposes would need to migrate
3. **Already addressed** — Documentation and API design already encourage using `read()` and `write()`. Callers using `as_slice()` are expected to understand the implications
4. **`raw_block()` exists** — The unsafe `raw_block()` method already exists for cases where hook bypass is needed, and its safety contract documents that hooks must be manually called. Making `as_slice()` also unsafe would be redundant
5. **Read-only restriction insufficient** — Even read-only access might be problematic for some guards (e.g., exposing ciphertext when only plaintext should be visible)

### Open questions

- Is restricting to read-only access sufficient, or do some guard implementations need to hide even read access to the raw slice?
- Would the migration burden outweigh the safety benefits?

---

## Adding `BStackVec<T>` for typed vector storage

**Feature flag:** `BSTACK_FEATURE_SET`
**Breaking change:** No (additive; new type only)

### Motivation

While `BStackSlice` provides a flexible byte-oriented view of allocated blocks, many applications would benefit from a typed vector abstraction that manages element layout, length tracking, and safe access patterns. A `BStackVec<T>` type would provide a convenient API for storing and manipulating sequences of typed data on a `BStack`, while still leveraging the underlying durability and crash-safety guarantees.

### Design

`BStackVec<T>` mirrors Rust's standard `Vec<T>` API, but backed by a `BStack` allocation. It would manage its own length and capacity metadata, stored in the block header or a reserved prefix.

#### Memory layout

The allocation layout would consist of:
- **Header** (16 bytes): length (u64) + capacity (u64)
- **Elements**: inline typed data, stored as `[T; capacity]`

The header occupies the first 16 bytes of the block, with element data following. This ensures that the metadata is always present and can be recovered even if the `BStackVec` handle is lost.

#### API surface

Core methods mirroring `Vec<T>`:

```rust
impl<'a, T: Copy, A: BStackAllocator> BStackVec<'a, T, A> {
    // Creation and destruction
    pub fn new(alloc: &'a A) -> Result<Self, A::Error>;
    pub fn with_capacity(capacity: u64, alloc: &'a A) -> Result<Self, A::Error>;
    pub fn from_slice(slice: &[T], alloc: &'a A) -> Result<Self, A::Error>;
    
    // Accessors
    pub fn len(&self) -> Result<u64, A::Error>;
    pub fn capacity(&self) -> Result<u64, A::Error>;
    pub fn is_empty(&self) -> Result<u64, A::Error>;
    
    // Element access
    pub fn get(&self, index: u64) -> Result<Option<T>, A::Error>;
    pub fn as_slice(&self) -> Result<BStackSlice<'a, A>, A::Error>;
    
    // Modification
    pub fn push(&mut self, value: T) -> Result<(), A::Error>;
    pub fn pop(&mut self) -> Result<Option<T>, A::Error>;
    pub fn truncate(&mut self, len: u64) -> Result<(), A::Error>;
    pub fn clear(&mut self) -> Result<(), A::Error>;
    pub fn reserve(&mut self, additional: u64) -> Result<(), A::Error>;
    pub fn resize(&mut self, new_len: u64, value: T) -> Result<(), A::Error>;
    
    // Iteration
    pub fn iter(&self) -> Result<BStackVecIter<'a, T, A>, A::Error>;
    pub fn iter_mut(&mut self) -> Result<BStackVecIterMut<'a, T, A>, A::Error>;
}
```

#### Growth strategy

When capacity is exceeded, `BStackVec` would use the `BStack`'s `realloc` to grow the block, similar to `Vec`'s doubling strategy (or a configurable policy). The metadata prefix would be preserved and updated with new capacity.

#### Guarding support

`BStackVec<T, A>` should integrate with `BStackGuardedSlice` by providing a `as_guarded_slice()` method that wraps the underlying slice with a guard, enabling transparent transforms across the entire vector.

#### Serialization considerations

`BStackVec` should be serializable to persistent storage by writing only the populated portion (length, not capacity) and reconstructable on load by reading back the metadata from the block header.

### Open questions

- **Generic bounds on `T`:** Should `T` be required to implement `Copy`, or should a `Drop` implementation be provided for types with destructors? A `Drop` impl would be complex, as it must be called on remaining elements when the vec is deallocated.
- **Error handling:** Should methods return `Result` for all operations, or should some (e.g., `len()`, `capacity()`) be infallible? Returning `Result` allows for better error propagation but may be more verbose for simple accessors.
- **Initialization of new elements:** When growing the vector, should new elements be zero-initialized, left uninitialized (i.e. `MaybeUninit`), or should we require `T` to be `Default` and call `default()`? Zero-initialization is safer and guranteed for newly allocated space, but may be unnecessary overhead for some types.
- **Zeroing on deallocation:** Should empty parts of the vector be zeroed on deallocation for security, or should this be left to the caller? Zeroing adds overhead but can prevent data leakage.

