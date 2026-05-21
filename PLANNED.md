# Planned Features

This document outlines upcoming features planned for the `bstack` crate. These enhancements aim to improve usability, performance, and integration while maintaining the core principles of durability, crash-safety, and simplicity. Changes aim to be backward-compatible. New features are suggested to be added as optional features under feature flags and new traits, instead of modifying existing ones, to avoid breaking changes. All features aim to follow [Rust's API design guidelines](https://rust-lang.github.io/api-guidelines/) and BStack's design principles.

---

## NOT PLANNED

### Deprecating `BStackGuardedSlice::as_slice` in favor of read-only access

Reasons:

`BStackGuardedSlice::as_slice` is not actually unsafe as data corruption through misuse doesn't violate memory safety or compromise allocator structure, so marking it unsafe would be misleading per Rust conventions. Documentation and API design already encourage using `read()` and `write()`. Callers using `as_slice()` are expected to understand the implications. In addition, the unsafe `raw_block()` method already exists for cases where hook bypass is needed, and its safety contract documents that hooks must be manually called. If this function is deprecated, it would break existing code, and callers who correctly use `as_slice()` for read-only purposes would need to migrate. Therefore, `as_slice()` as a safe function that returns a `BStackSlice` is sufficient, and the safety contract can be clearly documented without making it `unsafe fn`.

### Replacing `std::sync::RwLock` with `parking_lot::RwLock` or `usync::RwLock`

Reasons:

There are insufficient evidences that changing the implementation of RwLock will bring sufficient performance improvements to `BStack`. `std::sync::RwLock` remains the default. On Linux, `std` wins 10/14 benchmarks, often by 20–30% on write-heavy operations which dominate real workloads. The wins for `parking_lot` and `usync` are limited to fast read-path operations (`peek`, `get`, `len`) which are not the bottleneck.

For context, accepted optimizations like caching and locked region have demonstrated **~2-10× improvements**. The gains shown here do not meet that bar and introduce an additional dependency without a compelling cross-platform case.

Reference: https://github.com/williamwutq/bstack/pull/3

### Making `BStackAllocator::realloc` and `dealloc` `unsafe fn`

Reasons:

While `realloc` and `dealloc` have a slice-origin requirement, the hazard window is now significantly narrowed because constructing a bad handle already requires `unsafe` (via `BStackSlice::from_raw_parts`). A well-reviewed `unsafe` block that constructs a `BStackSlice::from_raw_parts` can be expected to read the safety contract and comply with the origin requirement. The remaining concern is sub-slices: `subslice` and `subslice_range` are *safe* functions that produce slices with an origin different from any allocator-returned handle. However, marking `realloc` and `dealloc` as `unsafe fn` would force all call sites into an `unsafe` block, which may be unnecessarily burdensome given the already narrow hazard window. The best conventions use `unsafe` for operations that can cause undefined behavior and overuse of `unsafe` can lead to desensitization and misuse. As of `BStack` 0.2, the allocator interfaces are already mature and breaking changes should be avoided. Since the origin requirement is a safety contract that can be documented and enforced through careful API design, it may be sufficient to keep `realloc` and `dealloc` as safe functions while clearly documenting the requirements and risks.

Furthermore, `alloc`, `realloc`, and `dealloc` in the `BStackAllocator` trait do not need to operate on `BStackSlice` directly — they operate on an associated handle type. Custom allocator implementations can define handle types that do not support sub-slicing at all, eliminating the origin problem at the type level for those allocators. The sub-slice concern is therefore a consequence of a specific design choice (using `BStackSlice` itself as the raw handle) rather than an inherent flaw in the trait. The recommended approach is for allocators to use a handle type distinct from `BStackSlice`, where converting from handle to `BStackSlice` is straightforward but the reverse is not possible — making the origin requirement a type-level guarantee. The default allocators in this crate currently do not follow this recommendation, but that is a separate concern addressed in the planned features below.

---

## Introducing a newtype handle for default allocators

**Breaking change:** Yes

### Motivation

The default allocators in this crate (e.g., `FirstFitBStackAllocator`) currently use `BStackSlice` as their raw handle type. Because `BStackSlice` exposes safe `subslice` and `subslice_range` methods, it is possible to produce a slice with an origin that does not correspond to any allocator-returned allocation and then pass it to `realloc` or `dealloc`, violating the origin requirement without any `unsafe` block at the call site. A dedicated newtype handle, constructible only by the allocator, closes this gap at the type level: it can be cheaply dereferenced to `BStackSlice` for reads and writes, but `BStackSlice` cannot be converted back into it, so sub-slices are statically prevented from being used as allocator handles.

A fuller redesign — described below — also reconsiders the roles of `BStackSlice` and the allocated handle type more broadly. The two types serve different purposes and should have different capabilities and ownership semantics to reflect that.

### Design

#### Allocated handle type

Introduce an allocated handle type (name TBD — see Open questions) as the associated handle type returned by `alloc` and accepted by `realloc` and `dealloc`. This type represents ownership of a specific allocation and has the following properties:

- **Not `Copy`.** The handle represents a unique allocation. Allowing it to be copied would make it possible to call `dealloc` on one copy while the other remains in use, or to pass two copies to `realloc` independently, both corrupting allocator metadata. Non-`Copy` ensures the handle is consumed when the allocation is released or resized.
- **No subslicing.** The handle does not expose `subslice` or `subslice_range`. A sub-range derived from an allocation is not itself an allocation, and preventing this at the type level closes the origin-requirement gap entirely without relying on documentation or convention.
- **Lifetime tied to the allocation, not the allocator.** The handle's lifetime parameter expresses that the underlying region is valid for reads and writes, not that the allocator is borrowed. In practice, this means the handle holds `&'a A` where `'a` is the allocator borrow, but the semantic intent is that the region is live for `'a` — which is guaranteed as long as the allocator (and thus the backing `BStack`) is alive.
- **Convertible to `BStackSlice` for I/O.** The handle can produce a `BStackSlice` for reading and writing the allocated region. This conversion is cheap (no allocation) and explicit.
- **Unsafe construction from `(allocator, BStackSlice)`.** For advanced use cases — such as reconstructing a handle from a serialized offset/length pair — an `unsafe` constructor takes an allocator reference and a `BStackSlice` and produces a handle. The caller must ensure the slice describes a valid, allocator-owned region.

#### `BStackSlice` without allocator pointer

`BStackSlice` currently carries a generic allocator reference `&'a A`. Since `BStackSlice` cannot directly call `realloc` or `dealloc` (those require the allocated handle type), it does not need to know the allocator's type. It only needs access to the backing `BStack` for I/O — and it should continue to carry a `&'a BStack` reference for exactly that purpose. Replacing `&'a A` with `&'a BStack` directly would:

- Simplify the type signature from `BStackSlice<'a, A>` to `BStackSlice<'a>`, removing the allocator type parameter entirely.
- Allow `BStackSlice` to be used across allocators or outside any allocator context, since the `&'a BStack` reference is sufficient for all read and write operations the slice needs to perform.
- Make the role of `BStackSlice` clearer: it is a view, not a handle. Like `&[u8]`, it is `Copy`, has no ownership, and carries no allocation identity.

The lifetime of a `BStackSlice` should tie to the stack (or the allocation it was derived from), but this is enforced by the context — the allocator or the allocated handle — rather than by `BStackSlice` itself. This is a known limitation: the crate cannot statically enforce that a `BStackSlice` does not outlive its allocation, since allocations have no RAII drop and the file is not memory. The allocated handle type is the mechanism for expressing allocation identity; `BStackSlice` is not.

This is a larger breaking change than the handle type alone, as it touches every existing use of `BStackSlice<'a, A>`.

### Open questions

- **Handle type name.** The name should clearly distinguish the handle from `BStackSlice` and signal that it represents an allocation, not just a region. Candidates include `BStackAlloc`, `BStackBlock`, or `BStackRegion`. The name should not suggest it is a slice (to avoid confusion with subslicing) and should not be so generic that it clashes with custom allocator handle types.
- **Whether to remove the allocator pointer from `BStackSlice`.** This is the more invasive part of the redesign. It may be worth doing as a separate change or deferring until the allocated handle type is stable. If deferred, `BStackSlice` retains `&'a A` temporarily, with a planned migration.
- **Unsafe back-conversion.** Should the allocated handle expose an `unsafe` method to recover a `BStackSlice` with no allocator type attached? This would be useful for serialization and low-level introspection, but requires the caller to uphold the origin invariant manually.

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

## Adding `SlabBStackAllocator` for fixed-block slab allocation

**Feature flag:** `alloc` + `set`
**Breaking change:** No (additive; new type only)

### Motivation

Workloads that repeatedly allocate and free regions of a similar small size benefit from a slab allocator that eliminates fragmentation by keeping all blocks the same size. Blocks carry **no metadata**. The free list pointers are stored inside freed blocks.

### Design

Introduce `SlabBStackAllocator` in `src/alloc/slab.rs`, implementing `BStackSliceAllocator` like all existing default allocators.

```rust
pub struct SlabBStackAllocator {
    stack: BStack,
    block_size: u64, // cached from header; must be ≥ 8; never changes after init
}
```

The payload begins with an allocator header followed by the block arena:

```text
[ reserved(24) | magic[8] | block_size[8] | free_head[8] | arena ... ]
  ^               ^
  offset 0        offset 24 (allocator header start)
  user data       offset 48 (arena start)
```

The magic number (e.g. `ALSL\x00\x01\x00\x00`) identifies the format and version. `block_size` and `free_head` are written back to the header on every mutation, making them durable. All blocks in the arena are exactly `block_size` bytes with **no header or footer**. When a block is free, its first 8 bytes hold the offset of the next free block (little-endian `u64`, sentinel `u64::MAX`); when live, those bytes belong to the caller — there is no per-block metadata at any time.

**`alloc(len)`:** If `len ≤ block_size`, pop from the free list (zero the next-pointer, update header) or extend the tail by `block_size`. If `len > block_size`, always extend the tail to a multiple of `block_size`. The returned slice covers exactly `len` bytes.

**`dealloc(slice)`:** If `len > block_size` and the slice is the tail, `discard`. For slab blocks, segment the backing region into `block_size` chunks, prepend each to the free list, and write the new `free_head` to the header. For oversized non-tail blocks, if the block size is N * `block_size`, segment it into N blocks, prepend all to the free list, and write the new `free_head` to the header. This allows oversized non-tail blocks to be reused without leaking.

**`realloc`:** If the block is oversized and the new size is ≤ `block_size`, the excess portion can be segmented into a new block and added to the free list. If the block is oversized and the new size is > `block_size`, it can be reallocated in place if it's the tail or if the next block(s) are free and can be coalesced. Otherwise, a new block can be allocated, data copied, and the old block deallocated.

### Open questions

- Should `block_size < 8` be a `Result` error or a panic?

---

## Adding `BStackVec<T>` for typed vector storage

**Feature flag:** `alloc` + `set`
**Breaking change:** No (additive; new type only)

### Motivation

`BStackByteVec` covers the common byte-buffer use case, but many applications store sequences of fixed-width typed values (e.g. `u32`, `u64`, or a small `repr(C)` struct). A general `BStackVec<T>` would extend the same API to arbitrary element types. The reason this is deferred rather than shipped today is soundness: reading back elements via raw bytes requires that every byte pattern is a valid `T`, and writing elements requires that no uninitialized padding bytes leak into the block. The `Copy` bound alone is not sufficient — types such as `bool`, `char`, `NonZero*`, enums, or structs with padding fields violate one or both requirements. Shipping an unsound API in a 1.0-ready crate is unacceptable, so the general case is deferred until a clean solution is in place.

### Design

`BStackVec<T>` mirrors `BStackByteVec` exactly, but parameterised by `T`. The on-disk memory layout is the same 16-byte header followed by `[T; cap]` elements, using the same little-endian `u64` encoding for `len` and `cap`.

#### Sound element bound

The key design decision is the trait bound on `T`. Options in increasing order of complexity:

1. **`bytemuck::Pod`** (external dependency): `bytemuck::Pod` guarantees no padding and any-bit-pattern validity. This is the most ergonomic approach but adds a dependency that may not be acceptable for all users.
2. **`zerocopy::FromBytes + zerocopy::IntoBytes`** (external dependency): similar guarantees via the `zerocopy` crate.
3. **A crate-local sealed `BStackPod` marker trait**: implementors manually assert the POD invariants. No external dependency, but requires users to `unsafe impl` the trait for their types, and adds maintenance burden.
4. **Make all typed element operations `unsafe`**: put the soundness obligation on the caller at each call site. Consistent with how `std::slice::from_raw_parts` works.

Option 1 (`bytemuck::Pod`) is the recommended approach if a dependency is acceptable; otherwise option 4 (unsafe element ops) is the fallback.

#### API surface

The API mirrors `BStackByteVec` with `u8` replaced by `T`:

```rust
impl<'a, T: bytemuck::Pod, A: BStackSliceAllocator> BStackVec<'a, T, A> {
    pub fn new(alloc: &'a A) -> io::Result<Self>;
    pub fn with_capacity(capacity: u64, alloc: &'a A) -> io::Result<Self>;
    pub fn from_slice(data: &[T], alloc: &'a A) -> io::Result<Self>;
    pub unsafe fn from_raw_block(slice: BStackSlice<'a, A>) -> Self;
    pub fn len(&self) -> io::Result<u64>;
    pub fn capacity(&self) -> io::Result<u64>;
    pub fn is_empty(&self) -> io::Result<bool>;
    pub fn get(&self, index: u64) -> io::Result<Option<T>>;
    pub fn read_vec(&self) -> io::Result<Vec<T>>;
    pub fn as_slice(&self) -> io::Result<BStackSlice<'a, A>>;
    pub fn push(&mut self, value: T) -> io::Result<()>;
    pub fn pop(&mut self) -> io::Result<Option<T>>;
    pub fn truncate(&mut self, new_len: u64) -> io::Result<()>;
    pub fn clear(&mut self) -> io::Result<()>;
    pub fn reserve(&mut self, additional: u64) -> io::Result<()>;
    pub fn resize(&mut self, new_len: u64, value: T) -> io::Result<()>;
    pub fn iter(&self) -> io::Result<BStackVecIter<'_, 'a, T, A>>;
    pub unsafe fn raw_block(&self) -> BStackSlice<'a, A>;
    pub fn into_raw_block(self) -> BStackSlice<'a, A>;
    pub fn dealloc(self) -> io::Result<()>;
}
```

### Open questions

- **Dependency policy:** Is adding `bytemuck` or `zerocopy` acceptable? If so, should it be an optional feature flag (e.g. `alloc + set + pod`) or always-on with `alloc + set`?
- **Sealed marker trait:** If no external dependency is acceptable, define a local `unsafe trait BStackPod` sealed within the crate. Decide whether to provide blanket impls for all primitive integer and float types.
- **ZST policy:** `size_of::<T>() == 0` makes `elem_offset` a no-op and `block_size` equal to `HEADER_LEN` regardless of capacity. Document clearly and decide whether ZST vecs are supported or rejected at construction time.
- **Padding detection:** If using the sealed-trait approach, consider adding a compile-time assertion (`assert_eq!(size_of::<T>(), /* expected */)`-style) or a `#[repr(C)]` requirement to help implementors avoid accidental padding.
- **Generic bounds on `T`:** Should `T` be required to implement `Copy`, or should a `Drop` implementation be provided for types with destructors? A `Drop` impl would be complex, as it must be called on remaining elements when the vec is deallocated.
- **Error handling:** Should methods return `Result` for all operations, or should some (e.g., `len()`, `capacity()`) be infallible? Returning `Result` allows for better error propagation but may be more verbose for simple accessors.
- **Initialization of new elements:** When growing the vector, should new elements be zero-initialized, left uninitialized (i.e. `MaybeUninit`), or should we require `T` to be `Default` and call `default()`? Zero-initialization is safer and guranteed for newly allocated space, but may be unnecessary overhead for some types.
- **Zeroing on deallocation:** Should empty parts of the vector be zeroed on deallocation for security, or should this be left to the caller? Zeroing adds overhead but can prevent data leakage.

---

## Requiring `&mut BStackSlice` for mutation

**Feature flag:** `set`
**Breaking change:** Yes

### Motivation

All write methods on `BStackSlice` — `write`, `write_range`, `zero`, `zero_range`, `writer`, and `writer_at` — currently take `&self`. This is because `BStack` uses interior mutability (`RwLock<File>`), so no exclusive reference to the slice is needed at the Rust level to perform the underlying I/O. However, this means a shared, copied, or aliased `BStackSlice` can silently mutate the same region of the file from multiple places, with no indication at the call site that mutation is occurring. This is surprising and contrary to Rust conventions.

Requiring `&mut self` for write methods makes mutation visible and explicit. It does not provide exclusive access to the underlying file region — `BStackSlice` is `Copy` and the file is shared — but it signals intent and prevents accidental mutation through a shared reference. It also lays the groundwork for future read-only slice types and borrow-checked slice access, where `&BStackSlice` and `&mut BStackSlice` could eventually carry stronger semantic guarantees.

### Design

Change the receiver of `write`, `write_range`, `zero`, `zero_range`, `writer`, and `writer_at` from `&self` to `&mut self`. Since `BStackSlice` is `Copy`, callers can always bind a local `mut` copy if needed — the change imposes no semantic restriction, only a mutability annotation.

The same change applies to the corresponding methods on `BStackGuardedSlice`, and to any future slice types such as `BStackVec`.

### Open questions

- Should `writer` and `writer_at` also require `&mut self`, given that they return a `BStackSliceWriter` rather than performing any mutation themselves? Requiring `&mut self` here is consistent with the general principle but may feel overly strict for a constructor that merely captures the slice.
- `BStackSliceWriter` is currently `Copy`. If `writer` requires `&mut self`, obtaining multiple writers from the same slice would require copying the slice first. Should `BStackSliceWriter` remain `Copy`, or should it be non-`Copy` to better reflect exclusive-write intent?
