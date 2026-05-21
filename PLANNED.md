# Planned Features

This document outlines upcoming features planned for the `bstack` crate. These enhancements aim to improve usability, performance, and integration while maintaining the core principles of durability, crash-safety, and simplicity. Changes aim to be backward-compatible. New features are suggested to be added as optional features under feature flags and new traits, instead of modifying existing ones, to avoid breaking changes. All features aim to follow [Rust's API design guidelines](https://rust-lang.github.io/api-guidelines/) and BStack's design principles.

---

## NOT PLANNED

### Deprecating `BStackGuardedSlice::as_slice` in favor of read-only access

Reasons:

`BStackGuardedSlice::as_slice` is not actually unsafe as data corruption through misuse doesn't violate memory safety or compromise allocator structure, so marking it unsafe would be misleading per Rust conventions. Documentation and API design already encourage using `read()` and `write()`. Callers using `as_slice()` are expected to understand the implications. In addition, the unsafe `raw_block()` method already exists for cases where hook bypass is needed, and its safety contract documents that hooks must be manually called. If this function is deprecated, it would break existing code, and callers who correctly use `as_slice()` for read-only purposes would need to migrate. Therefore, `as_slice()` as a safe function that returns a `BStackSlice` is sufficient, and the safety contract can be clearly documented without making it `unsafe fn`.

### Making `BStackAllocator::realloc` and `dealloc` `unsafe fn`

Reasons:

While `realloc` and `dealloc` have a slice-origin requirement, the hazard window is now significantly narrowed because constructing a bad handle already requires `unsafe` (via `BStackSlice::from_raw_parts`). A well-reviewed `unsafe` block that constructs a `BStackSlice::from_raw_parts` can be expected to read the safety contract and comply with the origin requirement. The remaining concern is sub-slices: `subslice` and `subslice_range` are *safe* functions that produce slices with an origin different from any allocator-returned handle. However, marking `realloc` and `dealloc` as `unsafe fn` would force all call sites into an `unsafe` block, which may be unnecessarily burdensome given the already narrow hazard window. The best conventions use `unsafe` for operations that can cause undefined behavior and overuse of `unsafe` can lead to desensitization and misuse. As of `BStack` 0.2, the allocator interfaces are already mature and breaking changes should be avoided. Since the origin requirement is a safety contract that can be documented and enforced through careful API design, it may be sufficient to keep `realloc` and `dealloc` as safe functions while clearly documenting the requirements and risks.

Furthermore, `alloc`, `realloc`, and `dealloc` in the `BStackAllocator` trait do not need to operate on `BStackSlice` directly — they operate on an associated handle type. Custom allocator implementations can define handle types that do not support sub-slicing at all, eliminating the origin problem at the type level for those allocators. The sub-slice concern is therefore a consequence of a specific design choice (using `BStackSlice` itself as the raw handle) rather than an inherent flaw in the trait. The recommended approach is for allocators to use a handle type distinct from `BStackSlice`, where converting from handle to `BStackSlice` is straightforward but the reverse is not possible — making the origin requirement a type-level guarantee. The default allocators in this crate currently do not follow this recommendation, but that is a separate concern addressed in the planned features below.

---

## Introducing a newtype handle for default allocators

**Breaking change:** Yes

### Motivation

The default allocators in this crate (e.g., `FirstFitBStackAllocator`) currently use `BStackSlice` as their raw handle type. Because `BStackSlice` exposes safe `subslice` and `subslice_range` methods, it is possible to produce a slice with an origin that does not correspond to any allocator-returned allocation and then pass it to `realloc` or `dealloc`, violating the origin requirement without any `unsafe` block at the call site. A dedicated newtype handle, constructible only by the allocator, closes this gap at the type level: it can be cheaply dereferenced to `BStackSlice` for reads and writes, but `BStackSlice` cannot be converted back into it, so sub-slices are statically prevented from being used as allocator handles.

### Design

Introduce a newtype (e.g., `BStackAllocatedSlice`) in each default allocator, wrapping `BStackSlice`. `BStackAllocatedSlice` would:

- Implement `Deref<Target = BStackSlice>` or `AsRef<BStackSlice>` for transparent access to slice operations.
- Implement `Into<BStackSlice>` for explicit, cheap conversion when a raw slice is needed.
- Not implement `From<BStackSlice>` — only the allocator internally constructs a `BStackAllocatedSlice`, ensuring origin is always valid.
- Be the associated handle type returned by `alloc` and accepted by `realloc` and `dealloc`.

This is a breaking change for any code that holds the allocator's associated handle type explicitly or passes a `BStackSlice` directly to `realloc` or `dealloc`. Code that only reads or writes through a handle without storing it by concrete type is largely unaffected. Custom allocator implementations built outside this crate are not affected, as the trait itself does not mandate a specific handle type.

### Open questions

- Should the new handle type be named `BStackAllocatedSlice`, or something more concise like `BStackHandle`? The former emphasizes the slice nature, while the latter is more general and concise. However, we also need to consider that there will be custom allocators that may want to define their own handle types, so a more generic name like `BStackHandle` might be too generic and could cause confusion if multiple handle types are in scope. 
- Should the new handle type have a unsafe method that allows backwards conversion to `BStackSlice` for advanced use cases, or should it be strictly one-way? Allowing an unsafe conversion could provide flexibility for advanced users who understand the risks, but it also introduces potential for misuse. If we choose to allow it, we would need to clearly document the safety contract and ensure that it is only used in scenarios where the caller can guarantee that the resulting `BStackSlice` is not misused as a sub-slice.

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

=======

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

## In-memory caching of the locked region

**Feature flag:** None
**Breaking change:** No (additive; opt-in)

### Motivation

Reads to the locked region already bypass the `RwLock`, but they still issue a `pread(2)` syscall per call. For workloads that read the locked region on every allocation or lookup — e.g., an allocator header or a hot metadata block — the syscall overhead dominates. Caching the locked region in memory would reduce reads to a plain slice copy with no kernel involvement.

### Design

When `lock_up_to(n)` is called (on a `BStack` opened with caching enabled), after publishing the new locked boundary the implementation reads the entire `[0, n)` region into a `Vec<u8>` and stores it in an `Option<Box<[u8]>>` field on `BStack`. Subsequent reads whose range falls entirely within the cached region are served by copying from that slice, with no syscall.

The cache is populated once per `lock_up_to` call. Because the locked region is immutable by definition, the cache never needs to be invalidated or written back.

`lock_up_to` is therefore significantly slower than today: it must read up to `n` bytes from disk into memory before returning. This is the central trade-off.

Consult the open questions below for design decisions that need to be made. Considering benchmarking the performance to determine whether this optimization is worthwhile, and if so, what the best design choices are.

### Open questions

- Should caching be opt-in at `open` time (e.g., `BStack::open_cached`) or toggled per `lock_up_to` call? This means it should be a constructor option, as the cache is tied to the locked region and cannot be easily enabled or disabled on the fly without significant complexity in managing cache state and consistency.
- Should the cache be a single contiguous `Box<[u8]>` (simplest) or a memory-mapped region (avoids the extra copy and lets the OS manage eviction)?
- How should extensions to the locked region (subsequent `lock_up_to` calls) update the cache — reallocate and copy, or maintain a growable `Vec<u8>`?
- Should the cached bytes be aligned to a cache line or page boundary for SIMD or `mmap` compatibility?
- Is the memory overhead acceptable for large locked regions? Should a size cap be enforced?

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
