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

---

## GhostTreeBstackAllocator

**Breaking change:** No

### Motivation

Intended to be a less robust but simpler "poor man's" allocator for users who want a general-purpose allocator but with no block headers on live block and O(log n) allocation and deallocation time. It is a pure AVL tree with no coalescing, so free blocks are never merged and fragmentation can occur. It is not intended for serious production use but serves as a useful reference implementation. Since it contain no metadata, it is quite compact. Due to its zeroing on free and compactness, the underlying memory is easier to inspect and debug, making it a good allocator for testing.

### Design

Allocator identifier: **ALGT**

#### Overview

A general-purpose memory allocator built on top of a BStack (a growable stack-like backing store). The allocator manages free memory using an AVL tree keyed on (size, address) for a total order. All memory is zeroed either at BStack extension time (via BStack's extend API) or at deallocation time, so allocated slices are always clean. The allocator exposes a slice-based interface — all operations accept and return (pointer, length) pairs. Callers are required by contract to only pass back slices originally returned by alloc or realloc.

```
BStack Layout
┌─────────────────────────────┐  offset 0
│   User-reserved (32 bytes)  │
├─────────────────────────────┤  offset 32
│   Magic number              │  identifies a valid allocator BStack
├─────────────────────────────┤  offset 40
│   AVL root pointer          │  pointer to root of free block tree
├─────────────────────────────┤  offset 48
│   (allocator data ends)     │
│   ... heap grows upward ... │
└─────────────────────────────┘
```

The first 32 bytes are reserved for user-defined purposes and never touched by the allocator. The magic number at offset 32 identifies this BStack as a valid allocator instance. The AVL root pointer at offset 40 is the sole allocator bookkeeping entry.

#### Alignment

All allocations are aligned to 32 bytes. The BStack itself has a 16-byte header, so allocator-managed memory begins at offset 48, which is 32-byte aligned on the underlying storage (32n + 16 in BStack address space = 32n + 16 + 16 = 32(n+1) on disk).

The minimum allocation size is 32 bytes, which is exactly the size of one AVL node. There is no crumb bin — all allocations go through the AVL tree.

Free Block Structure
Free blocks store their AVL node inline, requiring no separate metadata for live allocations:

```
┌──────────────────┐  +0
│  size (u64)      │  size of this free block in bytes
├──────────────────┤  +8
│  balance factor  │  i8, packed with 7 bytes padding (or into pointer low bits)
├──────────────────┤  +16
│  left child ptr  │  points to free block with smaller (size, address)
├──────────────────┤  +24
│  right child ptr │  points to free block with larger (size, address)
└──────────────────┘  +32
Live allocations have zero overhead — no headers, no footers, no metadata whatsoever. The only record of what is free is the AVL tree.
```

#### AVL Tree Ordering

The tree is keyed on the compound key (size, address):

Primary: block size ascending
Secondary: block address ascending (tiebreaker for equal-sized blocks)
This gives a strict total order, making insertion and removal unambiguous even for multiple free blocks of identical size.

#### Allocation

- Traverse the AVL tree to find the smallest block ≥ requested size (best-fit).
- If no block fits, extend the BStack to obtain a fresh zeroed block, and use that.
- Otherwise, remove the chosen block from the AVL tree. Let the block be n bytes and the request m bytes (rounded up to 32-byte alignment).
- If n - m >= 32, split: write an AVL node into the leading n - m bytes and insert it into the tree. The trailing m bytes are returned to the caller. Why choosing the tail for the returned slice? Because it removes the need for a zeroing operation.
- If n - m < 32, the entire block is given to the caller. The few extra bytes are included in the returned slice transparently. A zero operation on the existing header is needed.
- Return slice (ptr, m) pointing to the tail portion of the chosen block. Memory is already zeroed by invariant.
- The allocated region is always taken from the tail of the chosen free block so that the AVL node for the remainder can be written at the original block address without moving data.

#### Deallocation

Given slice (ptr, len):

- Zero the entire region (BStack zeroing invariant).
- Write an AVL node into the first 32 bytes of the region, with size = len. This can be combined with the previous step so only one full block write is needed.
- Insert into the AVL tree by (len, ptr).
- No coalescing is done eagerly. Adjacent free blocks may accumulate and are only merged on startup.

#### Reallocation

**Shrink (new_size < old_size)**:

- If the freed tail (old_size - new_size) >= 32, write an AVL node, insert into tree.
- If the freed tail < 32, those bytes are absorbed into the returned slice (the slice shrinks logically but the extra bytes remain in the caller's allocation — document this).
- Return (ptr, new_size).

**Grow (new_size > old_size)**:

- alloc(new_size)
- copy contents
- free(ptr, old_size).
- Return new slice.

#### Startup Coalescing (Crash Recovery)

Called when mounting an existing BStack. The AVL tree may be imbalanced or missing entries due to a prior crash — both are tolerated. Lost free blocks are simply unrecoverable and their memory is permanently unavailable until the next new (fresh initialization).

Procedure:

- Walk the AVL tree (in-order traversal, tolerating imbalance) to enumerate all known free blocks by address.
- Sort free blocks by address.
- Scan for adjacent pairs: two blocks at addresses a and b where a + size(a) == b.
- When found: remove both from the AVL tree, write a merged AVL node of size size(a) + size(b) at address a, insert merged block. It's possible that this insertion does no rebalance since we also want to recover from imbalance, so there will be a special full rebalance pass at the end to fix any remaining imbalance.
- Repeat until no adjacent pairs remain.
- Rebalance the AVL tree (standard rebalance pass).
- The result is a best-effort recovered state — free memory that was correctly recorded survives; anything lost to crash is gone.

#### Crash Safety Contract

| Event                                 | Effect                                                        |
|---------------------------------------|---------------------------------------------------------------|
| Crash during AVL rotation	            | Tree may be imbalanced; corrected on next mount               |
| Crash during free (before AVL insert)	| That block is lost permanently                                |
| Crash during alloc (after AVL remove)	| Block may be double-lost; allocation fails cleanly on retry   |
| Crash during BStack extend	        | Extension is lost; BStack remains at prior size               |

The allocator makes no write-ahead log, no shadow copy, no checksum guarantees. It is suitable for use cases where occasional free block loss is acceptable and the magic number provides sufficient integrity signal.

