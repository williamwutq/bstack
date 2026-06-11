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

### Adding a singly-linked `BStackSList<T>`

Reasons:

A singly-linked variant saves 8 bytes per node by omitting `prev_ptr`, but any workload that reaches for a disk-backed linked list is already paying the cost of allocator round-trips and random-access I/O per element. An 8-byte savings per node is negligible against those overheads, and the restriction to forward-only traversal eliminates `push_front`-with-O(1)-`pop_back`, bidirectional cursors, and `split_off` without a full scan. `BStackList<T>` covers all singly-linked use cases with no meaningful extra cost, so a separate singly-linked type adds complexity without benefit.

### Making `BStackAllocator::realloc` and `dealloc` `unsafe fn`

Reasons:

While `realloc` and `dealloc` have a slice-origin requirement, the hazard window is now significantly narrowed because constructing a bad handle already requires `unsafe` (via `BStackSlice::from_raw_parts`). A well-reviewed `unsafe` block that constructs a `BStackSlice::from_raw_parts` can be expected to read the safety contract and comply with the origin requirement. The remaining concern is sub-slices: `subslice` and `subslice_range` are *safe* functions that produce slices with an origin different from any allocator-returned handle. However, marking `realloc` and `dealloc` as `unsafe fn` would force all call sites into an `unsafe` block, which may be unnecessarily burdensome given the already narrow hazard window. The best conventions use `unsafe` for operations that can cause undefined behavior and overuse of `unsafe` can lead to desensitization and misuse. As of `BStack` 0.2, the allocator interfaces are already mature and breaking changes should be avoided. Since the origin requirement is a safety contract that can be documented and enforced through careful API design, it may be sufficient to keep `realloc` and `dealloc` as safe functions while clearly documenting the requirements and risks.

Furthermore, `alloc`, `realloc`, and `dealloc` in the `BStackAllocator` trait do not need to operate on `BStackSlice` directly — they operate on an associated handle type. Custom allocator implementations can define handle types that do not support sub-slicing at all, eliminating the origin problem at the type level for those allocators. The sub-slice concern is therefore a consequence of a specific design choice (using `BStackSlice` itself as the raw handle) rather than an inherent flaw in the trait. The recommended approach is for allocators to use a handle type distinct from `BStackSlice`, where converting from handle to `BStackSlice` is straightforward but the reverse is not possible — making the origin requirement a type-level guarantee. The default allocators in this crate currently do not follow this recommendation, but that is a separate concern addressed in the planned features below.

### Adding `BStackVec<T>` for typed vector storage

Reasons:

A typed vector is a data structure, not an I/O mechanism, and `bstack`'s role is to abstract crash-safe atomic file I/O — the stack, allocators, and slices — not to provide collection types. Such structures compose on top of those mechanisms and belong downstream. `BStackByteVec` already covers the byte-buffer case, and end push/pop is already crash-atomic via a single `clen` write inherited from the stack, so a generic `BStackVec<T>` would mainly add a `bytemuck`/`zerocopy` dependency and POD-soundness surface for a generalization downstream consumers can build themselves. Keeping it out preserves the lean core and avoids freezing API surface ahead of 1.0.

### Adding `BStackList<T>` for typed doubly-linked list storage

Reasons:

As with `BStackVec<T>`, a linked list is a data-structure policy that belongs downstream rather than in the I/O core. The downstream `bllist` crate already provides on-disk doubly-linked lists built on `bstack`, with a no-corruption, recoverable-leak model: every block write is atomic, so a crash degrades to an orphaned node reclaimed on reopen rather than data corruption. The only capability that would justify a first-party type is crash-atomic multi-block structural mutation (`append`, `split_off`), which depends on the write-in-progress journaling primitive planned for 0.5.0 — and once that primitive is public, downstream crates can consume it to achieve the same guarantee. A `bstack`-native list would therefore never hold an exclusive capability. `bstack` should instead ship the mechanisms such structures need (allocators and, later, the public transaction primitive) and leave the structures themselves to downstream.

---

## Introducing a newtype handle for default allocators (0.4.0)

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

## Truly crash-atomic `set` via write-in-progress journaling (0.4.0)

**Breaking change:** Yes (on-disk format / ABI)

### Motivation

`set` is not crash-atomic today. The visible stack `[0..clen]` is updated in place under a single `clen` field, so a crash mid-write can leave the stack with a region of partially-old, partially-new bytes. The 16 B header (`magic[8] | clen[8]`) is enough to validate length but carries no record that a write is in progress, so recovery on open cannot detect or undo a torn in-place write.

For `set` to be truly atomic — either the old value or the new value, never an interleaving — the header must encode an in-progress write region, and the bytes on disk must be laid out so that recovery can deterministically finish or roll back whatever was in flight.

### Design

#### Header layout

The header grows from 16 B to 32 B:

```
[ magic[8] | clen[8 LE] | wip_ptr[8 LE] | wip_aux[8 LE] ]
```

Magic bumps to a new 0.4.0 value so old binaries fail loudly on a new file rather than misinterpret the longer header as payload. `wip_ptr == 0` is the steady state ("no write in progress"); a non-zero `wip_ptr` names the start of the in-progress write slice. The slice length is **not** stored — it is reconstructed at recovery time as `file_size − clen`, which is exact by construction (see algorithm below).

`wip_aux` is reserved: in 0.4.0 it is always zero. A zero value signals "this is the same-length atomic-`set` case, recover by the inferred-length rule". A non-zero value is reserved for future splice (different-length `set`) operations, where it will encode whatever extra metadata splice needs — at minimum a direction flag, possibly a packed direction + target-clen tuple. Because `wip_aux` is already present in the 0.4.0 header, splice can be added later without another ABI bump.

Only `set` (and, eventually, splice) ever arms `wip` — `push` and `pop` remain crash-atomic under a single `clen` write and leave `wip_ptr == 0` throughout. The at-rest invariant is `file_size == clen`; during a `set` the file grows to `clen + n` to hold a tail backup of the new bytes.

#### `set` for the same-length case

Replacing `[a .. a+n]` with new bytes `dn`:

1. **Stage.** Extend the file to `clen + n`, write `dn` into the tail `[clen .. clen + n]`. Sync.
2. **Arm.** Write `wip_ptr = a`. Sync.
3. **Commit in place.** Copy `[clen .. clen + n]` into `[a .. a+n]`. Sync.
4. **Disarm.** Write `wip_ptr = 0`. Sync.
5. **Clean up.** Truncate file back to `clen`.

`wip_aux` stays zero throughout; the slice length is implied by `file_size − clen`. Arming is therefore a single 8 B header write, with no ordering subtlety between two fields — `wip_ptr` is the only armed bit.

#### Recovery on open

Recovery runs once during construction, while the write lock is held, before the `BStack` is exposed. It reads only the header and the file size:

- **`wip_ptr == 0`** — no active operation. Truncate to `clen` (drops any stale tail from a crashed step 1). Done.
- **`wip_ptr != 0`** and **`wip_aux == 0`** — a same-length `set` was in progress. The staged tail is at `[clen .. file_size)`, length `n = file_size − clen`. Copy it into `[wip_ptr .. wip_ptr + n)`, truncate to `clen`, clear `wip_ptr`. The new value is committed.
- **`wip_ptr != 0`** and **`wip_aux != 0`** — a splice operation. Handling is deferred; see open questions.

Every intermediate on-disk state of the algorithm is recoverable to either the old or the new value, never an interleaving. Crashes in step 1 leave `wip_ptr == 0` (rollback by truncate). Crashes in step 2 are either `wip_ptr == 0` (rollback) or `wip_ptr == a` (roll forward via the staged tail). Crashes in step 3 roll forward; the recovery copy is idempotent over any partial in-place write. Crashes in step 4 are either roll forward (one more idempotent copy) or `wip_ptr == 0` (the new value is already in place; just truncate).

Recovery must run to completion **before** the locked-region cache (#4) is populated, otherwise the cache could snapshot mid-rollback bytes. Any `set` that touches the locked region must also invalidate or refresh the cache atomically with the disk-level commit. This is a hard requirement of the journaling protocol, not an open question.

#### Migration from 0.1.0 files

Open old-magic files in a compatibility mode that rewrites them to the 0.4.0 layout: write the new header and payload into a sibling file, then `rename` into place (atomic within a filesystem on POSIX). One-shot per file; cost is proportional to file size. Document this in the changelog so users with large files aren't surprised.

#### Durability barriers

Crash-safety depends on three real barriers, in order: stage→arm, arm→in-place, in-place→disarm. Without any of these, recovery can observe a header that disagrees with on-disk content. Each "sync" step above is the existing `durable_sync` primitive (already `F_FULLFSYNC` on macOS with fdatasync fallback, `fsync` elsewhere); no new platform handling is introduced — the journaling protocol only adds barriers at the three transitions above.

### Open questions

- **Splice (different-length `set`) is deferred.** Replacing `[a..t]` of length `n_old` with bytes of length `n_new ≠ n_old` changes both slice contents and stack length. The current recovery rule in this document (length implied by `file_size − clen`) cannot distinguish the shrink and grow cases — the bytes immediately after the wip range carry opposite meanings (old content to discard vs new content to keep). Splice will use `wip_aux` to encode the metadata needed to disambiguate: at minimum a direction bit, possibly a packed direction + target-`clen` tuple. No header growth, no further ABI bump. The exact encoding inside `wip_aux`, the staging sequence for each direction should be considered before this is actually implemented.

---

## Requiring `&mut BStackSlice` for mutation (0.4.0)

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

---

## Typed region and I/O parameter types

**Feature flag:** None (additive API surface)
**Breaking change:** No

### Motivation

Many BStack and `BStackSlice` APIs share recurring parameter patterns that are passed as raw function arguments today:

- `(offset: u64, buf: impl AsRef<[u8]>)` — a write region: a position and the data to write there.
- `(offset: u64, buf: &mut [u8])` — a read region: a position and a buffer to fill; the length is implied by `buf.len()`.
- `(offset: u64, len: u64)` — a named range: a position and a byte count, with no associated data.
- `(a: u64, b: u64, len: u64)` — a cross-region pair: two positions and a shared length, as in `cross_exchange`.

These patterns appear in `set`, `get`, `get_range`, `zero`, `zero_range`, `cross_exchange`, `BStackOp::Read`, `BStackOp::Write`, and the generator closures for `get_batched_gen` and `process_gen`. Callers must remember the argument order and meaning at every call site. There is no type-level distinction between "an offset into this stack" and "a length", both of which are `u64`.

Introducing named types for these patterns — for example, `ReadRegion`, `WriteRegion`, `ByteRange`, and `CrossRegion` — would make call sites self-documenting and allow the compiler to reject transposed arguments.

### Design (sketch)

The types would be lightweight, `Copy` structs:

```rust
pub struct ByteRange       (Range<u64>);
pub struct ReadRegion<'a>  { pub offset: u64, pub buf: &'a mut [u8] }
pub struct WriteRegion<'a> { pub offset: u64, pub data: &'a [u8] }
pub struct CrossRegion     { pub src: u64, pub dst: u64, pub len: u64 }
```

Existing methods would gain overloaded entry points accepting these types via `Into` or `From`, or new companion methods alongside the originals. `BStackOp` variants could also be restructured around them. No existing call site would break.

### Open questions

- **Is the benefit real?** The patterns already have named parameters in Rust function signatures (`offset`, `buf`, `len`), so transposition is not silent — the compiler infers types, and `buf: impl AsRef<[u8]>` versus `len: u64` are already distinct. The main gain is readability at call sites, which is a matter of taste.
- **Proliferation cost.** Four new public types add documentation surface, appear in error messages, and must be maintained indefinitely. If the API grows, more pattern types may follow.
- **Naming.** The names above are illustrative. Alternatives: `Span`, `Slice` (conflicts with `BStackSlice`), `Region`, `Segment`. The right name should not collide with existing types and should signal that these are I/O coordinates, not data containers.
