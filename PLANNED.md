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

## Adding `bstack_bulk_allocator_vtbl_t` to the C allocator API

**Feature flag:** `BSTACK_FEATURE_ATOMIC`
**Breaking change:** No (additive extension to `bstack_alloc.h`)

### Motivation

The Rust allocator layer exposes `BStackBulkAllocator` — an extension trait that adds `alloc_bulk` and `dealloc_bulk` for atomically allocating or freeing multiple regions in a single operation. The C allocator API (`bstack_allocator_vtbl_t`) has no equivalent; callers must issue individual alloc/dealloc calls, which is neither atomic nor efficient when managing several related regions.

This matters most for structured records that span multiple slices (e.g., a key slice + a value slice in a hash-map entry): allocating them one at a time creates a window where a crash leaves one slice allocated and the other not, complicating recovery.

### Design

Add a second vtable struct `bstack_bulk_allocator_vtbl_t` (available only with `-DBSTACK_FEATURE_ATOMIC`) alongside the existing vtable:

```c
typedef struct {
    /* Allocate n slices of the given lengths atomically.
     * out_slices must hold room for n bstack_slice_t values.
     * Returns 0 on success, -1 on failure (errno set). */
    int (*alloc_bulk)(void *self, const size_t *lens, size_t n,
                      bstack_slice_t *out_slices);

    /* Free n slices atomically.
     * slices must all originate from the same allocator instance.
     * Returns 0 on success, -1 on failure (errno set). */
    int (*dealloc_bulk)(void *self, const bstack_slice_t *slices, size_t n);
} bstack_bulk_allocator_vtbl_t;
```

Each concrete allocator (`linear_bstack_allocator_t`, `first_fit_bstack_allocator_t`, `ghost_tree_bstack_allocator_t`) would optionally implement this vtable; callers query via a `bstack_allocator_bulk_vtbl(bstack_allocator_t *)` accessor that returns NULL when the allocator does not support bulk operations.

### Open questions

- Should bulk operations be a separate vtable pointer hanging off `bstack_allocator_t`, or a distinct handle type (analogous to the Rust trait extension pattern)?
- Should `linear_bstack_allocator_t` support `dealloc_bulk`? Linear allocation inherently does not support dealloc of individual slices, so bulk dealloc is equally unsupported — it would return `ENOTSUP`.

---

## Adding a guard/intercept layer to the C allocator API

**Feature flag:** `BSTACK_FEATURE_SET` (for write hooks)
**Breaking change:** No (additive; new types and functions only)

### Motivation

The Rust allocator layer has `BStackGuardedSlice` and `BStackGuardedSliceSubview` — traits that let callers attach lifecycle hooks (`pre_read`, `post_read`, `pre_write`, `post_write`) to a slice, enabling transparent I/O transforms such as encryption, compression, or integrity checks. The C allocator has no equivalent; transforms must be wired by hand at every call site.

Without a guard layer, patterns like "all reads from this region must be decrypted and all writes must be re-encrypted" require every caller to remember to apply the transform, which is error-prone and couples application logic to storage details.

### Design

Add a `bstack_guarded_slice_t` wrapper type and a vtable for hooks:

```c
typedef struct {
    int (*pre_read) (void *ctx, bstack_slice_t s);
    int (*post_read)(void *ctx, bstack_slice_t s, uint8_t *buf, size_t len);
    int (*pre_write)(void *ctx, bstack_slice_t s,
                     const uint8_t **buf, size_t *len);
    int (*post_write)(void *ctx, bstack_slice_t s);
} bstack_guard_vtbl_t;

typedef struct {
    bstack_slice_t           slice;
    const bstack_guard_vtbl_t *vtbl;
    void                    *ctx;
} bstack_guarded_slice_t;
```

Provide `bstack_guarded_slice_read`, `bstack_guarded_slice_write`, and `bstack_guarded_slice_subview` functions that delegate to the underlying slice while invoking the hooks at the appropriate points.

The subview analogue (`BStackGuardedSliceSubview`) would be expressed as a flag or second vtable on `bstack_guarded_slice_t` that additionally intercepts `subslice` calls, allowing a guard to restrict or transform the visible range.

### Open questions

- Should `pre_write` be allowed to replace the buffer pointer (e.g., to encrypt into a separate scratch buffer), or should it always mutate in place? Replacing the pointer is more flexible but requires the hook to manage scratch memory lifetime.
- How should hook errors interact with the C error-reporting convention (`errno`)? The hook returning -1 should propagate as a read/write failure, but the hook may also want to set a custom errno value.
- Should `bstack_guarded_slice_t` be opaque (allocated on the heap) or transparent (stack-allocatable struct)? Transparency is simpler but leaks the vtable layout.

