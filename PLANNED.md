# Planned Features

This document outlines upcoming features planned for the `bstack` crate. These enhancements aim to improve usability, performance, and integration while maintaining the core principles of durability, crash-safety, and simplicity. Changes aim to be backward-compatible. New features are suggested to be added as optional features under feature flags and new traits, instead of modifying existing ones, to avoid breaking changes. All features aim to follow [Rust's API design guidelines](https://rust-lang.github.io/api-guidelines/) and BStack's design principles.

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
- Whether to require `From<BStackSlice<'a, Self>>` as an additional bound (allowing generic code to construct a handle from a plain slice via re-computation) is left open. It is not required for correctness and can be added later.

---

## `GuardedSlice` — lifecycle-hook slice abstraction

**Feature flag:** `guarded` (proposed)
**Breaking change:** No

### Motivation

Some use cases need to intercept slice I/O transparently — encryption at rest, integrity checksumming, access auditing, or automatic key rotation. Threading these concerns through each call site is invasive and error-prone. A hook-based abstraction lets the interception logic live in one place, with the full read/write/cursor API derived from it automatically. A trivial no-op implementation means raw `BStackSlice` access is available through the same API at zero cost.

### Design

```rust
pub trait GuardedSlice {
    /// Called before a read at `[offset, offset + len)` within the slice.
    /// Return `Err` to deny the read.
    fn pre_read(&self, offset: u64, len: usize) -> io::Result<()> { Ok(()) }

    /// Called with the raw bytes returned from the underlying store.
    /// Return `Cow::Borrowed` to pass data through without allocation;
    /// return `Cow::Owned` for decryption or other transformations.
    fn post_read<'d>(&self, data: &'d [u8]) -> io::Result<Cow<'d, [u8]>> {
        Ok(Cow::Borrowed(data))
    }

    /// Called with the data about to be written.
    /// Return `Cow::Borrowed` to pass data through without allocation;
    /// return `Cow::Owned` for encryption or other transformations.
    fn pre_write<'d>(&self, data: &'d [u8]) -> io::Result<Cow<'d, [u8]>> {
        Ok(Cow::Borrowed(data))
    }

    /// Called after a successful write at `[offset, offset + len)`.
    fn post_write(&self, offset: u64, len: usize) -> io::Result<()> { Ok(()) }

    // --- auto-derived from the hooks above ---

    fn read(&self) -> io::Result<Vec<u8>> { ... }
    fn read_into(&self, buf: &mut [u8]) -> io::Result<()> { ... }
    fn write(&self, data: impl AsRef<[u8]>) -> io::Result<()> { ... }
    fn write_range(&self, start: u64, data: impl AsRef<[u8]>) -> io::Result<()> { ... }
    fn zero(&self) -> io::Result<()> { ... }
    fn zero_range(&self, start: u64, n: u64) -> io::Result<()> { ... }
    fn reader(&self) -> GuardedSliceReader<'_, Self> { ... }
    fn writer(&self) -> GuardedSliceWriter<'_, Self> { ... }
}
```

`pre_write` and `post_read` use `Cow<'_, [u8]>` so that no-op implementations return `Cow::Borrowed` and incur no allocation. Transforming implementations return `Cow::Owned`. The write-side accepts `impl AsRef<[u8]>` (not `impl Deref<Target=[u8]>`) for the same compatibility reasons as the rest of the crate.

A trivial blanket wrapper over `BStackSlice` implements all four hooks as no-ops, making raw slice access available through the guarded API at zero overhead.

### Subview extension trait

```rust
pub trait GuardedSliceSubview: GuardedSlice {
    /// Narrow the view to `[start, end)` within this slice,
    /// preserving the full hook scope of the parent.
    fn subview(&self, start: u64, end: u64) -> impl GuardedSlice + '_;
}
```

Narrowing via `BStackSlice::subslice` is insufficient here because it discards the hook context; `subview` is required to keep the interception contract intact across range reductions.

### Open questions — must be resolved before implementation

**1. Cursor hook granularity.**
`GuardedSliceReader` and `GuardedSliceWriter` issue many small reads and writes. The hooks can fire either:
- *Per cursor call* — each `Read::read` or `Write::write` invocation triggers the hooks independently. Simple, but broken for block-cipher encryption where the hook needs to see a complete logical chunk.
- *Per slice lifetime* — hooks fire once when the cursor is constructed (pre) and once when it is dropped or explicitly flushed (post), buffering internally. Correct for block-cipher use cases but requires the cursor to hold an internal buffer.

The choice determines the entire cursor architecture and must be settled before any cursor code is written.

**2. Lock scope.**
The hooks can run either inside or outside the `RwLock` that guards the underlying file:
- *Inside the lock* — hook latency directly extends the lock hold time, affecting all concurrent readers and writers.
- *Outside the lock* — a TOCTOU gap exists between a hook decision (e.g., an access-control check in `pre_read`) and the actual I/O.

The correct choice depends on the intended hook semantics and must be documented as an explicit guarantee, not left ambiguous.

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
