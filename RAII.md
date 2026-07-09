# BStack RAII via Block Ownership

## Overview

This document describes the ownership, lifetime, and on-disk layout system for BStack blocks. It is a typed layer built on top of the mainline `alloc` module's primitives — [`BStackRange`], [`BStackSlice`], and [`BStackOwnedSlice`] — which already enforce single ownership of allocations at compile time but leave `Drop` as a no-op. This document's contribution is to decouple disk-level destruction (`BStackDrop`) from Rust's process-scoped `Drop`, allowing persistent storage semantics while still offering RAII conveniences when desired.

The design of this RAII semantics is inspired by C++ RAII with shared ownership — specifically `std::unique_ptr`, `std::shared_ptr`, and `std::weak_ptr`. It is adapted to persistent storage and Rust `Drop` semantics.

---

## Primitive Handles: `BStackRange`, `BStackSlice`, `BStackOwnedSlice`

The mainline `alloc` module now splits what used to be a single foundational handle into three tiers, each adding a capability the previous one lacks:

- **`BStackRange`** — a bare `(offset, len)` coordinate pair. `Copy`, and carries no backing reference at all — not even a `&BStack`. This is the serialization/persistence form: store it on disk, send it across sessions, or pass it through code that should not perform I/O. It carries no validity guarantee; casting it into a `BStackSlice` or `BStackOwnedSlice` is an `unsafe` operation the caller must justify.
- **`BStackSlice<'a>`** — a borrowed, non-owning I/O view bound to a `&'a BStack`. It is deliberately *not* `Copy` (so `&mut self` write methods provide genuine single-writer exclusivity in safe code) but is `Clone` for cases where an explicit second view is needed. `Drop` is a no-op: the region persists on disk beyond the handle's scope regardless.
- **`BStackOwnedSlice<'a, A: BStackAllocator>`** — the exclusive allocation handle, bound to `&'a A` (the allocator, not just the stack). It is neither `Copy` nor `Clone`: an allocation has exactly one owner, and the type system turns use-after-free / use-after-realloc into compile errors, since `realloc`/`dealloc` consume the handle by value and no copy survives to be misused. `Drop` is still a no-op — the allocation persists on disk until explicitly passed to `allocator.dealloc()`.

---

## Handle Type Hierarchy

Built on top of `BStackRange` / `BStackSlice` / `BStackOwnedSlice`:

| Type                         | Ownership                                         | Drop Behavior                       |
|------------------------------|---------------------------------------------------|-------------------------------------|
| `BStackRange`                | None (bare coordinates, no backing ref)           | No-op                               |
| `BStackSlice<'a>`            | None (borrow/I/O view)                            | No-op                               |
| `BStackOwnedSlice<'a, A>`    | Exclusive (allocator-bound)                       | No-op — explicit `dealloc` required |
| `BStackRef<T>`               | None (typed wrapper over `BStackRange`)           | No-op                               |
| `BStackOwned<T: BStackDrop>` | Exclusive (typed wrapper over `BStackOwnedSlice`) | Calls `T::bstack_drop`              |
| `BStackRc<T: BStackDrop>`    | Shared (refcounted)                               | Calls `T::bstack_drop` at zero      |
| `BStackWeak<T>`              | None (non-owning)                                 | No-op                               |

---

## On-Disk Layout

### Block Header

Every block on disk begins with a header:

```rust
#[repr(C, packed)]
struct BlockHeader {
    size: u64,
    type: EightCC,  // 8-byte type tag (not FourCC — offsets are 64-bit)
}
```

`EightCC` is an 8-byte type tag, chosen over the traditional `FourCC` because offsets in BStack are 64-bit (8 bytes), making 8-byte alignment natural and consistent.

### The `#[bstack_block]` Macro

Programmers write the ergonomic form:

```rust
#[bstack_block]
struct X {
    #[bstack_blockref]
    a: A,
    #[bstack_blockref]
    b: B,
    c: u32,
}
```

The macro generates a parallel on-disk representation:

```rust
#[repr(C, packed)]
struct XOnDisk {
    header: BlockHeader,
    a: BStackRef<A>,   // u64 offset into BStack
    b: BStackRef<B>,   // u64 offset into BStack
    c: u32,            // POD, stored inline
}
```

Fields must be either annotated `#[bstack_blockref]` (becomes a `BStackRef<T>` offset on disk) or plain POD (stored inline). The macro enforces this.

### `X` in Memory

`X` in memory is a typed wrapper over `BStackOwnedSlice<'a, A>` (when owned via `BStackOwned<X>`/`BStackRc<X>`) or `BStackSlice<'a>` (when merely borrowed) — not a deserialized struct. Field access is via generated methods that read from the on-disk payload, going through `as_slice()` for I/O and `allocator()` to resolve child `BStackRef`s (which, like `BStackRange`, carry no backing reference of their own):

```rust
impl X {
    pub fn a(&self) -> A {
        let on_disk: &XOnDisk = self.0.as_slice().cast();
        A(on_disk.a.resolve(self.0.allocator()))
    }

    pub fn c(&self) -> u32 {
        let on_disk: &XOnDisk = self.0.as_slice().cast();
        on_disk.c
    }
}
```

`XOnDisk` is what gets read when actual values are needed. `X` itself is just a handle.

---

## Disk-Level Destruction: `BStackDrop`

`BStackDrop` is a trait describing how to recursively free a block and all its owned children. It is completely decoupled from Rust's `Drop` and is not tied to any process or scope lifetime.

Because `BStackAllocator::dealloc` consumes an allocator-bound `BStackOwnedSlice<'a, A>` (not a bare `BStackSlice`) and returns a `Result`, `bstack_drop` must be generic over the allocator and propagate errors:

```rust
trait BStackDrop {
    fn bstack_drop<A: BStackAllocator>(handle: BStackOwnedSlice<'_, A>) -> Result<(), A::Error>;
}
```

The macro generates the implementation. Destruction is **post-order** — children are freed before the parent:

```rust
impl BStackDrop for X {
    fn bstack_drop<A: BStackAllocator>(handle: BStackOwnedSlice<'_, A>) -> Result<(), A::Error> {
        let allocator = handle.allocator();
        let on_disk: &XOnDisk = handle.as_slice().cast();
        A::bstack_drop(on_disk.a.resolve(allocator))?;
        B::bstack_drop(on_disk.b.resolve(allocator))?;
        allocator.dealloc(handle).map_err(|e| e.source)
    }
}
```

`BStackOwned<T>` ties this to a Rust scope by calling `T::bstack_drop` in its `Drop` impl. `BStackRc<T>` calls it when the refcount reaches zero.

---

## Refcounting: `BStackRc<T>`

Blocks that need shared ownership use `#[bstack_block(rc)]`, which injects a refcount field inline after the header:

```rust
#[repr(C, packed)]
struct XOnDisk {
    header: BlockHeader,
    refcount: AtomicU64,   // injected by (rc)
    a: BStackRef<A>,
    b: BStackRef<B>,
    c: u32,
}
```

Internally, `BStackRc<'a, T, A>` holds a `BStackRange` plus `&'a A`, mirroring `BStackOwnedSlice`'s allocator-bound shape rather than duplicating a live `BStackOwnedSlice` per clone (which can't be duplicated by design). `BStackRc<T>` behavior:
- `clone` — increments refcount atomically
- `drop` — decrements refcount; on reaching zero, reconstructs a `BStackOwnedSlice` via `unsafe { BStackOwnedSlice::from_raw_range(allocator, range) }` and calls `T::bstack_drop` on it

Refcount is stored inline in the block rather than in a separate allocation, avoiding an extra indirection on access. The cost is that every `(rc)` block carries 8 bytes of refcount overhead unconditionally.

**Constraint:** BStack files must not be opened by two processes simultaneously. On Windows this is OS-enforced. `AtomicU64` is therefore sufficient — inter-process atomics are not required.

---

## Weak References: `BStackWeak<T>`

`BStackWeak<T>` is a non-owning handle that does not participate in `BStackDrop`. It is used for back-pointers and cycles (e.g. doubly linked lists) where ownership must not be shared.

### Session-Scoped Weak Refs

When a weak ref is derived from an owned handle within the same session, Rust lifetimes enforce that it cannot outlive the owner:

```rust
impl<T> BStackOwned<T> {
    pub fn weak(&self) -> BStackWeak<T, '_> { ... }
}
```

### Cross-Session / On-Disk Weak Refs

Weak refs stored on disk as `BStackRef` fields cannot be statically checked. Safety is runtime only. Since `BStackRef<T>`, like `BStackRange`, carries no backing reference of its own, `upgrade` takes the allocator (or stack) explicitly to perform the validating read:

```rust
impl<T> BStackWeak<T> {
    pub fn upgrade<A: BStackAllocator>(&self, allocator: &A) -> Option<BStackRef<T>> {
        // Validates block header EightCC via allocator.stack() before returning
    }
}
```

The programmer is responsible for not calling `bstack_drop` on a block while live on-disk weak refs to it exist. This is an explicitly unsafe contract.

**Note:** Reference cycles (e.g. doubly linked lists) cannot use `BStackDrop` on both directions without infinite recursion. Back-pointers must always be `BStackWeak`, never owned refs.

---

## Open Questions

1. **`bstack_move`** — destructuring an owned block into its children. The correct order is: read child offsets from `XOnDisk`, transfer ownership of children, then dealloc the parent block. This must be made explicit to avoid use-after-free.

2. **`BStackWeak` validation** — `upgrade()` currently checks the EightCC tag. This guards against type confusion but is not a liveness guarantee if a new block has been allocated at the same offset. Whether this is sufficient depends on allocator reuse policy.

3. **`bstack_cast`** — safe casting between a typed handle (e.g. `X`) and its underlying primitive (`BStackSlice` for borrowed views, `BStackOwnedSlice` for owned handles) and back. Needs a defined API.