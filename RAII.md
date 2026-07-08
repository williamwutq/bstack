# BStack RAII via Block Ownership

## Overview

This document describes the ownership, lifetime, and on-disk layout system for BStack blocks. This is a competing concept to the mainline BStack slice ownership model in the current alloc module, which is manually managed. The design decouples disk-level destruction (`BStackDrop`) from Rust's process-scoped `Drop`, allowing persistent storage semantics while still offering RAII conveniences when desired.

The design of this RAII semantics is inspired by C++ RAII with shared ownership — specifically `std::unique_ptr`, `std::shared_ptr`, and `std::weak_ptr`. It is adapted to persistent storage and Rust `Drop` semantics.

---

## Primitive Handle: `BStackSlice`

`BStackSlice` is the foundational handle. It is a lightweight `Copy` value carrying:

- A reference to the allocator
- A byte offset into the BStack file
- A length

`Drop` on `BStackSlice` does nothing. Deallocation is explicit — the block is freed only by passing the slice to `allocator.dealloc()`. This means `BStackSlice` is safe to copy and pass around without triggering any side effects.

---

## Handle Type Hierarchy

Built on top of `BStackSlice`:

| Type | Ownership | Drop Behavior |
|---|---|---|
| `BStackSlice` | None | No-op |
| `BStackRef<T>` | None (borrow/navigation) | No-op |
| `BStackOwned<T: BStackDrop>` | Exclusive | Calls `T::bstack_drop` |
| `BStackRc<T: BStackDrop>` | Shared (refcounted) | Decrements refcount; calls `T::bstack_drop` at zero |
| `BStackWeak<T>` | None (non-owning) | No-op |

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

`X` in memory is a typed wrapper over `BStackSlice` — not a deserialized struct. Field access is via generated methods that read from the on-disk payload:

```rust
impl X {
    pub fn a(&self) -> A {
        let on_disk: &XOnDisk = self.0.cast();
        A(on_disk.a.resolve(self.0.allocator))
    }

    pub fn c(&self) -> u32 {
        let on_disk: &XOnDisk = self.0.cast();
        on_disk.c
    }
}
```

`XOnDisk` is what gets read when actual values are needed. `X` itself is just a handle.

---

## Disk-Level Destruction: `BStackDrop`

`BStackDrop` is a trait describing how to recursively free a block and all its owned children. It is completely decoupled from Rust's `Drop` and is not tied to any process or scope lifetime.

```rust
trait BStackDrop {
    fn bstack_drop(slice: BStackSlice);
}
```

The macro generates the implementation. Destruction is **post-order** — children are freed before the parent:

```rust
impl BStackDrop for X {
    fn bstack_drop(slice: BStackSlice) {
        let on_disk: &XOnDisk = slice.cast();
        A::bstack_drop(on_disk.a.resolve(slice.allocator));
        B::bstack_drop(on_disk.b.resolve(slice.allocator));
        slice.allocator.dealloc(slice);
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

`BStackRc<T>` behavior:
- `clone` — increments refcount atomically
- `drop` — decrements refcount; calls `T::bstack_drop` when it reaches zero

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

Weak refs stored on disk as `BStackRef` fields cannot be statically checked. Safety is runtime only:

```rust
impl<T> BStackWeak<T> {
    pub fn upgrade(&self) -> Option<BStackRef<T>> {
        // Validates block header EightCC before returning
    }
}
```

The programmer is responsible for not calling `bstack_drop` on a block while live on-disk weak refs to it exist. This is an explicitly unsafe contract.

**Note:** Reference cycles (e.g. doubly linked lists) cannot use `BStackDrop` on both directions without infinite recursion. Back-pointers must always be `BStackWeak`, never owned refs.

---

## Open Questions

1. **`bstack_move`** — destructuring an owned block into its children. The correct order is: read child offsets from `XOnDisk`, transfer ownership of children, then dealloc the parent block. This must be made explicit to avoid use-after-free.

2. **`BStackWeak` validation** — `upgrade()` currently checks the EightCC tag. This guards against type confusion but is not a liveness guarantee if a new block has been allocated at the same offset. Whether this is sufficient depends on allocator reuse policy.

3. **`bstack_cast`** — safe casting between a typed handle (e.g. `X`) and `BStackSlice` and back. Needs a defined API.