# BStack RAII via Block Ownership

## Feature Flag: `raii` (depends on `alloc` + `set`)

## Overview

This document describes the ownership, lifetime, and on-disk layout system for BStack blocks. It is a typed layer built on top of the mainline `alloc` module's primitives: [`BStackRange`], [`BStackSlice`], and [`BStackOwnedSlice`]. These primitives already enforce single ownership of allocations at compile time. This document decouples disk-level destruction (`BStackDrop`) from Rust's process-scoped `Drop`. That allows persistent storage semantics while still offering RAII conveniences when desired.

The design of this RAII semantics is inspired by C++ RAII with shared ownership: `std::unique_ptr`, `std::shared_ptr`, and `std::weak_ptr`. It is adapted to persistent storage and Rust `Drop` semantics.

---

## Primitive Handles: `BStackRange`, `BStackSlice`, `BStackOwnedSlice`

The mainline `alloc` module splits what used to be a single foundational handle into three tiers. Each tier adds a capability the previous one lacks.

- **`BStackRange`**: a bare `(offset, len)` coordinate pair. It is `Copy` and carries no backing reference at all, not even a `&BStack`. This is the serialization form. Store it on disk, send it across sessions, or pass it through code that should not perform I/O. It carries no validity guarantee. Casting it into a `BStackSlice` or `BStackOwnedSlice` is `unsafe`, and the caller must justify it.
- **`BStackSlice<'a>`**: a borrowed, non-owning I/O view bound to a `&'a BStack`. It is deliberately not `Copy`, so `&mut self` write methods provide genuine single-writer exclusivity in safe code. It is `Clone` for cases where an explicit second view is needed. `Drop` is a no-op. The region persists on disk beyond the handle's scope regardless.
- **`BStackOwnedSlice<'a, A: BStackAllocator>`**: the exclusive allocation handle, bound to `&'a A`, the allocator rather than just the stack. It is neither `Copy` nor `Clone`. An allocation has exactly one owner. `realloc`/`dealloc` consume the handle by value, so no copy survives to be misused, and the type system turns use-after-free or use-after-realloc into compile errors. `Drop` is still a no-op. The allocation persists on disk until it is explicitly passed to `allocator.dealloc()`.

---

## Handle Type Hierarchy

Built on top of `BStackRange` / `BStackSlice` / `BStackOwnedSlice`:

| Type                         | Ownership                                         | Drop Behavior                       |
|------------------------------|---------------------------------------------------|--------------------------------------|
| `BStackRange`                | None (bare coordinates, no backing ref)           | No-op                               |
| `BStackSlice<'a>`            | None (borrow/I/O view)                            | No-op                               |
| `BStackOwnedSlice<'a, A>`    | Exclusive (allocator-bound)                       | No-op, explicit `dealloc` required  |
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
    type: EightCC,  // 8-byte type tag
}
```

`EightCC` is an 8-byte type tag. It is used instead of the traditional `FourCC` because offsets in BStack are 64-bit, so 8-byte alignment is natural and consistent.

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

Fields must be either annotated `#[bstack_blockref]`, which becomes a `BStackRef<T>` offset on disk, or plain POD, stored inline. The macro enforces this.

### `X` in Memory

`X` in memory is a typed wrapper. It wraps `BStackOwnedSlice<'a, A>` when owned via `BStackOwned<X>`/`BStackRc<X>`, or `BStackSlice<'a>` when merely borrowed. It is not a deserialized struct. Field access goes through generated methods that read from the on-disk payload. These methods call `as_slice()` for I/O and `allocator()` to resolve child `BStackRef`s. Like `BStackRange`, a `BStackRef` carries no backing reference of its own. For example:

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

`BStackAllocator::dealloc` consumes an allocator-bound `BStackOwnedSlice<'a, A>`, not a bare `BStackSlice`, and it returns a `Result`. So `bstack_drop` must be generic over the allocator and must propagate errors:

```rust
trait BStackDrop {
    fn bstack_drop<A: BStackAllocator>(handle: BStackOwnedSlice<'_, A>) -> Result<(), A::Error>;
}
```

The macro generates the implementation. Destruction is **post-order**:

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

Some blocks need shared ownership but no differentiation between strong and weak references. These use `#[bstack_block(rc)]`, which injects a refcount field inline after the header:

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

`BStackRc<'a, T, A>` holds a `BStackRange` plus `&'a A`. This mirrors `BStackOwnedSlice`'s allocator-bound shape instead of duplicating a live `BStackOwnedSlice` per clone, which can't be duplicated by design. `BStackRc<T>` behaves as follows.

- `clone`: increments refcount atomically.
- `drop`: decrements refcount. On reaching zero, it reconstructs a `BStackOwnedSlice` via `unsafe { BStackOwnedSlice::from_raw_range(allocator, range) }` and calls `T::bstack_drop` on it.

Refcount is stored inline in the block rather than in a separate allocation. This avoids an extra indirection on access. The cost is that every `(rc)` block carries 8 bytes of refcount overhead unconditionally.

Plain `#[bstack_block(rc)]` has no weak count and no control block. So `BStackRc<T>` for such a block does not expose `downgrade`/`weak` at all. Blocks that expect real `BStackWeak<T>` support must opt in with `#[bstack_block(rc, weak)]`.

---

## Control Block: Strong/Weak Split

Checking only the `EightCC` tag in `upgrade()` is not a liveness guarantee. If the block is deallocated, the allocator may later hand the same offset to a new unrelated allocation of the same type. Then the tag check passes on stale data. Blocks that expect weak references need a control block that governs liveness indirectly, the same way `std::sync::Arc`/`Weak` do.

Injecting `strong`/`weak` counters directly into the data block and deferring its `dealloc()` until both hit zero would work. But the block's entire storage scales with its actual fields, and that storage would stay pinned for as long as any `BStackWeak<T>` survives. Instead, the control block is kept as a separate allocation.

### On-Disk Layout

`#[bstack_block(rc, weak)]` splits the block into two independent on-disk allocations instead of one. Both counts live in the control block:

```rust
// The data block. It carries no counters, only a back-pointer to its
// control block, so it stays discoverable even without a live BStackRc<X>
// (e.g. reached via a child field on another block, or during crash recovery).
#[repr(C, packed)]
struct XOnDisk {
    header: BlockHeader,
    ctrl: BStackRef<XOnDiskRef>,   // back-pointer to the control block
    a: BStackRef<A>,
    b: BStackRef<B>,
    c: u32,
}

// The control block. It is the sole owner of both counts, so exactly one
// allocation governs liveness for `upgrade()`.
#[repr(C, packed)]
struct XOnDiskRef {
    header: BlockHeader,
    strong: AtomicU64,   // moved here, off the data block
    weak: AtomicU64,     // starts at 1 (phantom, see below)
    x: BStackRef<X>,     // forward-pointer to the data block
}
```

`weak` starts at `1`, not `0`. This phantom weak reference represents the claim that all live `BStackRc<T>` handles collectively hold on the control block. It is released once `strong` drops to zero.

The `ctrl` back-pointer costs the data block one field, 8 bytes. It makes the data block and its control block mutually locatable without requiring an in-memory `BStackRc<T>` to already hold both. Storing this back-pointer costs the same as storing the refcount in an ordinary `(rc)` block.

### In-Memory Shape

`BStackRc<T>` for an `(rc, weak)` block carries two references in memory. `x: BStackRef<X>` is for direct field access. `ctrl: BStackRef<XOnDiskRef>` is for refcount operations and for finalizing the control block on drop:

```rust
struct BStackRc<'a, T, A> {
    x: BStackRef<T>,
    ctrl: BStackRef<XOnDiskRef>,   // only present for (rc, weak) blocks
    allocator: &'a A,
}
```

`.clone()` and every non-final `.drop()` touch `ctrl.strong`. This is a single field read or write on an already-resolved reference, since `BStackRc<T>` already carries `ctrl` in memory. `.downgrade()` clones `ctrl` and increments `ctrl.weak`. The result is a `BStackWeak<T>` that just wraps `ctrl`.

`BStackWeak<T>` is `Clone`, the same way `BStackRc<T>` is. Cloning a `BStackWeak<T>` increments `ctrl.weak` directly, with no need to go through a live `BStackRc<T>`. So a `BStackWeak<T>` comes from either `BStackRc<T>::downgrade()` or `BStackWeak<T>::clone()`. This mirrors how `std::sync::Weak` can come from either `Arc::downgrade` or `Weak::clone`.

### Type-Level Gate

`downgrade`/`weak` exist only on `BStackRc<T>` for blocks declared `#[bstack_block(rc, weak)]`. This is enforced by a marker trait, not documentation:

```rust
/// Implemented only for blocks declared `#[bstack_block(rc, weak)]`.
/// Its presence is what lets the generated `BStackRc<T>` carry a `ctrl`
/// field and expose `downgrade`.
trait BStackWeakable: BStackDrop {
    type Control;   // = XOnDiskRef, generated alongside XOnDisk
}

impl<'a, T: BStackWeakable, A: BStackAllocator> BStackRc<'a, T, A> {
    pub fn downgrade(&self) -> BStackWeak<T> { ... }
}
```

A plain `#[bstack_block(rc)]` block's generated type does not implement `BStackWeakable`, since it has no `XOnDiskRef`. Its `BStackRc<T>` correspondingly has no `ctrl` field. Calling `.downgrade()` on it produces `error[E0599]: no method named 'downgrade'`. That is a compile error, not a runtime hazard left to programmer discipline.

### Two-Phase Teardown

Destruction happens in two independent phases, across two independent allocations.

1. **Strong count reaches zero.** This happens when the last `BStackRc<T>` is dropped, decrementing `ctrl.strong`. Follow `ctrl.x` to `XOnDisk`, free its children via the usual post-order walk, then call `allocator.dealloc()` on `XOnDisk` itself. Then release the phantom weak by decrementing `ctrl.weak`.
2. **Weak count reaches zero.** This happens when the last `BStackWeak<T>` is dropped, or when the phantom release above brings it to zero because no weak ref was ever taken. Call `allocator.dealloc()` on the control block, `XOnDiskRef`, itself.

`XOnDisk`'s storage is reclaimed the moment the last strong owner drops. This holds no matter how many `BStackWeak<T>` handles are still outstanding. Only the small, fixed-size control block remains pinned until the last weak handle is dropped.

### `upgrade()` Revisited

```rust
impl<T> BStackWeak<T> {
    pub fn upgrade<A: BStackAllocator>(&self, allocator: &A) -> Option<BStackRc<T, A>> {
        // self.ctrl: BStackRef<XOnDiskRef>
        // CAS-increment ctrl.strong, but only if it is currently nonzero.
        // On success, resolve ctrl.x to populate the returned BStackRc<T>'s
        // data-side reference.
    }
}
```

`strong` and `weak` live in the same allocation. So `upgrade()`'s check-and-increment is a single atomic operation on one always-valid location. There is no window where a concurrent drop can invalidate it. `BStack`'s atomic APIs make this enforceable, and it is the same guarantee `std::sync::Arc`/`Weak` provides.

---

## Weak References: `BStackWeak<T>`

`BStackWeak<T>` is a non-owning handle that does not participate in `BStackDrop`. It is used for back-pointers and cycles (e.g. doubly linked lists) where ownership must not be shared.

### Session-Scoped Back-References: Just Borrow

Within a single session, there is no need to derive a `BStackWeak<T>` from a `BStackOwned<T>` at all. Just borrow a `BStackSlice<'a>` or `BStackRef<T>` to reach back to the parent block. The only reason to use `BStackWeak<T>` is if the parent block may be deallocated while the child is still alive.

### Cross-Session / On-Disk Weak Refs

Weak refs stored on disk as `BStackRef` fields cannot be statically checked. Their safety is runtime only, backed by the control block above.

Like `BStackRange`, a `BStackRef<T>` carries no backing reference of its own. So `upgrade` takes the allocator, or the stack, explicitly. See [`upgrade()` Revisited](#upgrade-revisited) for the full signature and CAS behavior.

A `BStackWeak<T>` has only two safe origins: `BStackRc<T>::downgrade()` on a live strong owner, or `Clone::clone()` on an existing `BStackWeak<T>`. Both just increment `ctrl.weak` on an `(rc, weak)` block's control block (see [Type-Level Gate](#type-level-gate)). Neither `BStackOwned<T>` nor plain `#[bstack_block(rc)]` exposes any such path. So there is no safe way to produce an on-disk weak ref to a block without a control block. The residual risk is confined to explicit `unsafe` low-level reconstruction. That is guarded under `unsafe` code, and the caller must justify it.

**Note:** Reference cycles (e.g. doubly linked lists) cannot use `BStackDrop` on both directions without infinite recursion. Back-pointers must always be `BStackWeak`, never owned refs.

---

## Open Questions

1. **`bstack_move`**: destructuring an owned block into its children. The correct order is to read child offsets from `XOnDisk`, transfer ownership of children, then dealloc the parent block. This must be made explicit to avoid use-after-free.

2. **`bstack_cast`**: safe casting between a typed handle (e.g. `X`) and its underlying primitive (`BStackSlice` for borrowed views, `BStackOwnedSlice` for owned handles) and back. Needs a defined API.
