# BStack RAII via Block Ownership

## Feature Flag: `raii` (depends on `alloc` + `set`)

## Overview

This document describes the ownership, lifetime, and on-disk layout system for BStack blocks. It is a typed layer built on top of the mainline `alloc` module's primitives: [`BStackRange`], [`BStackSlice`], and [`BStackOwnedSlice`]. These primitives already enforce single ownership of allocations at compile time. This document decouples disk-level destruction (`BStackDrop`) from Rust's process-scoped `Drop`. That allows persistent storage semantics while still offering RAII conveniences when desired.

The design of this RAII semantics is inspired by C++ RAII with shared ownership: `std::unique_ptr`, `std::shared_ptr`, and `std::weak_ptr`. It is adapted to persistent storage and Rust `Drop` semantics.

## Why

The allocation layer, which exists on top of `BStack`'s atomicity and crash-safety guarantees, already abstracts away the details of persistent storage and complicated IO operations. While `BStackRange`, `BStackSlice`, and `BStackOwnedSlice` already provide the ownership, lifetime, mutability, and allocation guarantees needed for sound persistent storage, they deliberately operate at the level of byte ranges rather than typed values. This keeps the core allocator small, composable, and independent of any object model.

Working directly with byte slices, however, becomes repetitive for larger object graphs. Every block requires manual offset resolution, header parsing, field access, recursive destruction, and runtime type checking. While all of this is possible using the primitive APIs alone, it places the same boilerplate on every caller. This naturally require a new, higher level of abstractions that also concerns itself with Typing and automatic resource management, in addition to the existing guarantees of the underlying primitives. The RAII layer is that abstraction.

This layer exists to generate that boilerplate automatically. It introduces typed wrappers, generated accessors, recursive destruction, shared ownership, and compile-time validation while preserving the semantics of the underlying primitives. The ownership model is not replaced or redefined. Every typed handle ultimately wraps `BStackRange`, `BStackSlice`, or `BStackOwnedSlice`, and all persistent allocation semantics continue to be enforced by those foundational types.

As a result, the core API remains small, providing a minimal set of guarantees of atomicity and crash-safety, suitable for applications that need safe, but raw IO, the alloc layer provides allocation and lifetime guarantees, suitable for applications that builds complicated blocks of data inside single files while manual management of these blocks is desired, while higher-level code can opt into a strongly typed, RAII-style interface without sacrificing the guarantees of the underlying storage model.

## When

`bstack` crate requires stable features to be ABI stable since breaking ABI changes are to be avoided unless soundness or security is at stake. The RAII layer is a new feature that introduces a lot of ABI through block layouts, control blocks, and refcounting. It also introduces a lot of new APIs, which are not yet stable. Therefore, merging into `master` might take a while.

---

## Primitive Handles

The mainline `alloc` module splits what used to be a single foundational handle into three tiers. Each tier adds a capability the previous one lacks.

- **`BStackRange`**: a bare `(offset, len)` coordinate pair. It is `Copy` and carries no backing reference at all, not even a `&BStack`. This is the serialization form. Store it on disk, send it across sessions, or pass it through code that should not perform I/O. It carries no validity guarantee. Casting it into a `BStackSlice` or `BStackOwnedSlice` is `unsafe`, and the caller must justify it.
- **`BStackSlice<'a>`**: a borrowed, non-owning I/O view bound to a `&'a BStack`. It is deliberately not `Copy`, so `&mut self` write methods provide genuine single-writer exclusivity in safe code. It is `Clone` for cases where an explicit second view is needed. `Drop` is a no-op. The region persists on disk beyond the handle's scope regardless.
- **`BStackOwnedSlice<'a, A: BStackAllocator>`**: the exclusive allocation handle, bound to `&'a A`, the allocator rather than just the stack. It is neither `Copy` nor `Clone`. An allocation has exactly one owner. `realloc`/`dealloc` consume the handle by value, so no copy survives to be misused, and the type system turns use-after-free or use-after-realloc into compile errors. `Drop` is still a no-op. The allocation persists on disk until it is explicitly passed to `allocator.dealloc()`.

---

## Handle Type Hierarchy

Built on top of `BStackRange` / `BStackSlice` / `BStackOwnedSlice`:

| Type                         | Ownership                                         | Drop Behavior                      |
|------------------------------|---------------------------------------------------|------------------------------------|
| `BStackRange`                | None (bare coordinates, no backing ref)           | No-op                              |
| `BStackSlice<'a>`            | None (borrow/I/O view)                            | No-op                              |
| `BStackOwnedSlice<'a, A>`    | Exclusive (allocator-bound)                       | No-op, explicit `dealloc` required |
| `BStackRef<T>`               | None (typed wrapper over `BStackRange`)           | No-op                              |
| `BStackOwned<T: BStackDrop>` | Exclusive (newtype over `(ManuallyDrop<T>, &'a A)`) | Calls `T::bstack_drop`             |
| `BStackRc<T: BStackDrop>`    | Shared (newtype over `(StrongRef<T>, &'a A)` or `(StrongWeakRef<T>, &'a A)`) | Calls `T::bstack_drop` at zero     |
| `BStackWeak<T>`              | None (newtype over `(WeakRef<T>, &'a A)`)                                    | No-op                              |

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
    #[bstack_owned]
    a: A,
    #[bstack_owned]
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

Every non-POD field must carry exactly one of four ownership annotations. The macro enforces this. All four become a `BStackRef<T>` offset on disk; the annotation governs destruction semantics and what `bstack_move!` returns.

- `#[bstack_owned]`: the block owns this child exclusively. `BStackDrop` recurses into it. `bstack_move!` returns `BStackOwned<'a, T, A>`.
- `#[bstack_strong]`: the block holds a strong refcounted reference. `BStackDrop` decrements the refcount (and drops at zero). `bstack_move!` returns `BStackRc<'a, T, A>`.
- `#[bstack_weak]`: a non-owning weak reference. `BStackDrop` decrements the weak count only. `bstack_move!` returns `BStackWeak<'a, T, A>`.
- `#[bstack_ref]`: a raw reference with no ownership semantics. `BStackDrop` skips this field entirely. `bstack_move!` returns the bare `BStackRef<T>` with no allocator attached.

Plain POD fields (no annotation) are stored inline and copied by value.

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

The trait takes `self` — the **without-allocator** handle — plus an explicit allocator reference. This makes it generic over all handle-like types, not just `BStackOwnedSlice`:

```rust
trait BStackDrop: Sized {
    fn bstack_drop<A: BStackAllocator>(self, allocator: &A) -> Result<(), A::Error>;
}
```

Every `#[bstack_block]` type is a newtype over `BStackRange`. It carries no allocator. The allocator is supplied externally at drop time:

```rust
// Generated by #[bstack_block] for every block type.
struct X(BStackRange);
```

### Child Handle Types

Each field annotation maps to a child handle type. These are small, `Copy` types constructed transiently during `bstack_drop`. Each implements `BStackDrop` and encapsulates its own destruction logic, keeping the generated code for any given block type uniform:

```rust
// #[bstack_owned]: exclusive child. bstack_drop calls T::bstack_drop recursively.
struct OwnedRef<T>(BStackRef<T>);

// #[bstack_strong] on plain (rc) T. bstack_drop decrements the inline refcount;
// calls T::bstack_drop if it reaches zero.
struct StrongRef<T>(BStackRef<T>);

// #[bstack_strong] on (rc, weak) T. bstack_drop decrements ctrl.strong;
// at zero: calls T::bstack_drop, releases the phantom ctrl.weak, and frees the
// ctrl block if weak also reaches zero.
struct StrongWeakRef<T: BStackWeakable>(BStackRef<T>, BStackRef<T::Control>);

// #[bstack_weak] on (rc, weak) T. bstack_drop decrements ctrl.weak only;
// frees the ctrl block if weak reaches zero. Data block is never touched.
struct WeakRef<T: BStackWeakable>(BStackRef<T::Control>);

// #[bstack_ref]: BStackDrop for BStackRef<T> is a no-op. No child handle type needed.
// POD types: blanket no-op BStackDrop impl covers all Copy + !BStackOnDisk types.
```

POD types are identified via `bytemuck::Pod`. Any type that implements `bytemuck::Pod` is safe to store inline in an `XOnDisk` struct (since `Pod` guarantees `#[repr(C)]`-compatible, fully initialized, zero-padding-free bytes), and automatically receives the blanket no-op `BStackDrop` impl. The macro rejects non-annotated fields that do not implement `bytemuck::Pod` at compile time.

`StrongWeakRef` and `WeakRef` must resolve the ctrl ref from the child block before the parent is deallocated. Both provide a `from_disk` constructor that does one read:

```rust
impl<T: BStackWeakable> StrongWeakRef<T> {
    fn from_disk<S: BStackStorage>(data_ref: BStackRef<T>, stack: &S) -> Self {
        let on_disk: &T::OnDisk = stack.read(data_ref).cast();
        StrongWeakRef(data_ref, on_disk.ctrl)
    }
}

impl<T: BStackWeakable> WeakRef<T> {
    fn from_disk<S: BStackStorage>(data_ref: BStackRef<T>, stack: &S) -> Self {
        let on_disk: &T::OnDisk = stack.read(data_ref).cast();
        WeakRef(on_disk.ctrl)
    }
}
```

### Generated `bstack_drop`

The macro emits one `.bstack_drop(allocator)?` call per non-skipped field, then deallocates the block itself. Destruction is **post-order**. For the example block used throughout this document:

```rust
#[bstack_block]
struct X {
    #[bstack_owned]    a: A,
    #[bstack_strong]   b: B,   // B is #[bstack_block(rc, weak)]
    #[bstack_weak]     c: C,   // C is #[bstack_block(rc, weak)]
    #[bstack_ref]      e: E,
    d: u32,
}
```

```rust
impl BStackDrop for X {
    fn bstack_drop<A: BStackAllocator>(self, allocator: &A) -> Result<(), A::Error> {
        let stack = allocator.stack();
        let on_disk: &XOnDisk = stack.read(self.0).cast();

        OwnedRef(on_disk.a).bstack_drop(allocator)?;
        StrongWeakRef::from_disk(on_disk.b, stack).bstack_drop(allocator)?;
        WeakRef::from_disk(on_disk.c, stack).bstack_drop(allocator)?;
        // #[bstack_ref] e: BStackRef::bstack_drop is a no-op — skipped by macro
        // POD d: no action

        allocator.dealloc_range(self.0)
    }
}
```

All the atomic operations and two-phase teardown logic live inside the `BStackDrop` impls of `StrongWeakRef` and `WeakRef`, not in the generated block code. The macro output for any block is a flat sequence of uniform calls.

### With-Allocator Wrappers

`BStackOwned<T: BStackDrop, A>` is a newtype over `(ManuallyDrop<T>, &'a A)`. Rust's `Drop` takes the inner `T` out and calls `T::bstack_drop`. Errors during drop are swallowed, matching the contract of Rust's `Drop`:

```rust
struct BStackOwned<'a, T: BStackDrop, A> {
    inner: ManuallyDrop<T>,
    allocator: &'a A,
}

impl<T: BStackDrop, A: BStackAllocator> Drop for BStackOwned<'_, T, A> {
    fn drop(&mut self) {
        let inner = unsafe { ManuallyDrop::take(&mut self.inner) };
        let _ = T::bstack_drop(inner, self.allocator);
    }
}
```

`BStackRc<T>` calls `T::bstack_drop` (via `StrongWeakRef::bstack_drop`) when the strong refcount reaches zero, passing the allocator it already holds. The `BStackDrop` impls on the child handle types carry all refcount and ctrl-block logic, so `BStackRc`'s own `Drop` impl stays minimal.

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

`BStackRc<'a, T, A>` is a newtype over `(StrongRef<T>, &'a A)` for plain `(rc)` blocks. `StrongRef<T>` is the without-allocator inner handle; it carries just a `BStackRef<T>` and implements `BStackDrop`. The allocator is held alongside but not inside the inner type. `BStackRc` is neither `Copy` nor the same as duplicating a `BStackOwnedSlice` — it simply wraps the inner handle and an allocator reference. `BStackRc<T>` behaves as follows.

- `clone`: increments refcount atomically (reading it from the block on disk), then constructs a new `BStackRc` wrapping the same `StrongRef<T>` and `&'a A`.
- `drop`: calls `StrongRef<T>::bstack_drop(self.0, self.1)`, which decrements the refcount and calls `T::bstack_drop` if it reaches zero.

Refcount is stored inline in the block rather than in a separate allocation. This avoids an extra indirection on access. The cost is that every `(rc)` block carries 8 bytes of refcount overhead unconditionally.

Plain `#[bstack_block(rc)]` has no weak count and no control block. So `BStackRc<T>` for such a block does not expose `downgrade`/`weak` at all. Blocks that expect real `BStackWeak<T>` support must opt in with `#[bstack_block(rc, weak)]`.

---

## Strong and Weak References via Control Block

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

`BStackRc<T>` for an `(rc, weak)` block is a newtype over `(StrongWeakRef<T>, &'a A)`. `StrongWeakRef<T>` is the without-allocator inner handle: it holds both the data ref and the ctrl ref, and its `BStackDrop` impl encapsulates the two-phase teardown. `BStackWeak<T>` is analogously a newtype over `(WeakRef<T>, &'a A)`, where `WeakRef<T>` holds only the ctrl ref:

```rust
// (rc, weak) block
struct BStackRc<'a, T: BStackWeakable, A>(StrongWeakRef<T>, &'a A);
struct BStackWeak<'a, T: BStackWeakable, A>(WeakRef<T>, &'a A);

// plain (rc) block
struct BStackRc<'a, T, A>(StrongRef<T>, &'a A);
```

For `(rc, weak)`, `.clone()` increments `ctrl.strong` and returns a new `BStackRc` wrapping the same `StrongWeakRef<T>` and `&'a A`. `.drop()` calls `StrongWeakRef<T>::bstack_drop(self.0, self.1)`. `.downgrade()` increments `ctrl.weak`, copies the `WeakRef<T>` out of `self.0`, and wraps it with the allocator into a `BStackWeak<T>`.

`BStackWeak<T>` is `Clone`, the same way `BStackRc<T>` is. Cloning a `BStackWeak<T>` calls `WeakRef<T>::clone` on its inner handle, which increments `ctrl.weak` directly with no need for a live `BStackRc<T>`. `drop` calls `WeakRef<T>::bstack_drop`, which decrements `ctrl.weak` and frees the ctrl block if it reaches zero. So a `BStackWeak<T>` comes from either `BStackRc<T>::downgrade()` or `BStackWeak<T>::clone()`. This mirrors how `std::sync::Weak` can come from either `Arc::downgrade` or `Weak::clone`.

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
impl<'a, T: BStackWeakable, A: BStackAllocator> BStackWeak<'a, T, A> {
    pub fn upgrade(&self, allocator: &'a A) -> Option<BStackRc<'a, T, A>> {
        // self.0: WeakRef<T> — holds BStackRef<T::Control>
        // CAS-increment ctrl.strong, but only if it is currently nonzero.
        // On success, read ctrl.x (the data ref) and construct a StrongWeakRef<T>,
        // then wrap it with the allocator into a BStackRc<T>.
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

## `bstack_move!`: Destructuring an Owned Block

`bstack_move!` is a procedural macro for transferring ownership of all fields out of a `BStackOwned<X>`. It reads every field from `XOnDisk`, captures `BStackRef<T>` offsets and POD values before the dealloc, frees the parent shell (not the children), reconstructs typed handles with the allocator attached, and returns them as a tuple.

Usage:

```rust
let (a, b, c) = bstack_move!(x)?;
```

`bstack_move!` can only be called on a `BStackOwned<X, A>`. Using it on a `BStackRef<X>`, `BStackSlice`, or any non-owned handle is a compile error, enforced by the trait bound on `BStackOwned`.

### Return Types by Field Annotation

Each field's return type in the tuple is the allocator-stripped in-memory representation of the corresponding handle kind. The allocator is never included in the return — the caller already holds it.

| Field Annotation                      | Returned Type           | Returned Handle's Drop Behavior |
|---------------------------------------|-------------------------|---------------------------------|
| `#[bstack_owned]`                     | `BStackOwned<'a, T, A>` | Frees child (via `BStackDrop`)  |
| `#[bstack_strong]` on plain `(rc)` T  | `BStackRc<'a, T, A>`    | Frees child at refcount zero    |
| `#[bstack_strong]` on `(rc, weak)` T  | `BStackRc<'a, T, A>`    | Frees child at refcount zero    |
| `#[bstack_weak]`                      | `BStackWeak<'a, T, A>`  | Decrements weak count only      |
| `#[bstack_ref]`                       | `BStackRef<T>`          | No-op                           |
| POD field                             | `T`                     | N/A                             |

The lifetime `'a` is the same as the allocator lifetime carried by the input `BStackOwned<'a, X, A>`.

For `(rc, weak)` and weak fields, the macro performs one extra read per field to resolve the `ctrl` ref from the child block's on-disk `ctrl` back-pointer before `dealloc` is called on the parent, so it can construct the full `BStackRc` or `BStackWeak` in-place. For plain `(rc)` fields, `BStackRef<T>` alone is sufficient.

### Expansion

The macro expands to roughly the following. Given a block with all four field kinds:

```rust
#[bstack_block]
struct X {
    #[bstack_owned]    // owned
    a: A,
    #[bstack_strong]   // shared, B is #[bstack_block(rc, weak)]
    b: B,
    #[bstack_weak]     // weak back-pointer, C is #[bstack_block(rc, weak)]
    c: C,
    #[bstack_ref]      // raw ref, no ownership
    e: E,
    d: u32,
}
```

```rust
// bstack_move!(x) where x: BStackOwned<'a, X, Alloc>
// Returns: Result<(BStackOwned<'a, A, Alloc>, BStackRc<'a, B, Alloc>, BStackWeak<'a, C, Alloc>, BStackRef<E>, u32), Alloc::Error>
let (a, b, c, e, d) = {
    // Take the inner X(BStackRange) out of ManuallyDrop, preventing BStackDrop
    // from running on drop. The allocator is separated out alongside.
    let (inner, allocator): (X, &Alloc) = x.into_raw_parts();
    let stack = allocator.stack();
    // BStackRef<T> is Copy (just a u64 offset), so all refs can be captured
    // before the range is passed to dealloc_range.
    let on_disk: &XOnDisk = stack.read(inner.0).cast();

    // #[bstack_owned]: owned child — read offset, reconstruct as BStackOwned
    let a_ref: BStackRef<A> = on_disk.a;

    // #[bstack_strong] on (rc, weak) B: read B's ctrl ref via StrongWeakRef::from_disk
    let b_swr: StrongWeakRef<B> = StrongWeakRef::from_disk(on_disk.b, stack);

    // #[bstack_weak] on (rc, weak) C: read C's ctrl ref via WeakRef::from_disk
    let c_wr: WeakRef<C> = WeakRef::from_disk(on_disk.c, stack);

    // #[bstack_ref]: raw ref — copy offset as-is, no allocator attached
    let e_ref: BStackRef<E> = on_disk.e;

    // POD: copied inline
    let d_val: u32 = on_disk.d;

    // Free the parent shell only. Children are untouched on disk.
    allocator.dealloc_range(inner.0)?;

    // All unsafe reconstruction happens inside the macro, after dealloc_range,
    // using the allocator reference that outlives the consumed inner handle.
    // #[bstack_ref] fields are returned as bare BStackRef<T> — no unsafe needed.
    let owned_a = unsafe { BStackOwned::from_raw(X(a_ref.into_range()), allocator) };
    let rc_b    = unsafe { BStackRc::from_raw_parts(b_swr.0, b_swr.1, allocator) };
    let weak_c  = unsafe { BStackWeak::from_raw(c_wr.0, allocator) };
    (owned_a, rc_b, weak_c, e_ref, d_val)
}?;
```

Capturing values before `dealloc_range` is safe because `BStackRef<T>` and the child handle types are all `Copy` and carry no reference into the allocation being freed. After `dealloc_range` returns, all on-disk children remain intact. The allocator reference `&'a A` is still valid. Taking `X` out via `into_raw_parts()` prevents the `BStackOwned` `Drop` impl from running, which is the compile-time proof that no parallel destruction path exists. All `unsafe` reconstruction is emitted by the macro and justified by these invariants — the call site is fully safe.

### Error Handling

`bstack_move!` returns `Result<(...), A::Error>`. If `dealloc` fails on the parent shell, the macro returns `Err`. The raw child values are not in scope from the caller's perspective because `?` propagates before the tuple is bound. The parent block's allocation is still live in that case. The caller can retry or abort.

### Restrictions

- `bstack_move!` is not defined for `#[bstack_block(rc)]` or `#[bstack_block(rc, weak)]` blocks. Those blocks have a refcount or control block that would be bypassed. Use `BStackRc::try_unwrap` for the `(rc)` case instead.
- `#[bstack_weak]` fields require `T: BStackWeakable`. This is already enforced at `#[bstack_block]` expansion time, so it is not an additional constraint at the `bstack_move!` call site.
- `#[bstack_ref]` fields return a bare `BStackRef<T>` with no allocator and no ownership guarantee. The caller is responsible for the lifetime and validity of the referenced allocation.

---

## `bstack_cast!`: Type-Checked Handle Conversion

`bstack_cast!` converts between a typed block handle (e.g. `BStackOwned<X, A>` or borrowed `X`) and its untyped primitive (`BStackOwnedSlice<'a, A>` or `BStackSlice<'a>`), in both directions. Upcasts (typed → untyped) are infallible. Downcasts (untyped → typed) check the `EightCC` tag stored in the block header and are fallible.

The `EightCC` tag is derived from the type name at `#[bstack_block]` expansion time and written into every block's `BlockHeader` by the allocator at creation. It is the sole discriminant for safe downcasting.

### Trait

`BStackCast` is generated by `#[bstack_block]` for every block type and is the gate for all downcast paths:

```rust
/// Implemented by every `#[bstack_block]` type.
/// The `EightCC` returned here must match the tag in a block's `BlockHeader`
/// for a downcast to succeed.
pub trait BStackCast {
    fn eightcc() -> EightCC;
}
```

### Owned Casts

```rust
// Upcast: strip type info. Infallible. Consumes the typed handle.
impl<'a, T: BStackCast, A: BStackAllocator> BStackOwned<'a, T, A> {
    pub fn into_slice(self) -> BStackOwnedSlice<'a, A> { ... }
}

// Downcast: re-apply type by checking the EightCC tag.
// Returns Err(self) if the tag does not match, so the caller retains
// ownership and can try another type or handle the mismatch.
impl<'a, A: BStackAllocator> BStackOwnedSlice<'a, A> {
    pub fn cast_into<T: BStackCast>(self) -> Result<BStackOwned<'a, T, A>, Self> {
        let header: &BlockHeader = self.as_slice().cast();
        if header.type != T::eightcc() {
            return Err(self);
        }
        Ok(BStackOwned::from_inner(self))
    }
}
```

### Borrowed Casts

```rust
// Upcast: strip type info. Infallible. Generated per block type by the macro.
impl X {
    pub fn as_slice(&self) -> BStackSlice<'_> { ... }
}

// Downcast: re-apply type by checking the EightCC tag.
// Returns None if the tag does not match.
impl<'a> BStackSlice<'a> {
    pub fn cast_as<T: BStackCast>(&self) -> Option<T> {
        let header: &BlockHeader = self.cast();
        if header.type != T::eightcc() {
            return None;
        }
        Some(T::from_slice(self.clone()))
    }
}
```

### Macro Convenience

`bstack_cast!` is a convenience macro that infers the direction from the target type:

```rust
// Owned downcast (fallible): emits .cast_into::<X>() on the slice
let x: BStackOwned<X, _> = bstack_cast!(slice)?;

// Owned upcast (infallible): emits .into_slice() on the owned handle
let slice: BStackOwnedSlice<_, _> = bstack_cast!(x);

// Borrowed downcast (returns Option): emits .cast_as::<X>() on the slice ref
let x: Option<X> = bstack_cast!(&slice_ref);
```

The macro inspects whether the target type is `BStackOwnedSlice` / `BStackSlice` (upcast path) or a concrete `#[bstack_block]` type (downcast path) and emits the corresponding method call, removing the need to name the direction explicitly.
