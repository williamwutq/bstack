# bstack_raii

Typed, RAII-style ownership for persistent objects — `Rc`/`Weak` semantics that
survive a process restart or crash, backed by a single [`bstack`] file.

`std::rc::Rc` and `Weak` live and die with the process. `bstack_raii` gives you
the same model — shared strong handles, non-owning weak handles, and automatic
cleanup when the last owner drops — but the object graph *and its reference
counts* are stored on disk, crash-safely. You define blocks as ordinary structs
and enums; the [`#[bstack_block]`](#structs) / [`#[bstack_enum]`](#enums-bstack_enum)
macros generate the on-disk layout, typed accessors, constructors, recursive
teardown, and refcounting.

It is a thin, typed layer over the mainline `bstack` allocator
(`BStackRange` / `BStackSlice` / `BStackOwnedSlice`), which already provides the
atomicity, crash-safety, and single-ownership guarantees. This crate adds the
object model on top.

> **Status:** feature-complete and tested, but the on-disk ABI is not yet
> stable. Developed inside the [`bstack`] repository; not yet published to
> crates.io.

## Contents

- [Quick start](#quick-start)
- [Concepts](#concepts)
- [How it works on disk](#how-it-works-on-disk)
- [Handles & lifetimes](#handles--lifetimes)
- [Generated types](#generated-types)
- [Blocks](#blocks)
  - [Field ownership](#field-ownership)
  - [Structs](#structs)
  - [Reference-counted blocks](#reference-counted-blocks)
  - [Vectors and strings](#vectors-and-strings)
  - [Nullable fields: `Option`](#nullable-fields-option)
  - [Enums: `#[bstack_enum]`](#enums-bstack_enum)
  - [Field types](#field-types)
- [Moving out: `bstack_move!`](#moving-out-bstack_move)
- [Casting: `bstack_cast!`](#casting-bstack_cast)
- [Type tags (`EightCC`)](#type-tags-eightcc)
- [Limitations](#limitations)

## Quick start

Add both crates (`bstack` supplies the allocator; `bstack_raii` the object
layer):

```toml
[dependencies]
bstack_raii = { git = "https://github.com/williamwutq/bstack" }
bstack = "0.4"
```

```rust
use std::io;
use bstack::FirstFitBStackAllocator;
use bstack_raii::{BStack, BStackAllocator, BStackDrop, TryClone, bstack_block};

// A shared, reference-counted, weak-observable block.
#[bstack_block(rc, weak)]
struct Config {
    version: u64,
    flags: u64,
}

// A block that owns a *strong* reference to a shared Config.
#[bstack_block]
struct Session {
    id: u64,
    #[bstack_strong]
    config: Config,
}

fn main() -> io::Result<()> {
    let alloc = FirstFitBStackAllocator::new(BStack::open("app.bstack")?)?;
    let stack = alloc.stack();

    let config = Config::new(&alloc, 3, 0b1010)?;      // BStackRc<Config>, strong = 1
    let session = Session::new(&alloc, 0, config.try_clone()?)?; // strong = 2

    // Read fields through generated accessors.
    let cfg = session.handle().config(stack)?;         // -> a Config handle
    println!("v{} flags {:#b}", cfg.version(stack)?, cfg.flags(stack)?);

    drop(config);                  // strong = 1 — the session still owns it (Rc: auto-decrement)
    session.bstack_drop(&alloc)?;  // strong = 0 — Config freed automatically by its refcount
    Ok(())
}
```

Two things are load-bearing here, both covered below:

- **Owned vs. shared teardown.** A `Session` is a uniquely-owned block, so its
  handle frees **nothing on `Drop`** — you free it explicitly with `bstack_drop`,
  so a persistent root is never silently deleted when a handle goes out of scope.
  A shared `Config` handle (`BStackRc`) *does* auto-manage its refcount on `Drop`.
  See [Handles & lifetimes](#handles--lifetimes).
- Every block also gets a stable 8-byte on-disk type tag — see
  [Type tags](#type-tags-eightcc).

A fuller walk-through (shared ownership, weak observers, durability across a
reopen) is in [`examples/sessions.rs`](examples/sessions.rs):
`cargo run --example sessions`.

## Concepts

A **block** is a fixed-size record on disk. You write it as an ordinary `struct`
(or [`enum`](#enums-bstack_enum)) and annotate it; the macro generates a parallel
`#[repr(C, packed)]` on-disk layout plus all the machinery to work with it.

Both macros take the same three **modes** (chosen by their arguments — e.g.
`#[bstack_block(rc)]` or `#[bstack_enum(rc)]`):

| Mode         | Meaning                                                  |
|--------------|----------------------------------------------------------|
| *(none)*     | Plain, exclusively owned (like `Box`).                   |
| `(rc)`       | Reference-counted, inline count (like `Rc`, no `Weak`).  |
| `(rc, weak)` | Refcounted **and** weak-observable (like `Rc` + `Weak`). |

Every non-POD field carries an [ownership annotation](#field-ownership) deciding
how it is torn down. Plain-old-data fields (anything `Pod` — integers, `[u8; N]`,
…) are stored inline and copied by value.

> **Requires a real allocator.** This layer needs a `bstack` allocator that
> actually frees (`dealloc`) and reserves offset 0 for its own metadata — e.g.
> `FirstFitBStackAllocator`, `SlabBStackAllocator`, `GhostTreeBstackAllocator`.
> **Not `LinearBStackAllocator`**: its `dealloc` is a no-op (teardown would free
> nothing) and it can hand out offset 0 (breaking the `Option` niche). For
> growable fields, use a **realloc-safe** allocator (growth reallocates the
> backing block); `FirstFitBStackAllocator` is realloc-safe.

## How it works on disk

Every block begins with a 16-byte `BlockHeader { size: u64, tag: EightCC }`
(the [tag](#type-tags-eightcc) is the downcast discriminant). References between
blocks are stored as bare `u64` offsets; a target's length is recovered from its
compile-time `size_of::<T::OnDisk>()` — which is why blocks are **fixed-size**.

The three modes differ only in what is injected after the header:

- **`#[bstack_block]`** — nothing; the payload follows the header directly.
- **`(rc)`** — an inline `refcount: u64`. The block is freed when it hits zero.
- **`(rc, weak)`** — a `ctrl` back-pointer to a separate **control block**
  (`XOnDiskRef`) holding `strong` / `weak` counters and a forward pointer to the
  data. The data block is reclaimed when `strong` hits zero; the small control
  block persists until `weak` also hits zero — exactly like `Arc` / `Weak`, so a
  `Weak` can outlive the data and observe that it's gone.

**Growable fields** (`Vec` / `String`) don't fit a fixed-size block, so they live
out of line: the field holds a 16-byte descriptor `{ data_off, data_size }`
*inline*, pointing at a separate data block that reallocates as it grows. Since
the block owns the vector uniquely, that descriptor needs no block of its own —
details in [Vectors and strings](#vectors-and-strings).

Refcount updates are single-lock read-modify-writes on `bstack` (crash-atomic,
no spin loop). Everything is durable and speaks [`std::io::Result`].

## Handles & lifetimes

The typed handle `X` is a bare `(offset, len)` with no allocator — cheap,
`Copy`, and the thing you read fields through. Ownership wrappers layer on top:

| Handle               | Allocator | Ownership                        | Teardown                                                             |
|----------------------|-----------|----------------------------------|----------------------------------------------------------------------|
| `X` (the block type) | no        | none (borrowed view / bare ref)  | `x.bstack_drop(alloc)?` — explicit only                              |
| `BStackOwned<X>`     | no        | exclusive (ownership marker)     | `owned.bstack_drop(alloc)?` — **nothing on `Drop`**                  |
| `AutoDrop<T>`        | yes       | RAII guard over any `BStackDrop` | runs `bstack_drop` on Rust `Drop`                                    |
| `BStackRc<X>`        | yes       | shared strong                    | `try_clone` / `downgrade`; **auto-decrements on `Drop`**, frees at 0 |
| `BStackWeak<X>`      | yes       | none (keeps control block alive) | `try_clone` / `upgrade`; auto-decrements weak on `Drop`              |
| `BStackRef<X>`       | no        | none (raw offset)                | none                                                                 |

**Owned is manual; shared is automatic.** A uniquely-owned `BStackOwned<X>`
carries no allocator and frees **nothing** when it drops — so a persistent root
is never silently deleted by going out of scope. Free it explicitly, or wrap it
in an `AutoDrop` guard for RAII:

```rust
let owned: BStackOwned<Node> = Node::new(&alloc, /* … */)?;
let value = owned.handle().tag(stack)?;   // read a field (or `owned.tag(stack)?` via Deref)

owned.bstack_drop(&alloc)?;               // free it now, explicitly …
// … or: let _guard = owned.auto(&alloc); // RAII — freed when `_guard` drops
```

Shared handles (`BStackRc` / `BStackWeak`) *do* manage their counts on `Drop`,
like `std::rc`. Because duplicating one bumps an on-disk counter (fallible I/O),
cloning is the [`TryClone`] trait, not `Clone`.

## Generated types

Each macro generates a small, fixed set of types (for a block named `X` / `E`):

| Source                                | Types generated                                                                                                   |
|---------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| `#[bstack_block] struct X`            | `X` — the [handle](#handles--lifetimes); `XOnDisk` — the `#[repr(C, packed)]` on-disk payload                      |
| `#[bstack_block(rc, weak)] struct X`  | the above, plus `XOnDiskRef` — the [control block](#how-it-works-on-disk) (`strong`/`weak` counters)               |
| `#[bstack_enum] enum E`               | `E`, `EOnDisk` (plus `EOnDiskRef` for `(rc, weak)`), and two companion enums — see [Enums](#enums-bstack_enum): `EData` (owned form) and `EView` (read result) |

Alongside the types come the trait impls (`BStackBlock`, `BStackDrop`,
`BStackCast`, `BStackMove`, and for rc modes `BStackShared` / `BStackWeakable`)
and inherent methods (`new`, field accessors, `set_<field>`, enum `read`, …). In
your code you name only the handle (`X` / `E`) and, for enums, `EData` / `EView`;
`XOnDisk` and friends are internal.

## Blocks

A block is a `struct` or `enum` annotated with `#[bstack_block]` /
`#[bstack_enum]`. The rest of this section covers how fields and variants are
declared and what you can put in them.

### Field ownership

Every non-POD field carries exactly one annotation, which decides its teardown
and what [`bstack_move!`](#moving-out-bstack_move) yields:

| Annotation         | Child kind required    | On teardown                        | `bstack_move!` yields   |
|--------------------|------------------------|------------------------------------|-------------------------|
| `#[bstack_owned]`  | any block              | recursively frees the child        | `BStackOwned<T>`        |
| `#[bstack_strong]` | `(rc)` or `(rc, weak)` | decrements refcount; frees at zero | `BStackRc<T>`           |
| `#[bstack_weak]`   | `(rc, weak)`           | decrements weak count only         | `Option<BStackWeak<T>>` |
| `#[bstack_ref]`    | any block              | nothing                            | `BStackRef<T>`          |
| *(none)* — POD     | `Pod` type             | nothing (inline)                   | the value               |

Rules are enforced at compile time: a `#[bstack_weak]` field whose target isn't
`(rc, weak)`, or a non-`Pod` field with no annotation, is a compile error.

### Structs

```rust
#[bstack_block]
struct Node {
    #[bstack_owned]  payload: Payload,   // exclusively owns the child
    #[bstack_strong] shared: SharedThing, // a shared, refcounted reference
    #[bstack_weak]   parent: Node,        // a non-owning back-pointer (may dangle)
    #[bstack_ref]    sibling: Node,       // a raw reference, no ownership
    tag: u32,                             // POD, stored inline
}
```

Besides the [generated types](#generated-types), the macro emits inherent
methods:

- a `new(...)` **constructor** that allocates and wires the block, consuming the
  child handles it takes ownership of (`#[bstack_owned]` → `BStackOwned<T>`,
  `#[bstack_strong]` → `BStackRc<T>`, `#[bstack_ref]` → `BStackRef<T>`, POD by
  value; `#[bstack_weak]` fields are **not** parameters);
- **accessors** — `node.field(stack)` for each field;
- **`set_<field>`** setters for `#[bstack_weak]` fields (see below);
- recursive teardown, [casting](#casting-bstack_cast), and
  [moving](#moving-out-bstack_move).

A **tuple struct** works too, as long as every field is `Pod`: its positional
fields get synthetic names, so `struct Rgb(u8, u8, u8)` is constructed
`Rgb::new(&alloc, 10, 20, 30)`, read via `rgb.field0(stack)?` / `field1` / …, and
`bstack_move!` hands the fields back in order. A **unit struct**
(`#[bstack_block] struct Marker;`) is a valid **header-only** block — just the
16-byte header, no payload.

### Reference-counted blocks

Declare `#[bstack_block(rc)]` / `#[bstack_block(rc, weak)]` (the on-disk layout
is in [How it works on disk](#how-it-works-on-disk)). `new` then returns a
`BStackRc<X>`; clone it with `try_clone`, and for `(rc, weak)` get a non-owning
observer with `downgrade` / `upgrade` (see [Handles & lifetimes](#handles--lifetimes)):

```rust
let a = Config::new(&alloc, 1, 0)?;   // BStackRc<Config>, strong = 1
let b = a.try_clone()?;               // strong = 2
let w = a.downgrade()?;               // BStackWeak<Config>
drop(a); drop(b);                     // strong = 0 — the data is freed
assert!(w.upgrade()?.is_none());      // the object is gone
```

**Weak fields** are for back-pointers and cycles, where the target doesn't exist
at construction. They start null and are wired afterward; the accessor is an
*upgrade*. The field stores the target's *control-block* offset, so dropping the
strong owner first and the holder second is sound — no use-after-free:

```rust
let a = WNode::new(&alloc, 1)?;
let b = WNode::new(&alloc, 2)?;
b.handle().set_back(&alloc, a.downgrade()?)?;      // wire b.back -> a (weak)

if let Some(a2) = b.handle().back(&alloc)? {       // accessor upgrades
    println!("a still alive: {}", a2.handle().val(stack)?);
}
```

### Vectors and strings

A `Vec<T>` (POD `T`) or `String` field stores a growable sequence, backed by the
inline descriptor described in [How it works on disk](#how-it-works-on-disk).
(These are [field-type spellings](#field-types), not `std::vec::Vec` / `String`.)

```rust
#[bstack_block]
struct Record {
    name: String,
    tags: Vec<u32>,      // POD vectors are un-annotated (any Pod element type)
    id: u64,
}

let rec = Record::new(&alloc, "hello", &[1u32, 2, 3], 42)?;  // &str / &[T] / value
let mut tags = rec.handle().tags(&alloc)?;    // a BStackVec<u32> handle
tags.push(4)?;                                // grows; rewrites the inline descriptor
assert_eq!(rec.handle().tags(&alloc)?.to_vec()?, vec![1, 2, 3, 4]);
```

The accessor returns a [`BStackVec<T>`] (`len` / `to_vec` / `push`); freeing the
block frees the data. A vector built by `BStackVec::from_slice` or handed out by
`bstack_move!` is *detached* — it carries its descriptor in memory and is
persistent only once written into a field (the general
[moved-out-is-unrooted](#moving-out-bstack_move) rule).

When the elements are `#[bstack_block]` values, the **annotation** states the
elements' ownership (the descriptor + offset array stay owned by the struct). An
**un-annotated** `Vec<T>` is therefore always POD (`T: Pod`):

| Field                                | Element handle       | Accessor                 | On the struct's teardown                              |
|--------------------------------------|----------------------|--------------------------|-------------------------------------------------------|
| `Vec<T>` / `String` *(un-annotated)* | POD value (`T: Pod`) | `BStackVec<T>`           | frees the data block                                  |
| `#[bstack_owned] Vec<Thing>`         | `BStackOwned<Thing>` | `BStackBlockVec<Thing>`  | recursively frees every child, then the offset array  |
| `#[bstack_strong] Vec<Thing>`        | `BStackRc<Thing>`    | `BStackStrongVec<Thing>` | releases each strong ref (frees at 0), then the array |
| `#[bstack_weak] Vec<Thing>`          | `BStackWeak<Thing>`  | `BStackWeakVec<Thing>`   | releases each weak ref, then the array                |
| `#[bstack_ref] Vec<Thing>`           | `BStackRef<Thing>`   | `BStackRefVec<Thing>`    | frees the offset array only                           |

The constructor takes a `Vec` of the matching element handle; the accessor
returns the vector handle (`len` / `to_vec` / `get`; `BStackWeakVec` has
`upgrade(i)`; each has a `push_*`).

To **share** a vector between two structs, wrap it in its own `#[bstack_block]`
and share *that* block with `#[bstack_strong]` / `#[bstack_ref]` — a descriptor
has a single owner.

### Nullable fields: `Option`

Wrap a reference or vector field in `Option` (another
[field-type spelling](#field-types)) to make it nullable. On disk it's unchanged
— a `0` offset (or a `0` vector descriptor) is `None`, since no allocation ever
lives at offset 0 (an *empty* present vector still has a non-zero data block, so
it's distinct from `None`):

```rust
#[bstack_block]
struct Node {
    #[bstack_owned]  left: Option<Child>,     // may be absent
    #[bstack_strong] shared: Option<Thing>,
    labels: Option<Vec<u32>>,                 // nullable POD vector
}
```

The accessor returns `io::Result<Option<_>>`, the constructor takes an `Option`
(`Option<BStackOwned<Child>>` / `Option<&[T]>` / …), and `bstack_move!` yields
`Option<_>`. (`#[bstack_weak]` fields are already nullable.)

An `Option` on an **un-annotated POD** field is different: `Option<A>` is stored
*inline* whenever `A: bytemuck::PodInOption` (so `Option<A>: Pod`) — e.g.
`Option<NonZeroU32>` — riding bytemuck's niche, with the `Option<A>` handed back
by value. (A plain `Option<u32>` is *not* `Pod`, so it doesn't compile as a POD
field — annotate it, or use a `NonZero`.)

`Option` *is* a Rust `enum`, but this is a niche optimization baked into the
macro — **not** a [`#[bstack_enum]`](#enums-bstack_enum) (no discriminant byte, no
`EData` / `EView`, no extra block). `Option` is the only enum that gets it; any
other sum type is a `#[bstack_enum]`.

### Enums: `#[bstack_enum]`

A `#[bstack_enum]` lowers a Rust `enum` to a **tagged-union block**: a
discriminant plus a payload area sized to the largest variant. A variant is
either a **POD aggregate** — unit, an all-`Pod` tuple `V(A, B, …)`, or an
all-`Pod` struct `V { x: A, … }` (fields packed inline, no annotation) — or an
**annotated single-field tuple** whose annotation states the *variant's*
relationship, exactly like a struct field — `#[bstack_owned]` /
`#[bstack_strong]` / `#[bstack_weak]` / `#[bstack_ref]` (each a `u64` offset to
the child / control block):

```rust
#[bstack_enum]
enum Node {
    Empty,                            // unit (POD aggregate, 0 fields)
    Num(u32),                         // POD, inline
    Rect { w: u32, h: u32 },          // POD struct variant, packed inline
    #[bstack_ref]    Link(Leaf),      // borrowed reference (frees nothing)
    #[bstack_owned]  Child(Leaf),     // owned child (freed on teardown)
    #[bstack_strong] Shared(Thing),   // a strong ref (Thing is (rc)/(rc, weak))
    #[bstack_weak]   Watch(Thing),    // a weak ref (Thing is (rc, weak))
}

let node = Node::new(&alloc, NodeData::Child(leaf))?;   // construct a variant
match node.handle().read(&alloc)? {                     // read / match it
    NodeView::Child(c) => assert_eq!(c.val(stack)?, 7),
    _ => {}
}
node.bstack_drop(&alloc)?;                              // frees the owned child too
```

The two [companion enums](#generated-types) are duals of each other's directions:

- **`NodeData`** — the in-memory *owned* form (POD by value; `#[bstack_owned]` →
  `BStackOwned<T>`, `#[bstack_strong]` → `BStackRc<T>`, `#[bstack_weak]` →
  `BStackWeak<T>`, `#[bstack_ref]` → `BStackRef<T>`). The **same** type is passed
  to `new` and returned by [`bstack_move!`](#moving-out-bstack_move).
- **`NodeView`** — the read result: POD by value, owned/ref children as borrowed
  handles, a weak variant *upgraded* to `Option<BStackRc<T>>`. `read` takes the
  allocator (a weak variant upgrades through it).

Like a struct, an enum has [modes](#concepts): `#[bstack_enum(rc)]` /
`(rc, weak)` make the enum itself refcounted / weak-observable (`new` returns
`BStackRc<E>`), and such an enum can be a `#[bstack_strong]` / `#[bstack_weak]`
field of a struct. [`bstack_move!`](#moving-out-bstack_move) and
[`bstack_cast!`](#casting-bstack_cast) work as on structs — moving frees the enum
shell and hands the active variant out through `NodeData`:

```rust
match bstack_move!(node, &alloc)? {
    NodeData::Child(owned) => { /* owned: BStackOwned<Leaf> — you now own it */ }
    _ => {}
}
```

An enum is a block, so it is **always referenced** — store it as a struct field
(inline embedding isn't supported). A POD aggregate variant's fields must all be
`Pod`, and it takes no ownership annotation (only a single-field tuple variant
does). Duplicate discriminant values are a clear compile error (rustc's `E0081`
can't fire, since the macro replaces the `enum`).

#### Discriminant width

The discriminant defaults to the **smallest integer** that fits every variant's
value — honoring explicit `= value` discriminants (Rust's rules: explicit, else
previous + 1), and choosing a **signed** type if any value is negative. So a
plain enum is a `u8`; `enum S { Ok = 200, NotFound = 404 }` widens to `u16`;
`enum T { Freezing = -40, .. }` becomes `i8`.

Pin it with `repr(..)` — `#[bstack_enum(repr(u16))]` (any of
`u8|u16|u32|u64|i8|i16|i32|i64`; `usize`/`isize` are rejected, since bstack
offsets are 64-bit). `repr(aligned)` is `repr(u64)`: the 8-byte discriminant
leaves the payload **8-aligned**, so a variant's on-disk `u64` ref gets aligned
(single-I/O) writes.

Enums take the same tag controls as structs: `tag = "…"`, `ctrl_tag = "…"` (for
`(rc, weak)`), and `allow(overlong_tag)` — e.g.
`#[bstack_enum(repr(u64), rc, weak, tag = "NODE")]`.

### Field types

`Vec<T>`, `String`, and `Option<…>` in a field are **recognized spellings**, not
the `std` types — the macro lowers each to a bstack_raii on-disk form (a growable
[vector descriptor](#vectors-and-strings), a nullable offset, …). Nothing on disk
is ever an actual `std::vec::Vec` / `String` / `Option`; they're borrowed as
familiar names for convenience.

A **POD tuple** field — `a: (A, B, …)` where every element is `Pod` — also works,
even though a Rust tuple isn't itself `Pod`: it's stored through a generated
packed wrapper (alignment is irrelevant on disk) and handed back as a tuple by
the accessor. `bstack_move!` keeps each tuple as **one** element — a `(u8, u8)`
field comes back as `(u8, u8)`, not flattened into the surrounding tuple.

In the same spirit, a field written `&T` is coerced to owned `T` (and `&str` to
`String`) with a compile warning — a stray reference doesn't fail to compile, but
you're nudged to write the owned type. Silence it with
`#[bstack_block(allow(coerced_ref))]`.

## Moving out: `bstack_move!`

`bstack_move!` destructures a handle, transferring each field/variant out and
freeing only the parent *shell* — the children stay live on disk, now owned
independently.

On a **`BStackOwned<X>`** it is infallible. Because a bare owned handle carries
no allocator, pass one — `bstack_move!(owned, &alloc)` (symmetric with
`owned.bstack_drop(&alloc)`):

```rust
let pair: BStackOwned<Pair> = /* … */;
let (left, shared, right) = bstack_move!(pair, &alloc)?;
//   ^BStackOwned<Leaf>  ^BStackRc<Thing>  ^u32
```

On a **`BStackRc<X>`** (an `(rc)` / `(rc, weak)` block) it is a `try_unwrap`:
success only when this is the **sole strong owner** (atomic `strong: 1 → 0`),
else it hands the handle back. A weak observer doesn't block it. An
allocator-carrying handle — a `BStackRc`, or a `BStackOwned` wrapped as
`owned.auto(&alloc)` — takes the single-argument form:

```rust
match bstack_move!(rc)? {
    Ok((left, shared, right)) => { /* we were the only owner */ }
    Err(rc)                    => { /* someone else still holds it */ }
}
```

An [enum](#enums-bstack_enum) moves out through its `EData` companion instead of
a tuple.

> **Moved-out values are unrooted.** A handle from `bstack_move!` — like one from
> `X::new` — is detached from any persistent structure. Its block still lives on
> disk, but it is reachable *only* through your in-memory handle: drop it without
> re-attaching it (into another block's field) or freeing it and it becomes
> unreachable garbage. Persistence comes from being reachable through a struct.

## Casting: `bstack_cast!`

Convert between typed handles and the untyped `bstack` primitives. Upcasts are
infallible; downcasts check the block's [tag](#type-tags-eightcc). Because a
function-like macro can't read a `let x: T = …` annotation, the target is given
explicitly with `as`:

```rust
use bstack_raii::{BStackCastAs, BStackCastInto};   // the cast methods

let owned: BStackOwned<Node> = /* … */;
let slice = bstack_cast!(owned.auto(&alloc) as BStackOwnedSlice);   // owned upcast

match bstack_cast!(slice as BStackOwned<Node, _>)? {                // owned downcast
    Ok(node)   => { /* tag matched */ }
    Err(slice) => { /* tag mismatch — slice handed back */ }
}

let view = node.handle().as_slice(stack);                           // borrowed upcast
let maybe: Option<Node> = bstack_cast!(view as Node)?;              // borrowed downcast
```

The equivalent methods (`into_slice`, `cast_into::<T>`, `cast_as::<T>`,
`as_slice`) can also be called directly. Casting works the same for enums.

## Type tags (`EightCC`)

Each block's header carries an 8-byte tag — the discriminant a
[downcast](#casting-bstack_cast) checks. It's computed at compile time, not
random, so it's worth knowing how the 8 bytes are laid out: a short **readable
prefix** followed by a **hash tail**.

1. **Prefix** — derived from the type name. For a multi-word camel-case name, the
   uppercased word initials (`OrderLine` → `OL`); for a single word, its
   de-voweled uppercase (`Session` → `SSSN`, clamped). It's 2–5 bytes.
2. **Hash** — a 64-bit **FNV-1a** hash of `crate_name ++ "\0" ++ type_name`,
   little-endian. Every byte then has its **high bit set** (`| 0x80`), pushing it
   into the non-printable range so it can't be mistaken for prefix text.
3. **Overlay** — the prefix bytes overwrite the low bytes of the hash from the
   front; the remaining high bytes are the hash tail.

So a hex dump reads as a recognizable prefix followed by clearly-not-a-name
bytes (every hash byte ≥ `0x80`), e.g. `O L 8B C2 A9 F0 BD 91`. The hash keeps distinct types apart even
when their prefixes collide, and — being pure and deterministic — the tag is
stable across builds and versions, safe to treat as on-disk ABI. Override the
prefix for a documented, fixed tag (0–8 bytes; fewer than 8 leaves room for the
hash, exactly 8 is fully manual):

```rust
#[bstack_block(rc, tag = "ORDLINE")]        // explicit data tag
struct OrderLine { /* … */ }
```

`ctrl_tag = "…"` overrides the control-block tag (default: the data tag,
lowercased). An override longer than 8 bytes is truncated with a compile warning;
`#[bstack_block(allow(overlong_tag))]` silences it (as does `allow(coerced_ref)`
for the coercion warning, or a real `#[allow(deprecated)]` on the item).

This also works for `#[bstack_enum]` — e.g. `#[bstack_enum(rc, tag = "ENMTAG")] enum Mode { Unit, Val(u32) }`.

## Limitations

- **Fixed-size block payloads** — no *inline* variable-length arrays. Growable
  data lives out-of-line via an inline descriptor: `Vec<T>` / `String`,
  `#[bstack_owned/strong/weak/ref] Vec<Thing>`, and their `Option<…>` forms.
- **Requires a freeing allocator** that reserves offset 0 — not
  `LinearBStackAllocator` (see [Concepts](#concepts)).
- **No generic block types**; non-`Pod` fields must carry an annotation.
- **Enums** support unit / POD / all four annotated variant kinds in all three
  modes, plus `bstack_move!` / `bstack_cast!`; struct and multi-field tuple
  variants aren't supported.
- The on-disk **ABI is not yet stable**.

## License

MIT (same as `bstack`).

[`bstack`]: https://github.com/williamwutq/bstack
[`std::io::Result`]: https://doc.rust-lang.org/std/io/type.Result.html
[`TryClone`]: src/clone.rs
[`BStackVec<T>`]: src/vec.rs
