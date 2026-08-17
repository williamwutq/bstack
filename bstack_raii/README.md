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
  - [Fixed-size arrays: `[T; N]`](#fixed-size-arrays-t-n)
  - [Nullable fields: `Option`](#nullable-fields-option)
  - [Enums: `#[bstack_enum]`](#enums-bstack_enum)
  - [Field types](#field-types)
  - [Generic blocks](#generic-blocks)
- [Mutating fields: `#[bstack_mut]`](#mutating-fields-bstack_mut)
- [Moving out: `bstack_move!`](#moving-out-bstack_move)
- [Cloning: `TryCloneIn` / `TryClone`](#cloning-tryclonein--tryclone)
- [Casting: `bstack_cast!`](#casting-bstack_cast)
- [Cross-file pointers: `Foreign<T>`](#cross-file-pointers-foreignt)
- [Type tags (`EightCC`)](#type-tags-eightcc)
- [Standard library collections](#standard-library-collections)
- [Examples](#examples)
- [Limitations](#limitations)
- [Error codes](#error-codes)

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
    let cfg = session.handle().get_config(stack)?;      // -> a Config handle
    println!("v{} flags {:#b}", cfg.get_version(stack)?, cfg.get_flags(stack)?);

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

> **The allocator bound: [`BStackRaiiAllocator`].** Every operation in this crate
> — constructors, `try_clone_in`, `bstack_drop`, the stdlib collections — is
> generic over [`BStackRaiiAllocator`], the crate's front-door allocator
> capability. It is an `unsafe` trait over a freeing `bstack` allocator asserting
> the **null niche**: offset 0 is never handed out, so a `0` offset reads as
> "none" everywhere in the layer (the [`Option`](#nullable-fields-option) niche, a
> dead weak reference, an absent [`Foreign`](#cross-file-pointers-foreignt), …). It
> also exposes an *optional* WAL anchor — a stable reserved slot that lets teardown
> and clone automatically reclaim crash-orphaned allocations on the next open.
>
> **Every bstack-provided allocator implements it** — `FirstFitBStackAllocator`,
> `SlabBStackAllocator`, `GhostTreeBstackAllocator`, … — and a custom allocator
> that upholds the null niche opts in with a one-line
> `unsafe impl BStackRaiiAllocator for MyAlloc {}` (the anchor defaults to `None`).
> **Not `LinearBStackAllocator`**: its `dealloc` is a no-op (teardown would free
> nothing) and it can hand out offset 0, so it does *not* implement the trait. For
> growable fields, use a **realloc-safe** allocator (growth reallocates the backing
> block); `FirstFitBStackAllocator` is realloc-safe.
>
> **The WAL anchor in practice.** Two items are public because a caller may need
> them; everything else in the WAL's internal transaction log is crate-private.
> Call [`wal::finish`]`(&allocator)` once after `open` to complete any transaction
> a prior crash left in flight (reclaiming the slices it orphaned) —
> `try_clone_in` and `bstack_drop` also call it opportunistically, so this is a
> deterministic point to do it, not the only one. [`STD_WAL_ANCHOR`] is the anchor
> offset every bstack-provided allocator reserves; a custom `wal_anchor()` impl
> returns it (or its own, if it reserves a different user region).

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
let value = owned.handle().get_tag(stack)?;   // read a field (or `owned.get_tag(stack)?` via Deref)

owned.bstack_drop(&alloc)?;               // free it now, explicitly …
// … or: let _guard = owned.auto(&alloc); // RAII — freed when `_guard` drops
```

Shared handles (`BStackRc` / `BStackWeak`) *do* manage their counts on `Drop`,
like `std::rc`. Because duplicating one bumps an on-disk counter (fallible I/O),
cloning is the [`TryClone`] trait, not `Clone`. `BStackRc` also derefs to `X`
(`rc.get_field(stack)?`, no `.handle()` needed), same as `BStackOwned`; a
`BStackWeak` doesn't — like `std::rc::Weak`, it may not observe a live block, so
`upgrade` to a `BStackRc` first.

## Generated types

Each macro generates a small, fixed set of types (for a block named `X` / `E`):

| Source                               | Types generated                                                                                                                                                |
|--------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `#[bstack_block] struct X`           | `X` — the [handle](#handles--lifetimes); `XOnDisk` — the `#[repr(C, packed)]` on-disk payload                                                                  |
| `#[bstack_block(rc, weak)] struct X` | the above, plus `XOnDiskRef` — the [control block](#how-it-works-on-disk) (`strong`/`weak` counters)                                                           |
| `#[bstack_enum] enum E`              | `E`, `EOnDisk` (plus `EOnDiskRef` for `(rc, weak)`), and two companion enums — see [Enums](#enums-bstack_enum): `EData` (owned form) and `EView` (read result) |

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

| Annotation         | Child kind required    | On teardown                         | `bstack_move!` yields       |
|--------------------|------------------------|-------------------------------------|-----------------------------|
| `#[bstack_owned]`  | any block              | recursively frees the child         | `BStackOwned<T>`            |
| `#[embed]`         | any block              | frees the child's children in place | `BStackOwned<T>` (re-homed) |
| `#[bstack_strong]` | `(rc)` or `(rc, weak)` | decrements refcount; frees at zero  | `BStackRc<T>`               |
| `#[bstack_weak]`   | `(rc, weak)`           | decrements weak count only          | `Option<BStackWeak<T>>`     |
| `#[bstack_ref]`    | any block              | nothing                             | `BStackRef<T>`              |
| *(none)* — POD     | `Pod` type             | nothing (inline)                    | the value                   |

Rules are enforced at compile time: a `#[bstack_weak]` field whose target isn't
`(rc, weak)`, or a non-`Pod` field with no annotation, is a compile error.

#### `#[embed]` — inline a child block

`#[bstack_owned]` stores a `u64` **offset** to a separately-allocated child.
`#[embed]` instead stores the child's *whole on-disk form* — its header and all —
**inline** in the parent, so the parent block is one contiguous region and the
child needs no separate allocation:

```text
#[bstack_owned]:  [ parent header ][ .. u64 offset .. ] ─▶ [ child header ][ child fields ]
#[embed]:         [ parent header ][ child header ][ child fields ][ .. ]
```

```rust
#[bstack_block]
struct Holder {
    #[embed] child: Child,   // Child's OnDisk lives here, inline
    tag: u32,
}
enum Wrapper {
    #[embed] One(Child),     // also works as an enum variant
    None,
}
```

It's still **exclusive ownership** (like `#[bstack_owned]`): `new` takes a
`BStackOwned<Child>` — you build the child normally, and the parent folds its
bytes in and frees the child's now-redundant shell (the child's *own* children
stay live). The accessor (`holder.handle().get_child()`) hands back a borrowed
`Child` handle into the inline region; teardown frees the embedded child's
children in place; and `bstack_move!` re-homes the child to a fresh standalone
`BStackOwned<Child>`. You can embed any block (`#[bstack_block]` /
`#[bstack_enum]`), but not a tuple, a `Vec`, or an `Option`.

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
- **accessors** — `node.get_field(stack)` reads each field;
- **mutators** — writing a field is opt-in per
  [`#[bstack_mut]`](#mutating-fields-bstack_mut) (`set_<field>` / `replace_<field>`),
  plus a `set_<field>` for wiring each `#[bstack_weak]`
  [back-pointer](#reference-counted-blocks) after construction;
- recursive teardown, [casting](#casting-bstack_cast), and
  [moving](#moving-out-bstack_move).

A **tuple struct** works too, as long as every field is `Pod`: its positional
fields get synthetic names, so `struct Rgb(u8, u8, u8)` is constructed
`Rgb::new(&alloc, 10, 20, 30)`, read via `rgb.get_field0(stack)?` / `get_field1` / …, and
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

if let Some(a2) = b.handle().get_back(&alloc)? {   // accessor upgrades
    println!("a still alive: {}", a2.handle().get_val(stack)?);
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
let mut tags = rec.handle().get_tags(&alloc)?;    // a BStackVec<u32> handle
tags.push(4)?;                                    // grows; rewrites the inline descriptor
assert_eq!(rec.handle().get_tags(&alloc)?.to_vec()?, vec![1, 2, 3, 4]);
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

A vector's element may itself be a [fixed-size array](#fixed-size-arrays-t-n):
`Vec<[T; N]>` (and nested `Vec<[[T; N]; M]>`) is a growable sequence of
reference-arrays. It works for POD (`Vec<[u16; 4]>`, just a POD vector) and for
every annotated kind (`#[bstack_owned/strong/weak/ref] Vec<[Thing; N]>`), with
the accessor materializing `Vec<[Thing; N]>`.

A `Vec` element must be a single leaf, though: `Vec<Vec<T>>` (nested vectors) and
`Vec<(A, B)>` (a tuple element) are rejected with a directed error — wrap the
inner value in a named `#[bstack_block]` and store `Vec<ThatBlock>`.

To **share** a vector between two structs, wrap it in its own `#[bstack_block]`
and share *that* block with `#[bstack_strong]` / `#[bstack_ref]` — a descriptor
has a single owner.

### Fixed-size arrays: `[T; N]`

A fixed-size array `[T; N]` is stored **inline** — no separate data block. As with
a scalar field, the annotation states the elements' ownership; an un-annotated
array of `Pod` is itself `Pod`.

```rust
#[bstack_block]
struct Board {
    cells: [u16; 9],                    // POD array (un-annotated) — inline bytes
    #[bstack_owned] tiles: [Leaf; 3],   // 3 owned children (freed on teardown)
    #[bstack_ref]   marks: [Leaf; 2],   // 2 borrowed refs (free nothing)
    #[embed]        kids:  [Child; 2],   // 2 children embedded verbatim, inline
}

let b = Board::new(&alloc, [0; 9], [a, b, c], [r0, r1], [k0, k1])?;
let tiles: [Leaf; 3] = b.handle().get_tiles(stack)?;   // an array of block views
```

A reference array stores `[u64; N]` inline (one offset per element). The
constructor takes an array of the matching handle (`[BStackOwned<T>; N]` /
`[BStackRc<T>; N]` / `[BStackRef<T>; N]`) and the accessor hands back `[T; N]`
block views; teardown frees/releases each element per the annotation, exactly
like the vector kinds. `#[bstack_weak]` is wired per index
(`set_field(&alloc, i, weak)`) and its accessor upgrades each slot to
`[Option<BStackRc<T>>; N]`. `#[embed] [Child; N]` stores the N children's on-disk
forms back-to-back.

Arrays compose freely:

- **Per-element `Option`** — `[Option<T>; N]` makes each slot nullable (offset
  `0` == `None`), so the accessor/constructor use `[Option<Handle>; N]`. A
  whole-array `Option<[T; N]>` is rejected — put the `Option` on the element.
- **Nesting to any depth** — `[[T; N]; M]`, `[[[T; N]; M]; K]`, … work for every
  kind (POD / owned / strong / weak / ref / embed), in both structs and enums,
  the accessor/constructor trafficking in the matching nested `[[Handle; …]; …]`.

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
    NodeView::Child(c) => assert_eq!(c.get_val(stack)?, 7),
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

A single-field variant carries the same shapes a struct field does — not just a
scalar block, but a [fixed-size array](#fixed-size-arrays-t-n) or a
[vector](#vectors-and-strings):

```rust
#[bstack_enum]
enum Cell {
    Empty,
    Tags(Vec<u32>),                          // POD vector variant
    Text(String),                            // POD string variant
    #[bstack_owned] Kids(Vec<Leaf>),         // owned vector (freed on teardown)
    #[bstack_ref]   Row([Leaf; 3]),          // inline reference array
    #[bstack_owned] Grid(Vec<[Leaf; 2]>),    // vector of reference-arrays
}
```

An array variant `V([T; N])` mirrors a scalar `V(T)` per element (its offsets
sit inline in the payload); a vector variant `V(Vec<…>)` stores a descriptor in
the payload (build it as a `BStackVec` / `BStackBlockVec` / … and pass it in
`CellData::Kids(vec)`). A `Vec<[T; N]>` variant reads back as `Vec<[T; N]>`. The
same nesting and directed-error rules apply as for struct fields.

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

A fixed-size [array](#fixed-size-arrays-t-n) `[T; N]` is likewise recognized —
inline, per-element ownership, nestable to any depth.

A **POD tuple** field — `a: (A, B, …)` where every element is `Pod` — also works,
even though a Rust tuple isn't itself `Pod`: it's stored through a generated
packed wrapper (alignment is irrelevant on disk) and handed back as a tuple by
the accessor. `bstack_move!` keeps each tuple as **one** element — a `(u8, u8)`
field comes back as `(u8, u8)`, not flattened into the surrounding tuple. A tuple
is *not* a valid `Vec`/array element, though (it can't carry per-element
annotations) — `Vec<(A, B)>` is rejected in favor of a named `#[bstack_block]`.

These spellings **compose**, with two limits that draw a directed compile error
rather than a confusing one: a field takes at most one `Option` layer
(`Option<Option<T>>` → use a `#[bstack_enum]`), and a `Vec` element must be a
single leaf (`Vec<Vec<T>>` / `Vec<String>` → wrap the inner one in a named
`#[bstack_block]` and store `Vec<ThatBlock>`). So `Option<Vec<[Thing; N]>>`,
`[Option<T>; N]`, and `Vec<[[T; N]; M]>` are fine; `Vec<Vec<T>>` and
`Option<Option<T>>` are not.

In the same spirit, a field written `&T` is coerced to owned `T` (and `&str` to
`String`) with a compile warning — a stray reference doesn't fail to compile, but
you're nudged to write the owned type. Silence it with
`#[bstack_block(allow(coerced_ref))]`.

### Generic blocks

A `#[bstack_block]` / `#[bstack_enum]` may be **generic** — over type parameters
(and, for arrays, `const` parameters). Each concrete instantiation is its own
block type, with its own `XOnDisk` layout and its own [type tag](#type-tags-eightcc).

```rust
#[bstack_block]
struct Node<T> {
    #[bstack_owned] child: T,          // owns a child of any block type
    #[bstack_ref]   refs:  [T; 3],     // an array of references to T
    weight: u32,
}

#[bstack_block]
struct Buf<const N: usize> { data: [u16; N], len: u32 }   // const-length POD array

let n = Node::<Leaf>::new(&alloc, leaf, [r0, r1, r2], 5)?;
let b = Buf::<8>::new(&alloc, [0; 8], 0)?;
```

A type parameter works in **every** field shape it makes sense in:

- **Reference kinds** — `#[bstack_owned/strong/weak/ref]` (scalar, `Vec<T>`,
  `[T; N]`, `Vec<[T; N]>`, …): the on-disk form is a bare `u64` offset, so the
  layout is independent of `T`. The parameter is auto-bounded `BStackBlock` (plus
  `BStackShared` / `BStackWeakable` for a `#[bstack_strong]` / `#[bstack_weak]`
  use).
- **Inline kinds** — a POD field (`item: T`, bounded `T: Pod`) or `#[embed] item:
  T` (bounded `BStackBlock`): here `T` is stored *inline*, so `XOnDisk` becomes
  generic over it. A parameter can't be used **both** as POD and as a reference —
  those are incompatible bounds, and the macro says so.
- **`const N`** in an array length — `[T; N]` (single dimension). A nested
  `[[T; N]; M]` with a const dimension is rejected (its flattened length would be
  the const expression `N * M`, which stable Rust bars a generic parameter from);
  make one dimension concrete or use a single array.

Each instantiation folds its arguments into the tag, so
[`bstack_cast!`](#casting-bstack_cast) can't confuse `Node<A>` with `Node<B>`, or
`Buf<8>` with `Buf<16>`. A generic **enum** is supported in the layout-preserving
case (type parameters only in reference variants — a POD/`#[embed]` variant
storing `T` inline would make the payload width depend on it):

```rust
#[bstack_enum]
enum Tree<T> {
    Leaf(u32),
    #[bstack_owned] Branch(T),
}
```

When a *concrete* argument violates a rule the macro couldn't see through the
parameter — instantiating `Node<Vec<u32>>`, say — the failing trait bound carries
a directed message (`` `Vec<u32>` is not a `#[bstack_block]` type … a nested
`Vec`/`Option` or a tuple needs its own named `#[bstack_block]` wrapper``) via
`#[diagnostic::on_unimplemented]`.

Currently unsupported (a clear compile error): lifetime parameters, a generic
block in `rc` / `rc, weak` mode, and const parameters in a generic *enum*.

## Mutating fields: `#[bstack_mut]`

Every scalar field gets a reader (`get_<field>`). *Writing* one is opt-in: mark
it `#[bstack_mut]` and the macro adds the mutator appropriate to its kind. This
keeps immutability the default — a field is read-only unless you say otherwise —
while each generated write is a single crash-atomic `set`.

```rust
#[bstack_block]
struct Counter {
    #[bstack_mut] hits: u64,   // writable
    created_at: u64,           // read-only (no setter generated)
}

let c = Counter::new(&alloc, 0, now)?;
c.handle().set_hits(stack, 42)?;             // atomic overwrite
assert_eq!(c.handle().get_hits(stack)?, 42);
```

The mutator depends on the field's ownership:

| Field kind         | Mutator                              | Semantics                                                                     |
|--------------------|--------------------------------------|-------------------------------------------------------------------------------|
| POD                | `set_<field>(stack, value)`          | overwrite the inline bytes                                                    |
| `#[bstack_ref]`    | `set_<field>(stack, ref)`            | repoint the offset (nullable → `Option<BStackRef>`, `None` writes `0`)        |
| `#[bstack_owned]`  | `replace_<field>(stack, new)`        | install `new`, **return the old** `BStackOwned<T>` (neither leaked nor freed) |
| `#[bstack_strong]` | `replace_<field>(&alloc, new)`       | install `new`, return the old `BStackRc<T>` (dropping it decrements)          |
| `#[bstack_ref]`    | *(also)* `replace_<field>(stack, r)` | ref is the only kind with **both** `set_` and `replace_`                      |

`replace_` is a persistent `mem::replace`: an owned or strong field can't just be
overwritten (that would strand the old child / leak a strong count), so it hands
the old value back for you to reuse or free.

Because it **consumes** the new value, `replace_` returns `Result<Old,
ReplaceError<New>>` (not a bare `io::Result`): on an I/O failure it hands the
consumed value back in `ReplaceError.value`, rather than dropping it into an
unreachable orphan — the same region-hand-back contract as bstack's
`BStackAllocError`. The *old* value is never at risk (the swap is a single atomic
`set`, so on failure the field still holds it). The one `value: None` case is a
strong field whose old handle fails to reconstruct *after* the commit already
landed — then it's the old block that is reclaimable only via crash-recovery.

`#[bstack_weak]` fields already have their own
[`set_<field>`](#reference-counted-blocks) wiring; `#[bstack_mut]` on a weak field
is a no-op, and on an `#[embed]` field a compile error.

### Containers

`#[bstack_mut]` also works on POD tuples and block-reference arrays; a `Vec` is
already mutable and needs no annotation:

| Field shape                          | Mutator(s)                                                                 |
|--------------------------------------|----------------------------------------------------------------------------|
| POD tuple `(A, B, …)`                | `set_<field>(stack, tuple)` — one atomic overwrite (owns no children)       |
| `#[bstack_owned/strong] [T; N]`      | `replace_<field>_at(&alloc, i, new) -> old` and `replace_<field>(&alloc, arr) -> old_arr` |
| `#[bstack_ref] [T; N]`               | *(also)* `set_<field>_at` / `set_<field>` (a ref owns nothing)              |
| `Vec<T>` (any kind)                  | *none needed* — mutate in place via the `get_<field>()` handle (`push_*`, …) |
| `#[bstack_owned/strong/weak] Foreign<T>` | `replace_<field>(&alloc, new) -> old` — moves the old cross-file target out as `ForeignOwned`/`ForeignRc`/`ForeignWeak` |
| `#[bstack_ref] Foreign<T>`           | *(also)* `set_<field>` / `replace_<field>` trafficking in plain `Foreign`   |

Arrays are fixed-size, so there is no push/pop — only in-place change. The element
mutators use a **row-major flat** `index` (for a nested `[[T; M]; N]`, slot `i*M + j`
is `grid[i][j]`). Both `replace_` forms are one crash-atomic `set` — a single 8-byte
slot for `_at`, the whole inline `[u64; N]` region for the whole-array form — and
uphold the same `ReplaceError` hand-back contract as the scalar `replace_`. A `Vec`
persists its descriptor back to the field on every mutation, so mutating the handle
its accessor returns *is* mutating the field; `#[bstack_mut]` on a `Vec` is an
accepted no-op.

A **scalar `Foreign<T>` / `Option<Foreign<T>>`** field is mutable, and its swap is
notably *purely local* — one crash-atomic 16-byte `ForeignRepr` write — because the
cross-file responsibility travels with the returned handle: `replace_` hands the old
target back as its RAII dual, which you later `bstack_drop(&home)` (freeing it in its
own file) or re-store. So `replace_` needs no registry / host access and works even
if the target file is detached. Owning kinds get **only** `replace_` (a bare `set_`
would strand the old cross-file target); a foreign `ref` also gets `set_`. `Foreign`
inside a container or tuple (`Vec<Foreign>`, `[Foreign; N]`, a foreign tuple) has no
mutator yet (`#[bstack_mut]` there is a compile error).

### Enums

An enum's payload has no stable "field" to set — its meaning depends on the active
variant — so `#[bstack_mut]` on the **enum itself** generates a *whole-value*
mutator that overwrites the discriminant + payload together, as one crash-atomic
`set`:

```rust
#[bstack_enum]
#[bstack_mut]
enum State { Idle, Active(u32), #[bstack_owned] Holding(Child) }

// owns nothing → `set` (wholesale overwrite):
// e.handle().set(&alloc, StateData::Active(7))?;
// owns children → `replace` (moves the old value out, ReplaceError contract):
let old = e.handle().replace(&alloc, StateData::Idle)?; // old: StateData::Holding(BStackOwned<Child>)
```

`set` is generated when no variant owns anything (pure POD / `ref` / foreign-`ref`);
`replace` when some variant owns children (owned / strong / weak / foreign) — it
hands the old value back as the enum's owned form (`EData`), so nothing is stranded.
`#[bstack_mut]` on a **variant** is an error (it goes on the enum), as is
`#[bstack_mut]` on a shared (`rc` / `rc, weak`) enum or one with an `#[embed]`
variant.

There is also a raw escape hatch on **every** scalar field —
`unsafe fn raw_<field>_slice(stack) -> BStackSlice` — a view over the field's
inline storage (`.read()` / `.write()`). Reads are always valid; writing bypasses
the typed invariants, hence `unsafe`.

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

## Cloning: `TryCloneIn` / `TryClone`

Duplicating a handle means one of two things, depending on whether the block is
uniquely owned or shared.

### Deep-clone an owned block: `TryCloneIn`

A plain `#[bstack_block]` / `#[bstack_enum]` implements `TryCloneIn`, a **deep**,
fallible clone into a fresh, independent `BStackOwned<Self>`:

```rust
use bstack_raii::TryCloneIn;

let copy: BStackOwned<Node> = node.try_clone_in(&alloc)?;
```

Each field is duplicated according to its ownership — the mirror of teardown:

| Field                 | On clone                                                                                                               |
|-----------------------|------------------------------------------------------------------------------------------------------------------------|
| POD / `#[bstack_ref]` | byte-copied (a ref clone **aliases** the same target)                                                                  |
| `#[bstack_owned]`     | the child is recursively deep-cloned into a fresh block                                                                |
| `#[embed]`            | the inline child is folded — its own children deep-cloned in place                                                     |
| `#[bstack_strong]`    | the shared child stays shared; its strong count is bumped                                                              |
| `#[bstack_weak]`      | stays weak to the same target; its weak count is bumped                                                                |
| `Vec<Thing>`          | per element, by the vector's annotation (POD data copied; owned elements deep-cloned; strong/weak bumped; ref aliased) |

So an owned subtree is copied into independent storage while shared children are
*re-referenced* rather than duplicated: freeing the clone never disturbs the
original's owned data, and a shared target stays live as long as either handle
holds it.

> **Atomicity & crash-safety.** A clone allocates the whole new subtree up front,
> then commits every payload write *and* refcount bump as one crash-atomic batch
> (`BStack::inplace_gen`): a mid-clone allocation failure rolls back with nothing
> written, and a crash never leaves a torn copy. When the allocator names a WAL
> anchor, the fresh allocations are logged as they are made, so a crash *mid-clone*
> is reclaimed on the next open rather than leaked (down to a one-block window) —
> you don't opt in, and [`wal::finish`] completes it deterministically after `open`.
> On a bulk-capable allocator (one that also implements `BStackBulkAllocator`, such
> as `GhostTreeBstackAllocator`) the whole subtree is allocated in a single atomic
> `alloc_bulk` instead of block by block.

### Duplicate a shared handle: `TryClone`

A shared block is **not** deep-cloned. `BStackRc` / `BStackWeak` implement
`TryClone`, whose `try_clone` bumps the on-disk refcount and hands back another
handle to the *same* block — exactly like `Rc::clone` / `shared_ptr`:

```rust
use bstack_raii::TryClone;

let rc2 = rc.try_clone()?;      // another strong owner of the same block
let weak2 = weak.try_clone()?;  // another weak observer of the same block
```

An `(rc)` / `(rc, weak)` block therefore has no `try_clone_in` — calling it is a
compile error. This is deliberate: sharing, not copying, is what a reference
count *means*. It is clearest for a **weak** reference, which has no coherent deep
copy at all: a weak reference observes a live object's control block, and a "copy"
would either point at the same object (just another weak handle — a count bump) or
at some other object (observing nothing the original did — not a copy). So a weak
clone can only ever be another weak reference to the same target.

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

## Cross-file pointers: `Foreign<T>`

Every reference covered so far points *within one file*. A `Foreign<T>` crosses
the boundary: it is a **wide pointer** naming both a target **file** and an
offset inside it, so an object graph can span many `bstack` files — a sharded
store, an index file pointing at a data file, cross-document links — while each
file stays an independent, crash-safe unit.

### The file registry

Paths are long and awkward to store on disk, so a process-wide **registry** maps
each file's persistent path ↔ a small, stable numeric [`FileId`]. A `Foreign`
stores `(FileId, offset)`; the id is resolved to a live file through the
registry. It is entirely opt-in — a single-file program never touches it and pays
nothing.

```rust
use bstack_raii::registry;

registry::init("registry.bstack")?;                  // once, at startup
let store_id = registry::attach("store.bstack", store_alloc)?;  // hand a file to the registry
```

`init` brings up the registry (itself a tiny append-only `bstack` file mapping
paths to ids, so ids survive a restart). `attach` registers a file's path and
installs its allocator as the **live host** for that id — the thing a `Foreign`
into that file resolves through. The host is shared process-wide, so `attach`
takes a [`SyncBStackRaiiAllocator`](src/registry.rs) (a
[`BStackRaiiAllocator`] that is also `Send + Sync`) — every bstack allocator
qualifies. `FileId::SELF` (id `0`) is the current file, resolved against your
local allocator with no registry lookup at all.

### Declaring a foreign field

A `Foreign<T>` field **must** carry an [ownership annotation](#field-ownership),
exactly like an in-file reference — it just means the same thing *across* files.
The target `T` must be a `#[bstack_block]` (a foreign pointer targets a block,
never inline data, so an un-annotated / POD / `#[embed]` `Foreign` is a compile
error):

```rust
#[bstack_block]
struct Card {
    title: String,
    #[bstack_owned] body: Foreign<Document>,   // owns a Document in another file
}

// Construct with an explicit (file, offset) pointer. `Foreign::new` is `unsafe`
// (it asserts the offset names a valid `T`); the safe ways to obtain one are
// `bstack_cast!(slice as Foreign<T>)`, `Foreign::at(file, &handle)`, or simply
// reading a field.
let ptr = unsafe { Foreign::<Document>::new(store_id, doc_off) };
let card = Card::new(&catalog, "report", ptr)?;

// … and resolve it to read across the boundary. `with` runs a closure against
// the target and *its* file's stack: `Ok(None)` for a null pointer, `Err` if
// that file isn't currently live — the two failure modes are kept apart rather
// than conflated into one `Option`.
let size = card.handle().get_body(catalog.stack())?
    .with(&catalog, |doc, fs| doc.get_size(fs).unwrap())?  // io::Result<Option<u64>>
    .expect("owned Foreign is never null");
```

`Foreign<'a, T>` is a 16-byte, zero-cost enum: an **explicit** pointer (a real
`FileId` + offset, registry-resolved, borrow-free) or a [`SELF`] pointer (an offset
in the current file). A field accessor returns `Foreign<'a, T>` with `'a` **bound to
the `&'a BStack` it read through**, so a `SELF` pointer can never be stored into — or
outlive — the file it came from; an explicit pointer ignores the borrow and can be
[`detach`]ed to a `'static`, freely-movable `Foreign` (a `SELF` one cannot detach).
The `NonZeroU64` file-id niche encodes the explicit/`SELF` tag for free, so the
in-memory form is exactly the on-disk wire size.

The annotation decides what teardown and clone do **in the target's own file**:

| Annotation         | Cross-file teardown                       | Cross-file clone                                  | `bstack_move!` yields    |
|--------------------|-------------------------------------------|---------------------------------------------------|--------------------------|
| `#[bstack_owned]`  | frees the target in its file              | deep-clones it into a fresh block in that file    | `ForeignOwned<T>`        |
| `#[bstack_strong]` | decrements its refcount there (free at 0) | bumps its refcount there (stays shared)           | `ForeignRc<T>`           |
| `#[bstack_weak]`   | decrements its weak count there           | bumps its weak count there                        | `ForeignWeak<T>`         |
| `#[bstack_ref]`    | nothing                                   | byte-copies the pointer (aliases the same target) | `Foreign<T>`             |

So tearing down a `Card` reclaims its `Document` in the store file, and
deep-cloning a `Card` gives the copy its own independent `Document` there — the
catalog file never touches the store's bytes directly. (`#[bstack_owned]` needs a
deep-cloneable target, so `#[bstack_owned] Foreign<SharedBlock>` — a target that
is itself `(rc)` — is a compile error; use `#[bstack_strong]`.)

Moving an owning foreign field out with `bstack_move!` hands back its **RAII dual** —
`ForeignOwned` / `ForeignRc` / `ForeignWeak`, the cross-file analogues of
`BStackOwned` / `BStackRc` / `BStackWeak`. Each is non-`Copy` and carries
`.bstack_drop(&home)` (frees the target in its own file, resolved through the
registry) and `.into_foreign()` (relinquish ownership, handing back a plain `Foreign`
to re-store into another owning field). A `#[bstack_ref]` field moves out as a plain
`Foreign` (it owns nothing). Each also **resolves to its in-file handle** with
`.into_local(..)` in the target's own file — `ForeignOwned::into_local() →
BStackOwned<T>`, `ForeignRc::into_local(&target) → BStackRc<T>`,
`ForeignWeak::into_local(&target) → BStackWeak<T>` (rc/weak take the target file's
allocator, since those handles carry one) — the owning analogues of
`bstack_cast!(foreign as BStackRef<T>)`. Like `BStackOwned`, these don't free on `Drop` — a
forgotten handle leaks the target, exactly as in-file. *(Currently wired for scalar
foreign fields; an owning `Foreign` inside a `Vec`/array/tuple/enum still moves out as
a bare `Foreign`.)*

> **Nullable & atomicity.** `Option<Foreign<T>>` is nullable on the usual offset-0
> niche. Cross-file operations are *best-effort atomic*: the far side is committed
> before the home side, so a mid-op failure errs toward an over-provision (a
> leaked block or an over-count — reclaimable) and never an under-count (a
> premature free). If the target file is detached, teardown leaks (never
> corrupts) and a clone returns an error rather than aliasing an owner.

### Containers and shapes

A `Foreign<T>` composes everywhere an in-file reference does — its inert 16-byte
wire form (`ForeignRepr`) is `Pod`, so the container storage is reused and only the
per-element cross-file dispatch (and the read-side lifetime binding) is added:

```rust
#[bstack_owned] parts: Vec<Foreign<Document>>,          // a growable list of pointers
#[bstack_owned] shards: [Foreign<Document>; 4],          // an inline fixed array
#[bstack_ref]   pair: (u32, Foreign<Document>),          // a foreign element in a tuple
```

`Vec<Option<Foreign<T>>>`, nested arrays, and generic targets (`Foreign<T>` over a
type parameter) all work, in both struct fields and `#[bstack_enum]` variants —
scalar, `Vec`, array, and tuple variants alike.

The one firm rule is **no double pointer**: a `Foreign` must target a plain block,
not another pointer or a container — `Foreign<Vec<T>>`, `Foreign<Foreign<T>>`,
`Foreign<[T; N]>`, `Foreign<(A, B)>` are rejected with a directed error (bridge
through a named `#[bstack_block]`). This is distinct from the `Vec<Vec>` nesting
rule: a *collection of pointers* (`Vec<Foreign<T>>`) is fine; only a *pointer to a
collection* (`Foreign<Vec<T>>`) is barred.

Finally, [`bstack_cast!`](#casting-bstack_cast) bridges a `Foreign` and a local
handle: `slice as Foreign<T>` tags a local slice with its file identity (via the
reverse registry map), and `foreign as BStackRef<T>` recovers a same-file
reference when the target is local. Both return `Option` (no I/O).

A full two-file walk-through — resolution, cross-file ownership, deep clone, and
reclamation — is in [`examples/crossfile.rs`](examples/crossfile.rs):
`cargo run --example crossfile`.

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

## Standard library collections

Built entirely on the primitives above — nothing here has privileged access, so
each type doubles as a worked example of composing the ownership model. Every
collection is itself a plain [`BStackBlock`] (`BStackDrop` + `TryCloneIn`), so it
can be used bare — a top-level `BStackOwned<...>`, freed with `bstack_drop` — or
composed as a `#[bstack_owned]` field inside another block, nested inside
another collection, or held in a `#[bstack_enum]` variant.

| Type                              | Rust analogue                    | What it holds |
|------------------------------------|-----------------------------------|----------------|
| [`BStackCow<T>`]                  | `std::borrow::Cow`                | either a borrowed reference or an owned block, deep-copying on first write. |
| [`BStackBox<T>`]                  | `std::boxed::Box`                 | a single owned `Pod` value in its own block — the macro-free way to own a bare scalar/POD struct. |
| [`BStackLinkedList<T>`]           | `std::collections::LinkedList`    | an owned doubly-linked list of block values. Prefer `BStackDeque` / `BStackBlockVec` unless you need O(1) end/splice ops. |
| [`BStackDeque<T>`]                | `std::collections::VecDeque`      | an owned double-ended queue: a contiguous ring, O(1) amortized push/pop at both ends. |
| [`BStackHashMap<K, V>`]           | `std::collections::HashMap`       | an owned open-addressing map from a `Pod` key to a block value. |
| [`BStackBTreeMap<K, V>`]          | `std::collections::BTreeMap`      | an owned **ordered** map (copy-on-write B-tree) with sorted iteration. Keys are `Pod + Ord`. |
| [`BStackString`]                  | `std::string::String`             | a standalone owned, growable UTF-8 string block — the first-class way to own text (a deque element, a map value). |
| [`BStackCountingBloomFilter<K>`]  | (Bloom filter)                    | a probabilistic set: no false negatives, supports removal — a cheap fast-reject front for exact lookups. |
| [`BStackHashSet<K>`]              | `std::collections::HashSet`       | an owned open-addressing set of `Pod` keys, with an embedded Bloom-filter fast-reject front. |
| [`BStackBTreeSet<K>`]             | `std::collections::BTreeSet`      | an owned **ordered** set (copy-on-write B-tree), with an embedded Bloom-filter front. Keys are `Pod + Ord`. |
| [`BStackBinaryHeap<K, V>`]        | `std::collections::BinaryHeap`    | an owned priority queue (array-backed binary **min**-heap): `pop` returns the smallest-key entry. Keys are `Pod + Ord`. |

Each is constructed with `new` (or `with_capacity` where it applies), torn down
with `bstack_drop`, and deep-cloned with `try_clone_in` — same as any other
owned handle:

```rust
use bstack_raii::{BStackDrop, BStackHashMap, BStackString};

let map = BStackHashMap::<u32, BStackString>::new(&alloc)?;
map.insert(&alloc, 1, BStackString::new(&alloc, "one")?)?;
map.insert(&alloc, 2, BStackString::new(&alloc, "two")?)?;

let v = map.get(alloc.stack(), &1)?.unwrap();  // -> a BStackString handle
assert_eq!(v.to_string(alloc.stack())?, "one");

map.bstack_drop(&alloc)?;  // frees the map AND every owned BStackString value
```

Composing one into a block field works like any other owned type — deep clone
and teardown recurse through it automatically:

```rust
#[bstack_block]
struct Session {
    id: u64,
    #[bstack_owned]
    log: BStackDeque<BStackString>,
}
```

Each collection's iterator (`HashMapIter`, `DequeIter`, `ListIter`,
`BTreeMapIter`, `BTreeSetIter`, `HashSetIter`, …) borrows the allocator's
[`BStack`] and yields owned element handles — see the type's own docs for the
exact borrow shape.

## Examples

Runnable end-to-end programs live in [`examples/`](examples/):

| Example                                 | Run                             | Shows                                                                                                          |
|-----------------------------------------|---------------------------------|----------------------------------------------------------------------------------------------------------------|
| [`sessions.rs`](examples/sessions.rs)   | `cargo run --example sessions`  | shared `(rc, weak)` ownership, refcount-driven cleanup, durability across a reopen                             |
| [`expr.rs`](examples/expr.rs)           | `cargo run --example expr`      | a recursive `#[bstack_enum]` tree — evaluation, deep clone (`TryCloneIn`), `bstack_move!`                      |
| [`crossfile.rs`](examples/crossfile.rs) | `cargo run --example crossfile` | [`Foreign<T>`](#cross-file-pointers-foreignt) across two files — resolution, cross-file ownership, reclamation |

## Limitations

- **Fixed-size block payloads.** Fixed-size [arrays](#fixed-size-arrays-t-n)
  `[T; N]` (nested to any depth) are stored *inline*, but a *variable-length*
  sequence lives out-of-line via an inline descriptor: `Vec<T>` / `String`,
  `#[bstack_owned/strong/weak/ref] Vec<Thing>`, `Vec<[Thing; N]>`, and their
  `Option<…>` forms.
- **Requires a [`BStackRaiiAllocator`]** — a freeing allocator that reserves
  offset 0 (the null niche); not `LinearBStackAllocator` (see
  [Concepts](#concepts)).
- **[Generic blocks](#generic-blocks)** work over type parameters (in every field
  kind — reference, POD, and `#[embed]`) and `const` array lengths; the exceptions
  are lifetime parameters, `rc` / `rc, weak` mode, and const parameters in a
  generic enum. Non-`Pod` fields must still carry an annotation.
- **`Vec` / `Option` nesting** is capped at a single leaf / one `Option` layer
  (see [Field types](#field-types)); deeper nesting or a tuple element must be
  named as a `#[bstack_block]` / `#[bstack_enum]`.
- **Enums** support unit / POD / all four annotated variant kinds — as scalars,
  arrays `V([T; N])`, and vectors `V(Vec<…>)` — in all three modes, plus
  `bstack_move!` / `bstack_cast!`; struct and multi-field tuple variants aren't
  supported, and a variant can't be `#[embed]`ed.
- **[Cross-file pointers](#cross-file-pointers-foreignt)** (`Foreign<T>`) must
  target a plain block, never a pointer or a container (no "double pointer"), and
  their cross-file operations are *best-effort atomic* — a failure over-provisions
  (a reclaimable leak) rather than under-counts. Resolution requires the target
  file to be `attach`ed to the process registry.
- **[Standard library collections](#standard-library-collections)** can't be
  shared (`#[bstack_strong]` / `#[bstack_weak]`) — they aren't `(rc)` /
  `(rc, weak)` blocks, so two structs can't share one collection the way they
  share an `rc` block. `bstack_move!` only works on `BStackBox`; the others have
  no meaningful field-destructure.
- The on-disk **ABI is not yet stable**.

## Error codes

Every compile-time error from the `#[bstack_block]`, `#[bstack_enum]`,
`bstack_move!`, and `bstack_cast!` macros carries a stable `[BSTACKxxxx]` code, e.g.:

```text
error: [BSTACK0301] `Foreign` is a pointer and cannot be `#[embed]`ed
```

The message states the fix inline; **[ERRORS.md](ERRORS.md)** is the full reference,
with one entry per code (grouped by domain — attributes `00xx`, field shapes `01xx`,
enums `02xx`, `Foreign` `03xx`, generics `04xx`, `#[embed]` `05xx`, `#[bstack_mut]`
`06xx`, cast/move macros `07xx`).

## License

MIT (same as `bstack`).

[`bstack`]: https://github.com/williamwutq/bstack
[`std::io::Result`]: https://doc.rust-lang.org/std/io/type.Result.html
[`TryClone`]: src/clone.rs
[`BStackVec<T>`]: src/vec.rs
[`FileId`]: src/registry.rs
[`BStackRaiiAllocator`]: src/lib.rs
[`BStackBlock`]: src/block.rs
[`wal::finish`]: src/wal.rs
[`STD_WAL_ANCHOR`]: src/wal.rs
[`BStack`]: https://docs.rs/bstack
[`BStackCow<T>`]: src/stdlib/cow.rs
[`BStackBox<T>`]: src/stdlib/boxed.rs
[`BStackLinkedList<T>`]: src/stdlib/list.rs
[`BStackDeque<T>`]: src/stdlib/deque.rs
[`BStackHashMap<K, V>`]: src/stdlib/map.rs
[`BStackBTreeMap<K, V>`]: src/stdlib/tree.rs
[`BStackString`]: src/stdlib/string.rs
[`BStackCountingBloomFilter<K>`]: src/stdlib/bloom.rs
[`BStackHashSet<K>`]: src/stdlib/hashset.rs
[`BStackBTreeSet<K>`]: src/stdlib/btreeset.rs
[`BStackBinaryHeap<K, V>`]: src/stdlib/heap.rs
