# bstack_raii

Typed, RAII-style ownership for persistent objects — `Rc`/`Weak` semantics that
survive a process restart or crash, backed by a single [`bstack`] file.

`std::rc::Rc` and `Weak` live and die with the process. `bstack_raii` gives you
the same model — shared strong handles, non-owning weak handles, and automatic
cleanup when the last owner drops — but the object graph *and its reference
counts* are stored on disk, crash-safely. You define blocks as ordinary structs;
the `#[bstack_block]` macro generates the on-disk layout, typed accessors,
constructors, recursive teardown, and refcounting.

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
- [Defining blocks](#defining-blocks)
- [Field ownership](#field-ownership)
- [Enums: `#[bstack_enum]`](#enums-bstack_enum)
- [Handles & lifetimes](#handles--lifetimes)
- [Shared ownership & weak references](#shared-ownership--weak-references)
- [Moving fields out: `bstack_move!`](#moving-fields-out-bstack_move)
- [Casting: `bstack_cast!`](#casting-bstack_cast)
- [Type tags (`EightCC`)](#type-tags-eightcc)
- [How it works on disk](#how-it-works-on-disk)
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

    drop(config);                 // strong = 1 — the session still owns it (Rc: auto-decrement)
    session.bstack_drop(&alloc)?;  // strong = 0 — Config freed automatically by its refcount
    Ok(())
}
```

> **Owned vs. shared teardown.** A `Session` is a *uniquely owned* block, so its
> handle (`BStackOwned<Session>`) frees **nothing on `Drop`** — you free it
> explicitly with `bstack_drop`, so a persistent root is never silently deleted
> when a handle goes out of scope. A shared `Config` handle (`BStackRc`) *does*
> auto-manage its refcount on `Drop` (like `std::rc::Rc`). See
> [Handles & lifetimes](#handles--lifetimes).

A fuller walk-through (shared ownership, weak observers, durability across a
reopen) is in [`examples/sessions.rs`](examples/sessions.rs):
`cargo run --example sessions`.

## Concepts

A **block** is a fixed-size record on disk. You write it as an ordinary struct
and annotate it with `#[bstack_block]`; the macro generates a parallel
`#[repr(C, packed)]` on-disk layout plus all the machinery to work with it.

Three block **modes**:

| Mode                        | Meaning                                                  |
|-----------------------------|----------------------------------------------------------|
| `#[bstack_block]`           | Plain, exclusively owned (like `Box`).                   |
| `#[bstack_block(rc)]`       | Reference-counted, inline count (like `Rc`, no `Weak`).  |
| `#[bstack_block(rc, weak)]` | Refcounted **and** weak-observable (like `Rc` + `Weak`). |

Every non-POD field carries an ownership annotation that decides how it is torn
down. Plain-old-data fields (anything `Pod` — integers, `[u8; N]`, etc.) are
stored inline and copied by value.

> **Requires a real allocator.** This layer needs a `bstack` allocator that
> actually frees (`dealloc`) and reserves offset 0 for its own metadata — e.g.
> `FirstFitBStackAllocator`, `SlabBStackAllocator`, `GhostTreeBstackAllocator`.
> **Do not use `LinearBStackAllocator`**: it's a bump allocator whose `dealloc`
> is a no-op (so teardown frees nothing), and it can hand out offset 0 (which
> would break the `Option<T>` niche). RAII over a non-freeing allocator doesn't
> make sense anyway.
>
> For growable fields (`Vec<T>` / `String`), prefer a **realloc-safe** allocator:
> growth reallocates the backing block, and a torn realloc under a poorly-behaved
> allocator can corrupt it. `FirstFitBStackAllocator` is realloc-safe.

## Defining blocks

```rust
#[bstack_block]
struct Node {
    #[bstack_owned]           // this block exclusively owns the child
    payload: Payload,
    #[bstack_strong]          // a shared, refcounted reference
    shared: SharedThing,
    #[bstack_weak]            // a non-owning back-pointer (may dangle)
    parent: Node,
    #[bstack_ref]             // a raw reference, no ownership semantics
    sibling: Node,
    tag: u32,                 // POD, stored inline
}
```

For each block the macro generates:

- a typed handle `struct Node(BStackRange)` and its `NodeOnDisk` payload;
- a `new(...)` **constructor** that allocates and wires the block;
- **accessors** — `node.field(stack)` for each field;
- **`set_<field>`** setters for `#[bstack_weak]` fields;
- recursive **teardown** (`BStackDrop`), casting, and (for `rc` / `rc, weak`)
  the control block and refcount machinery.

## Field ownership

| Annotation         | Child kind required    | On drop                            | `bstack_move!` yields   |
|--------------------|------------------------|------------------------------------|-------------------------|
| `#[bstack_owned]`  | any block              | recursively frees the child        | `BStackOwned<T>`        |
| `#[bstack_strong]` | `(rc)` or `(rc, weak)` | decrements refcount; frees at zero | `BStackRc<T>`           |
| `#[bstack_weak]`   | `(rc, weak)`           | decrements weak count only         | `Option<BStackWeak<T>>` |
| `#[bstack_ref]`    | any block              | nothing                            | `BStackRef<T>`          |
| *(none)* — POD     | `Pod` type             | nothing (inline)                   | the value               |

Ownership rules are enforced at compile time: a `#[bstack_weak]` field whose
target isn't `(rc, weak)`, or a non-`Pod` field with no annotation, is a
compile error.

### Nullable references: `Option<T>`

Wrap a reference field in `Option` to make it nullable — on disk it's still a
single `u64`, with `0 == None` (no allocation ever lives at offset 0, so it's a
free niche, no tag byte):

```rust
#[bstack_block]
struct Node {
    #[bstack_owned] left: Option<Child>,   // may be absent
    #[bstack_strong] shared: Option<Thing>,
}
```

The accessor then returns `io::Result<Option<Handle>>`, the constructor takes
`Option<BStackOwned<Child>>` / `Option<BStackRc<Thing>>`, and `bstack_move!`
yields `Option<…>`. (`#[bstack_weak]` fields are already nullable by nature.)

### Variable-length: `Vec<T>` and `String`

A `Vec<T>` (POD `T`) or `String` field stores a growable sequence. On disk the
field holds a fixed-size **descriptor** — `{ data_off, data_size }` stored
*inline* — pointing at the (growable, reallocating) data block. The field stays
fixed-size while the data grows and moves; because the struct uniquely owns the
vector, the descriptor needs no separate block:

```rust
#[bstack_block]
struct Record {
    name: String,        // POD vectors are un-annotated
    tags: Vec<u32>,
    id: u64,
}

let rec = Record::new(&alloc, "hello", &[1u32, 2, 3], 42)?;  // &str / &[T] / value
let mut tags = rec.handle().tags(&alloc)?;    // a BStackVec<u32> handle
tags.push(4)?;                                // grows; rewrites the inline descriptor
assert_eq!(rec.handle().tags(&alloc)?.to_vec()?, vec![1, 2, 3, 4]);
```

The accessor returns a [`BStackVec<T>`] handle (`len` / `to_vec` / `push`); the
constructor takes `&str` / `&[T]`; freeing the block frees the data (the inline
descriptor goes with the struct). A field handle rewrites the inline descriptor
when a push reallocates.

A vector **not** resident in a field — built by `BStackVec::from_slice` or handed
out by `bstack_move!` — is *detached*: it carries its descriptor in memory and
frees only its data block on `bstack_drop`. It becomes persistent when written
into a struct field (which stamps the inline descriptor) — the general
[moved-out-values-are-unrooted](#moving-fields-out-bstack_move) rule.

Wrapping a vector field in `Option` makes it nullable — `Option<Vec<T>>` /
`Option<String>` (and `Option<#[bstack_owned] Vec<Thing>>`, etc.). On disk it is
the same inline descriptor with the `data_off == 0` niche as `None` (distinct
from an *empty* present vector, whose data block is at a non-zero offset). The
constructor takes `Option<&[T]>` / `Option<&str>` / `Option<Vec<Handle>>`, the
accessor returns `Option<_>`, and `bstack_move!` yields `Option<_>`.

#### Vectors of blocks: `#[bstack_owned/strong/weak/ref] Vec<Thing>`

When the elements are `#[bstack_block]` values, the field annotation states the
**elements'** ownership (the descriptor + offset array are still owned by the
struct). The vector stores each element's offset; the annotation decides what
happens to the elements on teardown — mirroring single-field annotations:

| Field                                | Element handle       | Accessor type            | On the struct's teardown                              |
|--------------------------------------|----------------------|--------------------------|-------------------------------------------------------|
| `Vec<T>` / `String` *(un-annotated)* | POD value (`T: Pod`) | `BStackVec<T>`           | frees the data block                                  |
| `#[bstack_owned] Vec<Thing>`         | `BStackOwned<Thing>` | `BStackBlockVec<Thing>`  | recursively frees every child, then the offset array  |
| `#[bstack_strong] Vec<Thing>`        | `BStackRc<Thing>`    | `BStackStrongVec<Thing>` | releases each strong ref (frees at 0), then the array |
| `#[bstack_weak] Vec<Thing>`          | `BStackWeak<Thing>`  | `BStackWeakVec<Thing>`   | releases each weak ref, then the array                |
| `#[bstack_ref] Vec<Thing>`           | `BStackRef<Thing>`   | `BStackRefVec<Thing>`    | frees the offset array only                           |

```rust
#[bstack_block]
struct Tree {
    #[bstack_owned] kids: Vec<Leaf>,   // Tree owns each Leaf
    label: u32,
}

let kids = vec![Leaf::new(&alloc, 10)?, Leaf::new(&alloc, 20)?];
let tree = Tree::new(&alloc, kids, 7)?;                 // ctor takes Vec<BStackOwned<Leaf>>
let v = tree.handle().kids(&alloc)?;                    // a BStackBlockVec<Leaf>
assert_eq!(v.get(1)?.unwrap().val(stack)?, 20);
tree.bstack_drop(&alloc)?;                              // recursively frees every child
```

The constructor takes a `Vec` of the corresponding element handle; the accessor
returns the vector handle (`len` / `to_vec` / `get`; `BStackWeakVec` has
`upgrade(i)`; each has a `push_*`). Because the annotation *is* what marks
block elements, an **un-annotated** `Vec<T>` is always POD and requires `T: Pod`.

**Sharing a vector** between two structs isn't done by pointing both at the same
descriptor (a descriptor has a single owner). Instead, wrap the vector in its own
`#[bstack_block]` and share *that* block with `#[bstack_strong]` / `#[bstack_ref]`.

### Ergonomic reference coercion

For convenience, a field written `&T` is coerced to owned `T` (and `&str` to
`String`) with a compile warning — so a stray reference doesn't fail to compile,
but you're nudged to write the owned type.

## Enums: `#[bstack_enum]`

A `#[bstack_enum]` lowers a Rust `enum` to a **tagged-union block**: a 1-byte
discriminant plus a payload area sized to the largest variant. Each variant is
**unit** (no data), a **POD** newtype `V(P)` (`P: Pod`, stored inline), or an
annotated newtype whose annotation states the *variant's* relationship, exactly
like a struct field — `#[bstack_owned]` / `#[bstack_strong]` / `#[bstack_weak]` /
`#[bstack_ref]` (each a `u64` offset to the child / control block).

```rust
#[bstack_enum]
enum Node {
    Empty,                            // unit
    Num(u32),                         // POD, inline
    #[bstack_ref]    Link(Leaf),      // borrowed reference (frees nothing)
    #[bstack_owned]  Child(Leaf),     // owned child (freed on teardown)
    #[bstack_strong] Shared(Thing),   // a strong ref (Thing is (rc)/(rc, weak))
    #[bstack_weak]   Watch(Thing),    // a weak ref (Thing is (rc, weak))
}

let leaf = Leaf::new(&alloc, 7)?;
let node = Node::new(&alloc, NodeInit::Child(leaf))?;   // construct a variant
match node.handle().read(&alloc)? {                     // read / match the current one
    NodeView::Child(c) => assert_eq!(c.val(stack)?, 7),
    _ => {}
}
node.bstack_drop(&alloc)?;                              // frees the owned child too
```

The macro generates the handle `Node`, plus **`NodeInit`** (construction input:
POD by value, `#[bstack_owned]` → `BStackOwned<T>`, `#[bstack_strong]` →
`BStackRc<T>`, `#[bstack_weak]` → `BStackWeak<T>`, `#[bstack_ref]` →
`BStackRef<T>`) and **`NodeView`** (the read result: POD by value, owned/ref
children as borrowed handles, a weak variant *upgraded* to `Option<BStackRc<T>>`).
`read` takes the allocator (a weak variant upgrades through it). Teardown matches
the discriminant and releases the variant's reference — recursively freeing an
owned child, decrementing a strong/weak count, and nothing for a ref.

Like a struct, an enum has **modes**: `#[bstack_enum]` is owned, while
`#[bstack_enum(rc)]` / `#[bstack_enum(rc, weak)]` make the enum itself
reference-counted / weak-observable — `new` then returns a `BStackRc<E>` (with
`try_clone` / `downgrade` / `upgrade`), and the enum can be a `#[bstack_strong]` /
`#[bstack_weak]` field of a struct.

An enum is a block, so it is **always referenced** — store it as a field of a
struct (inline embedding isn't supported). Struct and multi-field tuple variants
aren't supported.

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
carries no allocator and frees **nothing** when its handle drops — so a
persistent root is never silently deleted by going out of scope. You free it
explicitly, or wrap it in an `AutoDrop` guard for RAII:

```rust
let owned: BStackOwned<Node> = Node::new(&alloc, /* … */)?;
let value = owned.handle().tag(stack)?;   // read a field (or `owned.tag(stack)?` via Deref)

owned.bstack_drop(&alloc)?;               // free it now, explicitly …
// … or: let _guard = owned.auto(&alloc); // RAII — freed when `_guard` drops
```

Shared handles (`BStackRc` / `BStackWeak`) *do* manage their reference counts
automatically on `Drop`, exactly like `std::rc::Rc` / `Weak`.

Construction (`new`) consumes the children it takes ownership of:

- `#[bstack_owned] p: P`  → parameter `p: BStackOwned<P>`
- `#[bstack_strong] s: S` → parameter `s: BStackRc<S>`
- `#[bstack_ref] r: R`    → parameter `r: BStackRef<R>`
- POD `t: u32`            → parameter `t: u32`
- `#[bstack_weak]`        → **not** a parameter; starts null, wired later with
  `set_<field>` (see below).

## Shared ownership & weak references

`BStackRc<T>` is the on-disk `Rc`. Because duplicating it must atomically bump an
on-disk counter (which can fail with I/O), cloning is the fallible
[`TryClone`] trait rather than `Clone`:

```rust
let a = Config::new(&alloc, 1, 0)?;   // BStackRc<Config>, strong = 1
let b = a.try_clone()?;               // strong = 2
let w = a.downgrade()?;               // BStackWeak<Config>, weak observer
drop(a); drop(b);                     // strong = 0 — Config's data is freed
assert!(w.upgrade()?.is_none());      // upgrade fails: the object is gone
```

`BStackWeak<T>` never keeps the data alive, only the small control block, so
`upgrade()` is a sound liveness check (atomic CAS on the strong count).

**Weak fields** are for back-pointers and cycles, where you can't supply the
target at construction. They start null and are wired afterward; the accessor is
an *upgrade*:

```rust
#[bstack_block(rc, weak)]
struct WNode {
    #[bstack_weak]
    back: WNode,
    val: u32,
}

let a = WNode::new(&alloc, 1)?;         // BStackRc<WNode>
let b = WNode::new(&alloc, 2)?;
b.handle().set_back(&alloc, a.downgrade()?)?;      // wire b.back -> a (weak)

if let Some(a2) = b.handle().back(&alloc)? {       // upgrade the weak field
    println!("a is still alive: {}", a2.handle().val(stack)?);
}
```

A weak field stores its target's *control-block* offset, so dropping the strong
owner first and then the holder is sound — no use-after-free of freed data.

## Moving fields out: `bstack_move!`

`bstack_move!` destructures a handle into its fields, transferring ownership of
each out as a tuple and freeing only the parent *shell* — the children stay live
on disk, now owned independently.

On a **`BStackOwned<X>`** it is infallible (a unique owner). Because a bare
owned handle carries no allocator, pass one — `bstack_move!(owned, &alloc)`
(symmetric with `owned.bstack_drop(&alloc)`):

```rust
#[bstack_block]
struct Pair {
    #[bstack_owned] left: Leaf,
    #[bstack_strong] shared: Thing,
    right: u32,
}

let pair: BStackOwned<Pair> = /* … */;
let (left, shared, right) = bstack_move!(pair, &alloc)?;
//   ^BStackOwned<Leaf>  ^BStackRc<Thing>  ^u32
```

On a **`BStackRc<X>`** (an `(rc)` or `(rc, weak)` block) it is a `try_unwrap`: it
succeeds only when this handle is the **sole strong owner** (an atomic
`strong: 1 → 0`), otherwise it hands the handle back. A weak observer does *not*
block the move — afterward its `upgrade()` just returns `None`.

An allocator-carrying handle — a `BStackRc`, or a `BStackOwned` wrapped as
`owned.auto(&alloc)` — takes the single-argument form (the allocator rides along):

```rust
let rc: BStackRc<Pair> = /* … */;
match bstack_move!(rc)? {
    Ok((left, shared, right)) => { /* we were the only owner */ }
    Err(rc)                    => { /* someone else still holds it */ }
}
```

> **Moved-out values are unrooted.** A handle produced by `bstack_move!` — like
> one from `X::new` — is detached from any persistent structure. Its block still
> lives on disk, but it is reachable *only* through your in-memory handle: if the
> program ends without re-attaching it (storing it into another block's field) or
> freeing it (`bstack_drop`), it becomes unreachable garbage. Persistence comes
> from being reachable through a struct, not from having been moved out.

## Casting: `bstack_cast!`

Convert between typed handles and the untyped `bstack` primitives. Upcasts are
infallible; downcasts check the block's tag. Because a function-like macro can't
read a `let x: T = …` annotation, the target is given explicitly with `as`:

```rust
use bstack_raii::{BStackCastAs, BStackCastInto};   // the cast methods

let owned: BStackOwned<Node> = /* … */;

// Upcast needs an allocator, so wrap the bare owned handle first (`auto`):
let slice = bstack_cast!(owned.auto(&alloc) as BStackOwnedSlice);   // infallible

match bstack_cast!(slice as BStackOwned<Node, _>)? {        // owned downcast (bare handle)
    Ok(node)  => { /* tag matched */ }
    Err(slice) => { /* tag mismatch — slice handed back */ }
}

let view = node.handle().as_slice(stack);                   // borrowed upcast
let maybe: Option<Node> = bstack_cast!(view as Node)?;      // borrowed downcast
```

The equivalent methods (`into_slice`, `cast_into::<T>`, `cast_as::<T>`,
`as_slice`) can also be called directly.

## Type tags (`EightCC`)

Each block gets an 8-byte tag used as the downcast discriminant. It is a
**readable prefix** over a **hash tail**: camel-case initials (or a de-voweled
single word), followed by the high-bit-set tail of a 64-bit hash of the crate +
type name — so distinct types stay distinct even when their prefixes collide, and
the tag is deterministic and stable across builds. Override it if you want a
documented, fixed on-disk tag:

```rust
#[bstack_block(rc, tag = "ORDLINE")]        // explicit data tag
struct OrderLine { /* … */ }
```

`ctrl_tag = "…"` overrides the control-block tag (default: the data tag,
lowercased). An override longer than 8 bytes is truncated with a compile warning;
`#[bstack_block(allow(overlong_tag))]` silences it (as does the reference-coercion
warning's `allow(coerced_ref)`, or a real `#[allow(deprecated)]` on the struct).

## How it works on disk

Every block begins with a 16-byte `BlockHeader { size: u64, tag: EightCC }`.
References between blocks are stored as `u64` offsets; a target's length is
recovered from its compile-time `size_of::<T::OnDisk>()`.

- **`(rc)`** injects an inline `refcount` after the header.
- **`(rc, weak)`** splits into a *data block* (with a back-pointer to its control
  block) and a separate *control block* holding `strong` / `weak` counters and a
  forward pointer. The data block is reclaimed when `strong` hits zero; the small
  control block persists until `weak` also hits zero — exactly like `Arc`/`Weak`.

Refcount updates are single-lock read-modify-writes on `bstack` (crash-atomic,
no spin loop). All operations are durable and speak `std::io::Result`.

## Limitations

- **Fixed-size block payloads.** A block's `OnDisk` struct is fixed-size — no
  *inline* variable-length arrays or slices. Growable data lives out-of-line via
  an inline descriptor: `Vec<T>` / `String` (POD),
  `#[bstack_owned/strong/weak/ref] Vec<Thing>` (block elements), and their
  `Option<…>` (nullable) forms are all supported.
- **Requires a freeing allocator** that reserves offset 0 — not
  `LinearBStackAllocator` (see [Concepts](#concepts)).
- **No generic block types**, and non-`Pod` fields must carry an annotation.
- **Enums** ([`#[bstack_enum]`](#enums-bstack_enum)) support unit / POD /
  `#[bstack_owned]` / `#[bstack_strong]` / `#[bstack_weak]` / `#[bstack_ref]`
  variants in all three modes (owned / `(rc)` / `(rc, weak)`). Struct /
  multi-field tuple variants, and `bstack_move!` on an enum, are not done.
- The on-disk **ABI is not yet stable**.

## License

MIT (same as `bstack`).

[`bstack`]: https://github.com/williamwutq/bstack
[`TryClone`]: src/clone.rs
[`BStackVec<T>`]: src/vec.rs
[`BStackBlockVec<T>`]: src/vec.rs
[`BStackStrongVec<T>`]: src/vec.rs
[`BStackWeakVec<T>`]: src/vec.rs
[`BStackRefVec<T>`]: src/vec.rs
