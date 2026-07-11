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
- [Handles](#handles)
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
use bstack_raii::{BStack, BStackAllocator, TryClone, bstack_block};

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

    drop(config);   // strong = 1 — the session still owns it
    drop(session);  // strong = 0 — Config is freed from disk automatically
    Ok(())
}
```

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

An `#[bstack_owned] Vec<T>` (POD `T`) or `String` field stores a growable
sequence. On disk the field is a fixed-size pointer to a small **descriptor**
block, which in turn points to the (growable, reallocating) data block — so the
field stays fixed-size while the data can grow and move:

```rust
#[bstack_block]
struct Record {
    #[bstack_owned] name: String,
    #[bstack_owned] tags: Vec<u32>,
    id: u64,
}

let rec = Record::new(&alloc, "hello", &[1u32, 2, 3], 42)?;  // &str / &[T] / value
let mut tags = rec.handle().tags(&alloc)?;    // a BStackVec<u32> handle
tags.push(4)?;                                // grows in place, visible on re-read
assert_eq!(rec.handle().tags(&alloc)?.to_vec()?, vec![1, 2, 3, 4]);
```

The accessor returns a [`BStackVec<T>`] handle (`len` / `to_vec` / `push`); the
constructor takes `&str` / `&[T]`; `bstack_move!` yields the `BStackVec`. Freeing
the block frees the data + descriptor. Elements must be `Pod` — `Vec<Thing>`
(vectors of blocks), `#[bstack_ref] Vec<T>`, and `Option<Vec<T>>` are not
supported yet.

### Ergonomic reference coercion

For convenience, a field written `&T` is coerced to owned `T` (and `&str` to
`String`) with a compile warning — so a stray reference doesn't fail to compile,
but you're nudged to write the owned type.

## Handles

The typed handle `X` is a bare `(offset, len)` with no allocator — cheap,
`Copy`, and the thing you read fields through. The *owning* wrappers carry an
allocator and run teardown on `Drop`:

| Handle               | Ownership                        | Notes                                      |
|----------------------|----------------------------------|--------------------------------------------|
| `X` (the block type) | none (borrowed view)             | `x.field(stack)`; get from `.handle()`     |
| `BStackOwned<X>`     | exclusive                        | frees the block (recursively) on `Drop`    |
| `BStackRc<X>`        | shared strong                    | `try_clone`, `downgrade`; frees at count 0 |
| `BStackWeak<X>`      | none (keeps control block alive) | `try_clone`, `upgrade`                     |
| `BStackRef<X>`       | none (raw offset)                | resolve manually                           |

Get the read handle from a wrapper with `.handle()`:

```rust
let owned: BStackOwned<Node> = Node::new(&alloc, /* … */)?;
let value = owned.handle().tag(stack)?;   // read a field
```

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

On a **`BStackOwned<X>`** it is infallible (a unique owner):

```rust
#[bstack_block]
struct Pair {
    #[bstack_owned] left: Leaf,
    #[bstack_strong] shared: Thing,
    right: u32,
}

let pair: BStackOwned<Pair> = /* … */;
let (left, shared, right) = bstack_move!(pair)?;
//   ^BStackOwned<Leaf>  ^BStackRc<Thing>  ^u32
```

On a **`BStackRc<X>`** (an `(rc)` or `(rc, weak)` block) it is a `try_unwrap`: it
succeeds only when this handle is the **sole strong owner** (an atomic
`strong: 1 → 0`), otherwise it hands the handle back. A weak observer does *not*
block the move — afterward its `upgrade()` just returns `None`.

```rust
let rc: BStackRc<Pair> = /* … */;
match bstack_move!(rc)? {
    Ok((left, shared, right)) => { /* we were the only owner */ }
    Err(rc)                    => { /* someone else still holds it */ }
}
```

## Casting: `bstack_cast!`

Convert between typed handles and the untyped `bstack` primitives. Upcasts are
infallible; downcasts check the block's tag. Because a function-like macro can't
read a `let x: T = …` annotation, the target is given explicitly with `as`:

```rust
use bstack_raii::{BStackCastAs, BStackCastInto};   // the cast methods

let owned: BStackOwned<Node> = /* … */;

let slice = bstack_cast!(owned as BStackOwnedSlice);        // upcast (infallible)

match bstack_cast!(slice as BStackOwned<Node, _>)? {        // owned downcast
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

- **Fixed-size blocks.** A block's on-disk size equals its `OnDisk` struct size;
  there are no variable-length arrays or inline slices. Model collections as
  linked blocks.
- **Requires a freeing allocator** that reserves offset 0 — not
  `LinearBStackAllocator` (see [Concepts](#concepts)).
- **No generic block types**, and non-`Pod` fields must carry an annotation.
- No enums or variable-length fields yet (planned).
- The on-disk **ABI is not yet stable**.

## License

MIT (same as `bstack`).

[`bstack`]: https://github.com/williamwutq/bstack
[`TryClone`]: src/clone.rs
[`BStackVec<T>`]: src/vec.rs
