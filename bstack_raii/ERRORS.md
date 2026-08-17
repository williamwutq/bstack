# `bstack_raii` macro error codes

Every compile-time error reported by the `#[bstack_block]`, `#[bstack_enum]`,
`bstack_move!`, and `bstack_cast!` macros carries a stable `[BSTACKxxxx]` code.
This file explains each one — what triggers it and how to fix it. The message text
itself always states the fix too; this is the longer-form reference.

Codes are grouped by domain (the ranges are permanent; individual numbers never
change meaning):

| Range        | Domain                             |
|--------------|------------------------------------|
| `BSTACK00xx` | Attributes & macro arguments       |
| `BSTACK01xx` | Field / type shapes                |
| `BSTACK02xx` | Enum variants & discriminants      |
| `BSTACK03xx` | Cross-file pointers (`Foreign<T>`) |
| `BSTACK04xx` | Generic blocks                     |
| `BSTACK05xx` | `#[embed]`                         |
| `BSTACK06xx` | `#[bstack_mut]`                    |
| `BSTACK07xx` | `bstack_cast!` / `bstack_move!`    |

---

## Attributes & macro arguments (`00xx`)

### BSTACK0001 — more than one ownership annotation
A field or variant carries more than one of `#[bstack_owned]` / `#[bstack_strong]` /
`#[bstack_weak]` / `#[bstack_ref]` / `#[embed]`. **Fix:** keep exactly one — they are
mutually exclusive kinds of ownership.

### BSTACK0002 — `weak` without `rc`
`#[bstack_block(weak)]` (or `#[bstack_enum(weak)]`) was given without `rc`. A weak
handle observes a *reference-counted* block. **Fix:** use `rc, weak`.

### BSTACK0003 — unknown macro argument
An unrecognized option was passed to `#[bstack_block(...)]` / `#[bstack_enum(...)]`.
**Fix:** use one of `rc`, `weak`, `tag = "..."`, `ctrl_tag = "..."`, `allow(...)`, or
(enums only) `repr(...)`.

### BSTACK0004 — `tag` / `ctrl_tag` value is not a string literal
`tag = ` / `ctrl_tag = ` must be assigned a string literal. **Fix:** e.g.
`tag = "MYTG"`.

### BSTACK0005 — invalid `repr(...)` discriminant width
`repr(...)` names an unsupported discriminant type. **Fix:** use one of
`u8|u16|u32|u64|i8|i16|i32|i64`, or `aligned` (an alias for `u64`).

### BSTACK0006 — `repr(usize)` / `repr(isize)`
Pointer-width discriminants are disallowed because bstack offsets are always 64-bit.
**Fix:** pick an explicit width, e.g. `repr(u64)`.

### BSTACK0007 — unknown `allow(...)` flag
An unrecognized name was passed to `allow(...)`. **Fix:** use `overlong_tag`,
`coerced_ref`, or `deprecated`.

### BSTACK0008 — `repr(...)` on a `#[bstack_block]`
`repr(...)` selects an *enum* discriminant width and is only meaningful on
`#[bstack_enum]`. **Fix:** remove it from the struct.

---

## Field / type shapes (`01xx`)

### BSTACK0101 — nested `Option<Option<T>>`
A field / `Vec` slot lowers a single `Option` to the absent/`0` niche; a second layer
has nowhere to live on disk. **Fix:** model the states with a `#[bstack_enum]`, e.g.
`enum Slot { Missing, Empty, Present(T) }`.

### BSTACK0102 — nested `Vec<Vec<T>>` / `Vec<String>`
A `Vec` element must be a single leaf, not another dynamically-sized region. **Fix:**
wrap the inner vector in a `#[bstack_block]` struct and store `Vec<ThatStruct>`.

### BSTACK0103 — tuple as a `Vec` element (`Vec<(A, B)>`)
A tuple can't be a `Vec` element (it carries no per-element ownership annotations).
**Fix:** name it as a `#[bstack_block]` struct and store `Vec<ThatStruct>`.

### BSTACK0104 — `Option` around a whole sub-array
`Option` may not wrap an inner sub-array. **Fix:** move the `Option` onto the leaf
element, e.g. `[[Option<T>; N]; M]`.

### BSTACK0105 — whole-array `Option<[T; N]>`
A whole reference-array can't be nullable. **Fix:** use `[Option<T>; N]` (per-element
null via the `0` niche).

### BSTACK0106 — whole-tuple `Option<(..)>`
A whole tuple field can't be nullable. **Fix:** make the individual elements
`Option<...>` instead.

### BSTACK0107 — ownership annotation on `String`
`String` is always POD, so an ownership annotation is meaningless on it. **Fix:** drop
the annotation and store it as a plain `String`.

### BSTACK0108 — `Foreign` in an unsupported position
A `Foreign<T>` sits somewhere the macro can't lower it (e.g. inside a tuple or another
POD aggregate). It is supported as scalar `Foreign<T>` / `Option<Foreign<T>>`,
`Vec<Foreign<T>>` / `Vec<Option<Foreign<T>>>`, or `[Foreign<T>; N]`. **Fix:** elsewhere,
wrap the `Foreign` inside a `#[bstack_block]` struct and use that.

### BSTACK0110 — whole-array `Option<[Vec<T>; N]>`
A whole array-of-vectors can't be nullable. **Fix:** make each element nullable —
`[Option<Vec<T>>; N]`.

### BSTACK0111 — whole-array `Option<[Foreign<T>; N]>`
A whole foreign-array can't be nullable (a null foreign is already `offset 0`). **Fix:**
use `[Option<Foreign<T>>; N]`.

---

## Enum variants & discriminants (`02xx`)

### BSTACK0201 — duplicate discriminant
Two variants resolve to the same discriminant value. (The macro replaces the enum, so
rustc's own `E0081` never fires — this is its stand-in.) **Fix:** give the variants
distinct values.

### BSTACK0202 — discriminant overflow
Auto-incrementing the discriminant overflowed. **Fix:** set explicit values or widen
`repr(...)`.

### BSTACK0203 — discriminant out of range for `repr(...)`
A value doesn't fit the chosen `repr` width. **Fix:** widen `repr(...)` or lower the
value.

### BSTACK0204 — non-integer discriminant
A `= <expr>` discriminant is not an integer literal (or negated integer literal).
**Fix:** use an integer literal, e.g. `= 3` or `= -1`.

### BSTACK0205 — ownership annotation on a non-single-field variant
An ownership annotation was placed on a unit, multi-field tuple, or struct variant.
Only a single-field tuple variant may be annotated. **Fix:** e.g. `#[bstack_owned] V(T)`;
unit / multi-field / struct variants are POD aggregates and take no annotation.

---

## Cross-file pointers — `Foreign<T>` (`03xx`)

### BSTACK0301 — `#[embed]`ing a `Foreign`
A `Foreign<T>` is a pointer and can't be embedded inline. **Fix:** use an ownership
annotation (`#[bstack_owned/strong/weak/ref]`), not `#[embed]`.

### BSTACK0302 — `Foreign` without an ownership annotation
A `Foreign<T>` field / element / variant needs an annotation naming the *target's* kind
in its own file. **Fix:** add `#[bstack_owned/strong/weak/ref]`.

### BSTACK0303 — `Foreign<Foreign<T>>` (pointer to a pointer)
A `Foreign` target must be a block, not another `Foreign`. (A collection *of* pointers —
`Vec<Foreign<T>>`, `[Foreign<T>; N]` — is fine; a pointer *to* a pointer is not.) **Fix:**
bridge the inner type in a `#[bstack_block]` struct and point the `Foreign` at that.

### BSTACK0304 — `Foreign<Option<Foreign<T>>>` (double pointer)
A pointer to a nullable pointer. **Fix:** bridge with a `#[bstack_block]` struct.

### BSTACK0305 — `Foreign<Option<T>>`
Nullability belongs on the field/element as `Option<Foreign<T>>`, not on the target — a
null element is a `Foreign` with `offset 0`. **Fix:** use `Option<Foreign<T>>`.

### BSTACK0306 — `Foreign` target is a `Vec` / `String`
A `Foreign` must target a block, not a collection. (`Vec<Foreign<T>>` is fine;
`Foreign<Vec<T>>` is not.) **Fix:** bridge with a `#[bstack_block]` struct.

### BSTACK0307 — `Foreign` target is an array
A `Foreign` must target a block, not an array. (`[Foreign<T>; N]` is fine;
`Foreign<[T; N]>` is not.) **Fix:** bridge with a `#[bstack_block]` struct.

### BSTACK0308 — `Foreign` target is a tuple
A `Foreign` must target a `#[bstack_block]`, not a tuple. **Fix:** bridge with a
`#[bstack_block]` struct.

---

## Generic blocks (`04xx`)

### BSTACK0401 — lifetime parameter on a generic block
A generic `#[bstack_block]` supports type and `const` parameters, not lifetimes.
**Fix:** remove the lifetime parameter.

### BSTACK0402 — generic block in `rc` / `rc, weak` mode
Generic blocks currently support plain mode only. **Fix:** drop `rc` / `rc, weak`, or
use a concrete (non-generic) type.

### BSTACK0403 — non-type parameter on a generic enum
A generic `#[bstack_enum]` supports type parameters only (no lifetime or const). **Fix:**
remove the lifetime/const parameter.

### BSTACK0404 — type parameter used as both POD and reference
A parameter can't be used as a `Pod` field in one place and a `#[bstack_block]` reference
in another — they have incompatible bounds. **Fix:** use separate parameters, or a
concrete type in one position.

### BSTACK0405 — generic in a non-`Foreign` position of a `Foreign` field
A type parameter appears in the non-foreign part of a field that also holds a `Foreign`
(the macro can't classify it generically). **Fix:** use concrete types for the
non-foreign parts.

### BSTACK0406 — generic enum variant stored inline
A type parameter in a generic enum variant must be a *reference* kind
(`#[bstack_owned/strong/weak/ref]`) — a POD or `#[embed]` variant's payload width would
depend on the parameter. **Fix:** annotate the variant with a reference kind.

### BSTACK0407 — const dimension in a nested array
A nested array `[[T; N]; M]` with a const-parameter dimension would need a const
expression (`N * M`) for its flattened length, which stable Rust forbids from using a
generic parameter. **Fix:** use a single `[T; N]`, or make the dimensions concrete.

---

## `#[embed]` (`05xx`)

### BSTACK0501 — `#[embed]`ing a `Vec` / `String`
Only a `#[bstack_block]` / `#[bstack_enum]` can be embedded, not a dynamically-sized
collection. **Fix:** embed a block type, or store the collection out-of-line (drop
`#[embed]`).

### BSTACK0502 — `#[embed]`ing a tuple
A tuple can't be embedded. **Fix:** embed a `#[bstack_block]` / `#[bstack_enum]` type.

### BSTACK0503 — `#[embed]` with `Option`
`#[embed]` doesn't support a nullable child. **Fix:** store the child unconditionally, or
model the optionality with a `#[bstack_enum]`.

---

## `#[bstack_mut]` (`06xx`)

### BSTACK0601 — `#[bstack_mut]` on an `#[embed]` field
In-place mutation of an embedded child isn't supported yet. **Fix:** rebuild the parent,
or store the child by reference instead of `#[embed]`.

### BSTACK0602 — `#[bstack_mut]` on a `Foreign` in a container / tuple
Only a scalar `Foreign<T>` / `Option<Foreign<T>>` field is mutable; `Vec<Foreign>`,
`[Foreign; N]`, and foreign tuples have no mutator yet. **Fix:** remove `#[bstack_mut]`,
or use a scalar `Foreign` field.

### BSTACK0603 — `#[bstack_mut]` on an enum variant
An enum mutator is *whole-value* (the payload's meaning depends on the discriminant).
**Fix:** put `#[bstack_mut]` on the enum itself (generates `set` / `replace`), not on a
variant.

### BSTACK0604 — `#[bstack_mut]` on a shared enum
A shared (`rc` / `rc, weak`) enum's refcount / control block can't be overwritten in
place. **Fix:** use `#[bstack_mut]` only on a plain enum; otherwise rebuild the value.

### BSTACK0605 — `#[bstack_mut]` on an enum with an `#[embed]` variant
Not supported yet (the embed's post-write copy makes in-place replacement a separate
problem). **Fix:** drop `#[bstack_mut]`, or remove the `#[embed]` variant.

---

## `bstack_cast!` / `bstack_move!` (`07xx`)

### BSTACK0701 — malformed `bstack_cast!` invocation
`bstack_cast!` expects `expr as Target`. **Fix:** e.g.
`bstack_cast!(slice as BStackOwned<X, _>)`.

### BSTACK0702 — `bstack_cast!` target is not a type path
The cast target must be a type path. **Fix:** name a concrete type / handle type.

### BSTACK0703 — `bstack_cast!` to a borrowed `BStackSlice`
A borrowed slice needs a stack, which the macro can't supply. **Fix:** use
`handle.as_slice(stack)` instead.

### BSTACK0704 — malformed owned downcast target
An owned downcast needs `BStackOwned<BlockType, _>`. **Fix:** name the block type inside
`BStackOwned<...>`.

### BSTACK0705 — malformed `bstack_move!` invocation
`bstack_move!` takes either `handle` or `owned, allocator`. **Fix:** call it as
`bstack_move!(handle)` or `bstack_move!(owned, &alloc)`.
