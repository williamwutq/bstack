# RTTI bytecode format (proposal — for discussion)

The wire format of the persisted RTTI schema stack (see the RTTI design notes).
A **general program** reads this to interpret bstack_raii structures on disk with
no compiled-in Rust types. This document is a starting point, not frozen.

## Invariants

- **Little-endian, packed**, matching the rest of the crate. All multi-byte
  integers are LE and read via `from_le_bytes` on a byte slice, so field offsets
  need no natural alignment; the only padding is the explicit name-alignment
  noted below (names pad up to a 4- or 8-byte boundary so the run that follows
  them stays aligned).
- **The file is a `bstack`.** Each type descriptor is one record, appended with a
  single `BStack::push` — atomic and crash-safe by bstack's own contract, no extra
  header, commit-point, or WAL machinery on our side. Existing records are
  immutable → any number of readers, zero coordination.
- **Reference by eightcc, never inline.** A field that names another RTTI type
  stores that type's 8-byte tag; the reader resolves it against the in-memory
  index built by scanning the file once on open. Recursive types and
  append-order independence both fall out of this.
- **Offsets are absolute within a block's `OnDisk`** and already account for the
  16-byte `BlockHeader` and any injected rc/ctrl fields — the interpreter uses
  them verbatim, it never recomputes layout.

## File layout

The RTTI file **is a bstack**: a sequence of records, one per registered type,
each `push`ed as one atomic append. Registration = one `push`; enumeration =
walk records from offset 0 to `len()`, building the eightcc→offset map once on
open. Nothing else — bstack owns the framing, growth, and crash-safety.

The first record's magic pins the file as an RTTI stack; there is no separate
file header of ours.

### Record (8-byte aligned)

| off | field    | type     | notes                                        |
|-----|----------|----------|----------------------------------------------|
| 0   | eightcc  | [u8;8]   | the type tag — also the index key            |
| 8   | body_len | u32      | length of `body` (so a scanner can step)     |
| 12  | _pad     | u32      | 0 (keeps `body` 8-aligned)                   |
| 16  | body     | TypeDesc | see below; whole record padded up to 8 bytes |

## TypeDesc (record body)

| off | field       | type   | notes                                                                         |
|-----|-------------|--------|-------------------------------------------------------------------------------|
| 0   | flags       | u8     | bit0 = enum (else struct), bit1 = rc (refcount@+16), bit2 = weak (ctrl block) |
| 1   | disc_width  | u8     | enum: 1/2/4/8; 0 for struct                                                   |
| 2   | name_len    | u16    | type-name length                                                              |
| 4   | count       | u16    | struct: field count · enum: variant count                                     |
| 6   | disc_off    | u16    | enum: discriminant offset within `OnDisk`; 0 for struct                       |
| 8   | payload_off | u16    | enum: variant-payload start; 0 for struct                                     |
| 10  | ondisk_size | u64    | `size_of::<OnDisk>()` — the block stride                                      |
| 18  | name        | [u8;N] | UTF-8 type name (debugger-facing); pad to 8 → aligned body                    |
| …   | body        | —      | struct → `Field[count]`; enum → `Variant[count]`                              |

`flags` bit0 carries the kind (struct vs enum — no separate field); bits 1–2 tell
the interpreter how to treat a *target* block on clone: rc → bump `refcount@+16`;
rc,weak → go through the control block (`strong@+16`, `weak@+24`, `data@+32`).
Names are stored **once per type here**, never per-instance, so the
`size == size_of::<OnDisk>()` invariant is untouched.

## Field (variable, 4-byte aligned)

| off | field     | type   | notes                                             |
|-----|-----------|--------|---------------------------------------------------|
| 0   | offset    | u32    | absolute in `OnDisk` (struct) / in payload (enum) |
| 4   | name_len  | u16    | field-name length                                 |
| 6   | shape_len | u16    | bytes of the shape blob (skippable)               |
| 8   | name      | [u8;N] | UTF-8 field name; pad to 4 → aligned `shape`      |
| …   | shape     | Shape  | the info-complex, `shape_len` bytes; pad to 4     |

## Shape (the info-complex node)

A tiny type-tree. **Leaves carry the RAII kind** — the single branch the
interpreter dispatches on for get/set/clone/teardown. Depth is bounded by the
*source type* nesting (never by data depth: `Vec<[T;N]>`, `[Vec<T>;N]`,
`[[T;N];M]`, `Option`-leaf), so it parses with a small fixed work-stack.

| tag  | node    | payload        | inline on-disk form / interpretation                           |
|------|---------|----------------|----------------------------------------------------------------|
| 0x00 | POD     | width:u32      | `width` raw bytes; memcpy on clone                             |
| 0x01 | OWNED   | eightcc:[u8;8] | u64 offset; recurse + deep-clone / free child                  |
| 0x02 | STRONG  | eightcc:[u8;8] | u64 offset; **bump refcount** on clone (never copy)            |
| 0x03 | WEAK    | eightcc:[u8;8] | u64 offset; copy offset + **stop** (don't follow)              |
| 0x04 | REF     | eightcc:[u8;8] | u64 offset; **alias** on clone (design-ref-clone-alias)        |
| 0x05 | EMBED   | eightcc:[u8;8] | child `OnDisk` inlined (no offset); recurse in place           |
| 0x06 | FOREIGN | eightcc:[u8;8] | inline `ForeignRepr{file_id:u64, offset:u64}`; resolve         |
| 0x10 | OPTION  | inner:Shape    | 0-niche None over an offset-bearing inner                      |
| 0x11 | ARRAY   | n:u32, inner   | `[inner; n]`, contiguous                                       |
| 0x12 | VEC     | inner:Shape    | inline `VecDesc{data_off:u64, data_size:u64}`; elems = `inner` |
| 0x13 | TUPLE   | k:u8, inner×k  | POD tuple, fields in order                                     |

## Variant (enum, 8-byte aligned)

| off | field       | type   | notes                                             |
|-----|-------------|--------|---------------------------------------------------|
| 0   | disc_value  | i64    | discriminant literal, sign-extended               |
| 8   | name_len    | u16    | variant-name length                               |
| 10  | field_count | u16    | fields in this variant (0 = unit)                 |
| 12  | _pad        | u32    | 0                                                 |
| 16  | name        | [u8;N] | UTF-8 variant name, pad to 8                      |
| …   | fields      | —      | `Field[field_count]`, offsets relative to payload |

## Interpreter contract

- **Non-recursive over data:** an explicit work-stack of `(shape-node,
  abs-offset)`; each STRONG/WEAK/REF/OWNED/EMBED/FOREIGN leaf pushes the resolved
  child rather than recursing.
- **Reuse the existing machinery, no second commit path:** interpreted clone
  builds a `ClonePlan` (`alloc_many` up front, one `inplace_gen` commit);
  interpreted teardown = unlink then `free_many`; interpreted setter = one atomic
  data write. Get/set and clone/teardown are the two reverse-direction pairs over
  this same node tree.
- **Locks:** RTTI GIL (rwlock over this file) read during interpret, write during
  registration; the data file's own lock guards instances; order is GIL(read)
  before data-lock.

## Open questions

1. **On-file directory** vs scan-to-build the eightcc→offset index on open
   (current proposal: scan the bstack once; the record count is small). A
   directory record is the obvious later optimization if that ever bites.
2. **`union` kind** — deferred; enums already give the tagged case. Untagged
   overlay needs an "active member" rule before it can be interpreted.
3. **Generic instantiations** — each concrete instantiation is a distinct eightcc
   and thus a distinct record; confirm that's acceptable stack growth.
4. **Collision handling on `sync()`** — same eightcc, different stored `name` ⇒
   hard error (eightcc is the resolution key; a collision is corruption).
