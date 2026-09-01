# Equality, hashing, and ordering — advisory

Identity and comparison semantics for `BStack` and the slice/chunk view types,
including the caveats a caller keying containers on them should know. Rust
only — the C API exposes no equality surface.

---

## `BStack`: instance identity

Two `BStack` values are equal iff they are the **same instance in memory**
(`std::ptr::eq`). Because `open` holds an exclusive advisory lock for the
stack's lifetime, no two live instances in one process can refer to the same
file, so pointer identity is the only meaningful equality.

`Hash` does **not** hash the address. It hashes the raw file descriptor (Unix)
or handle (Windows), which is equally unique per live instance but — unlike
the address — stable when the value moves. On platforms that are neither Unix
nor Windows the instance address is hashed, and the hash is not move-stable.

### Advisory

- An owned `BStack` is a poor hash-map key regardless of the hash basis:
  inserting moves the value, and pointer-`eq` means no other value can ever
  compare equal to the stored one. Key on `&BStack` or `Arc<BStack>` instead;
  both are fully coherent (stable address, stable fd).
- Identity does not survive the process or a reopen: a closed fd number may be
  reused by a later, unrelated `BStack`. Never compare or hash across the
  lifetime boundary of the instance.

## Slice views: coordinate equality, stack ignored

All equality, hashing, and ordering on the view types compare **coordinates
only** — `(offset, len)`, plus the stride for `BStackChunk`. The underlying
`BStack` instance never participates, and neither does content.

| Type               | Eq / Hash / Ord key             |
|--------------------|---------------------------------|
| `BStackRange`      | `(offset, len)`                 |
| `BStackSlice`      | `(offset, len)`                 |
| `BStackOwnedSlice` | `(offset, len)`                 |
| `BStackChunk`      | stride, then `(offset, len)`    |

Cross-type `PartialEq` / `PartialOrd` (slice ↔ handle ↔ range) follow the same
rule, so the whole comparison graph is mutually consistent and transitive.

### Advisory

- **Equal does not mean interchangeable.** Views over *different* stacks
  compare equal (and hash alike) at identical coordinates, yet every actual
  operation between such a pair — `swap`, `copy_from_bstack_slice`, `join`,
  `dealloc` on a foreign handle — is rejected at run time. Equality answers
  "same coordinates", not "same bytes" or "same stack".
- Consequently a `HashSet` or `BTreeSet` of views deduplicates across stacks.
  In the common single-stack program this is invisible; with several stacks
  alive, key on `(something identifying the stack, view)` or on the stack
  itself (`&BStack` / `Arc<BStack>`, see above) when identity matters.
- To check the identity a comparison ignores: compare the borrowed stacks
  themselves (`BStack`'s `==` *is* instance identity, per the section above),
  or `BStackOwnedSlice::is_from` for allocator provenance — only
  `is_from` predicts whether `dealloc` will accept a handle.
- Equality is also indifferent to *when* the coordinates were read: a range
  can outlive the allocation it once described. Coordinate equality with a
  live view says nothing about the bytes there now.
