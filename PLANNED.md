# Planned Features

This document outlines upcoming features planned for the `bstack` crate. These enhancements aim to improve usability, performance, and integration while maintaining the core principles of durability, crash-safety, and simplicity. Changes aim to be backward-compatible. New features are suggested to be added as optional features under feature flags and new traits, instead of modifying existing ones, to avoid breaking changes. All features aim to follow [Rust's API design guidelines](https://rust-lang.github.io/api-guidelines/) and BStack's design principles.

---

## NOT PLANNED

### Deprecating `BStackGuardedSlice::as_slice` in favor of read-only access

Reasons:

`BStackGuardedSlice::as_slice` is not actually unsafe as data corruption through misuse doesn't violate memory safety or compromise allocator structure, so marking it unsafe would be misleading per Rust conventions. Documentation and API design already encourage using `read()` and `write()`. Callers using `as_slice()` are expected to understand the implications. In addition, the unsafe `raw_block()` method already exists for cases where hook bypass is needed, and its safety contract documents that hooks must be manually called. If this function is deprecated, it would break existing code, and callers who correctly use `as_slice()` for read-only purposes would need to migrate. Therefore, `as_slice()` as a safe function that returns a `BStackSlice` is sufficient, and the safety contract can be clearly documented without making it `unsafe fn`.

### Replacing `std::sync::RwLock` with `parking_lot::RwLock` or `usync::RwLock`

Reasons:

There are insufficient evidences that changing the implementation of RwLock will bring sufficient performance improvements to `BStack`. `std::sync::RwLock` remains the default. On Linux, `std` wins 10/14 benchmarks, often by 20–30% on write-heavy operations which dominate real workloads. The wins for `parking_lot` and `usync` are limited to fast read-path operations (`peek`, `get`, `len`) which are not the bottleneck.

For context, accepted optimizations like caching and locked region have demonstrated **~2-10× improvements**. The gains shown here do not meet that bar and introduce an additional dependency without a compelling cross-platform case.

Reference: https://github.com/williamwutq/bstack/pull/3

### Adding a singly-linked `BStackSList<T>`

Reasons:

A singly-linked variant saves 8 bytes per node by omitting `prev_ptr`, but any workload that reaches for a disk-backed linked list is already paying the cost of allocator round-trips and random-access I/O per element. An 8-byte savings per node is negligible against those overheads, and the restriction to forward-only traversal eliminates `push_front`-with-O(1)-`pop_back`, bidirectional cursors, and `split_off` without a full scan. `BStackList<T>` covers all singly-linked use cases with no meaningful extra cost, so a separate singly-linked type adds complexity without benefit.

### Making `BStackAllocator::realloc` and `dealloc` `unsafe fn`

Reasons:

While `realloc` and `dealloc` have a slice-origin requirement, the hazard window is now significantly narrowed because constructing a bad handle already requires `unsafe` (via `BStackSlice::from_raw_parts`). A well-reviewed `unsafe` block that constructs a `BStackSlice::from_raw_parts` can be expected to read the safety contract and comply with the origin requirement. The remaining concern is sub-slices: `subslice` and `subslice_range` are *safe* functions that produce slices with an origin different from any allocator-returned handle. However, marking `realloc` and `dealloc` as `unsafe fn` would force all call sites into an `unsafe` block, which may be unnecessarily burdensome given the already narrow hazard window. The best conventions use `unsafe` for operations that can cause undefined behavior and overuse of `unsafe` can lead to desensitization and misuse. As of `BStack` 0.2, the allocator interfaces are already mature and breaking changes should be avoided. Since the origin requirement is a safety contract that can be documented and enforced through careful API design, it may be sufficient to keep `realloc` and `dealloc` as safe functions while clearly documenting the requirements and risks.

Furthermore, `alloc`, `realloc`, and `dealloc` in the `BStackAllocator` trait do not need to operate on `BStackSlice` directly — they operate on an associated handle type. Custom allocator implementations can define handle types that do not support sub-slicing at all, eliminating the origin problem at the type level for those allocators. The sub-slice concern is therefore a consequence of a specific design choice (using `BStackSlice` itself as the raw handle) rather than an inherent flaw in the trait. The recommended approach is for allocators to use a handle type distinct from `BStackSlice`, where converting from handle to `BStackSlice` is straightforward but the reverse is not possible — making the origin requirement a type-level guarantee. The default allocators in this crate currently do not follow this recommendation, but that is a separate concern addressed in the planned features below.

### Adding `BStackVec<T>` for typed vector storage

Reasons:

A typed vector is a data structure, not an I/O mechanism, and `bstack`'s role is to abstract crash-safe atomic file I/O — the stack, allocators, and slices — not to provide collection types. Such structures compose on top of those mechanisms and belong downstream. `BStackByteVec` already covers the byte-buffer case, and end push/pop is already crash-atomic via a single `clen` write inherited from the stack, so a generic `BStackVec<T>` would mainly add a `bytemuck`/`zerocopy` dependency and POD-soundness surface for a generalization downstream consumers can build themselves. Keeping it out preserves the lean core and avoids freezing API surface ahead of 1.0.

### Adding `BStackList<T>` for typed doubly-linked list storage

Reasons:

As with `BStackVec<T>`, a linked list is a data-structure policy that belongs downstream rather than in the I/O core. The downstream `bllist` crate already provides on-disk doubly-linked lists built on `bstack`, with a no-corruption, recoverable-leak model: every block write is atomic, so a crash degrades to an orphaned node reclaimed on reopen rather than data corruption. The only capability that would justify a first-party type is crash-atomic multi-block structural mutation (`append`, `split_off`), which depends on the write-in-progress journaling primitive planned for 0.5.0 — and once that primitive is public, downstream crates can consume it to achieve the same guarantee. A `bstack`-native list would therefore never hold an exclusive capability. `bstack` should instead ship the mechanisms such structures need (allocators and, later, the public transaction primitive) and leave the structures themselves to downstream.

### Narrowing the `FirstFitBStackAllocator` mutex to guard only the free list

Reasons:

The mutex in `FirstFitBStackAllocator` serialises free-list mutation and tail extension/retraction. The size-conditional `try_extend`/`try_discard` primitives could pull the tail operations out from under it, leaving it to guard only the free list — but the win is small. Tail operations are not the common case, and a thread doing one still contends on the free-list mutex for the surrounding read-modify-write. While the free list remains a single mutex-guarded doubly-linked list (lock-free traversal is not possible yet), removing tail-op contention alone does not move the bottleneck — and it trades an obviously-correct design for an optimistic protocol resting on per-path ABA arguments (`try_*` checks payload *size*, not block identity).

---

## Introducing a newtype handle for default allocators (0.4.0)

**Breaking change:** Yes

### Motivation

The default allocators in this crate (e.g., `FirstFitBStackAllocator`) currently use `BStackSlice` as their raw handle type. Because `BStackSlice` exposes safe `subslice` and `subslice_range` methods, it is possible to produce a slice with an origin that does not correspond to any allocator-returned allocation and then pass it to `realloc` or `dealloc`, violating the origin requirement without any `unsafe` block at the call site. A dedicated newtype handle, constructible only by the allocator, closes this gap at the type level: it can be cheaply dereferenced to `BStackSlice` for reads and writes, but `BStackSlice` cannot be converted back into it, so sub-slices are statically prevented from being used as allocator handles.

A fuller redesign — described below — also reconsiders the roles of `BStackSlice` and the allocated handle type more broadly. The two types serve different purposes and should have different capabilities and ownership semantics to reflect that.

### Design

#### Allocated handle type

Introduce an allocated handle type (name TBD — see Open questions) as the associated handle type returned by `alloc` and accepted by `realloc` and `dealloc`. This type represents ownership of a specific allocation and has the following properties:

- **Not `Copy`.** The handle represents a unique allocation. Allowing it to be copied would make it possible to call `dealloc` on one copy while the other remains in use, or to pass two copies to `realloc` independently, both corrupting allocator metadata. Non-`Copy` ensures the handle is consumed when the allocation is released or resized.
- **No subslicing.** The handle does not expose `subslice` or `subslice_range`. A sub-range derived from an allocation is not itself an allocation, and preventing this at the type level closes the origin-requirement gap entirely without relying on documentation or convention.
- **Lifetime tied to the allocation, not the allocator.** The handle's lifetime parameter expresses that the underlying region is valid for reads and writes, not that the allocator is borrowed. In practice, this means the handle holds `&'a A` where `'a` is the allocator borrow, but the semantic intent is that the region is live for `'a` — which is guaranteed as long as the allocator (and thus the backing `BStack`) is alive.
- **Convertible to `BStackSlice` for I/O.** The handle can produce a `BStackSlice` for reading and writing the allocated region. This conversion is cheap (no allocation) and explicit.
- **Unsafe construction from `(allocator, BStackSlice)`.** For advanced use cases — such as reconstructing a handle from a serialized offset/length pair — an `unsafe` constructor takes an allocator reference and a `BStackSlice` and produces a handle. The caller must ensure the slice describes a valid, allocator-owned region.

#### `BStackSlice` without allocator pointer

`BStackSlice` currently carries a generic allocator reference `&'a A`. Since `BStackSlice` cannot directly call `realloc` or `dealloc` (those require the allocated handle type), it does not need to know the allocator's type. It only needs access to the backing `BStack` for I/O — and it should continue to carry a `&'a BStack` reference for exactly that purpose. Replacing `&'a A` with `&'a BStack` directly would:

- Simplify the type signature from `BStackSlice<'a, A>` to `BStackSlice<'a>`, removing the allocator type parameter entirely.
- Allow `BStackSlice` to be used across allocators or outside any allocator context, since the `&'a BStack` reference is sufficient for all read and write operations the slice needs to perform.
- Make the role of `BStackSlice` clearer: it is a view, not a handle. Like `&[u8]`, it is `Copy`, has no ownership, and carries no allocation identity.

The lifetime of a `BStackSlice` should tie to the stack (or the allocation it was derived from), but this is enforced by the context — the allocator or the allocated handle — rather than by `BStackSlice` itself. This is a known limitation: the crate cannot statically enforce that a `BStackSlice` does not outlive its allocation, since allocations have no RAII drop and the file is not memory. The allocated handle type is the mechanism for expressing allocation identity; `BStackSlice` is not.

This is a larger breaking change than the handle type alone, as it touches every existing use of `BStackSlice<'a, A>`.

### Open questions

- **Handle type name.** The name should clearly distinguish the handle from `BStackSlice` and signal that it represents an allocation, not just a region. Candidates include `BStackAlloc`, `BStackBlock`, or `BStackRegion`. The name should not suggest it is a slice (to avoid confusion with subslicing) and should not be so generic that it clashes with custom allocator handle types.
- **Whether to remove the allocator pointer from `BStackSlice`.** This is the more invasive part of the redesign. It may be worth doing as a separate change or deferring until the allocated handle type is stable. If deferred, `BStackSlice` retains `&'a A` temporarily, with a planned migration.
- **Unsafe back-conversion.** Should the allocated handle expose an `unsafe` method to recover a `BStackSlice` with no allocator type attached? This would be useful for serialization and low-level introspection, but requires the caller to uphold the origin invariant manually.

---

## Truly crash-atomic `set` via write-in-progress journaling (0.4.0)

**Breaking change:** Yes (on-disk format / ABI)

### Motivation

`set` is not crash-atomic today. The visible stack `[0..clen]` is updated in place under a single `clen` field, so a crash mid-write can leave the stack with a region of partially-old, partially-new bytes. The 16 B header (`magic[8] | clen[8]`) is enough to validate length but carries no record that a write is in progress, so recovery on open cannot detect or undo a torn in-place write.

For `set` to be truly atomic — either the old value or the new value, never an interleaving — the header must encode an in-progress write region, and the bytes on disk must be laid out so that recovery can deterministically finish or roll back whatever was in flight.

### Design

#### Header layout

The header grows from 16 B to 32 B:

```
[ magic[8] | clen[8 LE] | wip_ptr[8 LE] | wip_aux[8 LE] ]
```

Magic bumps to a new 0.4.0 value so old binaries fail loudly on a new file rather than misinterpret the longer header as payload. `wip_ptr == 0` is the steady state ("no write in progress"); a non-zero `wip_ptr` names the start of the in-progress write slice. The slice length is **not** stored — it is reconstructed at recovery time as `file_size − clen`, which is exact by construction (see algorithm below).

`wip_aux` discriminates which kind of write is armed, and is meaningful only while `wip_ptr != 0`. 0.4.0 ships the full set of modes below — same-length `set`, splice (grow/shrink), repeat-fill, and copy — so all of them are recognized by every 0.4.0 reader. The encoding leaves room for further modes with no change to the header *layout* (see *Forward compatibility of `wip_aux`* below for how modes are added later and when a new one forces a version bump).

- **`wip_aux == 0`** — same-length `set`. The slice length is inferred as `file_size − clen` (see below).
- **`wip_aux != 0`** — splice (different-length `set`). The top two bits are a **mode tag** and the low 62 bits carry the absolute length delta `d = |n_new − n_old| ≥ 1`:
  - `0b01` — **grow** (`n_new > n_old`); target length `clen' = clen + d`.
  - `0b10` — **shrink** (`n_new < n_old`); target length `clen' = clen − d`.
  - `0b00` is reserved. `0b11` is the **extension tag**: its low bits name a sub-mode. The repeat-fill and copy optimizations below are the first two, written `REPEAT` (sub-mode `1`) and `COPY` (sub-mode `2`), both shipping in 0.4.0; the remaining `0b11` sub-modes are open for later versions.

Storing the delta rather than the absolute `clen'` keeps `wip_aux` small (typical splices change the length by a handful of bytes) and, together with the non-zero mode tag, guarantees `wip_aux != 0` for every splice — so it can never be mistaken for the same-length case. Every single-region journal — same-length `set`, splice, and the extension modes — arms `wip_ptr != 0`; the multi-write journal (below) instead keeps `wip_ptr == 0` and parks its `u64::MAX` sentinel in `wip_aux`. The `wip_ptr` field alone therefore separates multi-write from every single-region mode, so no armed `wip_aux` can be confused with that sentinel.

Only the in-place mutators — `set`, splice, repeat-fill, and copy — ever arm `wip`; `push` and `pop` remain crash-atomic under a single `clen` write and leave `wip_ptr == 0` throughout. The at-rest invariant is `file_size == clen`; during a same-length `set` the file grows to `clen + n` to hold a tail backup of the new bytes, and during a splice it grows further to hold the rewritten suffix (see below).

#### `set` for the same-length case

Replacing `[a .. a+n]` with new bytes `dn`:

1. **Stage.** Extend the file to `clen + n`, write `dn` into the tail `[clen .. clen + n]`. Sync.
2. **Arm.** Write `wip_ptr = a`. Sync.
3. **Commit in place.** Copy `[clen .. clen + n]` into `[a .. a+n]`. Sync.
4. **Disarm.** Write `wip_ptr = 0`. Sync.
5. **Clean up.** Truncate file back to `clen`.

`wip_aux` stays zero throughout; the slice length is implied by `file_size − clen`. Arming is therefore a single 8 B header write, with no ordering subtlety between two fields — `wip_ptr` is the only armed bit.

#### `splice` for the different-length case

Splice replaces `[a .. a+n_old)` with new bytes `dn` of length `n_new ≠ n_old`. It changes both the slice contents and the stack length, so every byte above the splice point moves. Let:

- `clen' = clen − n_old + n_new` — the new stack length,
- `d = |n_new − n_old|` — the length delta armed in `wip_aux` (grow or shrink),
- `L = clen' − a` — the length of the **rewritten suffix** (the new bytes followed by the relocated tail),
- `S = max(clen, clen')` — the **staging base**.

`S` sits at or beyond both the old end `clen` and the new end `clen'`, so the staging region `[S .. S+L)` overlaps neither the live payload `[0 .. clen)` nor the in-place rewrite target `[a .. clen')`. That disjointness is what makes replay idempotent, in both directions, with no memmove-style overlap hazard.

1. **Stage.** Extend the file to `S + L`. Write the rewritten suffix into `[S .. S+L)`: `dn` into `[S .. S+n_new)`, then the surviving tail `[a+n_old .. clen)` into `[S+n_new .. S+L)`. Sync.
2. **Arm.** Atomically write `wip_ptr = a` and `wip_aux = (tag << 62) | d`. Both fields lie within the first 32 B, so this is a single aligned-block header write — `wip_ptr` and `wip_aux` can never tear apart. Sync.
3. **Replay in place.** Copy `[S .. S+L)` into `[a .. clen')`. Source and destination are disjoint (`a + L = clen' ≤ S`), so this is a plain forward block copy, restartable from the start. Sync.
4. **Disarm & commit length.** Atomically write `clen = clen'`, `wip_ptr = 0`, and `wip_aux = 0` (offsets 8/16/24, all within the first block). This single write is the commit point: the new length and the disarmed journal land together or not at all. Sync.
5. **Clean up.** Truncate the file to `clen'`, dropping the staged suffix (and, on a shrink, the vacated gap `[clen' .. clen)`).

When `n_new = n_old` the tail does not move, so the cheaper same-length path above stages and rewrites only the `n` changed bytes rather than the whole suffix; splice is used only when the length actually changes (`d ≥ 1`).

Every intermediate state recovers to the old or the new value, never a mix, by the same argument as the same-length case. A crash before **Arm** leaves `wip_ptr == 0` and rolls back — the base rule truncates to `clen`, discarding the staged suffix. A crash at or after **Arm** rolls forward, re-running the idempotent replay from the immutable staged suffix; because **Arm** and **Disarm** are each single aligned-block header writes, there is no torn state in which `wip_ptr`, `wip_aux`, and `clen` disagree. After **Disarm** the header already reads `clen'` with `wip_ptr == 0`, so a crash before **Clean up** finishes under the base `wip_ptr == 0` rule (truncate to `clen'`).

Splice writes the rewritten suffix twice — once to the staging region, once in place — i.e. `O(clen − a)`. This is inherent: a length change relocates every byte above `a`, so splice is cheap near the top of the stack and expensive near the bottom.

#### Repeating-fill (`repeat`) optimization

A same-length write whose new bytes are a repeating pattern does not need to stage the whole region — only the pattern and the repeat count. To fill `[n .. m)` with `k` copies of a slice `S` (so `m − n = k·|S|`, with `|S| ≥ 1` and `k ≥ 1`), the length is unchanged, so `clen` stays constant. `repeat` uses the extension `wip_aux` mode `REPEAT`.

1. **Stage.** Extend the file by `8 + |S|`; write `k` as a u64 (LE) into `[clen .. clen+8)` and `S` into `[clen+8 .. clen+8+|S|)`. Sync.
2. **Arm.** Atomically write `wip_ptr = n` and `wip_aux = REPEAT`. Sync.
3. **Fill in place.** Write `S` repeated `k` times across `[n .. m)`. Sync.
4. **Disarm.** Write `wip_ptr = 0` and `wip_aux = 0`. Sync.
5. **Clean up.** Truncate back to `clen`, dropping the `8 + |S|`-byte tail.

As with `set`, the tail is fully staged and synced before the journal is armed, so an armed `REPEAT` always names a complete, durable `[k | S]` tail — this is why the arm follows the stage rather than preceding it. Recovery (`wip_ptr != 0`, `wip_aux == REPEAT`) reads `k` from the first 8 tail bytes and `S` from the remaining `file_size − clen − 8`, refills `[wip_ptr .. wip_ptr + k·|S|)` with `k` copies of `S` (idempotent — the tail is immutable and disjoint from the target), truncates to `clen`, and clears the journal.

The win is that staging is `8 + |S|` bytes regardless of how large `[n, m)` is, versus the `m − n` bytes a plain `set` would back up. **Zeroing is the extreme case:** `zero(n, len)` is `repeat` with `S = [0x00]` and `k = len`, so the tail is a fixed 9 bytes no matter how many bytes are cleared.

Because its in-place fill destroys the previous contents of `[n, m)` with no verbatim backup, a reader that does not understand `REPEAT` cannot recover a crashed `repeat` safely. Shipping it in 0.4.0 is fine — the 0.4.0 magic bump away from 0.1.x already keeps it out of the hands of readers that would mishandle it — and the same reasoning is what would force a version bump for any *further* destructive mode added after 0.4.0 (see *Forward compatibility of `wip_aux`*).

#### Copy (`copy`) optimization

Copying an existing slice from a **disjoint** source `[s .. s+n)` onto a destination `[c .. c+n)` needs no data staged at all. The source is committed data that the copy does not touch, so it is already a durable backup; the destination's old bytes are being discarded, so they need none. Only the *coordinates* have to be journaled. `copy` uses the extension `wip_aux` mode `COPY`, with `wip_ptr = c` (the destination — the write target, as in every other mode) and a 16-byte tail holding the length `n` and the source start `s`. Like `set` and `repeat`, it is same-length, so `clen` stays constant.

1. **Stage.** Extend the file by 16; write `n` (u64 LE) into `[clen .. clen+8)` and `s` (u64 LE) into `[clen+8 .. clen+16)`. Sync.
2. **Arm.** Atomically write `wip_ptr = c` and `wip_aux = COPY`. Sync.
3. **Copy in place.** Copy `[s .. s+n)` into `[c .. c+n)`. Sync.
4. **Disarm.** Write `wip_ptr = 0` and `wip_aux = 0`. Sync.
5. **Clean up.** Truncate back to `clen`, dropping the 16-byte tail.

Recovery (`wip_ptr != 0`, `wip_aux == COPY`) reads `n` and `s` from the tail and re-copies `[s .. s+n)` into `[wip_ptr .. wip_ptr + n)`, then truncates to `clen` and clears the journal. This is idempotent precisely because the source is disjoint from the destination: the copy never modifies `[s .. s+n)`, so a replay always reads the same original bytes no matter how far a crashed copy had progressed. The win is that the tail is a fixed 16 bytes of metadata rather than the `n` bytes of source data a plain `set` would have to back up.

**Disjointness is mandatory, and `cross_exchange` is explicitly excluded.** The optimization rests on the source surviving the copy intact. If `[s, s+n)` and `[c, c+n)` overlapped, the in-place write would clobber source bytes before they were read, and a post-crash replay would read corrupted source — so an overlapping copy must fall back to the same-length `set` path, which stages the actual bytes in the tail. **`cross_exchange` can never use `COPY`** for the same reason: a swap makes each region simultaneously a source and a destination, so writing either half destroys data the other half still needs. It must copy the region contents into the tail first — the general backup-in-tail path — to preserve data integrity.

#### Recovery on open

Recovery runs once during construction, while the write lock is held, before the `BStack` is exposed. It reads only the header and the file size:

- **`wip_ptr == 0`** and **`wip_aux == 0`** — no operation in flight. Truncate to `clen` (drops any stale tail from a crashed stage). Done.
- **`wip_ptr == 0`** and **`wip_aux ==` the multi-write sentinel** — a fully staged multi-write. Replay it (see *Multi-write journaling*).
- **`wip_ptr != 0`** and **`wip_aux == 0`** — a same-length `set` was in progress. The staged tail is at `[clen .. file_size)`, length `n = file_size − clen`. Copy it into `[wip_ptr .. wip_ptr + n)`, truncate to `clen`, clear `wip_ptr`. The new value is committed.
- **`wip_ptr != 0`** and **`wip_aux` is a splice tag (`0b01`/`0b10`)** — a splice was in progress. Decode `a = wip_ptr` and delta `d` from `wip_aux`; set `clen' = clen + d` (grow) or `clen − d` (shrink) per the tag, then `S = max(clen, clen')` and `L = clen' − a`. Copy the staged suffix `[S .. S+L)` into `[a .. clen')` — idempotent, since the source is immutable and disjoint from the target — then atomically set `clen = clen'` and clear `wip_ptr`/`wip_aux`, and truncate to `clen'`. The new value is committed.
- **`wip_ptr != 0`** and **`wip_aux == REPEAT`** — a repeat-fill was in progress. Read `k` from the first 8 tail bytes and `S` from `[clen+8 .. file_size)`; refill `[wip_ptr .. wip_ptr + k·|S|)` with `k` copies of `S`, truncate to `clen`, clear the journal. The new value is committed.
- **`wip_ptr != 0`** and **`wip_aux == COPY`** — a copy was in progress. Read `n` and `s` from the 16-byte tail; copy `[s .. s+n)` into `[wip_ptr .. wip_ptr + n)`, truncate to `clen`, clear the journal. The new value is committed.
- **any other combination** — an operation from a newer release that shares this magic. Apply the **default**: roll back to `clen` (truncate the tail, clear the journal), abandoning the in-flight operation. This is safe by construction — any mode whose abandonment could lose committed data bumps the magic, so it never reaches a program that would mishandle it (see *Forward compatibility of `wip_aux`*).

Every intermediate on-disk state of the algorithm is recoverable to either the old or the new value, never an interleaving. Crashes in step 1 leave `wip_ptr == 0` (rollback by truncate). Crashes in step 2 are either `wip_ptr == 0` (rollback) or `wip_ptr == a` (roll forward via the staged tail). Crashes in step 3 roll forward; the recovery copy is idempotent over any partial in-place write. Crashes in step 4 are either roll forward (one more idempotent copy) or `wip_ptr == 0` (the new value is already in place; just truncate).

Recovery must run to completion **before** the locked-region cache (#4) is populated, otherwise the cache could snapshot mid-rollback bytes. Any `set` that touches the locked region must also invalidate or refresh the cache atomically with the disk-level commit. This is a hard requirement of the journaling protocol, not an open question.

#### Forward compatibility of `wip_aux`

The `wip_aux` mode space is open: releases after 0.4.0 may define new modes in the reserved encoding (the `0b11` extension tag has ample sub-mode room). Two rules keep this safe across versions:

1. **Unrecognized modes recover by default.** A program that opens a file armed with a `wip_aux` mode it does not recognize does not guess at the staging format. It applies the **default recovery** — roll back to the last committed `clen` (truncate the tail, clear the journal), abandoning the in-flight operation as though the crash had landed one step earlier.

2. **Data-lossy modes bump the version.** The default is safe only for modes whose abandonment cannot lose committed data — modes that stage their result outside `[0, clen)` and commit with a single atomic header flip, so rolling back merely discards uncommitted scratch. A mode that overwrites committed bytes in place before committing (same-length `set`, `splice`, `repeat`, and `copy` all do) cannot be finished by a program that does not understand it, and rolling it back would leave a torn region. **Introducing such a mode bumps the minor version** (`0.4.0 → 0.5.0`, and so on), which changes the magic so older releases refuse the file at open rather than corrupting it.

Together these guarantee that whenever the default path actually runs, the mode it is defaulting on is non-destructive, so no data is lost; and any destructive new mode is gated behind a magic that older releases reject. It is the same mechanism that protects the 0.4.0 journal itself: 0.4.0's own destructive modes — same-length `set`, splice, repeat-fill, and copy — all have in-place recovery a 0.1.x reader could not perform, and the single 0.4.0 magic bump away from 0.1.x already keeps the file out of its hands. The rule bites again only for a *further* destructive mode introduced after 0.4.0.

#### Migration from 0.1.0 files

Open old-magic files in a compatibility mode that rewrites them to the 0.4.0 layout: write the new header and payload into a sibling file, then `rename` into place (atomic within a filesystem on POSIX). One-shot per file; cost is proportional to file size. Document this in the changelog so users with large files aren't surprised.

#### Derived atomicity beyond 8 B

The 8 B primitive is the conservative baseline, but bstack's stricter runtime guarantees — serialized writers and an explicit `sync` before any pointer update — unlock broader atomicity on real storage. Because the OS maps a file's first byte to the beginning of a sector-aligned block, any write that is confined to one such aligned block is effectively atomic at the storage level: the block either lands in full or does not land at all. Two useful primitives follow:

1. **Aligned-block write (≤ n bytes).** A write starting at any offset that is a multiple of the system block size, and whose length does not cross the next block boundary, is a single atomic unit. The maximum safe `n` is the system block size (4 KB on most modern systems); 256 B is a conservative upper bound that holds on virtually all hardware, including eMMC and older NVMe controllers that advertise larger blocks but only guarantee 512 B or 256 B power-fail atomicity.

2. **Header writes.** The bstack header lives at offset 0 — the very start of the first block. Any write to the header that stays within the first 256 B (or the actual block size, if known) is therefore automatically an aligned-block write and inherits the same atomicity. This means a future design could stage a richer set of header fields — not just `wip_ptr` — in a single atomic step without requiring a separate sync between them, as long as the total updated region stays within one block.

3. **Inline data (filesystem-level).** Some filesystems (ext4 with `inline_data`, APFS with inline extents for small files, btrfs inline extents) store the file's payload directly inside the inode structure when the file is small enough to fit. This has two competing effects on the atomicity picture. On one hand, the block-alignment argument above rests on the file's first byte mapping to the start of a dedicated data block; with inline data there is no separate data block, so that specific alignment guarantee no longer holds. On the other hand, the entire file — header and payload together — now lives within a single inode block, which the filesystem must write atomically when flushing that metadata block. If the inode block itself is treated as the atomic unit (as journaling filesystems generally guarantee for metadata), then *all* in-file offsets become co-located in the same atomic region, potentially offering stronger end-to-end atomicity than the aligned-data-block argument alone. Whether this guarantee is durable across power loss (versus only crash-consistent) depends on the filesystem's journal mode (`data=writeback` vs `data=ordered` vs `data=journal` on ext4, for example) and is not safe to assume without explicit detection or documentation of the target filesystem.

#### Multi-write journaling

The single-write journal treats `wip_ptr` and `clen` as a two-field descriptor for one (range, payload) pair. To atomically apply `k` non-overlapping writes `{(s_i, e_i, data_i)}` in a single crash-safe step, the design generalises by replacing that two-field descriptor with a concatenated sequence of variable-length blocks appended into the same tail region, using `wip_aux` — the header field after `wip_ptr` — as the state discriminant while `wip_ptr` remains `0` throughout.

**Tail layout.** Starting at `clen`, blocks are packed back-to-back with no padding or explicit links. Each block is self-delimiting: the header fields `s_i` and `e_i` encode the target range, and the block's payload length is simply `e_i − s_i`.

```
[head + 0  .. head + 8)              →  s_i   (start of target range)
[head + 8  .. head + 16)             →  e_i   (end of target range)
[head + 16 .. head + 16 + (e_i−s_i)) →  data_i (payload bytes)
```

The next block begins immediately after the previous payload. The sequence runs from `clen` to `file_size`; no explicit count or end pointer is needed — `file_size` is the boundary.

**Protocol.**

1. **Stage.** Append blocks one by one to the tail (`file_size` grows with each append). The header is not touched; `wip_ptr` and `wip_aux` remain `0`.
2. **Arm.** Set `wip_aux` to the *intent-complete* sentinel. Sync.
3. **Replay.** Scan the sequence from `clen` to `file_size`. For each block, copy `data_i` into `[s_i .. e_i)` in the payload region. Order within the replay does not matter so long as the ranges are non-overlapping.
4. **Disarm.** Clear `wip_aux = 0`. Sync. Truncate to `clen`.

**Recovery.** The existing `wip_ptr == 0` rule (truncate to `clen`) already handles a crash during step 1: no header sentinel was set, the tail is partial, and truncation discards it silently — identical to having never started. The only new recovery case is:

- `wip_ptr == 0` and `wip_aux == intent-complete sentinel` → all blocks are fully staged; scan the sequence from `[clen .. file_size)` and replay each block, then disarm as in step 4.

Every other combination is handled by the existing rules. The sentinel value for `wip_aux` in the multi-write case must be chosen so that it cannot be confused with the non-zero `wip_aux` values used by splice (which only appear when `wip_ptr != 0`); since the `wip_ptr == 0` prefix already disambiguates, any non-zero value is valid, but an explicit reserved constant (e.g., `u64::MAX`) is cleaner.

**Constraint: no file-size-changing operations may be compounded with a multi-write.** The protocol relies on `clen` as the fixed start of the staging region and `file_size` as its end. Any operation that moves either boundary — `push`, `pop`, extension of the stack, or truncation/discard — would corrupt the staging region or make it unreadable during recovery. Multi-write is therefore strictly limited to in-place mutations of existing payload bytes; it cannot be batched together with operations that grow or shrink the file.

#### Durability barriers

Crash-safety depends on three real barriers, in order: stage→arm, arm→in-place, in-place→disarm. Without any of these, recovery can observe a header that disagrees with on-disk content. Each "sync" step above is the existing `durable_sync` primitive (already `F_FULLFSYNC` on macOS with fdatasync fallback, `fsync` elsewhere); no new platform handling is introduced — the journaling protocol only adds barriers at the three transitions above.

### Open questions

- **Aligned-block atomicity in `set`.** If the target slice fits within a single aligned block (i.e. `n ≤ block_size` and the write does not cross a block boundary), the write is atomic at the storage level and the `wip` journal could be skipped entirely — reducing a 4-sync protocol to a single write+sync. Determine whether this optimization is safe and, if so, define the precise size and alignment conditions under which `wip` is unnecessary.

---

## Requiring `&mut BStackSlice` for mutation (0.4.0)

**Feature flag:** `set`
**Breaking change:** Yes

### Motivation

All write methods on `BStackSlice` — `write`, `write_range`, `zero`, `zero_range`, `writer`, and `writer_at` — currently take `&self`. This is because `BStack` uses interior mutability (`RwLock<File>`), so no exclusive reference to the slice is needed at the Rust level to perform the underlying I/O. However, this means a shared, copied, or aliased `BStackSlice` can silently mutate the same region of the file from multiple places, with no indication at the call site that mutation is occurring. This is surprising and contrary to Rust conventions.

Requiring `&mut self` for write methods makes mutation visible and explicit. It does not provide exclusive access to the underlying file region — `BStackSlice` is `Copy` and the file is shared — but it signals intent and prevents accidental mutation through a shared reference. It also lays the groundwork for future read-only slice types and borrow-checked slice access, where `&BStackSlice` and `&mut BStackSlice` could eventually carry stronger semantic guarantees.

### Design

Change the receiver of `write`, `write_range`, `zero`, `zero_range`, `writer`, and `writer_at` from `&self` to `&mut self`. Since `BStackSlice` is `Copy`, callers can always bind a local `mut` copy if needed — the change imposes no semantic restriction, only a mutability annotation.

The same change applies to the corresponding methods on `BStackGuardedSlice`, and to any future slice types such as `BStackVec`.

### Open questions

- Should `writer` and `writer_at` also require `&mut self`, given that they return a `BStackSliceWriter` rather than performing any mutation themselves? Requiring `&mut self` here is consistent with the general principle but may feel overly strict for a constructor that merely captures the slice.
- `BStackSliceWriter` is currently `Copy`. If `writer` requires `&mut self`, obtaining multiple writers from the same slice would require copying the slice first. Should `BStackSliceWriter` remain `Copy`, or should it be non-`Copy` to better reflect exclusive-write intent?

---

## Caching the logical length (`clen`) for lock-free `len`

**Feature flag:** None (additive API surface)
**Breaking change:** No (signature change to `len`/`is_empty` deferred — see open questions)

### Motivation

[`BStack::len`](src/lib.rs) currently takes the read lock and calls `File::metadata` to query the on-disk file size via `fstat`/`GetFileSizeEx`, then subtracts the header. This is correct, but it costs a syscall and an `RwLock::read()` acquisition on every call — even though `clen` only ever changes under the write lock, during `push`, `pop`, `pop_into`, `extend`, `discard`, `try_discard`, `set`, `splice`, `splice_into`, `atrunc`, and `replace`.

Mirroring the `locked` field (an `AtomicU64` already used for the locked-region fast path), an in-memory cache of `clen` updated by every write-lock-held operation that changes the payload length would let `len` and `is_empty` become a single uncontended `Ordering::Acquire` atomic load with no syscall and no lock acquisition. This matters for callers that poll `len`/`is_empty` frequently — e.g. to check capacity before each `push`/`pop`.

### Design

Add a `clen` field to `BStack`, seeded from the validated header during construction (after recovery has run, so the seed value is always the post-recovery `clen`). Every operation that commits a new `clen` to the on-disk header also updates `self.clen` in the same write-lock critical section, right after (or as part of) the durable header write.

#### Open question: `AtomicU64` vs. plain field under the existing `RwLock`

Two implementations are possible, and the choice is not obvious:

- **`AtomicU64` (lock-free).** `len` becomes a single uncontended load with no lock acquisition at all:

  ```rust
  pub fn len(&self) -> u64 {
      self.clen.load(Ordering::Acquire)
  }
  ```

  This mirrors `locked`, but `locked` needs lock-freedom because it is checked on hot read/write paths that otherwise avoid the rwlock entirely. `len` is not on such a path today.

- **Plain `u64` under the existing `RwLock`.** All `clen`-changing operations already hold the write lock when they would update the cache, so no new synchronisation is needed — the field is just read alongside the rest of the locked state:

  ```rust
  pub fn len(&self) -> u64 {
      self.lock.read().unwrap().clen
  }
  ```

  This keeps `len` exactly as cheap as today *minus the `metadata` syscall*, with no atomic-ordering reasoning required, at the cost of retaining the (uncontended) lock acquisition.

The atomic avoids that lock acquisition but introduces an `Ordering::Release`/`Acquire` pair and a second source of truth that must be kept in lockstep with the header write at every call site — more places to get wrong for a cache whose main competing cost (the syscall) is removed either way. The plain-field approach is simpler and lower-risk, and matches the existing pattern that `len` is already a locking operation. Given the modest further win of full lock-freedom versus the simplicity of reusing the existing lock, the plain-field approach is the better default unless profiling shows the read-lock acquisition itself is a measurable cost for `len`-polling callers.

### Concurrency considerations

- The cache reflects the *logical* post-operation `clen`, not necessarily a value that has reached durable storage at the instant of the store — consistent with how `locked` already provides a logical, non-durability-tied guarantee rather than a crash-safety one.
- Recovery already computes the correct `clen` from the header and file size at construction time; the cache is seeded from that result and introduces no new recovery logic.
- Bypassing `BStack` to truncate or extend the backing file directly is already outside its supported usage and would desynchronise the cache from the file regardless of this change.

### Open questions

- **Return type.** `len` and `is_empty` currently return `io::Result<u64>` / `io::Result<bool>` to surface `File::metadata` errors. With a lock-free atomic load, no I/O occurs and no error can arise. Dropping `io::Result` is a breaking signature change — it could be bundled with another 0.4.0 breaking release, or `io::Result` could be retained (always `Ok`) for source compatibility until then.

---

## `BStackUninitAllocator` extension trait for uninitialised allocation

**Feature flag:** None (additive API surface)
**Breaking change:** No

### Motivation

`BStackAllocator::alloc` guarantees that the returned region is zero-initialised, and `realloc` zero-fills any newly added bytes when growing. This guarantee costs a write: a fresh region pulled from the free list may hold leftover bytes from a previous allocation, and the allocator must zero it explicitly (e.g. `first_fit`'s `alloc` builds a zeroed buffer of the full block size before writing it).

Many callers immediately overwrite the entire region with `write` right after `alloc` — for example, a caller that allocates a block solely to `write` a serialized record into it has no use for the zero-fill, since every byte will be overwritten before being read. For these callers, the zero-fill is pure overhead: an extra full-region write (and, depending on the allocator, a larger I/O than the eventual payload write).

An opt-in `alloc_uninit`/`realloc_uninit` pair lets such callers skip the zero-fill. The returned region's bytes are **unspecified** — they may be zero, or may be leftover bytes from a previous allocation that occupied the same on-disk space — but they are always valid `u8` values, so reading them before writing is safe (no undefined behavior, unlike `MaybeUninit<u8>` in memory). This mirrors `Vec::with_capacity` followed by `set_len`, except no analog of `set_len` is needed since the bytes are always valid to read, just unspecified in value.

### Design

```rust
/// Extension trait for allocators that can skip zero-initialisation of newly
/// allocated or grown regions.
///
/// The bytes in a region returned by [`alloc_uninit`](Self::alloc_uninit), or
/// in the newly added portion of a region returned by
/// [`realloc_uninit`](Self::realloc_uninit), are **unspecified**: they may be
/// zero, or may be leftover bytes from a previous allocation that occupied the
/// same on-disk space. They are always valid to read — no undefined behavior
/// results — but callers must not rely on their value until they have written
/// to the region themselves.
pub trait BStackUninitAllocator: BStackAllocator {
    /// Allocate `len` bytes without zero-initialising them.
    ///
    /// Equivalent to [`alloc`](BStackAllocator::alloc) except that the
    /// returned region's contents are unspecified rather than guaranteed
    /// zero. `len = 0` is valid.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` on failure.
    fn alloc_uninit(&self, len: u64) -> Result<Self::Allocated<'_>, Self::Error>;

    /// Resize the region described by `handle` to `new_len` bytes without
    /// zero-initialising any newly added bytes.
    ///
    /// Equivalent to [`realloc`](BStackAllocator::realloc) except that, when
    /// `new_len` is larger than the current length, the contents of the added
    /// bytes are unspecified rather than guaranteed zero. Shrinking is
    /// unaffected, as no new bytes are introduced.
    ///
    /// # Slice origin requirement
    ///
    /// Same requirement as [`realloc`](BStackAllocator::realloc): `handle`
    /// must have been returned by [`alloc`](BStackAllocator::alloc),
    /// [`alloc_uninit`](Self::alloc_uninit), [`realloc`](BStackAllocator::realloc),
    /// or [`realloc_uninit`](Self::realloc_uninit) on this same allocator
    /// instance.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` on failure, including when the implementation
    /// does not support reallocation.
    fn realloc_uninit<'a>(
        &'a self,
        handle: Self::Allocated<'a>,
        new_len: u64,
    ) -> Result<Self::Allocated<'a>, Self::Error>;
}
```

Allocators implement this trait by reusing their existing free-list/extension logic for `alloc`/`realloc`, but skipping the zero-fill step before returning the handle. For an allocator whose growth always goes through `BStack::extend`, the newly extended tail is already zero (via `set_len` on a sparse file), so `alloc_uninit` may be no cheaper than `alloc` in that path — the savings are concentrated in the free-list-reuse path, where a previously-occupied block is handed back without being scrubbed first.

Implementing this trait is optional. Allocators for which zero-fill is already free (e.g. always-extend bump allocators) may implement it as a thin wrapper around `alloc`/`realloc` with no savings, or simply not implement it at all.

### Open questions

- **Default implementations.** Could `BStackAllocator` provide default `alloc_uninit`/`realloc_uninit` implementations that simply delegate to `alloc`/`realloc` (i.e., always zero), with `BStackUninitAllocator` only needed as a marker for allocators that actually skip the zero-fill? This would let generic code call `alloc_uninit` without a separate trait bound, at the cost of the trait no longer signalling "this allocator supports the fast path."
- **New Type** Should the unspecification contract be encoded in the type system, allowing the return to be `Self::UninitAllocated<'a>` instead of `Self::Allocated<'a>`? This would make it impossible to accidentally treat an uninitialised allocation as a normal one, but it would also require more boilerplate for callers who want to use the fast path, as they would need to convert from `UninitAllocated` to `Allocated` after writing to the region, or an allocator could simply set `Self::UninitAllocated<'a>` to `Self::Allocated<'a>` if no such distinction is needed.

---

## Typed region and I/O parameter types

**Feature flag:** None (additive API surface)
**Breaking change:** No

### Motivation

Many BStack and `BStackSlice` APIs share recurring parameter patterns that are passed as raw function arguments today:

- `(offset: u64, buf: impl AsRef<[u8]>)` — a write region: a position and the data to write there.
- `(offset: u64, buf: &mut [u8])` — a read region: a position and a buffer to fill; the length is implied by `buf.len()`.
- `(offset: u64, len: u64)` — a named range: a position and a byte count, with no associated data.
- `(a: u64, b: u64, len: u64)` — a cross-region pair: two positions and a shared length, as in `cross_exchange`.

These patterns appear in `set`, `get`, `get_range`, `zero`, `zero_range`, `cross_exchange`, `BStackOp::Read`, `BStackOp::Write`, and the generator closures for `get_batched_gen` and `process_gen`. Callers must remember the argument order and meaning at every call site. There is no type-level distinction between "an offset into this stack" and "a length", both of which are `u64`.

Introducing named types for these patterns — for example, `ReadRegion`, `WriteRegion`, `ByteRange`, and `CrossRegion` — would make call sites self-documenting and allow the compiler to reject transposed arguments.

### Design (sketch)

The types would be lightweight, `Copy` structs:

```rust
pub struct ByteRange       (Range<u64>);
pub struct ReadRegion<'a>  { pub offset: u64, pub buf: &'a mut [u8] }
pub struct WriteRegion<'a> { pub offset: u64, pub data: &'a [u8] }
pub struct CrossRegion     { pub src: u64, pub dst: u64, pub len: u64 }
```

Existing methods would gain overloaded entry points accepting these types via `Into` or `From`, or new companion methods alongside the originals. `BStackOp` variants could also be restructured around them. No existing call site would break.

### Open questions

- **Is the benefit real?** The patterns already have named parameters in Rust function signatures (`offset`, `buf`, `len`), so transposition is not silent — the compiler infers types, and `buf: impl AsRef<[u8]>` versus `len: u64` are already distinct. The main gain is readability at call sites, which is a matter of taste.
- **Proliferation cost.** Four new public types add documentation surface, appear in error messages, and must be maintained indefinitely. If the API grows, more pattern types may follow.
- **Naming.** The names above are illustrative. Alternatives: `Span`, `Slice` (conflicts with `BStackSlice`), `Region`, `Segment`. The right name should not collide with existing types and should signal that these are I/O coordinates, not data containers.
