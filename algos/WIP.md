# Write-In-Progress (WIP) Journal

This document specifies every mode of the BStack write-in-progress journal: the
on-disk layout, each mode's protocol, crash-safety argument, recovery procedure,
and the write-strategy hierarchy that selects among them. It is the authoritative
reference for `io_core.rs`.

---

## Header layout

The 32-byte file header encodes the journal:

```
offset   field       type       meaning
------   -----       ----       -------
 0 .. 8  magic       [u8; 8]    format-version discriminant
 8 ..16  clen        u64 LE     committed payload length
16 ..24  wip_ptr     u64 LE     journal target (0 = disarmed)
24 ..32  wip_aux     u64 LE     journal mode (0 = Set; meaningful only when wip_ptr != 0,
                                except for the multi-write sentinel)
```

At rest: `wip_ptr == 0` and `wip_aux == 0`. `clen` is the only payload field
that changes during a `push`/`pop`; neither `wip_ptr` nor `wip_aux` is ever
touched by a size-changing operation. `file_size == 32 + clen` is the at-rest
invariant.

All three header fields — `clen`, `wip_ptr`, `wip_aux` — live in the first 32
bytes (one aligned block). Writes that span any two of them are issued as a
single `write_all` to that prefix, so the fields can never be observed
partially updated by a crash.

---

## WipAux mode encoding

```
WipAux value     Mode
-----------      ----
0                Set          — same-length verbatim replay
u64::MAX - 1     SpliceGrow   — length-growing tail replace (clen' > clen)
u64::MAX - 2     SpliceShrink — length-shrinking tail replace (clen' < clen)
u64::MAX - 3     Repeat       — repeating-pattern fill
u64::MAX - 4     Copy         — disjoint in-file copy (coordinate-only staging)
u64::MAX - 5     MultiWrite   — multi-write intent-complete sentinel
other            unknown      — roll back on recovery
```

Non-zero modes take values near `u64::MAX`, decrementing as modes are added,
so the low-value range is free for future packed encodings and unrecognized
values are unmistakable. `u64::MAX` should never be used. An unrecognized value
is always treated as unknown and triggers rollback. All modes in the current list
shipped in 0.4.0, so every 0.4.0 reader recognizes all of them.

---

## Derived atomicity — skipping the journal entirely

Before reaching for a journal, a write that is confined to a single power-fail
atomic aligned block can be written atomically at the storage level: the block
either lands in full or not at all.

**Why 256 bytes.** Real storage writes whole sectors atomically. Devices differ
(commonly 512 B or 4 KB), but 256 B is a lower bound that holds on virtually
all hardware, including eMMC and older NVMe controllers that advertise larger
blocks yet only guarantee 512 B or 256 B power-fail atomicity. Because 256 B
divides those sizes and the file's first byte is block-aligned, a write
confined to one 256 B-aligned region is always contained within a single
hardware block.

**The header is always atomic.** Offsets 0–31 lie in the first block.
`write_wip` (16 bytes at offset 16) and `write_header_commit` (24 bytes at
offset 8) are both confined to the first 256 B, so `wip_ptr` and `wip_aux`
can never tear apart, and a splice's combined `(clen', wip_ptr=0, wip_aux=0)`
commit is also a single atomic unit.

**Inline-data caveat.** On filesystems that store small files inline in the
inode (ext4 `inline_data`, APFS inline extents, btrfs inline extents) there is
no separate data block and the block-alignment argument does not apply. The
journal remains correct; only the fast path (skip journal for aligned writes)
may be overly conservative or overly optimistic depending on the filesystem's
specific atomicity guarantees. BStack does not attempt to detect this and
always applies the 256 B rule.

**Predicate.** `is_atomic_write(offset, len)` returns `true` iff the physical
range `[32 + offset, 32 + offset + len)` is confined to one 256 B block:

```
(32 + offset) / 256 == (32 + offset + len - 1) / 256   (len > 0)
```

When this holds, a plain `write_at` + `durable_sync` is the only step needed.

---

## Durability barriers

Crash-safety depends on three ordered barriers in every journaled write:

1. **Stage → Arm.** The staged tail must be durable before the journal is
   armed. A crash between them leaves `wip_ptr == 0`; recovery rolls back.
2. **Arm → In-place write.** The journal must be durable before the in-place
   write begins. A crash between them leaves the journal armed but nothing
   written; recovery replays and reaches the same result.
3. **In-place → Disarm.** All in-place writes must be durable before the journal
   is cleared. A crash between them leaves the journal armed; recovery replays
   (idempotent) and disarms.

Each "sync" is `durable_sync`: `fcntl(F_FULLFSYNC)` on macOS (falls back to
`fdatasync`), `fdatasync` on other Unix, `FlushFileBuffers` on Windows.

---

## Choosing a write strategy

Every mutation picks the cheapest correct strategy. Evaluated in order; first
match wins:

1. **Derived atomicity.** `is_atomic_write(offset, len)` → plain `write_at` +
   sync. No journal, no `clen` change, no file-size change.
2. **Already safe by construction.** `push`/`pop`/`extend`/`discard` (and
   `resize`/`ensure`/`ensure_with`, which dispatch to these) commit through a
   single atomic `clen` write. A crash rolls back by truncation. Never arm `wip`.
3. **Compact-journal mode.** The preconditions for `Repeat` or `Copy` hold
   (repeating pattern, or disjoint source) → use those modes; the tail is
   `O(1)` metadata, not `O(n)` bytes.
4. **General single-region journal.** Stage the full new bytes in the tail →
   arm → replay → disarm. Used by `Set` and `Splice`.
5. **Multi-write journal.** Several non-overlapping in-place writes must commit
   atomically. Stage all blocks, arm the multi-write sentinel, replay, disarm.

---

## Mode: Set

**Backs:** `set`, `swap`, `cas`, `copy` (overlapping), `cross_exchange`,
`process`, and the same-length tail case of `atrunc`/`splice`/`replace`.

**Precondition:** `offset + n <= clen`, `n > 0`, `is_atomic_write` is false.

### Protocol

Let `P = clen` (current payload length), the staging base.

1. **Stage.** Append the new bytes `dn[0..n]` at physical `[32+P, 32+P+n)`.
   (`write_at(file, P, dn)`) → sync.
2. **Arm.** `write_wip(file, 32 + offset, WipAux::Set)` → sync.
3. **Commit.** `write_at(file, offset, dn)` → sync.
4. **Disarm.** `write_wip(file, 0, WipAux::Set)` → sync.
5. **Truncate.** `file.set_len(32 + P)`.

### Crash-safety

| Crash point | On-disk state | Recovery |
|-------------|---------------|----------|
| During step 1 | `wip_ptr == 0`, tail partial | base rule: truncate to `32 + clen` — discards tail, old value intact |
| Between 1 and 2 | `wip_ptr == 0`, tail staged | same — truncate, old value |
| During step 2 | `wip_ptr` either 0 or `32+offset` (single block write) | either old value (if 0) or armed → replay |
| After arm, before step 3 | `wip_ptr != 0`, in-place bytes unchanged | replay: copy tail into `[offset, offset+n)` — idempotent |
| During step 3 | `wip_ptr != 0`, in-place partial | replay — idempotent, tail unchanged |
| Between 3 and 4 | `wip_ptr != 0`, in-place complete | replay — idempotent |
| During step 4 | either armed or disarmed (single-block write) | if armed → one more replay; if disarmed → truncate is a no-op |
| After step 4 | `wip_ptr == 0`, tail still present | base rule: truncate — new value already in place |

The staged tail is immutable after sync in step 1 and disjoint from `[offset,
offset+n)` (it sits beyond `clen`), so every replay produces exactly the same
result regardless of how far step 3 had progressed.

### Recovery (`wip_ptr != 0`, `wip_aux == Set`)

`tail_start = 32 + clen`; `tail_len = file_size - tail_start`.

Validate: `tail_len > 0`, `wip_ptr >= 32`, `wip_ptr + tail_len <= tail_start`
(the target lies wholly within the committed payload). If valid: `move_chunked`
from `tail_start` into `wip_ptr` for `tail_len` bytes → sync. Then finalize.

---

## Mode: Repeat

**Backs:** `zero`, `repeat`.

**Precondition:** `offset + k * s.len() <= clen`, `s.len() >= 1`, `k >= 1`,
`is_atomic_write` is false.

### Protocol

Let `P = clen`. The tail stores `[k: u64 LE | s]` — 8 + `s.len()` bytes,
independent of the fill length.

1. **Stage.** Write `k` as u64 LE then `s` at `[32+P, 32+P+8+s.len())` → sync.
2. **Arm.** `write_wip(file, 32 + offset, WipAux::Repeat)` → sync.
3. **Fill.** `write_repeated(file, 32+offset, s, k)` — streams in chunks, O(s.len()) memory → sync.
4. **Disarm.** `write_wip(file, 0, WipAux::Set)` → sync.
5. **Truncate.** `file.set_len(32 + P)`.

The win: staging is `8 + s.len()` bytes regardless of the fill size. `zero` is
`repeat` with `s = [0x00]`; staging is always 9 bytes.

### Crash-safety

Same argument as Set. The tail is immutable (k, s) and disjoint from the fill
target. Every replay of step 3 writes the same pattern — idempotent.

### Recovery (`wip_ptr != 0`, `wip_aux == Repeat`)

Read `k` (8 bytes at `tail_start`), `s` (remaining `tail_len - 8` bytes).
Validate: `tail_len >= 8`, `s.len() > 0`, `wip_ptr >= 32`,
`wip_ptr + k * s.len() <= 32 + clen`. If valid: `write_repeated` → sync.
Then finalize.

---

## Mode: Copy

**Backs:** `copy` (disjoint source and destination only).

**Precondition:** `[src, src+n)` and `[dst, dst+n)` are disjoint, both within
`[0, clen)`, `n > 0`, `is_atomic_write(dst, n)` is false.

### Protocol

Let `P = clen`. The tail stores `[src: u64 LE | n: u64 LE]` — exactly 16 bytes.
`wip_ptr` names the destination (the write target, consistent with all modes).

1. **Stage.** Write `[src | n]` at `[32+P, 32+P+16)` → sync.
2. **Arm.** `write_wip(file, 32 + dst, WipAux::Copy)` → sync.
3. **Copy.** `move_chunked(file, src, dst, n)` → sync.
4. **Disarm.** `write_wip(file, 0, WipAux::Set)` → sync.
5. **Truncate.** `file.set_len(32 + P)`.

The win: staging is always 16 bytes regardless of `n`, because the source
`[src, src+n)` is committed data that the copy does not touch — it is its own
backup. This is the crucial difference from Set (which must copy the source into
the tail before writing, because the in-place write destroys the original).

**Disjointness is required.** If source and destination overlapped, the in-place
write would clobber source bytes before they were read, and a replay would read
corrupted data. Overlapping copies fall back to Set (stage the full bytes).
`cross_exchange` can never use Copy: each region is simultaneously a source and
a destination, so the same overlap argument applies.

**Same location is a no-op.** If `src == dst`, the copy leaves every byte
unchanged; it is skipped before any I/O.

### Crash-safety

| Crash point | Recovery |
|-------------|----------|
| During step 1 | `wip_ptr == 0` → truncate, old value intact |
| Between 1 and 2 | `wip_ptr == 0` → truncate, old value intact |
| After arm | `wip_ptr != 0, wip_aux == Copy` → read `[src|n]` from tail, `move_chunked(src → dst)` — idempotent because `src` is disjoint from `dst` and was never modified |
| After step 3 | armed but complete → one more (no-op) replay |
| After disarm | `wip_ptr == 0` → truncate the 16-byte tail |

### Recovery (`wip_ptr != 0`, `wip_aux == Copy`)

Read `src` and `n` (16 bytes at `tail_start`). Let `dst = wip_ptr - 32`.
Validate: `tail_len >= 16`, `n > 0`, `src + n <= clen`, `dst + n <= clen`,
`dst >= src + n` or `src >= dst + n` (disjoint). If valid:
`move_chunked(file, src, dst, n)` → sync. Then finalize.

---

## Mode: SpliceGrow / SpliceShrink

**Backs:** the length-changing case of `atrunc`, `splice`, `splice_into`,
`replace` (when `m != n_old` and `m > 0` and `n_old > 0`).

Let:
- `a = clen - n_old` — the splice point (start of the tail being replaced)
- `m = len(dn)` — length of the new tail bytes
- `clen' = a + m` — the new committed length
- `S = max(clen, clen')` — staging base (past both the old and the new payload end)

`wip_ptr` stores `32 + a` (the splice point as a physical offset, which lets
recovery derive `a`). The mode (`SpliceGrow` if `m > n_old`,
`SpliceShrink` otherwise) tells recovery which direction to expect; `clen'` is
**not** stored in the header — it is derived from the file size.

### Protocol

1. **Extend and stage.** `file.set_len(32 + S + m)`. Write `dn` at `[32+S, 32+S+m)` → sync.
2. **Arm.** `write_wip(file, 32 + a, direction)` → sync.
3. **Replay.** `move_chunked(file, S, a, m)` — streams `dn` from staging into `[a, a+m)` → sync.
4. **Commit + Disarm.** `write_header_commit(file, clen', 0, WipAux::Set)` — single
   24-byte write at offset 8: `[clen' | 0 | 0]`. Both the new length and the
   disarm land atomically → update in-memory `clen` → sync.
5. **Truncate.** `file.set_len(32 + clen')`.

### Why S is the staging base

`S = max(clen, clen')` ensures the staging region `[32+S, 32+S+m)` is disjoint
from both the live payload `[32, 32+clen)` and the replay target `[32+a, 32+a+m)`.
Specifically, `a + m = clen' <= S`, so `[32+a, 32+clen') ⊆ [32, 32+S)` and the
staging region starts at or beyond `32+S`. The source and destination of the
replay are disjoint, making `move_chunked` in step 3 a plain forward copy,
restartable from the start.

### Recovering `clen'` from the file size

Recovery does not read `clen'` from the header; it derives it from `file_size`
and the direction:

```
payload_end = file_size - 32          (= S + m, the amount of data in the file)
a           = wip_ptr - 32            (the splice point, from the armed wip_ptr)

SpliceGrow:   payload_end = 2*clen' - a    →  clen' = (payload_end + a) / 2
SpliceShrink: payload_end = clen + (clen' - a)  →  clen' = payload_end + a - clen
```

Validation before applying:
- `a <= clen` (splice point is within the committed payload)
- `clen' >= a` (new length is at least as large as the splice point)
- direction matches (`clen' > clen` for Grow, `clen' < clen` for Shrink)
- `S + m == payload_end` exactly (rejects the odd-sum case that integer division
  would otherwise round through for Grow, and double-checks consistency for Shrink)

If any check fails, roll back.

### Crash-safety

| Crash point | On-disk state | Recovery |
|-------------|---------------|----------|
| During step 1 | `wip_ptr == 0`, `clen` unchanged | base rule: truncate to `32+clen` |
| Between 1 and 2 | `wip_ptr == 0` | base rule: truncate |
| During step 2 | `wip_ptr` either 0 or `32+a` (single block) | either truncate or roll-forward |
| After arm, before step 4 | `wip_ptr != 0`, old `clen` in header | derive `clen'`, replay `move_chunked` — idempotent |
| During step 4 | `clen'` + disarm in one block write | either old `clen` (re-replay) or new `clen` + disarmed (truncate) |
| After step 4 | `wip_ptr == 0`, `clen = clen'` | base rule: truncate to `32+clen'` (drops staging bytes) |

The step-4 write sets `clen`, `wip_ptr`, and `wip_aux` in a single 24-byte
aligned-block write. By the derived-atomicity argument, these three fields
either all land or none do — there is no intermediate state in which `clen`
shows `clen'` while `wip_ptr` is still nonzero, or vice versa.

### Recovery (`wip_ptr != 0`, `wip_aux == SpliceGrow` or `SpliceShrink`)

Derive `clen'` as above. Validate. If valid: `move_chunked(file, S, a, m)` →
sync → `final_clen = clen'`. Then finalize (which commits `clen'` atomically
with the disarm).

---

## Finalize (common to all modes)

After a mode-specific replay, every recovery path ends with the same sequence:

```
write_header_commit(file, final_clen, 0, WipAux::Set)   // atomic: clen + wip disarm
durable_sync(file)
file.set_len(32 + final_clen)                           // truncate staging tail
durable_sync(file)
```

`final_clen == clen` for all modes except `SpliceGrow`/`SpliceShrink`, which
may have updated it. The disarm lands with the length in one block write.

---

## Multi-write journal

**Backs:** `set_batched` and `inplace_gen`, each once two or more disjoint
in-place edits have accumulated. Both fast-path a lone edit to the single-write
path and a zero-edit batch to a no-op, so this mode is reached only at two or
more blocks.

When several non-overlapping in-place writes must commit atomically, the
multi-write journal generalizes the single-region protocol. `wip_ptr` stays `0`
throughout; `wip_aux` carries the intent-complete sentinel (`MultiWrite`) once all
blocks are staged.

**Constraint:** only in-place mutations (`set`-equivalent writes). No size-
changing operations (`push`, `pop`, `extend`, `discard`, `splice`) may be
compounded with a multi-write. `clen` is the fixed start of the staging region;
`file_size` is its end; either moving would corrupt the staging region.

### Tail layout

Blocks are packed back-to-back starting at physical offset `32 + clen`:

```
[block start + 0  .. +8)              s_i   — start of target range (u64 LE)
[block start + 8  .. +16)             e_i   — end of target range   (u64 LE)
[block start + 16 .. +16 + (e_i-s_i)) data_i — new bytes
```

The next block begins immediately after `data_i`. The full sequence runs from
`32 + clen` to `file_size`; no explicit count or end pointer is needed.

### Protocol

1. **Stage.** Append blocks one by one to the tail. The header is not touched;
   `wip_ptr` and `wip_aux` remain 0. One sync after all blocks are appended
   suffices — the arm in step 2 is the commit point, so the staged blocks only
   need to be durable before it.
2. **Arm.** Write `wip_aux =  MultiWrite` (intent-complete sentinel), `wip_ptr`
   stays 0 — single header write → sync.
3. **Replay.** Scan `[32+clen, file_size)`. For each block, copy `data_i` into
   `[32+s_i, 32+e_i)`. Order is arbitrary as long as ranges are non-overlapping.
   → sync.
4. **Disarm.** Write `wip_aux = 0` → sync. Truncate to `32 + clen`.

### Recovery

- `wip_ptr == 0`, `wip_aux == 0` (steady state or crash during step 1): base
  rule — truncate to `32 + clen`. Partial tail discarded silently.
- `wip_ptr == 0`, `wip_aux == MultiWrite` (crash after arm): scan
  `[32+clen, file_size)`, replay each block, then disarm (step 4).

All other combinations are handled by the existing single-region rules (which
require `wip_ptr != 0`). The intent-complete sentinel is only valid when
`wip_ptr == 0`, so there is no confusion with any single-region armed state.

### Crash-safety

| Crash point | State | Recovery |
|-------------|-------|----------|
| During step 1 | `wip_aux == 0`, partial tail | truncate — old values intact |
| Between 1 and 2 | `wip_aux == 0`, full tail staged | truncate — old values intact |
| During step 2 | `wip_aux` is 0 or sentinel (single block) | if 0 → truncate; if sentinel → replay |
| After arm, during step 3 | sentinel, partial in-place writes | replay — ranges non-overlapping so replay is order-independent and idempotent |
| After step 3 | sentinel, all in-place complete | replay is idempotent → disarm → new values |
| After disarm | `wip_aux == 0`, tail still present | truncate — new values already in place |

---

## Forward compatibility of `wip_aux`

The mode space is open: future releases may define new modes by decrementing
from `u64::MAX - 5`. Two rules keep this safe across versions:

**Rule 1 — Unknown modes roll back.** A reader that sees an unrecognized
`wip_aux` does not guess at the staging format. It applies the default: roll back
to the last committed `clen` (truncate the tail, clear the journal), abandoning
the in-flight operation as though the crash had landed one step earlier.

**Rule 2 — Destructive new modes bump the minor version.** The default rollback
is safe only for modes that never overwrite committed bytes without also staging a
recoverable backup. A new mode that overwrites committed bytes in place before the
commit point (as Set, Repeat, Copy, and Splice all do) cannot be safely abandoned
by a reader that does not understand it — rolling it back would leave a torn
region. Introducing such a mode bumps the on-disk format version (minor bump
changes the magic), so older readers refuse the file at open rather than silently
corrupting it.

Together: whenever the default rollback fires, the mode is non-destructive and
no committed data is lost. Destructive modes are always gated behind a magic that
older readers reject. This is the same mechanism that protected the 0.4.0 journal
from 0.1.x readers (the 0.4.0 magic bump away from 0.1.x); the rule bites again
only for modes introduced after 0.4.0.

---

## Full recovery dispatch

Recovery runs once during `open`, under the write lock, before the `BStack` is
exposed to any caller. Reads only the header and the file size.

```
wip_ptr  wip_aux          action
-------  -------          ------
  0        0              steady state; if file_size > 32+clen truncate (partial push rolled back);
                          if file_size < 32+clen set clen = file_size-32 (partial pop rolled back)
  0        MultiWrite     multi-write: scan [32+clen, file_size), replay each block, finalize
  0        other          unknown multi-write sentinel: roll back (truncate to 32+clen)
  !=0      Set (0)        same-length set: replay staged tail into [wip_ptr, wip_ptr+tail_len), finalize
  !=0      Repeat         repeat-fill: read [k|s] from tail, write_repeated, finalize
  !=0      Copy           disjoint copy: read [src|n] from tail, move_chunked(src→wip_ptr-32), finalize
  !=0      SpliceGrow     derive clen', replay move_chunked(S→a), finalize with clen'
  !=0      SpliceShrink   derive clen', replay move_chunked(S→a), finalize with clen'
  !=0      other          unknown: roll back (finalize with clen unchanged)
```

All replay operations are idempotent: the staged tail is immutable and disjoint
from its target (or, for Copy, the source is disjoint from the destination and
was never modified). A crash during recovery is safe to re-run from the
beginning.

After recovery, `durable_sync` ensures the repaired state is on stable storage
before any caller can observe or modify the file.
