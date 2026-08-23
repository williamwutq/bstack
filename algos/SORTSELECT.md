# Sorting and Selection over a `BStack` Region

This advisory covers the record-level ordering operations on `BStackChunk`
(feature `alloc`, plus `set` + `atomic` for the mutating ones): the
single-operation `sort_by` / `sort_by_key` / `select_nth_by` /
`select_nth_by_key`, the read-only `binary_search_by[_key]`, and the
bounded-memory out-of-core `sort_partial_by` / `sort_partial_by_key` and
`select_nth_partial_by` / `select_nth_partial_by_key`. The focus is the
out-of-core operations: the model they build on, how they map onto `BStack`'s
crash-atomic primitives, why they converge, and one tempting block-merge
shortcut that is *unsound* and must not be used.

A `BStackChunk` views an aligned region as `n` fixed-stride records of
`chunk_len` bytes. All coordinates below are record indices in `[0, n)`; a byte
offset is `region_start + index * chunk_len`.

## The two regimes

| Operation                     | Memory                  | Atomicity        | Notes                                                                           |
|-------------------------------|-------------------------|------------------|---------------------------------------------------------------------------------|
| `sort_by` / `sort_by_key`     | O(region)               | single operation | reads the whole region into one `Vec<u8>`, permutes, commits with one `process` |
| `select_nth_by[_key]`         | O(region)               | single operation | `select_nth_unstable` on an in-memory proxy index, one `process`                |
| `binary_search_by[_key]`      | O(chunk_len)            | read-only        | O(log n) probed-chunk reads, never the whole region                             |
| `sort_partial_by[_key]`       | **O(1) in region size** | **per-step**     | in-place out-of-core merge sort; converges to fully sorted                      |
| `select_nth_partial_by[_key]` | **O(1) in region size** | **per-step**     | in-place out-of-core quickselect; converges to the selected partition           |

`sort_by` is optimal when the region fits memory: one region-sized read, one
crash-atomic write. Its bound is memory — a region too large for a single
`Vec<u8>` cannot be sorted this way. `sort_partial_by` lifts that bound at the
cost of whole-operation atomicity (see *Convergence*).

## Model: sorting a remote array through fixed windows

Treat the region as an array `A[0..n)` accessed only through a small resident
budget. The only charged operations are remote transfers; comparisons and
permutation *within* resident memory are free. `BStack` supplies exactly three
such operations, each individually crash-atomic:

| Model op                                            | `BStack` primitive        | Guarantee                                                                             |
|-----------------------------------------------------|---------------------------|---------------------------------------------------------------------------------------|
| `LOAD` a window, permute it freely, `STORE` it back | `process(start, end, f)`  | reads `[start,end)`, runs `f` on the bytes in memory, commits in one atomic operation |
| `RSWAP(i, j)` — swap two equal, disjoint ranges     | `cross_exchange(a, b, n)` | staged tail backup, single `wip_ptr` flip; crash rolls fully back or fully forward    |
| compare / move within a window                      | in-memory                 | free                                                                                  |

The budget is `SORT_WINDOWS * SORT_BLOCK_BYTES` (3 × 2048 = 6144 bytes), so the
block size is `K = max(1, SORT_BLOCK_BYTES / chunk_len)` records and no step ever
holds more than the budget resident. There is **no scratch region**: a
`BStackChunk` carries only `&BStack` and its own coordinates, no allocator. That
rules out the textbook out-of-place `(w−1)`-way merge sort (which needs an
`n`-sized second region) and forces an *in-place* method built on `RSWAP`.

## The algorithm: in-place bottom-up merge sort

`sort_partial_by` (see `merge_sort` / `imerge` in `src/alloc/chunk.rs`) is a
standard bottom-up merge sort whose every remote step is one of the three atomic
primitives above.

1. **Run formation.** Sort each `run0 = SORT_WINDOWS * K`-record window with one
   `process`. The region becomes `⌈n / run0⌉` sorted runs.
2. **Merge passes.** For `width = run0, 2·run0, 4·run0, …`, merge each adjacent
   pair of runs `[lo, mid)` and `[mid, hi)` in place until one run remains.

A pair is merged by `imerge`, a rotation merge (SymMerge):

- If `(hi − lo)` fits the budget, one `process` sorts the whole range. Because
  the range is two already-sorted runs, an in-memory sort settles it; this is
  the base case and keeps small merges to a single operation.
- Otherwise pick a pivot in the middle of the **larger** run, binary-search its
  partner position in the other run (`lower_bound` / `upper_bound`, each a
  handful of single-record reads), and rotate the two middle blocks past each
  other. A rotation is three range reversals, and each reversal is a sequence of
  single-record `cross_exchange` swaps. Then merge the two resulting halves the
  same way.

Ragged inputs (a record count that is not a multiple of `K`, or a final short
run) need no special case: `imerge` works at record granularity, and the
budget/`≤ 2`-record base case absorbs the remainder.

### Iterative, not recursive

`imerge` is written as a loop over an explicit work stack, not by recursion. At
each split it continues on the *smaller* half in place and pushes the *larger*
half, so the stack holds at most `O(log(hi − lo))` entries and there is no
call-stack depth to overflow even on a region far larger than memory. The stack
is a single `Vec` reused across every merge in the sort.

## Convergence and crash behavior

Every remote step — a run-forming `process`, a base-case `process`, a rotation
`cross_exchange` — is individually crash-atomic and **permutation-preserving**:
it only ever reorders the records it touches, never adds, drops, or duplicates
one. Therefore:

- On success the region is **fully sorted**.
- A crash, or an early return on a genuine I/O error, leaves the region as some
  *valid permutation* of the original records — a partially merged intermediate,
  never corrupted or lost data. "Partial" names this interruptible middle state,
  not a partially-ordered result on success.
- Re-running on any such permutation completes the sort. So as long as I/O
  eventually succeeds, **repeated calls converge to fully sorted**; one clean run
  suffices.

`sort_partial_by` is **not** stable: the out-of-core merge reorders equal-keyed
records. Use `sort_by` when the region fits memory and stability is required.

## Pitfall: block tail-order + single carry pass is UNSOUND

A well-known in-place 2-way merge shortcut is tempting and **must not be used
here**: reorder the `K`-record blocks of the two runs by each block's *tail*
(last) record with `RSWAP`s, then make one left-to-right "carry" pass that sorts
each overlapping two-block window. It promises O(n/K) block swaps instead of the
rotation merge's O(n log n) record swaps.

It is incorrect. A block ordered by its tail can still contain an element
smaller than elements in earlier blocks, and a single left-to-right pass can
never move that element leftward. Concretely, merging the sorted run
`A = [0 … 23]` (six blocks, `K = 4`) with the one-block run
`B = [105, 782, 827, 830]`: `B`'s tail is `830`, larger than every `A` block
tail, so tail-ordering places `B` last — stranding `105` at the end, where the
carry pass cannot pull it to the front. The published proof's step "tail-ordering
guarantees `head(c) ≤ w`" is false: blocks are ordered by tail, not head, so the
implication it relies on does not hold. This survives naive tests (small inputs
sort fine) and fails on the first block whose head and tail straddle another
block's range.

The rotation merge above avoids the trap entirely — it never assumes a block
occupies a contiguous slice of the sorted output — at the cost of moving records
rather than whole blocks. A correct block-granularity merge (internal-buffer /
Kronrod-style, which handles the "broken block") would restore the O(n/K)
transfer count and is the natural future optimization; the naive tail-order +
carry is not it.

## Complexity

For `n` records, budget `M` bytes, block `K = M/chunk_len` records:

| Phase                       | Transfers                                                           |
|-----------------------------|---------------------------------------------------------------------|
| Run formation               | `2n/K` block I/O (one `process` per `run0` window)                  |
| Merge, in-budget sub-ranges | one `process` per base case                                         |
| Merge, rotations            | O(n log n) single-record `cross_exchange` for the above-budget part |

The rotation cost is the price of a correct in-place merge without scratch and
with only an equal-length swap primitive. It is I/O-bound (each `cross_exchange`
carries its own durability barriers), so `sort_partial_by` is for regions that
genuinely exceed memory; prefer `sort_by` whenever the region fits.

## Selection

`select_nth_by[_key]` is single-operation: it runs `select_nth_unstable_by`
on an in-memory proxy index under one `process`, so it inherits `sort_by`'s
memory bound. `select_nth_partial_by[_key]` (see `select_nth` /
`partition_select` / `sample_pivot_index` in `src/alloc/chunk.rs`) is the
bounded-memory version, with the same budget and per-step crash-atomicity as
`sort_partial_by`. It does **not** reuse the run-and-merge machinery —
quickselect's whole point is to avoid sorting — so it is a distinct engine:
bounded-memory quickselect with a cross-region pivot sample.

### Rank-band invariant

Selection narrows an active record range `[lo, hi)` known to contain global rank
`n`, holding a **rank-band invariant**: every record left of `lo` is ≤ every
record in `[lo, hi)`, which is ≤ every record right of `hi`. It starts at
`[0, count)`; each round partitions `[lo, hi)` and discards the side that cannot
contain `n`, shrinking the band while the invariant holds. When the band fits
the budget, one `process` finishes it exactly.

### The loop

```text
lo, hi = 0, count
loop:
    if (hi - lo) * chunk_len <= budget:
        process([lo, hi)): select_nth_unstable_by(n - lo, cmp)   // exact, atomic
        return
    pivot = sample_median(lo, hi)          // bounded read (below)
    p     = partition(lo, hi, pivot)       // in-place, bounded, atomic per step
    if n < p { hi = p } else { lo = p + 1 }   // p is a fixed pivot record; exclude it
```

The base case is the sort's trick reused: a range that fits the budget is
settled by one in-memory `select_nth_unstable_by`. It is correct because the
band invariant makes `[lo, hi)` a contiguous rank slice, so its local
`(n − lo)`-th record is the global `n`-th. Only ranges larger than the budget
need out-of-core partitioning.

### Pivot selection — the cross-section pass

A good pivot keeps the band shrinking geometrically. Read a bounded **sample**
spread across `[lo, hi)` — one record every `(hi − lo) / S` positions for
`S = budget / chunk_len` samples, a single strided pass — hold it in one window,
and take its median as the pivot value (copied into a one-record pivot buffer).
This is the "pivot-selection pass across sections": the sample straddles every
section, so the median is representative without loading any section whole, and
median-of-sample makes catastrophic imbalance vanishingly unlikely on real data.

### Partition — bounded, in place, crash-safe

Partition `[lo, hi)` around the pivot **value** with a two-pointer scan:

- Scan from both ends for an out-of-place pair — a left record ≥ pivot and a
  right record < pivot — and swap it with one `cross_exchange` (atomic,
  permutation-preserving). Reads are block-buffered (load a `K`-record block,
  scan it in memory), so scanning costs `O((hi − lo) / K)` reads rather than one
  per record.
- Each swap is its own crash-atomic step, so a crash mid-partition leaves a
  valid permutation — never lost or duplicated data — and re-running from scratch
  completes (the goal is idempotent). `Err` only on a genuine
  `cross_exchange`/`process` I/O failure.

Memory stays within budget: a pivot record, one or two `K`-record scan blocks,
`O(1)` cursors — no scratch region, no allocator.

### Termination and equal keys

To guarantee progress, fix one pivot **record** at its final split position each
round (swap the chosen pivot to the end, partition, swap it to `p`) and recurse
into `[lo, p)` or `[p + 1, hi)`, excluding `p`. Each round then removes at least
the pivot, so the band strictly shrinks; with a sample-median pivot the shrink is
geometric (expected `O(log)` rounds), and the budget base case means any residual
imbalance only matters while the band still exceeds the budget.

Many equal keys are the one pathological case for two-way partitioning (each
round peels off only the pivot). A three-way (Dutch-flag / Bentley–McIlroy)
partition — `< pivot | == pivot | > pivot` — settles it: if `n` lands in the
equal zone, any record there is the answer; otherwise recurse into `<` or `>`,
dropping the whole equal run. Its out-of-core form keeps three cursors instead
of two, same atomic-swap discipline.

### Cost

| Phase                    | Reads                     | Swaps                                       |
|--------------------------|---------------------------|---------------------------------------------|
| Pivot sample (per round) | `S = budget / chunk_len`  | —                                           |
| Partition (per round)    | `O((hi − lo) / K)` blocks | `O(hi − lo)` single-record `cross_exchange` |
| Base case                | one `process`             | —                                           |

Rounds shrink the band geometrically in expectation, so summed reads are
`O(count / K)` and swaps `O(count)` — linear, like in-memory quickselect, with
the same fsync-per-swap caveat as the sort's rotation merge. The single-record
swaps are the natural target for the same `set_batched` two-block batching
optimization noted for the merge, deferred for the same reason.

### Surface

The split mirrors the sort: `select_nth_by[_key]` stays single-operation for
regions that fit memory, and `select_nth_partial_by[_key]` is the shipped
bounded-memory variant. The current implementation uses the two-way (Lomuto)
partition above; the three-way partition for heavy-duplicate regions remains the
open optimization, alongside the shared `set_batched` block-batched-swap idea.
