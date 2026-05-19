# Planned Features

This document outlines upcoming features planned for the `bstack` crate. These enhancements aim to improve usability, performance, and integration while maintaining the core principles of durability, crash-safety, and simplicity. Changes aim to be backward-compatible. New features are suggested to be added as optional features under feature flags and new traits, instead of modifying existing ones, to avoid breaking changes. All features aim to follow [Rust's API design guidelines](https://rust-lang.github.io/api-guidelines/) and BStack's design principles.

---

## Making `BStackAllocator::realloc` and `dealloc` `unsafe fn`

**Feature flag:** N/A (core trait)
**Breaking change:** Yes — all callers and all `impl BStackAllocator` blocks must change

### Motivation

Both `realloc` and `dealloc` carry a *slice-origin requirement* that the
type system cannot enforce: the supplied handle must have been returned by
`alloc` or a prior `realloc` on the **same allocator instance**.  Passing an
arbitrary slice — constructed via `BStackSlice::from_raw_parts` or derived
via `subslice` / `subslice_range` — may silently corrupt the allocator's
persistent on-disk metadata (free-list pointers, AVL node fields, block
headers/footers) in a way that is difficult or impossible to recover from.

This is structurally identical to why `std::alloc::GlobalAlloc::dealloc` and
`realloc` are `unsafe fn`: the contract cannot be expressed as a type
invariant and violation causes severe, irreversible state corruption.

### Design

Change the trait signatures:

```rust
// Before
fn realloc<'a>(&'a self, handle: Self::Allocated<'a>, new_len: u64)
    -> Result<Self::Allocated<'a>, Self::Error>;
fn dealloc(&self, handle: Self::Allocated<'_>) -> Result<(), Self::Error>;

// After
unsafe fn realloc<'a>(&'a self, handle: Self::Allocated<'a>, new_len: u64)
    -> Result<Self::Allocated<'a>, Self::Error>;
unsafe fn dealloc(&self, handle: Self::Allocated<'_>) -> Result<(), Self::Error>;
```

All internal implementations (`LinearBStackAllocator`, `FirstFitBStackAllocator`,
`GhostTreeBstackAllocator`) would be updated to `unsafe fn` as well.

### Open questions

**Is this change actually necessary given `BStackSlice::from_raw_parts` is
already `unsafe`?**

Since `BStackSlice::new` is now deprecated and `from_raw_parts` is `unsafe`,
a caller cannot construct an allocator-corrupting slice *without already being
in an `unsafe` block*.  A well-reviewed `unsafe` block that constructs a
`BStackSlice::from_raw_parts` can be expected to read the safety contract and
comply with the origin requirement.

The remaining concern is sub-slices: `subslice` and `subslice_range` are *safe*
functions that produce slices with an origin different from any allocator-returned
handle.  A caller could accidentally (or intentionally) pass such a sub-slice to
`dealloc` without any `unsafe` at the call site.  Making `dealloc` / `realloc`
`unsafe fn` would force that call site into an `unsafe` block and require the
caller to explicitly reason about validity.

Counter-argument: the pain of a breaking change across every `impl BStackAllocator`
and every call site is high, and the hazard window is now significantly narrowed
because constructing a bad handle already requires `unsafe`.  A simpler alternative
is to mark `subslice` and `subslice_range` with a prominent warning that the
returned slices must not be passed to `realloc` or `dealloc`.

---

## Optimizing `FirstFitBStackAllocator` with atomic feature

**Feature flag:** `atomic`
**Breaking change:** No (if added as optional)

### Motivation

The `FirstFitBStackAllocator` could benefit from atomic operations to improve performance and thread-safety in concurrent environments. Atomic operations can reduce contention and allow for lock-free or reduced-lock implementations in certain scenarios. It also allows for better crash resilience by ensuring that metadata updates are atomic, reducing the risk of corruption in the event of a crash.

### Design

[To be determined — implementation details would involve using atomic primitives for metadata updates and allocation tracking.]

### Open questions

- Should this optimization be added as an optional feature flag, or required for all users? If added, we end up maintaining two implementations of `FirstFitBStackAllocator`; if required, all users need the atomic flag.

---

## Deprecating `BStackGuardedSlice::as_slice` in favor of read-only access

**Feature flag:** `guarded`
**Breaking change:** Yes — callers using `as_slice` would need to migrate to alternative APIs

### Motivation

The `as_slice()` method on `BStackGuardedSlice` returns a `Result<BStackSlice<'a, A>, io::Error>`, exposing the underlying slice directly. This creates a potential hazard: callers can use the returned slice to bypass the guard's hook system entirely, writing directly to the underlying `BStack` and potentially corrupting data that the guard was meant to protect (e.g., encrypted data written as plaintext, compressed data written uncompressed).

While such misuse does not compromise allocator structure or create memory-safety issues (hence `as_slice` is not `unsafe`), it can lead to logical data corruption that violates the guard's invariants. For example:

- An encryption guard expects all writes to go through `pre_write` to encrypt data
- A caller obtains the slice via `as_slice()` and writes directly, bypassing encryption
- The underlying data is now partially plaintext, violating the encryption guarantee

### Design options

#### Option: Deprecate and replace with reader-only access

Deprecate `as_slice()` and introduce a new method that returns a read-only cursor or reader type:

```rust
fn as_reader(&self) -> BStackSliceReader<'a, A>;
```

This prevents accidental writes while still allowing inspection of the underlying slice for debugging or metadata purposes.

### Arguments in favor of deprecation

1. **Safety by design** — Making it harder to accidentally bypass hooks aligns with Rust's principle of making the safe path the easy path
2. **Clear intent** — If a caller truly needs to bypass hooks, they can still use `unsafe { raw_block() }`, making the intent explicit
3. **Consistency** — The recommended API (subslicing, `read()`, `write()`) already doesn't use `as_slice()`

### Arguments against deprecation

1. **Not actually unsafe** — Data corruption through misuse doesn't violate memory safety or compromise allocator structure, so marking it unsafe would be misleading per Rust conventions
2. **Breaking change burden** — This would break existing code, and callers who correctly use `as_slice()` for read-only purposes would need to migrate
3. **Already addressed** — Documentation and API design already encourage using `read()` and `write()`. Callers using `as_slice()` are expected to understand the implications
4. **`raw_block()` exists** — The unsafe `raw_block()` method already exists for cases where hook bypass is needed, and its safety contract documents that hooks must be manually called. Making `as_slice()` also unsafe would be redundant
5. **Read-only restriction insufficient** — Even read-only access might be problematic for some guards (e.g., exposing ciphertext when only plaintext should be visible)

### Open questions

- Is restricting to read-only access sufficient, or do some guard implementations need to hide even read access to the raw slice?
- Would the migration burden outweigh the safety benefits?

---

## Adding a guard/intercept layer to the C allocator API

**Feature flag:** `BSTACK_FEATURE_SET` (for write hooks)
**Breaking change:** No (additive; new types and functions only)

### Motivation

The Rust allocator layer has `BStackGuardedSlice` and `BStackGuardedSliceSubview` — traits that let callers attach lifecycle hooks (`pre_read`, `post_read`, `pre_write`, `post_write`) to a slice, enabling transparent I/O transforms such as encryption, compression, or integrity checks. The C allocator has no equivalent; transforms must be wired by hand at every call site.

Without a guard layer, patterns like "all reads from this region must be decrypted and all writes must be re-encrypted" require every caller to remember to apply the transform, which is error-prone and couples application logic to storage details.

### Design

Add a `bstack_guarded_slice_t` wrapper type and a vtable for hooks:

```c
typedef struct {
    int (*pre_read) (void *ctx, bstack_slice_t s);
    int (*post_read)(void *ctx, bstack_slice_t s, uint8_t *buf, size_t len);
    int (*pre_write)(void *ctx, bstack_slice_t s,
                     const uint8_t **buf, size_t *len);
    int (*post_write)(void *ctx, bstack_slice_t s);
} bstack_guard_vtbl_t;

typedef struct {
    bstack_slice_t           slice;
    const bstack_guard_vtbl_t *vtbl;
    void                    *ctx;
} bstack_guarded_slice_t;
```

Provide `bstack_guarded_slice_read`, `bstack_guarded_slice_write`, and `bstack_guarded_slice_subview` functions that delegate to the underlying slice while invoking the hooks at the appropriate points.

The subview analogue (`BStackGuardedSliceSubview`) would be expressed as a flag or second vtable on `bstack_guarded_slice_t` that additionally intercepts `subslice` calls, allowing a guard to restrict or transform the visible range.

### Open questions

- Should `pre_write` be allowed to replace the buffer pointer (e.g., to encrypt into a separate scratch buffer), or should it always mutate in place? Replacing the pointer is more flexible but requires the hook to manage scratch memory lifetime.
- How should hook errors interact with the C error-reporting convention (`errno`)? The hook returning -1 should propagate as a read/write failure, but the hook may also want to set a custom errno value.
- Should `bstack_guarded_slice_t` be opaque (allocated on the heap) or transparent (stack-allocatable struct)? Transparency is simpler but leaks the vtable layout.

---

## Fix Audit Findings in GhostTreeBstackAllocator

### Motivation

`GhostTreeBstackAllocator` is documented as a multi-call allocator with **no
write-ahead log and no checksums** ([src/alloc/ghost_tree.rs:106-110](src/alloc/ghost_tree.rs#L106-L110)).
The implementation honours that statement faithfully — but the consequences are
more severe than the doc table suggests. Almost every state-changing path
(`alloc` split, `dealloc`, every shrink, every AVL rotation) is multi-call, and
a crash in any of them either silently leaks a free block or orphans a whole
subtree. The only recovery mechanism is `coalesce_and_rebalance`, which walks
the tree from `read_root()` *trusting* child pointers, so any subtree that
becomes unreachable (the common outcome of a torn rotation) is permanently
lost. A separate correctness bug in `alloc_bulk` discards the `(_, size)` of
the found block and unconditionally uses `total`, leaking the remainder when
best-fit returns a strictly larger block. Documentation is unusually candid
about most of these limitations, but the doc table marks shrink-tail and grow
non-tail as "multi-call" without explaining that on the multi-call paths the
recovery is `None`, not "rebuilt on next open."

### Issues

#### [Critical] Torn AVL rotation orphans an entire subtree
**Severity:** Critical
**Category:** Crash safety
**Location:** [src/alloc/ghost_tree.rs:283-306](src/alloc/ghost_tree.rs#L283-L306) (`avl_rotate_right` / `avl_rotate_left`)
**Description:** Rotations write the two affected nodes back-to-back with no
ordering guarantee that preserves reachability. `avl_rotate_right` first
rewrites `node`'s children to `(pivot_r, node_r)` — at that instant, `pivot` is
no longer reachable from `node`, but `pivot` itself has not yet been rewritten
to point at `(pivot_l, node)`. If the process crashes between the two
`write_node` calls, the entire subtree rooted at `pivot` (including `pivot_l`
and everything below it) is dropped from the tree.
**Scenario:**
1. Tree state: `parent -> node -> {pivot, node_r}`, `pivot -> {pivot_l, pivot_r}`.
2. Insert or remove triggers `avl_rotate_right(node)`.
3. `avl_write_and_update(node, …, pivot_r, node_r)` returns successfully (durable).
4. **Crash before** `avl_write_and_update(pivot, …, pivot_l, node)`.
5. On reopen, `coalesce_and_rebalance` walks `parent -> node -> {pivot_r, node_r}`.
   `pivot` and `pivot_l` (and any descendants of either) are never visited.
**Impact:** Permanent loss of every free block in the orphaned subtree. The
on-disk arena bytes for those blocks remain "live" forever. Repeated rotation
crashes are cumulative.
**Recommendation:** Either (a) introduce a write-ahead step that durably
records the rotation intent so `coalesce_and_rebalance` can also walk a
"pending rotation" list, or (b) walk the entire arena in linear order on every
open (similar to FirstFit's recovery) so unreachable free blocks can be
rediscovered. Option (b) is the simpler retrofit because free blocks already
have a self-describing AVL header (size at offset 0).

#### [Medium] `dealloc` permanently loses the block on crash between zero and AVL insert
**Severity:** Medium
**Category:** Crash safety
**Location:** [src/alloc/ghost_tree.rs:714-735](src/alloc/ghost_tree.rs#L714-L735)
**Description:** Non-tail `dealloc` does `self.stack.zero(ptr, true_len)` then
`self.avl_insert(ptr, true_len)`. The two operations are not journaled. A
crash in between zeroes the block but does not record it in the free tree.
The block becomes free space that the allocator believes is "live" and will
never reissue.
**Scenario:**
1. User calls `alloc.dealloc(handle)` on a 1 KiB non-tail allocation.
2. `self.stack.zero(ptr, 1024)` returns.
3. **Crash** before `avl_insert`.
4. On reopen, `coalesce_and_rebalance` walks the tree — the freed block is not
   in the tree, so it stays "allocated" forever.
**Impact:** Per-dealloc permanent space leak proportional to the freed block
size. Crash-tolerant workloads that call `dealloc` aggressively will see
unbounded on-disk growth that no later operation can recover.
**Recommendation:** Re-order to write-ahead: insert into the AVL tree *first*
(the AVL node already lives at offset 0 of the block, so writing the node
implicitly "consumes" the prior live data), then it is acceptable for any
following step to be unnecessary because the block is already reclaimable.
Even simpler: detach the user payload by writing the AVL node header in one
`set` call, which both zeroes the leading 32 bytes and records the size — the
rest of the block is "junk that need not be zero" so long as the invariant is
relaxed to allow non-zero free-block payloads on crash. Alternatively,
document the per-dealloc leak rate and provide a manual repair API.

#### [Medium] Multi-call `realloc` grow (non-tail) leaks new block on crash
**Severity:** Medium
**Category:** Crash safety
**Location:** [src/alloc/ghost_tree.rs:696-702](src/alloc/ghost_tree.rs#L696-L702)
**Description:** Non-tail grow is `alloc(new) + copy + dealloc(old)`. There is
no recovery linking the new and old blocks. A crash after `alloc` but before
`dealloc` leaves the new block "allocated" (removed from the AVL tree) without
any caller-visible handle, **and** the old block also "allocated". Both are
permanent leaks.
**Scenario:**
1. `realloc(handle, larger)` on a non-tail slice.
2. `self.alloc(new_len)` succeeds — new block removed from AVL.
3. **Crash** before the copy completes / before `dealloc(slice)`.
4. On reopen: the new block has no handle and is not in the tree (lost). The
   old block is also not in the tree (still "allocated"); the caller may or
   may not have persisted the old handle, but the new one is unrecoverable.
**Impact:** Same as the `dealloc` case but doubled: up to the new allocation
size is leaked per crashed realloc.
**Recommendation:** Until a journal is introduced, document the leak per
realloc explicitly in the doc table (currently the row says "multi-call" but
does not call out the leak). Long term, the cleanest fix is a single-slot
"pending realloc" header field that records `(old_ptr, new_ptr)` before
`alloc`; on reopen, if the field is set, free whichever block looks live and
clear the field.

#### [Medium] `alloc` split leaks the remainder on crash between AVL remove and AVL insert
**Severity:** Medium
**Category:** Crash safety
**Location:** [src/alloc/ghost_tree.rs:589-606](src/alloc/ghost_tree.rs#L589-L606)
**Description:** The split path does `avl_find_best_fit_and_remove` (which
removes the full block from the tree) then `avl_insert(ptr, remainder)` for
the leading fragment. A crash between the two leaves the full original block
*outside* the tree. The fragment is permanently lost. The same hazard applies
to the no-split path (zero+return) but in that case the entire block is the
intended allocation, so only a partial-handle problem.
**Scenario:**
1. `alloc(64)` finds a 256-byte free block, removes it.
2. **Crash** before `avl_insert(ptr, 192)` records the 192-byte remainder.
3. On reopen, the 192-byte fragment is unreachable.
**Impact:** Per-crashed-split leak up to `block_size - aligned_request - MIN_ALLOC`
bytes.
**Recommendation:** Reverse the order — insert the remainder *first* using a
known-safe pattern (the AVL node bytes overlay the original block's AVL
header, so writing the smaller node both shrinks the block in-tree and frees
the tail as the user's allocation), then return. This also reduces the
critical window of `alloc` to zero AVL writes.

#### [Medium] `alloc_bulk` discards the found-block size — silently leaks remainder
**Severity:** Medium
**Category:** Invariant
**Location:** [src/alloc/ghost_tree.rs:786-794](src/alloc/ghost_tree.rs#L786-L794)
**Description:**
```rust
let block_ptr = if let Some((ptr, _)) = self.avl_find_best_fit_and_remove(total)? {
    self.stack.zero(ptr, MIN_ALLOC)?;
    ptr
} else {
    self.stack.extend(total)?
};
```
The block's actual size is bound to `_` and discarded. If best-fit returns a
block strictly larger than `total` (always possible, since the tree is keyed
on `(size, address)` and the search returns the smallest `>= total`), the
remainder (a multiple of 32, possibly very large) is consumed silently. No
split, no re-insert.
**Scenario:**
1. Free tree contains one 4096-byte block; arena otherwise empty.
2. `alloc_bulk([128, 128])` → `total = 256`.
3. `avl_find_best_fit_and_remove(256)` returns `(ptr, 4096)`.
4. The 4096-byte block is removed; only 256 bytes are used; the trailing 3840
   bytes are silently leaked.
**Impact:** Bulk allocations can consume arbitrarily more bytes than
requested. The leak is permanent until manual recovery. Live size on disk
diverges from accounting.
**Recommendation:** Mirror the single `alloc` split logic — if
`block_size - total >= MIN_ALLOC`, `avl_insert(ptr, block_size - total)` for
the leading fragment and use `ptr + (block_size - total)` for the bulk
allocation. Then the implementation table line "single-call (crash-safe by
construction)" is no longer accurate; either reorder (insert first) or
downgrade the row.

#### [Medium] `coalesce_and_rebalance` mishandles duplicate visits from a corrupted tree
**Severity:** Medium
**Category:** Crash safety
**Location:** [src/alloc/ghost_tree.rs:498-557](src/alloc/ghost_tree.rs#L498-L557)
**Description:** If a previous rotation crashed in a way that left a node
reachable from two parents (e.g. step-1-of-2 in a rotation leaves `pivot_r` as
both `node.left` and `pivot.right`), the in-order walk visits the duplicated
node twice. Both visits push `(ptr, size)` into `blocks`. The coalesce step
only merges when `last.0 + last.1 == ptr`, which is *false* for duplicates
(`ptr + size != ptr`), so both entries survive into the rebuild. `build` then
writes an AVL node at the duplicated `ptr` twice; the second write clobbers
the first node's child pointers, and the subtree the first write described is
lost.
**Scenario:** Crash during the first write of a left-right rotation; reopen.
Tree walk reaches the shared node from both sides; rebuild loses one of the
duplicate's subtrees.
**Impact:** Compounds with the rotation-crash leak above — recovery itself can
delete additional reachable free blocks.
**Recommendation:** Deduplicate the `blocks` vector before coalescing
(`blocks.sort(); blocks.dedup()` after the in-order walk), or detect duplicate
visits during the walk by maintaining a "seen" set keyed on `ptr` and erroring
out so the user is informed of corruption rather than silently widening it.

#### [Medium] AVL walk uses unbounded recursion
**Severity:** Medium
**Category:** Invariant
**Location:** [src/alloc/ghost_tree.rs:477-489](src/alloc/ghost_tree.rs#L477-L489), [src/alloc/ghost_tree.rs:342-356](src/alloc/ghost_tree.rs#L342-L356), [src/alloc/ghost_tree.rs:376-404](src/alloc/ghost_tree.rs#L376-L404)
**Description:** `avl_walk_inorder`, `avl_insert_rec`, `avl_remove_rec`, and
`avl_find_best_fit_and_remove_rec` are all naturally recursive. On a balanced
tree this is fine (depth ≤ ~60 for any realistic arena). On a *corrupted*
tree (rotation crash creating a cycle via stale pointers) the recursion has
no cycle guard and will stack-overflow.
**Scenario:** A torn rotation leaves a cycle in the tree (e.g. node `A`'s
right pointer became `B`, and `B`'s right pointer still points back to `A`).
Any subsequent operation that walks through `A` recurses until the stack
overflows the OS thread limit.
**Impact:** Recovery cannot run — the process aborts on `coalesce_and_rebalance`
inside `new()`, making the file unopenable until manual repair.
**Recommendation:** Bound the recursion: pass a `remaining_depth` parameter
(start at, say, 128) and return an error when it reaches zero. Or convert the
walk to an explicit stack-based loop with a visited-set guard.

#### [Low] Doc table claim "single-call" is not accurate for `alloc_bulk`
**Severity:** Low
**Category:** Documentation
**Location:** [src/alloc/ghost_tree.rs:65](src/alloc/ghost_tree.rs#L65), [src/alloc/ghost_tree.rs:749-752](src/alloc/ghost_tree.rs#L749-L752)
**Description:** The summary table row reads `| alloc_bulk | one block for the
combined size, then split | single-call|`. The actual implementation does
`avl_find_best_fit_and_remove` (multiple writes for the AVL rotation/rebalance
cascade) + `self.stack.zero(ptr, MIN_ALLOC)` — at least two `BStack` calls,
typically many more on a non-trivial tree.
**Recommendation:** Re-label the row as multi-call and describe the recovery
path (or lack thereof).

#### [Low] `realloc` shrink non-tail leaks freed tail on crash
**Severity:** Low
**Category:** Crash safety
**Location:** [src/alloc/ghost_tree.rs:678-684](src/alloc/ghost_tree.rs#L678-L684)
**Description:** Non-tail shrink does `self.stack.zero(...)` then
`self.avl_insert(tail_ptr, freed_tail)`. A crash between the two leaks
`freed_tail` bytes. Severity is lower than the dealloc case because shrink is
rarer.
**Recommendation:** Same write-ahead reorder as the `dealloc` fix; or
document explicitly that shrink-non-tail leaks on crash.

