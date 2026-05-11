# Planned Features

This document outlines upcoming features planned for the `bstack` crate. These enhancements aim to improve usability, performance, and integration while maintaining the core principles of durability, crash-safety, and simplicity. Changes aim to be backward-compatible. New features are suggested to be added as optional features under feature flags and new traits, instead of modifying existing ones, to avoid breaking changes. All features aim to follow [Rust's API design guidelines](https://rust-lang.github.io/api-guidelines/) and BStack's design principles.

---

## `BStack::lock_up_to` — monotonically growing locked region

**Feature flag:** none — baked directly into `BStack`
**Breaking change:** No — `locked_len()` returns `0` by default, preserving all existing behaviour

### Motivation

Some workloads have a stable block of data at the beginning of the payload —
any arbitrary bytes that are written once and read very frequently by concurrent
readers.  In the current `BStack` every read acquires the shared half of the
rwlock, which is unnecessary for bytes that are never mutated.  By tracking a
monotonically growing partition boundary inside `BStack` itself, reads to the
locked region `[0, partition)` can bypass the rwlock entirely with no wrapper
type and no overhead for callers that never call `lock_up_to`.

### Design

A single `AtomicU64 locked` field is added to `BStack` in memory only.  It is
**not persisted to disk** — the on-disk format is unchanged.  The partition
starts at `0` on every open; callers that use this feature call `lock_up_to`
once after opening (before any concurrent readers start) to re-establish the
boundary.

The partition is **monotonically increasing**.  It can never be decremented.
This invariant is what makes lock-free reads safe: once a byte index is below
`partition`, it is frozen forever, and any thread can read it at any time
without coordination.  A caller that truly needs to reset the partition can
close and reopen the file — the boundary returns to `0` on the next open.

### Concurrency model

`locked` is stored as an `AtomicU64`.

**Lock-free read path** (for reads entirely within `[0, partition)`):

1. Load `locked` with `Acquire`.
2. Verify the requested range lies entirely within `[0, locked)`.
3. `pread` directly — no rwlock taken.

**`lock_up_to(n)` path**:

1. Acquire the exclusive write lock (ensuring no dynamic-tail writes are
   in flight and all preceding writes to `[0, n)` are complete).
2. Verify `n >= locked.load(Relaxed)` and `n <= len`.
3. Store `n` to `locked` with `Release`, then release the write lock.

The `Release` on the store ensures that all writes to `[0, n)` that happened
before `lock_up_to` are visible to any thread that subsequently loads `locked`
with `Acquire` and reads within the locked region.

Because `locked` only ever increases, there is no race between a lock-free
read and a concurrent `lock_up_to`: either the read sees the old (smaller)
partition and falls through to the rwlock path, or it sees the new (larger)
partition and reads without a lock — both are safe.

### API additions to `BStack`

```rust
impl BStack {
    /// Extend the locked region to cover `[0, n)`.
    ///
    /// `n` must be ≥ the current locked length and ≤ the current payload
    /// length.  After this call, reads to `[0, n)` are lock-free and all
    /// write and shrink operations that would touch `[0, n)` return
    /// `InvalidInput`.
    ///
    /// Acquires the exclusive write lock to ensure all in-flight writes to
    /// `[0, n)` have completed before the region is declared immutable.
    pub fn lock_up_to(&self, n: u64) -> io::Result<()>;

    /// Returns the current locked length.  `0` means no bytes are locked.
    pub fn locked_len(&self) -> u64;

    /// Open a `BStack` and immediately lock the first `n` bytes.
    ///
    /// Equivalent to `BStack::open(path)` followed by `lock_up_to(n)`, but
    /// expressed as a single call for the common pattern where the locked
    /// region is known ahead of time (e.g. a fixed-size metadata block whose
    /// size is a compile-time or configuration constant).  Returns an error if
    /// `n` exceeds the payload length of the opened file.
    pub fn open_locked_up_to(path: impl AsRef<Path>, n: u64) -> io::Result<Self>;
}
```

Existing read methods (`get`, `peek`, `get_into`, `peek_into`, etc.) gain an
internal fast path: if the requested range lies entirely within `[0, locked)`,
they skip the rwlock.  From the caller's perspective the API is unchanged.

Write methods (`set`, `zero`, `atrunc`, `splice`, `swap`, `cas`, etc.) return
`InvalidInput` if the target range overlaps `[0, locked)`.  Shrink operations
(`discard`, `pop`, `atrunc`) return `InvalidInput` if they would reduce `len`
below `locked`.

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

