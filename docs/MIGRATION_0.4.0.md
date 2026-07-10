# Migrating to 0.4.0

This guide covers upgrading from **0.2.x** to **0.4.0**. It lists only the
changes that require action; new features that don't affect existing code are
described in the [CHANGELOG](../CHANGELOG.md).

Two things changed that touch existing code:

1. **The on-disk file format** — affects both C and Rust, and any existing data
   file. See [On-disk format](#on-disk-format).
2. **The allocator / slice API** (Rust `alloc` feature only) — a source-level
   rewrite of how allocated regions are represented. See
   [Allocator & slice API (Rust)](#allocator--slice-api-rust).

If you don't use the `alloc` feature, only the format change applies to you.

---

## On-disk format

The header grew from 16 bytes to 32 bytes and the magic changed from
`BSTK\x00\x01\x0f\x00` to `BSTK\x00\x04\x00\x00`. A 0.2.x file is written in the
old layout, so **0.4.0 rejects it on open**. This affects the C and Rust
implementations identically.

Upgrade each existing file once, in place:

```rust
// Rust
BStack::migrate("data.bstk")?;   // no-op-safe: errors only if not a legacy file
let stack = BStack::open("data.bstk")?;
```

```c
/* C */
bstack_migrate("data.bstk");     /* returns 0 on success */
bstack_open(&s, "data.bstk");
```

`migrate` rewrites the file through a sibling `data.bstk.migrating` and renames
it atomically, so a crash mid-upgrade leaves either the intact original or the
finished new file — never a corrupt one. The committed length is preserved. It
errors if the file is *not* a legacy file (wrong magic, or too short), so it is
safe to skip when you already have a 0.4.0 file.

Newly created files are 0.4.0 automatically; no code change is needed for those.

### Behavior change (no code change)

In-place mutations (`set`, `zero`, `swap`, `cas`, `copy`, `cross_exchange`,
`process`, the `crds` family, and the tail-replace family) are now
**crash-atomic** via a write-in-progress journal. This is transparent — same
signatures, same results — but it is the reason the format grew. No action
required beyond migrating the file.

---

## Allocator & slice API (Rust)

> Applies only to the `alloc` feature. If you never named `BStackSlice`,
> implemented `BStackAllocator`, or used `BStackByteVec`, you can stop here.

In 0.2.x, a single `BStackSlice<'a, A>` type played three roles at once: a raw
coordinate, an ownership handle, and an I/O view — and it was `Copy`, so an
allocation could be silently used after being freed or resized.

0.4.0 splits that into **three types**, each with one job:

| Type                      | Role                           | `Copy`/`Clone` | Alloc ops | I/O               |
|---------------------------|--------------------------------|----------------|-----------|-------------------|
| `BStackRange`             | raw `(offset, len)` coordinate | `Copy`         | no        | no                |
| `BStackOwnedSlice<'a, A>` | ownership handle               | neither        | yes       | via borrowed view |
| `BStackSlice<'a>`         | borrowed I/O view              | `Clone`        | no        | read/write        |

The key consequence: an allocation now has exactly **one owner**
(`BStackOwnedSlice`). Because `realloc` and `dealloc` take it by value,
use-after-free and use-after-realloc are compile errors.

### 1. Update `impl BStackAllocator` blocks

Set the associated type to the new owned-slice handle:

```rust
impl BStackAllocator for MyAllocator {
    type Error = io::Error;
    type Allocated<'a> = BStackOwnedSlice<'a, Self>;   // <- was some Copy slice
    // ...
}
```

The `Allocated<'a>` bound changed from `Copy + TryInto<BStackSlice<'a, Self>>`
to `Into<BStackOwnedSlice<'a, Self>>`.

If you previously used a custom handle type, you just need to ensure that an
implementation of `Into<BStackOwnedSlice<'a, Self>>` exists for it.

### 2. `realloc` / `dealloc` now return the handle on failure

Their error type changed from `Self::Error` to `BStackAllocError<'a, Self>`,
which carries the *surviving* allocation back to you in a `handle` field.

If you just want to propagate the error and drop the recovered handle:

```rust
// before
alloc.realloc(handle, new_len)?;
alloc.dealloc(handle)?;

// after — surface just the error
let handle = alloc.realloc(handle, new_len).map_err(|e| e.source)?;
alloc.dealloc(handle).map_err(|e| e.source)?;
```

To recover and retry instead of leaking:

```rust
match alloc.realloc(handle, new_len) {
    Ok(new_handle) => { /* use new_handle */ }
    Err(e) => {
        if let Some(recovered) = e.into_handle() {
            // original region is intact — retry, fall back, or free it
        }
        // None means the region was genuinely lost (recoverable via crash recovery)
    }
}
```

`BStackAllocError` implements `Display`, `Debug`, and `std::error::Error`, so it
works with `?` in functions that return it.

`BStackBulkAllocator::dealloc_bulk` changed the same way: it now returns
`BStackBulkAllocError<'a, Self>`, whose `handles: Vec<_>` holds the blocks it did
*not* free. Use `.map_err(|e| e.source)?` or inspect `e.handles`.

### 3. `BStackSlice` lost its `A` parameter and its `Copy`

`BStackSlice<'a, A>` is now `BStackSlice<'a>` (it carries `&'a BStack`
directly). It is no longer `Copy` — `Clone` is retained for making an explicit
second view. Write methods (`write`, `write_range`, `zero`, `zero_range`,
`writer`, `writer_at`) now take `&mut self`.

```rust
// before
let s: BStackSlice<A> = /* ... */;

// after
let s: BStackSlice = /* ... */;   // drop the `A`
```

You obtain an I/O view by borrowing from the owned handle — shared with
`as_slice(&self)`, exclusive with `as_slice_mut(&mut self)`:

```rust
let mut owned: BStackOwnedSlice<A> = alloc.alloc(64)?;

owned.as_slice_mut().write(b"hello")?;      // exclusive borrow to write
let bytes = owned.as_slice().read()?;       // shared borrow to read
```

For convenience, `BStackOwnedSlice` also has delegate `read*` / `write*` /
`zero*` / `reader*` / `writer*` methods, so you often don't need to borrow
explicitly:

```rust
owned.write(b"hello")?;   // delegates through as_slice_mut()
let bytes = owned.read()?;
```

### 4. Trait bound rename

`BStackSliceAllocator` was renamed to `BStackOwnedSliceAllocator`:

```rust
// before
fn f<A: BStackSliceAllocator>(a: &A) { /* ... */ }
// after
fn f<A: BStackOwnedSliceAllocator>(a: &A) { /* ... */ }
```

### 5. `BStackByteVec` now requires `BStackOwnedSliceAllocator`

`BStackByteVec<'a, A>` bounds `A: BStackOwnedSliceAllocator` and holds a
`BStackOwnedSlice` internally. Call-site changes:

- `from_raw_block` takes a `BStackOwnedSlice`; `into_raw_block` returns one.
- `raw_block` returns `BStackSlice<'a>` (no `A` parameter).
- `as_slice` now returns `io::Result<BStackSlice<'_>>` — the lifetime is
  shortened to the borrow of `self`, so narrow any lifetime annotations.
- Write-helper methods take `&mut self`.

`grow_to` adopts the handle returned by a failed `realloc` automatically. If a
`realloc` reports the block was genuinely lost (`handle == None` — only possible
with a custom allocator), the vec detaches and every later operation fails
cleanly rather than corrupting a reused region.

### 6. Removed items

| Removed                  | Replacement                                                                   |
|--------------------------|-------------------------------------------------------------------------------|
| `BStackSlice::new`       | `unsafe { BStackSlice::from_raw_parts(stack, offset, len) }`                  |
| `ManualAllocator`        | Use `BStackRange` directly for externally-managed `(offset, len)` coordinates |
| `DebugCheckingAllocator` | Temporarily gone pending a rework; expected to return in a later version      |

---

## Checklist

- [ ] Run `migrate` on every existing data file before opening it (C and Rust).
- [ ] Set `type Allocated<'a> = BStackOwnedSlice<'a, Self>` in each allocator impl.
- [ ] Handle `BStackAllocError` from `realloc`/`dealloc` (`.map_err(|e| e.source)?` or recover `e.handle`).
- [ ] Handle `BStackBulkAllocError` from `dealloc_bulk`.
- [ ] Drop the `A` parameter from `BStackSlice<'a, A>` annotations; add `.clone()` where you relied on `Copy`.
- [ ] Rename `BStackSliceAllocator` bounds to `BStackOwnedSliceAllocator`.
- [ ] Update `BStackByteVec` `from_raw_block` / `into_raw_block` call sites and `as_slice` lifetimes.
- [ ] Replace any use of `BStackSlice::new`, `ManualAllocator`, or `DebugCheckingAllocator`.
