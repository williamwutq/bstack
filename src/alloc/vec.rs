//! Growable byte vector backed by a [`BStack`] allocation.
//!
//! Requires features `alloc` and `set`.

use super::{BStackOwnedSlice, BStackOwnedSliceAllocator, BStackRange, BStackSlice};
use std::fmt;
use std::io;
#[cfg(feature = "atomic")]
use std::ops::Range;

/// Byte offset of the first element within the block (past the 16-byte header).
const HEADER_LEN: u64 = 16;

/// A growable byte vector backed by a [`crate::BStack`] allocation.
///
/// `BStackByteVec<'a, A>` stores `u8` elements inside a [`crate::BStack`]
/// allocation managed by allocator `A`.  Every mutation issues a durable sync
/// through the allocator so the contents survive a process crash.
///
/// For a general typed vector (e.g. over `u32`, structs, etc.), a general type
/// parameter requires a
/// sound POD/byte-castable bound that would add an external dependency; this
/// type covers the common byte-buffer use case without any additional
/// requirements on element validity.
///
/// ## Memory layout
///
/// ```text
/// ┌──────────────────────┬──────────────────────┬────────────────────────────┐
/// │   len  (8 B, LE u64) │   cap  (8 B, LE u64) │   elements: [u8; cap]      │
/// └──────────────────────┴──────────────────────┴────────────────────────────┘
///   byte 0                 byte 8                  byte 16
/// ```
///
/// Both `len` and `cap` are re-read from the block header on every call, so the
/// metadata is recoverable after a crash even if the `BStackByteVec` handle is
/// reconstructed from the raw block via [`BStackByteVec::from_raw_block`].
///
/// ## Growth strategy
///
/// When [`push`](BStackByteVec::push) would exceed the current capacity, the
/// block is reallocated to `max(cap * 2, 4)` bytes.  New element space is
/// zero-initialised by [`crate::BStack::extend`].
///
/// ## Zeroing
///
/// [`pop`](BStackByteVec::pop) decrements `len` first, then zeros the vacated
/// slot.  [`truncate`](BStackByteVec::truncate) writes the new `len` first,
/// then zeros all removed slots in a single `zero_range` call.
/// Deallocation zeroing is delegated to the allocator.
///
/// ## Thread safety
///
/// `BStackByteVec` is `Send` when `A: Sync` and `Sync` when `A: Sync` (both
/// conditions hold for all allocators in this library).  The underlying
/// [`crate::BStack`] serialises concurrent writers through an internal
/// `RwLock`, so multiple threads may call `&self` methods concurrently.
/// Methods that take `&mut self` (`push`, `pop`, `truncate`, `clear`,
/// `reserve`, `resize`) require exclusive access and may not be called from
/// multiple threads simultaneously.
///
/// ## Crash consistency and atomicity
///
/// Every individual [`crate::BStack`] call (`set`, `zero`, `extend`,
/// `discard`) is durably synced before returning and is crash-safe in
/// isolation.  However, all multi-step `BStackByteVec` methods issue **two or
/// more** such calls in sequence and are **not atomic** with respect to
/// process crashes.
///
/// The crash-recovery state for each mutating method is:
///
/// | Method | Step order | Crash-recovery state |
/// |--------|-----------|----------------------|
/// | `push` (no realloc) | write element → increment `len` | Crash after element write: element on disk but `len` not updated; slot is effectively invisible. Re-running `push` with the same value recovers correctly. |
/// | `push` (with realloc) | `realloc` → write `cap` → write element → increment `len` | Crash at any point: header re-read on next open reflects the committed state; worst case is allocator-specific intermediate metadata or a stale cap value. |
/// | `extend_from_slice` | `reserve` → write elements → write `len` | Appended bytes may be partially written beyond the old `len`; they are invisible until the `len` write commits, and re-running with the same `data` recovers correctly. |
/// | `pop` | read element → decrement `len` → zero slot | Crash after `len` decrement but before zero: stale byte may remain in the now out-of-range slot, but reads never include it because it is beyond `len`. |
/// | `truncate` | write `len` → zero removed slots | Crash after `len` write but before zero: stale bytes may remain in now out-of-range slots, but reads never include them because they are beyond `len`. |
/// | `resize` (grow) | `reserve` → write elements → write `len` | Elements between the old and new `len` may be partially written. |
/// | `clear` | (delegates to `truncate(0)`) | See `truncate`. |
/// | `reserve`, `reserve_exact` | `realloc` → write `cap` | Crash between the two: cap field may reflect the old value; the block is larger than cap indicates. Harmless — the next `push` re-checks and may realloc again unnecessarily. |
/// | `shrink_to`, `shrink_to_fit` | `realloc` (shrink) → write `cap` | Crash between the two: block is smaller than the stale `cap` claims; the next `push` re-checks and grows as needed. |
/// | `set` | write element | Single crash-atomic write; no torn state. |
/// | `fill` | one `repeat` | Single crash-atomic fill of the whole `len`; no torn state. |
/// | `split_off` (`atomic` only) | `copy` tail into new vec → write tail `len` → truncate `self` | Crash after the copy but before `self`'s truncate: tail bytes duplicated in both vecs — logically torn but structurally valid. |
/// | `drain` (`atomic` only) | read removed bytes → `copy` tail down → truncate to shorter `len` | Crash after the `copy` but before the truncate: payload already compacted but `len` still claims the old extent — logically torn but structurally valid. |
///
/// In all cases the header is re-read from disk on the next call, so the
/// on-disk `(len, cap)` always reflects the last fully committed step.  The
/// [`from_raw_block`](BStackByteVec::from_raw_block) constructor can be used
/// to reconstruct the handle after a reopen without any additional recovery
/// logic.
///
/// The `atomic`-gated byte movers ([`extend_from_within`](Self::extend_from_within),
/// [`insert`](Self::insert), [`remove`](Self::remove),
/// [`swap_remove`](Self::swap_remove), [`drain`](Self::drain),
/// [`split_off`](Self::split_off), and the cross-slice
/// [`extend_from_bstack_slice`](Self::extend_from_bstack_slice) /
/// [`copy_into_bstack_slice`](Self::copy_into_bstack_slice) /
/// [`append_from_owned`](Self::append_from_owned) /
/// [`move_tail_into`](Self::move_tail_into)) build on the crash-atomic
/// [`crate::BStack::copy`] and [`crate::BStack::cross_exchange`] primitives.  The
/// append-only ones keep the same benign model as `push`; the in-place ones
/// (`insert`/`remove`/`swap_remove`/`drain`/`split_off`/`move_tail_into`) can
/// leave a *logically torn but structurally valid* vec on a crash
/// mid-operation.  See the dedicated `impl` block for the per-method details.
///
/// ## Feature flags
///
/// Requires both the `alloc` and `set` Cargo features.  The byte-mover methods
/// listed above additionally require the `atomic` feature and are compiled only
/// when it is enabled.
pub struct BStackByteVec<'a, A: BStackOwnedSliceAllocator> {
    /// The full block: header (16 B) followed by byte data.
    slice: BStackOwnedSlice<'a, A>,
}

impl<'a, A: BStackOwnedSliceAllocator> fmt::Debug for BStackByteVec<'a, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.read_header() {
            Ok((len, cap)) => f
                .debug_struct("BStackByteVec")
                .field("len", &len)
                .field("capacity", &cap)
                .finish_non_exhaustive(),
            Err(e) => write!(f, "BStackByteVec(error reading header: {e})"),
        }
    }
}

impl<'a, A: BStackOwnedSliceAllocator> BStackByteVec<'a, A> {
    fn block_size(capacity: u64) -> io::Result<u64> {
        capacity.checked_add(HEADER_LEN).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "BStackByteVec: block size overflows u64",
            )
        })
    }

    fn byte_offset(index: u64) -> u64 {
        HEADER_LEN + index
    }

    fn read_header(&self) -> io::Result<(u64, u64)> {
        let mut hdr = [0u8; 16];
        self.slice.read_range_into(0, &mut hdr)?;
        let len = read_buf_le!(hdr, 0 => u64);
        let cap = read_buf_le!(hdr, 8 => u64);
        Ok((len, cap))
    }

    fn write_len_field(&mut self, len: u64) -> io::Result<()> {
        self.slice.write_range(0, len.to_le_bytes())
    }

    fn write_cap_field(&mut self, cap: u64) -> io::Result<()> {
        self.slice.write_range(8, cap.to_le_bytes())
    }

    fn write_header(&mut self, len: u64, cap: u64) -> io::Result<()> {
        let mut hdr = [0u8; 16];
        write_buf!(len => hdr, 0);
        write_buf!(cap => hdr, 8);
        self.slice.write_range(0, hdr)
    }

    fn read_byte_at(&self, index: u64) -> io::Result<u8> {
        let start = Self::byte_offset(index);
        let mut byte = [0u8; 1];
        self.slice.read_range_into(start, &mut byte)?;
        Ok(byte[0])
    }

    fn write_byte_at(&mut self, index: u64, value: u8) -> io::Result<()> {
        let start = Self::byte_offset(index);
        self.slice.write_range(start, [value])
    }

    fn write_bytes_at(&mut self, start_index: u64, values: &[u8]) -> io::Result<()> {
        let start = Self::byte_offset(start_index);
        self.slice.write_range(start, values)
    }

    fn zero_byte_at(&mut self, index: u64) -> io::Result<()> {
        self.slice
            .as_slice_mut()
            .zero_range(Self::byte_offset(index), 1)
    }

    /// Absolute payload offset of logical byte `index` within the backing
    /// [`crate::BStack`], i.e. the coordinate accepted by [`crate::BStack::copy`],
    /// [`crate::BStack::cross_exchange`], and [`crate::BStack::repeat`].
    ///
    /// Equal to the block's start plus the 16-byte header plus `index`.  Must be
    /// recomputed after any reallocation, since the block's start may move.
    fn abs_offset(&self, index: u64) -> u64 {
        self.slice
            .start()
            .saturating_add(HEADER_LEN)
            .saturating_add(index)
    }

    /// Reallocate the block to hold `new_cap` bytes, updating `self.slice`.
    ///
    /// Handles both growth (`push`, `reserve`, `reserve_exact`) and shrink
    /// (`shrink_to`, `shrink_to_fit`).  Uses `mem::replace` with a placeholder
    /// (same coords) to transfer ownership to `realloc`.
    ///
    /// On failure, `realloc` returns the surviving allocation in
    /// [`BStackAllocError::handle`]:
    ///
    /// * `Some(handle)` — we adopt it, so `self` tracks the real region (the
    ///   untouched original, or a fully committed new region whose old block
    ///   could not be freed) rather than the stale placeholder.
    /// * `None` — the backing block was genuinely lost mid-operation. Keeping
    ///   the placeholder would leave `self` pointing at a freed region that a
    ///   later allocation may reuse, so a subsequent `push` could corrupt an
    ///   unrelated allocation. Instead we detach to the empty sentinel: the
    ///   vec loses its backing (its contents are gone with the allocation) and
    ///   every subsequent operation fails cleanly rather than risking corruption.
    fn realloc_to(&mut self, new_cap: u64) -> io::Result<()> {
        let new_size = Self::block_size(new_cap)?;
        let alloc: &'a A = self.slice.allocator();
        let range: BStackRange = self.slice.range().into();
        // Create a placeholder with the same coords so mem::replace leaves
        // self.slice valid even on realloc failure.
        let placeholder = unsafe { BStackOwnedSlice::from_raw_range(alloc, range) };
        let old = std::mem::replace(&mut self.slice, placeholder);
        match alloc.realloc(old, new_size) {
            Ok(new_slice) => {
                self.slice = new_slice;
                Ok(())
            }
            Err(e) => {
                self.slice = match e.handle {
                    Some(handle) => handle,
                    None => BStackOwnedSlice::empty(alloc),
                };
                Err(e.source)
            }
        }
    }
}

// ── public API ────────────────────────────────────────────────────────────────

impl<'a, A: BStackOwnedSliceAllocator> BStackByteVec<'a, A> {
    /// Create an empty `BStackByteVec` with zero capacity.
    ///
    /// Allocates a 16-byte block for the header only.  The first
    /// [`push`](Self::push) will trigger a reallocation to 4 bytes.
    #[inline]
    pub fn new(alloc: &'a A) -> io::Result<Self> {
        let slice = alloc.alloc(HEADER_LEN)?;
        // Header is zero-initialised by the allocator: len=0, cap=0.
        Ok(Self { slice })
    }

    /// Create an empty `BStackByteVec` pre-sized for at least `capacity` bytes.
    #[inline]
    pub fn with_capacity(capacity: u64, alloc: &'a A) -> io::Result<Self> {
        let slice = alloc.alloc(Self::block_size(capacity)?)?;
        let mut vec = Self { slice };
        vec.write_cap_field(capacity)?;
        Ok(vec)
    }

    /// Allocate a `BStackByteVec` and populate it from a byte slice.
    ///
    /// The resulting vec has `len == capacity == data.len()`.
    pub fn from_slice(data: &[u8], alloc: &'a A) -> io::Result<Self> {
        let len = data.len() as u64;
        let slice = alloc.alloc(Self::block_size(len)?)?;
        let mut vec = Self { slice };
        if len > 0 {
            vec.write_header(len, len)?;
            vec.write_bytes_at(0, data)?;
        }
        Ok(vec)
    }

    /// Reconstruct a `BStackByteVec` from a raw block handle.
    ///
    /// # Safety
    ///
    /// `slice` must be the original allocation handle returned by one of the
    /// `BStackByteVec` constructors on the same allocator, and the block header
    /// must have been written by a `BStackByteVec<A>`.  Passing an unrelated
    /// handle is undefined behaviour.
    #[inline]
    #[must_use]
    pub unsafe fn from_raw_block(slice: BStackOwnedSlice<'a, A>) -> Self {
        Self { slice }
    }

    /// Return the number of bytes currently stored.
    ///
    /// Re-reads `len` from the block header on every call.
    #[inline]
    pub fn len(&self) -> io::Result<u64> {
        Ok(self.read_header()?.0)
    }

    /// Return the number of bytes the current allocation can hold without
    /// reallocation.
    ///
    /// Re-reads `cap` from the block header on every call.
    #[inline]
    pub fn capacity(&self) -> io::Result<u64> {
        Ok(self.read_header()?.1)
    }

    /// Return `true` if the vec contains no bytes.
    #[inline]
    pub fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Return the byte at `index`, or `None` if `index >= len`.
    pub fn get(&self, index: u64) -> io::Result<Option<u8>> {
        let (len, _) = self.read_header()?;
        if index >= len {
            return Ok(None);
        }
        Ok(Some(self.read_byte_at(index)?))
    }

    /// Read all logical bytes and return them as a [`Vec<u8>`].
    ///
    /// Equivalent to collecting [`iter`](Self::iter), but returns a single
    /// `io::Result<Vec<u8>>`.
    pub fn read_bytes(&self) -> io::Result<Vec<u8>> {
        let (len, _) = self.read_header()?;
        if len == 0 {
            return Ok(Vec::new());
        }
        self.slice
            .as_slice()
            .read_range(HEADER_LEN, HEADER_LEN + len)
    }

    /// Return a [`BStackSlice`] spanning only the populated byte region.
    ///
    /// The slice covers `[16, 16 + len)` within the block and is borrowed
    /// from `self`.  It is a sub-slice and must **not** be passed to
    /// `realloc` or `dealloc`; use [`into_raw_block`](Self::into_raw_block)
    /// for that.
    ///
    /// # Panics
    ///
    /// Panics if the `len` read from the block header is corrupt (larger than
    /// the block can hold), causing the computed end offset to exceed the
    /// block's length.  Corruption is not a recoverable condition here.
    #[track_caller]
    pub fn as_slice(&self) -> io::Result<BStackSlice<'_>> {
        let (len, _) = self.read_header()?;
        Ok(self.slice.as_slice().subslice(HEADER_LEN, HEADER_LEN + len))
    }

    /// Append `value` to the end of the vec.
    ///
    /// If `len == capacity`, reallocates to `max(cap * 2, 4)` bytes before
    /// writing.
    pub fn push(&mut self, value: u8) -> io::Result<()> {
        let (len, cap) = self.read_header()?;
        if len == cap {
            let new_cap = cap.saturating_mul(2).max(4);
            self.realloc_to(new_cap)?;
            self.write_cap_field(new_cap)?;
        }
        self.write_byte_at(len, value)?;
        self.write_len_field(len + 1)
    }

    /// Append every byte in `data` to the end of the vec.
    ///
    /// This is the bulk counterpart to [`push`](Self::push): it reserves the
    /// required capacity in a single reallocation (if any) and writes all of
    /// `data` with a single durable `set` before committing the new `len`,
    /// rather than issuing a grow/write/len cycle per byte.  A no-op when
    /// `data` is empty.
    ///
    /// # Crash consistency
    ///
    /// The step order is `reserve` → write elements → write `len`.  A crash
    /// before the `len` write leaves the appended bytes on disk but beyond the
    /// committed `len`, so they are invisible; re-running `extend_from_slice`
    /// with the same `data` recovers correctly.
    pub fn extend_from_slice(&mut self, data: &[u8]) -> io::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let (len, _) = self.read_header()?;
        let additional = data.len() as u64;
        // `reserve` re-reads the header, checks `len + additional` for overflow,
        // and grows the block if needed; `len` is left unchanged.
        self.reserve(additional)?;
        self.write_bytes_at(len, data)?;
        self.write_len_field(len + additional)
    }

    /// Remove and return the last byte, or `None` if empty.
    ///
    /// `len` is decremented before the vacated slot is zeroed.
    pub fn pop(&mut self) -> io::Result<Option<u8>> {
        let (len, _) = self.read_header()?;
        if len == 0 {
            return Ok(None);
        }
        let value = self.read_byte_at(len - 1)?;
        self.write_len_field(len - 1)?;
        self.zero_byte_at(len - 1)?;
        Ok(Some(value))
    }

    /// Shorten the vec to `new_len` bytes.
    ///
    /// No-op when `new_len >= len`. `len` is updated first, then removed slots
    /// are zeroed in a single `zero_range` call; capacity is unchanged.
    pub fn truncate(&mut self, new_len: u64) -> io::Result<()> {
        let (len, _) = self.read_header()?;
        if new_len >= len {
            return Ok(());
        }
        let start = Self::byte_offset(new_len);
        let removed = len - new_len;
        self.write_len_field(new_len)?;
        self.slice.zero_range(start, removed)
    }

    /// Remove all bytes without releasing the allocation.
    ///
    /// Equivalent to `truncate(0)`.
    #[inline]
    pub fn clear(&mut self) -> io::Result<()> {
        self.truncate(0)
    }

    /// Reserve capacity for at least `additional` more bytes.
    ///
    /// After this call `capacity() >= len() + additional`.  Does nothing if
    /// the current capacity is already sufficient.
    pub fn reserve(&mut self, additional: u64) -> io::Result<()> {
        let (len, cap) = self.read_header()?;
        let needed = len.checked_add(additional).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "BStackByteVec::reserve: capacity overflow",
            )
        })?;
        if needed <= cap {
            return Ok(());
        }
        let new_cap = needed.max(cap.saturating_mul(2));
        self.realloc_to(new_cap)?;
        self.write_cap_field(new_cap)?;
        Ok(())
    }

    /// Reserve capacity for exactly `additional` more bytes, without the
    /// amortising over-allocation of [`reserve`](Self::reserve).
    ///
    /// After this call `capacity() >= len() + additional`, growing the block to
    /// exactly `len + additional` when it is currently too small.  Does nothing
    /// if the current capacity is already sufficient.  Prefer [`reserve`](Self::reserve)
    /// when more insertions are expected; use this when the final size is known.
    pub fn reserve_exact(&mut self, additional: u64) -> io::Result<()> {
        let (len, cap) = self.read_header()?;
        let needed = len.checked_add(additional).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "BStackByteVec::reserve_exact: capacity overflow",
            )
        })?;
        if needed <= cap {
            return Ok(());
        }
        self.realloc_to(needed)?;
        self.write_cap_field(needed)
    }

    /// Shrink the capacity to `min_capacity`, but never below the current `len`.
    ///
    /// No-op if the current capacity is already `<= min_capacity` (so it never
    /// grows).  Mirrors the reallocation path used for growth, reallocating the
    /// block to `max(len, min_capacity)` bytes.
    pub fn shrink_to(&mut self, min_capacity: u64) -> io::Result<()> {
        let (len, cap) = self.read_header()?;
        let target = min_capacity.max(len);
        if target >= cap {
            return Ok(());
        }
        self.realloc_to(target)?;
        self.write_cap_field(target)
    }

    /// Shrink the capacity to match `len`, releasing all spare capacity.
    ///
    /// Equivalent to `shrink_to(0)`.
    #[inline]
    pub fn shrink_to_fit(&mut self) -> io::Result<()> {
        self.shrink_to(0)
    }

    /// Overwrite the byte at `index` with `value`.
    ///
    /// A single in-place, crash-atomic write to an existing slot; capacity and
    /// `len` are unchanged.
    ///
    /// Returns `Ok(None)` if `index >= len` (nothing is written), mirroring
    /// [`get`](Self::get); `Ok(Some(()))` on success.  `Err` is reserved for I/O
    /// failures.
    pub fn set(&mut self, index: u64, value: u8) -> io::Result<Option<()>> {
        let (len, _) = self.read_header()?;
        if index >= len {
            return Ok(None);
        }
        self.write_byte_at(index, value)?;
        Ok(Some(()))
    }

    /// Overwrite every logical byte with `value`.
    ///
    /// Backed by a single [`crate::BStack::repeat`], so the whole populated
    /// region is filled crash-atomically with a fixed-size journal regardless of
    /// `len`.  A no-op on an empty vec; capacity and `len` are unchanged.
    pub fn fill(&mut self, value: u8) -> io::Result<()> {
        let (len, _) = self.read_header()?;
        if len == 0 {
            return Ok(());
        }
        let offset = self.abs_offset(0);
        self.slice.allocator().stack().repeat(offset, [value], len)
    }

    /// Set the length to `new_len`, filling any new slots with `value`.
    ///
    /// If `new_len <= len`, equivalent to [`truncate`](Self::truncate) and
    /// `value` is ignored.
    pub fn resize(&mut self, new_len: u64, value: u8) -> io::Result<()> {
        let (len, _) = self.read_header()?;
        if new_len <= len {
            return self.truncate(new_len);
        }
        let additional = new_len - len;
        self.reserve(additional)?;
        let count = usize::try_from(additional).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "BStackByteVec::resize: growth exceeds usize",
            )
        })?;
        let fill: Vec<u8> = std::iter::repeat_n(value, count).collect();
        self.write_bytes_at(len, &fill)?;
        self.write_len_field(new_len)
    }

    /// Return an iterator over the bytes.
    ///
    /// `len` is snapshotted at construction time.  The vec is borrowed
    /// immutably for the iterator's lifetime, preventing concurrent mutation.
    /// Each byte is read from disk on demand; errors surface as
    /// `io::Result::Err` items.
    #[inline]
    pub fn iter(&self) -> io::Result<BStackByteVecIter<'_, 'a, A>> {
        let (len, _) = self.read_header()?;
        Ok(BStackByteVecIter {
            vec: self,
            index: 0,
            len,
        })
    }

    /// Return a borrowed view of the underlying block (header + all allocated
    /// byte space) with the full allocator lifetime `'a`.
    ///
    /// Because the returned slice has lifetime `'a` (not tied to `&self`),
    /// the caller may continue to mutate the vec while holding the slice.
    /// This is intended for patterns where raw byte access and vec mutation
    /// must interleave (e.g. tests that inspect zeroed slots after a `pop`).
    ///
    /// # Safety
    ///
    /// Any reallocation of this vec (via `push`, `reserve`, `resize`)
    /// invalidates previously returned slices — they will still point at the
    /// original region on disk, which may no longer be the vec's live block.
    /// Re-fetch with `raw_block()` after any call that may reallocate.
    #[inline]
    #[must_use]
    pub unsafe fn raw_block(&self) -> BStackSlice<'a> {
        let alloc: &'a A = self.slice.allocator();
        unsafe { BStackSlice::from_raw_range(alloc.stack(), self.slice.range().into()) }
    }

    /// Consume the vec and return the underlying block as an owned handle.
    ///
    /// The caller takes responsibility for the allocation.  Reconstruct with
    /// [`BStackByteVec::from_raw_block`].
    #[inline]
    #[must_use]
    pub fn into_raw_block(self) -> BStackOwnedSlice<'a, A> {
        self.slice
    }

    /// Deallocate the underlying block and consume the vec.
    ///
    /// Preferred over `alloc.dealloc(v.into_raw_block())` as it keeps the
    /// dealloc call co-located with the type.
    #[inline]
    pub fn dealloc(self) -> io::Result<()> {
        let alloc: &'a A = self.slice.allocator();
        // The vec is consumed, so there is nowhere to return a recovered handle;
        // surface only the underlying error.
        alloc.dealloc(self.slice).map_err(|e| e.source)
    }
}

impl<'a, A: BStackOwnedSliceAllocator> io::Write for BStackByteVec<'a, A> {
    /// Append `buf` via [`extend_from_slice`](Self::extend_from_slice) and
    /// return `buf.len()`.
    ///
    /// Every call re-reads the 16-byte header via `read_header` and may
    /// `realloc` to grow capacity, so `write_all` over many small chunks is
    /// materially worse than one `extend_from_slice` call. Call
    /// [`reserve`](Self::reserve) beforehand to avoid the repeated regrowth.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.extend_from_slice(buf)?;
        Ok(buf.len())
    }

    /// A no-op: every [`extend_from_slice`](Self::extend_from_slice) is
    /// already durably synced through the underlying [`crate::BStack`] write.
    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// ── atomic bulk / positional operations (requires the `atomic` feature) ─────────

/// Operations built on the crash-atomic in-file byte movers
/// [`crate::BStack::copy`] and [`crate::BStack::cross_exchange`].
///
/// These need the `atomic` Cargo feature **in addition** to the `alloc` + `set`
/// features the rest of the type requires, so they are compiled only when
/// `atomic` is enabled; the base API is unaffected.
///
/// ## Crash-consistency classes
///
/// The append-only movers ([`extend_from_within`](Self::extend_from_within),
/// [`extend_from_bstack_slice`](Self::extend_from_bstack_slice),
/// [`append_from_owned`](Self::append_from_owned)) copy into spare capacity and
/// commit `len` last, so a crash before the commit leaves the extra bytes
/// invisible — the same benign, re-runnable model as
/// [`push`](Self::push)/[`extend_from_slice`](Self::extend_from_slice).
///
/// The in-place movers ([`insert`](Self::insert), [`remove`](Self::remove),
/// [`swap_remove`](Self::swap_remove), [`drain`](Self::drain),
/// [`split_off`](Self::split_off), [`move_tail_into`](Self::move_tail_into))
/// mutate the live region before committing the new `len`.  Every individual
/// `BStack` call is still crash-atomic, so the on-disk `(len, cap)` header is
/// never left invalid, but the multi-step method is not atomic: a crash between
/// the byte move and the `len` commit leaves a *logically torn* (but
/// structurally valid) vec that is not automatically recovered.  Callers needing
/// all-or-nothing semantics for these must layer their own journaling.
#[cfg(feature = "atomic")]
impl<'a, A: BStackOwnedSliceAllocator> BStackByteVec<'a, A> {
    /// Append a copy of the existing bytes `[start, start + count)` to the end of
    /// the vec.
    ///
    /// The source range must lie within the current `len`.  Backed by a single
    /// crash-atomic [`crate::BStack::copy`] into spare capacity; benign crash
    /// model identical to [`extend_from_slice`](Self::extend_from_slice)
    /// (`reserve` → copy → write `len`, so a crash before the commit leaves the
    /// copied bytes invisible and re-running recovers).
    ///
    /// Returns `Ok(None)` if the source range is out of bounds — `start + count`
    /// overflows `u64` or exceeds `len` — and `Ok(Some(()))` on success (an empty
    /// range is a successful no-op).  `Err` is reserved for I/O failures.
    pub fn extend_from_within(&mut self, start: u64, count: u64) -> io::Result<Option<()>> {
        if count == 0 {
            return Ok(Some(()));
        }
        let (len, _) = self.read_header()?;
        // Out of bounds (overflow or past `len`) → None, per the get()-style contract.
        match start.checked_add(count) {
            Some(end) if end <= len => {}
            _ => return Ok(None),
        }
        let alloc: &'a A = self.slice.allocator();
        let stack = alloc.stack();
        self.reserve(count)?;
        // Recompute offsets after `reserve`: a realloc may have moved the block.
        let src = self.abs_offset(start);
        let dst = self.abs_offset(len);
        stack.copy(src, dst, count)?;
        self.write_len_field(len + count)?;
        Ok(Some(()))
    }

    /// Insert `value` at `index`, shifting every byte at or after `index` one slot
    /// to the right.
    ///
    /// The shift is a single crash-atomic [`crate::BStack::copy`] (an overlapping
    /// move, handled internally by the write-in-progress journal).  In-place
    /// mover: see the impl-level note — a crash between the shift and the `len`
    /// commit leaves a logically torn but structurally valid vec.
    ///
    /// Returns `Ok(None)` if `index > len` (out of bounds; nothing is inserted)
    /// and `Ok(Some(()))` on success.  `Err` is reserved for I/O failures.
    pub fn insert(&mut self, index: u64, value: u8) -> io::Result<Option<()>> {
        let (len, _) = self.read_header()?;
        if index > len {
            return Ok(None);
        }
        let alloc: &'a A = self.slice.allocator();
        let stack = alloc.stack();
        self.reserve(1)?;
        if index < len {
            let n = len - index;
            stack.copy(self.abs_offset(index), self.abs_offset(index + 1), n)?;
        }
        self.write_byte_at(index, value)?;
        self.write_len_field(len + 1)?;
        Ok(Some(()))
    }

    /// Remove and return the byte at `index`, shifting every later byte one slot
    /// to the left (preserves order).
    ///
    /// The shift is a single crash-atomic [`crate::BStack::copy`]; the vacated
    /// tail slot is then zeroed as in [`pop`](Self::pop).  In-place mover: a crash
    /// between the shift and the `len` commit leaves a logically torn but
    /// structurally valid vec (see the impl-level note).
    ///
    /// Returns `Ok(None)` if `index >= len` (out of bounds; nothing is removed)
    /// and `Ok(Some(byte))` with the removed byte on success.  `Err` is reserved
    /// for I/O failures.
    pub fn remove(&mut self, index: u64) -> io::Result<Option<u8>> {
        let (len, _) = self.read_header()?;
        if index >= len {
            return Ok(None);
        }
        let value = self.read_byte_at(index)?;
        let tail = len - index - 1;
        if tail > 0 {
            let alloc: &'a A = self.slice.allocator();
            let stack = alloc.stack();
            stack.copy(self.abs_offset(index + 1), self.abs_offset(index), tail)?;
        }
        self.write_len_field(len - 1)?;
        self.zero_byte_at(len - 1)?;
        Ok(Some(value))
    }

    /// Remove the byte at `index` and return it, replacing the hole with the last
    /// byte (O(1), does **not** preserve order).
    ///
    /// Uses a single crash-atomic [`crate::BStack::cross_exchange`] to swap the
    /// element into the tail slot, which is then dropped as in [`pop`](Self::pop).
    /// In-place mover: a crash after the exchange but before the `len` commit
    /// leaves the element at `index` and the last element swapped — a reordering,
    /// not corruption; the vec stays structurally valid (see the impl-level note).
    ///
    /// Returns `Ok(None)` if `index >= len` (out of bounds; nothing is removed)
    /// and `Ok(Some(byte))` with the removed byte on success.  `Err` is reserved
    /// for I/O failures.
    pub fn swap_remove(&mut self, index: u64) -> io::Result<Option<u8>> {
        let (len, _) = self.read_header()?;
        if index >= len {
            return Ok(None);
        }
        let value = self.read_byte_at(index)?;
        let last = len - 1;
        if index != last {
            let alloc: &'a A = self.slice.allocator();
            let stack = alloc.stack();
            stack.cross_exchange(self.abs_offset(index), self.abs_offset(last), 1)?;
        }
        self.write_len_field(last)?;
        self.zero_byte_at(last)?;
        Ok(Some(value))
    }

    /// Append the bytes of an on-disk [`BStackSlice`] to the end of the vec.
    ///
    /// `src` must be backed by the same [`crate::BStack`] as this vec (the bytes
    /// are copied within one file).  Backed by a single crash-atomic
    /// [`crate::BStack::copy`] into spare capacity; benign crash model identical
    /// to [`extend_from_slice`](Self::extend_from_slice).
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::InvalidInput`] if `src` is backed by a different `BStack`.
    pub fn extend_from_bstack_slice(&mut self, src: &BStackSlice<'_>) -> io::Result<()> {
        let alloc: &'a A = self.slice.allocator();
        let stack = alloc.stack();
        if !std::ptr::eq(src.stack(), stack) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "BStackByteVec::extend_from_bstack_slice: source belongs to a different BStack",
            ));
        }
        let n = src.len();
        if n == 0 {
            return Ok(());
        }
        let src_start = src.start();
        let (len, _) = self.read_header()?;
        self.reserve(n)?;
        let dst = self.abs_offset(len);
        stack.copy(src_start, dst, n)?;
        self.write_len_field(len + n)
    }

    /// Copy `dst.len()` bytes from the vec, starting at logical `start`, into the
    /// destination [`BStackSlice`] (overwriting it).
    ///
    /// The number of bytes copied is the destination's length.  `dst` must be
    /// backed by the same [`crate::BStack`] as this vec.  A single crash-atomic
    /// [`crate::BStack::copy`]; the vec itself is not modified.
    ///
    /// Returns `Ok(None)` if the source range is out of bounds — `start +
    /// dst.len()` overflows `u64` or exceeds `len` — and `Ok(Some(()))` on success
    /// (an empty destination is a successful no-op).
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::InvalidInput`] if `dst` is backed by a different `BStack`
    /// (a misuse, distinct from an out-of-range request); otherwise `Err` is
    /// reserved for I/O failures.
    pub fn copy_into_bstack_slice(
        &self,
        start: u64,
        dst: &mut BStackSlice<'_>,
    ) -> io::Result<Option<()>> {
        let alloc: &'a A = self.slice.allocator();
        let stack = alloc.stack();
        if !std::ptr::eq(dst.stack(), stack) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "BStackByteVec::copy_into_bstack_slice: destination belongs to a different BStack",
            ));
        }
        let n = dst.len();
        if n == 0 {
            return Ok(Some(()));
        }
        let (len, _) = self.read_header()?;
        // Out of bounds (overflow or past `len`) → None.
        match start.checked_add(n) {
            Some(end) if end <= len => {}
            _ => return Ok(None),
        }
        stack.copy(self.abs_offset(start), dst.start(), n)?;
        Ok(Some(()))
    }

    /// Append the bytes of `other` to the vec, then deallocate `other` — a move
    /// that consumes the owned handle.
    ///
    /// `other`'s bytes are copied into spare capacity with a single crash-atomic
    /// [`crate::BStack::copy`], `len` is committed, and `other` is freed through
    /// its allocator.  `other` must be backed by the same [`crate::BStack`] as
    /// this vec.  The copy targets invisible spare capacity and `len` is committed
    /// before the free, so a crash before the free leaves the vec correct with
    /// `other` merely still allocated (recoverable), never data loss.
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::InvalidInput`] if `other` is backed by a different
    /// `BStack`; otherwise propagates the append or dealloc I/O error.  `other` is
    /// consumed — and, wherever possible, freed — on every path, so it is never
    /// leaked silently.
    pub fn append_from_owned(&mut self, other: BStackOwnedSlice<'a, A>) -> io::Result<()> {
        let alloc: &'a A = self.slice.allocator();
        let stack = alloc.stack();
        let other_alloc: &'a A = other.allocator();
        if !std::ptr::eq(other_alloc.stack(), stack) {
            // `other` belongs to a different BStack (a misuse). Free it through its
            // own allocator so the call is not a leak; if that free itself fails,
            // surface the I/O error rather than swallowing it — either way the
            // dealloc result is not discarded.
            other_alloc.dealloc(other).map_err(|e| e.source)?;
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "BStackByteVec::append_from_owned: source belongs to a different BStack",
            ));
        }
        // Append first, capturing any error, but always fall through to the free
        // so `other` is never leaked on an append failure.
        let appended = (|| -> io::Result<()> {
            let n = other.len();
            if n == 0 {
                return Ok(());
            }
            let src = other.start();
            let (len, _) = self.read_header()?;
            self.reserve(n)?;
            let dst = self.abs_offset(len);
            stack.copy(src, dst, n)?;
            self.write_len_field(len + n)
        })();
        let freed = other_alloc.dealloc(other).map_err(|e| e.source);
        appended.and(freed)
    }

    /// Move the last `dest.len()` bytes of the vec into `dest`, shrinking the vec
    /// by that many bytes.
    ///
    /// The tail is swapped into `dest` with a single crash-atomic
    /// [`crate::BStack::cross_exchange`] (so the moved bytes exist in exactly one
    /// place afterward), and the vacated tail — now holding `dest`'s former
    /// contents — is dropped and zeroed by shrinking `len` via
    /// [`truncate`](Self::truncate).  `dest` must be backed by the same
    /// [`crate::BStack`] and sized to exactly the tail being moved.
    ///
    /// In-place mover: a crash after the exchange but before the truncate leaves
    /// the vec's still-visible tail holding `dest`'s former bytes — a logically
    /// torn but structurally valid state (see the impl-level note).  `dest` holds
    /// the moved bytes once the exchange commits.
    ///
    /// Returns `Ok(None)` if `dest.len() > len` (out of bounds; the vec is
    /// unchanged) and `Ok(Some(()))` on success (a zero-length `dest` is a
    /// successful no-op).
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::InvalidInput`] if `dest` is backed by a different `BStack`
    /// (a misuse, distinct from an out-of-range request); otherwise `Err` is
    /// reserved for I/O failures.
    pub fn move_tail_into(&mut self, dest: &mut BStackOwnedSlice<'a, A>) -> io::Result<Option<()>> {
        let alloc: &'a A = self.slice.allocator();
        let stack = alloc.stack();
        if !std::ptr::eq(dest.allocator().stack(), stack) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "BStackByteVec::move_tail_into: destination belongs to a different BStack",
            ));
        }
        let n = dest.len();
        if n == 0 {
            return Ok(Some(()));
        }
        let (len, _) = self.read_header()?;
        if n > len {
            return Ok(None);
        }
        let start = len - n;
        stack.cross_exchange(self.abs_offset(start), dest.start(), n)?;
        self.truncate(start)?;
        Ok(Some(()))
    }

    /// Split the vec in two at `at`: `self` keeps `[0, at)` and a new vec holding
    /// `[at, len)` is returned.
    ///
    /// The new vec is allocated with exactly `len - at` bytes of capacity and
    /// the tail is transferred with a single crash-atomic [`crate::BStack::copy`]
    /// straight between the two blocks, never passing through process memory —
    /// this is why the method is only available under `atomic`; there is no
    /// in-memory-copy fallback.  In-place mover: a crash after the copy but
    /// before `self`'s `len` commit leaves the tail bytes duplicated in both
    /// vecs — a logically torn but structurally valid state (see the
    /// impl-level note on this block).
    ///
    /// Returns `Ok(None)` if `at > len` (out of bounds; `self` is unchanged) and
    /// `Ok(Some(tail))` on success (`at == len` returns an empty tail).
    pub fn split_off(&mut self, at: u64) -> io::Result<Option<Self>> {
        let (len, _) = self.read_header()?;
        if at > len {
            return Ok(None);
        }
        let tail_len = len - at;
        let alloc: &'a A = self.slice.allocator();
        let stack = alloc.stack();
        // Even when length is zero, allocate a new vec with a 16-byte header and zero capacity.
        let mut tail = Self::with_capacity(tail_len, alloc)?;
        if tail_len > 0 {
            stack.copy(self.abs_offset(at), tail.abs_offset(0), tail_len)?;
            tail.write_len_field(tail_len)?;
        }
        self.truncate(at)?;
        Ok(Some(tail))
    }

    /// Remove the bytes in `range`, shifting every later byte down to close the
    /// gap, and return the removed bytes.
    ///
    /// The removed bytes are read out first, the tail (if any) is shifted down
    /// with a single crash-atomic [`crate::BStack::copy`], and the shorter `len`
    /// is committed last.  Like [`split_off`](Self::split_off), only available
    /// under `atomic`: there is no crash-atomic way to perform the shift
    /// without it, and no in-memory-copy fallback is offered.
    /// In-place mover: a crash after the shift but before the `len` commit
    /// leaves the payload already compacted while `len` still claims the old,
    /// larger extent — a logically torn but structurally valid state (see the
    /// impl-level note on this block).
    ///
    /// Returns `Ok(None)` if `range.start > range.end` or `range.end > len`
    /// (out of bounds; the vec is unchanged) and `Ok(Some(bytes))` with the
    /// removed bytes on success (an empty range is a successful no-op that
    /// returns an empty `Vec`).
    pub fn drain(&mut self, range: Range<u64>) -> io::Result<Option<Vec<u8>>> {
        let (len, _) = self.read_header()?;
        if range.start > range.end || range.end > len {
            return Ok(None);
        }
        let count = range.end - range.start;
        if count == 0 {
            return Ok(Some(Vec::new()));
        }
        let removed = self
            .slice
            .as_slice()
            .read_range(Self::byte_offset(range.start), Self::byte_offset(range.end))?;
        let tail = len - range.end;
        if tail > 0 {
            let alloc: &'a A = self.slice.allocator();
            let stack = alloc.stack();
            stack.copy(
                self.abs_offset(range.end),
                self.abs_offset(range.start),
                tail,
            )?;
        }
        self.truncate(len - count)?;
        Ok(Some(removed))
    }
}

// ── iterator ──────────────────────────────────────────────────────────────────

/// An iterator over the bytes of a [`BStackByteVec`].
///
/// Constructed by [`BStackByteVec::iter`].  `len` is snapshotted at
/// construction; bytes pushed after construction are not visible.  Each byte
/// is read from disk on demand; I/O errors surface as `Err` items.
pub struct BStackByteVecIter<'b, 'a: 'b, A: BStackOwnedSliceAllocator> {
    vec: &'b BStackByteVec<'a, A>,
    index: u64,
    len: u64,
}

impl<'b, 'a: 'b, A: BStackOwnedSliceAllocator> fmt::Debug for BStackByteVecIter<'b, 'a, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BStackByteVecIter")
            .field("index", &self.index)
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

impl<'b, 'a: 'b, A: BStackOwnedSliceAllocator> Iterator for BStackByteVecIter<'b, 'a, A> {
    type Item = io::Result<u8>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.len {
            return None;
        }
        let result = self.vec.read_byte_at(self.index);
        self.index += 1;
        Some(result)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = (self.len - self.index).min(usize::MAX as u64) as usize;
        (remaining, Some(remaining))
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BStack;
    use crate::alloc::{BStackAllocator, BStackRange, LinearBStackAllocator};
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_path() -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        std::env::temp_dir().join(format!("bstack_bytevec_test_{pid}_{id}.bin"))
    }

    struct Guard(std::path::PathBuf);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn make_alloc() -> (LinearBStackAllocator, std::path::PathBuf) {
        let path = temp_path();
        let alloc = LinearBStackAllocator::new(BStack::open(&path).unwrap());
        (alloc, path)
    }

    #[test]
    fn new_is_empty_with_zero_cap() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let v = BStackByteVec::new(&alloc).unwrap();
        assert_eq!(v.len().unwrap(), 0);
        assert_eq!(v.capacity().unwrap(), 0);
        assert!(v.is_empty().unwrap());
    }

    #[test]
    fn with_capacity_has_zero_len_and_correct_cap() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let v = BStackByteVec::with_capacity(8, &alloc).unwrap();
        assert_eq!(v.len().unwrap(), 0);
        assert_eq!(v.capacity().unwrap(), 8);
        assert!(v.is_empty().unwrap());
    }

    #[test]
    fn from_slice_roundtrip() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let src = [10u8, 20, 30, 40, 50];
        let v = BStackByteVec::from_slice(&src, &alloc).unwrap();
        assert_eq!(v.len().unwrap(), 5);
        assert_eq!(v.capacity().unwrap(), 5);
        for (i, &expected) in src.iter().enumerate() {
            assert_eq!(v.get(i as u64).unwrap(), Some(expected));
        }
        assert_eq!(v.get(5).unwrap(), None);
    }

    #[test]
    fn from_slice_empty() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let v = BStackByteVec::from_slice(&[], &alloc).unwrap();
        assert_eq!(v.len().unwrap(), 0);
        assert_eq!(v.capacity().unwrap(), 0);
    }

    #[test]
    fn raw_block_roundtrip() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);

        let mut v = BStackByteVec::new(&alloc).unwrap();
        v.push(1).unwrap();
        v.push(2).unwrap();
        v.push(3).unwrap();

        let block = v.into_raw_block();
        // Reconstruct from raw block and verify header and elements survive.
        let v2 = unsafe { BStackByteVec::from_raw_block(block) };
        assert_eq!(v2.len().unwrap(), 3);
        assert_eq!(v2.get(0).unwrap(), Some(1u8));
        assert_eq!(v2.get(1).unwrap(), Some(2u8));
        assert_eq!(v2.get(2).unwrap(), Some(3u8));
    }

    #[test]
    fn reopen_header_recovery() {
        // Verify that (len, cap) survive a drop-and-reopen via from_raw_block.
        let path = temp_path();
        let _g = Guard(path.clone());

        let block_range = {
            let alloc = LinearBStackAllocator::new(BStack::open(&path).unwrap());
            let mut v = BStackByteVec::new(&alloc).unwrap();
            v.push(111).unwrap();
            v.push(222).unwrap();
            v.push(33).unwrap();
            // Serialise the raw block coords for later reconstruction.
            let raw = v.into_raw_block();
            let range = BStackRange::new(raw.start(), raw.len());
            // `alloc` (and the BStack file) are closed here.
            range
        };

        // Reopen and reconstruct.
        let alloc = LinearBStackAllocator::new(BStack::open(&path).unwrap());
        let block = unsafe { crate::alloc::BStackOwnedSlice::from_raw_range(&alloc, block_range) };
        let v = unsafe { BStackByteVec::from_raw_block(block) };
        assert_eq!(v.len().unwrap(), 3);
        assert_eq!(v.get(0).unwrap(), Some(111u8));
        assert_eq!(v.get(1).unwrap(), Some(222u8));
        assert_eq!(v.get(2).unwrap(), Some(33u8));
    }

    #[test]
    fn push_pop_lifo() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::new(&alloc).unwrap();
        for i in 0..10u8 {
            v.push(i * 11).unwrap();
        }
        assert_eq!(v.len().unwrap(), 10);
        for i in (0..10u8).rev() {
            assert_eq!(v.pop().unwrap(), Some(i * 11));
        }
        assert_eq!(v.pop().unwrap(), None);
        assert!(v.is_empty().unwrap());
    }

    #[test]
    fn pop_zeros_vacated_slot() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::new(&alloc).unwrap();
        v.push(0xABu8).unwrap();

        // SAFETY: We do not call any reallocation method while holding `block`.
        let block = unsafe { v.raw_block() };
        v.pop().unwrap();

        // Slot 0's byte (at block offset 16) should now be zeroed.
        let slot_bytes = block.read_range(16, 17).unwrap();
        assert_eq!(
            slot_bytes, [0u8; 1],
            "vacated slot must be zeroed after pop"
        );
    }

    #[test]
    fn get_out_of_bounds_returns_none() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::new(&alloc).unwrap();
        v.push(42).unwrap();
        assert_eq!(v.get(0).unwrap(), Some(42u8));
        assert_eq!(v.get(1).unwrap(), None);
        assert_eq!(v.get(u64::MAX).unwrap(), None);
    }

    #[test]
    fn read_bytes_returns_all_elements() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let src = [7u8, 11, 13, 17];
        let v = BStackByteVec::from_slice(&src, &alloc).unwrap();
        assert_eq!(v.read_bytes().unwrap(), src);
    }

    #[test]
    fn push_triggers_growth_from_zero() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::new(&alloc).unwrap();
        // First push must grow from cap=0 to cap=4.
        v.push(1).unwrap();
        assert!(v.capacity().unwrap() >= 4);
        assert_eq!(v.len().unwrap(), 1);
    }

    #[test]
    fn push_doubles_capacity_on_overflow() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::with_capacity(2, &alloc).unwrap();
        v.push(1).unwrap();
        v.push(2).unwrap();
        let cap_before = v.capacity().unwrap();
        assert_eq!(cap_before, 2);
        v.push(3).unwrap(); // triggers doubling
        assert!(v.capacity().unwrap() >= 4);
        assert_eq!(v.len().unwrap(), 3);
    }

    #[test]
    fn extend_from_slice_appends_all_bytes() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::new(&alloc).unwrap();
        v.push(1).unwrap();
        v.extend_from_slice(&[2, 3, 4, 5]).unwrap();
        assert_eq!(v.len().unwrap(), 5);
        assert_eq!(v.read_bytes().unwrap(), [1u8, 2, 3, 4, 5]);
    }

    #[test]
    fn extend_from_slice_empty_is_noop() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::from_slice(&[9u8, 8], &alloc).unwrap();
        let cap_before = v.capacity().unwrap();
        v.extend_from_slice(&[]).unwrap();
        assert_eq!(v.len().unwrap(), 2);
        assert_eq!(v.capacity().unwrap(), cap_before);
        assert_eq!(v.read_bytes().unwrap(), [9u8, 8]);
    }

    #[test]
    fn extend_from_slice_grows_capacity_once() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::new(&alloc).unwrap();
        // cap starts at 0; extending by 100 must grow to hold all bytes.
        let data: Vec<u8> = (0..100u8).collect();
        v.extend_from_slice(&data).unwrap();
        assert_eq!(v.len().unwrap(), 100);
        assert!(v.capacity().unwrap() >= 100);
        assert_eq!(v.read_bytes().unwrap(), data);
    }

    #[test]
    fn extend_from_slice_onto_empty_vec() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::new(&alloc).unwrap();
        v.extend_from_slice(&[10u8, 20, 30]).unwrap();
        assert_eq!(v.read_bytes().unwrap(), [10u8, 20, 30]);
    }

    #[test]
    fn io_write_appends_and_returns_len() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::new(&alloc).unwrap();
        let n = io::Write::write(&mut v, &[1u8, 2, 3]).unwrap();
        assert_eq!(n, 3);
        assert_eq!(v.read_bytes().unwrap(), [1u8, 2, 3]);
    }

    #[test]
    fn io_write_write_all_appends_multiple_chunks() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::new(&alloc).unwrap();
        io::Write::write_all(&mut v, &[1u8, 2]).unwrap();
        io::Write::write_all(&mut v, &[3u8, 4, 5]).unwrap();
        assert_eq!(v.read_bytes().unwrap(), [1u8, 2, 3, 4, 5]);
    }

    #[test]
    fn io_write_flush_is_noop() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::from_slice(&[7u8], &alloc).unwrap();
        io::Write::flush(&mut v).unwrap();
        assert_eq!(v.read_bytes().unwrap(), [7u8]);
    }

    #[test]
    fn truncate_shortens_and_zeros_slots() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::new(&alloc).unwrap();
        v.push(0xAAu8).unwrap();
        v.push(0xBBu8).unwrap();
        v.push(0xCCu8).unwrap();

        // SAFETY: We do not call any reallocation method while holding `block`.
        let block = unsafe { v.raw_block() };
        v.truncate(1).unwrap();

        assert_eq!(v.len().unwrap(), 1);
        assert_eq!(v.capacity().unwrap(), 4); // cap unchanged

        // Slots 1 and 2 (offsets 17..19) must be zeroed.
        let removed = block.read_range(17, 19).unwrap();
        assert_eq!(removed, [0u8; 2], "truncated slots must be zeroed");
    }

    #[test]
    fn truncate_noop_when_new_len_ge_len() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::new(&alloc).unwrap();
        v.push(7).unwrap();
        v.truncate(5).unwrap(); // no-op
        assert_eq!(v.len().unwrap(), 1);
        assert_eq!(v.get(0).unwrap(), Some(7u8));
    }

    #[test]
    fn clear_zeros_all_byte_slots() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::new(&alloc).unwrap();
        v.push(1).unwrap();
        v.push(2).unwrap();
        v.push(3).unwrap();

        // SAFETY: We do not call any reallocation method while holding `block`.
        let block = unsafe { v.raw_block() };
        v.clear().unwrap();

        assert_eq!(v.len().unwrap(), 0);
        // All three byte slots must be zeroed (offsets 16..19).
        let elems = block.read_range(16, 19).unwrap();
        assert_eq!(elems, vec![0u8; 3]);
    }

    #[test]
    fn reserve_noop_when_capacity_sufficient() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::with_capacity(10, &alloc).unwrap();
        v.push(1).unwrap();
        v.reserve(5).unwrap(); // len=1, cap=10 => sufficient
        assert_eq!(v.capacity().unwrap(), 10);
    }

    #[test]
    fn reserve_grows_when_needed() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::new(&alloc).unwrap();
        v.push(1).unwrap();
        // len=1, cap>=4; reserve(100) must grow to at least 101.
        v.reserve(100).unwrap();
        assert!(v.capacity().unwrap() >= 101);
    }

    #[test]
    fn reserve_overflow_returns_error() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::new(&alloc).unwrap();
        v.push(1).unwrap(); // len=1
        // Requesting u64::MAX additional would overflow len+additional.
        let err = v.reserve(u64::MAX).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn resize_grow_fills_with_value() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::new(&alloc).unwrap();
        v.push(1).unwrap();
        v.resize(5, 99u8).unwrap();
        assert_eq!(v.len().unwrap(), 5);
        assert_eq!(v.get(0).unwrap(), Some(1u8));
        for i in 1..5 {
            assert_eq!(v.get(i).unwrap(), Some(99u8));
        }
    }

    #[test]
    fn resize_shrink_truncates() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::from_slice(&[1, 2, 3, 4, 5], &alloc).unwrap();
        v.resize(2, 0).unwrap();
        assert_eq!(v.len().unwrap(), 2);
        assert_eq!(v.get(0).unwrap(), Some(1u8));
        assert_eq!(v.get(1).unwrap(), Some(2u8));
        assert_eq!(v.get(2).unwrap(), None);
    }

    #[test]
    fn as_slice_covers_populated_bytes_only() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let v = BStackByteVec::from_slice(&[10u8, 20, 30], &alloc).unwrap();
        let s = v.as_slice().unwrap();
        assert_eq!(s.len(), 3);
        let bytes = s.read().unwrap();
        assert_eq!(bytes, [10u8, 20, 30]);
    }

    #[test]
    fn iter_yields_all_bytes_in_order() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let src = [3u8, 1, 4, 1, 5, 9, 2, 6];
        let v = BStackByteVec::from_slice(&src, &alloc).unwrap();
        let collected: Vec<u8> = v.iter().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(collected, src);
    }

    #[test]
    fn iter_size_hint_tracks_remaining() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let v = BStackByteVec::from_slice(&[1u8, 2, 3, 4, 5], &alloc).unwrap();
        let mut it = v.iter().unwrap();
        assert_eq!(it.size_hint(), (5, Some(5)));
        it.next().unwrap().unwrap();
        assert_eq!(it.size_hint(), (4, Some(4)));
        it.next().unwrap().unwrap();
        it.next().unwrap().unwrap();
        assert_eq!(it.size_hint(), (2, Some(2)));
    }

    #[test]
    fn iter_stops_at_len_snapshot() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let v = BStackByteVec::from_slice(&[10u8, 20, 30], &alloc).unwrap();
        let count = v.iter().unwrap().count();
        assert_eq!(count, 3);
    }

    #[test]
    fn iter_on_empty_vec_yields_nothing() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let v = BStackByteVec::new(&alloc).unwrap();
        let count = v.iter().unwrap().count();
        assert_eq!(count, 0);
    }

    #[test]
    fn with_capacity_overflow_returns_error() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        // u64::MAX + 16 overflows u64.
        let err = BStackByteVec::with_capacity(u64::MAX, &alloc).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn reserve_overflow_through_grow_returns_error() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::new(&alloc).unwrap();
        v.push(1).unwrap();
        let err = v.reserve(u64::MAX).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    // ── integration: interop with BStackSliceReader ───────────────────────────

    #[test]
    fn as_slice_readable_via_slice_reader() {
        use std::io::Read;
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let v = BStackByteVec::from_slice(&[0x0Au8, 0x0B, 0x0C], &alloc).unwrap();

        let s = v.as_slice().unwrap();
        let mut reader = s.reader();
        let mut buf = [0u8; 3];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, &[0x0Au8, 0x0B, 0x0C]);
    }

    #[test]
    fn dealloc_reclaims_tail_from_linear_allocator() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);

        let size_before = alloc.stack().len().unwrap();
        let mut v = BStackByteVec::new(&alloc).unwrap();
        v.push(1).unwrap();
        v.push(2).unwrap();
        let size_after_push = alloc.stack().len().unwrap();
        assert!(size_after_push > size_before);

        v.dealloc().unwrap();
        // LinearBStackAllocator::dealloc on the tail slice calls BStack::discard,
        // so the stack should shrink back to its pre-allocation size.
        assert_eq!(alloc.stack().len().unwrap(), size_before);
    }

    #[test]
    fn two_vecs_on_same_allocator_do_not_interfere() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);

        // Pre-size both vecs so neither triggers a realloc: LinearBStackAllocator
        // can only grow the tail allocation, so interleaved pushes would fail
        // if the blocks needed to move.
        let mut a = BStackByteVec::with_capacity(4, &alloc).unwrap();
        let mut b = BStackByteVec::with_capacity(4, &alloc).unwrap();

        a.push(10).unwrap();
        b.push(20).unwrap();
        a.push(11).unwrap();
        b.push(21).unwrap();

        assert_eq!(a.len().unwrap(), 2);
        assert_eq!(b.len().unwrap(), 2);
        assert_eq!(a.get(0).unwrap(), Some(10u8));
        assert_eq!(a.get(1).unwrap(), Some(11u8));
        assert_eq!(b.get(0).unwrap(), Some(20u8));
        assert_eq!(b.get(1).unwrap(), Some(21u8));
    }

    #[test]
    fn as_slice_len_tracks_vec_len() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::new(&alloc).unwrap();

        assert_eq!(v.as_slice().unwrap().len(), 0);
        v.push(1).unwrap();
        assert_eq!(v.as_slice().unwrap().len(), 1);
        v.push(2).unwrap();
        assert_eq!(v.as_slice().unwrap().len(), 2);
        v.pop().unwrap();
        assert_eq!(v.as_slice().unwrap().len(), 1);
    }

    // ── set / reserve_exact / shrink / fill (alloc + set) ─────────────────────

    #[test]
    fn set_overwrites_existing_byte() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::from_slice(&[1, 2, 3], &alloc).unwrap();
        assert_eq!(v.set(1, 99).unwrap(), Some(()));
        assert_eq!(v.read_bytes().unwrap(), [1, 99, 3]);
        // Out of bounds → None, not an error.
        assert_eq!(v.set(3, 0).unwrap(), None);
        assert_eq!(v.read_bytes().unwrap(), [1, 99, 3]);
    }

    #[test]
    fn reserve_exact_grows_to_exact_capacity() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::new(&alloc).unwrap();
        v.push(1).unwrap(); // len=1, cap>=4
        v.reserve_exact(9).unwrap(); // needs exactly 10
        assert_eq!(v.capacity().unwrap(), 10);
    }

    #[test]
    fn shrink_to_fit_releases_spare_capacity() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::with_capacity(16, &alloc).unwrap();
        v.extend_from_slice(&[1, 2, 3]).unwrap();
        assert_eq!(v.capacity().unwrap(), 16);
        v.shrink_to_fit().unwrap();
        assert_eq!(v.capacity().unwrap(), 3);
        assert_eq!(v.read_bytes().unwrap(), [1, 2, 3]);
    }

    #[test]
    fn shrink_to_respects_len_lower_bound_and_never_grows() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::with_capacity(20, &alloc).unwrap();
        v.extend_from_slice(&[1, 2, 3, 4, 5]).unwrap(); // len=5
        v.shrink_to(2).unwrap(); // below len -> clamps up to len
        assert_eq!(v.capacity().unwrap(), 5);
        v.shrink_to(100).unwrap(); // above cap -> no-op
        assert_eq!(v.capacity().unwrap(), 5);
        assert_eq!(v.read_bytes().unwrap(), [1, 2, 3, 4, 5]);
    }

    #[test]
    fn fill_overwrites_all_bytes() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::from_slice(&[1, 2, 3, 4], &alloc).unwrap();
        v.fill(0xEE).unwrap();
        assert_eq!(v.read_bytes().unwrap(), [0xEE; 4]);
    }

    // ── atomic movers: extend_from_within / insert / remove / swap_remove ─────

    #[cfg(feature = "atomic")]
    #[test]
    fn extend_from_within_appends_copy() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::from_slice(&[1, 2, 3, 4], &alloc).unwrap();
        assert_eq!(v.extend_from_within(1, 2).unwrap(), Some(())); // copies [2, 3]
        assert_eq!(v.read_bytes().unwrap(), [1, 2, 3, 4, 2, 3]);
        // Source range out of bounds → None.
        assert_eq!(v.extend_from_within(4, 5).unwrap(), None);
        assert_eq!(v.read_bytes().unwrap(), [1, 2, 3, 4, 2, 3]);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn insert_shifts_bytes_right() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::from_slice(&[10, 20, 30], &alloc).unwrap();
        assert_eq!(v.insert(1, 99).unwrap(), Some(()));
        assert_eq!(v.read_bytes().unwrap(), [10, 99, 20, 30]);
        assert_eq!(v.insert(4, 40).unwrap(), Some(())); // at the end
        assert_eq!(v.read_bytes().unwrap(), [10, 99, 20, 30, 40]);
        // Index past len → None.
        assert_eq!(v.insert(10, 0).unwrap(), None);
        assert_eq!(v.read_bytes().unwrap(), [10, 99, 20, 30, 40]);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn remove_shifts_bytes_left() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::from_slice(&[10, 20, 30, 40], &alloc).unwrap();
        assert_eq!(v.remove(1).unwrap(), Some(20));
        assert_eq!(v.read_bytes().unwrap(), [10, 30, 40]);
        assert_eq!(v.remove(2).unwrap(), Some(40)); // last element
        assert_eq!(v.read_bytes().unwrap(), [10, 30]);
        // Index out of bounds → None.
        assert_eq!(v.remove(5).unwrap(), None);
        assert_eq!(v.read_bytes().unwrap(), [10, 30]);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn swap_remove_replaces_with_last() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::from_slice(&[10, 20, 30, 40], &alloc).unwrap();
        assert_eq!(v.swap_remove(1).unwrap(), Some(20));
        // slot 1 now holds the former last element (40); order is not preserved.
        assert_eq!(v.read_bytes().unwrap(), [10, 40, 30]);
        assert_eq!(v.swap_remove(2).unwrap(), Some(30)); // removing the last element
        assert_eq!(v.read_bytes().unwrap(), [10, 40]);
        // Index out of bounds → None.
        assert_eq!(v.swap_remove(9).unwrap(), None);
        assert_eq!(v.read_bytes().unwrap(), [10, 40]);
    }

    // ── atomic cross-slice movers ─────────────────────────────────────────────

    #[cfg(feature = "atomic")]
    #[test]
    fn extend_from_bstack_slice_appends() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        // Pre-size so the vec never reallocs once `src` becomes the tail.
        let mut v = BStackByteVec::with_capacity(16, &alloc).unwrap();
        v.extend_from_slice(&[1, 2]).unwrap();
        let mut src = alloc.alloc(3).unwrap();
        src.write([7u8, 8, 9]).unwrap();
        v.extend_from_bstack_slice(&src.as_slice()).unwrap();
        assert_eq!(v.read_bytes().unwrap(), [1, 2, 7, 8, 9]);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn copy_into_bstack_slice_writes_destination() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let v = BStackByteVec::from_slice(&[10, 20, 30, 40, 50], &alloc).unwrap();
        let mut dst = alloc.alloc(3).unwrap();
        assert_eq!(
            v.copy_into_bstack_slice(1, &mut dst.as_slice_mut())
                .unwrap(),
            Some(())
        );
        assert_eq!(dst.read().unwrap(), [20, 30, 40]);
        // start + dst.len() beyond the vec's len → None.
        assert_eq!(
            v.copy_into_bstack_slice(4, &mut dst.as_slice_mut())
                .unwrap(),
            None
        );
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn append_from_owned_moves_and_frees() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::with_capacity(16, &alloc).unwrap();
        v.extend_from_slice(&[1, 2, 3]).unwrap();
        let mut owned = alloc.alloc(2).unwrap();
        owned.write([8u8, 9]).unwrap();
        v.append_from_owned(owned).unwrap();
        assert_eq!(v.read_bytes().unwrap(), [1, 2, 3, 8, 9]);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn move_tail_into_transfers_tail_bytes() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::from_slice(&[1, 2, 3, 4, 5], &alloc).unwrap();
        let mut dest = alloc.alloc(2).unwrap();
        assert_eq!(v.move_tail_into(&mut dest).unwrap(), Some(()));
        assert_eq!(dest.read().unwrap(), [4, 5]);
        assert_eq!(v.read_bytes().unwrap(), [1, 2, 3]);
        assert_eq!(v.len().unwrap(), 3);
        // A tail larger than len → None, vec unchanged.
        let mut too_big = alloc.alloc(9).unwrap();
        assert_eq!(v.move_tail_into(&mut too_big).unwrap(), None);
        assert_eq!(v.read_bytes().unwrap(), [1, 2, 3]);
    }

    // ── split_off (alloc + set + atomic) ───────────────────────────────────────

    #[cfg(feature = "atomic")]
    #[test]
    fn split_off_moves_tail_into_new_vec() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::from_slice(&[1, 2, 3, 4, 5], &alloc).unwrap();
        let tail = v.split_off(2).unwrap().unwrap();
        assert_eq!(v.read_bytes().unwrap(), [1, 2]);
        assert_eq!(tail.read_bytes().unwrap(), [3, 4, 5]);
        assert_eq!(tail.capacity().unwrap(), 3);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn split_off_at_zero_moves_everything() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::from_slice(&[1, 2, 3], &alloc).unwrap();
        let tail = v.split_off(0).unwrap().unwrap();
        assert_eq!(v.read_bytes().unwrap(), Vec::<u8>::new());
        assert_eq!(tail.read_bytes().unwrap(), [1, 2, 3]);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn split_off_at_len_returns_empty_tail() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::from_slice(&[1, 2, 3], &alloc).unwrap();
        let tail = v.split_off(3).unwrap().unwrap();
        assert_eq!(v.read_bytes().unwrap(), [1, 2, 3]);
        assert_eq!(tail.read_bytes().unwrap(), Vec::<u8>::new());
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn split_off_past_len_returns_none() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::from_slice(&[1, 2, 3], &alloc).unwrap();
        assert!(v.split_off(4).unwrap().is_none());
        assert_eq!(v.read_bytes().unwrap(), [1, 2, 3]);
    }

    // ── drain (set + atomic) ───────────────────────────────────────────────────

    #[cfg(feature = "atomic")]
    #[test]
    fn drain_removes_interior_range_and_returns_it() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::from_slice(&[1, 2, 3, 4, 5], &alloc).unwrap();
        let removed = v.drain(1..3).unwrap().unwrap();
        assert_eq!(removed, [2, 3]);
        assert_eq!(v.read_bytes().unwrap(), [1, 4, 5]);
        assert_eq!(v.len().unwrap(), 3);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn drain_at_tail_needs_no_shift() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::from_slice(&[1, 2, 3, 4], &alloc).unwrap();
        let removed = v.drain(2..4).unwrap().unwrap();
        assert_eq!(removed, [3, 4]);
        assert_eq!(v.read_bytes().unwrap(), [1, 2]);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn drain_empty_range_is_noop() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::from_slice(&[1, 2, 3], &alloc).unwrap();
        let removed = v.drain(1..1).unwrap().unwrap();
        assert_eq!(removed, Vec::<u8>::new());
        assert_eq!(v.read_bytes().unwrap(), [1, 2, 3]);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn drain_out_of_bounds_returns_none() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::from_slice(&[1, 2, 3], &alloc).unwrap();
        assert!(v.drain(2..4).unwrap().is_none());
        #[allow(clippy::reversed_empty_ranges)]
        let out_of_order = v.drain(3..2).unwrap();
        assert!(out_of_order.is_none());
        assert_eq!(v.read_bytes().unwrap(), [1, 2, 3]);
    }
}
