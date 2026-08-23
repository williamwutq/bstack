//! Growable byte vector backed by a [`BStack`] allocation.
//!
//! Requires features `alloc` and `set`.

use super::{BStackSlice, BStackSliceAllocator};
use std::fmt;
use std::io;

/// Byte offset of the first element within the block (past the 16-byte header).
const HEADER_LEN: u64 = 16;

/// A growable byte vector backed by a [`crate::BStack`] allocation.
///
/// `BStackByteVec<'a, A>` stores `u8` elements inside a [`crate::BStack`]
/// allocation managed by allocator `A`.  Every mutation issues a durable sync
/// through the allocator so the contents survive a process crash.
///
/// For a general typed vector (e.g. over `u32`, structs, etc.), see the
/// `BStackVec<T>` entry in `PLANNED.md`.  A general type parameter requires a
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
/// then zeros all removed slots in a single [`BStackSlice::zero_range`] call.
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
/// | `pop` | read element → decrement `len` → zero slot | Crash after `len` decrement but before zero: stale byte may remain in the now out-of-range slot, but reads never include it because it is beyond `len`. |
/// | `truncate` | write `len` → zero removed slots | Crash after `len` write but before zero: stale bytes may remain in now out-of-range slots, but reads never include them because they are beyond `len`. |
/// | `resize` (grow) | `reserve` → write elements → write `len` | Elements between the old and new `len` may be partially written. |
/// | `clear` | (delegates to `truncate(0)`) | See `truncate`. |
/// | `reserve`, `reserve_exact` | `realloc` → write `cap` | Crash between the two: cap field may reflect the old value; the block is larger than cap indicates. Harmless — the next `push` re-checks and may realloc again unnecessarily. |
/// | `shrink_to`, `shrink_to_fit` | `realloc` (shrink) → write `cap` | Crash between the two: block is smaller than the stale `cap` claims; the next `push` re-checks and grows as needed. |
/// | `set` | write element | Single crash-atomic write; no torn state. |
/// | `fill` | one `repeat` | Single crash-atomic fill of the whole `len`; no torn state. |
///
/// In all cases the header is re-read from disk on the next call, so the
/// on-disk `(len, cap)` always reflects the last fully committed step.  The
/// [`from_raw_block`](BStackByteVec::from_raw_block) constructor can be used
/// to reconstruct the handle after a reopen without any additional recovery
/// logic.
///
/// ## Feature flags
///
/// Requires both the `alloc` and `set` Cargo features.
pub struct BStackByteVec<'a, A: BStackSliceAllocator> {
    /// The full block: header (16 B) followed by byte data.
    slice: BStackSlice<'a, A>,
}

impl<'a, A: BStackSliceAllocator> fmt::Debug for BStackByteVec<'a, A> {
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

// ── private helpers ──────────────────────────────────────────────────────────

impl<'a, A: BStackSliceAllocator> BStackByteVec<'a, A> {
    /// Compute the total block size in bytes for `capacity` bytes of data.
    ///
    /// Returns `Err(InvalidInput)` if the arithmetic overflows `u64` — this
    /// prevents passing a wrapped-around value to the allocator and accidentally
    /// obtaining a block that is too small.
    fn block_size(capacity: u64) -> io::Result<u64> {
        capacity.checked_add(HEADER_LEN).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "BStackByteVec: block size overflows u64",
            )
        })
    }

    fn byte_offset(index: u64) -> u64 {
        // `index` is always < `len` which was read from a header we wrote, so
        // this addition cannot overflow in well-formed data.  A corrupt
        // on-disk `len` could cause overflow and a debug-mode panic; that is
        // acceptable — corruption is not a recoverable condition here.
        HEADER_LEN + index
    }

    /// Re-read `(len, capacity)` from the block header on disk.
    fn read_header(&self) -> io::Result<(u64, u64)> {
        let mut hdr = [0u8; 16];
        self.slice.read_range_into(0, &mut hdr)?;
        // `read_range_into` fills exactly 16 bytes on success; the slices
        // are 8 bytes each so try_into() is always Ok — these cannot panic.
        let len = u64::from_le_bytes(hdr[..8].try_into().unwrap());
        let cap = u64::from_le_bytes(hdr[8..].try_into().unwrap());
        Ok((len, cap))
    }

    fn write_len_field(&self, len: u64) -> io::Result<()> {
        self.slice.write_range(0, len.to_le_bytes())
    }

    fn write_cap_field(&self, cap: u64) -> io::Result<()> {
        self.slice.write_range(8, cap.to_le_bytes())
    }

    fn write_header(&self, len: u64, cap: u64) -> io::Result<()> {
        let mut hdr = [0u8; 16];
        hdr[0..8].copy_from_slice(&len.to_le_bytes());
        hdr[8..16].copy_from_slice(&cap.to_le_bytes());
        self.slice.write_range(0, hdr)
    }

    fn read_byte_at(&self, index: u64) -> io::Result<u8> {
        let start = Self::byte_offset(index);
        let mut byte = [0u8; 1];
        self.slice.read_range_into(start, &mut byte)?;
        Ok(byte[0])
    }

    fn write_byte_at(&self, index: u64, value: u8) -> io::Result<()> {
        let start = Self::byte_offset(index);
        self.slice.write_range(start, [value])
    }

    fn write_bytes_at(&self, start_index: u64, values: &[u8]) -> io::Result<()> {
        let start = Self::byte_offset(start_index);
        self.slice.write_range(start, values)
    }

    fn zero_byte_at(&self, index: u64) -> io::Result<()> {
        self.slice.zero_range(Self::byte_offset(index), 1)
    }

    /// Reallocate the block to hold `new_cap` bytes, updating `self.slice`.
    ///
    /// Handles both growth (`push`, `reserve`, `reserve_exact`) and shrink
    /// (`shrink_to`, `shrink_to_fit`): the underlying allocator `realloc`
    /// reallocates in either direction, so a `new_cap` below the current
    /// capacity shrinks the backing block.
    fn realloc_to(&mut self, new_cap: u64) -> io::Result<()> {
        let new_size = Self::block_size(new_cap)?;
        // SAFETY: Slice origin requirement is upheld because `self.slice` is
        // the original allocation handle returned by the constructor, and
        // `realloc` returns a new slice for the same block.
        let new_slice = self.slice.allocator().realloc(self.slice, new_size)?;
        self.slice = new_slice;
        Ok(())
    }
}

// ── public API ────────────────────────────────────────────────────────────────

impl<'a, A: BStackSliceAllocator> BStackByteVec<'a, A> {
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
        let vec = Self { slice };
        // len is already 0 (zeroed by alloc); write the non-zero cap field.
        vec.write_cap_field(capacity)?;
        Ok(vec)
    }

    /// Allocate a `BStackByteVec` and populate it from a byte slice.
    ///
    /// The resulting vec has `len == capacity == data.len()`.
    pub fn from_slice(data: &[u8], alloc: &'a A) -> io::Result<Self> {
        let len = data.len() as u64;
        let slice = alloc.alloc(Self::block_size(len)?)?;
        let vec = Self { slice };
        if len > 0 {
            vec.write_header(len, len)?;
            vec.write_bytes_at(0, data)?;
        }
        Ok(vec)
    }

    /// Reconstruct a `BStackByteVec` from a raw block slice.
    ///
    /// # Safety
    ///
    /// `slice` must be the original allocation handle (not a sub-slice) returned
    /// by one of the `BStackByteVec` constructors on the same allocator, and the
    /// block header must have been written by a `BStackByteVec<A>`.  Passing an
    /// unrelated slice is undefined behaviour.
    #[inline]
    #[must_use]
    pub unsafe fn from_raw_block(slice: BStackSlice<'a, A>) -> Self {
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
        self.slice.read_range(HEADER_LEN, HEADER_LEN + len)
    }

    /// Return a [`BStackSlice`] spanning only the populated byte region.
    ///
    /// The slice covers `[16, 16 + len)` within the block.
    /// It is a sub-slice and must **not** be passed to `realloc` or `dealloc`;
    /// use [`raw_block`](Self::raw_block) for that.
    ///
    /// # Panics
    ///
    /// Panics if the `len` read from the block header is corrupt (larger than
    /// the block can hold), causing the computed end offset to exceed the
    /// block's length.  Corruption is not a recoverable condition here.
    #[track_caller]
    pub fn as_slice(&self) -> io::Result<BStackSlice<'a, A>> {
        let (len, _) = self.read_header()?;
        Ok(self.slice.subslice(HEADER_LEN, HEADER_LEN + len))
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
    /// are zeroed in a single [`BStackSlice::zero_range`] call; capacity is
    /// unchanged.
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
    /// Backed by a single [`crate::BStack::repeat`] over the populated region,
    /// with the same durability as `set` (on this line `repeat` writes the whole
    /// region directly — it has no write-in-progress journal).  A no-op on an
    /// empty vec; capacity and `len` are unchanged.
    pub fn fill(&mut self, value: u8) -> io::Result<()> {
        let (len, _) = self.read_header()?;
        if len == 0 {
            return Ok(());
        }
        // Fill only the populated region `[0, len)` with a single `repeat`,
        // mirroring `BStackSlice::fill`.
        let mut region = self.slice.subslice(HEADER_LEN, HEADER_LEN + len);
        region.fill(value)
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

    /// Return the underlying block slice (header + all allocated byte space).
    ///
    /// This is the current allocation handle. If you need to take ownership of
    /// that allocation in order to pass it to
    /// [`crate::BStackAllocator::realloc`] or [`crate::BStackAllocator::dealloc`],
    /// first consume the vec with [`BStackByteVec::into_raw_block`].
    ///
    /// # Safety
    ///
    /// The caller must ensure that the allocation remains owned by this
    /// `BStackByteVec` for as long as the vec may still be used. In particular,
    /// the returned handle must not be passed to `dealloc`, `realloc`, or used
    /// in any other way that invalidates or transfers ownership of the
    /// allocation while this vec is still alive and accessible.
    ///
    /// The caller must also ensure that the returned handle is not used after
    /// any reallocation of this vec occurs. Any call that may reallocate
    /// (`push`, `reserve`, `resize`) invalidates previously returned handles:
    /// using a stale handle with `realloc` or `dealloc` can corrupt allocator
    /// state or lose data. Re-fetch with `raw_block()` after any mutation that
    /// may reallocate.
    #[inline]
    #[must_use]
    pub unsafe fn raw_block(&self) -> BStackSlice<'a, A> {
        self.slice
    }

    /// Consume the vec and return the underlying block slice.
    ///
    /// The caller takes responsibility for the allocation.  Reconstruct with
    /// [`BStackByteVec::from_raw_block`].
    #[inline]
    #[must_use]
    pub fn into_raw_block(self) -> BStackSlice<'a, A> {
        self.slice
    }

    /// Deallocate the underlying block and consume the vec.
    ///
    /// Preferred over `alloc.dealloc(v.into_raw_block())` as it keeps the
    /// dealloc call co-located with the type.  The slice origin requirement is
    /// upheld because `self.slice` is always the original allocation handle
    /// returned by the constructor (or a subsequent `realloc`).
    ///
    /// After this call the backing storage is released; no further I/O on any
    /// handle derived from this vec (e.g. a prior [`raw_block`](Self::raw_block)
    /// copy) is valid.
    #[inline]
    pub fn dealloc(self) -> io::Result<()> {
        self.slice.allocator().dealloc(self.slice)
    }
}

// ── iterator ──────────────────────────────────────────────────────────────────

/// An iterator over the bytes of a [`BStackByteVec`].
///
/// Constructed by [`BStackByteVec::iter`].  `len` is snapshotted at
/// construction; bytes pushed after construction are not visible.  Each byte
/// is read from disk on demand; I/O errors surface as `Err` items.
pub struct BStackByteVecIter<'b, 'a: 'b, A: BStackSliceAllocator> {
    vec: &'b BStackByteVec<'a, A>,
    index: u64,
    len: u64,
}

impl<'b, 'a: 'b, A: BStackSliceAllocator> fmt::Debug for BStackByteVecIter<'b, 'a, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BStackByteVecIter")
            .field("index", &self.index)
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

impl<'b, 'a: 'b, A: BStackSliceAllocator> Iterator for BStackByteVecIter<'b, 'a, A> {
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
        // `self.index <= self.len` is invariant; subtraction cannot underflow.
        // On 32-bit platforms the cast saturates to usize::MAX, which is a
        // valid (conservative) hint per the Iterator contract.
        let remaining = (self.len - self.index).min(usize::MAX as u64) as usize;
        (remaining, Some(remaining))
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BStack;
    use crate::alloc::{BStackAllocator, LinearBStackAllocator};
    use std::sync::atomic::{AtomicU64, Ordering};

    // ── helpers ───────────────────────────────────────────────────────────────

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

    // ── constructors and header recovery ─────────────────────────────────────

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

        let block_bytes = {
            let alloc = LinearBStackAllocator::new(BStack::open(&path).unwrap());
            let mut v = BStackByteVec::new(&alloc).unwrap();
            v.push(111).unwrap();
            v.push(222).unwrap();
            v.push(33).unwrap(); // distinct value to verify each slot independently
            // Serialise the raw block handle for later reconstruction.
            let bytes: [u8; 16] = v.into_raw_block().into();
            bytes
            // `alloc` (and the BStack file) are closed here.
        };

        // Reopen and reconstruct.
        let alloc = LinearBStackAllocator::new(BStack::open(&path).unwrap());
        let block = unsafe { crate::alloc::BStackSlice::from_bytes(&alloc, block_bytes) };
        let v = unsafe { BStackByteVec::from_raw_block(block) };
        assert_eq!(v.len().unwrap(), 3);
        assert_eq!(v.get(0).unwrap(), Some(111u8));
        assert_eq!(v.get(1).unwrap(), Some(222u8));
        assert_eq!(v.get(2).unwrap(), Some(33u8));
    }

    // ── push / pop / get ─────────────────────────────────────────────────────

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

    // ── growth ───────────────────────────────────────────────────────────────

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

    // ── truncate / clear ──────────────────────────────────────────────────────

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

    // ── reserve / resize overflow ─────────────────────────────────────────────

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

    // ── as_slice ──────────────────────────────────────────────────────────────

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

    // ── iterator snapshot semantics ───────────────────────────────────────────

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

    // ── integration: block_size overflow ─────────────────────────────────────

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

    // ── integration: dealloc reclaims tail ───────────────────────────────────

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

    // ── integration: two vecs on the same allocator ───────────────────────────

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

    // ── integration: as_slice length after mutation ───────────────────────────

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

    // ── set / fill / reserve_exact / shrink_to / shrink_to_fit ────────────────

    #[test]
    fn set_overwrites_existing_byte() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::from_slice(&[1, 2, 3], &alloc).unwrap();
        assert_eq!(v.set(1, 99).unwrap(), Some(()));
        assert_eq!(v.read_bytes().unwrap(), [1, 99, 3]);
        // Out of bounds → None, not an error, and nothing is written.
        assert_eq!(v.set(3, 0).unwrap(), None);
        assert_eq!(v.set(u64::MAX, 0).unwrap(), None);
        assert_eq!(v.read_bytes().unwrap(), [1, 99, 3]);
    }

    #[test]
    fn fill_overwrites_all_bytes() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::from_slice(&[1, 2, 3, 4], &alloc).unwrap();
        v.fill(0xEE).unwrap();
        assert_eq!(v.read_bytes().unwrap(), [0xEE; 4]);
        assert_eq!(v.len().unwrap(), 4);
    }

    #[test]
    fn fill_on_empty_vec_is_noop() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::new(&alloc).unwrap();
        v.fill(0x55).unwrap();
        assert_eq!(v.len().unwrap(), 0);
        assert!(v.read_bytes().unwrap().is_empty());
    }

    #[test]
    fn reserve_exact_grows_to_exact_capacity() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::new(&alloc).unwrap();
        v.push(1).unwrap(); // len=1, cap>=4
        v.reserve_exact(9).unwrap(); // needs exactly len+9 = 10
        assert_eq!(v.capacity().unwrap(), 10);
        assert_eq!(v.len().unwrap(), 1);
        assert_eq!(v.get(0).unwrap(), Some(1u8)); // data preserved
    }

    #[test]
    fn reserve_exact_noop_when_sufficient() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::with_capacity(10, &alloc).unwrap();
        v.push(7).unwrap(); // len=1, cap=10
        v.reserve_exact(5).unwrap(); // needs 6 <= 10 → no-op
        assert_eq!(v.capacity().unwrap(), 10);
    }

    #[test]
    fn reserve_exact_overflow_returns_error() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::new(&alloc).unwrap();
        v.push(1).unwrap(); // len=1
        let err = v.reserve_exact(u64::MAX).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn shrink_to_fit_releases_spare_capacity() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::with_capacity(16, &alloc).unwrap();
        v.push(1).unwrap();
        v.push(2).unwrap();
        v.push(3).unwrap();
        assert_eq!(v.capacity().unwrap(), 16);
        v.shrink_to_fit().unwrap();
        assert_eq!(v.capacity().unwrap(), 3); // now exactly len
        assert_eq!(v.read_bytes().unwrap(), [1, 2, 3]); // data preserved
    }

    #[test]
    fn shrink_to_respects_len_lower_bound_and_never_grows() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v = BStackByteVec::with_capacity(20, &alloc).unwrap();
        for b in [1u8, 2, 3, 4, 5] {
            v.push(b).unwrap(); // len=5, cap=20
        }
        v.shrink_to(2).unwrap(); // below len → clamps up to len (5)
        assert_eq!(v.capacity().unwrap(), 5);
        v.shrink_to(100).unwrap(); // above cap → no-op
        assert_eq!(v.capacity().unwrap(), 5);
        assert_eq!(v.read_bytes().unwrap(), [1, 2, 3, 4, 5]); // data preserved
    }

    #[test]
    fn set_and_capacity_persist_across_reopen() {
        // set + reserve_exact must survive a drop-and-reopen via from_raw_block.
        let path = temp_path();
        let _g = Guard(path.clone());

        let block_bytes = {
            let alloc = LinearBStackAllocator::new(BStack::open(&path).unwrap());
            let mut v = BStackByteVec::from_slice(&[10u8, 20, 30], &alloc).unwrap();
            v.set(1, 200).unwrap();
            v.reserve_exact(5).unwrap(); // cap becomes exactly 8
            let bytes: [u8; 16] = v.into_raw_block().into();
            bytes
        };

        let alloc = LinearBStackAllocator::new(BStack::open(&path).unwrap());
        let block = unsafe { crate::alloc::BStackSlice::from_bytes(&alloc, block_bytes) };
        let v = unsafe { BStackByteVec::from_raw_block(block) };
        assert_eq!(v.len().unwrap(), 3);
        assert_eq!(v.capacity().unwrap(), 8);
        assert_eq!(v.read_bytes().unwrap(), [10, 200, 30]);
    }
}
