//! Growable byte vector backed by a [`BStack`] allocation.
//!
//! Requires features `alloc` and `set`.

use super::{BStackOwnedSlice, BStackOwnedSliceAllocator, BStackRange, BStackSlice};
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
/// | `pop` | read element → decrement `len` → zero slot | Crash after `len` decrement but before zero: stale byte may remain in the now out-of-range slot, but reads never include it because it is beyond `len`. |
/// | `truncate` | write `len` → zero removed slots | Crash after `len` write but before zero: stale bytes may remain in now out-of-range slots, but reads never include them because they are beyond `len`. |
/// | `resize` (grow) | `reserve` → write elements → write `len` | Elements between the old and new `len` may be partially written. |
/// | `clear` | (delegates to `truncate(0)`) | See `truncate`. |
/// | `reserve` | `realloc` → write `cap` | Crash between the two: cap field may reflect the old value; the block is larger than cap indicates. Harmless — the next `push` re-checks and may realloc again unnecessarily. |
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
        let len = u64::from_le_bytes(hdr[..8].try_into().unwrap());
        let cap = u64::from_le_bytes(hdr[8..].try_into().unwrap());
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

    /// Reallocate the block to hold `new_cap` bytes, updating `self.slice`.
    ///
    /// Uses `mem::replace` with a placeholder (same coords) to transfer
    /// ownership to `realloc`.  If realloc fails, the placeholder still
    /// points at the original region (Drop is a no-op), so `self` remains
    /// in a valid state.
    fn grow_to(&mut self, new_cap: u64) -> io::Result<()> {
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
            Err(e) => Err(e),
        }
    }
}

// ── public API ────────────────────────────────────────────────────────────────

impl<'a, A: BStackOwnedSliceAllocator> BStackByteVec<'a, A> {
    /// Create an empty `BStackByteVec` with zero capacity.
    ///
    /// Allocates a 16-byte block for the header only.  The first
    /// [`push`](Self::push) will trigger a reallocation to 4 bytes.
    pub fn new(alloc: &'a A) -> io::Result<Self> {
        let slice = alloc.alloc(HEADER_LEN)?;
        // Header is zero-initialised by the allocator: len=0, cap=0.
        Ok(Self { slice })
    }

    /// Create an empty `BStackByteVec` pre-sized for at least `capacity` bytes.
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
    pub unsafe fn from_raw_block(slice: BStackOwnedSlice<'a, A>) -> Self {
        Self { slice }
    }

    /// Return the number of bytes currently stored.
    ///
    /// Re-reads `len` from the block header on every call.
    pub fn len(&self) -> io::Result<u64> {
        Ok(self.read_header()?.0)
    }

    /// Return the number of bytes the current allocation can hold without
    /// reallocation.
    ///
    /// Re-reads `cap` from the block header on every call.
    pub fn capacity(&self) -> io::Result<u64> {
        Ok(self.read_header()?.1)
    }

    /// Return `true` if the vec contains no bytes.
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
            self.grow_to(new_cap)?;
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
        self.grow_to(new_cap)?;
        self.write_cap_field(new_cap)?;
        Ok(())
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
    pub unsafe fn raw_block(&self) -> BStackSlice<'a> {
        let alloc: &'a A = self.slice.allocator();
        unsafe { BStackSlice::from_raw_range(alloc.stack(), self.slice.range().into()) }
    }

    /// Consume the vec and return the underlying block as an owned handle.
    ///
    /// The caller takes responsibility for the allocation.  Reconstruct with
    /// [`BStackByteVec::from_raw_block`].
    pub fn into_raw_block(self) -> BStackOwnedSlice<'a, A> {
        self.slice
    }

    /// Deallocate the underlying block and consume the vec.
    ///
    /// Preferred over `alloc.dealloc(v.into_raw_block())` as it keeps the
    /// dealloc call co-located with the type.
    pub fn dealloc(self) -> io::Result<()> {
        let alloc: &'a A = self.slice.allocator();
        alloc.dealloc(self.slice)
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

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.len {
            return None;
        }
        let result = self.vec.read_byte_at(self.index);
        self.index += 1;
        Some(result)
    }

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
}
