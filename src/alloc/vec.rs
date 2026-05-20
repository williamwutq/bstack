//! Typed, growable vector backed by a [`BStack`] allocation.
//!
//! Requires features `alloc` and `set`.

use super::{BStackSlice, BStackSliceAllocator};
use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::mem::size_of;

/// Byte offset of the first element within the block (past the 16-byte header).
const HEADER_LEN: u64 = 16;

/// A typed, growable vector backed by a [`crate::BStack`] allocation.
///
/// `BStackVec<'a, T, A>` mirrors the core API of [`Vec<T>`] but stores its
/// elements inside a [`crate::BStack`] allocation managed by allocator `A`.  Every
/// mutation issues a durable sync through the allocator so the contents survive
/// a process crash.
///
/// ## Memory layout
///
/// ```text
/// ┌──────────────────────┬──────────────────────┬────────────────────────────┐
/// │   len  (8 B, LE u64) │   cap  (8 B, LE u64) │   elements: [T; cap]       │
/// └──────────────────────┴──────────────────────┴────────────────────────────┘
///   byte 0                 byte 8                  byte 16
/// ```
///
/// Both `len` and `cap` are re-read from the block header on every call, so the
/// metadata is recoverable after a crash even if the `BStackVec` handle is
/// reconstructed from the raw block via [`BStackVec::from_raw_block`].
///
/// ## Element type
///
/// `T` must be `Copy`.  Elements are written as raw bytes and read back with
/// [`std::ptr::read_unaligned`].  All slots `0..len` always hold byte patterns that
/// were explicitly written by a `BStackVec` method, so reads are never issued
/// against uninitialised memory.
///
/// ## Growth strategy
///
/// When [`push`](BStackVec::push) would exceed the current capacity, the block
/// is reallocated to `max(cap * 2, 4)` elements.  New element space is
/// zero-initialised by [`crate::BStack::extend`].
///
/// ## Zeroing
///
/// [`pop`](BStackVec::pop) zeros the vacated element slot before decrementing
/// `len`.  [`truncate`](BStackVec::truncate) zeros all removed slots in a single
/// [`BStackSlice::zero_range`] call.  Deallocation zeroing is delegated to the
/// allocator.
///
/// ## Thread safety
///
/// `BStackVec` is `Send` when `T: Send` and `A: Sync`, and `Sync` when
/// `T: Sync` and `A: Sync` (both conditions hold for all allocators in this
/// library).  The underlying [`crate::BStack`] serialises concurrent writers
/// through an internal `RwLock`, so multiple threads may call `&self` methods
/// concurrently.  Methods that take `&mut self` (`push`, `pop`, `truncate`,
/// `clear`, `reserve`, `resize`) require exclusive access and may not be called
/// from multiple threads simultaneously.
///
/// ## Crash consistency and atomicity
///
/// Every individual [`crate::BStack`] call (`set`, `zero`, `extend`, `discard`)
/// is durably synced before returning and is crash-safe in isolation.  However,
/// all multi-step `BStackVec` methods issue **two or more** such calls in
/// sequence and are **not atomic** with respect to process crashes.
///
/// The crash-recovery state for each mutating method is:
///
/// | Method | Step order | Crash-recovery state |
/// |--------|-----------|----------------------|
/// | `push` (no realloc) | write element → increment `len` | Crash after element write: element on disk but `len` not updated; slot is effectively invisible. Re-running `push` with the same value recovers correctly. |
/// | `push` (with realloc) | `realloc` → write `cap` → write element → increment `len` | Crash at any point: header re-read on next open reflects the committed state; worst case is a leaked block (with `LinearBStackAllocator`) or a stale cap value. |
/// | `pop` | read element → zero slot → decrement `len` | Crash after zero but before `len` decrement: element is zeroed on disk but `len` still counts it; `get(len-1)` returns a zero-bit `T`. Decrementing `len` on the next `pop` call removes it cleanly. |
/// | `truncate` | zero removed slots → write `len` | Crash after zeroing but before `len` write: logically removed elements are on disk as zeros; `len` still points past them. Calling `truncate` again to the same target is idempotent. |
/// | `resize` (grow) | `reserve` → write elements → write `len` | Same as `push` repeated; elements between the old and new `len` may be partially written. |
/// | `clear` | (delegates to `truncate(0)`) | See `truncate`. |
/// | `reserve` | `realloc` → write `cap` | Crash between the two: cap field may reflect the old value; the block is larger than cap indicates. Harmless — the next `push` re-checks and may realloc again unnecessarily. |
///
/// In all cases the header is re-read from disk on the next call, so the
/// on-disk `(len, cap)` always reflects the last fully committed step.  The
/// [`from_raw_block`](BStackVec::from_raw_block) constructor can be used to
/// reconstruct the handle after a reopen without any additional recovery logic.
///
/// ## Feature flags
///
/// Requires both the `alloc` and `set` Cargo features.
pub struct BStackVec<'a, T: Copy, A: BStackSliceAllocator> {
    /// The full block: header (16 B) followed by element data.
    slice: BStackSlice<'a, A>,
    _phantom: PhantomData<T>,
}

impl<'a, T: Copy, A: BStackSliceAllocator> fmt::Debug for BStackVec<'a, T, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.read_header() {
            Ok((len, cap)) => f
                .debug_struct("BStackVec")
                .field("len", &len)
                .field("capacity", &cap)
                .finish_non_exhaustive(),
            Err(e) => write!(f, "BStackVec(error reading header: {e})"),
        }
    }
}

// ── private helpers ──────────────────────────────────────────────────────────

impl<'a, T: Copy, A: BStackSliceAllocator> BStackVec<'a, T, A> {
    fn elem_size() -> u64 {
        size_of::<T>() as u64
    }

    /// Compute the total block size in bytes for `capacity` elements.
    ///
    /// Returns `Err(InvalidInput)` if the arithmetic overflows `u64` — this
    /// prevents passing a wrapped-around value to the allocator and accidentally
    /// obtaining a block that is too small to hold the requested elements.
    fn block_size(capacity: u64) -> io::Result<u64> {
        capacity
            .checked_mul(Self::elem_size())
            .and_then(|elem_bytes| elem_bytes.checked_add(HEADER_LEN))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "BStackVec: block size overflows u64",
                )
            })
    }

    fn elem_offset(index: u64) -> u64 {
        // `index` is always < `len` which was read from a header we wrote, so
        // this multiplication cannot overflow in well-formed data.  A corrupt
        // on-disk `len` could cause overflow and a debug-mode panic; that is
        // acceptable — corruption is not a recoverable condition here.
        HEADER_LEN + index * Self::elem_size()
    }

    /// Re-read `(len, capacity)` from the block header on disk.
    fn read_header(&self) -> io::Result<(u64, u64)> {
        let mut hdr = [0u8; 16];
        self.slice.read_range_into(0, &mut hdr)?;
        // `read_range(0, 16)` returns exactly 16 bytes on success; the slices
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

    fn read_elem_at(&self, index: u64) -> io::Result<T> {
        let size = size_of::<T>();
        let start = Self::elem_offset(index);
        // This has to be allocating since we need to copy to memory from disk
        let bytes = self.slice.read_range(start, start + size as u64)?;
        // SAFETY: `bytes` has exactly `size_of::<T>()` bytes.  Every slot
        // `0..len` was written with a valid `T` value before this read.
        Ok(unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const T) })
    }

    fn write_elem_at(&self, index: u64, value: T) -> io::Result<()> {
        let size = size_of::<T>();
        let start = Self::elem_offset(index);
        // SAFETY: `value` is a valid, initialized `T`; we borrow its bytes.
        let bytes = unsafe { std::slice::from_raw_parts(&value as *const T as *const u8, size) };
        self.slice.write_range(start, bytes)
    }

    fn write_elems_at(&self, start_index: u64, values: &[T]) -> io::Result<()> {
        let _size = size_of::<T>();
        let start = Self::elem_offset(start_index);
        // SAFETY: `values` is a valid slice of initialized `T`s; we borrow its bytes.
        let bytes = unsafe {
            std::slice::from_raw_parts(values.as_ptr() as *const u8, std::mem::size_of_val(values))
        };
        self.slice.write_range(start, bytes)
    }

    fn zero_elem_at(&self, index: u64) -> io::Result<()> {
        self.slice
            .zero_range(Self::elem_offset(index), Self::elem_size())
    }

    /// Reallocate the block to hold `new_cap` elements, updating `self.slice`.
    fn grow_to(&mut self, new_cap: u64) -> io::Result<()> {
        let new_size = Self::block_size(new_cap)?;
        // SAFETY: Slice origin requirement is upheld because `self.slice` is the original allocation handle
        // returned by the constructor, and `realloc` returns a new slice for the same block.
        let new_slice = self.slice.allocator().realloc(self.slice, new_size)?;
        self.slice = new_slice;
        Ok(())
    }
}

// ── public API ────────────────────────────────────────────────────────────────

impl<'a, T: Copy, A: BStackSliceAllocator> BStackVec<'a, T, A> {
    /// Create an empty `BStackVec` with zero capacity.
    ///
    /// Allocates a 16-byte block for the header only.  The first
    /// [`push`](Self::push) will trigger a reallocation to 4 elements.
    pub fn new(alloc: &'a A) -> io::Result<Self> {
        let slice = alloc.alloc(HEADER_LEN)?;
        // Header is zero-initialised by the allocator: len=0, cap=0.
        Ok(Self {
            slice,
            _phantom: PhantomData,
        })
    }

    /// Create an empty `BStackVec` pre-sized for at least `capacity` elements.
    pub fn with_capacity(capacity: u64, alloc: &'a A) -> io::Result<Self> {
        let slice = alloc.alloc(Self::block_size(capacity)?)?;
        let vec = Self {
            slice,
            _phantom: PhantomData,
        };
        // len is already 0 (zeroed by alloc); write the non-zero cap field.
        vec.write_cap_field(capacity)?;
        Ok(vec)
    }

    /// Allocate a `BStackVec` and populate it from a Rust slice.
    ///
    /// The resulting vec has `len == capacity == data.len()`.
    pub fn from_slice(data: &[T], alloc: &'a A) -> io::Result<Self> {
        let len = data.len() as u64;
        let slice = alloc.alloc(Self::block_size(len)?)?;
        let vec = Self {
            slice,
            _phantom: PhantomData,
        };
        if len > 0 {
            // Write len and cap; elements are written individually below.
            vec.write_header(len, len)?;
            // writes all elements in one call; see write_elems_at() for safety comments
            vec.write_elems_at(0, data)?;
        }
        Ok(vec)
    }

    /// Reconstruct a `BStackVec` from a raw block slice.
    ///
    /// # Safety
    ///
    /// `slice` must be the original allocation handle (not a sub-slice) returned
    /// by one of the `BStackVec` constructors on the same allocator, and the
    /// block header must have been written by a `BStackVec<T, A>`.  Passing an
    /// unrelated slice or one with a mismatched element type is undefined
    /// behaviour.
    pub unsafe fn from_raw_block(slice: BStackSlice<'a, A>) -> Self {
        Self {
            slice,
            _phantom: PhantomData,
        }
    }

    /// Return the number of elements currently stored.
    ///
    /// Re-reads `len` from the block header on every call.
    pub fn len(&self) -> io::Result<u64> {
        Ok(self.read_header()?.0)
    }

    /// Return the number of elements the current allocation can hold without
    /// reallocation.
    ///
    /// Re-reads `cap` from the block header on every call.
    pub fn capacity(&self) -> io::Result<u64> {
        Ok(self.read_header()?.1)
    }

    /// Return `true` if the vec contains no elements.
    pub fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Return the element at `index`, or `None` if `index >= len`.
    pub fn get(&self, index: u64) -> io::Result<Option<T>> {
        let (len, _) = self.read_header()?;
        if index >= len {
            return Ok(None);
        }
        Ok(Some(self.read_elem_at(index)?))
    }

    /// Return a [`BStackSlice`] spanning only the populated element bytes.
    ///
    /// The slice covers `[16, 16 + len * size_of::<T>())` within the block.
    /// It is a sub-slice and must **not** be passed to `realloc` or `dealloc`;
    /// use [`raw_block`](Self::raw_block) for that.
    ///
    /// # Panics
    ///
    /// Panics if the `len` read from the block header is corrupt (larger than
    /// the block can hold), causing the computed end offset to exceed the
    /// block's length.  Corruption is not a recoverable condition here.
    pub fn as_slice(&self) -> io::Result<BStackSlice<'a, A>> {
        let (len, _) = self.read_header()?;
        Ok(self
            .slice
            .subslice(HEADER_LEN, HEADER_LEN + len * Self::elem_size()))
    }

    /// Append `value` to the end of the vec.
    ///
    /// If `len == capacity`, reallocates to `max(cap * 2, 4)` elements before
    /// writing.
    pub fn push(&mut self, value: T) -> io::Result<()> {
        let (len, cap) = self.read_header()?;
        if len == cap {
            let new_cap = cap.saturating_mul(2).max(4);
            self.grow_to(new_cap)?;
            self.write_cap_field(new_cap)?;
        }
        self.write_elem_at(len, value)?;
        self.write_len_field(len + 1)
    }

    /// Remove and return the last element, or `None` if empty.
    ///
    /// The vacated slot is zeroed before `len` is decremented.
    pub fn pop(&mut self) -> io::Result<Option<T>> {
        let (len, _) = self.read_header()?;
        if len == 0 {
            return Ok(None);
        }
        let value = self.read_elem_at(len - 1)?;
        self.zero_elem_at(len - 1)?;
        self.write_len_field(len - 1)?;
        Ok(Some(value))
    }

    /// Shorten the vec to `new_len` elements.
    ///
    /// No-op when `new_len >= len`.  Removed slots are zeroed in a single
    /// [`BStackSlice::zero_range`] call; capacity is unchanged.
    pub fn truncate(&mut self, new_len: u64) -> io::Result<()> {
        let (len, _) = self.read_header()?;
        if new_len >= len {
            return Ok(());
        }
        let start = Self::elem_offset(new_len);
        let removed_bytes = (len - new_len) * Self::elem_size();
        self.slice.zero_range(start, removed_bytes)?;
        self.write_len_field(new_len)
    }

    /// Remove all elements without releasing the allocation.
    ///
    /// Equivalent to `truncate(0)`.
    pub fn clear(&mut self) -> io::Result<()> {
        self.truncate(0)
    }

    /// Reserve capacity for at least `additional` more elements.
    ///
    /// After this call `capacity() >= len() + additional`.  Does nothing if
    /// the current capacity is already sufficient.
    pub fn reserve(&mut self, additional: u64) -> io::Result<()> {
        let (len, cap) = self.read_header()?;
        let needed = len.checked_add(additional).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "BStackVec::reserve: capacity overflow",
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
    pub fn resize(&mut self, new_len: u64, value: T) -> io::Result<()> {
        let (len, _) = self.read_header()?;
        if new_len <= len {
            return self.truncate(new_len);
        }
        self.reserve(new_len - len)?;
        self.write_elems_at(
            len,
            std::iter::repeat_n(value, (new_len - len) as usize)
                .collect::<Vec<_>>()
                .as_slice(),
        )?;
        self.write_len_field(new_len)
    }

    /// Return an iterator over the elements.
    ///
    /// `len` is snapshotted at construction time.  The vec is borrowed
    /// immutably for the iterator's lifetime, preventing concurrent mutation.
    /// Each element is read from disk on demand; errors surface as
    /// `io::Result::Err` items.
    pub fn iter(&self) -> io::Result<BStackVecIter<'_, 'a, T, A>> {
        let (len, _) = self.read_header()?;
        Ok(BStackVecIter {
            vec: self,
            index: 0,
            len,
        })
    }

    /// Return the underlying block slice (header + all allocated element space).
    ///
    /// This is the original allocation handle and may be passed to
    /// [`crate::BStackAllocator::realloc`] or [`crate::BStackAllocator::dealloc`].
    pub fn raw_block(&self) -> BStackSlice<'a, A> {
        self.slice
    }

    /// Consume the vec and return the underlying block slice.
    ///
    /// The caller takes responsibility for the allocation.  Reconstruct with
    /// [`BStackVec::from_raw_block`].
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
    pub fn dealloc(self) -> io::Result<()> {
        self.slice.allocator().dealloc(self.slice)
    }
}

// ── iterator ──────────────────────────────────────────────────────────────────

/// An iterator over the elements of a [`BStackVec`].
///
/// Constructed by [`BStackVec::iter`].  `len` is snapshotted at construction;
/// elements pushed after construction are not visible.  Each element is read
/// from disk on demand; I/O errors surface as `Err` items.
pub struct BStackVecIter<'b, 'a: 'b, T: Copy, A: BStackSliceAllocator> {
    vec: &'b BStackVec<'a, T, A>,
    index: u64,
    len: u64,
}

impl<'b, 'a: 'b, T: Copy, A: BStackSliceAllocator> fmt::Debug for BStackVecIter<'b, 'a, T, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BStackVecIter")
            .field("index", &self.index)
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

impl<'b, 'a: 'b, T: Copy, A: BStackSliceAllocator> Iterator for BStackVecIter<'b, 'a, T, A> {
    type Item = io::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.len {
            return None;
        }
        let result = self.vec.read_elem_at(self.index);
        self.index += 1;
        Some(result)
    }

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
        std::env::temp_dir().join(format!("bstack_vec_test_{pid}_{id}.bin"))
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
        let v: BStackVec<u64, _> = BStackVec::new(&alloc).unwrap();
        assert_eq!(v.len().unwrap(), 0);
        assert_eq!(v.capacity().unwrap(), 0);
        assert!(v.is_empty().unwrap());
    }

    #[test]
    fn with_capacity_has_zero_len_and_correct_cap() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let v: BStackVec<u32, _> = BStackVec::with_capacity(8, &alloc).unwrap();
        assert_eq!(v.len().unwrap(), 0);
        assert_eq!(v.capacity().unwrap(), 8);
        assert!(v.is_empty().unwrap());
    }

    #[test]
    fn from_slice_roundtrip() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let src = [10u64, 20, 30, 40, 50];
        let v = BStackVec::from_slice(&src, &alloc).unwrap();
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
        let v: BStackVec<u8, _> = BStackVec::from_slice(&[], &alloc).unwrap();
        assert_eq!(v.len().unwrap(), 0);
        assert_eq!(v.capacity().unwrap(), 0);
    }

    #[test]
    fn raw_block_roundtrip() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);

        let mut v: BStackVec<i32, _> = BStackVec::new(&alloc).unwrap();
        v.push(1).unwrap();
        v.push(2).unwrap();
        v.push(3).unwrap();

        let block = v.into_raw_block();
        // Reconstruct from raw block and verify header and elements survive.
        let v2: BStackVec<i32, _> = unsafe { BStackVec::from_raw_block(block) };
        assert_eq!(v2.len().unwrap(), 3);
        assert_eq!(v2.get(0).unwrap(), Some(1));
        assert_eq!(v2.get(1).unwrap(), Some(2));
        assert_eq!(v2.get(2).unwrap(), Some(3));
    }

    #[test]
    fn reopen_header_recovery() {
        // Verify that (len, cap) survive a drop-and-reopen via from_raw_block.
        let path = temp_path();
        let _g = Guard(path.clone());

        let block_bytes = {
            let alloc = LinearBStackAllocator::new(BStack::open(&path).unwrap());
            let mut v: BStackVec<u64, _> = BStackVec::new(&alloc).unwrap();
            v.push(111).unwrap();
            v.push(222).unwrap();
            v.push(333).unwrap();
            // Serialise the raw block handle for later reconstruction.
            let bytes: [u8; 16] = v.into_raw_block().into();
            bytes
            // `alloc` (and the BStack file) are closed here.
        };

        // Reopen and reconstruct.
        let alloc = LinearBStackAllocator::new(BStack::open(&path).unwrap());
        let block = unsafe { crate::alloc::BStackSlice::from_bytes(&alloc, block_bytes) };
        let v: BStackVec<u64, _> = unsafe { BStackVec::from_raw_block(block) };
        assert_eq!(v.len().unwrap(), 3);
        assert_eq!(v.get(0).unwrap(), Some(111));
        assert_eq!(v.get(1).unwrap(), Some(222));
        assert_eq!(v.get(2).unwrap(), Some(333));
    }

    // ── push / pop / get ─────────────────────────────────────────────────────

    #[test]
    fn push_pop_lifo() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v: BStackVec<u64, _> = BStackVec::new(&alloc).unwrap();
        for i in 0..10u64 {
            v.push(i * 11).unwrap();
        }
        assert_eq!(v.len().unwrap(), 10);
        for i in (0..10u64).rev() {
            assert_eq!(v.pop().unwrap(), Some(i * 11));
        }
        assert_eq!(v.pop().unwrap(), None);
        assert!(v.is_empty().unwrap());
    }

    #[test]
    fn pop_zeros_vacated_slot() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v: BStackVec<u64, _> = BStackVec::new(&alloc).unwrap();
        v.push(0xDEAD_BEEF_CAFE_BABEu64).unwrap();

        let block = v.raw_block();
        v.pop().unwrap();

        // Slot 0's bytes (at block offset 16) should now be zeroed.
        let slot_bytes = block.read_range(16, 24).unwrap();
        assert_eq!(
            slot_bytes, [0u8; 8],
            "vacated slot must be zeroed after pop"
        );
    }

    #[test]
    fn get_out_of_bounds_returns_none() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v: BStackVec<u32, _> = BStackVec::new(&alloc).unwrap();
        v.push(42).unwrap();
        assert_eq!(v.get(0).unwrap(), Some(42u32));
        assert_eq!(v.get(1).unwrap(), None);
        assert_eq!(v.get(u64::MAX).unwrap(), None);
    }

    // ── growth ───────────────────────────────────────────────────────────────

    #[test]
    fn push_triggers_growth_from_zero() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v: BStackVec<u32, _> = BStackVec::new(&alloc).unwrap();
        // First push must grow from cap=0 to cap=4.
        v.push(1).unwrap();
        assert!(v.capacity().unwrap() >= 4);
        assert_eq!(v.len().unwrap(), 1);
    }

    #[test]
    fn push_doubles_capacity_on_overflow() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v: BStackVec<u32, _> = BStackVec::with_capacity(2, &alloc).unwrap();
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
        let mut v: BStackVec<u64, _> = BStackVec::new(&alloc).unwrap();
        v.push(0xAAAA_AAAA_AAAA_AAAAu64).unwrap();
        v.push(0xBBBB_BBBB_BBBB_BBBBu64).unwrap();
        v.push(0xCCCC_CCCC_CCCC_CCCCu64).unwrap();

        let block = v.raw_block();
        v.truncate(1).unwrap();

        assert_eq!(v.len().unwrap(), 1);
        assert_eq!(v.capacity().unwrap(), 4); // cap unchanged

        // Slots 1 and 2 (offsets 24..40) must be zeroed.
        let removed = block.read_range(24, 40).unwrap();
        assert_eq!(removed, [0u8; 16], "truncated slots must be zeroed");
    }

    #[test]
    fn truncate_noop_when_new_len_ge_len() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v: BStackVec<u32, _> = BStackVec::new(&alloc).unwrap();
        v.push(7).unwrap();
        v.truncate(5).unwrap(); // no-op
        assert_eq!(v.len().unwrap(), 1);
        assert_eq!(v.get(0).unwrap(), Some(7u32));
    }

    #[test]
    fn clear_zeros_all_element_slots() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v: BStackVec<u64, _> = BStackVec::new(&alloc).unwrap();
        v.push(1).unwrap();
        v.push(2).unwrap();
        v.push(3).unwrap();

        let block = v.raw_block();
        v.clear().unwrap();

        assert_eq!(v.len().unwrap(), 0);
        // All three element slots must be zeroed (offsets 16..40).
        let elems = block.read_range(16, 16 + 3 * 8).unwrap();
        assert_eq!(elems, vec![0u8; 24]);
    }

    // ── reserve / resize overflow ─────────────────────────────────────────────

    #[test]
    fn reserve_noop_when_capacity_sufficient() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v: BStackVec<u32, _> = BStackVec::with_capacity(10, &alloc).unwrap();
        v.push(1).unwrap();
        v.reserve(5).unwrap(); // len=1, cap=10 => sufficient
        assert_eq!(v.capacity().unwrap(), 10);
    }

    #[test]
    fn reserve_grows_when_needed() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v: BStackVec<u32, _> = BStackVec::new(&alloc).unwrap();
        v.push(1).unwrap();
        // len=1, cap>=4; reserve(100) must grow to at least 101.
        v.reserve(100).unwrap();
        assert!(v.capacity().unwrap() >= 101);
    }

    #[test]
    fn reserve_overflow_returns_error() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v: BStackVec<u32, _> = BStackVec::new(&alloc).unwrap();
        v.push(1).unwrap(); // len=1
        // Requesting u64::MAX additional would overflow len+additional.
        let err = v.reserve(u64::MAX).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn resize_grow_fills_with_value() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v: BStackVec<u32, _> = BStackVec::new(&alloc).unwrap();
        v.push(1).unwrap();
        v.resize(5, 99u32).unwrap();
        assert_eq!(v.len().unwrap(), 5);
        assert_eq!(v.get(0).unwrap(), Some(1u32));
        for i in 1..5 {
            assert_eq!(v.get(i).unwrap(), Some(99u32));
        }
    }

    #[test]
    fn resize_shrink_truncates() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v: BStackVec<u32, _> = BStackVec::from_slice(&[1, 2, 3, 4, 5], &alloc).unwrap();
        v.resize(2, 0).unwrap();
        assert_eq!(v.len().unwrap(), 2);
        assert_eq!(v.get(0).unwrap(), Some(1u32));
        assert_eq!(v.get(1).unwrap(), Some(2u32));
        assert_eq!(v.get(2).unwrap(), None);
    }

    // ── as_slice ──────────────────────────────────────────────────────────────

    #[test]
    fn as_slice_covers_populated_elements_only() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let v = BStackVec::from_slice(&[10u32, 20, 30], &alloc).unwrap();
        let s = v.as_slice().unwrap();
        assert_eq!(s.len(), 3 * size_of::<u32>() as u64);
        let bytes = s.read().unwrap();
        let mut expected = [0u8; 12];
        expected[0..4].copy_from_slice(&10u32.to_le_bytes());
        expected[4..8].copy_from_slice(&20u32.to_le_bytes());
        expected[8..12].copy_from_slice(&30u32.to_le_bytes());
        assert_eq!(bytes, expected);
    }

    // ── iterator snapshot semantics ───────────────────────────────────────────

    #[test]
    fn iter_yields_all_elements_in_order() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let src = [3u64, 1, 4, 1, 5, 9, 2, 6];
        let v = BStackVec::from_slice(&src, &alloc).unwrap();
        let collected: Vec<u64> = v.iter().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(collected, src);
    }

    #[test]
    fn iter_size_hint_tracks_remaining() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let v = BStackVec::from_slice(&[1u32, 2, 3, 4, 5], &alloc).unwrap();
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
        // Build a vec, take an iter (which snapshots len), then verify the iter
        // sees exactly that many elements.  The borrow checker prevents mutation
        // while the iter is live, so we verify the element count indirectly.
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let v = BStackVec::from_slice(&[10u64, 20, 30], &alloc).unwrap();
        let count = v.iter().unwrap().count();
        assert_eq!(count, 3);
    }

    #[test]
    fn iter_on_empty_vec_yields_nothing() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let v: BStackVec<u64, _> = BStackVec::new(&alloc).unwrap();
        let count = v.iter().unwrap().count();
        assert_eq!(count, 0);
    }

    // ── zero-sized T ─────────────────────────────────────────────────────────

    #[test]
    fn zst_push_pop_tracks_len() {
        // `size_of::<()>() == 0`; the block never grows beyond the 16-byte header.
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v: BStackVec<(), _> = BStackVec::new(&alloc).unwrap();
        assert_eq!(v.len().unwrap(), 0);
        v.push(()).unwrap();
        v.push(()).unwrap();
        v.push(()).unwrap();
        assert_eq!(v.len().unwrap(), 3);
        assert_eq!(v.get(2).unwrap(), Some(()));
        assert_eq!(v.pop().unwrap(), Some(()));
        assert_eq!(v.len().unwrap(), 2);
    }

    #[test]
    fn zst_iter_yields_correct_count() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let v = BStackVec::from_slice(&[(), (), (), ()], &alloc).unwrap();
        let count = v.iter().unwrap().count();
        assert_eq!(count, 4);
    }

    #[test]
    fn zst_block_size_is_always_header_only() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v: BStackVec<(), _> = BStackVec::new(&alloc).unwrap();
        // Push many ZSTs; the raw block should stay at 16 bytes.
        for _ in 0..100 {
            v.push(()).unwrap();
        }
        assert_eq!(v.raw_block().len(), 16);
    }

    // ── integration: block_size overflow ─────────────────────────────────────

    #[test]
    fn with_capacity_overflow_returns_error() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        // u64::MAX elements of u64 = u64::MAX * 8, which overflows u64.
        let err = BStackVec::<u64, _>::with_capacity(u64::MAX, &alloc).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn with_capacity_overflow_zst_is_fine() {
        // ZST elem_size = 0 so u64::MAX * 0 = 0; no overflow.
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let v = BStackVec::<(), _>::with_capacity(u64::MAX, &alloc).unwrap();
        assert_eq!(v.capacity().unwrap(), u64::MAX);
        assert_eq!(v.raw_block().len(), 16);
    }

    #[test]
    fn reserve_overflow_through_grow_returns_error() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        // Start with a vec near the edge of what block_size can represent.
        // After one push (cap grows to 4), request enough additional that
        // len + additional overflows u64.
        let mut v: BStackVec<u64, _> = BStackVec::new(&alloc).unwrap();
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
        let v = BStackVec::from_slice(&[0x0A0Bu16, 0x0C0D, 0x0E0F], &alloc).unwrap();

        let s = v.as_slice().unwrap();
        let mut reader = s.reader();
        let mut buf = [0u8; 6];
        reader.read_exact(&mut buf).unwrap();

        // Elements are stored in the native repr of u16 (little-endian on all
        // supported platforms), laid out consecutively starting at byte 0 of
        // the element region.
        let expected: Vec<u8> = [0x0A0Bu16, 0x0C0D, 0x0E0F]
            .iter()
            .flat_map(|x| x.to_ne_bytes())
            .collect();
        assert_eq!(&buf, expected.as_slice());
    }

    // ── integration: dealloc reclaims tail ───────────────────────────────────

    #[test]
    fn dealloc_reclaims_tail_from_linear_allocator() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);

        let size_before = alloc.stack().len().unwrap();
        let mut v: BStackVec<u64, _> = BStackVec::new(&alloc).unwrap();
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
        let mut a: BStackVec<u32, _> = BStackVec::with_capacity(4, &alloc).unwrap();
        let mut b: BStackVec<u32, _> = BStackVec::with_capacity(4, &alloc).unwrap();

        a.push(10).unwrap();
        b.push(20).unwrap();
        a.push(11).unwrap();
        b.push(21).unwrap();

        assert_eq!(a.len().unwrap(), 2);
        assert_eq!(b.len().unwrap(), 2);
        assert_eq!(a.get(0).unwrap(), Some(10u32));
        assert_eq!(a.get(1).unwrap(), Some(11u32));
        assert_eq!(b.get(0).unwrap(), Some(20u32));
        assert_eq!(b.get(1).unwrap(), Some(21u32));
    }

    // ── integration: as_slice length after mutation ───────────────────────────

    #[test]
    fn as_slice_len_tracks_vec_len() {
        let (alloc, path) = make_alloc();
        let _g = Guard(path);
        let mut v: BStackVec<u64, _> = BStackVec::new(&alloc).unwrap();

        assert_eq!(v.as_slice().unwrap().len(), 0);
        v.push(1).unwrap();
        assert_eq!(v.as_slice().unwrap().len(), size_of::<u64>() as u64);
        v.push(2).unwrap();
        assert_eq!(v.as_slice().unwrap().len(), 2 * size_of::<u64>() as u64);
        v.pop().unwrap();
        assert_eq!(v.as_slice().unwrap().len(), size_of::<u64>() as u64);
    }
}
