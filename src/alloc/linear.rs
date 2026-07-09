use super::{BStackAllocator, BStackBulkAllocator, BStackOwnedSlice};
use crate::BStack;
#[cfg(not(feature = "atomic"))]
use std::cell::Cell;
#[cfg(not(feature = "atomic"))]
use std::marker::PhantomData;
use std::{fmt, io};

/// A simple bump allocator that owns a [`BStack`] and allocates regions
/// sequentially by appending to the tail.
///
/// # Realloc policy
///
/// `realloc` is O(1) for the tail allocation.  `realloc` of a non-tail
/// allocation returns [`io::ErrorKind::Unsupported`].
///
/// # Dealloc policy
///
/// `dealloc` reclaims the tail allocation via [`BStack::discard`] (or
/// [`BStack::try_discard`] with the `atomic` feature).  For non-tail
/// allocations it is a no-op — the bytes remain on disk but are logically
/// unreachable through this allocator.
///
/// # Crash consistency
///
/// Every operation maps to exactly one [`BStack`] call and is therefore
/// crash-safe by inheritance:
///
/// | Operation            | Without `atomic`    | With `atomic`           |
/// |----------------------|---------------------|-------------------------|
/// | `alloc`              | [`BStack::extend`]  | [`BStack::extend`]      |
/// | `realloc` grow       | [`BStack::extend`]  | [`BStack::try_extend`]  |
/// | `realloc` shrink     | [`BStack::discard`] | [`BStack::try_discard`] |
/// | `dealloc` (tail)     | [`BStack::discard`] | [`BStack::try_discard`] |
/// | `dealloc` (non-tail) | no-op               | no-op                   |
///
/// # Thread safety
///
/// Without the `atomic` feature, `LinearBStackAllocator` is **`Send`** but
/// **not `Sync`**: `realloc` and `dealloc` read the tail length and then
/// modify it in two separate steps, so concurrent `&self` calls would race.
///
/// With the `atomic` feature, `LinearBStackAllocator` is **`Send + Sync`**.
/// The tail-modifying operations are replaced with their `try_*` counterparts,
/// which check-and-modify the tail in a single locked step:
///
/// * **`alloc`** / **`alloc_bulk`**: a single [`BStack::extend`] returns a
///   distinct region to every caller regardless of concurrency.
/// * **`realloc`**: uses [`BStack::try_extend`] / [`BStack::try_discard`]
///   with `slice.end()` as the sentinel.  If the tail has moved (another
///   thread raced), the call returns [`io::ErrorKind::Unsupported`] — the
///   same error as a non-tail realloc on a single thread.
/// * **`dealloc`** / **`dealloc_bulk`**: uses [`BStack::try_discard`]; if the
///   tail has moved the discard is silently skipped, matching the existing
///   non-tail no-op semantics.
///
/// ```
/// fn assert_send<T: Send>() {}
/// assert_send::<bstack::LinearBStackAllocator>();
/// ```
///
/// # Example
///
/// ```no_run
/// use bstack::{BStack, BStackAllocator, LinearBStackAllocator};
///
/// # fn main() -> std::io::Result<()> {
/// let alloc = LinearBStackAllocator::new(BStack::open("data.bstack")?);
/// let mut slice = alloc.alloc(128)?;
/// let data = slice.read()?;
/// alloc.dealloc(slice)?;
/// let stack = alloc.into_stack();
/// # Ok(())
/// # }
/// ```
pub struct LinearBStackAllocator {
    stack: BStack,
    #[cfg(not(feature = "atomic"))]
    _not_sync: PhantomData<Cell<()>>,
}

impl LinearBStackAllocator {
    /// Create a new `LinearBStackAllocator` that takes ownership of `stack`.
    pub fn new(stack: BStack) -> Self {
        Self {
            stack,
            #[cfg(not(feature = "atomic"))]
            _not_sync: PhantomData,
        }
    }
}

impl fmt::Debug for LinearBStackAllocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LinearBStackAllocator")
            .finish_non_exhaustive()
    }
}

impl From<BStack> for LinearBStackAllocator {
    fn from(stack: BStack) -> Self {
        Self::new(stack)
    }
}

impl From<LinearBStackAllocator> for BStack {
    fn from(alloc: LinearBStackAllocator) -> Self {
        alloc.into_stack()
    }
}

impl BStackAllocator for LinearBStackAllocator {
    type Error = io::Error;
    type Allocated<'a> = BStackOwnedSlice<'a, Self>;

    fn stack(&self) -> &BStack {
        &self.stack
    }

    fn into_stack(self) -> BStack {
        self.stack
    }

    fn alloc(&self, len: u64) -> io::Result<BStackOwnedSlice<'_, Self>> {
        let offset = self.stack.extend(len)?;
        // SAFETY: offset and len come from a fresh allocation via self.stack.extend
        Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, offset, len) })
    }

    #[cfg(not(feature = "atomic"))]
    fn realloc<'a>(
        &'a self,
        handle: BStackOwnedSlice<'a, Self>,
        new_len: u64,
    ) -> io::Result<BStackOwnedSlice<'a, Self>> {
        let start = handle.start();
        let old_len = handle.len();
        let end = handle.end();
        // handle is dropped here (no-op Drop); we reconstruct below on success
        let current_tail = self.stack.len()?;
        if end != current_tail {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "LinearBStackAllocator does not support reallocation of non-tail slices; \
                 callers must alloc a new region and copy manually. \
                 Note: dealloc of the old (non-tail) slice is also a no-op and leaks the region.",
            ));
        }
        match new_len.cmp(&old_len) {
            std::cmp::Ordering::Equal => {
                // SAFETY: same region, reconstructed handle
                Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, start, old_len) })
            }
            std::cmp::Ordering::Greater => {
                self.stack.extend(new_len - old_len)?;
                // SAFETY: tail was extended in place
                Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, start, new_len) })
            }
            std::cmp::Ordering::Less => {
                self.stack.discard(old_len - new_len)?;
                // SAFETY: tail was shrunk in place
                Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, start, new_len) })
            }
        }
    }

    // With the `atomic` feature the tail check and the modification are a
    // single locked step (`try_extend`/`try_discard`), eliminating the TOCTOU
    // race that exists in the non-atomic version.  A `false` return means
    // another thread moved the tail first; we surface this as `Unsupported`.
    #[cfg(feature = "atomic")]
    fn realloc<'a>(
        &'a self,
        handle: BStackOwnedSlice<'a, Self>,
        new_len: u64,
    ) -> io::Result<BStackOwnedSlice<'a, Self>> {
        let start = handle.start();
        let old_len = handle.len();
        let end = handle.end();
        // handle dropped (no-op); reconstruct below on success
        match new_len.cmp(&old_len) {
            std::cmp::Ordering::Equal => {
                Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, start, old_len) })
            }
            std::cmp::Ordering::Greater => {
                let delta = new_len - old_len;
                let zeros = vec![0u8; delta as usize];
                if !self.stack.try_extend(end, zeros)? {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "LinearBStackAllocator does not support reallocation of non-tail slices; \
                         callers must alloc a new region and copy manually. \
                         Note: dealloc of the old (non-tail) slice is also a no-op and leaks the region.",
                    ));
                }
                // SAFETY: tail atomically extended in place
                Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, start, new_len) })
            }
            std::cmp::Ordering::Less => {
                if !self.stack.try_discard(end, old_len - new_len)? {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "LinearBStackAllocator does not support reallocation of non-tail slices; \
                         callers must alloc a new region and copy manually. \
                         Note: dealloc of the old (non-tail) slice is also a no-op and leaks the region.",
                    ));
                }
                // SAFETY: tail atomically shrunk in place
                Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, start, new_len) })
            }
        }
    }

    #[cfg(not(feature = "atomic"))]
    fn dealloc(&self, handle: BStackOwnedSlice<'_, Self>) -> io::Result<()> {
        let end = handle.end();
        let len = handle.len();
        let current_tail = self.stack.len()?;
        if end == current_tail {
            self.stack.discard(len)?;
        }
        Ok(())
    }

    #[cfg(feature = "atomic")]
    fn dealloc(&self, handle: BStackOwnedSlice<'_, Self>) -> io::Result<()> {
        // try_discard is a no-op when the tail has moved, matching non-tail dealloc semantics.
        self.stack.try_discard(handle.end(), handle.len())?;
        Ok(())
    }
}

impl BStackBulkAllocator for LinearBStackAllocator {
    /// Allocate all slices with a single [`BStack::extend`] call.
    ///
    /// The total byte count is computed first; if it overflows `u64` the call
    /// returns [`io::ErrorKind::InvalidInput`] without modifying the file.
    /// Otherwise one `extend` (and one durable sync) covers all allocations.
    ///
    /// # Zero-length entries
    ///
    /// Zero-length requests are permitted and produce zero-length slices. Their
    /// offset is the same as the immediately following non-zero slice because
    /// adding zero to the running offset leaves it unchanged. Multiple
    /// consecutive zero-length entries therefore produce slices that compare
    /// equal by `(offset, len)`. This is harmless for I/O but callers that use
    /// slice identity to distinguish allocations should avoid zero-length entries
    /// or filter them out.
    fn alloc_bulk(
        &self,
        lengths: impl AsRef<[u64]>,
    ) -> Result<Vec<Self::Allocated<'_>>, Self::Error> {
        let lengths = lengths.as_ref();
        if lengths.is_empty() {
            return Ok(Vec::new());
        }
        let total = lengths
            .iter()
            .try_fold(0u64, |acc, &len| acc.checked_add(len))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "alloc_bulk: total length overflows u64",
                )
            })?;
        let base = self.stack.extend(total)?;
        let mut result = Vec::with_capacity(lengths.len());
        let mut offset = base;
        for &len in lengths {
            // SAFETY: offset and len are within the bulk allocation starting at base
            result.push(unsafe { BStackOwnedSlice::from_raw_parts(self, offset, len) });
            offset += len;
        }
        Ok(result)
    }

    /// Reclaim the largest contiguous region at the tail with a single
    /// [`BStack::discard`] call.
    ///
    /// Handles that form a contiguous sequence ending at the current tail are
    /// all removed in one operation.  Handles that do not touch the tail (or
    /// that are separated from the tail by handles not included in `handles`)
    /// are silently ignored, matching the single-item
    /// [`dealloc`](BStackAllocator::dealloc) semantics.
    ///
    /// # Duplicates and overlapping handles
    ///
    /// Duplicate or overlapping handles are silently coalesced — no error is
    /// returned, and only the bytes they collectively cover are discarded once.
    #[cfg(not(feature = "atomic"))]
    fn dealloc_bulk<'a>(
        &'a self,
        handles: impl IntoIterator<Item = Self::Allocated<'a>>,
    ) -> Result<(), Self::Error> {
        let current_tail = self.stack.len()?;
        let mut sorted: Vec<BStackOwnedSlice<'a, Self>> = handles.into_iter().collect();
        if sorted.is_empty() {
            return Ok(());
        }
        sorted.sort_by_key(|s| std::cmp::Reverse(s.end()));
        let mut discard_start = current_tail;
        for handle in &sorted {
            if handle.end() == discard_start {
                discard_start = handle.start();
            }
        }
        let to_discard = current_tail - discard_start;
        if to_discard > 0 {
            self.stack.discard(to_discard)?;
        }
        Ok(())
    }

    // With `atomic`, we snapshot the tail once to determine what to discard,
    // then use `try_discard` to make the actual removal atomic.  If another
    // thread has moved the tail since the snapshot, `try_discard` returns
    // `false` and the discard is silently skipped.
    #[cfg(feature = "atomic")]
    fn dealloc_bulk<'a>(
        &'a self,
        handles: impl IntoIterator<Item = Self::Allocated<'a>>,
    ) -> Result<(), Self::Error> {
        let current_tail = self.stack.len()?;
        let mut sorted: Vec<BStackOwnedSlice<'a, Self>> = handles.into_iter().collect();
        if sorted.is_empty() {
            return Ok(());
        }
        sorted.sort_by_key(|s| std::cmp::Reverse(s.end()));
        let mut discard_start = current_tail;
        for handle in &sorted {
            if handle.end() == discard_start {
                discard_start = handle.start();
            }
        }
        let to_discard = current_tail - discard_start;
        if to_discard > 0 {
            self.stack.try_discard(current_tail, to_discard)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod _assertions {
    use super::LinearBStackAllocator;
    // LinearBStackAllocator is always Send.
    fn _send()
    where
        LinearBStackAllocator: Send,
    {
    }
    // LinearBStackAllocator is Sync only when the `atomic` feature is enabled.
    #[cfg(feature = "atomic")]
    fn _sync()
    where
        LinearBStackAllocator: Sync,
    {
    }
}
