use super::{BStackAllocator, BStackBulkAllocator, BStackSlice};
use crate::BStack;
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
/// `dealloc` reclaims the tail allocation via [`BStack::discard`].  For
/// non-tail allocations it is a no-op — the bytes remain on disk but are
/// logically unreachable through this allocator.
///
/// # Crash consistency
///
/// Every operation maps to exactly one [`BStack`] call and is therefore
/// crash-safe by inheritance:
///
/// | Operation            | Underlying call     |
/// |----------------------|---------------------|
/// | `alloc`              | [`BStack::extend`]  |
/// | `realloc` grow       | [`BStack::extend`]  |
/// | `realloc` shrink     | [`BStack::discard`] |
/// | `dealloc` (tail)     | [`BStack::discard`] |
/// | `dealloc` (non-tail) | no-op               |
///
/// # Example
///
/// ```no_run
/// use bstack::{BStack, BStackAllocator, LinearBStackAllocator};
///
/// # fn main() -> std::io::Result<()> {
/// let alloc = LinearBStackAllocator::new(BStack::open("data.bstack")?);
/// let slice = alloc.alloc(128)?;
/// let data = slice.read()?;
/// alloc.dealloc(slice)?;
/// let stack = alloc.into_stack();
/// # Ok(())
/// # }
/// ```
pub struct LinearBStackAllocator {
    stack: BStack,
}

impl LinearBStackAllocator {
    /// Create a new `LinearBStackAllocator` that takes ownership of `stack`.
    pub fn new(stack: BStack) -> Self {
        Self { stack }
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
    fn stack(&self) -> &BStack {
        &self.stack
    }

    fn into_stack(self) -> BStack {
        self.stack
    }

    fn alloc(&self, len: u64) -> io::Result<BStackSlice<'_, Self>> {
        let offset = self.stack.extend(len)?;
        Ok(BStackSlice::new(self, offset, len))
    }

    fn realloc<'a>(
        &'a self,
        slice: BStackSlice<'a, Self>,
        new_len: u64,
    ) -> io::Result<BStackSlice<'a, Self>> {
        let current_tail = self.stack.len()?;
        if slice.end() != current_tail {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "LinearBStackAllocator::realloc: non-tail slice cannot be resized in place",
            ));
        }
        match new_len.cmp(&slice.len()) {
            std::cmp::Ordering::Equal => Ok(slice),
            std::cmp::Ordering::Greater => {
                self.stack.extend(new_len - slice.len())?;
                Ok(BStackSlice::new(self, slice.start(), new_len))
            }
            std::cmp::Ordering::Less => {
                self.stack.discard(slice.len() - new_len)?;
                Ok(BStackSlice::new(self, slice.start(), new_len))
            }
        }
    }

    fn dealloc(&self, slice: BStackSlice<'_, Self>) -> io::Result<()> {
        let current_tail = self.stack.len()?;
        if slice.end() == current_tail {
            self.stack.discard(slice.len())?;
        }
        Ok(())
    }
}

impl BStackBulkAllocator for LinearBStackAllocator {
    /// Allocate all slices with a single [`BStack::extend`] call.
    ///
    /// The total byte count is computed first; if it overflows `u64` the call
    /// returns [`io::ErrorKind::InvalidInput`] without modifying the file.
    /// Otherwise one `extend` (and one durable sync) covers all allocations.
    fn alloc_bulk(&self, lengths: impl AsRef<[u64]>) -> io::Result<Vec<BStackSlice<'_, Self>>> {
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
            result.push(BStackSlice::new(self, offset, len));
            offset += len;
        }
        Ok(result)
    }

    /// Reclaim the largest contiguous region at the tail with a single
    /// [`BStack::discard`] call.
    ///
    /// Slices that form a contiguous sequence ending at the current tail are
    /// all removed in one operation.  Slices that do not touch the tail (or
    /// that are separated from the tail by slices not included in `slices`)
    /// are silently ignored, matching the single-item
    /// [`dealloc`](BStackAllocator::dealloc) semantics.
    fn dealloc_bulk<'a>(&'a self, slices: impl AsRef<[BStackSlice<'a, Self>]>) -> io::Result<()> {
        let slices = slices.as_ref();
        if slices.is_empty() {
            return Ok(());
        }
        let current_tail = self.stack.len()?;
        // Walk from the tail backwards, collecting contiguous covered bytes.
        let mut sorted: Vec<BStackSlice<'_, Self>> = slices.to_vec();
        sorted.sort_by_key(|s| std::cmp::Reverse(s.end()));
        let mut discard_start = current_tail;
        for slice in &sorted {
            if slice.end() == discard_start {
                discard_start = slice.start();
            }
        }
        let to_discard = current_tail - discard_start;
        if to_discard > 0 {
            self.stack.discard(to_discard)?;
        }
        Ok(())
    }
}
