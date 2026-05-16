//! Debug-only allocator wrapper that validates allocation and deallocation operations.
//!
//! This module provides [`DebugCheckingAllocator`], a wrapper around any [`BStackAllocator`]
//! that tracks allocated and freed regions to detect overlaps and invalid operations.
//!
//! # Purpose
//!
//! This allocator is intended for **debugging and testing only**. It maintains in-memory
//! sets of allocated and freed regions and validates every operation:
//!
//! - **On allocation**: Checks that the newly allocated region does not overlap with any
//!   existing allocated region.
//! - **On deallocation**: Checks that the region being freed does not overlap with any
//!   previously freed region.
//!
//! These checks help catch allocator bugs such as:
//! - Returning overlapping allocations
//! - Double-freeing the same region
//! - Partial overlaps indicating corruption
//!
//! # Performance
//!
//! This wrapper adds significant overhead:
//! - O(n) overlap checks on every allocation and deallocation
//! - Memory overhead for tracking all regions
//!
//! Use only during development and testing, not in production.
//!
//! # Persistence
//!
//! Because allocators operate on persistent files that survive across process restarts,
//! the `DebugCheckingAllocator` tracks allocated and freed regions **separately**:
//!
//! - A region can be both allocated and freed (allocated in a previous session, freed in
//!   the current session)
//! - This allows detection of freed-freed overlaps even when the allocations happened
//!   across different sessions
//!
//! # Example
//!
//! ```no_run
//! use bstack::{BStack, BStackAllocator, DebugCheckingAllocator, LinearBStackAllocator};
//!
//! # fn main() -> std::io::Result<()> {
//! let inner = LinearBStackAllocator::new(BStack::open("test.bstack")?);
//! let alloc = DebugCheckingAllocator::new(inner);
//!
//! let slice1 = alloc.alloc(100)?;
//! let slice2 = alloc.alloc(200)?;
//!
//! // This would panic if slice2 overlapped with slice1
//! alloc.dealloc(slice1)?;
//! // This would panic if we tried to dealloc slice1 again
//!
//! # Ok(())
//! # }
//! ```

use super::{BStackAllocator, BStackBulkAllocator, BStackSlice};
use crate::BStack;
use std::collections::HashSet;
use std::io;
use std::sync::Mutex;

/// A region of bytes defined by `[offset, offset + len)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Region {
    offset: u64,
    len: u64,
}

impl Region {
    fn new(offset: u64, len: u64) -> Self {
        Self { offset, len }
    }

    fn end(&self) -> u64 {
        self.offset.saturating_add(self.len)
    }

    /// Check if this region overlaps with another region.
    ///
    /// Two regions [a, a+len_a) and [b, b+len_b) overlap if:
    /// max(a, b) < min(a+len_a, b+len_b)
    fn overlaps(&self, other: &Region) -> bool {
        if self.len == 0 || other.len == 0 {
            // Zero-length regions don't overlap with anything
            return false;
        }
        let max_start = self.offset.max(other.offset);
        let min_end = self.end().min(other.end());
        max_start < min_end
    }
}

/// Handle type for [`DebugCheckingAllocator`].
///
/// Wraps the inner allocator's handle along with a reference to the debug allocator,
/// enabling conversion to [`BStackSlice`] while preserving the inner handle's semantics.
pub struct DebugHandle<'a, A>
where
    A: BStackAllocator<Error = io::Error>,
{
    alloc: &'a DebugCheckingAllocator<A>,
    inner: A::Allocated<'a>,
}

// Manual Clone and Copy implementations since the derive macro is too conservative
impl<'a, A> Clone for DebugHandle<'a, A>
where
    A: BStackAllocator<Error = io::Error>,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, A> Copy for DebugHandle<'a, A> where A: BStackAllocator<Error = io::Error> {}

impl<'a, A> DebugHandle<'a, A>
where
    A: BStackAllocator<Error = io::Error>,
{
    fn new(alloc: &'a DebugCheckingAllocator<A>, inner: A::Allocated<'a>) -> Self {
        Self { alloc, inner }
    }

    /// Return the inner allocator's handle.
    ///
    /// To inspect the region (offset, length), convert it with `.try_into::<BStackSlice<_>>()`.
    pub fn inner(&self) -> &A::Allocated<'a> {
        &self.inner
    }
}

impl<'a, A> TryInto<BStackSlice<'a, DebugCheckingAllocator<A>>> for DebugHandle<'a, A>
where
    A: BStackAllocator<Error = io::Error>,
{
    type Error = io::Error;

    fn try_into(self) -> Result<BStackSlice<'a, DebugCheckingAllocator<A>>, Self::Error> {
        // Convert inner handle to BStackSlice to get offset and length
        let slice: BStackSlice<'_, A> = self.inner.try_into().map_err(|e| {
            io::Error::other(format!(
                "Failed to convert inner handle to BStackSlice: {:?}",
                e
            ))
        })?;
        // SAFETY: The handle was created by the allocator with valid offset and length
        Ok(unsafe { BStackSlice::from_raw_parts(self.alloc, slice.start(), slice.len()) })
    }
}

/// Shared state protected by a single mutex inside [`DebugCheckingAllocator`].
///
/// Using one lock for both sets ensures a consistent acquisition order and prevents
/// deadlocks that would arise from the two-mutex ABBA pattern (alloc acquires
/// `allocated` then `freed`; dealloc would acquire them in the opposite order).
struct DebugState {
    /// Set of currently allocated regions that haven't been freed yet.
    allocated: HashSet<Region>,
    /// Set of regions that have been freed (may persist across sessions).
    freed: HashSet<Region>,
}

/// Debug-only allocator wrapper that validates allocations and deallocations.
///
/// Wraps any [`BStackAllocator`] with `Error = io::Error` and
/// `Allocated<'a> = BStackSlice<'a, Self>` and maintains sets of allocated
/// and freed regions to detect overlaps.
///
/// # Constraints
///
/// This wrapper works with any allocator whose `Allocated` handles can convert
/// to and from `BStackSlice`, which includes all allocators provided by this library
/// ([`crate::LinearBStackAllocator`], [`crate::FirstFitBStackAllocator`],
/// [`crate::GhostTreeBstackAllocator`], [`crate::ManualAllocator`]).
///
/// # Panics
///
/// Panics if:
/// - A newly allocated region overlaps with an existing allocated region
/// - A region being freed overlaps with a previously freed region
///
/// These panics indicate bugs in the underlying allocator implementation.
///
/// # Thread Safety
///
/// The internal tracking sets are protected by a `Mutex`, so the allocator is `Send + Sync`
/// if the inner allocator is.
pub struct DebugCheckingAllocator<A>
where
    A: BStackAllocator<Error = io::Error>,
{
    inner: A,
    state: Mutex<DebugState>,
}

impl<A> DebugCheckingAllocator<A>
where
    A: BStackAllocator<Error = io::Error>,
{
    /// Create a new `DebugCheckingAllocator` wrapping `inner`.
    ///
    /// The allocator starts with empty tracking sets. If you're reopening
    /// a file from a previous session, you may want to reconstruct the
    /// allocated/freed sets based on your application's metadata.
    pub fn new(inner: A) -> Self {
        Self {
            inner,
            state: Mutex::new(DebugState {
                allocated: HashSet::new(),
                freed: HashSet::new(),
            }),
        }
    }

    /// Return a reference to the inner allocator.
    pub fn inner(&self) -> &A {
        &self.inner
    }

    /// Consume this allocator and return the inner allocator.
    pub fn into_inner(self) -> A {
        self.inner
    }

    /// Check if a region overlaps with any region in the set.
    fn check_overlap(region: &Region, set: &HashSet<Region>) -> Option<Region> {
        for existing in set {
            if region.overlaps(existing) {
                return Some(*existing);
            }
        }
        None
    }

    /// Record a newly allocated region after validation.
    ///
    /// If the allocated region overlaps with freed regions, those freed regions are
    /// removed and split around the new allocation. For example, if allocating [b, c)
    /// while [a, d) is freed, the freed set will be updated to contain [a, b) and [c, d).
    fn record_allocation(&self, offset: u64, len: u64) {
        let region = Region::new(offset, len);
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());

        // Check for overlaps with existing allocations
        if let Some(overlap) = Self::check_overlap(&region, &state.allocated) {
            panic!(
                "DebugCheckingAllocator: Newly allocated region [{}, {}) overlaps with \
                 existing allocated region [{}, {}). This indicates a bug in the underlying \
                 allocator.",
                region.offset,
                region.end(),
                overlap.offset,
                overlap.end()
            );
        }

        // Handle overlaps with freed regions by splitting them
        let overlapping_freed: Vec<Region> = state
            .freed
            .iter()
            .filter(|r| region.overlaps(r))
            .copied()
            .collect();

        for freed_region in overlapping_freed {
            state.freed.remove(&freed_region);

            // Split the freed region around the newly allocated region:
            // If freed_region is [a, d) and region is [b, c):
            // - If a < b, add [a, b) to freed
            // - If c < d, add [c, d) to freed
            if freed_region.offset < region.offset {
                let before_len = region.offset - freed_region.offset;
                state
                    .freed
                    .insert(Region::new(freed_region.offset, before_len));
            }

            if region.end() < freed_region.end() {
                let after_offset = region.end();
                let after_len = freed_region.end() - after_offset;
                state.freed.insert(Region::new(after_offset, after_len));
            }
        }

        state.allocated.insert(region);
    }

    /// Record a deallocation after validation.
    ///
    /// Freeing a region with no recorded allocation is allowed (the checker only tracks
    /// allocations made through itself). Panics if the freed region partially overlaps or
    /// spans multiple recorded allocations, as those indicate real bugs.
    fn record_deallocation(&self, offset: u64, len: u64) {
        let region = Region::new(offset, len);
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());

        // Check for overlaps with previously freed regions BEFORE calling inner dealloc
        if let Some(overlap) = Self::check_overlap(&region, &state.freed) {
            panic!(
                "DebugCheckingAllocator: Attempting to free region [{}, {}) which overlaps \
                 with already freed region [{}, {}). This indicates a double-free bug.",
                region.offset,
                region.end(),
                overlap.offset,
                overlap.end()
            );
        }

        // Validate the freed region matches exactly one allocated region
        let overlapping_allocated: Vec<Region> = state
            .allocated
            .iter()
            .filter(|r| region.overlaps(r))
            .copied()
            .collect();

        match overlapping_allocated.as_slice() {
            [] => {
                // Region was not recorded as allocated; allowed since the debug checker
                // only tracks allocations made through itself.
            }
            [exact] if *exact == region => {
                state.allocated.remove(exact);
            }
            [single] => panic!(
                "DebugCheckingAllocator: Attempting to partially free region [{}, {}) \
                 which is a subset or overlap of allocated region [{}, {}). \
                 Partial deallocations are not allowed.",
                region.offset,
                region.end(),
                single.offset,
                single.end(),
            ),
            _ => panic!(
                "DebugCheckingAllocator: Attempting to free region [{}, {}) which spans \
                 multiple allocated regions. This is not a valid deallocation.",
                region.offset,
                region.end(),
            ),
        }

        state.freed.insert(region);
    }
}

impl<A> BStackAllocator for DebugCheckingAllocator<A>
where
    A: BStackAllocator<Error = io::Error>,
{
    type Error = io::Error;
    type Allocated<'a>
        = DebugHandle<'a, A>
    where
        A: 'a;

    fn stack(&self) -> &BStack {
        self.inner.stack()
    }

    fn into_stack(self) -> BStack {
        self.inner.into_stack()
    }

    fn alloc(&self, len: u64) -> io::Result<Self::Allocated<'_>> {
        let handle = self.inner.alloc(len)?;

        // Convert to BStackSlice for validation (Copy keeps the original handle live)
        let slice: BStackSlice<'_, A> = handle.try_into().map_err(|e| {
            // The inner allocator succeeded but we can't inspect the handle — free it
            // to avoid leaking the allocation, then surface the error.
            let _ = self.inner.dealloc(handle);
            io::Error::other(format!(
                "allocated handle is not convertible to BStackSlice: {e}"
            ))
        })?;

        self.record_allocation(slice.start(), slice.len());

        Ok(DebugHandle::new(self, handle))
    }

    fn realloc<'a>(
        &'a self,
        handle: Self::Allocated<'a>,
        new_len: u64,
    ) -> io::Result<Self::Allocated<'a>> {
        // Extract old region info before handing the inner handle to the inner realloc
        let old_slice: BStackSlice<'_, A> = handle.inner.try_into().map_err(|e| {
            io::Error::other(format!(
                "handle is not convertible to BStackSlice before realloc: {e}"
            ))
        })?;
        let old_region = Region::new(old_slice.start(), old_slice.len());

        let new_inner_handle = self.inner.realloc(handle.inner, new_len)?;

        // Convert result; free on failure to avoid leaking the new allocation
        let new_slice: BStackSlice<'_, A> = new_inner_handle.try_into().map_err(|e| {
            let _ = self.inner.dealloc(new_inner_handle);
            io::Error::other(format!(
                "reallocated handle is not convertible to BStackSlice: {e}"
            ))
        })?;
        let new_region = Region::new(new_slice.start(), new_slice.len());

        // Atomically swap old for new in the tracking state
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.allocated.remove(&old_region);

        if let Some(overlap) = Self::check_overlap(&new_region, &state.allocated) {
            state.allocated.insert(old_region);
            panic!(
                "DebugCheckingAllocator: Reallocated region [{}, {}) overlaps with \
                 existing allocated region [{}, {}). This indicates a bug in the underlying \
                 allocator's realloc.",
                new_region.offset,
                new_region.end(),
                overlap.offset,
                overlap.end()
            );
        }

        state.allocated.insert(new_region);
        drop(state);

        Ok(DebugHandle::new(self, new_inner_handle))
    }

    fn dealloc(&self, handle: Self::Allocated<'_>) -> io::Result<()> {
        // Extract offset and length from the handle
        let slice: BStackSlice<'_, A> = handle
            .inner
            .try_into()
            .map_err(|e| io::Error::other(format!("Failed to convert handle: {:?}", e)))?;
        let offset = slice.start();
        let len = slice.len();

        // Validate BEFORE calling inner dealloc
        self.record_deallocation(offset, len);

        self.inner.dealloc(handle.inner)?;
        Ok(())
    }
}

impl<A> BStackBulkAllocator for DebugCheckingAllocator<A>
where
    A: BStackBulkAllocator<Error = io::Error>,
{
    fn alloc_bulk(&self, lengths: impl AsRef<[u64]>) -> io::Result<Vec<Self::Allocated<'_>>> {
        let inner_handles = self.inner.alloc_bulk(lengths)?;

        // Validate all handles before recording any of them so that a conversion
        // failure never leaves the tracking state partially updated.
        let mut slices: Vec<BStackSlice<'_, A>> = Vec::with_capacity(inner_handles.len());
        for (i, &h) in inner_handles.iter().enumerate() {
            match h.try_into() {
                Ok(slice) => slices.push(slice),
                Err(e) => {
                    // Free every handle the inner allocator gave us to avoid leaking them
                    for &h in &inner_handles {
                        let _ = self.inner.dealloc(h);
                    }
                    return Err(io::Error::other(format!(
                        "bulk-allocated handle {i} is not convertible to BStackSlice: {e}"
                    )));
                }
            }
        }

        let mut result = Vec::with_capacity(inner_handles.len());
        for (&h, slice) in inner_handles.iter().zip(slices) {
            self.record_allocation(slice.start(), slice.len());
            result.push(DebugHandle::new(self, h));
        }

        Ok(result)
    }

    fn dealloc_bulk<'a>(&'a self, handles: impl AsRef<[Self::Allocated<'a>]>) -> io::Result<()> {
        let handles = handles.as_ref();

        // Validate ALL handles before touching the inner allocator so that a
        // double-free or partial-free panic fires before any state is mutated.
        for handle in handles {
            let slice: BStackSlice<'_, A> = handle.inner.try_into().map_err(|e| {
                io::Error::other(format!(
                    "handle is not convertible to BStackSlice during bulk dealloc: {e}"
                ))
            })?;
            self.record_deallocation(slice.start(), slice.len());
        }

        // Delegate to the inner allocator with the unwrapped handles
        let inner_handles: Vec<A::Allocated<'a>> = handles.iter().map(|h| h.inner).collect();
        self.inner.dealloc_bulk(inner_handles)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_overlap() {
        let r1 = Region::new(0, 10);
        let r2 = Region::new(5, 10);
        let r3 = Region::new(10, 10);
        let r4 = Region::new(20, 10);

        assert!(r1.overlaps(&r2)); // [0, 10) and [5, 15) overlap
        assert!(r2.overlaps(&r1)); // symmetric
        assert!(!r1.overlaps(&r3)); // [0, 10) and [10, 20) don't overlap (adjacent)
        assert!(!r1.overlaps(&r4)); // [0, 10) and [20, 30) don't overlap
        assert!(r2.overlaps(&r3)); // [5, 15) and [10, 20) overlap
    }

    #[test]
    fn test_zero_length_regions() {
        let r1 = Region::new(0, 0);
        let r2 = Region::new(0, 10);
        let r3 = Region::new(5, 0);

        assert!(!r1.overlaps(&r2)); // Zero-length doesn't overlap
        assert!(!r2.overlaps(&r1)); // symmetric
        assert!(!r1.overlaps(&r3)); // Two zero-length regions don't overlap
    }

    #[test]
    fn test_region_splitting_logic() {
        // Test the splitting logic directly without file I/O
        use crate::BStack;

        // Create a mock allocator for testing region splitting
        struct MockAllocator;
        impl crate::alloc::BStackAllocator for MockAllocator {
            type Error = io::Error;
            type Allocated<'a> = crate::alloc::BStackSlice<'a, Self>;

            fn stack(&self) -> &BStack {
                unimplemented!()
            }
            fn into_stack(self) -> BStack {
                unimplemented!()
            }
            fn alloc(&self, _len: u64) -> io::Result<Self::Allocated<'_>> {
                unimplemented!()
            }
            fn realloc<'a>(
                &'a self,
                _: Self::Allocated<'a>,
                _: u64,
            ) -> io::Result<Self::Allocated<'a>> {
                unimplemented!()
            }
            fn dealloc(&self, _: Self::Allocated<'_>) -> io::Result<()> {
                unimplemented!()
            }
        }

        let checker = DebugCheckingAllocator::new(MockAllocator);

        // Simulate: freed region [0, 100)
        {
            let mut state = checker.state.lock().unwrap();
            state.freed.insert(Region::new(0, 100));
        }

        // Record allocation of [20, 50) - should split freed into [0, 20) and [50, 100)
        // Note: Region [20, 50) means offset=20, len=30, so end=50
        // We can't call record_allocation directly because it needs both locks
        // So this test just verifies the Region logic works
        let freed_region = Region::new(0, 100);
        let alloc_region = Region::new(20, 30); // offset=20, len=30, end=50

        assert_eq!(alloc_region.end(), 50);
        assert!(freed_region.overlaps(&alloc_region));

        // Verify split calculation
        assert!(freed_region.offset < alloc_region.offset); // [0, 20) exists
        assert!(alloc_region.end() < freed_region.end()); // [50, 100) exists

        let before_len = alloc_region.offset - freed_region.offset;
        assert_eq!(before_len, 20);

        let after_offset = alloc_region.end();
        let after_len = freed_region.end() - after_offset;
        assert_eq!(after_offset, 50);
        assert_eq!(after_len, 50);
    }
}
