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
//! The tracking state (which regions are allocated or freed) is **in-memory only** and
//! is lost when the process exits. The underlying allocator's data, however, is persistent.
//!
//! Because of this asymmetry, allocated and freed regions are tracked **separately**:
//!
//! - A freed region may have no corresponding entry in the allocated set — it may have
//!   been originally allocated in a prior session that this instance never observed.
//! - This allows double-free detection within a single session even when the original
//!   allocation happened in a prior run.
//!
//! If you need cross-session validation, use [`DebugCheckingAllocator::with_state`]
//! to pre-populate the tracking sets from your application's own metadata.
//!
//! # Example
//!
//! ```no_run
//! use bstack::{BStack, BStackAllocator, DebugCheckingAllocator, LinearBStackAllocator};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let inner = LinearBStackAllocator::new(BStack::open("test.bstack")?);
//! let alloc = DebugCheckingAllocator::new(inner);
//!
//! let slice1 = alloc.alloc(100)?;
//! let slice2 = alloc.alloc(200)?;
//!
//! // This would panic if slice2 overlapped with slice1
//! alloc.dealloc(slice1).map_err(|e| e.source)?;
//! // This would panic if we tried to dealloc slice1 again
//!
//! # Ok(())
//! # }
//! ```

use super::{
    BStackAllocError, BStackAllocator, BStackBulkAllocError, BStackBulkAllocator, BStackOwnedSlice,
};
use crate::BStack;
use std::collections::HashSet;
use std::io;
use std::ops::Range;
use std::sync::Mutex;

/// Returns `true` if two half-open byte ranges overlap.
fn overlaps(a: &Range<u64>, b: &Range<u64>) -> bool {
    !a.is_empty() && !b.is_empty() && a.start.max(b.start) < a.end.min(b.end)
}

/// Returns the first range in `set` that overlaps `region`, or `None`.
fn check_overlap(region: &Range<u64>, set: &HashSet<Range<u64>>) -> Option<Range<u64>> {
    set.iter().find(|r| overlaps(region, r)).cloned()
}

/// Validates that `region` can be freed given the current `state` and a set of regions
/// already queued in the same bulk operation (`pending_freed`).
///
/// Returns `true` if `region` is tracked in `state.allocated` (caller must remove it on
/// commit), or `false` if untracked (allowed — may have been allocated in a prior session).
///
/// Panics on double-free (overlap with `state.freed` or `pending_freed`), partial-free,
/// or multi-span violations.
fn check_deallocation(
    region: &Range<u64>,
    state: &DebugState,
    pending_freed: &HashSet<Range<u64>>,
) -> bool {
    if let Some(overlap) = check_overlap(region, &state.freed) {
        panic!(
            "DebugCheckingAllocator: Attempting to free region [{}, {}) which overlaps \
             with already freed region [{}, {}). This indicates a double-free bug.",
            region.start, region.end, overlap.start, overlap.end
        );
    }
    if let Some(overlap) = check_overlap(region, pending_freed) {
        panic!(
            "DebugCheckingAllocator: Attempting to free region [{}, {}) which overlaps \
             with region [{}, {}) already queued in the same bulk deallocation. \
             This indicates a double-free bug.",
            region.start, region.end, overlap.start, overlap.end
        );
    }
    let overlapping_allocated: Vec<Range<u64>> = state
        .allocated
        .iter()
        .filter(|r| overlaps(region, r))
        .cloned()
        .collect();
    match overlapping_allocated.as_slice() {
        [] => false,
        [exact] if *exact == *region => true,
        [single] => panic!(
            "DebugCheckingAllocator: Attempting to partially free region [{}, {}) \
             which is a subset or overlap of allocated region [{}, {}). \
             Partial deallocations are not allowed.",
            region.start, region.end, single.start, single.end,
        ),
        _ => panic!(
            "DebugCheckingAllocator: Attempting to free region [{}, {}) which spans \
             multiple allocated regions. This is not a valid deallocation.",
            region.start, region.end,
        ),
    }
}

/// Record an allocated region in `state`, splitting any overlapping freed regions.
fn record_allocated_region(
    state: &mut DebugState,
    region: Range<u64>,
    operation: &str,
    allocator_context: &str,
) {
    if region.is_empty() {
        return;
    }
    if let Some(overlap) = check_overlap(&region, &state.allocated) {
        panic!(
            "DebugCheckingAllocator: {operation} [{}, {}) overlaps with \
             existing allocated region [{}, {}). This indicates a bug in the underlying \
             allocator{allocator_context}.",
            region.start, region.end, overlap.start, overlap.end
        );
    }

    let overlapping_freed: Vec<Range<u64>> = state
        .freed
        .iter()
        .filter(|r| overlaps(&region, r))
        .cloned()
        .collect();

    for freed_region in overlapping_freed {
        state.freed.remove(&freed_region);

        if freed_region.start < region.start {
            state.freed.insert(freed_region.start..region.start);
        }

        if region.end < freed_region.end {
            state.freed.insert(region.end..freed_region.end);
        }
    }

    state.allocated.insert(region);
}

/// Record a freed region in `state`.
fn record_freed_region(state: &mut DebugState, region: Range<u64>) {
    if region.is_empty() {
        return;
    }
    if let Some(overlap) = check_overlap(&region, &state.freed) {
        panic!(
            "DebugCheckingAllocator: Attempting to free region [{}, {}) which overlaps \
             with already freed region [{}, {}). This indicates a double-free bug.",
            region.start, region.end, overlap.start, overlap.end
        );
    }

    state.freed.insert(region);
}

/// Validate the initial state for `with_state`, ensuring no overlaps and filtering empty ranges.
///
/// Panics if:
/// - Any two ranges within `allocated` overlap
/// - Any two ranges within `freed` overlap
/// - Any range in `allocated` overlaps with any range in `freed`
fn validate_initial_state(allocated: &mut HashSet<Range<u64>>, freed: &mut HashSet<Range<u64>>) {
    allocated.retain(|r| !r.is_empty());
    freed.retain(|r| !r.is_empty());

    let allocated_vec: Vec<_> = allocated.iter().cloned().collect();
    for i in 0..allocated_vec.len() {
        for j in (i + 1)..allocated_vec.len() {
            if overlaps(&allocated_vec[i], &allocated_vec[j]) {
                panic!(
                    "DebugCheckingAllocator::with_state: Initial allocated set contains \
                     overlapping ranges [{}, {}) and [{}, {}). The initial state must be \
                     consistent.",
                    allocated_vec[i].start,
                    allocated_vec[i].end,
                    allocated_vec[j].start,
                    allocated_vec[j].end
                );
            }
        }
    }

    let freed_vec: Vec<_> = freed.iter().cloned().collect();
    for i in 0..freed_vec.len() {
        for j in (i + 1)..freed_vec.len() {
            if overlaps(&freed_vec[i], &freed_vec[j]) {
                panic!(
                    "DebugCheckingAllocator::with_state: Initial freed set contains \
                     overlapping ranges [{}, {}) and [{}, {}). The initial state must be \
                     consistent.",
                    freed_vec[i].start, freed_vec[i].end, freed_vec[j].start, freed_vec[j].end
                );
            }
        }
    }

    for alloc_range in allocated.iter() {
        if let Some(freed_range) = check_overlap(alloc_range, freed) {
            panic!(
                "DebugCheckingAllocator::with_state: Initial state has allocated range \
                 [{}, {}) overlapping with freed range [{}, {}). The initial state must be \
                 consistent.",
                alloc_range.start, alloc_range.end, freed_range.start, freed_range.end
            );
        }
    }
}

/// Shared state protected by a single mutex inside [`DebugCheckingAllocator`].
///
/// Using one lock for both sets ensures a consistent acquisition order and prevents
/// deadlocks that would arise from the two-mutex ABBA pattern (alloc acquires
/// `allocated` then `freed`; dealloc would acquire them in the opposite order).
struct DebugState {
    allocated: HashSet<Range<u64>>,
    freed: HashSet<Range<u64>>,
}

/// Debug-only allocator wrapper that validates allocations and deallocations.
///
/// Wraps any allocator whose `Allocated` type is [`BStackOwnedSlice`] and whose
/// `Error` is [`io::Error`]. Maintains in-memory sets of allocated and freed
/// regions and validates every operation against them.
///
/// # Panics
///
/// Panics if:
/// - A newly allocated region overlaps with an existing allocated region
/// - A reallocated region overlaps with an existing allocated region
/// - A region being freed overlaps with a previously freed region
///
/// These panics indicate bugs in the underlying allocator implementation or a
/// double free in the calling code.
///
/// # Thread Safety
///
/// The internal tracking sets are protected by a `Mutex` for internal bookkeeping, but
/// allocation operations and tracking updates must not be assumed to be an atomic,
/// cross-thread synchronization boundary. Concurrent use of this debug wrapper is therefore
/// not supported unless the caller provides external synchronization.
pub struct DebugCheckingAllocator<A>
where
    A: BStackAllocator<Error = io::Error>,
{
    inner: A,
    state: Mutex<DebugState>,
}

impl<A> std::fmt::Debug for DebugCheckingAllocator<A>
where
    A: BStackAllocator<Error = io::Error> + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        f.debug_struct("DebugCheckingAllocator")
            .field("inner", &self.inner)
            .field("allocated_count", &state.allocated.len())
            .field("freed_count", &state.freed.len())
            .finish()
    }
}

impl<A> DebugCheckingAllocator<A>
where
    A: BStackAllocator<Error = io::Error>,
{
    /// Create a new `DebugCheckingAllocator` wrapping `inner`.
    ///
    /// The allocator starts with empty tracking sets. If you're reopening
    /// a file from a previous session and want to pre-populate those sets,
    /// use [`Self::with_state`] instead.
    pub fn new(inner: A) -> Self {
        Self {
            inner,
            state: Mutex::new(DebugState {
                allocated: HashSet::new(),
                freed: HashSet::new(),
            }),
        }
    }

    /// Create a new `DebugCheckingAllocator` wrapping `inner`, with pre-populated tracking sets.
    ///
    /// Use this when reopening a file from a previous session and you have metadata
    /// to reconstruct which regions were allocated or freed.
    ///
    /// # Panics
    ///
    /// Panics if the initial state is inconsistent:
    /// - Any two ranges within `allocated` overlap
    /// - Any two ranges within `freed` overlap
    /// - Any range in `allocated` overlaps with any range in `freed`
    pub fn with_state(
        inner: A,
        allocated: impl IntoIterator<Item = Range<u64>>,
        freed: impl IntoIterator<Item = Range<u64>>,
    ) -> Self {
        let mut allocated_set = allocated.into_iter().collect::<HashSet<_>>();
        let mut freed_set = freed.into_iter().collect::<HashSet<_>>();
        validate_initial_state(&mut allocated_set, &mut freed_set);
        Self {
            inner,
            state: Mutex::new(DebugState {
                allocated: allocated_set,
                freed: freed_set,
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

    /// Record a newly allocated region after validation.
    fn record_allocation(&self, offset: u64, len: u64) {
        let region = offset..offset + len;
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        record_allocated_region(&mut state, region, "Newly allocated region", "");
    }

    /// Record a deallocation after validation.
    fn record_deallocation(&self, offset: u64, len: u64) {
        if len == 0 {
            return;
        }
        let region = offset..offset + len;
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let was_in_allocated = check_deallocation(&region, &state, &HashSet::new());
        if was_in_allocated {
            state.allocated.remove(&region);
        }
        state.freed.insert(region);
    }

    /// Remove a region from the allocated set without inserting it into freed.
    ///
    /// Used when the inner allocator reports a lost handle (`handle: None` in
    /// [`BStackAllocError`]): the region's fate is unknown, so tracking it as
    /// either "live" or "freed" would produce false positives.
    fn forget_region(&self, offset: u64, len: u64) {
        if len == 0 {
            return;
        }
        let region = offset..offset + len;
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.allocated.remove(&region);
    }
}

impl<A> BStackAllocator for DebugCheckingAllocator<A>
where
    A: 'static + BStackAllocator<Error = io::Error>,
    for<'b> A: BStackAllocator<Allocated<'b> = BStackOwnedSlice<'b, A>>,
{
    type Error = io::Error;
    type Allocated<'a>
        = BStackOwnedSlice<'a, Self>
    where
        Self: 'a;

    fn stack(&self) -> &BStack {
        self.inner.stack()
    }

    fn into_stack(self) -> BStack {
        self.inner.into_stack()
    }

    fn alloc(&self, len: u64) -> io::Result<Self::Allocated<'_>> {
        let inner_handle = self.inner.alloc(len)?;

        let inner_slice: BStackOwnedSlice<'_, A> = inner_handle;
        let offset = inner_slice.start();
        let len = inner_slice.len();

        self.record_allocation(offset, len);

        // SAFETY: The inner allocator successfully returned this region.
        Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, offset, len) })
    }

    fn realloc<'a>(
        &'a self,
        handle: BStackOwnedSlice<'a, Self>,
        new_len: u64,
    ) -> Result<BStackOwnedSlice<'a, Self>, BStackAllocError<'a, Self>> {
        let old_offset = handle.start();
        let old_len = handle.len();
        let old_region = old_offset..old_offset + old_len;

        // SAFETY: Reconstructing the inner handle from coordinates that were
        // originally produced by the inner allocator.
        let inner_handle =
            unsafe { BStackOwnedSlice::from_raw_parts(&self.inner, old_offset, old_len) };

        let new_inner_handle = match self.inner.realloc(inner_handle, new_len) {
            Ok(h) => h,
            Err(inner_err) => {
                // Re-wrap any surviving inner handle as a wrapper handle.
                // If the handle is None the region is lost — stop tracking it.
                let handle = match inner_err.handle {
                    Some(h) => {
                        let o = h.start();
                        let l = h.len();
                        // SAFETY: The inner allocator confirmed this region survives.
                        Some(unsafe { BStackOwnedSlice::from_raw_parts(self, o, l) })
                    }
                    None => {
                        self.forget_region(old_offset, old_len);
                        None
                    }
                };
                return Err(BStackAllocError {
                    source: inner_err.source,
                    handle,
                });
            }
        };

        let new_inner_slice: BStackOwnedSlice<'_, A> = new_inner_handle;
        let new_offset = new_inner_slice.start();
        let new_len = new_inner_slice.len();
        let new_region = new_offset..new_offset + new_len;

        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let overlapping_allocation = state
            .allocated
            .iter()
            .find(|region| **region != old_region && overlaps(&new_region, region))
            .cloned();
        if let Some(overlap) = overlapping_allocation {
            panic!(
                "DebugCheckingAllocator: Reallocated region [{}, {}) overlaps with \
                 existing allocated region [{}, {}). This indicates a bug in the underlying \
                 allocator's realloc.",
                new_region.start, new_region.end, overlap.start, overlap.end
            );
        }

        state.allocated.remove(&old_region);
        record_freed_region(&mut state, old_region.clone());
        record_allocated_region(&mut state, new_region, "Reallocated region", "'s realloc");
        drop(state);

        // SAFETY: The inner allocator successfully returned this region via realloc.
        Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, new_offset, new_len) })
    }

    fn dealloc<'a>(
        &'a self,
        handle: BStackOwnedSlice<'a, Self>,
    ) -> Result<(), BStackAllocError<'a, Self>> {
        let offset = handle.start();
        let len = handle.len();

        {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            check_deallocation(&(offset..offset + len), &state, &HashSet::new());
        }

        // SAFETY: Reconstructing the inner handle from coordinates that were
        // originally produced by the inner allocator.
        let inner_handle = unsafe { BStackOwnedSlice::from_raw_parts(&self.inner, offset, len) };

        if let Err(inner_err) = self.inner.dealloc(inner_handle) {
            let handle = match inner_err.handle {
                Some(h) => {
                    let o = h.start();
                    let l = h.len();
                    // SAFETY: The inner allocator confirmed this region survives.
                    Some(unsafe { BStackOwnedSlice::from_raw_parts(self, o, l) })
                }
                None => {
                    self.forget_region(offset, len);
                    None
                }
            };
            return Err(BStackAllocError {
                source: inner_err.source,
                handle,
            });
        }

        self.record_deallocation(offset, len);
        Ok(())
    }
}

impl<A> BStackBulkAllocator for DebugCheckingAllocator<A>
where
    A: 'static + BStackBulkAllocator<Error = io::Error>,
    for<'b> A: BStackAllocator<Allocated<'b> = BStackOwnedSlice<'b, A>>,
{
    fn alloc_bulk(&self, lengths: impl AsRef<[u64]>) -> io::Result<Vec<Self::Allocated<'_>>> {
        let inner_handles = self.inner.alloc_bulk(lengths)?;

        let inner_slices: Vec<BStackOwnedSlice<'_, A>> = inner_handles.into_iter().collect();

        let mut result = Vec::with_capacity(inner_slices.len());
        for inner_slice in inner_slices {
            let offset = inner_slice.start();
            let len = inner_slice.len();
            self.record_allocation(offset, len);

            // SAFETY: The inner allocator successfully returned this region.
            result.push(unsafe { BStackOwnedSlice::from_raw_parts(self, offset, len) });
        }

        Ok(result)
    }

    fn dealloc_bulk<'a>(
        &'a self,
        handles: impl IntoIterator<Item = BStackOwnedSlice<'a, Self>>,
    ) -> Result<(), BStackBulkAllocError<'a, Self>> {
        let handles: Vec<_> = handles.into_iter().collect();

        // Pass 1: extract coordinates and validate all handles.
        let mut coords: Vec<(u64, u64)> = Vec::with_capacity(handles.len());
        let mut pending_freed: HashSet<Range<u64>> = HashSet::with_capacity(handles.len());
        {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            for handle in &handles {
                let offset = handle.start();
                let len = handle.len();
                let region = offset..offset + len;
                check_deallocation(&region, &state, &pending_freed);
                pending_freed.insert(region);
                coords.push((offset, len));
            }
        }

        // Convert to inner handles.
        let inner_handles: Vec<BStackOwnedSlice<'a, A>> = coords
            .iter()
            .map(|&(offset, len)| {
                // SAFETY: Reconstructing inner handles from coordinates that were
                // originally produced by the inner allocator.
                unsafe { BStackOwnedSlice::from_raw_parts(&self.inner, offset, len) }
            })
            .collect();

        if let Err(inner_err) = self.inner.dealloc_bulk(inner_handles) {
            // Re-wrap surviving inner handles as wrapper handles.
            let surviving: Vec<BStackOwnedSlice<'a, Self>> = inner_err
                .handles
                .into_iter()
                .map(|h| {
                    let o = h.start();
                    let l = h.len();
                    // SAFETY: The inner allocator confirmed these regions survive.
                    unsafe { BStackOwnedSlice::from_raw_parts(self, o, l) }
                })
                .collect();

            // If the inner returned fewer handles than we passed in, some
            // regions were partially freed (progressive free). Forget those
            // that weren't returned — their fate is unknown.
            if surviving.len() < coords.len() {
                let surviving_set: HashSet<Range<u64>> = surviving
                    .iter()
                    .map(|h| h.start()..h.start() + h.len())
                    .collect();
                for &(offset, len) in &coords {
                    let region = offset..offset + len;
                    if !surviving_set.contains(&region) {
                        self.forget_region(offset, len);
                    }
                }
            }

            return Err(BStackBulkAllocError {
                source: inner_err.source,
                handles: surviving,
            });
        }

        // Pass 2: commit tracking updates.
        for (offset, len) in coords {
            self.record_deallocation(offset, len);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alloc::BStackAllocator;

    struct MockAllocator;

    impl crate::alloc::BStackAllocator for MockAllocator {
        type Error = io::Error;
        type Allocated<'a> = crate::alloc::BStackOwnedSlice<'a, Self>;

        fn stack(&self) -> &crate::BStack {
            unimplemented!()
        }
        fn into_stack(self) -> crate::BStack {
            unimplemented!()
        }
        fn alloc(&self, _len: u64) -> io::Result<Self::Allocated<'_>> {
            unimplemented!()
        }
        fn realloc<'a>(
            &'a self,
            _: Self::Allocated<'a>,
            _: u64,
        ) -> Result<Self::Allocated<'a>, BStackAllocError<'a, Self>> {
            unimplemented!()
        }
    }

    fn checker() -> DebugCheckingAllocator<MockAllocator> {
        DebugCheckingAllocator::new(MockAllocator)
    }

    #[test]
    fn test_alloc_dealloc_basic() {
        let c = checker();
        c.record_allocation(0, 100);
        c.record_deallocation(0, 100);
    }

    #[test]
    fn test_adjacent_allocs_do_not_overlap() {
        let c = checker();
        c.record_allocation(0, 50);
        c.record_allocation(50, 50);
    }

    #[test]
    fn test_untracked_disjoint_alloc_is_allowed() {
        let c = DebugCheckingAllocator::with_state(MockAllocator, [0..100], [200..300]);
        c.record_allocation(120, 50);

        let state = c.state.lock().unwrap();
        assert!(state.allocated.contains(&(0..100)));
        assert!(state.allocated.contains(&(120..170)));
        assert!(state.freed.contains(&(200..300)));
    }

    #[test]
    fn test_dealloc_untracked_region_is_allowed() {
        let c = checker();
        c.record_deallocation(0, 100);
    }

    #[test]
    #[should_panic(expected = "double-free")]
    fn test_double_free_panics() {
        let c = checker();
        c.record_allocation(0, 100);
        c.record_deallocation(0, 100);
        c.record_deallocation(0, 100);
    }

    #[test]
    #[should_panic(expected = "double-free")]
    fn test_partial_overlap_with_freed_region_panics() {
        let c = checker();
        c.record_deallocation(0, 100);
        c.record_deallocation(50, 25);
    }

    #[test]
    #[should_panic(expected = "overlaps with existing allocated region")]
    fn test_overlapping_alloc_panics() {
        let c = checker();
        c.record_allocation(0, 100);
        c.record_allocation(50, 100);
    }

    #[test]
    #[should_panic(expected = "Partial deallocations are not allowed")]
    fn test_partial_free_panics() {
        let c = checker();
        c.record_allocation(0, 100);
        c.record_deallocation(0, 50);
    }

    #[test]
    #[should_panic(expected = "Partial deallocations are not allowed")]
    fn test_superset_free_panics() {
        let c = checker();
        c.record_allocation(20, 50);
        c.record_deallocation(0, 100);
    }

    #[test]
    #[should_panic(expected = "spans multiple allocated regions")]
    fn test_spanning_free_panics() {
        let c = checker();
        c.record_allocation(0, 50);
        c.record_allocation(50, 50);
        c.record_deallocation(0, 100);
    }

    #[test]
    fn test_freed_region_split_on_reallocation() {
        let c = checker();
        c.record_allocation(0, 100);
        c.record_deallocation(0, 100);
        c.record_allocation(20, 30);

        let state = c.state.lock().unwrap();
        assert!(state.freed.contains(&(0..20)));
        assert!(state.freed.contains(&(50..100)));
        assert!(!state.freed.contains(&(0..100)));
        assert!(state.allocated.contains(&(20..50)));
    }

    #[test]
    fn test_freed_region_split_left_edge() {
        let c = checker();
        c.record_allocation(0, 100);
        c.record_deallocation(0, 100);
        c.record_allocation(0, 30);

        let state = c.state.lock().unwrap();
        assert!(!state.freed.contains(&(0..100)));
        assert!(!state.freed.iter().any(|r| r.start < 30));
        assert!(state.freed.contains(&(30..100)));
    }

    #[test]
    fn test_freed_region_split_right_edge() {
        let c = checker();
        c.record_allocation(0, 100);
        c.record_deallocation(0, 100);
        c.record_allocation(70, 30);

        let state = c.state.lock().unwrap();
        assert!(!state.freed.contains(&(0..100)));
        assert!(state.freed.contains(&(0..70)));
        assert!(!state.freed.iter().any(|r| r.end > 70));
    }

    #[test]
    fn test_freed_region_exact_reuse_removes_entry() {
        let c = checker();
        c.record_allocation(0, 100);
        c.record_deallocation(0, 100);
        c.record_allocation(0, 100);

        let state = c.state.lock().unwrap();
        assert!(state.freed.is_empty());
        assert!(state.allocated.contains(&(0..100)));
    }

    #[test]
    fn test_forget_region_removes_from_allocated_only() {
        let c = checker();
        c.record_allocation(0, 100);
        c.record_allocation(200, 50);
        c.forget_region(0, 100);

        let state = c.state.lock().unwrap();
        assert!(!state.allocated.contains(&(0..100)));
        assert!(state.allocated.contains(&(200..50 + 200)));
        assert!(!state.freed.contains(&(0..100)));
    }

    // --- Public API integration tests ---

    use std::cell::{Cell, RefCell};
    use std::rc::Rc;

    #[derive(Debug, Clone, Default)]
    struct MockAllocatorConfig {
        fail_dealloc: bool,
        fail_dealloc_bulk: bool,
        fail_realloc: bool,
        realloc_in_place: bool,
        /// When true, a failed realloc reports the handle as lost (None).
        realloc_lose_handle: bool,
        /// When true, a failed dealloc reports the handle as lost (None).
        dealloc_lose_handle: bool,
    }

    struct ControllableMockAllocator {
        stack: BStack,
        next_offset: Cell<u64>,
        allocated: RefCell<HashSet<Range<u64>>>,
        config: Rc<RefCell<MockAllocatorConfig>>,
    }

    impl ControllableMockAllocator {
        fn new(stack: BStack, config: Rc<RefCell<MockAllocatorConfig>>) -> Self {
            Self {
                stack,
                next_offset: Cell::new(0),
                allocated: RefCell::new(HashSet::new()),
                config,
            }
        }
    }

    impl BStackAllocator for ControllableMockAllocator {
        type Error = io::Error;
        type Allocated<'a> = BStackOwnedSlice<'a, Self>;

        fn stack(&self) -> &BStack {
            &self.stack
        }

        fn into_stack(self) -> BStack {
            self.stack
        }

        fn alloc(&self, len: u64) -> io::Result<Self::Allocated<'_>> {
            let offset = self.next_offset.get();
            self.next_offset.set(offset + len);
            let region = offset..offset + len;
            self.allocated.borrow_mut().insert(region);
            // SAFETY: Mock allocator test harness — regions are within bounds.
            Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, offset, len) })
        }

        fn realloc<'a>(
            &'a self,
            handle: Self::Allocated<'a>,
            new_len: u64,
        ) -> Result<Self::Allocated<'a>, BStackAllocError<'a, Self>> {
            let config = self.config.borrow();
            if config.fail_realloc {
                let lose = config.realloc_lose_handle;
                drop(config);
                let source = io::Error::other("mock realloc failure");
                return if lose {
                    Err(BStackAllocError::lost(source))
                } else {
                    Err(BStackAllocError::with_handle(source, handle))
                };
            }
            let realloc_in_place = config.realloc_in_place;
            drop(config);

            let old_offset = handle.start();
            let old_len = handle.len();
            let old_region = old_offset..old_offset + old_len;
            self.allocated.borrow_mut().remove(&old_region);

            let new_offset = if realloc_in_place {
                let new_end = old_offset + new_len;
                if self.next_offset.get() < new_end {
                    self.next_offset.set(new_end);
                }
                old_offset
            } else {
                let new_offset = self.next_offset.get();
                self.next_offset.set(new_offset + new_len);
                new_offset
            };
            let new_region = new_offset..new_offset + new_len;
            self.allocated.borrow_mut().insert(new_region);

            // SAFETY: Mock allocator test harness.
            Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, new_offset, new_len) })
        }

        fn dealloc<'a>(
            &'a self,
            handle: Self::Allocated<'a>,
        ) -> Result<(), BStackAllocError<'a, Self>> {
            let config = self.config.borrow();
            if config.fail_dealloc {
                let lose = config.dealloc_lose_handle;
                drop(config);
                let source = io::Error::other("mock dealloc failure");
                return if lose {
                    Err(BStackAllocError::lost(source))
                } else {
                    Err(BStackAllocError::with_handle(source, handle))
                };
            }
            drop(config);

            let offset = handle.start();
            let len = handle.len();
            let region = offset..offset + len;
            self.allocated.borrow_mut().remove(&region);
            Ok(())
        }
    }

    impl BStackBulkAllocator for ControllableMockAllocator {
        fn alloc_bulk(&self, lengths: impl AsRef<[u64]>) -> io::Result<Vec<Self::Allocated<'_>>> {
            lengths
                .as_ref()
                .iter()
                .map(|&len| self.alloc(len))
                .collect()
        }

        fn dealloc_bulk<'a>(
            &'a self,
            handles: impl IntoIterator<Item = Self::Allocated<'a>>,
        ) -> Result<(), BStackBulkAllocError<'a, Self>> {
            let handles: Vec<_> = handles.into_iter().collect();
            if self.config.borrow().fail_dealloc_bulk {
                return Err(BStackBulkAllocError::with_handles(
                    io::Error::other("mock dealloc_bulk failure"),
                    handles,
                ));
            }

            for handle in handles {
                let offset = handle.start();
                let len = handle.len();
                let region = offset..offset + len;
                self.allocated.borrow_mut().remove(&region);
            }
            Ok(())
        }
    }

    fn create_test_stack() -> io::Result<(BStack, std::path::PathBuf)> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("bstack_debug_test_{pid}_{id}.bin"));
        let stack = BStack::open(&path)?;
        Ok((stack, path))
    }

    struct TestGuard(std::path::PathBuf);
    impl Drop for TestGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[test]
    fn test_alloc_success_updates_tracking() -> io::Result<()> {
        let (stack, path) = create_test_stack()?;
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig::default()));
        let inner = ControllableMockAllocator::new(stack, config.clone());
        let alloc = DebugCheckingAllocator::new(inner);

        let handle1 = alloc.alloc(100)?;
        let handle2 = alloc.alloc(200)?;

        let state = alloc.state.lock().unwrap();
        assert_eq!(state.allocated.len(), 2);
        assert!(state.allocated.contains(&(0..100)));
        assert!(state.allocated.contains(&(100..300)));

        drop(state);
        alloc.dealloc(handle1).map_err(|e| e.source)?;
        alloc.dealloc(handle2).map_err(|e| e.source)?;

        Ok(())
    }

    #[test]
    fn test_realloc_success() -> io::Result<()> {
        let (stack, path) = create_test_stack()?;
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig::default()));
        let inner = ControllableMockAllocator::new(stack, config.clone());
        let alloc = DebugCheckingAllocator::new(inner);

        let handle = alloc.alloc(100)?;
        {
            let state = alloc.state.lock().unwrap();
            assert!(state.allocated.contains(&(0..100)));
        }

        let new_handle = alloc.realloc(handle, 200).map_err(|e| e.source)?;
        {
            let state = alloc.state.lock().unwrap();
            assert!(!state.allocated.contains(&(0..100)));
            assert!(state.allocated.contains(&(100..300)));
        }

        alloc.dealloc(new_handle).map_err(|e| e.source)?;
        Ok(())
    }

    #[test]
    fn test_realloc_into_freed_region_updates_freed_tracking() -> io::Result<()> {
        let (stack, path) = create_test_stack()?;
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig::default()));
        let inner = ControllableMockAllocator::new(stack, config.clone());
        let alloc = DebugCheckingAllocator::with_state(inner, [], [150..300]);

        let handle = alloc.alloc(100)?;
        alloc.inner().next_offset.set(150);

        let new_handle = alloc.realloc(handle, 100).map_err(|e| e.source)?;

        {
            let state = alloc.state.lock().unwrap();
            assert!(state.allocated.contains(&(150..250)));
            assert!(!state.allocated.contains(&(0..100)));
            assert!(state.freed.contains(&(0..100)));
            assert!(state.freed.contains(&(250..300)));
            assert!(!state.freed.contains(&(150..300)));
        }

        alloc.dealloc(new_handle).map_err(|e| e.source)?;
        Ok(())
    }

    #[test]
    fn test_realloc_in_place_shrink_marks_released_tail_as_freed() -> io::Result<()> {
        let (stack, path) = create_test_stack()?;
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig {
            realloc_in_place: true,
            ..Default::default()
        }));
        let inner = ControllableMockAllocator::new(stack, config);
        let alloc = DebugCheckingAllocator::new(inner);

        let handle = alloc.alloc(100)?;
        let new_handle = alloc.realloc(handle, 60).map_err(|e| e.source)?;

        {
            let state = alloc.state.lock().unwrap();
            assert!(state.allocated.contains(&(0..60)));
            assert!(!state.allocated.contains(&(0..100)));
            assert!(state.freed.contains(&(60..100)));
        }

        alloc.dealloc(new_handle).map_err(|e| e.source)?;
        Ok(())
    }

    #[test]
    #[should_panic(expected = "overlaps with already freed region")]
    fn test_realloc_stale_handle_after_shrink_panics() {
        let (stack, path) = create_test_stack().unwrap();
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig {
            realloc_in_place: true,
            ..Default::default()
        }));
        let inner = ControllableMockAllocator::new(stack, config);
        let alloc = DebugCheckingAllocator::new(inner);

        let handle = alloc.alloc(100).unwrap();
        let old_offset = handle.start();
        let old_len = handle.len();

        let _new_handle = alloc.realloc(handle, 60).ok().unwrap();

        // SAFETY: Intentionally creating an invalid stale handle for testing.
        let stale_handle = unsafe { BStackOwnedSlice::from_raw_parts(&alloc, old_offset, old_len) };

        alloc.dealloc(stale_handle).ok().unwrap();
    }

    #[test]
    fn test_realloc_inner_failure_preserves_tracking() -> io::Result<()> {
        let (stack, path) = create_test_stack()?;
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig::default()));
        let inner = ControllableMockAllocator::new(stack, config.clone());
        let alloc = DebugCheckingAllocator::new(inner);

        let handle = alloc.alloc(100)?;
        {
            let state = alloc.state.lock().unwrap();
            assert!(state.allocated.contains(&(0..100)));
        }

        config.borrow_mut().fail_realloc = true;

        let result = alloc.realloc(handle, 200);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(
            err.handle.is_some(),
            "Surviving handle should be returned on realloc failure"
        );

        {
            let state = alloc.state.lock().unwrap();
            assert!(
                state.allocated.contains(&(0..100)),
                "Original allocation should still be tracked after realloc failure"
            );
        }

        Ok(())
    }

    #[test]
    fn test_realloc_inner_failure_lost_handle_forgets_region() -> io::Result<()> {
        let (stack, path) = create_test_stack()?;
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig {
            fail_realloc: true,
            realloc_lose_handle: true,
            ..Default::default()
        }));
        let inner = ControllableMockAllocator::new(stack, config);
        let alloc = DebugCheckingAllocator::new(inner);

        let handle = alloc.alloc(100)?;

        let result = alloc.realloc(handle, 200);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(err.handle.is_none(), "Lost handle should propagate as None");

        {
            let state = alloc.state.lock().unwrap();
            assert!(
                !state.allocated.contains(&(0..100)),
                "Lost region should be removed from allocated set"
            );
            assert!(
                !state.freed.contains(&(0..100)),
                "Lost region should not appear in freed set"
            );
        }

        Ok(())
    }

    #[test]
    fn test_dealloc_success_updates_tracking() -> io::Result<()> {
        let (stack, path) = create_test_stack()?;
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig::default()));
        let inner = ControllableMockAllocator::new(stack, config.clone());
        let alloc = DebugCheckingAllocator::new(inner);

        let handle = alloc.alloc(100)?;
        {
            let state = alloc.state.lock().unwrap();
            assert!(state.allocated.contains(&(0..100)));
            assert!(!state.freed.contains(&(0..100)));
        }

        alloc.dealloc(handle).map_err(|e| e.source)?;
        {
            let state = alloc.state.lock().unwrap();
            assert!(!state.allocated.contains(&(0..100)));
            assert!(state.freed.contains(&(0..100)));
        }

        Ok(())
    }

    #[test]
    fn test_dealloc_inner_failure_preserves_tracking() -> io::Result<()> {
        let (stack, path) = create_test_stack()?;
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig::default()));
        let inner = ControllableMockAllocator::new(stack, config.clone());
        let alloc = DebugCheckingAllocator::new(inner);

        let handle = alloc.alloc(100)?;

        config.borrow_mut().fail_dealloc = true;

        let result = alloc.dealloc(handle);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(
            err.handle.is_some(),
            "Surviving handle should be returned on dealloc failure"
        );

        {
            let state = alloc.state.lock().unwrap();
            assert!(
                state.allocated.contains(&(0..100)),
                "Allocation should still be tracked after dealloc failure"
            );
            assert!(
                !state.freed.contains(&(0..100)),
                "Failed dealloc should not add region to freed set"
            );
        }

        Ok(())
    }

    #[test]
    fn test_dealloc_inner_failure_lost_handle_forgets_region() -> io::Result<()> {
        let (stack, path) = create_test_stack()?;
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig {
            fail_dealloc: true,
            dealloc_lose_handle: true,
            ..Default::default()
        }));
        let inner = ControllableMockAllocator::new(stack, config);
        let alloc = DebugCheckingAllocator::new(inner);

        let handle = alloc.alloc(100)?;

        let result = alloc.dealloc(handle);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(err.handle.is_none());

        {
            let state = alloc.state.lock().unwrap();
            assert!(
                !state.allocated.contains(&(0..100)),
                "Lost region should be removed from allocated set"
            );
            assert!(
                !state.freed.contains(&(0..100)),
                "Lost region should not appear in freed set"
            );
        }

        Ok(())
    }

    #[test]
    #[should_panic(expected = "double-free")]
    fn test_dealloc_double_free_via_public_api() {
        let (stack, path) = create_test_stack().unwrap();
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig::default()));
        let inner = ControllableMockAllocator::new(stack, config);
        let alloc = DebugCheckingAllocator::new(inner);

        let handle = alloc.alloc(100).unwrap();
        alloc.dealloc(handle).ok().unwrap();
        alloc.record_deallocation(0, 100);
    }

    #[test]
    fn test_dealloc_untracked_region_via_public_api_is_allowed() -> io::Result<()> {
        let (stack, path) = create_test_stack()?;
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig::default()));
        let inner = ControllableMockAllocator::new(stack, config);
        let alloc = DebugCheckingAllocator::new(inner);

        // SAFETY: Creating a fake handle for testing.
        let fake_handle = unsafe { BStackOwnedSlice::from_raw_parts(&alloc, 500, 100) };
        alloc.dealloc(fake_handle).map_err(|e| e.source)?;

        let state = alloc.state.lock().unwrap();
        assert!(state.freed.contains(&(500..600)));
        Ok(())
    }

    #[test]
    fn test_alloc_bulk_success() -> io::Result<()> {
        let (stack, path) = create_test_stack()?;
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig::default()));
        let inner = ControllableMockAllocator::new(stack, config);
        let alloc = DebugCheckingAllocator::new(inner);

        let handles = alloc.alloc_bulk(&[100, 200, 300])?;
        assert_eq!(handles.len(), 3);

        {
            let state = alloc.state.lock().unwrap();
            assert_eq!(state.allocated.len(), 3);
            assert!(state.allocated.contains(&(0..100)));
            assert!(state.allocated.contains(&(100..300)));
            assert!(state.allocated.contains(&(300..600)));
        }

        alloc.dealloc_bulk(handles).map_err(|e| e.source)?;
        Ok(())
    }

    #[test]
    fn test_dealloc_bulk_success() -> io::Result<()> {
        let (stack, path) = create_test_stack()?;
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig::default()));
        let inner = ControllableMockAllocator::new(stack, config);
        let alloc = DebugCheckingAllocator::new(inner);

        let handles = alloc.alloc_bulk(&[100, 200, 300])?;

        alloc.dealloc_bulk(handles).map_err(|e| e.source)?;

        {
            let state = alloc.state.lock().unwrap();
            assert!(state.allocated.is_empty());
            assert_eq!(state.freed.len(), 3);
            assert!(state.freed.contains(&(0..100)));
            assert!(state.freed.contains(&(100..300)));
            assert!(state.freed.contains(&(300..600)));
        }

        Ok(())
    }

    #[test]
    fn test_dealloc_bulk_inner_failure_preserves_tracking() -> io::Result<()> {
        let (stack, path) = create_test_stack()?;
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig::default()));
        let inner = ControllableMockAllocator::new(stack, config.clone());
        let alloc = DebugCheckingAllocator::new(inner);

        let handles = alloc.alloc_bulk(&[100, 200, 300])?;

        config.borrow_mut().fail_dealloc_bulk = true;

        let result = alloc.dealloc_bulk(handles);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.handles.len(), 3, "All handles should survive");

        {
            let state = alloc.state.lock().unwrap();
            assert_eq!(
                state.allocated.len(),
                3,
                "all regions should still be allocated"
            );
            assert!(
                state.freed.is_empty(),
                "no regions should be freed after inner failure"
            );
        }

        Ok(())
    }

    #[test]
    #[should_panic(expected = "double-free")]
    fn test_dealloc_bulk_double_free_panics() {
        let (stack, path) = create_test_stack().unwrap();
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig::default()));
        let inner = ControllableMockAllocator::new(stack, config);
        let alloc = DebugCheckingAllocator::new(inner);

        let handles = alloc.alloc_bulk(&[100, 200]).unwrap();
        alloc.dealloc_bulk(handles).ok().unwrap();
        alloc.record_deallocation(0, 100);
    }

    #[test]
    fn test_with_state_valid_disjoint_ranges() {
        let c = DebugCheckingAllocator::with_state(MockAllocator, [0..100, 200..300], [400..500]);
        let state = c.state.lock().unwrap();
        assert_eq!(state.allocated.len(), 2);
        assert_eq!(state.freed.len(), 1);
    }

    #[test]
    fn test_with_state_filters_empty_ranges() {
        let c = DebugCheckingAllocator::with_state(
            MockAllocator,
            [0..100, 100..100, 200..300],
            [400..400, 500..600],
        );
        let state = c.state.lock().unwrap();
        assert_eq!(state.allocated.len(), 2);
        assert_eq!(state.freed.len(), 1);
        assert!(!state.allocated.contains(&(100..100)));
        assert!(!state.freed.contains(&(400..400)));
    }

    #[test]
    #[should_panic(expected = "Initial allocated set contains overlapping ranges")]
    fn test_with_state_panics_on_overlapping_allocated() {
        DebugCheckingAllocator::with_state(MockAllocator, [0..100, 50..150], []);
    }

    #[test]
    #[should_panic(expected = "Initial freed set contains overlapping ranges")]
    fn test_with_state_panics_on_overlapping_freed() {
        DebugCheckingAllocator::with_state(MockAllocator, [], [0..100, 50..150]);
    }

    #[test]
    #[should_panic(expected = "allocated range")]
    fn test_with_state_panics_on_allocated_freed_overlap() {
        DebugCheckingAllocator::with_state(MockAllocator, [0..100, 200..300], [50..150, 400..500]);
    }

    #[test]
    fn test_with_state_valid_adjacent_ranges() {
        let c = DebugCheckingAllocator::with_state(
            MockAllocator,
            [0..100, 100..200],
            [200..300, 300..400],
        );
        let state = c.state.lock().unwrap();
        assert_eq!(state.allocated.len(), 2);
        assert_eq!(state.freed.len(), 2);
    }
}
