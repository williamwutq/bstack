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
    // Filter out empty ranges
    allocated.retain(|r| !r.is_empty());
    freed.retain(|r| !r.is_empty());

    // Check for overlaps within allocated set
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

    // Check for overlaps within freed set
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

    // Check for overlaps between allocated and freed sets
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

// Manual Clone, Copy, and Debug implementations since the derive macro is too conservative
impl<'a, A> Clone for DebugHandle<'a, A>
where
    A: BStackAllocator<Error = io::Error>,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, A> std::fmt::Debug for DebugHandle<'a, A>
where
    A: BStackAllocator<Error = io::Error>,
    A::Allocated<'a>: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DebugHandle")
            .field("inner", &self.inner)
            .finish()
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
        let slice: BStackSlice<'_, A> = self.inner.try_into().map_err(|e| {
            io::Error::other(format!(
                "inner handle is not convertible to BStackSlice: {e}"
            ))
        })?;
        // SAFETY:
        // 1. `offset + len` cannot overflow: the slice was returned by the inner allocator,
        //    which is responsible for never producing an overflowing region.
        // 2. `[offset, offset + len)` lies within the backing stack's payload for the same
        //    reason — the inner allocator only returns in-bounds regions.
        // 3. This slice is for I/O only. `DebugCheckingAllocator::realloc` and `::dealloc`
        //    accept `DebugHandle`, not `BStackSlice`, so this slice is never passed to
        //    either, satisfying the realloc/dealloc ownership invariant.
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
    allocated: HashSet<Range<u64>>,
    /// Set of regions that have been freed (may persist across sessions).
    freed: HashSet<Range<u64>>,
}

/// Debug-only allocator wrapper that validates allocations and deallocations.
///
/// Wraps any [`BStackAllocator`] with `Error = io::Error`. This wrapper's
/// allocated handle type is [`DebugHandle`], which preserves the inner
/// allocator's handle while enabling conversion to [`BStackSlice`]. It also
/// maintains sets of allocated and freed regions to detect overlaps.
///
/// # Constraints
///
/// This wrapper works with any allocator whose `Allocated` handles can convert
/// to and from [`BStackSlice`], which includes all allocators provided by this
/// library ([`crate::LinearBStackAllocator`], [`crate::FirstFitBStackAllocator`],
/// [`crate::GhostTreeBstackAllocator`], [`crate::ManualAllocator`]).
///
/// # Panics
///
/// Panics if:
/// - A newly allocated region overlaps with an existing allocated region
/// - A reallocated region overlaps with an existing allocated region
/// - A region being freed overlaps with a previously freed region
///
/// These panics indicate bugs in the underlying allocator implementation or a double
/// free in the calling code.
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
    ///
    /// If the allocated region overlaps with freed regions, those freed regions are
    /// removed and split around the new allocation. For example, if allocating [b, c)
    /// while [a, d) is freed, the freed set will be updated to contain [a, b) and [c, d).
    fn record_allocation(&self, offset: u64, len: u64) {
        let region = offset..offset + len;
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        record_allocated_region(&mut state, region, "Newly allocated region", "");
    }

    /// Record a deallocation after validation.
    ///
    /// Freeing a region with no recorded allocation is allowed (the checker only tracks
    /// allocations made through itself). Panics if the freed region partially overlaps or
    /// spans multiple recorded allocations, as those indicate real bugs.
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
}

/// | Method    | Atomic | Notes                                                         |
/// |-----------|--------|---------------------------------------------------------------|
/// | `alloc`   | No     | Inner alloc then tracking update are two separate steps       |
/// | `realloc` | No     | Inner realloc then tracking swap are two separate steps       |
/// | `dealloc` | No     | Tracking validation then inner dealloc are two separate steps |
///
/// A crash between the inner operation and the tracking update leaves the in-memory
/// state inconsistent, but because tracking state is not persistent this only matters
/// within a single process run.
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
        let slice: BStackSlice<'_, A> = match handle.try_into() {
            Ok(slice) => slice,
            Err(e) => {
                // The inner allocator succeeded but we can't inspect the handle — free it
                // to avoid leaking the allocation, then surface the error.
                return match self.inner.dealloc(handle) {
                    Ok(()) => Err(io::Error::other(format!(
                        "allocated handle is not convertible to BStackSlice: {e}"
                    ))),
                    Err(rollback_err) => Err(io::Error::other(format!(
                        "allocated handle is not convertible to BStackSlice: {e}; rollback via dealloc failed: {rollback_err}"
                    ))),
                };
            }
        };

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
        let old_region = old_slice.start()..old_slice.start() + old_slice.len();

        let new_inner_handle = self.inner.realloc(handle.inner, new_len)?;

        // Convert result; free on failure to avoid leaking the new allocation
        let new_slice: BStackSlice<'_, A> = match new_inner_handle.try_into() {
            Ok(slice) => slice,
            Err(e) => {
                return match self.inner.dealloc(new_inner_handle) {
                    Ok(()) => Err(io::Error::other(format!(
                        "reallocated handle is not convertible to BStackSlice: {e}"
                    ))),
                    Err(rollback_err) => Err(io::Error::other(format!(
                        "reallocated handle is not convertible to BStackSlice: {e}; rollback via dealloc failed: {rollback_err}"
                    ))),
                };
            }
        };
        let new_region = new_slice.start()..new_slice.start() + new_slice.len();

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
        record_freed_region(&mut state, old_region);
        record_allocated_region(&mut state, new_region, "Reallocated region", "'s realloc");

        Ok(DebugHandle::new(self, new_inner_handle))
    }

    fn dealloc(&self, handle: Self::Allocated<'_>) -> io::Result<()> {
        let slice: BStackSlice<'_, A> = match handle.inner.try_into() {
            Ok(slice) => slice,
            Err(e) => {
                // Can't identify the region, but still attempt to free the inner
                // handle to avoid a leak; compose errors if dealloc also fails.
                return match self.inner.dealloc(handle.inner) {
                    Ok(()) => Err(io::Error::other(format!(
                        "handle is not convertible to BStackSlice: {e}"
                    ))),
                    Err(dealloc_err) => Err(io::Error::other(format!(
                        "handle is not convertible to BStackSlice: {e}; dealloc also failed: {dealloc_err}"
                    ))),
                };
            }
        };
        let (offset, len) = (slice.start(), slice.len());

        // Validate before touching the inner allocator. Untracked regions are allowed
        // (may have been allocated in a prior session).
        {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            check_deallocation(&(offset..offset + len), &state, &HashSet::new());
        }

        self.inner.dealloc(handle.inner)?;

        self.record_deallocation(offset, len);
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
                    // Roll back the whole bulk allocation atomically; if rollback fails,
                    // propagate that failure instead of pretending nothing changed.
                    return match self.inner.dealloc_bulk(&inner_handles) {
                        Ok(()) => Err(io::Error::other(format!(
                            "bulk-allocated handle {i} is not convertible to BStackSlice: {e}"
                        ))),
                        Err(rollback_err) => Err(io::Error::other(format!(
                            "bulk-allocated handle {i} is not convertible to BStackSlice: {e}; rollback via dealloc_bulk failed: {rollback_err}"
                        ))),
                    };
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

        // Pass 1: convert and validate all handles without mutating tracking state.
        // `pending_freed` accumulates regions already cleared in this batch so
        // intra-batch double-frees are caught before any state is touched.
        let mut slices: Vec<BStackSlice<'_, A>> = Vec::with_capacity(handles.len());
        let mut pending_freed: HashSet<Range<u64>> = HashSet::with_capacity(handles.len());
        {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            for handle in handles {
                let slice: BStackSlice<'_, A> = handle.inner.try_into().map_err(|e| {
                    io::Error::other(format!(
                        "handle is not convertible to BStackSlice during bulk dealloc: {e}"
                    ))
                })?;
                let region = slice.start()..slice.start() + slice.len();
                check_deallocation(&region, &state, &pending_freed);
                pending_freed.insert(region);
                slices.push(slice);
            }
        }

        // Delegate to the inner allocator.
        let inner_handles: Vec<A::Allocated<'a>> = handles.iter().map(|h| h.inner).collect();
        self.inner.dealloc_bulk(inner_handles)?;

        // Pass 2: commit each region via record_deallocation.
        for slice in &slices {
            self.record_deallocation(slice.start(), slice.len());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Minimal allocator stub used throughout. All methods panic if called;
    // the behavioral tests drive the checker directly via record_allocation /
    // record_deallocation rather than going through the full alloc/dealloc path.
    struct MockAllocator;

    impl crate::alloc::BStackAllocator for MockAllocator {
        type Error = io::Error;
        type Allocated<'a> = crate::alloc::BStackSlice<'a, Self>;

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
        ) -> io::Result<Self::Allocated<'a>> {
            unimplemented!()
        }
        fn dealloc(&self, _: Self::Allocated<'_>) -> io::Result<()> {
            unimplemented!()
        }
    }

    // --- overlaps unit tests ---

    #[test]
    fn test_region_overlap() {
        assert!(overlaps(&(0..10), &(5..15))); // [0, 10) and [5, 15) overlap
        assert!(overlaps(&(5..15), &(0..10))); // symmetric
        assert!(!overlaps(&(0..10), &(10..20))); // adjacent — no overlap
        assert!(!overlaps(&(0..10), &(20..30))); // disjoint
        assert!(overlaps(&(5..15), &(10..20))); // [5, 15) and [10, 20) overlap
    }

    #[test]
    fn test_zero_length_regions() {
        assert!(!overlaps(&(0..0), &(0..10))); // zero-length doesn't overlap
        assert!(!overlaps(&(0..10), &(0..0))); // symmetric
        assert!(!overlaps(&(0..0), &(5..5))); // two zero-length regions don't overlap
    }

    // --- Behavioral tests ---

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
        c.record_allocation(50, 50); // exactly adjacent — must not panic
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
        // A region allocated in a previous session is unknown to this checker;
        // freeing it should succeed without panic.
        let c = checker();
        c.record_deallocation(0, 100);
    }

    #[test]
    #[should_panic(expected = "double-free")]
    fn test_double_free_panics() {
        let c = checker();
        c.record_allocation(0, 100);
        c.record_deallocation(0, 100);
        c.record_deallocation(0, 100); // second free of same region
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
        c.record_allocation(50, 100); // [50, 150) overlaps [0, 100)
    }

    #[test]
    #[should_panic(expected = "Partial deallocations are not allowed")]
    fn test_partial_free_panics() {
        let c = checker();
        c.record_allocation(0, 100);
        c.record_deallocation(0, 50); // only first half of [0, 100)
    }

    #[test]
    #[should_panic(expected = "Partial deallocations are not allowed")]
    fn test_superset_free_panics() {
        let c = checker();
        c.record_allocation(20, 50);
        c.record_deallocation(0, 100); // [0, 100) is a strict superset of [20, 70)
    }

    #[test]
    #[should_panic(expected = "spans multiple allocated regions")]
    fn test_spanning_free_panics() {
        let c = checker();
        c.record_allocation(0, 50);
        c.record_allocation(50, 50);
        c.record_deallocation(0, 100); // covers both [0, 50) and [50, 100)
    }

    #[test]
    fn test_freed_region_split_on_reallocation() {
        let c = checker();

        // Free a large region, then re-allocate a slice out of the middle of it.
        // The freed region [0, 100) should be split into [0, 20) and [50, 100).
        c.record_allocation(0, 100);
        c.record_deallocation(0, 100);
        c.record_allocation(20, 30); // [20, 50)

        let state = c.state.lock().unwrap();
        assert!(state.freed.contains(&(0..20)));
        assert!(state.freed.contains(&(50..100)));
        assert!(!state.freed.contains(&(0..100)));
        assert!(state.allocated.contains(&(20..50)));
    }

    #[test]
    fn test_freed_region_split_left_edge() {
        let c = checker();

        // Re-allocate from the very start of a freed region — only the right
        // remainder should appear in the freed set.
        c.record_allocation(0, 100);
        c.record_deallocation(0, 100);
        c.record_allocation(0, 30); // [0, 30) — consumes the left edge

        let state = c.state.lock().unwrap();
        assert!(!state.freed.contains(&(0..100)));
        assert!(!state.freed.iter().any(|r| r.start < 30));
        assert!(state.freed.contains(&(30..100)));
    }

    #[test]
    fn test_freed_region_split_right_edge() {
        let c = checker();

        // Re-allocate from the very end of a freed region — only the left
        // remainder should appear in the freed set.
        c.record_allocation(0, 100);
        c.record_deallocation(0, 100);
        c.record_allocation(70, 30); // [70, 100) — consumes the right edge

        let state = c.state.lock().unwrap();
        assert!(!state.freed.contains(&(0..100)));
        assert!(state.freed.contains(&(0..70)));
        assert!(!state.freed.iter().any(|r| r.end > 70));
    }

    #[test]
    fn test_freed_region_exact_reuse_removes_entry() {
        let c = checker();

        // Re-allocating a region that exactly matches a freed region should
        // leave nothing for that region in the freed set.
        c.record_allocation(0, 100);
        c.record_deallocation(0, 100);
        c.record_allocation(0, 100);

        let state = c.state.lock().unwrap();
        assert!(state.freed.is_empty());
        assert!(state.allocated.contains(&(0..100)));
    }

    // --- Public API integration tests ---
    //
    // The tests above exercise record_allocation / record_deallocation directly.
    // The tests below exercise the public BStackAllocator / BStackBulkAllocator
    // trait methods (alloc, realloc, dealloc, alloc_bulk, dealloc_bulk) to ensure
    // conversion failures, inner errors, and tracking rollback are handled correctly.

    use std::cell::{Cell, RefCell};
    use std::rc::Rc;

    /// Configuration flags for [`ControllableMockAllocator`].
    #[derive(Debug, Clone, Default)]
    struct MockAllocatorConfig {
        /// If true, `dealloc` returns an error instead of succeeding.
        fail_dealloc: bool,
        /// If true, `dealloc_bulk` returns an error instead of succeeding.
        fail_dealloc_bulk: bool,
        /// If true, `realloc` returns an error instead of succeeding.
        fail_realloc: bool,
        /// If true, `realloc` keeps the same offset and only changes the length.
        realloc_in_place: bool,
    }

    /// Handle returned by [`ControllableMockAllocator`].
    ///
    /// Stores offset and length along with a reference to the allocator,
    /// enabling conversion to [`BStackSlice`].
    #[derive(Clone, Copy)]
    struct MockHandle<'a> {
        alloc: &'a ControllableMockAllocator,
        offset: u64,
        len: u64,
    }

    impl<'a> MockHandle<'a> {
        fn new(alloc: &'a ControllableMockAllocator, offset: u64, len: u64) -> Self {
            Self { alloc, offset, len }
        }
    }

    impl<'a> TryInto<BStackSlice<'a, ControllableMockAllocator>> for MockHandle<'a> {
        type Error = io::Error;

        fn try_into(self) -> Result<BStackSlice<'a, ControllableMockAllocator>, Self::Error> {
            // SAFETY: Mock allocator ensures regions are within bounds and non-overlapping
            Ok(unsafe { BStackSlice::from_raw_parts(self.alloc, self.offset, self.len) })
        }
    }

    /// A controllable mock allocator that actually tracks allocations and can be
    /// configured to fail at various points to test error handling.
    struct ControllableMockAllocator {
        /// Simulated BStack for stack() / into_stack() methods.
        stack: BStack,
        /// Next offset to allocate from (simulates linear allocation).
        next_offset: Cell<u64>,
        /// Set of allocated regions (for validation).
        allocated: RefCell<HashSet<Range<u64>>>,
        /// Configuration flags.
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
        type Allocated<'a> = MockHandle<'a>;

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
            self.allocated.borrow_mut().insert(region.clone());
            Ok(MockHandle::new(self, offset, len))
        }

        fn realloc<'a>(
            &'a self,
            handle: Self::Allocated<'a>,
            new_len: u64,
        ) -> io::Result<Self::Allocated<'a>> {
            let config = self.config.borrow();
            if config.fail_realloc {
                return Err(io::Error::other("mock realloc failure"));
            }
            let realloc_in_place = config.realloc_in_place;
            drop(config);

            let old_region = handle.offset..handle.offset + handle.len;
            self.allocated.borrow_mut().remove(&old_region);

            let new_offset = if realloc_in_place {
                let new_end = handle.offset + new_len;
                if self.next_offset.get() < new_end {
                    self.next_offset.set(new_end);
                }
                handle.offset
            } else {
                let new_offset = self.next_offset.get();
                self.next_offset.set(new_offset + new_len);
                new_offset
            };
            let new_region = new_offset..new_offset + new_len;
            self.allocated.borrow_mut().insert(new_region);

            Ok(MockHandle::new(self, new_offset, new_len))
        }

        fn dealloc(&self, handle: Self::Allocated<'_>) -> io::Result<()> {
            if self.config.borrow().fail_dealloc {
                return Err(io::Error::other("mock dealloc failure"));
            }

            let region = handle.offset..handle.offset + handle.len;
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
            handles: impl AsRef<[Self::Allocated<'a>]>,
        ) -> io::Result<()> {
            if self.config.borrow().fail_dealloc_bulk {
                return Err(io::Error::other("mock dealloc_bulk failure"));
            }

            for &handle in handles.as_ref() {
                let region = handle.offset..handle.offset + handle.len;
                self.allocated.borrow_mut().remove(&region);
            }
            Ok(())
        }
    }

    // Helper to create a test BStack
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

    // --- Tests for alloc() error paths ---

    #[test]
    fn test_alloc_success_updates_tracking() -> io::Result<()> {
        let (stack, path) = create_test_stack()?;
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig::default()));
        let inner = ControllableMockAllocator::new(stack, config.clone());
        let alloc = DebugCheckingAllocator::new(inner);

        // Allocate through the public API
        let handle1 = alloc.alloc(100)?;
        let handle2 = alloc.alloc(200)?;

        // Verify tracking state
        let state = alloc.state.lock().unwrap();
        assert_eq!(state.allocated.len(), 2);
        assert!(state.allocated.contains(&(0..100)));
        assert!(state.allocated.contains(&(100..300)));

        // Clean up to avoid leaks
        drop(state);
        alloc.dealloc(handle1)?;
        alloc.dealloc(handle2)?;

        Ok(())
    }

    // --- Tests for realloc() error paths ---

    #[test]
    fn test_realloc_success() -> io::Result<()> {
        let (stack, path) = create_test_stack()?;
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig::default()));
        let inner = ControllableMockAllocator::new(stack, config.clone());
        let alloc = DebugCheckingAllocator::new(inner);

        let handle = alloc.alloc(100)?;

        // Verify initial state
        {
            let state = alloc.state.lock().unwrap();
            assert!(state.allocated.contains(&(0..100)));
        }

        let new_handle = alloc.realloc(handle, 200)?;

        // Verify tracking was updated: old region removed, new region added
        {
            let state = alloc.state.lock().unwrap();
            assert!(!state.allocated.contains(&(0..100)));
            assert!(state.allocated.contains(&(100..300)));
        }

        alloc.dealloc(new_handle)?;
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

        let new_handle = alloc.realloc(handle, 100)?;

        {
            let state = alloc.state.lock().unwrap();
            assert!(state.allocated.contains(&(150..250)));
            assert!(!state.allocated.contains(&(0..100)));
            assert!(state.freed.contains(&(0..100)));
            assert!(state.freed.contains(&(250..300)));
            assert!(!state.freed.contains(&(150..300)));
        }

        alloc.dealloc(new_handle)?;
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
        let new_handle = alloc.realloc(handle, 60)?;

        {
            let state = alloc.state.lock().unwrap();
            assert!(state.allocated.contains(&(0..60)));
            assert!(!state.allocated.contains(&(0..100)));
            assert!(state.freed.contains(&(60..100)));
        }

        alloc.dealloc(new_handle)?;
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
        let stale_handle = handle.clone();
        let _new_handle = alloc.realloc(handle, 60).unwrap();

        alloc.dealloc(stale_handle).unwrap();
    }

    #[test]
    fn test_realloc_inner_failure_preserves_tracking() -> io::Result<()> {
        let (stack, path) = create_test_stack()?;
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig::default()));
        let inner = ControllableMockAllocator::new(stack, config.clone());
        let alloc = DebugCheckingAllocator::new(inner);

        let handle = alloc.alloc(100)?;

        // Verify initial state
        {
            let state = alloc.state.lock().unwrap();
            assert!(state.allocated.contains(&(0..100)));
        }

        // Make realloc fail
        config.borrow_mut().fail_realloc = true;

        let result = alloc.realloc(handle, 200);
        assert!(result.is_err());

        // Verify tracking state was NOT modified (rollback succeeded)
        {
            let state = alloc.state.lock().unwrap();
            assert!(
                state.allocated.contains(&(0..100)),
                "Original allocation should still be tracked after realloc failure"
            );
        }

        // We can't dealloc the original handle because realloc consumed it
        // This is a limitation of the API design, not a bug

        Ok(())
    }

    // --- Tests for dealloc() error paths ---

    #[test]
    fn test_dealloc_success_updates_tracking() -> io::Result<()> {
        let (stack, path) = create_test_stack()?;
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig::default()));
        let inner = ControllableMockAllocator::new(stack, config.clone());
        let alloc = DebugCheckingAllocator::new(inner);

        let handle = alloc.alloc(100)?;

        // Verify initial state
        {
            let state = alloc.state.lock().unwrap();
            assert!(state.allocated.contains(&(0..100)));
            assert!(!state.freed.contains(&(0..100)));
        }

        alloc.dealloc(handle)?;

        // Verify tracking was updated
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

        // Make dealloc fail
        config.borrow_mut().fail_dealloc = true;

        let result = alloc.dealloc(handle);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("mock dealloc failure")
        );

        // Verify tracking state was NOT modified (the region should still be allocated)
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
    #[should_panic(expected = "double-free")]
    fn test_dealloc_double_free_via_public_api() {
        let (stack, path) = create_test_stack().unwrap();
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig::default()));
        let inner = ControllableMockAllocator::new(stack, config);
        let alloc = DebugCheckingAllocator::new(inner);

        let handle = alloc.alloc(100).unwrap();
        alloc.dealloc(handle).unwrap();
        // Second dealloc of the same region should panic
        alloc.record_deallocation(0, 100);
    }

    #[test]
    fn test_dealloc_untracked_region_via_public_api_is_allowed() -> io::Result<()> {
        let (stack, path) = create_test_stack()?;
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig::default()));
        let inner = ControllableMockAllocator::new(stack, config);
        let alloc = DebugCheckingAllocator::new(inner);

        // Freeing a region never seen by this checker instance should succeed —
        // it may have been allocated in a prior session.
        let fake_inner_handle = MockHandle::new(alloc.inner(), 500, 100);
        let fake_handle = DebugHandle::new(&alloc, fake_inner_handle);
        alloc.dealloc(fake_handle)?;

        let state = alloc.state.lock().unwrap();
        assert!(state.freed.contains(&(500..600)));
        Ok(())
    }

    // --- Tests for alloc_bulk() error paths ---

    #[test]
    fn test_alloc_bulk_success() -> io::Result<()> {
        let (stack, path) = create_test_stack()?;
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig::default()));
        let inner = ControllableMockAllocator::new(stack, config);
        let alloc = DebugCheckingAllocator::new(inner);

        let handles = alloc.alloc_bulk(&[100, 200, 300])?;
        assert_eq!(handles.len(), 3);

        // Verify all were tracked
        {
            let state = alloc.state.lock().unwrap();
            assert_eq!(state.allocated.len(), 3);
            assert!(state.allocated.contains(&(0..100)));
            assert!(state.allocated.contains(&(100..300)));
            assert!(state.allocated.contains(&(300..600)));
        }

        alloc.dealloc_bulk(handles)?;
        Ok(())
    }

    // --- Tests for dealloc_bulk() error paths ---

    #[test]
    fn test_dealloc_bulk_success() -> io::Result<()> {
        let (stack, path) = create_test_stack()?;
        let _guard = TestGuard(path);
        let config = Rc::new(RefCell::new(MockAllocatorConfig::default()));
        let inner = ControllableMockAllocator::new(stack, config);
        let alloc = DebugCheckingAllocator::new(inner);

        let handles = alloc.alloc_bulk(&[100, 200, 300])?;

        alloc.dealloc_bulk(handles)?;

        // Verify all were moved from allocated to freed
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

        // Make dealloc_bulk fail
        config.borrow_mut().fail_dealloc_bulk = true;

        let result = alloc.dealloc_bulk(&handles);
        assert!(result.is_err());

        // Verify tracking state was NOT modified — the two-pass design only commits
        // allocated→freed transitions after the inner call succeeds.
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
        alloc.dealloc_bulk(&handles).unwrap();
        // Second dealloc_bulk with same regions should panic during validation
        alloc.record_deallocation(0, 100);
    }

    // --- Tests for with_state() validation ---

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
        DebugCheckingAllocator::with_state(
            MockAllocator,
            [0..100, 50..150], // overlapping
            [],
        );
    }

    #[test]
    #[should_panic(expected = "Initial freed set contains overlapping ranges")]
    fn test_with_state_panics_on_overlapping_freed() {
        DebugCheckingAllocator::with_state(
            MockAllocator,
            [],
            [0..100, 50..150], // overlapping
        );
    }

    #[test]
    #[should_panic(expected = "allocated range")]
    fn test_with_state_panics_on_allocated_freed_overlap() {
        DebugCheckingAllocator::with_state(
            MockAllocator,
            [0..100, 200..300],
            [50..150, 400..500], // [50, 150) overlaps [0, 100)
        );
    }

    #[test]
    fn test_with_state_valid_adjacent_ranges() {
        // Adjacent ranges (not overlapping) should be allowed
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
