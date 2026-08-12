//! [`alloc_many`] / [`free_many`]: multi-block allocation and free with
//! all-or-nothing rollback on the allocation side.
//!
//! ## Why these are sequential, not atomic bulk
//!
//! bstack provides atomic [`BStackBulkAllocator::alloc_bulk`] /
//! [`dealloc_bulk`](bstack::BStackBulkAllocator::dealloc_bulk), but only some
//! allocators implement it, and **all** of `bstack_raii` is generic over
//! `A: BStackRaiiAllocator`. On stable Rust a generic function cannot
//! dispatch on whether its concrete `A` *also* implements `BStackBulkAllocator`:
//! trait-method selection happens once, at the generic definition site, where the
//! extra bound is unprovable — so any "prefer bulk when available" shim (autoref
//! specialization included) collapses to the sequential path in generic code, and
//! `min_specialization` is nightly-only. Requiring the bulk bound instead would
//! exclude `FirstFit` (and every current test), so these helpers stay sequential:
//!
//! * each individual `alloc` / `dealloc` **is** crash-atomic (allocator contract);
//! * the *set* is not — a crash mid-sequence orphans the blocks done so far (a
//!   leak, never a torn structure), which the WAL layer reclaims.
//!
//! A caller that statically knows its allocator is bulk-capable can still call
//! `alloc_bulk` / `dealloc_bulk` directly for atomicity.

use std::io;

use bstack::BStackRange;

use crate::BStackRaiiAllocator;
use crate::teardown::dealloc_range;

/// Allocate one block per entry in `sizes`, in order. On any failure the blocks
/// already allocated are freed (reverse order) before the error is returned, so a
/// partial allocation never leaks within the call.
pub fn alloc_many<A: BStackRaiiAllocator>(
    allocator: &A,
    sizes: &[u64],
) -> io::Result<Vec<BStackRange>> {
    let mut out = Vec::with_capacity(sizes.len());
    for &size in sizes {
        match allocator.alloc(size) {
            Ok(slice) => out.push(slice.as_range()),
            Err(e) => {
                for r in out.into_iter().rev() {
                    // SAFETY: our own fresh, unshared allocations.
                    let _ = unsafe { dealloc_range(allocator, r) };
                }
                return Err(e);
            }
        }
    }
    Ok(out)
}

/// Free every range in turn. Stops and propagates on the first error (the
/// remaining ranges are left allocated for the caller to handle).
pub fn free_many<A: BStackRaiiAllocator>(
    allocator: &A,
    ranges: impl IntoIterator<Item = BStackRange>,
) -> io::Result<()> {
    for r in ranges {
        // SAFETY: the caller's contract, as for `dealloc_range`.
        unsafe { dealloc_range(allocator, r)? };
    }
    Ok(())
}
