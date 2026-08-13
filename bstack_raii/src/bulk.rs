//! Sequential fallbacks for [`BStackRaiiAllocator::alloc_many`] /
//! [`BStackRaiiAllocator::free_many`].
//!
//! ## How bulk dispatch works
//!
//! bstack provides atomic [`BStackBulkAllocator::alloc_bulk`] /
//! [`dealloc_bulk`](bstack::BStackBulkAllocator::dealloc_bulk), but only some
//! allocators implement it, and **all** of `bstack_raii` is generic over
//! `A: BStackRaiiAllocator`. On stable Rust a generic function cannot dispatch on
//! whether its concrete `A` *also* implements `BStackBulkAllocator` (that needs the
//! nightly `specialization` feature; autoref specialization collapses to the
//! fallback in generic code), so we don't try. Instead `alloc_many` / `free_many`
//! are **provided methods on [`BStackRaiiAllocator`]**: the default bodies are these
//! sequential helpers, and each bulk-capable allocator *overrides* them to call
//! `alloc_bulk` / `dealloc_bulk` (see the impls in [`crate::wal`]). Ordinary trait
//! dispatch then picks the override at monomorphization — the fast path flows
//! through generic code, and non-bulk allocators (e.g. `FirstFit`) keep the
//! sequential path — with no bound leaking onto the public API.
//!
//! Atomicity of the fallback:
//!
//! * each individual `alloc` / `dealloc` **is** crash-atomic (allocator contract);
//! * the *set* is not — a crash mid-sequence orphans the blocks done so far (a
//!   leak, never a torn structure), which the WAL layer reclaims.

use std::io;

use bstack::BStackRange;

use crate::BStackRaiiAllocator;
use crate::teardown::dealloc_range;

/// Sequential fallback for [`BStackRaiiAllocator::alloc_many`]: allocate one block
/// per entry in `sizes`, in order. On any failure the blocks already allocated are
/// freed (reverse order) before the error is returned, so a partial allocation
/// never leaks within the call.
pub(crate) fn seq_alloc_many<A: BStackRaiiAllocator>(
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

/// Sequential fallback for [`BStackRaiiAllocator::free_many`]: free every range in
/// turn. Stops and propagates on the first error (the remaining ranges are left
/// allocated for the caller to handle).
pub(crate) fn seq_free_many<A: BStackRaiiAllocator>(
    allocator: &A,
    ranges: impl IntoIterator<Item = BStackRange>,
) -> io::Result<()> {
    for r in ranges {
        // SAFETY: the caller's contract, as for `dealloc_range`.
        unsafe { dealloc_range(allocator, r)? };
    }
    Ok(())
}
