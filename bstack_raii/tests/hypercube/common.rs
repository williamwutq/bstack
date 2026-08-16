//! Shared test harness: a temp-file `BStack`, allocator constructors (FirstFit =
//! non-bulk, GhostTree = bulk), and the leak-freedom oracle.
#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};

use bstack::{BStack, FirstFitBStackAllocator, GhostTreeBstackAllocator};
use bstack_raii::{BStackDrop, BStackRaiiAllocator};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A uniquely named temp `.bstack` file, removed on drop.
pub struct TempStack {
    pub path: std::path::PathBuf,
}

impl TempStack {
    pub fn new() -> Self {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut path = std::env::temp_dir();
        path.push(format!("bstack_raii_hc_{}_{n}.bstack", std::process::id()));
        let _ = std::fs::remove_file(&path);
        TempStack { path }
    }

    pub fn open(&self) -> BStack {
        BStack::open(&self.path).unwrap()
    }

    /// The default allocator (FirstFit): exercises the *sequential* fallback of
    /// `alloc_many` / `free_many`.
    pub fn allocator(&self) -> FirstFitBStackAllocator {
        FirstFitBStackAllocator::new(self.open()).unwrap()
    }

    /// A GhostTree allocator: the bstack-provided allocator that implements
    /// `BStackBulkAllocator`, exercising the *atomic-bulk* path.
    pub fn ghost_allocator(&self) -> GhostTreeBstackAllocator {
        GhostTreeBstackAllocator::new(self.open()).unwrap()
    }
}

impl Drop for TempStack {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// The leak-freedom oracle — the single most important invariant, applicable to
/// *every* cell: tearing a structure down reclaims all of it (no leaked child
/// blocks), including deep grandchildren, so it doubles as a recursion check.
///
/// `build` constructs the structure fresh each call. We build+tear down once to warm
/// and size the persistent WAL block (which stays allocated by design), snapshot the
/// baseline, then build+tear down the *identical* structure again and assert the
/// stack returned exactly to baseline — so the constant WAL overhead cancels and
/// only a real leak shows.
pub fn assert_teardown_reclaims<A, T>(alloc: &A, mut build: impl FnMut() -> T)
where
    A: BStackRaiiAllocator,
    T: BStackDrop,
{
    build().bstack_drop(alloc).unwrap();
    let base = alloc.stack().len().unwrap();
    build().bstack_drop(alloc).unwrap();
    assert_eq!(
        alloc.stack().len().unwrap(),
        base,
        "teardown leaked (non-recursive?)"
    );
}
