#![cfg(all(test, feature = "alloc", feature = "set"))]

mod alloc_fuzz_tests {
    use crate::alloc::{
        BStackOwnedSlice, BStackOwnedSliceAllocator, BStackRange, FirstFitBStackAllocator,
        GhostTreeBstackAllocator, SlabBStackAllocator,
    };
    use crate::{BStack, CheckedSlabBStackAllocator};
    use rand::RngExt;
    use std::sync::atomic::{AtomicU64, Ordering};

    const FUZZ_COUNT: usize = 10000;
    const SESSIONS: usize = 20;
    const OPS_PER_SESSION: usize = 100;

    // ── shared helpers ────────────────────────────────────────────────────────

    struct Guard(std::path::PathBuf);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn temp_path(prefix: &str) -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        std::env::temp_dir().join(format!("bstack_fuzz_{prefix}_{pid}_{id}.bin"))
    }

    /// Fill `buf` with a deterministic pattern derived from `id`.
    fn fill(buf: &mut [u8], id: u64) {
        for (i, b) in buf.iter_mut().enumerate() {
            *b = ((id >> ((i % 8) * 8)) & 0xFF) as u8;
        }
    }

    /// Assert that `buf` matches the pattern for `id`.
    fn check(buf: &[u8], id: u64, ctx: &str) {
        for (i, &b) in buf.iter().enumerate() {
            assert_eq!(
                b,
                ((id >> ((i % 8) * 8)) & 0xFF) as u8,
                "{ctx}: corruption at [{i}]"
            );
        }
    }

    fn write_id<A: BStackOwnedSliceAllocator>(s: &mut BStackOwnedSlice<'_, A>, id: u64) {
        let mut buf = vec![0u8; s.len() as usize];
        fill(&mut buf, id);
        s.write(&buf).unwrap();
    }

    fn verify_id<A: BStackOwnedSliceAllocator>(s: &BStackOwnedSlice<'_, A>, id: u64, ctx: &str) {
        check(&s.read().unwrap(), id, ctx);
    }

    // ── generic fuzz runners ──────────────────────────────────────────────────

    fn run_alloc_dealloc<A, F>(make: F)
    where
        A: BStackOwnedSliceAllocator,
        F: Fn(BStack) -> std::io::Result<A>,
    {
        let path = temp_path("ad");
        let _guard = Guard(path.clone());
        let alloc = make(BStack::open(&path).unwrap()).unwrap();
        let mut rng = rand::rng();
        let mut live = Vec::new();

        for _ in 0..FUZZ_COUNT {
            if rng.random_bool(0.7) || live.is_empty() {
                let size = rng.random_range(0..=1024);
                if let Ok(mut s) = alloc.alloc(size) {
                    let id = live.len() as u64;
                    write_id(&mut s, id);
                    live.push((s, id));
                }
            } else {
                let i = rng.random_range(0..live.len());
                let (s, id) = live.swap_remove(i);
                verify_id(&s, id, &format!("dealloc {id}"));
                alloc.dealloc(s).unwrap();
            }
        }
    }

    fn run_alloc_realloc_dealloc<A, F>(make: F)
    where
        A: BStackOwnedSliceAllocator,
        F: Fn(BStack) -> std::io::Result<A>,
    {
        let path = temp_path("ard");
        let _guard = Guard(path.clone());
        let alloc = make(BStack::open(&path).unwrap()).unwrap();
        let mut rng = rand::rng();
        let mut live = Vec::new();

        for _ in 0..FUZZ_COUNT {
            if rng.random_bool(0.6) || live.is_empty() {
                let size = rng.random_range(0..=1024);
                if let Ok(mut s) = alloc.alloc(size) {
                    let id = live.len() as u64;
                    write_id(&mut s, id);
                    live.push((s, id));
                }
            } else {
                let i = rng.random_range(0..live.len());
                let (s, id) = live.swap_remove(i);
                if rng.random_bool(0.8) {
                    let new_size = rng.random_range(0..=1024);
                    let old_len = s.len();
                    if let Ok(mut s2) = alloc.realloc(s, new_size) {
                        let buf = s2.read().unwrap();
                        let overlap = old_len.min(new_size) as usize;
                        check(&buf[..overlap], id, &format!("realloc {id} overlap"));
                        for (j, &b) in buf.iter().enumerate().skip(overlap) {
                            assert_eq!(b, 0, "realloc {id}: non-zero at new byte [{j}]");
                        }
                        write_id(&mut s2, id);
                        live.push((s2, id));
                    }
                } else {
                    verify_id(&s, id, &format!("dealloc {id}"));
                    alloc.dealloc(s).unwrap();
                }
            }
        }
    }

    fn run_reopen<A, F>(make: F)
    where
        A: BStackOwnedSliceAllocator,
        F: Fn(BStack) -> std::io::Result<A>,
    {
        let path = temp_path("reopen");
        let _guard = Guard(path.clone());
        drop(make(BStack::open(&path).unwrap()).unwrap());

        let mut rng = rand::rng();
        let mut live: Vec<(BStackRange, u64)> = Vec::new();
        let mut next_id: u64 = 1;

        for session in 0..SESSIONS {
            let alloc = make(BStack::open(&path).unwrap()).unwrap();

            for (i, &(range, id)) in live.iter().enumerate() {
                let s =
                    unsafe { BStackOwnedSlice::from_raw_parts(&alloc, range.start(), range.len()) };
                check(
                    &s.read().unwrap()[..range.len() as usize],
                    id,
                    &format!("s{session} rec{i}"),
                );
                // Drop s without dealloc (no-op Drop).
            }

            for _ in 0..OPS_PER_SESSION {
                let choice = if live.is_empty() {
                    0
                } else {
                    rng.random_range(0u32..4)
                };
                match choice {
                    0 => {
                        let len = rng.random_range(0..=512);
                        if let Ok(mut s) = alloc.alloc(len) {
                            let id = next_id;
                            next_id += 1;
                            write_id(&mut s, id);
                            live.push((BStackRange::new(s.start(), len), id));
                        }
                    }
                    1 => {
                        let i = rng.random_range(0..live.len());
                        let (range, id) = live[i];
                        let new_len = rng.random_range(0..=512);
                        let s = unsafe {
                            BStackOwnedSlice::from_raw_parts(&alloc, range.start(), range.len())
                        };
                        if let Ok(mut s2) = alloc.realloc(s, new_len) {
                            let overlap = range.len().min(new_len) as usize;
                            check(
                                &s2.read().unwrap()[..overlap],
                                id,
                                &format!("s{session} realloc{i}"),
                            );
                            let new_id = next_id;
                            next_id += 1;
                            write_id(&mut s2, new_id);
                            live[i] = (BStackRange::new(s2.start(), new_len), new_id);
                        }
                    }
                    2 => {
                        let i = rng.random_range(0..live.len());
                        let (range, id) = live.swap_remove(i);
                        let s = unsafe {
                            BStackOwnedSlice::from_raw_parts(&alloc, range.start(), range.len())
                        };
                        check(
                            &s.read().unwrap()[..range.len() as usize],
                            id,
                            &format!("s{session} dealloc{i}"),
                        );
                        alloc.dealloc(s).unwrap();
                    }
                    _ => {
                        let i = rng.random_range(0..live.len());
                        let (range, id) = live[i];
                        let s = unsafe {
                            BStackOwnedSlice::from_raw_parts(&alloc, range.start(), range.len())
                        };
                        check(
                            &s.read().unwrap()[..range.len() as usize],
                            id,
                            &format!("s{session} verify{i}"),
                        );
                        // No dealloc — Drop is no-op.
                    }
                }
            }

            drop(alloc.into_stack());
        }
    }

    // Note: This is not required by the API, but it is a common edge case that zero-size allocations
    // should be handled gracefully and not cause panics or corruption. We test this explicitly to
    // ensure that all allocators provided by the crate itself handle zero-size allocs correctly.
    fn run_zero_size_alloc<A, F>(make: F)
    where
        A: BStackOwnedSliceAllocator,
        F: Fn(BStack) -> std::io::Result<A>,
    {
        let path = temp_path("zalloc");
        let _guard = Guard(path.clone());
        let alloc = make(BStack::open(&path).unwrap()).unwrap();

        let mut slices = Vec::new();
        for _ in 0..FUZZ_COUNT {
            let s = alloc.alloc(0).unwrap();
            assert_eq!(s.len(), 0, "zero alloc must have len 0");
            assert_eq!(s.start(), 0, "zero alloc must have start 0");
            slices.push(s);
        }
        // All zero slices are identical by (start, len); dealloc each is a no-op.
        for s in slices {
            alloc.dealloc(s).unwrap();
        }
    }

    // Similar to the above, this tests that interleaving zero-size alloc/dealloc pairs with real
    // allocations does not cause any corruption or panics.
    fn run_free_zero_slices<A, F>(make: F)
    where
        A: BStackOwnedSliceAllocator,
        F: Fn(BStack) -> std::io::Result<A>,
    {
        let path = temp_path("fzero");
        let _guard = Guard(path.clone());
        let alloc = make(BStack::open(&path).unwrap()).unwrap();
        let mut rng = rand::rng();

        // Interleave real allocations with zero-length alloc/dealloc pairs to
        // verify that zero slices never disturb the allocator state.
        let mut live = Vec::new();
        for i in 0..FUZZ_COUNT {
            let zero = alloc.alloc(0).unwrap();
            alloc.dealloc(zero).unwrap();

            if rng.random_bool(0.5) || live.is_empty() {
                let mut s = alloc.alloc(rng.random_range(16..=256)).unwrap();
                write_id(&mut s, i as u64);
                live.push((s, i as u64));
            } else {
                let idx = rng.random_range(0..live.len());
                let (s, id) = live.swap_remove(idx);
                verify_id(&s, id, "free_zero_slices");
                alloc.dealloc(s).unwrap();
            }
        }
    }

    // This tests that attempting to free the same slice twice results in an error rather than panicking
    // or corrupting the allocator state. We test this by allocating three adjacent slices, freeing the
    // middle one, then reconstructing a handle to the same region via from_raw_parts and freeing it again.
    //
    // It is beneficial for allocators provided by this crate to handle this case gracefully and return
    // an error rather than panic or corrupt state, so we test it explicitly.
    fn run_double_free_error<A, F>(make: F)
    where
        A: BStackOwnedSliceAllocator,
        F: Fn(BStack) -> std::io::Result<A>,
    {
        let path = temp_path("dfree");
        let _guard = Guard(path.clone());
        let alloc = make(BStack::open(&path).unwrap()).unwrap();

        // Allocate a non-tail block by sandwiching it between two others.
        let before = alloc.alloc(64).unwrap();
        let target = alloc.alloc(64).unwrap();
        let after = alloc.alloc(64).unwrap();

        let (start, len) = (target.start(), target.len());
        alloc.dealloc(target).unwrap();

        // Reconstruct a handle to the same region and free it a second time.
        let again = unsafe { BStackOwnedSlice::from_raw_parts(&alloc, start, len) };
        let result = alloc.dealloc(again);
        assert!(result.is_err(), "double-free must return an error");

        alloc.dealloc(before).unwrap();
        alloc.dealloc(after).unwrap();
    }

    // ── test suite macro ──────────────────────────────────────────────────────

    macro_rules! fuzz_suite {
        ($mod_name:ident, $make:expr) => {
            mod $mod_name {
                use super::*;
                #[test]
                fn alloc_dealloc() {
                    super::run_alloc_dealloc($make);
                }
                #[test]
                fn alloc_realloc_dealloc() {
                    super::run_alloc_realloc_dealloc($make);
                }
                #[test]
                fn reopen() {
                    super::run_reopen($make);
                }
                #[test]
                fn zero_size_alloc() {
                    super::run_zero_size_alloc($make);
                }
                #[test]
                fn free_zero_slices() {
                    super::run_free_zero_slices($make);
                }
            }
        };
    }

    fuzz_suite!(first_fit, FirstFitBStackAllocator::new);
    fuzz_suite!(ghost_tree, GhostTreeBstackAllocator::new);
    fuzz_suite!(slab_8, |bs: BStack| {
        if bs.is_empty().unwrap() {
            SlabBStackAllocator::new(bs, 8)
        } else {
            SlabBStackAllocator::open(bs)
        }
    });
    fuzz_suite!(slab_16, |bs: BStack| {
        if bs.is_empty().unwrap() {
            SlabBStackAllocator::new(bs, 16)
        } else {
            SlabBStackAllocator::open(bs)
        }
    });
    fuzz_suite!(slab_64, |bs: BStack| {
        if bs.is_empty().unwrap() {
            SlabBStackAllocator::new(bs, 64)
        } else {
            SlabBStackAllocator::open(bs)
        }
    });
    fuzz_suite!(check_slab_16, |bs: BStack| {
        if bs.is_empty().unwrap() {
            CheckedSlabBStackAllocator::new(bs, 16)
        } else {
            CheckedSlabBStackAllocator::open(bs)
        }
    });
    fuzz_suite!(check_slab_64, |bs: BStack| {
        if bs.is_empty().unwrap() {
            CheckedSlabBStackAllocator::new(bs, 64)
        } else {
            CheckedSlabBStackAllocator::open(bs)
        }
    });

    mod double_free {
        use super::*;
        #[test]
        fn first_fit() {
            super::run_double_free_error(FirstFitBStackAllocator::new);
        }

        #[test]
        fn check_slab_16() {
            super::run_double_free_error(|bs: BStack| {
                if bs.is_empty().unwrap() {
                    CheckedSlabBStackAllocator::new(bs, 16)
                } else {
                    CheckedSlabBStackAllocator::open(bs)
                }
            });
        }
    }
}
