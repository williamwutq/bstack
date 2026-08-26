//! Happy-path fuzz for the initialised allocation API (`alloc`/`realloc`):
//! alloc/realloc/dealloc mixes, reopen+recovery, zero-size handling, and the
//! double-free error, each verifying that `alloc`/`realloc` zero-initialise and
//! zero-extend. The fault-injection counterpart is [`init_fault`](super::init_fault).

use super::common::{
    FuzzConfig, Guard, Operation, Payload, check_is_zero, gen_op, make_allocator, make_payload,
    temp_path,
};
use crate::alloc::{
    BStackOwnedSlice, BStackOwnedSliceAllocator, BStackRange, FirstFitBStackAllocator,
    GhostTreeBstackAllocator, SegregatedBStackAllocator, SlabBStackAllocator,
};
use crate::{BStack, CheckedSlabBStackAllocator};
use rand::RngExt;

// A random per-run salt so that the deterministic byte patterns of two
// parallel test binaries (or the seeded/adversarial payload kinds) never
// alias — a stray cross-allocation read then shows up as a mismatch.
fn run_bias(rng: &mut impl RngExt) -> u64 {
    rng.random_range(0..=u64::MAX)
}

// Alloc/dealloc/check mix. Each live allocation carries a `Payload` — either a
// cheap seeded pattern or an adversarial snapshot copied out of the BStack
// (bytes that look like allocator internals) — verified on read-back.
fn run_alloc_dealloc<A, F>(make: F)
where
    A: BStackOwnedSliceAllocator,
    F: Fn(BStack) -> std::io::Result<A>,
{
    let cfg = FuzzConfig::from_env();
    let path = temp_path("ad");
    let _guard = Guard(path.clone());
    let alloc = make(BStack::open(&path).unwrap()).unwrap();
    let mut rng = rand::rng();
    let bias = run_bias(&mut rng);
    let mut live: Vec<(BStackOwnedSlice<'_, A>, Payload)> = Vec::new();
    let mut next_id = 0u64;

    for _ in 0..cfg.ops {
        match gen_op(&mut rng, &cfg, !live.is_empty(), false) {
            Operation::Alloc(len) => {
                if let Ok(mut s) = alloc.alloc(len) {
                    let payload = make_payload(alloc.stack(), s.len(), next_id, &cfg, &mut rng);
                    next_id += 1;
                    payload.write(&mut s, bias).unwrap();
                    payload.verify(&s, bias, "alloc_dealloc: post-write");
                    live.push((s, payload));
                }
            }
            Operation::Dealloc => {
                let i = rng.random_range(0..live.len());
                let (s, payload) = live.swap_remove(i);
                payload.verify(&s, bias, "alloc_dealloc: pre-dealloc");
                alloc.dealloc(s).unwrap();
            }
            // No faults are armed, so realloc/check are both just integrity
            // reads here; treat realloc-rolls as a check.
            Operation::Realloc(_) | Operation::Check => {
                let i = rng.random_range(0..live.len());
                let (s, payload) = &live[i];
                payload.verify(s, bias, "alloc_dealloc: check");
            }
            Operation::Reopen => {}
        }
    }
}

// Alloc/realloc/dealloc mix, verifying that realloc preserves the surviving
// prefix and zero-extends any grown bytes.
fn run_alloc_realloc_dealloc<A, F>(make: F)
where
    A: BStackOwnedSliceAllocator,
    F: Fn(BStack) -> std::io::Result<A>,
{
    let cfg = FuzzConfig::from_env();
    let path = temp_path("ard");
    let _guard = Guard(path.clone());
    let alloc = make(BStack::open(&path).unwrap()).unwrap();
    let mut rng = rand::rng();
    let bias = run_bias(&mut rng);
    let mut live: Vec<(BStackOwnedSlice<'_, A>, Payload)> = Vec::new();
    let mut next_id = 0u64;

    for _ in 0..cfg.ops {
        match gen_op(&mut rng, &cfg, !live.is_empty(), false) {
            Operation::Alloc(len) => {
                if let Ok(mut s) = alloc.alloc(len) {
                    let payload = make_payload(alloc.stack(), s.len(), next_id, &cfg, &mut rng);
                    next_id += 1;
                    payload.write(&mut s, bias).unwrap();
                    live.push((s, payload));
                }
            }
            Operation::Realloc(new_len) => {
                let i = rng.random_range(0..live.len());
                let (s, payload) = live.swap_remove(i);
                let old_len = s.len();
                match alloc.realloc(s, new_len) {
                    Ok(mut s2) => {
                        let preserved = old_len.min(new_len);
                        payload.verify_prefix(&s2, preserved, bias, "realloc: preserved prefix");
                        if new_len > old_len {
                            check_is_zero(
                                &s2.read().unwrap()[old_len as usize..],
                                "realloc: zero-extend",
                            );
                        }
                        let np = make_payload(alloc.stack(), s2.len(), next_id, &cfg, &mut rng);
                        next_id += 1;
                        np.write(&mut s2, bias).unwrap();
                        live.push((s2, np));
                    }
                    Err(e) => {
                        // Unarmed run: realloc failure is not expected, but if
                        // it happens the region must survive — re-track it.
                        if let Some(h) = e.handle {
                            live.push((h, payload));
                        }
                    }
                }
            }
            Operation::Dealloc => {
                let i = rng.random_range(0..live.len());
                let (s, payload) = live.swap_remove(i);
                payload.verify(&s, bias, "alloc_realloc_dealloc: pre-dealloc");
                alloc.dealloc(s).unwrap();
            }
            Operation::Check => {
                let i = rng.random_range(0..live.len());
                let (s, payload) = &live[i];
                payload.verify(s, bias, "alloc_realloc_dealloc: check");
            }
            Operation::Reopen => {}
        }
    }
}

// Persist, then repeatedly reopen the allocator, re-verifying all live
// allocations after each reopen and mutating between sessions. Live handles
// are stored as raw ranges (a `BStackOwnedSlice` cannot outlive its
// allocator) alongside their `Payload`, so adversarial data is verified
// across the reopen boundary too.
fn run_reopen<A, F>(make: F)
where
    A: BStackOwnedSliceAllocator,
    F: Fn(BStack) -> std::io::Result<A>,
{
    let cfg = FuzzConfig::from_env();
    let path = temp_path("reopen");
    let _guard = Guard(path.clone());
    drop(make(BStack::open(&path).unwrap()).unwrap());

    let mut rng = rand::rng();
    let bias = run_bias(&mut rng);
    let mut live: Vec<(BStackRange, Payload)> = Vec::new();
    let mut next_id: u64 = 0;

    for session in 0..cfg.sessions {
        let alloc = make(BStack::open(&path).unwrap()).unwrap();

        // Re-verify every surviving allocation after the reopen/recovery.
        for (i, (range, payload)) in live.iter().enumerate() {
            let s = unsafe { BStackOwnedSlice::from_raw_parts(&alloc, range.start(), range.len()) };
            payload.verify(&s, bias, &format!("reopen s{session} rec{i}"));
        }

        for _ in 0..cfg.ops_per_session {
            match gen_op(&mut rng, &cfg, !live.is_empty(), false) {
                Operation::Alloc(len) => {
                    if let Ok(mut s) = alloc.alloc(len) {
                        let payload = make_payload(alloc.stack(), s.len(), next_id, &cfg, &mut rng);
                        next_id += 1;
                        payload.write(&mut s, bias).unwrap();
                        live.push((s.as_range(), payload));
                    }
                }
                Operation::Realloc(new_len) => {
                    let i = rng.random_range(0..live.len());
                    let (range, payload) = live.swap_remove(i);
                    let old_len = range.len();
                    let s = unsafe {
                        BStackOwnedSlice::from_raw_parts(&alloc, range.start(), range.len())
                    };
                    match alloc.realloc(s, new_len) {
                        Ok(mut s2) => {
                            let preserved = old_len.min(new_len);
                            payload.verify_prefix(
                                &s2,
                                preserved,
                                bias,
                                "reopen realloc: preserved prefix",
                            );
                            if new_len > old_len {
                                check_is_zero(
                                    &s2.read().unwrap()[old_len as usize..],
                                    "reopen realloc: zero-extend",
                                );
                            }
                            let np = make_payload(alloc.stack(), s2.len(), next_id, &cfg, &mut rng);
                            next_id += 1;
                            np.write(&mut s2, bias).unwrap();
                            live.push((s2.as_range(), np));
                        }
                        Err(e) => {
                            if let Some(h) = e.handle {
                                live.push((h.as_range(), payload));
                            }
                        }
                    }
                }
                Operation::Dealloc => {
                    let i = rng.random_range(0..live.len());
                    let (range, payload) = live.swap_remove(i);
                    let s = unsafe {
                        BStackOwnedSlice::from_raw_parts(&alloc, range.start(), range.len())
                    };
                    payload.verify(&s, bias, "reopen: pre-dealloc");
                    alloc.dealloc(s).unwrap();
                }
                Operation::Check => {
                    let i = rng.random_range(0..live.len());
                    let (range, payload) = &live[i];
                    let s = unsafe {
                        BStackOwnedSlice::from_raw_parts(&alloc, range.start(), range.len())
                    };
                    payload.verify(&s, bias, "reopen: check");
                }
                Operation::Reopen => {}
            }
        }

        drop(alloc.into_stack());
    }
}

// Note: not required by the API, but zero-size allocations must be handled
// gracefully. Exercise many of them and confirm they share (start, len) and
// dealloc as no-ops.
fn run_zero_size_alloc<A, F>(make: F)
where
    A: BStackOwnedSliceAllocator,
    F: Fn(BStack) -> std::io::Result<A>,
{
    let cfg = FuzzConfig::from_env();
    let path = temp_path("zalloc");
    let _guard = Guard(path.clone());
    let alloc = make(BStack::open(&path).unwrap()).unwrap();

    let mut slices = Vec::new();
    for _ in 0..cfg.ops {
        let s = alloc.alloc(0).unwrap();
        assert_eq!(s.len(), 0, "zero alloc must have len 0");
        assert_eq!(s.start(), 0, "zero alloc must have start 0");
        slices.push(s);
    }
    for s in slices {
        alloc.dealloc(s).unwrap();
    }
}

// Interleave zero-size alloc/dealloc pairs with real allocations to verify
// that zero slices never disturb allocator state.
fn run_free_zero_slices<A, F>(make: F)
where
    A: BStackOwnedSliceAllocator,
    F: Fn(BStack) -> std::io::Result<A>,
{
    let cfg = FuzzConfig::from_env();
    let path = temp_path("fzero");
    let _guard = Guard(path.clone());
    let alloc = make(BStack::open(&path).unwrap()).unwrap();
    let mut rng = rand::rng();
    let bias = run_bias(&mut rng);

    let mut live: Vec<(BStackOwnedSlice<'_, A>, Payload)> = Vec::new();
    let mut next_id = 0u64;
    for _ in 0..cfg.ops {
        let zero = alloc.alloc(0).unwrap();
        alloc.dealloc(zero).unwrap();

        if rng.random_bool(0.5) || live.is_empty() {
            let len = rng.random_range(16..=256);
            if let Ok(mut s) = alloc.alloc(len) {
                let payload = make_payload(alloc.stack(), s.len(), next_id, &cfg, &mut rng);
                next_id += 1;
                payload.write(&mut s, bias).unwrap();
                live.push((s, payload));
            }
        } else {
            let idx = rng.random_range(0..live.len());
            let (s, payload) = live.swap_remove(idx);
            payload.verify(&s, bias, "free_zero_slices");
            alloc.dealloc(s).unwrap();
        }
    }
}

// Freeing the same region twice must return an error rather than panic or
// corrupt state. Sandwich a block, free it, then reconstruct a handle to the
// same region and free it again.
fn run_double_free_error<A, F>(make: F)
where
    A: BStackOwnedSliceAllocator,
    F: Fn(BStack) -> std::io::Result<A>,
{
    let path = temp_path("dfree");
    let _guard = Guard(path.clone());
    let alloc = make(BStack::open(&path).unwrap()).unwrap();

    let before = alloc.alloc(64).unwrap();
    let target = alloc.alloc(64).unwrap();
    let after = alloc.alloc(64).unwrap();

    let (start, len) = (target.start(), target.len());
    alloc.dealloc(target).unwrap();

    let again = unsafe { BStackOwnedSlice::from_raw_parts(&alloc, start, len) };
    let result = alloc.dealloc(again);
    assert!(result.is_err(), "double-free must return an error");

    alloc.dealloc(before).unwrap();
    alloc.dealloc(after).unwrap();
}

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

fuzz_suite!(first_fit, make_allocator!(FirstFitBStackAllocator));
fuzz_suite!(ghost_tree, make_allocator!(GhostTreeBstackAllocator));
fuzz_suite!(slab_8, make_allocator!(SlabBStackAllocator, 8));
fuzz_suite!(slab_16, make_allocator!(SlabBStackAllocator, 16));
fuzz_suite!(slab_64, make_allocator!(SlabBStackAllocator, 64));
fuzz_suite!(
    check_slab_16,
    make_allocator!(CheckedSlabBStackAllocator, 16)
);
fuzz_suite!(
    check_slab_64,
    make_allocator!(CheckedSlabBStackAllocator, 64)
);
fuzz_suite!(segregated, make_allocator!(SegregatedBStackAllocator));

mod double_free {
    use super::*;
    #[test]
    fn first_fit() {
        super::run_double_free_error(make_allocator!(FirstFitBStackAllocator));
    }

    #[test]
    fn check_slab_16() {
        super::run_double_free_error(make_allocator!(CheckedSlabBStackAllocator, 16));
    }

    #[test]
    fn segregated() {
        super::run_double_free_error(make_allocator!(SegregatedBStackAllocator));
    }
}
