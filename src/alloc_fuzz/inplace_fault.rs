//! Fault-injection fuzz for in-place resize
//! ([`BStackInPlaceResizeAllocator`](crate::alloc::BStackInPlaceResizeAllocator)).
//!
//! Arms a random [`FaultPolicy`] around each `alloc`/`realloc_inplace`/`dealloc`
//! and asserts the failure contract under torn multi-call operations, the way
//! [`init_fault`](super::init_fault) does for `realloc`:
//!
//! * every operation succeeds or fails cleanly (an error, never a panic);
//! * `realloc_inplace` never commits a *moved* region, so a returned handle is
//!   always the untouched original — verified byte-for-byte at its original
//!   `(start, len)` — and a `None` handle is a genuine (acceptable) leak;
//! * a fault leaves at most a leak, never corruption: after **reopen +
//!   recovery** every still-live allocation reads back byte-for-byte.
//!
//! The structural in-place paths — a segregated tail extend / shrink-reclaim, a
//! first-fit front carve or free-neighbour merge — are exactly the ones a crash
//! can tear, so this is where their leak-preferring recovery is exercised. A
//! clean `Unsupported`/`InvalidInput` rejection is distinguished from an injected
//! fault by error kind and does not force a reopen.

use super::common::{
    FuzzConfig, Guard, Operation, gen_inplace_deltas, gen_op, make_allocator,
    policies::{RandomFaults, per_mille},
    temp_path, verify_pattern, write_pattern,
};
use super::inplace::apply_ok;
use crate::BStack;
use crate::alloc::{
    BStackInPlaceResizeAllocator, BStackOwnedSlice, BStackOwnedSliceAllocator, BStackRange,
    FirstFitBStackAllocator, GhostTreeBstackAllocator, SegregatedBStackAllocator,
};
use crate::fault::FaultPolicy;
use rand::{RngExt, SeedableRng, rngs::StdRng};
use std::io;
use std::sync::Arc;

/// Disarm, reopen (running recovery), and re-verify every live allocation.
/// Panics on any data mismatch — that is the corruption signal.
fn reopen_and_verify<A, F>(
    alloc: A,
    make: &F,
    live: &[(BStackRange, u64)],
    bias: u64,
    ctx: &str,
) -> A
where
    A: BStackOwnedSliceAllocator + BStackInPlaceResizeAllocator,
    F: Fn(BStack) -> io::Result<A>,
{
    alloc.stack().set_fault_policy(None);
    let stack = alloc.into_stack();
    let alloc = make(stack).unwrap();
    for (i, (range, id)) in live.iter().enumerate() {
        let s = unsafe { BStackOwnedSlice::from_raw_parts(&alloc, range.start(), range.len()) };
        verify_pattern(&s, *id, bias, &format!("{ctx} rec{i}"));
    }
    alloc
}

fn run_inplace_fault_fuzz<A, F>(make: F, seed_salt: u64)
where
    A: BStackOwnedSliceAllocator + BStackInPlaceResizeAllocator,
    F: Fn(BStack) -> io::Result<A>,
{
    let cfg = FuzzConfig::from_env();
    let path = temp_path("ip_fault");
    let _guard = Guard(path.clone());
    // Reproducible: `BSTACK_FUZZ_SEED=<n>` replays an identical run.
    let master_seed = std::env::var("BSTACK_FUZZ_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| rand::rng().random_range(0..=u64::MAX));
    eprintln!("[alloc_fuzz::inplace_fault salt={seed_salt:#06x}] BSTACK_FUZZ_SEED={master_seed}");
    let mut rng = StdRng::seed_from_u64(master_seed ^ seed_salt);
    let bias = rng.random_range(0..=u64::MAX);
    let fault_seed = rng.random_range(0..=u64::MAX);
    let policy: Arc<dyn FaultPolicy> = Arc::new(RandomFaults::new(fault_seed, per_mille()));

    let mut alloc = make(BStack::open(&path).unwrap()).unwrap();
    let mut live: Vec<(BStackRange, u64)> = Vec::new();
    let mut next_id = 0u64;

    for step in 0..cfg.ops {
        // The op faulted iff it returned an *injected* error (not a clean
        // Unsupported/InvalidInput rejection); a fault may have left the
        // allocator mid-mutation, so we reopen (recover) before continuing.
        let mut faulted = false;

        match gen_op(&mut rng, &cfg, !live.is_empty(), false) {
            Operation::Alloc(len) => {
                alloc.stack().set_fault_policy(Some(policy.clone()));
                let r = alloc.alloc(len);
                alloc.stack().set_fault_policy(None);
                match r {
                    Ok(mut s) => {
                        let id = next_id;
                        next_id += 1;
                        write_pattern(&mut s, id, bias).unwrap();
                        live.push((s.as_range(), id));
                    }
                    Err(_) => faulted = true,
                }
            }
            Operation::Realloc(_) => {
                let i = rng.random_range(0..live.len());
                let (range, id) = live.swap_remove(i);
                let (start, len) = (range.start(), range.len());
                let s = unsafe { BStackOwnedSlice::from_raw_parts(&alloc, start, len) };
                let (prepend, append) = gen_inplace_deltas(&mut rng, len, &cfg);
                alloc.stack().set_fault_policy(Some(policy.clone()));
                let r = alloc.realloc_inplace(s, prepend, append);
                alloc.stack().set_fault_policy(None);
                match r {
                    Ok(s2) => {
                        if let Some(s2) = apply_ok(
                            s2,
                            start,
                            len,
                            prepend,
                            append,
                            id,
                            bias,
                            "fault inplace: resize",
                        ) {
                            live.push((s2.as_range(), id));
                        }
                    }
                    Err(e) => {
                        let kind = e.source.kind();
                        if matches!(
                            kind,
                            io::ErrorKind::Unsupported | io::ErrorKind::InvalidInput
                        ) {
                            // Clean rejection, not a fault: original returned intact.
                            let h = e.handle.expect("inplace rejection must return the handle");
                            assert_eq!(h.start(), start, "inplace rejection: start preserved");
                            assert_eq!(h.len(), len, "inplace rejection: len preserved");
                            verify_pattern(&h, id, bias, "inplace rejection: original intact");
                            live.push((h.as_range(), id));
                        } else {
                            // Injected fault. realloc_inplace never commits a moved
                            // region, so a returned handle is the untouched original.
                            faulted = true;
                            if let Some(h) = e.handle {
                                assert_eq!(h.start(), start, "fault inplace: survivor is original");
                                assert_eq!(h.len(), len, "fault inplace: survivor is original");
                                verify_pattern(&h, id, bias, "fault inplace: original intact");
                                live.push((h.as_range(), id));
                            }
                            // None → region genuinely lost; drop it.
                        }
                    }
                }
            }
            Operation::Dealloc => {
                let i = rng.random_range(0..live.len());
                let (range, id) = live.swap_remove(i);
                let s =
                    unsafe { BStackOwnedSlice::from_raw_parts(&alloc, range.start(), range.len()) };
                alloc.stack().set_fault_policy(Some(policy.clone()));
                let r = alloc.dealloc(s);
                alloc.stack().set_fault_policy(None);
                match r {
                    Ok(()) => {}
                    Err(e) => {
                        faulted = true;
                        if let Some(h) = e.handle {
                            // Free failed, region still live and unchanged.
                            verify_pattern(&h, id, bias, "fault inplace dealloc err: retained");
                            live.push((h.as_range(), id));
                        }
                        // None → region lost/leaked; drop it.
                    }
                }
            }
            Operation::Check => {
                let i = rng.random_range(0..live.len());
                let (range, id) = &live[i];
                let s =
                    unsafe { BStackOwnedSlice::from_raw_parts(&alloc, range.start(), range.len()) };
                verify_pattern(&s, *id, bias, "fault inplace: check");
            }
            Operation::Reopen => {}
        }

        let periodic = cfg.reopen_every > 0 && step > 0 && step % cfg.reopen_every == 0;
        if faulted || periodic {
            alloc = reopen_and_verify(alloc, &make, &live, bias, &format!("inplace reopen@{step}"));
        }
    }

    // Final integrity pass.
    let _alloc = reopen_and_verify(alloc, &make, &live, bias, "inplace final");
}

macro_rules! inplace_fault_suite {
    ($mod_name:ident, $make:expr, $salt:expr) => {
        mod $mod_name {
            use super::*;
            #[test]
            fn fault_fuzz() {
                super::run_inplace_fault_fuzz($make, $salt);
            }
        }
    };
}

inplace_fault_suite!(first_fit, make_allocator!(FirstFitBStackAllocator), 0x1B1B);
inplace_fault_suite!(
    ghost_tree,
    make_allocator!(GhostTreeBstackAllocator),
    0x2B2B
);
inplace_fault_suite!(
    segregated,
    make_allocator!(SegregatedBStackAllocator),
    0x7B7B
);
