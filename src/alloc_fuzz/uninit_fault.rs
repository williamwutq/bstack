//! Fault-injection fuzz for the uninitialised allocation API
//! ([`BStackUninitAllocator`](crate::alloc::BStackUninitAllocator)).
//!
//! Arms a random [`FaultPolicy`] around each `alloc_uninit`/`realloc_uninit`/
//! `dealloc` and asserts the same failure contract [`init_fault`](super::init_fault)
//! checks for the initialised API — every operation succeeds or fails cleanly, a
//! returned handle is the untouched original or the fully-committed new region,
//! and a fault leaves at most a leak (never corruption) so every live allocation
//! reads back byte-for-byte after reopen + recovery.
//!
//! It differs from [`init_fault`](super::init_fault) only in the two places the
//! scrubs would have mattered: `realloc_uninit` does not zero-fill grown bytes,
//! so the driver makes no zero-extension assertion and overwrites the whole
//! region with a fresh payload before re-verifying.

use super::common::{
    FuzzConfig, Guard, Operation, Payload, gen_op, make_allocator, make_payload,
    policies::{RandomFaults, per_mille},
    temp_path,
};
use crate::alloc::{
    BStackOwnedSlice, BStackOwnedSliceAllocator, BStackRange, BStackUninitAllocator,
    FirstFitBStackAllocator, GhostTreeBstackAllocator, SegregatedBStackAllocator,
    SlabBStackAllocator,
};
use crate::fault::FaultPolicy;
use crate::{BStack, CheckedSlabBStackAllocator};
use rand::{RngExt, SeedableRng, rngs::StdRng};
use std::io;
use std::sync::Arc;

/// Disarm, reopen (running recovery), and re-verify every live allocation.
/// Panics on any data mismatch — that is the corruption signal.
fn reopen_and_verify<A, F>(
    alloc: A,
    make: &F,
    live: &[(BStackRange, Payload)],
    bias: u64,
    ctx: &str,
) -> A
where
    A: BStackOwnedSliceAllocator + BStackUninitAllocator,
    F: Fn(BStack) -> io::Result<A>,
{
    alloc.stack().set_fault_policy(None);
    let stack = alloc.into_stack();
    let alloc = make(stack).unwrap();
    for (i, (range, payload)) in live.iter().enumerate() {
        let s = unsafe { BStackOwnedSlice::from_raw_parts(&alloc, range.start(), range.len()) };
        payload.verify(&s, bias, &format!("{ctx} rec{i}"));
    }
    alloc
}

fn run_uninit_fault_fuzz<A, F>(make: F, seed_salt: u64)
where
    A: BStackOwnedSliceAllocator + BStackUninitAllocator,
    F: Fn(BStack) -> io::Result<A>,
{
    let cfg = FuzzConfig::from_env();
    let path = temp_path("u_fault");
    let _guard = Guard(path.clone());
    // Reproducible: `BSTACK_FUZZ_SEED=<n>` replays an identical run.
    let master_seed = std::env::var("BSTACK_FUZZ_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| rand::rng().random_range(0..=u64::MAX));
    eprintln!("[alloc_fuzz::uninit_fault salt={seed_salt:#06x}] BSTACK_FUZZ_SEED={master_seed}");
    let mut rng = StdRng::seed_from_u64(master_seed ^ seed_salt);
    let bias = rng.random_range(0..=u64::MAX);
    let fault_seed = rng.random_range(0..=u64::MAX);
    let policy: Arc<dyn FaultPolicy> = Arc::new(RandomFaults::new(fault_seed, per_mille()));

    let mut alloc = make(BStack::open(&path).unwrap()).unwrap();
    let mut live: Vec<(BStackRange, Payload)> = Vec::new();
    let mut next_id = 0u64;

    for step in 0..cfg.ops {
        // The op faulted iff it returned an error; a fault may have left the
        // allocator mid-mutation, so we reopen (recover) before continuing.
        let mut faulted = false;

        match gen_op(&mut rng, &cfg, !live.is_empty(), false) {
            Operation::Alloc(len) => {
                alloc.stack().set_fault_policy(Some(policy.clone()));
                let r = alloc.alloc_uninit(len);
                alloc.stack().set_fault_policy(None);
                match r {
                    Ok(mut s) => {
                        let payload = make_payload(alloc.stack(), s.len(), next_id, &cfg, &mut rng);
                        next_id += 1;
                        payload.write(&mut s, bias).unwrap();
                        live.push((s.as_range(), payload));
                    }
                    Err(_) => faulted = true,
                }
            }
            Operation::Realloc(new_len) => {
                let i = rng.random_range(0..live.len());
                let (range, payload) = live.swap_remove(i);
                let old_len = range.len();
                let s =
                    unsafe { BStackOwnedSlice::from_raw_parts(&alloc, range.start(), range.len()) };
                alloc.stack().set_fault_policy(Some(policy.clone()));
                let r = alloc.realloc_uninit(s, new_len);
                alloc.stack().set_fault_policy(None);
                match r {
                    Ok(mut s2) => {
                        let preserved = old_len.min(new_len);
                        payload.verify_prefix(
                            &s2,
                            preserved,
                            bias,
                            "uninit fault realloc: preserved prefix",
                        );
                        // Grown bytes unspecified — overwrite, do not check.
                        let np = make_payload(alloc.stack(), s2.len(), next_id, &cfg, &mut rng);
                        next_id += 1;
                        np.write(&mut s2, bias).unwrap();
                        live.push((s2.as_range(), np));
                    }
                    Err(e) => {
                        faulted = true;
                        if let Some(mut h) = e.handle {
                            // Strict contract: a returned handle is either the
                            // untouched original (old_len, byte-identical) or the
                            // fully-committed new region (new_len). The length
                            // tells us which; the surviving prefix is preserved
                            // either way. Grown bytes on a committed new region
                            // are unspecified (uninit), so only the prefix is
                            // verified.
                            if h.len() == old_len {
                                payload.verify(
                                    &h,
                                    bias,
                                    "uninit fault realloc err: untouched original",
                                );
                            } else {
                                let preserved = old_len.min(new_len);
                                payload.verify_prefix(
                                    &h,
                                    preserved,
                                    bias,
                                    "uninit fault realloc err: committed-new prefix",
                                );
                            }
                            let np = make_payload(alloc.stack(), h.len(), next_id, &cfg, &mut rng);
                            next_id += 1;
                            np.write(&mut h, bias).unwrap();
                            live.push((h.as_range(), np));
                        }
                        // None → region genuinely lost; drop it.
                    }
                }
            }
            Operation::Dealloc => {
                let i = rng.random_range(0..live.len());
                let (range, payload) = live.swap_remove(i);
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
                            payload.verify(&h, bias, "uninit fault dealloc err: retained");
                            live.push((h.as_range(), payload));
                        }
                        // None → region lost/leaked; drop it.
                    }
                }
            }
            Operation::Check => {
                let i = rng.random_range(0..live.len());
                let (range, payload) = &live[i];
                let s =
                    unsafe { BStackOwnedSlice::from_raw_parts(&alloc, range.start(), range.len()) };
                payload.verify(&s, bias, "uninit fault: check");
            }
            Operation::Reopen => {}
        }

        let periodic = cfg.reopen_every > 0 && step > 0 && step % cfg.reopen_every == 0;
        if faulted || periodic {
            alloc = reopen_and_verify(alloc, &make, &live, bias, &format!("uninit reopen@{step}"));
        }
    }

    // Final integrity pass.
    let _alloc = reopen_and_verify(alloc, &make, &live, bias, "uninit final");
}

macro_rules! uninit_fault_suite {
    ($mod_name:ident, $make:expr, $salt:expr) => {
        mod $mod_name {
            use super::*;
            #[test]
            fn fault_fuzz() {
                super::run_uninit_fault_fuzz($make, $salt);
            }
        }
    };
}

uninit_fault_suite!(first_fit, make_allocator!(FirstFitBStackAllocator), 0x1A1A);
uninit_fault_suite!(
    ghost_tree,
    make_allocator!(GhostTreeBstackAllocator),
    0x2A2A
);
uninit_fault_suite!(slab_16, make_allocator!(SlabBStackAllocator, 16), 0x3A3A);
uninit_fault_suite!(slab_64, make_allocator!(SlabBStackAllocator, 64), 0x4A4A);
uninit_fault_suite!(
    check_slab_16,
    make_allocator!(CheckedSlabBStackAllocator, 16),
    0x5A5A
);
uninit_fault_suite!(
    check_slab_64,
    make_allocator!(CheckedSlabBStackAllocator, 64),
    0x6A6A
);
uninit_fault_suite!(
    segregated,
    make_allocator!(SegregatedBStackAllocator),
    0x7A7A
);
