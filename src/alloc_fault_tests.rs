#![cfg(all(
    test,
    debug_assertions,
    feature = "fault-injection",
    feature = "alloc",
    feature = "set"
))]

//! Fault-injection fuzz for the crash-consistent allocators.
//!
//! Where [`alloc_fuzz_tests`](crate::alloc_fuzz_tests) exercises the happy path,
//! this suite arms a random [`FaultPolicy`] around each `alloc`/`realloc`/
//! `dealloc` and asserts the 0.4.0 failure contract holds under torn
//! multi-call operations:
//!
//! * every operation either **succeeds** or **fails cleanly** (returns an error,
//!   never panics);
//! * on a failed `realloc`/`dealloc` the [`BStackAllocError`] handle contract is
//!   honoured — `Some` hands back a live region whose data is intact, `None`
//!   means the region was genuinely lost (a leak, acceptable per the contract);
//! * a fault leaves at most a leak, never corruption: after **reopen +
//!   recovery**, every still-live allocation reads back byte-for-byte, including
//!   adversarial payloads that look like allocator internals.
//!
//! Because the crash-consistency model repairs state only at `open`, the driver
//! reopens (which runs each allocator's recovery) after *every* faulted
//! operation before continuing — modelling "crash → restart → recover" — and
//! also on a fixed period. [`LinearBStackAllocator`](crate::LinearBStackAllocator)
//! is intentionally excluded: it keeps no metadata and has no recovery path.

mod alloc_fault_tests {
    use crate::alloc::{
        BStackOwnedSlice, BStackOwnedSliceAllocator, BStackRange, FirstFitBStackAllocator,
        GhostTreeBstackAllocator, SlabBStackAllocator,
    };
    use crate::alloc_test_common::{
        FuzzConfig, Guard, Operation, Payload, check_is_zero, gen_op, make_allocator, make_payload,
        temp_path,
    };
    use crate::fault::FaultPolicy;
    use crate::{BStack, CheckedSlabBStackAllocator};
    use rand::RngExt;
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Fails a pseudo-random subset of consultations at rate `per_mille`
    /// (faults per thousand), deterministically from `seed` and an internal
    /// counter. The counter is the policy's own — not the stack's `seq` — so it
    /// survives the disarm/re-arm the driver performs around every operation,
    /// keeping the schedule reproducible across arming boundaries.
    struct RandomFaults {
        seed: u64,
        per_mille: u64,
        counter: AtomicU64,
    }

    impl RandomFaults {
        fn new(seed: u64, per_mille: u64) -> Self {
            Self {
                seed,
                per_mille,
                counter: AtomicU64::new(0),
            }
        }
    }

    impl FaultPolicy for RandomFaults {
        fn next_fault(&self, op: &'static str, _seq: u64) -> Option<io::Error> {
            let n = self.counter.fetch_add(1, Ordering::Relaxed);
            // splitmix64 hash of (seed, n) → uniform u64.
            let mut z = self
                .seed
                .wrapping_add(n.wrapping_mul(0x9E37_79B9_7F4A_7C15));
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            (z % 1000 < self.per_mille)
                .then(|| io::Error::other(format!("injected fault at {op} (n={n})")))
        }
    }

    fn per_mille() -> u64 {
        std::env::var("BSTACK_FAULT_PER_MILLE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30)
    }

    /// Disarm, reopen (running recovery), and re-verify every live allocation.
    /// Returns the reopened allocator. Panics on any data mismatch — that is the
    /// corruption signal.
    fn reopen_and_verify<A, F>(
        alloc: A,
        make: &F,
        live: &[(BStackRange, Payload)],
        bias: u64,
        ctx: &str,
    ) -> A
    where
        A: BStackOwnedSliceAllocator,
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

    fn run_fault_fuzz<A, F>(make: F, seed_salt: u64)
    where
        A: BStackOwnedSliceAllocator,
        F: Fn(BStack) -> io::Result<A>,
    {
        let cfg = FuzzConfig::from_env();
        let path = temp_path("fault");
        let _guard = Guard(path.clone());
        let mut rng = rand::rng();
        let bias = rng.random_range(0..=u64::MAX);
        let seed = rng.random_range(0..=u64::MAX) ^ seed_salt;
        let policy: Arc<dyn FaultPolicy> = Arc::new(RandomFaults::new(seed, per_mille()));

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
                    let r = alloc.alloc(len);
                    alloc.stack().set_fault_policy(None);
                    match r {
                        Ok(mut s) => {
                            let payload =
                                make_payload(alloc.stack(), s.len(), next_id, &cfg, &mut rng);
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
                    let s = unsafe {
                        BStackOwnedSlice::from_raw_parts(&alloc, range.start(), range.len())
                    };
                    alloc.stack().set_fault_policy(Some(policy.clone()));
                    let r = alloc.realloc(s, new_len);
                    alloc.stack().set_fault_policy(None);
                    match r {
                        Ok(mut s2) => {
                            let preserved = old_len.min(new_len);
                            payload.verify_prefix(
                                &s2,
                                preserved,
                                bias,
                                "fault realloc: preserved prefix",
                            );
                            if new_len > old_len {
                                check_is_zero(
                                    &s2.read().unwrap()[old_len as usize..],
                                    "fault realloc: zero-extend",
                                );
                            }
                            let np = make_payload(alloc.stack(), s2.len(), next_id, &cfg, &mut rng);
                            next_id += 1;
                            np.write(&mut s2, bias).unwrap();
                            live.push((s2.as_range(), np));
                        }
                        Err(e) => {
                            faulted = true;
                            if let Some(mut h) = e.handle {
                                // The survivor is either the untouched original
                                // (old_len) or the committed new region (new_len);
                                // either way its first min(old,len) bytes are the
                                // old data. Verify that, then normalise tracking by
                                // writing a fresh full-length payload.
                                let vlen = old_len.min(h.len());
                                payload.verify_prefix(
                                    &h,
                                    vlen,
                                    bias,
                                    "fault realloc err: surviving prefix",
                                );
                                let np =
                                    make_payload(alloc.stack(), h.len(), next_id, &cfg, &mut rng);
                                next_id += 1;
                                np.write(&mut h, bias).unwrap();
                                live.push((h.as_range(), np));
                            }
                            // e.handle == None → region genuinely lost; drop it.
                        }
                    }
                }
                Operation::Dealloc => {
                    let i = rng.random_range(0..live.len());
                    let (range, payload) = live.swap_remove(i);
                    let s = unsafe {
                        BStackOwnedSlice::from_raw_parts(&alloc, range.start(), range.len())
                    };
                    alloc.stack().set_fault_policy(Some(policy.clone()));
                    let r = alloc.dealloc(s);
                    alloc.stack().set_fault_policy(None);
                    match r {
                        Ok(()) => {}
                        Err(e) => {
                            faulted = true;
                            if let Some(h) = e.handle {
                                // Free failed, region still live and unchanged.
                                payload.verify(&h, bias, "fault dealloc err: retained");
                                live.push((h.as_range(), payload));
                            }
                            // None → region lost/leaked; drop it.
                        }
                    }
                }
                Operation::Check => {
                    let i = rng.random_range(0..live.len());
                    let (range, payload) = &live[i];
                    let s = unsafe {
                        BStackOwnedSlice::from_raw_parts(&alloc, range.start(), range.len())
                    };
                    payload.verify(&s, bias, "fault: check");
                }
                Operation::Reopen => {}
            }

            let periodic = cfg.reopen_every > 0 && step > 0 && step % cfg.reopen_every == 0;
            if faulted || periodic {
                alloc = reopen_and_verify(alloc, &make, &live, bias, &format!("reopen@{step}"));
            }
        }

        // Final integrity pass.
        let _alloc = reopen_and_verify(alloc, &make, &live, bias, "final");
    }

    macro_rules! fault_suite {
        ($mod_name:ident, $make:expr, $salt:expr) => {
            mod $mod_name {
                use super::*;
                #[test]
                fn fault_fuzz() {
                    super::run_fault_fuzz($make, $salt);
                }
            }
        };
    }

    fault_suite!(first_fit, make_allocator!(FirstFitBStackAllocator), 0x1111);
    fault_suite!(
        ghost_tree,
        make_allocator!(GhostTreeBstackAllocator),
        0x2222
    );
    fault_suite!(slab_16, make_allocator!(SlabBStackAllocator, 16), 0x3333);
    fault_suite!(slab_64, make_allocator!(SlabBStackAllocator, 64), 0x4444);
    fault_suite!(
        check_slab_16,
        make_allocator!(CheckedSlabBStackAllocator, 16),
        0x5555
    );
    fault_suite!(
        check_slab_64,
        make_allocator!(CheckedSlabBStackAllocator, 64),
        0x6666
    );
}
