//! Happy-path fuzz for the uninitialised allocation API
//! ([`BStackUninitAllocator`](crate::alloc::BStackUninitAllocator)).
//!
//! [`init`](super::init) drives `alloc`/`realloc`, which guarantee a
//! zero-initialised region and zero-fill any grown bytes. This suite drives
//! `alloc_uninit`/`realloc_uninit` instead, which skip those scrubs and leave
//! newly allocated or newly grown bytes **unspecified**. The two invariants that
//! must still hold are exactly the ones the scrubs do not provide:
//!
//! * **Prefix preservation.** Bytes the caller has written stay byte-for-byte
//!   intact across a `realloc_uninit` — the surviving `min(old_len, new_len)`
//!   prefix on a resize, and the whole region when it does not move.
//! * **Crash consistency.** A fully-written live allocation reads back
//!   byte-for-byte after **reopen + recovery**, including adversarial payloads
//!   that look like allocator internals.
//!
//! What this suite deliberately does **not** assert is the content of the
//! unspecified region: after an `alloc_uninit` or a `realloc_uninit` grow the new
//! bytes may be zero or stale, both allowed, so the driver overwrites the whole
//! region with a fresh payload before it is verified again. The fault-injection
//! counterpart is [`uninit_fault`](super::uninit_fault).

use super::common::{
    FuzzConfig, Guard, Operation, Payload, gen_op, make_allocator, make_payload, temp_path,
};
use crate::alloc::{
    BStackOwnedSlice, BStackOwnedSliceAllocator, BStackRange, BStackUninitAllocator,
    FirstFitBStackAllocator, GhostTreeBstackAllocator, SegregatedBStackAllocator,
    SlabBStackAllocator,
};
use crate::{BStack, CheckedSlabBStackAllocator};
use rand::RngExt;

// A random per-run salt so the deterministic byte patterns of two parallel
// test binaries (or the seeded/adversarial payload kinds) never alias — a
// stray cross-allocation read then shows up as a mismatch.
fn run_bias(rng: &mut impl RngExt) -> u64 {
    rng.random_range(0..=u64::MAX)
}

// alloc_uninit/dealloc/check mix. Each live allocation is fully written with a
// `Payload` — a cheap seeded pattern or an adversarial snapshot copied out of
// the BStack (bytes that look like allocator internals) — and verified on
// read-back. A block handed back by `alloc_uninit` starts unspecified, so the
// full write is what makes it verifiable; the check confirms the write stuck
// and that no later operation disturbs it.
fn run_uninit_alloc_dealloc<A, F>(make: F)
where
    A: BStackOwnedSliceAllocator + BStackUninitAllocator,
    F: Fn(BStack) -> std::io::Result<A>,
{
    let cfg = FuzzConfig::from_env();
    let path = temp_path("u_ad");
    let _guard = Guard(path.clone());
    let alloc = make(BStack::open(&path).unwrap()).unwrap();
    let mut rng = rand::rng();
    let bias = run_bias(&mut rng);
    let mut live: Vec<(BStackOwnedSlice<'_, A>, Payload)> = Vec::new();
    let mut next_id = 0u64;

    for _ in 0..cfg.ops {
        match gen_op(&mut rng, &cfg, !live.is_empty(), false) {
            Operation::Alloc(len) => {
                if let Ok(mut s) = alloc.alloc_uninit(len) {
                    let payload = make_payload(alloc.stack(), s.len(), next_id, &cfg, &mut rng);
                    next_id += 1;
                    payload.write(&mut s, bias).unwrap();
                    payload.verify(&s, bias, "uninit alloc_dealloc: post-write");
                    live.push((s, payload));
                }
            }
            Operation::Dealloc => {
                let i = rng.random_range(0..live.len());
                let (s, payload) = live.swap_remove(i);
                payload.verify(&s, bias, "uninit alloc_dealloc: pre-dealloc");
                alloc.dealloc(s).unwrap();
            }
            // No faults armed: realloc/check rolls are both integrity reads.
            Operation::Realloc(_) | Operation::Check => {
                let i = rng.random_range(0..live.len());
                let (s, payload) = &live[i];
                payload.verify(s, bias, "uninit alloc_dealloc: check");
            }
            Operation::Reopen => {}
        }
    }
}

// alloc_uninit/realloc_uninit/dealloc mix. Verifies that `realloc_uninit`
// preserves the surviving prefix; the grown bytes are unspecified, so they are
// not checked — the driver overwrites the whole region with a fresh payload
// instead, restoring a fully-defined, verifiable allocation.
fn run_uninit_alloc_realloc_dealloc<A, F>(make: F)
where
    A: BStackOwnedSliceAllocator + BStackUninitAllocator,
    F: Fn(BStack) -> std::io::Result<A>,
{
    let cfg = FuzzConfig::from_env();
    let path = temp_path("u_ard");
    let _guard = Guard(path.clone());
    let alloc = make(BStack::open(&path).unwrap()).unwrap();
    let mut rng = rand::rng();
    let bias = run_bias(&mut rng);
    let mut live: Vec<(BStackOwnedSlice<'_, A>, Payload)> = Vec::new();
    let mut next_id = 0u64;

    for _ in 0..cfg.ops {
        match gen_op(&mut rng, &cfg, !live.is_empty(), false) {
            Operation::Alloc(len) => {
                if let Ok(mut s) = alloc.alloc_uninit(len) {
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
                match alloc.realloc_uninit(s, new_len) {
                    Ok(mut s2) => {
                        let preserved = old_len.min(new_len);
                        payload.verify_prefix(
                            &s2,
                            preserved,
                            bias,
                            "uninit realloc: preserved prefix",
                        );
                        // Grown bytes are unspecified — do not check them;
                        // overwrite the whole region with a fresh payload.
                        let np = make_payload(alloc.stack(), s2.len(), next_id, &cfg, &mut rng);
                        next_id += 1;
                        np.write(&mut s2, bias).unwrap();
                        live.push((s2, np));
                    }
                    Err(e) => {
                        // Unarmed run: failure is not expected, but if it
                        // happens the region must survive — re-track it.
                        if let Some(h) = e.handle {
                            live.push((h, payload));
                        }
                    }
                }
            }
            Operation::Dealloc => {
                let i = rng.random_range(0..live.len());
                let (s, payload) = live.swap_remove(i);
                payload.verify(&s, bias, "uninit alloc_realloc_dealloc: pre-dealloc");
                alloc.dealloc(s).unwrap();
            }
            Operation::Check => {
                let i = rng.random_range(0..live.len());
                let (s, payload) = &live[i];
                payload.verify(s, bias, "uninit alloc_realloc_dealloc: check");
            }
            Operation::Reopen => {}
        }
    }
}

// Persist, then repeatedly reopen the allocator, re-verifying every live
// allocation after each reopen (i.e. after recovery) and mutating between
// sessions with the uninitialised API. Live handles are stored as raw ranges
// (a `BStackOwnedSlice` cannot outlive its allocator) alongside their fully
// written `Payload`, so adversarial data is verified across the reopen boundary
// too.
fn run_uninit_reopen<A, F>(make: F)
where
    A: BStackOwnedSliceAllocator + BStackUninitAllocator,
    F: Fn(BStack) -> std::io::Result<A>,
{
    let cfg = FuzzConfig::from_env();
    let path = temp_path("u_reopen");
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
            payload.verify(&s, bias, &format!("uninit reopen s{session} rec{i}"));
        }

        for _ in 0..cfg.ops_per_session {
            match gen_op(&mut rng, &cfg, !live.is_empty(), false) {
                Operation::Alloc(len) => {
                    if let Ok(mut s) = alloc.alloc_uninit(len) {
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
                    match alloc.realloc_uninit(s, new_len) {
                        Ok(mut s2) => {
                            let preserved = old_len.min(new_len);
                            payload.verify_prefix(
                                &s2,
                                preserved,
                                bias,
                                "uninit reopen realloc: preserved prefix",
                            );
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
                    payload.verify(&s, bias, "uninit reopen: pre-dealloc");
                    alloc.dealloc(s).unwrap();
                }
                Operation::Check => {
                    let i = rng.random_range(0..live.len());
                    let (range, payload) = &live[i];
                    let s = unsafe {
                        BStackOwnedSlice::from_raw_parts(&alloc, range.start(), range.len())
                    };
                    payload.verify(&s, bias, "uninit reopen: check");
                }
                Operation::Reopen => {}
            }
        }

        drop(alloc.into_stack());
    }
}

// Zero-size `alloc_uninit` must behave like `alloc(0)`: share (start, len) =
// (0, 0) and dealloc as no-ops.
fn run_uninit_zero_size<A, F>(make: F)
where
    A: BStackOwnedSliceAllocator + BStackUninitAllocator,
    F: Fn(BStack) -> std::io::Result<A>,
{
    let cfg = FuzzConfig::from_env();
    let path = temp_path("u_zalloc");
    let _guard = Guard(path.clone());
    let alloc = make(BStack::open(&path).unwrap()).unwrap();

    let mut slices = Vec::new();
    for _ in 0..cfg.ops {
        let s = alloc.alloc_uninit(0).unwrap();
        assert_eq!(s.len(), 0, "zero alloc_uninit must have len 0");
        assert_eq!(s.start(), 0, "zero alloc_uninit must have start 0");
        slices.push(s);
    }
    for s in slices {
        alloc.dealloc(s).unwrap();
    }
}

macro_rules! uninit_fuzz_suite {
    ($mod_name:ident, $make:expr) => {
        mod $mod_name {
            use super::*;
            #[test]
            fn alloc_dealloc() {
                super::run_uninit_alloc_dealloc($make);
            }
            #[test]
            fn alloc_realloc_dealloc() {
                super::run_uninit_alloc_realloc_dealloc($make);
            }
            #[test]
            fn reopen() {
                super::run_uninit_reopen($make);
            }
            #[test]
            fn zero_size_alloc() {
                super::run_uninit_zero_size($make);
            }
        }
    };
}

uninit_fuzz_suite!(first_fit, make_allocator!(FirstFitBStackAllocator));
uninit_fuzz_suite!(ghost_tree, make_allocator!(GhostTreeBstackAllocator));
uninit_fuzz_suite!(slab_8, make_allocator!(SlabBStackAllocator, 8));
uninit_fuzz_suite!(slab_16, make_allocator!(SlabBStackAllocator, 16));
uninit_fuzz_suite!(slab_64, make_allocator!(SlabBStackAllocator, 64));
uninit_fuzz_suite!(
    check_slab_16,
    make_allocator!(CheckedSlabBStackAllocator, 16)
);
uninit_fuzz_suite!(
    check_slab_64,
    make_allocator!(CheckedSlabBStackAllocator, 64)
);
uninit_fuzz_suite!(segregated, make_allocator!(SegregatedBStackAllocator));
