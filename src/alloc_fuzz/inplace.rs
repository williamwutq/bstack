//! Happy-path fuzz for in-place resize
//! ([`BStackInPlaceResizeAllocator`](crate::alloc::BStackInPlaceResizeAllocator)).
//!
//! `realloc_inplace(handle, prepend, append)` moves the front edge, the back
//! edge, or both **without relocating** the retained bytes. This suite drives
//! random `(prepend, append)` deltas against each live allocation and checks the
//! trait's contract:
//!
//! * **Exact-position guarantee.** On success the returned range is *exactly*
//!   `(start − prepend, end + append)` — a resize that would have to relocate the
//!   retained bytes must fail with `Unsupported`, never quietly return a
//!   correctly-sized region at a different offset. The driver asserts the new
//!   `start`/`len` match to the byte.
//! * **Retained bytes are untouched.** Every byte in the physical overlap of the
//!   old and new ranges still reads its pre-resize value. A payload keyed on the
//!   absolute stack offset (see [`common::pat`](super::common::pat)) makes this a
//!   single read of the overlap.
//! * **Clean rejection.** An unsupported `(prepend, append)` (or one whose
//!   resulting length is negative) returns `Unsupported`/`InvalidInput` with the
//!   original handle intact — never a panic, never a lost region.
//!
//! Newly-added bytes (a front or back grow) are *not* asserted: the trait leaves
//! their contents unspecified, so the driver overwrites the whole region after a
//! successful resize. Only [`FirstFitBStackAllocator`], [`GhostTreeBstackAllocator`],
//! and [`SegregatedBStackAllocator`] implement the trait; the slab allocators do
//! not. The fault-injection counterpart is [`inplace_fault`](super::inplace_fault).

use super::common::{
    FuzzConfig, Guard, Operation, gen_inplace_deltas, gen_op, make_allocator, temp_path,
    verify_pattern, verify_pattern_range, write_pattern,
};
use crate::BStack;
use crate::alloc::{
    BStackInPlaceResizeAllocator, BStackOwnedSlice, BStackOwnedSliceAllocator, BStackRange,
    FirstFitBStackAllocator, GhostTreeBstackAllocator, SegregatedBStackAllocator,
};
use rand::RngExt;
use std::io;

// Apply a successful `realloc_inplace` result: assert the exact-position
// guarantee and that the retained overlap is byte-identical, then overwrite the
// whole (possibly grown) region with a fresh pattern and return it. `None` means
// the resize freed the region (resulting length 0).
#[allow(clippy::too_many_arguments)] // distinct scalars; a struct would not aid clarity
pub(super) fn apply_ok<'a, A>(
    s2: BStackOwnedSlice<'a, A>,
    start: u64,
    len: u64,
    prepend: i64,
    append: i64,
    id: u64,
    bias: u64,
    ctx: &str,
) -> Option<BStackOwnedSlice<'a, A>>
where
    A: BStackOwnedSliceAllocator + BStackInPlaceResizeAllocator,
{
    if s2.is_empty() {
        // Resized to zero → the region was freed.
        return None;
    }
    let exp_start = (start as i64 - prepend) as u64;
    let exp_len = (len as i64 + prepend + append) as u64;
    assert_eq!(
        s2.start(),
        exp_start,
        "{ctx}: position start (p={prepend}, a={append})"
    );
    assert_eq!(
        s2.len(),
        exp_len,
        "{ctx}: position len (p={prepend}, a={append})"
    );
    // The physical overlap of old [start, start+len) and new [exp_start, exp_start+exp_len)
    // must still hold the pre-resize bytes.
    let lo = start.max(exp_start);
    let hi = (start + len).min(exp_start + exp_len);
    verify_pattern_range(&s2, lo, hi, id, bias, ctx);
    let mut s2 = s2;
    write_pattern(&mut s2, id, bias).unwrap();
    Some(s2)
}

// alloc / realloc_inplace / dealloc / check mix, holding live slices directly.
fn run_inplace_resize<A, F>(make: F)
where
    A: BStackOwnedSliceAllocator + BStackInPlaceResizeAllocator,
    F: Fn(BStack) -> io::Result<A>,
{
    let cfg = FuzzConfig::from_env();
    let path = temp_path("ip");
    let _guard = Guard(path.clone());
    let alloc = make(BStack::open(&path).unwrap()).unwrap();
    let mut rng = rand::rng();
    let bias = rng.random_range(0..=u64::MAX);
    let mut live: Vec<(BStackOwnedSlice<'_, A>, u64)> = Vec::new();
    let mut next_id = 0u64;

    for _ in 0..cfg.ops {
        match gen_op(&mut rng, &cfg, !live.is_empty(), false) {
            Operation::Alloc(len) => {
                if let Ok(mut s) = alloc.alloc(len) {
                    let id = next_id;
                    next_id += 1;
                    write_pattern(&mut s, id, bias).unwrap();
                    live.push((s, id));
                }
            }
            Operation::Realloc(_) => {
                let i = rng.random_range(0..live.len());
                let (s, id) = live.swap_remove(i);
                let (start, len) = (s.start(), s.len());
                let (prepend, append) = gen_inplace_deltas(&mut rng, len, &cfg);
                match alloc.realloc_inplace(s, prepend, append) {
                    Ok(s2) => {
                        if let Some(s2) =
                            apply_ok(s2, start, len, prepend, append, id, bias, "inplace: resize")
                        {
                            live.push((s2, id));
                        }
                    }
                    Err(e) => {
                        // Happy path: the only failures are clean pre-mutation
                        // rejections, which must return the untouched handle.
                        let kind = e.source.kind();
                        assert!(
                            matches!(
                                kind,
                                io::ErrorKind::Unsupported | io::ErrorKind::InvalidInput
                            ),
                            "inplace: unexpected error {kind:?} (p={prepend}, a={append})"
                        );
                        let h = e.handle.expect("inplace rejection must return the handle");
                        assert_eq!(h.start(), start, "inplace rejection: start preserved");
                        assert_eq!(h.len(), len, "inplace rejection: len preserved");
                        verify_pattern(&h, id, bias, "inplace rejection: original intact");
                        live.push((h, id));
                    }
                }
            }
            Operation::Dealloc => {
                let i = rng.random_range(0..live.len());
                let (s, id) = live.swap_remove(i);
                verify_pattern(&s, id, bias, "inplace: pre-dealloc");
                alloc.dealloc(s).unwrap();
            }
            Operation::Check => {
                let i = rng.random_range(0..live.len());
                let (s, id) = &live[i];
                verify_pattern(s, *id, bias, "inplace: check");
            }
            Operation::Reopen => {}
        }
    }
}

// Persist, then repeatedly reopen the allocator, re-verifying every live
// allocation after each reopen (i.e. after recovery) and resizing in place
// between sessions. Live handles are stored as raw ranges (a `BStackOwnedSlice`
// cannot outlive its allocator) alongside their id, so the physical-offset
// pattern is verified across the reopen boundary — the structural in-place paths
// (front carve, tail extend) are exactly the ones recovery must repair.
fn run_inplace_reopen<A, F>(make: F)
where
    A: BStackOwnedSliceAllocator + BStackInPlaceResizeAllocator,
    F: Fn(BStack) -> io::Result<A>,
{
    let cfg = FuzzConfig::from_env();
    let path = temp_path("ip_reopen");
    let _guard = Guard(path.clone());
    drop(make(BStack::open(&path).unwrap()).unwrap());

    let mut rng = rand::rng();
    let bias = rng.random_range(0..=u64::MAX);
    let mut live: Vec<(BStackRange, u64)> = Vec::new();
    let mut next_id: u64 = 0;

    for session in 0..cfg.sessions {
        let alloc = make(BStack::open(&path).unwrap()).unwrap();

        for (i, (range, id)) in live.iter().enumerate() {
            let s = unsafe { BStackOwnedSlice::from_raw_parts(&alloc, range.start(), range.len()) };
            verify_pattern(&s, *id, bias, &format!("inplace reopen s{session} rec{i}"));
        }

        for _ in 0..cfg.ops_per_session {
            match gen_op(&mut rng, &cfg, !live.is_empty(), false) {
                Operation::Alloc(len) => {
                    if let Ok(mut s) = alloc.alloc(len) {
                        let id = next_id;
                        next_id += 1;
                        write_pattern(&mut s, id, bias).unwrap();
                        live.push((s.as_range(), id));
                    }
                }
                Operation::Realloc(_) => {
                    let i = rng.random_range(0..live.len());
                    let (range, id) = live.swap_remove(i);
                    let (start, len) = (range.start(), range.len());
                    let s = unsafe { BStackOwnedSlice::from_raw_parts(&alloc, start, len) };
                    let (prepend, append) = gen_inplace_deltas(&mut rng, len, &cfg);
                    match alloc.realloc_inplace(s, prepend, append) {
                        Ok(s2) => {
                            if let Some(s2) = apply_ok(
                                s2,
                                start,
                                len,
                                prepend,
                                append,
                                id,
                                bias,
                                "inplace reopen: resize",
                            ) {
                                live.push((s2.as_range(), id));
                            }
                        }
                        Err(e) => {
                            let kind = e.source.kind();
                            assert!(
                                matches!(
                                    kind,
                                    io::ErrorKind::Unsupported | io::ErrorKind::InvalidInput
                                ),
                                "inplace reopen: unexpected error {kind:?} (p={prepend}, a={append})"
                            );
                            let h = e.handle.expect("inplace rejection must return the handle");
                            verify_pattern(
                                &h,
                                id,
                                bias,
                                "inplace reopen rejection: original intact",
                            );
                            live.push((h.as_range(), id));
                        }
                    }
                }
                Operation::Dealloc => {
                    let i = rng.random_range(0..live.len());
                    let (range, id) = live.swap_remove(i);
                    let s = unsafe {
                        BStackOwnedSlice::from_raw_parts(&alloc, range.start(), range.len())
                    };
                    verify_pattern(&s, id, bias, "inplace reopen: pre-dealloc");
                    alloc.dealloc(s).unwrap();
                }
                Operation::Check => {
                    let i = rng.random_range(0..live.len());
                    let (range, id) = &live[i];
                    let s = unsafe {
                        BStackOwnedSlice::from_raw_parts(&alloc, range.start(), range.len())
                    };
                    verify_pattern(&s, *id, bias, "inplace reopen: check");
                }
                Operation::Reopen => {}
            }
        }

        drop(alloc.into_stack());
    }
}

macro_rules! inplace_fuzz_suite {
    ($mod_name:ident, $make:expr) => {
        mod $mod_name {
            use super::*;
            #[test]
            fn resize() {
                super::run_inplace_resize($make);
            }
            #[test]
            fn reopen() {
                super::run_inplace_reopen($make);
            }
        }
    };
}

inplace_fuzz_suite!(first_fit, make_allocator!(FirstFitBStackAllocator));
inplace_fuzz_suite!(ghost_tree, make_allocator!(GhostTreeBstackAllocator));
inplace_fuzz_suite!(segregated, make_allocator!(SegregatedBStackAllocator));
