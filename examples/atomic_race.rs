//! Multi-threaded races demonstrating the `atomic` feature's thread safety.
//!
//! ## Scenario 1 — `try_extend` ticket grab (`atomic`)
//!
//! `THREADS` threads each spin calling `try_extend` until they atomically
//! append their own thread-id byte.  Because only one thread can win any given
//! size-check, all others retry.  The final stack contains exactly one ticket
//! per thread; the arrival order and per-thread retry counts reveal contention.
//!
//! ## Scenario 2 — `process` shared counter (`set` + `atomic`)
//!
//! `THREADS` threads each call `process` `ITERS` times to increment a 4-byte
//! counter in place.  Because `process` holds the write lock across the entire
//! read-modify-write, no increment is ever lost: the final value is always
//! exactly `THREADS * ITERS`.
//!
//! ## How to run
//!
//! ```text
//! cargo run --example atomic_race --features atomic
//! # for both scenarios:
//! cargo run --example atomic_race --features "atomic,set"
//! ```

#[cfg(feature = "atomic")]
use bstack::BStack;
#[cfg(feature = "atomic")]
use std::io;
#[cfg(feature = "atomic")]
use std::sync::Arc;
#[cfg(feature = "atomic")]
use std::thread;

#[cfg(feature = "atomic")]
const THREADS: usize = 8;
#[cfg(all(feature = "atomic", feature = "set"))]
const ITERS: u32 = 500;

// ── Scenario 1: try_extend ticket grab ───────────────────────────────────────

#[cfg(feature = "atomic")]
fn ticket_race() -> io::Result<()> {
    println!("=== try_extend ticket race ({THREADS} threads) ===");

    let path = "atomic_race_tickets.bstack";
    let _ = std::fs::remove_file(path);
    let stack = Arc::new(BStack::open(path)?);

    // Each thread repeatedly reads the current stack size and attempts to
    // atomically extend it by exactly one byte.  try_extend is a no-op when
    // the size has changed since the snapshot (another thread won the race),
    // so losers just retry with a fresh snapshot.
    let handles: Vec<_> = (0..THREADS)
        .map(|id| {
            let s = Arc::clone(&stack);
            thread::spawn(move || -> io::Result<(usize, u32)> {
                let mut retries = 0u32;
                loop {
                    let snap = s.len()?;
                    if s.try_extend(snap, &[id as u8])? {
                        return Ok((id, retries));
                    }
                    retries += 1;
                }
            })
        })
        .collect();

    for h in handles {
        let (id, retries) = h.join().unwrap()?;
        println!("  thread {id}: claimed slot after {retries} retry(ies)");
    }

    let tickets = stack.peek(0)?;
    println!("arrival order: {:?}", tickets);
    assert_eq!(
        tickets.len(),
        THREADS,
        "each thread must record exactly one ticket"
    );
    println!("ok — {THREADS} tickets, zero duplicates\n");
    Ok(())
}

// ── Scenario 2: process shared counter ───────────────────────────────────────

#[cfg(all(feature = "atomic", feature = "set"))]
fn counter_race() -> io::Result<()> {
    println!("=== process counter race ({THREADS} threads x {ITERS} increments each) ===");

    let path = "atomic_race_counter.bstack";
    let _ = std::fs::remove_file(path);
    let stack = Arc::new(BStack::open(path)?);

    // Reserve a 4-byte little-endian counter initialised to 0.
    stack.push(&0u32.to_le_bytes())?;

    // Every thread increments the counter ITERS times.  process holds the
    // write lock for the full read-modify-write, so concurrent calls
    // serialise correctly — no two threads can interleave their read and write.
    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let s = Arc::clone(&stack);
            thread::spawn(move || -> io::Result<()> {
                for _ in 0..ITERS {
                    s.process(0, 4, |buf| {
                        let n = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
                        buf.copy_from_slice(&(n + 1).to_le_bytes());
                    })?;
                }
                Ok(())
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap()?;
    }

    let raw = stack.get(0, 4)?;
    let count = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
    let expected = THREADS as u32 * ITERS;
    println!("final count: {count}  expected: {expected}");
    assert_eq!(count, expected, "no increments must be lost");
    println!("ok — zero lost updates\n");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "atomic")]
fn main() -> io::Result<()> {
    ticket_race()?;
    #[cfg(feature = "set")]
    counter_race()?;
    Ok(())
}

#[cfg(not(feature = "atomic"))]
fn main() {
    eprintln!("This example requires the `atomic` feature.");
    eprintln!("Run with: cargo run --example atomic_race --features atomic");
}
