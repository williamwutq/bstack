//! Atomic compound operations on a bstack event log.
//!
//! Demonstrates all `atomic`-feature operations:
//! [`replace`](bstack::BStack::replace),
//! [`atrunc`](bstack::BStack::atrunc),
//! [`splice`](bstack::BStack::splice),
//! [`splice_into`](bstack::BStack::splice_into),
//! [`try_extend`](bstack::BStack::try_extend), and
//! [`try_discard`](bstack::BStack::try_discard); plus the `set`+`atomic`
//! operations [`swap`](bstack::BStack::swap), [`cas`](bstack::BStack::cas),
//! and [`process`](bstack::BStack::process) when `set` is also enabled.
//!
//! All log entries are fixed-width 11-byte ASCII lines (`"[ok] xxxxx\n"`)
//! so the byte counts in each call are obvious by inspection.
//!
//! ## How to run
//!
//! ```text
//! cargo run --example atomic_ops --features atomic
//! # to also exercise the set+atomic operations:
//! cargo run --example atomic_ops --features "atomic,set"
//! ```

#[cfg(feature = "atomic")]
use bstack::BStack;
#[cfg(feature = "atomic")]
use std::io;

#[cfg(feature = "atomic")]
fn show(label: &str, stack: &BStack) -> io::Result<()> {
    println!("{label}: {:?}", String::from_utf8_lossy(&stack.peek(0)?));
    Ok(())
}

#[cfg(feature = "atomic")]
fn main() -> io::Result<()> {
    let path = "atomic_ops_example.bstack";
    let _ = std::fs::remove_file(path);
    let stack = BStack::open(path)?;

    // Each log line is exactly 11 bytes: "[ok] xxxxx\n"
    stack.push(b"[ok] start\n")?;
    stack.push(b"[ok] login\n")?;
    stack.push(b"[ok] fetch\n")?;
    show("initial      ", &stack)?;

    // replace: read the last N bytes, pass them to a callback, write back the
    // result — all under a single write lock with no observable intermediate state.
    stack.replace(11, |old| old.to_ascii_uppercase())?;
    show("after replace", &stack)?;

    // atrunc: atomically remove the last N bytes and append new bytes in place.
    stack.atrunc(11, b"[ok] store\n")?;
    show("after atrunc ", &stack)?;

    // splice: like atrunc but also returns the removed bytes as a Vec.
    let removed = stack.splice(11, b"[ok] flush\n")?;
    println!("splice removed {:?}", String::from_utf8_lossy(&removed));
    show("after splice ", &stack)?;

    // splice_into: buffer-reuse counterpart of splice; writes removed bytes
    // into a caller-supplied slice instead of allocating.
    let mut old_line = [0u8; 11];
    stack.splice_into(&mut old_line, b"[ok] close\n")?;
    println!(
        "splice_into removed {:?}",
        String::from_utf8_lossy(&old_line)
    );
    show("after s_into ", &stack)?;

    // try_extend: append only when the current size equals the expected value.
    // Useful for idempotent replay — the second call observes the updated size
    // and is a no-op.
    let snap = stack.len()?;
    let pushed1 = stack.try_extend(snap, b"[ok] retry\n")?;
    let pushed2 = stack.try_extend(snap, b"[ok] retry\n")?; // size changed — no-op
    println!("try_extend: first={pushed1} second={pushed2}");
    show("after t_ext  ", &stack)?;

    // try_discard: remove N bytes only when the current size matches.
    let snap = stack.len()?;
    let ok1 = stack.try_discard(snap, 11)?;
    let ok2 = stack.try_discard(snap, 11)?; // size changed — no-op
    println!("try_discard: first={ok1} second={ok2}");
    show("after t_disc ", &stack)?;

    // swap, cas, process — require both `set` and `atomic`
    #[cfg(feature = "set")]
    {
        // Push an 8-byte status record: 4-byte ASCII tag + 4-byte LE counter.
        let status_off = stack.push(&[0u8; 8])?;
        println!("status record at offset {status_off}");

        // swap: atomically read N bytes at offset and overwrite them, returning
        // the original bytes.
        let prev = stack.swap(status_off, b"RUN\x00")?;
        println!("swap wrote 'RUN\\0', got back {:02x?}", prev);

        // cas: compare-and-exchange; writes new bytes only when current bytes
        // match old.  The second call is a no-op because the tag is now "DONE".
        let ok_cas1 = stack.cas(status_off, b"RUN\x00", b"DONE")?;
        let ok_cas2 = stack.cas(status_off, b"RUN\x00", b"FAIL")?;
        println!("cas: first={ok_cas1} second={ok_cas2}");
        println!(
            "tag now: {:?}",
            String::from_utf8_lossy(&stack.get(status_off, status_off + 4)?)
        );

        // process: read a range into a buffer, pass it to a closure for
        // in-place mutation, then write it back — all under one write lock.
        // Here we increment the 4-byte counter in bytes 4–7 of the record.
        stack.process(status_off, status_off + 8, |buf| {
            let n = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
            buf[4..8].copy_from_slice(&(n + 1).to_le_bytes());
        })?;
        let record = stack.get(status_off, status_off + 8)?;
        let counter = u32::from_le_bytes([record[4], record[5], record[6], record[7]]);
        println!(
            "after process: tag={:?} counter={counter}",
            String::from_utf8_lossy(&record[..4])
        );
    }

    Ok(())
}

#[cfg(not(feature = "atomic"))]
fn main() {
    eprintln!("This example requires the `atomic` feature.");
    eprintln!("Run with: cargo run --example atomic_ops --features atomic");
}
