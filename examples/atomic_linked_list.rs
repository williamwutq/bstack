//! Free-list push and atomic pop without any allocator-level mutex: a `SlabBStackAllocator`-style
//! free list maintained using only `cross_exchange` to push and `process_gen` to pop.
//!
//!
//! ## Push: splice a freed block onto the head with `cross_exchange`
//!
//! 1. Plant a self-pointer placeholder in the block's `next` slot —
//!    `stack.set(b_addr, b_addr.to_le_bytes())` — seeding it with an
//!    in-bounds value that no reader ever observes.
//! 2. Atomically swap that slot with `free_head` —
//!    `stack.cross_exchange(b_addr, FREE_HEAD_OFFSET, 8)`. Before the call
//!    `b_addr` holds `b_addr` and `free_head` holds the old head `H`; after,
//!    `free_head` holds `b_addr` (b is the new head) and `b_addr` holds `H`
//!    (b's `next` is the old head) — spliced in under one write lock, in a
//!    single step.
//!
//! ## Pop: one held lock across read, read, write with `process_gen`
//!
//! 1. Read `free_head`.
//! 2. If it isn't the sentinel, read that block's `next` pointer.
//! 3. Write `next` back into `free_head`, advancing the list.
//!
//! `process_gen` acquires the BStack write lock once, *before* the first
//! read, and holds it — unreleased — through every subsequent read and the
//! terminating write. The whole read-read-write sequence runs as a single
//! indivisible step, so no other thread can observe or modify `free_head` or
//! any node's `next` pointer in between. This is precisely what closes the
//! ABA race window that a `get_batched_gen` (read, release lock) + `cas`
//! (re-acquire, compare, write) pairing would leave open: between the release
//! and the re-acquisition, another thread could pop the head, pop the next
//! node, and push the first node back — making `free_head` cycle back to the
//! exact byte value the first reader saw, even though the list underneath has
//! completely changed. A subsequent CAS would then "succeed" while handing
//! out a node that is already live elsewhere. Holding one lock across the
//! whole sequence makes that interleaving impossible: every one of those
//! steps needs the same write lock, so they simply block until the sequence —
//! including its terminating write — has finished.
//!
//! ## Layout
//!
//! - `free_head: u64` (8 bytes, LE) at offset 0 — offset of the first free
//!   block, or `u64::MAX` (`SENTINEL`) when the list is empty.
//! - `block[i]: u64` (8 bytes, LE) — while free, holds the offset of the
//!   next free block (or `SENTINEL` for the list's tail).
//!
//! ## How to run
//!
//! ```text
//! cargo run --example atomic_linked_list --features "set,atomic"
//! ```

#[cfg(all(feature = "set", feature = "atomic"))]
use bstack::{BStack, BStackGenOp, bstack_unsafe_reborrow, bstack_unsafe_reborrow_mut};
#[cfg(all(feature = "set", feature = "atomic"))]
use std::collections::HashSet;
#[cfg(all(feature = "set", feature = "atomic"))]
use std::io;
#[cfg(all(feature = "set", feature = "atomic"))]
use std::sync::Arc;
#[cfg(all(feature = "set", feature = "atomic"))]
use std::thread;

#[cfg(all(feature = "set", feature = "atomic"))]
const SENTINEL: u64 = u64::MAX;
#[cfg(all(feature = "set", feature = "atomic"))]
const FREE_HEAD_OFFSET: u64 = 0;
#[cfg(all(feature = "set", feature = "atomic"))]
const BLOCK_SIZE: u64 = 8;
#[cfg(all(feature = "set", feature = "atomic"))]
const FIRST_BLOCK: u64 = FREE_HEAD_OFFSET + BLOCK_SIZE;
#[cfg(all(feature = "set", feature = "atomic"))]
const NUM_BLOCKS: u64 = 6;

#[cfg(all(feature = "set", feature = "atomic"))]
fn main() -> io::Result<()> {
    let path = "atomic_linked_list_example.bstack";
    let _ = std::fs::remove_file(path);
    let stack = BStack::open(path)?;

    // Reserve `free_head` plus NUM_BLOCKS fixed-size slots, all zeroed.
    // The list starts empty: free_head = SENTINEL.
    let mut payload = Vec::new();
    payload.extend_from_slice(&SENTINEL.to_le_bytes());
    payload.extend_from_slice(&vec![0u8; (NUM_BLOCKS * BLOCK_SIZE) as usize]);
    stack.push(&payload)?;
    println!("Reserved {NUM_BLOCKS} blocks of {BLOCK_SIZE} bytes each; free list starts empty.\n");

    push_demo(&stack)?;
    pop_demo(&stack)?;
    concurrent_pop_demo(stack)?;

    Ok(())
}

/// Free each block in turn via `set` + `cross_exchange`, printing the list
/// after every splice. Each push prepends, so freeing 0, 1, 2, … yields the
/// list in reverse insertion order — a textbook LIFO free list.
#[cfg(all(feature = "set", feature = "atomic"))]
fn push_demo(stack: &BStack) -> io::Result<()> {
    println!("=== Push: splice onto the head with cross_exchange ===");
    for i in 0..NUM_BLOCKS {
        let b_addr = block_offset(i);

        // 1. Plant a self-pointer placeholder — never observed by any
        //    reader; `cross_exchange` replaces it with the old head in the
        //    same atomic step that publishes `b` as the new head.
        stack.set(b_addr, b_addr.to_le_bytes())?;

        // 2. Atomically swap b's slot with `free_head` under one write lock.
        stack.cross_exchange(b_addr, FREE_HEAD_OFFSET, BLOCK_SIZE)?;

        println!("  freed block {i} -> {}", free_list_string(stack)?);
    }
    Ok(())
}

/// Pop every block via `process_gen`, printing each pop and the resulting
/// list, until the sentinel comes back and the sequence reports `None`.
#[cfg(all(feature = "set", feature = "atomic"))]
fn pop_demo(stack: &BStack) -> io::Result<()> {
    println!("\n=== Pop: read, read, write under one lock with process_gen ===");
    loop {
        match pop(stack)? {
            Some(b_addr) => {
                println!(
                    "  popped block {} -> {}",
                    block_index(b_addr),
                    free_list_string(stack)?
                );
            }
            None => {
                println!("  list empty -> {}", free_list_string(stack)?);
                break;
            }
        }
    }
    Ok(())
}

/// Rebuild a full free list, then have `NUM_BLOCKS` threads race to pop it
/// concurrently — with no allocator-level mutex. `process_gen` holds the
/// BStack write lock across each thread's whole read-read-write sequence, so
/// — unlike a `get_batched_gen` + `cas` pairing — no thread can ever observe
/// a `free_head` that cycled back to a stale value mid-sequence (the classic
/// ABA scenario). Every block therefore comes out exactly once.
#[cfg(all(feature = "set", feature = "atomic"))]
fn concurrent_pop_demo(stack: BStack) -> io::Result<()> {
    println!("\n=== Concurrent pop: {NUM_BLOCKS} threads race process_gen, no mutex ===");

    // Rebuild a fresh list of NUM_BLOCKS blocks (LIFO, as in push_demo).
    for i in 0..NUM_BLOCKS {
        let b_addr = block_offset(i);
        stack.set(b_addr, b_addr.to_le_bytes())?;
        stack.cross_exchange(b_addr, FREE_HEAD_OFFSET, BLOCK_SIZE)?;
    }
    println!("  rebuilt list -> {}", free_list_string(&stack)?);

    let stack = Arc::new(stack);
    let handles: Vec<_> = (0..NUM_BLOCKS)
        .map(|_| {
            let stack = Arc::clone(&stack);
            thread::spawn(move || pop(&stack))
        })
        .collect();

    let mut popped = Vec::new();
    for handle in handles {
        if let Some(b_addr) = handle.join().unwrap()? {
            popped.push(b_addr);
        }
    }

    let mut seen = HashSet::new();
    for &b_addr in &popped {
        assert!(
            seen.insert(b_addr),
            "block at offset {b_addr} was popped more than once — ABA corruption!"
        );
    }
    println!(
        "  {} threads popped {} distinct blocks — no duplicates, nothing lost",
        NUM_BLOCKS,
        popped.len()
    );
    println!("  final state -> {}", free_list_string(&stack)?);

    Ok(())
}

/// Pop the head block via a single `process_gen` sequence: read `free_head`,
/// read its `next`, write `next` back into `free_head`. The write lock
/// acquired for the first read is held through the terminating write, so the
/// three steps execute as one atomic, uninterruptible unit.
#[cfg(all(feature = "set", feature = "atomic"))]
fn pop(stack: &BStack) -> io::Result<Option<u64>> {
    let mut head_buf = [0u8; 8];
    let mut next_buf = [0u8; 8];
    let mut step = 0usize;
    let mut popped: Option<u64> = None;

    stack.process_gen(|| {
        let op = match step {
            // Step 0: read the current head pointer.
            0 => Some(BStackGenOp::Read {
                offset: FREE_HEAD_OFFSET,
                // SAFETY: `head_buf` outlives this whole `process_gen` call.
                buf: bstack_unsafe_reborrow_mut!(&mut head_buf[..]),
            }),
            // Step 1: `head_buf` now holds `free_head`. Sentinel means the
            // list is empty — end the sequence with no write. Otherwise read
            // the head block's `next` pointer next.
            1 => {
                let head = u64::from_le_bytes(head_buf);
                if head == SENTINEL {
                    None
                } else {
                    popped = Some(head);
                    Some(BStackGenOp::Read {
                        offset: head,
                        // SAFETY: `next_buf` outlives this whole `process_gen` call.
                        buf: bstack_unsafe_reborrow_mut!(&mut next_buf[..]),
                    })
                }
            }
            // Step 2: `next_buf` now holds the popped block's `next`.
            // Writing it into `free_head` advances the list and ends the
            // sequence — still under the lock acquired for step 0's read.
            2 => Some(BStackGenOp::Write {
                offset: FREE_HEAD_OFFSET,
                // SAFETY: `next_buf` outlives this whole `process_gen` call.
                data: bstack_unsafe_reborrow!(&next_buf[..]),
            }),
            _ => None,
        };
        step += 1;
        op
    })?;

    Ok(popped)
}

#[cfg(all(feature = "set", feature = "atomic"))]
fn block_offset(i: u64) -> u64 {
    FIRST_BLOCK + i * BLOCK_SIZE
}

#[cfg(all(feature = "set", feature = "atomic"))]
fn block_index(b_addr: u64) -> u64 {
    (b_addr - FIRST_BLOCK) / BLOCK_SIZE
}

/// Render the free list as `head -> B3 -> B1 -> B0 -> sentinel` by walking
/// `next` pointers with `peek_into`.
#[cfg(all(feature = "set", feature = "atomic"))]
fn free_list_string(stack: &BStack) -> io::Result<String> {
    let mut out = String::from("head");
    let mut offset = read_u64(stack, FREE_HEAD_OFFSET)?;
    while offset != SENTINEL {
        out.push_str(&format!(" -> B{}", block_index(offset)));
        offset = read_u64(stack, offset)?;
    }
    out.push_str(" -> sentinel");
    Ok(out)
}

#[cfg(all(feature = "set", feature = "atomic"))]
fn read_u64(stack: &BStack, offset: u64) -> io::Result<u64> {
    let mut buf = [0u8; 8];
    stack.peek_into(offset, &mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

#[cfg(not(all(feature = "set", feature = "atomic")))]
fn main() {
    eprintln!("This example requires the `set` and `atomic` features.");
    eprintln!("Run: cargo run --example atomic_linked_list --features \"set,atomic\"");
}
