//! Move semantics with `cross_exchange` and copy-on-write with `copy`.
//!
//! ## Scenario 1: Move semantics with `cross_exchange`
//!
//! A file-backed message queue where messages are fixed-size slots. When a
//! consumer "takes" a message, we swap it with a sentinel value (all zeros)
//! to mark it as consumed, atomically retrieving the old content.
//!
//! ## Scenario 2: Copy-on-write with `copy`
//!
//! A versioned key-value store where updates create new versions by copying
//! the old record and appending the new one, leaving the original intact.
//!
//! ## How to run
//!
//! ```text
//! cargo run --example move_and_cow --features "set,atomic"
//! ```

#[cfg(all(feature = "set", feature = "atomic"))]
use bstack::BStack;
#[cfg(all(feature = "set", feature = "atomic"))]
use std::io;

#[cfg(all(feature = "set", feature = "atomic"))]
const MSG_SIZE: u64 = 32;

#[cfg(all(feature = "set", feature = "atomic"))]
fn main() -> io::Result<()> {
    move_semantics_demo()?;
    println!();
    copy_on_write_demo()?;
    Ok(())
}

#[cfg(all(feature = "set", feature = "atomic"))]
fn move_semantics_demo() -> io::Result<()> {
    println!("=== Move semantics with cross_exchange ===");

    let path = "move_example.bstack";
    let _ = std::fs::remove_file(path);
    let stack = BStack::open(path)?;

    // Create a message queue with 4 fixed-size slots.
    for i in 0..4 {
        let mut msg = vec![0u8; MSG_SIZE as usize];
        let text = format!("Message #{}", i + 1);
        msg[..text.len()].copy_from_slice(text.as_bytes());
        stack.push(&msg)?;
        println!("Enqueued: {}", text);
    }

    // "Take" message #2 (offset 32) by swapping it with zeros.
    // We need a staging area for the zeros and the retrieved message.
    let sentinel_offset = stack.push(&vec![0u8; MSG_SIZE as usize])?;
    println!("\nTaking message #2 (offset 32)...");

    // cross_exchange(a, b, n): swaps [a, a+n) with [b, b+n).
    // We swap slot #2 (offset 32) with the sentinel (offset 128).
    stack.cross_exchange(MSG_SIZE, sentinel_offset, MSG_SIZE)?;

    // Now slot #2 is zeros, and the sentinel area holds the old message.
    let taken = stack.get(sentinel_offset, sentinel_offset + MSG_SIZE)?;
    let msg_text = String::from_utf8_lossy(&taken);
    let msg_text = msg_text.trim_end_matches('\0');
    println!("Taken: {}", msg_text);

    // Show the queue state.
    println!("\nQueue after take:");
    for i in 0..4 {
        let offset = i * MSG_SIZE;
        let msg = stack.get(offset, offset + MSG_SIZE)?;
        let text = String::from_utf8_lossy(&msg)
            .trim_end_matches('\0')
            .to_string();
        if text.is_empty() {
            println!("  Slot {}: <empty>", i);
        } else {
            println!("  Slot {}: {}", i, text);
        }
    }

    Ok(())
}

#[cfg(all(feature = "set", feature = "atomic"))]
fn copy_on_write_demo() -> io::Result<()> {
    println!("=== Copy-on-write with copy ===");

    let path = "cow_example.bstack";
    let _ = std::fs::remove_file(path);
    let stack = BStack::open(path)?;

    // Version 1: initial key-value record (16 bytes: 8-byte key + 8-byte value).
    let key = 42u64;
    let value_v1 = 1000u64;
    let mut record = Vec::with_capacity(16);
    record.extend_from_slice(&key.to_le_bytes());
    record.extend_from_slice(&value_v1.to_le_bytes());
    let offset_v1 = stack.push(&record)?;
    println!(
        "v1: key={}, value={} at offset {}",
        key, value_v1, offset_v1
    );

    // Version 2: copy the old record and append an updated one.
    let value_v2 = 2000u64;

    // First, extend the stack to make room for the copy.
    stack.push(&vec![0u8; 16])?;
    let offset_v2 = offset_v1 + 16; // the new copy location

    // Now copy v1 to v2.
    stack.copy(offset_v1, offset_v2, 16)?;

    // Now overwrite just the value field (bytes 8..16) in the new copy.
    stack.set(offset_v2 + 8, &value_v2.to_le_bytes())?;
    println!(
        "v2: key={}, value={} at offset {} (copied from v1)",
        key, value_v2, offset_v2
    );

    // Version 3: another update.
    let value_v3 = 3000u64;

    // Extend for v3.
    stack.push(&vec![0u8; 16])?;
    let offset_v3 = offset_v2 + 16;

    // Copy v2 to v3.
    stack.copy(offset_v2, offset_v3, 16)?;
    stack.set(offset_v3 + 8, &value_v3.to_le_bytes())?;
    println!(
        "v3: key={}, value={} at offset {} (copied from v2)",
        key, value_v3, offset_v3
    );

    // All versions are preserved.
    println!("\nAll versions in the stack:");
    for (i, offset) in [offset_v1, offset_v2, offset_v3].iter().enumerate() {
        let rec = stack.get(*offset, *offset + 16)?;
        let k = u64::from_le_bytes(rec[0..8].try_into().unwrap());
        let v = u64::from_le_bytes(rec[8..16].try_into().unwrap());
        println!("  v{}: key={}, value={}", i + 1, k, v);
    }

    Ok(())
}

#[cfg(not(all(feature = "set", feature = "atomic")))]
fn main() {
    eprintln!("This example requires the 'set' and 'atomic' features.");
    eprintln!("Run: cargo run --example move_and_cow --features \"set,atomic\"");
}
