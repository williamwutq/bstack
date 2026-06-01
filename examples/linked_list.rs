//! Traversing a variable-sized linked list stored in bstack using `get_batched_gen`.
//!
//! This example demonstrates the generator pattern for reading linked lists where
//! nodes have different sizes. The generator reads the entire chain into a single
//! contiguous buffer under one lock by:
//! 1. Reading a node's header (next_ptr + size)
//! 2. Parsing the size to determine data length
//! 3. Issuing the next read for the appropriate buffer slice
//!
//! ## Layout
//!
//! Each node has variable size:
//! - `next_offset: u64` (8 bytes, LE) — offset to the next node, or `u64::MAX` for tail
//! - `data_size: u64` (8 bytes, LE) — size of the data payload in bytes
//! - `data: [u8]` (variable bytes) — node payload
//!
//! ## Example data
//!
//! The string "This example demonstrates the usage of get_batched_gen generator pattern"
//! is split into blocks and stored as a linked list.
//!
//! ## How to run
//!
//! ```text
//! cargo run --example linked_list --features atomic
//! ```

#[cfg(feature = "atomic")]
use bstack::BStack;
#[cfg(feature = "atomic")]
use std::io;

#[cfg(feature = "atomic")]
const SENTINEL: u64 = u64::MAX;
#[cfg(feature = "atomic")]
const HEADER_SIZE: usize = 16; // next_offset (8) + data_size (8)

#[cfg(feature = "atomic")]
fn main() -> io::Result<()> {
    let path = "linked_list_example.bstack";
    let _ = std::fs::remove_file(path);
    let stack = BStack::open(path)?;

    // Build a linked list of text blocks by appending each as the head.
    // Layout: [next_offset: u64 | data_size: u64 | data: [u8]]
    let mut blocks = [
        "This example ",
        "demonstrates ",
        "the usage of ",
        "get_batched_gen ",
        "generator pattern",
    ];

    blocks.reverse();

    let mut head_offset = SENTINEL; // initially empty
    for block in blocks.iter() {
        let data_bytes = block.as_bytes();
        let data_size = data_bytes.len() as u64;

        let mut node = Vec::with_capacity(HEADER_SIZE + data_bytes.len());
        node.extend_from_slice(&head_offset.to_le_bytes()); // next pointer
        node.extend_from_slice(&data_size.to_le_bytes()); // data size
        node.extend_from_slice(data_bytes); // payload

        head_offset = stack.push(&node)?;
    }

    println!(
        "Built {}-node list; head at offset {}",
        blocks.len(),
        head_offset
    );

    // Traverse the list using get_batched_gen: read the entire linked list into
    // a single contiguous buffer. For variable-sized nodes, we:
    // 1. Read the node's header (next_ptr + data_size)
    // 2. Parse data_size to determine data length
    // 3. Read the data portion
    // The entire list is read under a single lock.
    let mut buffer = Vec::new();
    let mut current_offset = head_offset;
    let mut current_pos = 0usize;
    let mut node_count = 0;
    let mut reading_header = true;
    let mut pending_data_size = 0u64;

    stack.get_batched_gen(|| {
        // Stop if we've reached the end or detect a cycle
        if current_offset == SENTINEL || node_count >= 100 {
            return None;
        }

        if reading_header {
            // Read the node header (next_ptr + data_size)
            buffer.resize(current_pos + HEADER_SIZE, 0);

            let offset = current_offset;
            let buf_ptr = buffer[current_pos..current_pos + HEADER_SIZE].as_mut_ptr();
            let buf_len = HEADER_SIZE;

            // After this read completes, we'll parse data_size and read the data next
            reading_header = false;

            // SAFETY: Buffer is resized to include this range; slice is within bounds
            let buf_slice = unsafe { std::slice::from_raw_parts_mut(buf_ptr, buf_len) };
            Some((offset, buf_slice))
        } else {
            // Parse the header we just read to get data_size
            let header_start = current_pos;
            let data_size_bytes: [u8; 8] = buffer[header_start + 8..header_start + 16]
                .try_into()
                .unwrap();
            pending_data_size = u64::from_le_bytes(data_size_bytes);

            // Read the data portion
            buffer.resize(current_pos + HEADER_SIZE + pending_data_size as usize, 0);

            let data_offset = current_offset + HEADER_SIZE as u64;
            let buf_ptr = buffer
                [current_pos + HEADER_SIZE..current_pos + HEADER_SIZE + pending_data_size as usize]
                .as_mut_ptr();
            let buf_len = pending_data_size as usize;

            // Move to the next node
            let next_bytes: [u8; 8] = buffer[header_start..header_start + 8].try_into().unwrap();
            current_offset = u64::from_le_bytes(next_bytes);
            current_pos += HEADER_SIZE + pending_data_size as usize;
            node_count += 1;
            reading_header = true;

            // SAFETY: Buffer is resized to include this range; slice is within bounds
            let buf_slice = unsafe { std::slice::from_raw_parts_mut(buf_ptr, buf_len) };
            Some((data_offset, buf_slice))
        }
    })?;

    println!("\nTraversed {} nodes under a single lock", node_count);
    println!("Total buffer size: {} bytes", buffer.len());

    // Parse the buffer and reconstruct the string
    let mut reconstructed = String::new();
    let mut pos = 0usize;
    for i in 0..node_count {
        let next_bytes: [u8; 8] = buffer[pos..pos + 8].try_into().unwrap();
        let next = u64::from_le_bytes(next_bytes);
        let size_bytes: [u8; 8] = buffer[pos + 8..pos + 16].try_into().unwrap();
        let size = u64::from_le_bytes(size_bytes) as usize;
        let data = &buffer[pos + HEADER_SIZE..pos + HEADER_SIZE + size];
        let text = std::str::from_utf8(data).unwrap();

        reconstructed.push_str(text);

        println!(
            "  Node {}: size={}, text={:?}, next={}",
            i,
            size,
            text,
            if next == SENTINEL {
                "null".to_string()
            } else {
                next.to_string()
            }
        );

        pos += HEADER_SIZE + size;
    }

    println!("\nReconstructed text: {:?}", reconstructed);
    println!(
        "Expected: \"This example demonstrates the usage of get_batched_gen generator pattern\""
    );

    Ok(())
}

#[cfg(not(feature = "atomic"))]
fn main() {
    eprintln!("This example requires the 'atomic' feature.");
    eprintln!("Run: cargo run --example linked_list --features atomic");
}
