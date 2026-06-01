//! Traversing a variable-sized linked list stored in bstack using `get_batched_gen`.
//!
//! This example demonstrates the generator pattern for reading linked lists where
//! nodes have different sizes. The generator reads all data payloads into a single
//! contiguous buffer under one lock by:
//! 1. Reading a node's header (next_ptr + size) into a stack-allocated slot
//! 2. Parsing the size to determine data length
//! 3. Issuing the next read for the data directly into the buffer
//!
//! Headers are never stored in the buffer — only data payloads are accumulated.
//! This avoids interleaved header/data layout and eliminates the need to skip
//! over headers during reconstruction.
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

    // Traverse the list using get_batched_gen.  Headers are read into a single
    // stack-allocated slot (reused each iteration) and never stored in the main
    // buffer.  Only data payloads are accumulated, so no header-skipping is
    // needed during reconstruction.
    let mut buffer = Vec::new(); // data payloads only
    let mut header = [0u8; HEADER_SIZE]; // reused for every node header
    // node_info records (data_size, next_offset) so the post-traversal loop can
    // print each node individually.  In a real application you would consume each
    // payload inside the generator itself (e.g. build the string on the fly) and
    // skip this array entirely.
    let mut node_info: Vec<(usize, u64)> = Vec::new();
    let mut current_offset = head_offset;
    let mut current_pos = 0usize;
    let mut reading_header = true;

    stack.get_batched_gen(|| {
        if current_offset == SENTINEL || node_info.len() >= 100 {
            return None;
        }

        if reading_header {
            // Yield the stack-allocated header slot.
            reading_header = false;
            // SAFETY: `header` lives for the duration of get_batched_gen
            let buf_slice =
                unsafe { std::slice::from_raw_parts_mut(header.as_mut_ptr(), HEADER_SIZE) };
            Some((current_offset, buf_slice))
        } else {
            // Header has been filled; parse it.
            let next_offset = u64::from_le_bytes(header[..8].try_into().unwrap());
            let data_size = u64::from_le_bytes(header[8..].try_into().unwrap()) as usize;
            let data_file_offset = current_offset + HEADER_SIZE as u64;

            buffer.resize(current_pos + data_size, 0);
            let buf_ptr = buffer[current_pos..current_pos + data_size].as_mut_ptr();

            node_info.push((data_size, next_offset));
            current_offset = next_offset;
            current_pos += data_size;
            reading_header = true;

            // SAFETY: buffer is resized to include this range; slice is within bounds
            let buf_slice = unsafe { std::slice::from_raw_parts_mut(buf_ptr, data_size) };
            Some((data_file_offset, buf_slice))
        }
    })?;

    let node_count = node_info.len();
    println!("\nTraversed {} nodes under a single lock", node_count);
    println!("Total buffer size: {} bytes", buffer.len());

    // Print each node directly from the data-only buffer — no header-skipping needed.
    let mut pos = 0usize;
    for (i, &(data_size, next_offset)) in node_info.iter().enumerate() {
        let data = &buffer[pos..pos + data_size];
        let text = std::str::from_utf8(data).unwrap();

        println!(
            "  Node {}: size={}, text={:?}, next={}",
            i,
            data_size,
            text,
            if next_offset == SENTINEL {
                "null".to_string()
            } else {
                next_offset.to_string()
            }
        );

        pos += data_size;
    }

    println!(
        "\nReconstructed text: {:?}",
        std::str::from_utf8(&buffer).unwrap()
    );
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
