//! Persistent byte buffer using [`BStackByteVec`].
//!
//! Demonstrates:
//!
//! * Storing raw bytes in a `BStackByteVec` backed by a `LinearBStackAllocator`.
//! * Iterating, querying, and mutating the live vec.
//! * Serialising the raw block handle, closing the file, reopening it, and
//!   reconstructing the vec — the reopen / crash-recovery path.

use bstack::{BStack, BStackByteVec, BStackOwnedSlice, LinearBStackAllocator};
use std::io;
use std::path::PathBuf;

fn print_bytes(v: &BStackByteVec<LinearBStackAllocator>) -> io::Result<()> {
    let len = v.len()?;
    print!("  {} byte(s): [", len);
    for (i, item) in v.iter()?.enumerate() {
        if i > 0 {
            print!(", ");
        }
        print!("0x{:02X}", item?);
    }
    println!("]");
    Ok(())
}

fn main() -> io::Result<()> {
    let path = PathBuf::from("bytevec_example.bstack");

    // ── Session 1: create and populate ───────────────────────────────────────

    println!("=== Session 1: creating byte buffer ===");

    let block_bytes: [u8; 16] = {
        let alloc = LinearBStackAllocator::new(BStack::open(&path)?);
        let mut buf: BStackByteVec<_> = BStackByteVec::new(&alloc)?;

        // Append the bytes of a short ASCII message.
        for &b in b"Hello, BStack!" {
            buf.push(b)?;
        }

        println!("After initial push:");
        print_bytes(&buf)?;
        println!("  capacity={}", buf.capacity()?);

        // Pop the last byte, inspect it, then push a replacement.
        let last = buf.pop()?.expect("expected a byte");
        println!("\nPopped: 0x{:02X} ('{}')", last, last as char);

        buf.push(b'!')?;
        buf.push(b'!')?;

        println!("\nAfter replacement:");
        print_bytes(&buf)?;

        // Read all bytes back at once.
        let all = buf.read_bytes()?;
        println!("\nread_bytes(): {:?}", String::from_utf8_lossy(&all));

        // Serialise the raw block handle (offset + len as 16 bytes).
        let bytes: [u8; 16] = buf.into_raw_block().into();
        bytes
        // `alloc` (and the BStack) are dropped here, closing the file.
    };

    println!("\nFile closed.  Block handle: {:?}", block_bytes);

    // ── Session 2: reopen and recover ────────────────────────────────────────

    println!("\n=== Session 2: reopen and recover ===");

    {
        let alloc = LinearBStackAllocator::new(BStack::open(&path)?);

        // Reconstruct the BStackOwnedSlice from the serialised bytes.
        // SAFETY: `block_bytes` was produced by `BStackOwnedSlice::into()` in
        // Session 1 on the same file; the offset and length are valid.
        let block = unsafe { BStackOwnedSlice::from_bytes(&alloc, block_bytes) };

        // Reconstruct the BStackByteVec.
        // SAFETY: the block was created by `BStackByteVec::new` with the same
        // allocator; the header layout matches.
        let buf: BStackByteVec<_> = unsafe { BStackByteVec::from_raw_block(block) };

        println!("Recovered {} byte(s):", buf.len()?);
        print_bytes(&buf)?;

        // Verify the content survived the round-trip.
        let recovered = buf.read_bytes()?;
        assert_eq!(recovered, b"Hello, BStack!!");
        println!("\nVerified: {:?}", String::from_utf8_lossy(&recovered));

        // Demonstrate resize: pad to 20 bytes with spaces.
        let mut buf = buf;
        buf.resize(20, b' ')?;
        println!("\nAfter resize to 20 (space-filled):");
        print_bytes(&buf)?;

        // Truncate back to the original content.
        buf.truncate(15)?;
        println!("\nAfter truncate to 15:");
        print_bytes(&buf)?;

        // Clean up via dealloc.
        buf.dealloc()?;
    }

    // Remove the example file.
    std::fs::remove_file(&path)?;
    println!("\nDone.");
    Ok(())
}
