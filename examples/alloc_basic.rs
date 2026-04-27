//! Basic region management with `FirstFitBStackAllocator`.
//!
//! Demonstrates:
//! * Allocating, writing to, and reading back a `BStackSlice`.
//! * Resizing a slice in-place with `realloc`.
//! * Freeing a slice and confirming the slot is reused on the next alloc.
//! * Persisting allocations across a close/reopen cycle.
//!
//! Run:
//!
//! ```sh
//! cargo run --example alloc_basic --features alloc,set
//! ```

#[cfg(all(feature = "alloc", feature = "set"))]
use bstack::{BStack, BStackAllocator, FirstFitBStackAllocator};
#[cfg(all(feature = "alloc", feature = "set"))]
use std::io;

#[cfg(all(feature = "alloc", feature = "set"))]
fn main() -> io::Result<()> {
    let path = "alloc_basic_example.bstack";

    // ------------------------------------------------------------------
    // Session 1: allocate, mutate, realloc, dealloc.
    // ------------------------------------------------------------------
    let start: u64;
    {
        let alloc = FirstFitBStackAllocator::new(BStack::open(path)?)?;

        // Allocate 16 bytes.
        let a = alloc.alloc(16)?;
        start = a.start();
        println!("allocated 16 bytes at offset {start}");

        // Write a pattern.
        a.write(b"Hello, allocator")?;
        println!("wrote: {:?}", String::from_utf8_lossy(&a.read()?));

        // Grow the slice to 32 bytes.  Data is preserved; new bytes are zero.
        let a = alloc.realloc(a, 32)?;
        assert_eq!(a.start(), start); // grew in-place (tail block)
        let data = a.read()?;
        assert_eq!(&data[..16], b"Hello, allocator");
        assert_eq!(&data[16..], &[0u8; 16]);
        println!("realloced to 32 bytes at offset {}", a.start());

        // Allocate a second block so the first is no longer the tail.
        let b = alloc.alloc(16)?;
        println!("allocated second block at offset {}", b.start());

        // Free the first block. Its slot goes to the free list.
        alloc.dealloc(a)?;
        println!("freed first block");

        // The next alloc reuses the freed slot (first-fit).
        let c = alloc.alloc(16)?;
        println!("reused slot at offset {} (expected {start})", c.start());
        assert_eq!(c.start(), start);

        // into_stack() is only callable once all BStackSlices are dropped.
        let _ = c;
        let _ = b;
        drop(alloc.into_stack());
    }

    // ------------------------------------------------------------------
    // Session 2: reopen and read back data written before close.
    // ------------------------------------------------------------------
    {
        let alloc = FirstFitBStackAllocator::new(BStack::open(path)?)?;

        // The free-list and block layout are fully persisted.
        // Allocating reclaims the same slot the session-1 realloc used.
        let d = alloc.alloc(24)?;
        d.write(b"persisted across reopens")?;
        println!(
            "session 2 alloc at offset {}; data: {:?}",
            d.start(),
            String::from_utf8_lossy(&d.read()?)
        );

        let _ = d;
        drop(alloc.into_stack());
    }

    // Clean up.
    std::fs::remove_file(path).ok();
    Ok(())
}

#[cfg(not(all(feature = "alloc", feature = "set")))]
fn main() {
    println!("This example requires the 'alloc' and 'set' features.");
}
