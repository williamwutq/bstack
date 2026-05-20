//! Persistent order log using [`BStackVec`].
//!
//! Demonstrates:
//!
//! * Storing a typed, multi-field struct (`Order`) in a `BStackVec` backed by
//!   a `LinearBStackAllocator`.
//! * Iterating, querying, and mutating the live vec.
//! * Serialising the raw block handle, closing the file, reopening it, and
//!   reconstructing the vec — the reopen / crash-recovery path.

use bstack::{BStack, BStackAllocator, BStackVec, LinearBStackAllocator};
use std::io;
use std::path::PathBuf;

// ── Order type ────────────────────────────────────────────────────────────────

/// A single order record.
///
/// All fields are fixed-width and the struct is `repr(C)` so the on-disk
/// layout is stable across compilations.  Fixed-point money values use
/// integer cents (2 implied decimal places).
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq)]
struct Order {
    /// Unix timestamp (seconds since epoch).
    timestamp: u64,
    /// Price per item in hundredths of the currency unit (e.g. 1999 = $19.99).
    price_cents: i64,
    /// Shipping fee in hundredths of the currency unit.
    shipping_cents: i64,
    /// Number of items ordered (negative for returns).
    amount: i32,
    /// Fulfilment class: 0 = standard, 1 = express, 2 = overnight.
    order_type: u8,
    _pad: [u8; 3],
}

impl Order {
    fn new(
        timestamp: u64,
        amount: i32,
        price_cents: i64,
        shipping_cents: i64,
        order_type: u8,
    ) -> Self {
        Self {
            timestamp,
            amount,
            price_cents,
            shipping_cents,
            order_type,
            _pad: [0; 3],
        }
    }

    fn total_cents(&self) -> i64 {
        self.price_cents * self.amount as i64 + self.shipping_cents
    }

    fn type_name(&self) -> &'static str {
        match self.order_type {
            0 => "standard",
            1 => "express",
            2 => "overnight",
            _ => "unknown",
        }
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn print_orders(v: &BStackVec<Order, LinearBStackAllocator>) -> io::Result<()> {
    let len = v.len()?;
    println!("  {} order(s):", len);
    for item in v.iter()? {
        let o = item?;
        println!(
            "    ts={:<12} {:>3} x {:>7} + {:>5} ship  [{:>9}]  total={:>9}¢",
            o.timestamp,
            o.amount,
            format!("{}¢", o.price_cents),
            format!("{}¢", o.shipping_cents),
            o.type_name(),
            o.total_cents(),
        );
    }
    Ok(())
}

// ── main ──────────────────────────────────────────────────────────────────────

fn main() -> io::Result<()> {
    let path = PathBuf::from("orders_example.bstack");

    // ── Session 1: create and populate ───────────────────────────────────────

    println!("=== Session 1: creating order log ===");

    let block_bytes: [u8; 16] = {
        let alloc = LinearBStackAllocator::new(BStack::open(&path)?);
        let mut orders: BStackVec<Order, _> = BStackVec::new(&alloc)?;

        orders.push(Order::new(1_748_000_000, 2, 1999, 499, 0))?; // 2× $19.99, $4.99 ship
        orders.push(Order::new(1_748_000_120, 1, 8999, 0, 1))?; // 1× $89.99, free express
        orders.push(Order::new(1_748_000_300, 5, 599, 999, 0))?; // 5× $5.99, $9.99 ship
        orders.push(Order::new(1_748_001_000, -1, 1999, 0, 0))?; // 1 return, $19.99 refund

        println!("After initial push:");
        print_orders(&orders)?;
        println!("  capacity={}", orders.capacity()?);

        // Pop the return record, inspect it, then push a replacement.
        let ret = orders.pop()?.expect("expected a record");
        assert_eq!(ret.amount, -1);
        println!("\nPopped return record: {:?}", ret);

        // Replace with an overnight order.
        orders.push(Order::new(1_748_001_500, 3, 2499, 1499, 2))?;

        println!("\nAfter replacement:");
        print_orders(&orders)?;

        // Serialise the raw block handle (offset + len as 16 bytes).
        let bytes: [u8; 16] = orders.into_raw_block().into();
        bytes
        // `alloc` (and the BStack) are dropped here, closing the file.
    };

    println!("\nFile closed.  Block handle: {:?}", block_bytes);

    // ── Session 2: reopen and recover ────────────────────────────────────────

    println!("\n=== Session 2: reopen and recover ===");

    {
        let alloc = LinearBStackAllocator::new(BStack::open(&path)?);

        // Reconstruct the BStackSlice from the serialised bytes.
        // SAFETY: `block_bytes` was produced by `BStackSlice::to_bytes` in
        // Session 1 on the same file; the offset and length are valid.
        let block = unsafe { bstack::BStackSlice::from_bytes(&alloc, block_bytes) };

        // Reconstruct the BStackVec.
        // SAFETY: the block was created by `BStackVec<Order, _>::new` with the
        // same element type; the header layout matches.
        let orders: BStackVec<Order, _> = unsafe { BStackVec::from_raw_block(block) };

        println!("Recovered {} order(s):", orders.len()?);
        print_orders(&orders)?;

        // Verify a specific record survives the round-trip.
        let second = orders.get(1)?.expect("expected record at index 1");
        assert_eq!(second.price_cents, 8999);
        assert_eq!(second.order_type, 1);
        println!("\nVerified: record[1] = {:?}", second);

        // Demonstrate resize: pad to 8 slots with a zero-value placeholder.
        let placeholder = Order::new(0, 0, 0, 0, 0);
        let mut orders = orders;
        // We must hold the mutable vec; reconstruct a mutable binding.
        // (In real code the vec would be mut from the start.)
        orders.resize(8, placeholder)?;
        println!("\nAfter resize to 8 (placeholder-filled):");
        print_orders(&orders)?;

        // Truncate back to the real data.
        orders.truncate(4)?;
        println!("\nAfter truncate to 4:");
        print_orders(&orders)?;

        // Clean up via dealloc.
        alloc.dealloc(orders.into_raw_block())?;
    }

    // Remove the example file.
    std::fs::remove_file(&path)?;
    println!("\nDone.");
    Ok(())
}
