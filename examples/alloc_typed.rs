//! Typed persistent records using `FirstFitBStackAllocator`.
//!
//! Shows how to build a simple key-value store where each entry is a
//! fixed-size struct serialised into a `BStackSlice`.  Slice handles are
//! themselves serialised as 16-byte tokens (via `From<BStackSlice>`) so they
//! survive a close/reopen cycle and can be used to update or delete specific
//! records without scanning the whole file.
//!
//! ## On-disk layout per entry
//!
//! ```text
//! [ key: u64 LE | value: 56 bytes | _pad: 0 bytes ] = 64-byte payload
//! ```
//!
//! ## Token storage
//!
//! The example stores the 16-byte `[u8; 16]` slice tokens in a plain `Vec`
//! in memory.  In a real application you would append them to a separate
//! bstack (or any durable store) so the token list also survives restarts.
//!
//! Run:
//!
//! ```sh
//! cargo run --example alloc_typed --features alloc,set
//! ```

#[cfg(all(feature = "alloc", feature = "set"))]
use bstack::{BStack, BStackAllocator, BStackSlice, FirstFitBStackAllocator};
#[cfg(all(feature = "alloc", feature = "set"))]
use std::io;

// -----------------------------------------------------------------------
// Record layout: 8-byte key + 56-byte value, total 64 bytes.
// -----------------------------------------------------------------------
#[cfg(all(feature = "alloc", feature = "set"))]
const RECORD_SIZE: u64 = 64;
#[cfg(all(feature = "alloc", feature = "set"))]
const VALUE_SIZE: usize = 56;

#[cfg(all(feature = "alloc", feature = "set"))]
struct Record {
    key: u64,
    value: [u8; VALUE_SIZE],
}

#[cfg(all(feature = "alloc", feature = "set"))]
impl Record {
    fn to_bytes(&self) -> [u8; RECORD_SIZE as usize] {
        let mut buf = [0u8; RECORD_SIZE as usize];
        buf[..8].copy_from_slice(&self.key.to_le_bytes());
        buf[8..8 + VALUE_SIZE].copy_from_slice(&self.value);
        buf
    }

    fn from_bytes(buf: &[u8]) -> Self {
        let key = u64::from_le_bytes(buf[..8].try_into().unwrap());
        let mut value = [0u8; VALUE_SIZE];
        value.copy_from_slice(&buf[8..8 + VALUE_SIZE]);
        Record { key, value }
    }
}

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

/// Write `record` into a fresh allocation and return the 16-byte token.
#[cfg(all(feature = "alloc", feature = "set"))]
fn insert(alloc: &FirstFitBStackAllocator, record: &Record) -> io::Result<[u8; 16]> {
    let slice = alloc.alloc(RECORD_SIZE)?;
    slice.write(&record.to_bytes())?;
    Ok(<[u8; 16]>::from(slice))
}

/// Read the record pointed to by `token`.
#[cfg(all(feature = "alloc", feature = "set"))]
fn read(alloc: &FirstFitBStackAllocator, token: &[u8; 16]) -> io::Result<Record> {
    let slice = BStackSlice::from_bytes(alloc, *token);
    let buf = slice.read()?;
    Ok(Record::from_bytes(&buf))
}

/// Overwrite the record pointed to by `token` with new data.
#[cfg(all(feature = "alloc", feature = "set"))]
fn update(alloc: &FirstFitBStackAllocator, token: &[u8; 16], record: &Record) -> io::Result<()> {
    let slice = BStackSlice::from_bytes(alloc, *token);
    slice.write(&record.to_bytes())
}

/// Free the record pointed to by `token`.
#[cfg(all(feature = "alloc", feature = "set"))]
fn delete(alloc: &FirstFitBStackAllocator, token: &[u8; 16]) -> io::Result<()> {
    let slice = BStackSlice::from_bytes(alloc, *token);
    alloc.dealloc(slice)
}

// -----------------------------------------------------------------------
// Main
// -----------------------------------------------------------------------
#[cfg(all(feature = "alloc", feature = "set"))]
fn main() -> io::Result<()> {
    let path = "alloc_typed_example.bstack";

    // ------------------------------------------------------------------
    // Session 1: insert three records and persist their tokens.
    // ------------------------------------------------------------------
    let tokens: Vec<[u8; 16]>;
    {
        let alloc = FirstFitBStackAllocator::new(BStack::open(path)?)?;

        let mut t = Vec::new();
        for i in 0u64..3 {
            let mut value = [0u8; VALUE_SIZE];
            let msg = format!("entry {i}");
            value[..msg.len()].copy_from_slice(msg.as_bytes());
            t.push(insert(&alloc, &Record { key: i, value })?);
            println!("inserted key={i} at token {:?}", &t.last().unwrap()[..8]);
        }
        tokens = t;

        drop(alloc.into_stack());
    }

    // ------------------------------------------------------------------
    // Session 2: reopen, read all, update one, delete one.
    // ------------------------------------------------------------------
    {
        let alloc = FirstFitBStackAllocator::new(BStack::open(path)?)?;

        println!("\nsession 2 — reading all records:");
        for tok in &tokens {
            let r = read(&alloc, tok)?;
            println!(
                "  key={} value={:?}",
                r.key,
                String::from_utf8_lossy(r.value.split(|&b| b == 0).next().unwrap_or(&[]))
            );
        }

        // Update record 1.
        let mut new_value = [0u8; VALUE_SIZE];
        new_value[..7].copy_from_slice(b"updated");
        update(
            &alloc,
            &tokens[1],
            &Record {
                key: 1,
                value: new_value,
            },
        )?;
        println!("\nupdated key=1");

        // Delete record 0.
        delete(&alloc, &tokens[0])?;
        println!("deleted key=0");

        // Verify the update and that a new alloc reuses the deleted slot.
        let r1 = read(&alloc, &tokens[1])?;
        println!(
            "key=1 now: {:?}",
            String::from_utf8_lossy(r1.value.split(|&b| b == 0).next().unwrap_or(&[]))
        );

        let new_slot = alloc.alloc(RECORD_SIZE)?;
        println!(
            "new alloc at offset {} (deleted key=0 was at offset {})",
            new_slot.start(),
            BStackSlice::from_bytes(&alloc, tokens[0]).start(),
        );

        let _ = new_slot;
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
