//! Checksummed block with in-memory cache protected by CRDS operations.
//!
//! ## Layout
//!
//! A single 64-byte block stored at offset 0:
//! - Bytes [0..8): 8-byte XOR checksum (LE u64)
//! - Bytes [8..64): 56-byte payload
//!
//! ## Write path
//!
//! Use `eq_crds` to compare the cached checksum against the on-disk checksum
//! and, if they match, atomically replace the entire block.  On a mismatch the
//! stale cache is refreshed via `get_into` and the call returns `false` so the
//! caller can retry.
//!
//! ## Read path
//!
//! Use `get_batched_gen` to read the checksum and, only on a mismatch with the
//! cached value, fetch the new payload — both under a single read lock.
//! 
//! Note that XOR checksums are not collision-resistant, so this example is only
//! intended to demonstrate the pattern, not to provide strong integrity guarantees.
//!
//! ## Thread model
//!
//! The underlying `BStack` is wrapped in an `Arc` and shared across threads.
//! The per-thread cache (`cached_checksum`, `cached_payload`) lives in each
//! thread's own `ChecksummedBlock` instance, constructed inside the thread.
//!
//! ## How to run
//!
//! ```text
//! cargo run --example checksummed_cache --features "set,atomic"
//! ```

#[cfg(all(feature = "set", feature = "atomic"))]
use bstack::BStack;
#[cfg(all(feature = "set", feature = "atomic"))]
use std::io;
#[cfg(all(feature = "set", feature = "atomic"))]
use std::sync::{Arc, Barrier};
#[cfg(all(feature = "set", feature = "atomic"))]
use std::thread;

#[cfg(all(feature = "set", feature = "atomic"))]
const BLOCK_SIZE: usize = 64;
#[cfg(all(feature = "set", feature = "atomic"))]
const PAYLOAD_OFFSET: u64 = 8;
#[cfg(all(feature = "set", feature = "atomic"))]
const PAYLOAD_SIZE: usize = 56;

#[cfg(all(feature = "set", feature = "atomic"))]
struct ChecksummedBlock {
    stack: Arc<BStack>,
    /// Per-thread cache — each thread owns its own ChecksummedBlock.
    cached_checksum: Option<[u8; 8]>,
    cached_payload: [u8; PAYLOAD_SIZE],
}

#[cfg(all(feature = "set", feature = "atomic"))]
impl ChecksummedBlock {
    /// Initialise the backing file and return a shared stack handle.
    fn open(path: &str) -> io::Result<Arc<BStack>> {
        let _ = std::fs::remove_file(path);
        let stack = BStack::open(path)?;
        stack.push(&[0u8; BLOCK_SIZE])?;
        Ok(Arc::new(stack))
    }

    /// Create a per-thread view over a shared stack.
    fn new(stack: Arc<BStack>) -> Self {
        Self {
            stack,
            cached_checksum: None,
            cached_payload: [0u8; PAYLOAD_SIZE],
        }
    }

    /// Compute XOR checksum of the payload.
    fn compute_checksum(payload: &[u8]) -> u64 {
        payload.iter().fold(0u64, |acc, &byte| acc ^ (byte as u64))
    }

    /// Write `new_payload` atomically.
    ///
    /// Returns `true` if the write was committed, or `false` if the cached
    /// checksum was stale.  On a stale miss the cache is refreshed from the
    /// file via `get_into` so the caller can retry immediately.
    fn write(&mut self, new_payload: &[u8]) -> io::Result<bool> {
        assert!(new_payload.len() <= PAYLOAD_SIZE);

        let new_checksum = Self::compute_checksum(new_payload);
        let mut block = [0u8; BLOCK_SIZE];
        block[0..8].copy_from_slice(&new_checksum.to_le_bytes());
        block[PAYLOAD_OFFSET as usize..PAYLOAD_OFFSET as usize + new_payload.len()]
            .copy_from_slice(new_payload);

        // Verify cached checksum (or expect zeroed checksum for first write).
        let expected_checksum = self.cached_checksum.unwrap_or([0u8; 8]);

        if self
            .stack
            .eq_crds(0, &expected_checksum, 0, block)?
            .is_some()
        {
            // Committed — update the cache to reflect the new file state.
            self.cached_checksum = Some(new_checksum.to_le_bytes());
            self.cached_payload[..new_payload.len()].copy_from_slice(new_payload);
            self.cached_payload[new_payload.len()..].fill(0);
            return Ok(true);
        }

        // Cache is stale — refresh it so the caller can retry.
        let mut disk = [0u8; BLOCK_SIZE];
        self.stack.get_into(0, &mut disk)?;
        self.cached_checksum = Some(disk[0..8].try_into().unwrap());
        self.cached_payload
            .copy_from_slice(&disk[PAYLOAD_OFFSET as usize..]);
        Ok(false)
    }

    fn read(&mut self) -> io::Result<&[u8]> {
        let mut need_checksum_read = true;
        let mut read_checksum = [0u8; 8];
        self.stack.get_batched_gen(|| {
            if need_checksum_read {
                need_checksum_read = false;
                Some((
                    0,
                    // SAFETY: `read_checksum` lives for the duration of get_batched_gen
                    unsafe { std::slice::from_raw_parts_mut(read_checksum.as_mut_ptr(), 8) },
                ))
            } else {
                if let Some(old_checksum) = self.cached_checksum
                    && read_checksum == old_checksum
                {
                    return None;
                }
                self.cached_checksum = Some(read_checksum);
                Some((
                    PAYLOAD_OFFSET,
                    // SAFETY: `cached_payload` lives for the duration of get_batched_gen
                    unsafe {
                        std::slice::from_raw_parts_mut(
                            self.cached_payload.as_mut_ptr(),
                            PAYLOAD_SIZE,
                        )
                    },
                ))
            }
        })?;
        Ok(&self.cached_payload)
    }
}

/// Return the payload bytes up to the last non-zero byte.
#[cfg(all(feature = "set", feature = "atomic"))]
fn trim(payload: &[u8]) -> &str {
    let len = payload.iter().rposition(|&b| b != 0).map_or(0, |i| i + 1);
    std::str::from_utf8(&payload[..len]).unwrap_or("")
}

#[cfg(all(feature = "set", feature = "atomic"))]
fn main() -> io::Result<()> {
    let stack = ChecksummedBlock::open("checksummed_cache_example.bstack")?;

    // A barrier lets thread 1 commit its write before thread 2 attempts its own,
    // guaranteeing that thread 2's cache is stale when it first tries to write.
    let barrier = Arc::new(Barrier::new(2));

    // ── Thread 1: write first, then release thread 2 ──────────────────────────
    let stack1 = Arc::clone(&stack);
    let barrier1 = Arc::clone(&barrier);
    let t1 = thread::spawn(move || -> io::Result<()> {
        let mut block = ChecksummedBlock::new(stack1);

        assert!(block.write(b"Written by thread 1.")?);
        println!("[t1] write          → committed");

        let payload = block.read()?;
        println!("[t1] read           → hit  {:?}", trim(payload));

        barrier1.wait(); // release thread 2
        Ok(())
    });

    // ── Thread 2: wait for thread 1, then write with a stale cache ────────────
    let stack2 = Arc::clone(&stack);
    let barrier2 = Arc::clone(&barrier);
    let t2 = thread::spawn(move || -> io::Result<()> {
        let mut block = ChecksummedBlock::new(stack2);

        barrier2.wait(); // thread 1 has written by now; our cache (None) is stale

        // First attempt: stale — eq_crds sees a checksum mismatch, cache is
        // refreshed via get_into, returns false.
        let committed = block.write(b"Written by thread 2.")?;
        println!("[t2] write (stale)  → committed={committed}, cache refreshed");

        // Retry: cache is now current, write succeeds.
        let committed = block.write(b"Written by thread 2.")?;
        println!("[t2] write (retry)  → committed={committed}");

        let payload = block.read()?;
        println!("[t2] read           → hit  {:?}", trim(payload));

        Ok(())
    });

    t1.join().unwrap()?;
    t2.join().unwrap()?;
    Ok(())
}

#[cfg(not(all(feature = "set", feature = "atomic")))]
fn main() {
    eprintln!("This example requires the 'set' and 'atomic' features.");
    eprintln!("Run: cargo run --example checksummed_cache --features \"set,atomic\"");
}
