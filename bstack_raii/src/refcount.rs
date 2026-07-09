//! Atomic operations on on-disk `u64` counters, built on [`bstack::BStack::cas`].
//!
//! Every counter is stored **little-endian** (fixed by the `bstack` ABI). Each
//! function takes the absolute byte offset of the counter within the stack
//! payload. `cas` gives a byte-range compare-and-swap; the increment/decrement
//! helpers wrap it in a read-modify-CAS retry loop.

use std::io;

use bstack::BStack;

/// Load the current value of the counter at `offset` (little-endian).
pub fn load(stack: &BStack, offset: u64) -> io::Result<u64> {
    todo!("read 8 LE bytes at offset")
}

/// Atomically add `delta`, returning the previous value. Retries on contention.
pub fn fetch_add(stack: &BStack, offset: u64, delta: u64) -> io::Result<u64> {
    todo!("read-modify-CAS loop: load, cas(old -> old + delta), retry on mismatch")
}

/// Atomically subtract `delta`, returning the previous value. Retries on
/// contention. Callers must ensure the counter never underflows.
pub fn fetch_sub(stack: &BStack, offset: u64, delta: u64) -> io::Result<u64> {
    todo!("read-modify-CAS loop: load, cas(old -> old - delta), retry on mismatch")
}

/// Increment the counter only if it is currently non-zero, returning the new
/// value on success or `None` if it was zero. This is the primitive behind
/// [`crate::BStackWeak::upgrade`]: it must not resurrect a counter that a
/// concurrent drop has already driven to zero.
pub fn increment_if_nonzero(stack: &BStack, offset: u64) -> io::Result<Option<u64>> {
    todo!("read-modify-CAS loop, bailing out with None when the loaded value is 0")
}
