//! Atomic operations on on-disk `u64` counters, built on [`bstack::BStack`].
//!
//! Every counter is stored **little-endian** (fixed by the `bstack` ABI). Each
//! function takes the absolute payload offset of the counter within the stack.
//!
//! The read-modify-write helpers use [`BStack::process`], which reads the
//! counter, runs a closure to mutate it in place, and writes it back — all under
//! one held write lock, crash-atomically. That does the whole RMW in a single
//! lock acquisition with **no compare-and-swap spin loop**. (`BStack::cas` would
//! also be correct here, since each counter's dangerous value — zero — is a sink
//! state and so immune to ABA; `process` is preferred purely to avoid retrying.)
//!
//! Because `process`'s closure returns `()` and always writes the buffer back,
//! the error paths (overflow / underflow) signal out through a captured flag and
//! leave the buffer *unchanged*, so the write-back is a no-op on those paths.

use std::io;

use bstack::BStack;

fn read_u64(buf: &[u8]) -> u64 {
    u64::from_le_bytes(buf[..8].try_into().unwrap())
}

fn overflow_err() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "refcount overflow")
}

fn underflow_err() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "refcount underflow")
}

/// Load the current value of the counter at `offset` (little-endian). Read-only,
/// so it takes only `get_into` (no lock upgrade, no write-back).
pub fn load(stack: &BStack, offset: u64) -> io::Result<u64> {
    let mut bytes = [0u8; 8];
    stack.get_into(offset, &mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

/// Atomically add `delta`, returning the previous value. Errors on overflow
/// rather than wrapping (leaving the counter unchanged in that case).
pub fn fetch_add(stack: &BStack, offset: u64, delta: u64) -> io::Result<u64> {
    let mut prev = 0u64;
    let mut overflow = false;
    stack.process(offset, offset + 8, |buf| {
        let cur = read_u64(buf);
        prev = cur;
        match cur.checked_add(delta) {
            Some(new) => buf.copy_from_slice(&new.to_le_bytes()),
            None => overflow = true, // leave buf unchanged; report below
        }
    })?;
    if overflow {
        return Err(overflow_err());
    }
    Ok(prev)
}

/// Atomically subtract `delta`, returning the previous value. Errors on
/// underflow rather than wrapping (leaving the counter unchanged in that case).
pub fn fetch_sub(stack: &BStack, offset: u64, delta: u64) -> io::Result<u64> {
    let mut prev = 0u64;
    let mut underflow = false;
    stack.process(offset, offset + 8, |buf| {
        let cur = read_u64(buf);
        prev = cur;
        match cur.checked_sub(delta) {
            Some(new) => buf.copy_from_slice(&new.to_le_bytes()),
            None => underflow = true, // leave buf unchanged; report below
        }
    })?;
    if underflow {
        return Err(underflow_err());
    }
    Ok(prev)
}

/// Increment the counter only if it is currently non-zero, returning the new
/// value on success or `None` if it was zero. The primitive behind
/// [`crate::BStackWeak::upgrade`]: it must never resurrect a counter that a
/// concurrent drop has already driven to zero.
///
/// A read-only fast path returns `None` without any write when the counter is
/// already zero (the common "the object is long dead" case); zero is terminal,
/// so that observation is authoritative. When the fast path sees non-zero, the
/// `process` closure re-checks under the lock — the value may have raced to zero
/// in between — before committing the increment.
pub fn increment_if_nonzero(stack: &BStack, offset: u64) -> io::Result<Option<u64>> {
    if load(stack, offset)? == 0 {
        return Ok(None);
    }
    let mut result = None;
    let mut overflow = false;
    stack.process(offset, offset + 8, |buf| {
        let cur = read_u64(buf);
        if cur == 0 {
            return; // raced to zero after the fast-path read; leave unchanged
        }
        match cur.checked_add(1) {
            Some(new) => {
                buf.copy_from_slice(&new.to_le_bytes());
                result = Some(new);
            }
            None => overflow = true,
        }
    })?;
    if overflow {
        return Err(overflow_err());
    }
    Ok(result)
}
