//! Shared on-disk plumbing for the stdlib collections.
//!
//! These helpers are the common core the pointer-based containers
//! ([`crate::BStackLinkedList`], [`crate::stdlib::BStackDeque`]) build their
//! atomic mutators on: a `u64` field read, a whole-image block allocation, and
//! the [`atomic_update`] read-modify-write generator.

use std::io;

use bstack::{BStack, BStackGenOp, BStackOwnedSliceAllocator, BStackRange};

/// Read a little-endian `u64` at absolute offset `off`.
pub(super) fn get_u64(stack: &BStack, off: u64) -> io::Result<u64> {
    let mut b = [0u8; 8];
    stack.get_into(off, &mut b)?;
    Ok(u64::from_le_bytes(b))
}

/// Inline capacity of a [`Scratch`] buffer. Sized to hold a B-tree node (or a
/// map bucket / key) for any reasonably-small `Pod` key entirely on the stack;
/// unusually large keys spill to the heap. A `BStackBTreeMap` node with minimum
/// degree 8 is `280 + 15 * size_of::<K>()` bytes, so this covers keys up to
/// ~49 bytes without a heap allocation.
const SCRATCH_INLINE: usize = 1024;

/// A reusable read buffer whose storage is **inline (stack)** for the common
/// small case and spills to the heap only when a requested length exceeds
/// [`SCRATCH_INLINE`].
///
/// This lets the hot lookup paths (`get`) read a whole node/bucket without a heap
/// allocation for typical `Pod` keys, while imposing **no** compile-time cap on
/// the key size — the generic-dependent buffer length rules out a plain stack
/// array (`[u8; node_size()]` is rejected as "constant expression depends on a
/// generic parameter"), so this hand-rolled small-buffer stands in for it.
pub(super) struct Scratch {
    inline: [u8; SCRATCH_INLINE],
    spill: Vec<u8>,
}

impl Scratch {
    pub(super) fn new() -> Self {
        Scratch {
            inline: [0u8; SCRATCH_INLINE],
            spill: Vec::new(),
        }
    }

    /// A `&mut [u8]` of length `n` to read into. The bytes are not cleared — the
    /// caller overwrites the whole slice with a `get_into`.
    pub(super) fn buf(&mut self, n: usize) -> &mut [u8] {
        if n <= SCRATCH_INLINE {
            &mut self.inline[..n]
        } else {
            if self.spill.len() < n {
                self.spill.resize(n, 0);
            }
            &mut self.spill[..n]
        }
    }
}

/// Allocate a block and write `bytes` as its whole image (one write; released
/// without leaking on write failure).
pub(super) fn alloc_image<A: BStackOwnedSliceAllocator>(
    allocator: &A,
    bytes: &[u8],
) -> io::Result<BStackRange> {
    let mut slice = allocator.alloc(bytes.len() as u64)?;
    if let Err(e) = slice.write_range(0, bytes) {
        let _ = allocator.dealloc(slice);
        return Err(e);
    }
    Ok(slice.as_range())
}

/// Commit an atomic, **external-lock-free** read-modify-write to a container's
/// on-disk metadata via [`BStack::inplace_gen`].
///
/// `reads1` are absolute offsets of `u64` slots read in a first round; the values
/// are handed to `reads2` to compute a second round of offsets that may *depend*
/// on the first (e.g. the `prev`/`value` slots of a node found via a pointer, or
/// the live element slots of a ring found via `head`/`cap`). `plan` then turns
/// both read rounds into the writes to commit.
///
/// The point of routing every mutator through this: all reads happen **inside**
/// the generator, under bstack's single write lock, so the values reflect the
/// committed state at the one commit point and no other thread can interleave
/// between the reads and the dependent writes — no external lock, no torn
/// structure. Every write lands as one crash-atomic batch (all-or-nothing).
///
/// Only in-place reads/writes ride the generator; allocations and frees, which
/// change the stack's size, are done by the caller *around* it (a freshly
/// allocated block is an orphan until the commit links it; a freed block is
/// already unlinked), so a crash can at worst leak, never tear the structure.
pub(super) fn atomic_update<A, R2, W>(
    allocator: &A,
    reads1: &[u64],
    reads2: R2,
    plan: W,
) -> io::Result<()>
where
    A: BStackOwnedSliceAllocator,
    R2: FnOnce(&[u64]) -> Vec<u64>,
    W: FnOnce(&[u64], &[u64]) -> Vec<(u64, Vec<u8>)>,
{
    // Buffers that must outlive the whole `inplace_gen` call (bstack's documented
    // generator pattern): read-back values and the computed writes.
    let mut buf1: Vec<[u8; 8]> = vec![[0u8; 8]; reads1.len()];
    let mut vals1: Vec<u64> = Vec::new();
    let mut offs2: Vec<u64> = Vec::new();
    let mut buf2: Vec<[u8; 8]> = Vec::new();
    let mut writes: Vec<(u64, Vec<u8>)> = Vec::new();

    let mut reads2 = Some(reads2);
    let mut plan = Some(plan);

    let mut r1 = 0usize;
    let mut did_a = false;
    let mut r2 = 0usize;
    let mut did_b = false;
    let mut w = 0usize;

    allocator.stack().inplace_gen(|_feedback| {
        // Round 1 — read the fixed offsets.
        if r1 < reads1.len() {
            let i = r1;
            r1 += 1;
            // SAFETY: `buf1` outlives this call; one read op uses slot `i` at a time.
            let b: &mut [u8] = unsafe { core::mem::transmute::<&mut [u8], _>(&mut buf1[i][..]) };
            return Some(BStackGenOp::Read {
                offset: reads1[i],
                buf: b,
            });
        }
        // Transition A — compute the (possibly dependent) round-2 offsets.
        if !did_a {
            did_a = true;
            vals1 = buf1.iter().map(|x| u64::from_le_bytes(*x)).collect();
            offs2 = (reads2.take().unwrap())(&vals1);
            buf2 = vec![[0u8; 8]; offs2.len()];
        }
        // Round 2 — read the dependent offsets.
        if r2 < offs2.len() {
            let i = r2;
            r2 += 1;
            // SAFETY: `buf2` outlives this call and is not resized after Transition A.
            let b: &mut [u8] = unsafe { core::mem::transmute::<&mut [u8], _>(&mut buf2[i][..]) };
            return Some(BStackGenOp::Read {
                offset: offs2[i],
                buf: b,
            });
        }
        // Transition B — compute the writes from both read rounds.
        if !did_b {
            did_b = true;
            let vals2: Vec<u64> = buf2.iter().map(|x| u64::from_le_bytes(*x)).collect();
            writes = (plan.take().unwrap())(&vals1, &vals2);
        }
        // Commit phase — emit every write; they land together atomically.
        if w < writes.len() {
            let i = w;
            w += 1;
            let (off, ref bytes) = writes[i];
            // SAFETY: `writes` outlives this call and is not mutated after Transition B.
            let d: &[u8] = unsafe { core::mem::transmute::<&[u8], _>(bytes.as_slice()) };
            return Some(BStackGenOp::Write { offset: off, data: d });
        }
        None
    })
}
