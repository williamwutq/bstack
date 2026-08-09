//! Shared on-disk plumbing for the stdlib collections.
//!
//! These helpers are the common core the pointer-based containers
//! ([`crate::BStackLinkedList`], [`crate::stdlib::BStackDeque`]) build their
//! atomic mutators on: a `u64` field read, a whole-image block allocation, and
//! the [`atomic_update`] read-modify-write generator.

use std::io;

use crate::wal::BStackWalAnchor;
use bstack::{BStack, BStackGenOp, BStackOwnedSliceAllocator, BStackRange};

use crate::layout::{HEADER_SIZE, get_u64};

/// Read a little-endian `u64` at absolute offset `off`.
pub(super) fn read_u64(stack: &BStack, off: u64) -> io::Result<u64> {
    let mut b = [0u8; 8];
    stack.get_into(off, &mut b)?;
    Ok(u64::from_le_bytes(b))
}

/// Read `N` **contiguous** little-endian `u64` fields starting at `off` in a
/// *single* I/O call, returning them as an array. Use this instead of several
/// [`read_u64`] calls when the fields are adjacent (e.g. a handle's metadata) —
/// one `get_into` is one lock/seek/read, not `N`.
pub(super) fn read_fields<const N: usize>(stack: &BStack, off: u64) -> io::Result<[u64; N]> {
    debug_assert!(N <= 8, "read_fields: at most 8 u64 fields");
    let buf = &mut [0u8; 64][..N * 8];
    stack.get_into(off, buf)?;
    let mut out = [0u64; N];
    #[allow(clippy::chunks_exact_to_as_chunks)]
    for (dst, chunk) in out.iter_mut().zip(buf.chunks_exact(8)) {
        *dst = get_u64(chunk);
    }
    Ok(out)
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
pub(super) fn alloc_image<A: BStackWalAnchor>(
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
    A: BStackWalAnchor,
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
            return Some(BStackGenOp::Write {
                offset: off,
                data: d,
            });
        }
        None
    })
}

/// A snapshot of an open-addressing table's four handle metadata fields
/// (`table`, `cap`, `len`, `used`), read inside a generator. Both
/// [`crate::BStackHashMap`] and [`crate::stdlib::BStackHashSet`] lay these out
/// contiguously at `handle + HEADER_SIZE`.
pub(super) struct Meta {
    pub(super) table: u64,
    pub(super) cap: u64,
    pub(super) len: u64,
    pub(super) used: u64,
}

/// A probe step returned by the `inspect` closure of [`probe_commit`].
pub(super) enum ProbeStep {
    /// This bucket isn't the target — keep probing.
    Continue,
    /// Stop here and commit these writes (empty = commit nothing).
    Stop(Vec<(u64, Vec<u8>)>),
}

/// Run an atomic, external-lock-free linear probe over an open-addressing bucket
/// table under one [`BStack::inplace_gen`].
///
/// Reads the four-`u64` handle metadata (at `handle + HEADER_SIZE`), then linearly
/// probes buckets from `hash & (cap-1)`, reading the full `stride`-byte bucket
/// each step and handing it to `inspect`. The first `inspect` returning
/// [`ProbeStep::Stop`] commits its writes and ends; if all `cap` buckets are
/// probed without a stop, `exhausted` produces the final writes. Every read and
/// write rides the one generator, so the probe sees a consistent snapshot and the
/// writes land as one crash-atomic batch. Shared by the hash map and hash set.
pub(super) fn probe_commit<A, I, E>(
    allocator: &A,
    handle: u64,
    stride: u64,
    hash: u64,
    mut inspect: I,
    exhausted: E,
) -> io::Result<()>
where
    A: BStackWalAnchor,
    I: FnMut(&Meta, u64, &[u8]) -> ProbeStep,
    E: FnOnce(&Meta) -> Vec<(u64, Vec<u8>)>,
{
    let mut meta_buf = [0u8; 32];
    let mut bucket_buf = vec![0u8; stride as usize];
    let mut writes: Vec<(u64, Vec<u8>)> = Vec::new();

    let mut meta_issued = false;
    let mut meta: Option<Meta> = None;
    let mut mask = 0u64;
    let mut cur = 0u64;
    let mut idx_at_read = 0u64;
    let mut probe_pending = false;
    let mut probed = 0u64;
    let mut decided = false;
    let mut exhausted = Some(exhausted);
    let mut w = 0usize;

    allocator.stack().inplace_gen(|_feedback| {
        // 1. Read the 32-byte metadata block.
        if !meta_issued {
            meta_issued = true;
            // SAFETY: `meta_buf` outlives the call; used by this one read.
            let b: &mut [u8] = unsafe { core::mem::transmute::<&mut [u8], _>(&mut meta_buf[..]) };
            return Some(BStackGenOp::Read {
                offset: handle + HEADER_SIZE,
                buf: b,
            });
        }
        // 2. Parse it once.
        if meta.is_none() {
            let m = Meta {
                table: get_u64(&meta_buf[0..8]),
                cap: get_u64(&meta_buf[8..16]),
                len: get_u64(&meta_buf[16..24]),
                used: get_u64(&meta_buf[24..32]),
            };
            mask = m.cap.wrapping_sub(1);
            cur = if m.cap == 0 { 0 } else { hash & mask };
            meta = Some(m);
        }
        let m = meta.as_ref().unwrap();

        // 3a. Inspect a completed bucket read.
        if probe_pending {
            probe_pending = false;
            if let ProbeStep::Stop(ws) = inspect(m, idx_at_read, &bucket_buf) {
                writes = ws;
                decided = true;
            }
        }
        // 3b. Issue the next probe, or finish by exhaustion.
        if !decided {
            if m.cap == 0 || probed >= m.cap {
                writes = (exhausted.take().unwrap())(m);
                decided = true;
            } else {
                idx_at_read = cur;
                probe_pending = true;
                probed += 1;
                cur = (cur + 1) & mask;
                let off = m.table + idx_at_read * stride;
                // SAFETY: `bucket_buf` outlives the call; each read completes
                // (and is inspected) before the next is issued.
                let b: &mut [u8] =
                    unsafe { core::mem::transmute::<&mut [u8], _>(&mut bucket_buf[..]) };
                return Some(BStackGenOp::Read {
                    offset: off,
                    buf: b,
                });
            }
        }
        // 4. Commit the chosen writes together.
        if w < writes.len() {
            let i = w;
            w += 1;
            let (off, ref bytes) = writes[i];
            // SAFETY: `writes` outlives the call and is not mutated after this point.
            let d: &[u8] = unsafe { core::mem::transmute::<&[u8], _>(bytes.as_slice()) };
            return Some(BStackGenOp::Write {
                offset: off,
                data: d,
            });
        }
        None
    })
}
