//! Segregated (binned) free-list allocator for [`BStack`]-backed storage.
//!
//! Provides [`SegregatedBStackAllocator`], which generalises the fixed-block
//! slab to `NUM_CLASSES` size classes sharing one arena. Each class is an
//! independent intrusive free list; `alloc` computes the class from the request
//! with register arithmetic and pops a single fixed-shape head, falling back to
//! a tail extension on a miss (the slab `pop, else extend` rule). Requests above
//! the largest class collapse onto one shared *oversized* list.
//!
//! The size→class policy (quantum 16, `linear_max`, `subclass_bits`,
//! `max_class`, and the resulting `NUM_CLASSES`) is a compile-time constant
//! encoded by the magic version, not per-file state — a format change bumps the
//! magic rather than reinterpreting a stored field.
//!
//! Implemented: `new`/`open`, `alloc`, `dealloc`, `realloc`, and `recover`
//! (linear-scan free-list rebuild + leak reclaim). Still pending: the background
//! coalescer.
//!
//! # Feature flags
//!
//! Requires `set`; `atomic` is optional. The module compiles and is fully
//! functional under both. With `atomic`, free-list splices ride
//! `process_gen`/`inplace_gen` (write lock held across the dependent read →
//! modify → write); the tail grow and oversized-discard paths use the
//! size-guarded `try_extend_zeros`/`try_discard` — one locked critical section
//! that fuses the tail check with the mutation — and the tail shrink rides a
//! `Len` + `Atrunc` `process_gen`, which likewise fuses the tail check with the
//! block replacement. Together these also make the allocator `Sync`. Without
//! `atomic`, the splices become a plain read-then-write and the tail grow a
//! `len`-check then `extend`: still crash-safe (each issues a single `bstack`
//! write, and multi-write splices are leak-preferring), but the allocator is
//! `Send` and **not** `Sync`, so concurrent use must be externally synchronised.
//! A *shrink* reclaims its freed excess (tail `Atrunc` or an in-place carve) only
//! under `atomic`, where the transaction fuses recording the new physical size
//! with dropping the excess; the non-`atomic` build cannot fuse them without a
//! crash window `recover` mis-parses, so it simply **retains** the excess inside
//! the still-recorded larger block (zero extra writes, no move).

use super::{
    BStackAllocError, BStackAllocator, BStackOwnedSlice, BStackUninitAllocator, ensure_own_handle,
};
use crate::BStack;
#[cfg(feature = "atomic")]
use crate::BStackGenOp;
#[cfg(feature = "atomic")]
use crate::{bstack_unsafe_reborrow, bstack_unsafe_reborrow_mut};
#[cfg(not(feature = "atomic"))]
use std::cell::Cell;
#[cfg(not(feature = "atomic"))]
use std::marker::PhantomData;
use std::{fmt, io};

/// Magic: `ALSG` + major 0 + minor 2; the version encodes the fixed class scheme
/// and the in-use overhead recording the block's physical size (minor 1 recorded
/// the caller's length instead — a `\x01` file fails `new` with `InvalidData`).
const ALSG_MAGIC: [u8; 8] = *b"ALSG\x00\x02\x00\x00";
/// Compatibility prefix checked on open (`ALSG` + major 0 + minor 2).
const ALSG_MAGIC_PREFIX: [u8; 6] = *b"ALSG\x00\x02";

/// A segregated free-list allocator implementing [`BStackAllocator`] on top of a
/// [`BStack`].
///
/// # On-disk layout
///
/// ```text
/// offset  0  reserved (user)                24 B
/// offset 24  magic  "ALSG\x00\x02\x00\x00"   8 B
/// offset 32  _reserved                       8 B
/// offset 40  free_head[NUM_CLASSES] : u64          # last entry = oversized list
/// arena start (16-B aligned; header ends 16-aligned already)
/// ```
///
/// Every arena block is `[ overhead(8) | data(block − 8) ]`; the caller pointer
/// is the data start (`block_start + 8`). The overhead is a single tagged word,
/// carrying the **physical block size `>> 4`** in its low 63 bits under both
/// tags: high bit set ⇒ in use, high bit clear ⇒ free (the size then doubling as
/// the class tag). The caller's visible length is *not* recorded on disk — it
/// lives in the returned [`BStackOwnedSlice`] — so a live block may be physically
/// larger than its request needs (retained excess). A free block stores its
/// `next_free` offset inline at the data start, so live allocations carry no
/// space overhead beyond the 8-byte word.
///
/// # Thread safety
///
/// `SegregatedBStackAllocator` is always **`Send`** — ownership can be transferred
/// to another thread safely.
///
/// ```
/// fn assert_send<T: Send>() {}
/// assert_send::<bstack::SegregatedBStackAllocator>();
/// ```
///
/// Under `atomic`, it is also `Sync`: `alloc`/`dealloc` drive [`BStack::process_gen`] /
/// [`BStack::inplace_gen`] sequences that hold `BStack`'s write lock across the
/// dependent read/modify/write, so no allocator-level lock is taken.
/// [`recover`](Self::recover) is the one exception — it is `unsafe` and shifts the
/// no-concurrent-access obligation to the caller (see its `# Safety`).
///
/// Without `atomic` the type is `!Sync` (this fails to compile); with `atomic`
/// it is `Sync` via its [`BStack`] (this compiles):
///
#[cfg_attr(not(feature = "atomic"), doc = "```compile_fail")]
#[cfg_attr(feature = "atomic", doc = "```")]
/// fn assert_sync<T: Sync>() {}
/// assert_sync::<bstack::SegregatedBStackAllocator>();
/// ```
#[cfg(feature = "set")]
pub struct SegregatedBStackAllocator {
    stack: BStack,
    #[cfg(not(feature = "atomic"))]
    _not_sync: PhantomData<Cell<()>>,
}

#[cfg(feature = "set")]
impl SegregatedBStackAllocator {
    // Class scheme (compile-time, encoded by the magic version)
    /// Minimum physical block size and the size quantum; every block is a
    /// multiple of this.
    const QUANTUM: u64 = 16;
    /// Per-block overhead prefix.
    const OVERHEAD: u64 = 8;
    /// Top of the linear (step-16) region.
    const LINEAR_MAX: u64 = 256;
    /// `log2(LINEAR_MAX)` — the first geometric octave.
    const LINEAR_OCTAVE: u32 = 8;
    /// `log2(MAX_CLASS)` — the last geometric octave.
    const MAX_OCTAVE: u32 = 12;
    /// Largest classed physical block; above this is the oversized bucket.
    const MAX_CLASS: u64 = 1 << Self::MAX_OCTAVE; // 4096
    /// Subclasses per geometric octave: `2^SUBCLASS_BITS`.
    const SUBCLASS_BITS: u32 = 2;
    const SUBCLASSES: u64 = 1 << Self::SUBCLASS_BITS; // 4
    /// Linear classes: 16, 32, …, LINEAR_MAX.
    const LINEAR_CLASSES: u64 = Self::LINEAR_MAX / Self::QUANTUM; // 16
    /// Geometric classes across octaves `[LINEAR_OCTAVE, MAX_OCTAVE)`.
    const GEO_CLASSES: u64 = (Self::MAX_OCTAVE - Self::LINEAR_OCTAVE) as u64 * Self::SUBCLASSES; // 16
    /// Total heads: linear + geometric + 1 shared oversized bucket.
    const NUM_CLASSES: u64 = Self::LINEAR_CLASSES + Self::GEO_CLASSES + 1; // 33
    /// Index of the shared oversized free-list head.
    const OVERSIZED_CLASS: u64 = Self::NUM_CLASSES - 1; // 32

    // Header layout (compile-time, fixed offsets)
    /// Bytes before the allocator header reserved for caller use.
    const OFFSET_SIZE: u64 = 24;
    // offset 32: 8 reserved bytes (see the on-disk layout) — no field yet.
    /// Offset of `free_head[0]`.
    const FREE_HEAD_BASE: u64 = 40;
    /// Payload offset of the first arena block: header rounded up to the 16-B
    /// quantum (which every block start must satisfy for the pointer check). The
    /// header already ends 16-aligned, so this is exact: `40 + 33*8 = 304`.
    const ARENA_START: u64 = (Self::FREE_HEAD_BASE + Self::NUM_CLASSES * 8 + 15) & !15;

    /// Maximum pieces a greedy carve emits: a region `> MAX_CLASS` is one
    /// oversized block, and any region `≤ MAX_CLASS` decomposes into `≤ 3`
    /// distinct-class blocks under this scheme (verified over every multiple of
    /// 16 up to `MAX_CLASS`). Lets [`commit_carve`](Self::commit_carve) stay
    /// heap-free with fixed stack buffers.
    const MAX_CARVE_PIECES: usize = 3;

    /// Minimum excess (bytes) worth reclaiming into free blocks instead of
    /// retaining as internal slack. Since this value does not affect on-disk
    /// format, fine-tuning it is not a breaking change.
    const SPLIT_MIN: u64 = Self::LINEAR_MAX;

    /// Free-list sentinel: `0` (offset 0 is the header, never a block).
    const SENTINEL: u64 = 0;
    /// High bit of the overhead word: set when a block is live.
    const IN_USE_BIT: u64 = 0x8000_0000_0000_0000;

    /// Round a caller `len` up to the physical need `round_up(len + 8, 16)`.
    #[inline]
    fn phys_need(len: u64) -> io::Result<u64> {
        let n = len
            .checked_add(Self::OVERHEAD + Self::QUANTUM - 1)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "allocation length overflows u64",
                )
            })?;
        Ok(n & !(Self::QUANTUM - 1))
    }

    /// Snap a physical need (`= round_up(len + 8, 16)`) up to its class block
    /// size. Linear needs pass through; geometric needs round up to the octave
    /// subclass width; oversized needs pass through (raw multiple-of-16 path).
    #[inline]
    fn class_blocksize(need: u64) -> u64 {
        if need <= Self::LINEAR_MAX {
            return need;
        }
        if need <= Self::MAX_CLASS {
            let k = 63 - (need - 1).leading_zeros(); // octave: need ∈ (2^k, 2^{k+1}]
            let w = 1u64 << (k - Self::SUBCLASS_BITS); // subclass width = 2^{k-2}
            return (need + w - 1) & !(w - 1);
        }
        need
    }

    /// Map a physical block size (multiple of 16, ≥ 16) to its free-list head
    /// index. Sizes above `MAX_CLASS` collapse onto the oversized bucket.
    #[inline]
    fn classify(size: u64) -> u64 {
        if size <= Self::LINEAR_MAX {
            return (size >> 4) - 1;
        }
        if size <= Self::MAX_CLASS {
            let k = 63 - (size - 1).leading_zeros();
            let sub = (size - 1 - (1u64 << k)) >> (k - Self::SUBCLASS_BITS);
            return Self::LINEAR_CLASSES
                + (k as u64 - Self::LINEAR_OCTAVE as u64) * Self::SUBCLASSES
                + sub;
        }
        Self::OVERSIZED_CLASS
    }

    /// Largest class block size `≤ v` (with `16 ≤ v ≤ MAX_CLASS`, `v` a multiple
    /// of 16). Snaps `v` **down** to a class boundary — the dual of
    /// [`class_blocksize`](Self::class_blocksize), used by the greedy carve.
    #[inline]
    fn largest_class_le(v: u64) -> u64 {
        if v <= Self::LINEAR_MAX {
            return v; // every multiple of 16 ≤ LINEAR_MAX is itself a class
        }
        let k = 63 - v.leading_zeros(); // 2^k ≤ v < 2^{k+1}
        let w = 1u64 << (k - Self::SUBCLASS_BITS); // subclass width
        v & !(w - 1) // round down to a multiple of w in the octave (a class)
    }

    /// Payload offset of the free-list head for `class`.
    #[inline]
    fn head_off(class: u64) -> u64 {
        Self::FREE_HEAD_BASE + class * core::mem::size_of::<u64>() as u64
    }

    /// Validate a caller data pointer and return its block base offset.
    ///
    /// A valid data pointer is `block_base + OVERHEAD`, where `block_base` is a
    /// 16-aligned arena offset `>= ARENA_START`. Equivalently the pointer is of
    /// the form `16·n + 8`: `ptr ≡ OVERHEAD (mod QUANTUM)` and
    /// `ptr >= ARENA_START + OVERHEAD`. Reject anything else — a header-range,
    /// underflowing, or mid-block pointer would let an operation reinterpret
    /// interior bytes as an overhead word.
    #[inline]
    fn block_start_of(ptr: u64) -> io::Result<u64> {
        if ptr < Self::ARENA_START + Self::OVERHEAD || ptr % Self::QUANTUM != Self::OVERHEAD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "slice start is not a valid block pointer",
            ));
        }
        Ok(ptr - Self::OVERHEAD)
    }

    /// Create a new allocator over `stack` or reopen an existing one.
    ///
    /// If `stack` is empty this writes the header and returns a fresh
    /// allocator. Otherwise it validates the header and arena alignment and
    /// runs recovery before returning the allocator.
    ///
    /// # Errors
    ///
    /// * [`io::ErrorKind::UnexpectedEof`] if the stack is too short to contain
    ///   the header or the arena is not a multiple of the block quantum.
    /// * [`io::ErrorKind::InvalidData`] if the magic is wrong (not a Segregated
    ///   allocator of the expected version).
    /// * Any [`io::Error`] from the underlying [`BStack`] operations.
    pub fn new(stack: BStack) -> io::Result<Self> {
        if stack.is_empty()? {
            // Initialize a new stack: write the header and return a fresh allocator.
            const OFFSET_OFFSET: usize = SegregatedBStackAllocator::OFFSET_SIZE as usize;
            let mut hdr = [0u8; OFFSET_OFFSET + 8];
            hdr[OFFSET_OFFSET..].copy_from_slice(&ALSG_MAGIC);
            // the reserved words and every free_head remain 0.
            let _ = stack.extend_sparse(hdr, Self::ARENA_START)?;
            return Ok(Self {
                stack,
                #[cfg(not(feature = "atomic"))]
                _not_sync: PhantomData,
            });
        }

        // Reopen an existing file
        let stack_len = stack.len()?;
        if stack_len < Self::FREE_HEAD_BASE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "stack too short to contain allocator header",
            ));
        }

        let mut magic = [0u8; 8];
        stack.get_into(Self::OFFSET_SIZE, &mut magic)?;
        if magic[..ALSG_MAGIC_PREFIX.len()] != ALSG_MAGIC_PREFIX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid magic: not a SegregatedBStackAllocator file of the expected version",
            ));
        }
        if stack_len < Self::ARENA_START {
            // Make a zeroed free_head array
            let needed = Self::ARENA_START - stack_len;
            let _ = stack.extend(needed)?;
        } else if (stack_len - Self::ARENA_START) % Self::QUANTUM != 0 {
            // Every block is a multiple of QUANTUM, so the arena byte count is too.
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "arena is not a multiple of the block quantum",
            ));
        }
        let allocator = Self {
            stack,
            #[cfg(not(feature = "atomic"))]
            _not_sync: PhantomData,
        };
        // SAFETY: `allocator` was just constructed and has not yet escaped this
        // function, so no other thread can hold it — it is trivially quiescent.
        unsafe { allocator.recover()? };
        Ok(allocator)
    }

    /// Reclaim blocks leaked by an unclean shutdown and return the count that
    /// could not be classified with certainty (`0` = fully accounted for).
    ///
    /// Rebuilds **every** free list from scratch by a single linear scan of the
    /// arena's overhead words: a live block (high bit set) is strided over by its
    /// **recorded physical size** (the word's low 63 bits `<< 4`), read directly
    /// with no length-to-class derivation; a free block (high bit clear, non-zero)
    /// is relinked by its stored physical `size` onto the head of the largest
    /// class `≤ size` (`classify(largest_class_le(size))`, or the oversized head
    /// above `MAX_CLASS`), which reclaims any block leaked by a crashed `alloc`
    /// pop/claim (still free-tagged but reachable from no head) and degrades a
    /// malformed non-class size to a leak rather than a head that would hand out
    /// more bytes than the block holds. A fully-zeroed region — a crashed tail
    /// `extend` whose overhead write never landed — is discarded as an orphaned
    /// tail.
    ///
    /// Because the scan trusts only the overhead words (never the stored
    /// `next_free` links) and [`new`](Self::new) runs it before any live
    /// operation, it is **idempotent and crash-safe by re-running**: a crash
    /// mid-rebuild leaves half-written links that the next `open`'s scan simply
    /// rebuilds again. Blocks orphaned *in-use* (e.g. the old block of a crashed
    /// realloc move) are not reclaimable by a bare scan and are left live; a deep
    /// reachability GC that could reclaim them (needing a per-op journal of the
    /// affected block, not just a dirty bit) is deferred to a later pass.
    ///
    /// The rebuilt head table is published as a single crash-atomic contiguous
    /// [`BStack::set`], so the table never becomes half-updated. Stops at the first
    /// unclassifiable overhead, counting the remaining arena as unsure.
    ///
    /// # Safety
    ///
    /// The caller must guarantee the allocator is **quiescent** for the duration
    /// of the call: no other thread may run any `alloc`/`dealloc`/`realloc` (or
    /// another `recover`) on this allocator concurrently. `recover` rebuilds every
    /// free list from a single linear snapshot and then replaces the whole head
    /// table wholesale; a concurrent operation between the snapshot and the flip
    /// would be clobbered — resurrecting a just-allocated block onto a free list,
    /// or dropping a just-freed one — which is memory-unsafe for the allocator's
    /// consumers. Construction ([`new`](Self::new)) satisfies this trivially by
    /// running it before the allocator handle escapes.
    pub unsafe fn recover(&self) -> io::Result<u64> {
        let stack_len = self.stack.len()?;
        if stack_len <= Self::ARENA_START {
            return Ok(0);
        }
        let mut heads = [0u64; Self::NUM_CLASSES as usize];
        let mut unsure = 0u64;
        let mut p = Self::ARENA_START;
        while p < stack_len {
            let word = u64::from_le_bytes(read_bstack!(self.stack, p => u64));
            if word & Self::IN_USE_BIT != 0 {
                // Live: stride by the recorded physical size (no derivation).
                let size = (word & !Self::IN_USE_BIT) << 4;
                if size < Self::QUANTUM || size % Self::QUANTUM != 0 || p + size > stack_len {
                    unsure += (stack_len - p) / Self::QUANTUM;
                    break;
                }
                p += size;
            } else if word == 0 {
                // Zeroed tail from a crashed `extend`: discard it (free blocks
                // always store size >> 4 ≥ 1, so a zero word is never valid).
                self.stack.discard(stack_len - p)?;
                break;
            } else {
                // Free: relink by the stored physical size (reclaims leaks too).
                let size = word << 4;
                if size < Self::QUANTUM || size % Self::QUANTUM != 0 || p + size > stack_len {
                    unsure += (stack_len - p) / Self::QUANTUM;
                    break;
                }
                // Relink by the largest class ≤ size so a malformed non-class
                // size degrades to a leak, never a head that overruns the block.
                let c = if size > Self::MAX_CLASS {
                    Self::OVERSIZED_CLASS
                } else {
                    Self::classify(Self::largest_class_le(size))
                } as usize;
                // Prepend: next_free ← current head of this class, then head ← p.
                self.stack.set(p + Self::OVERHEAD, heads[c].to_le_bytes())?;
                heads[c] = p;
                p += size;
            }
        }
        // Publish the rebuilt head table.
        let mut head_bytes = [0u8; Self::NUM_CLASSES as usize * 8];
        for (c, &h) in heads.iter().enumerate() {
            write_buf!(h => head_bytes, c * 8);
        }
        self.stack.set(Self::FREE_HEAD_BASE, head_bytes)?;
        Ok(unsure)
    }

    /// Pop the head of `class`, returning its block-start offset or `None`.
    ///
    /// The method is **not** thread-safe and must be externally synchronised if the
    /// allocator is used concurrently. However, since it only issues one `bstack`
    /// write, it is trivially crash-safe.
    #[cfg(not(feature = "atomic"))]
    fn pop_class(&self, class: u64) -> io::Result<Option<u64>> {
        let head_off = Self::head_off(class);
        let head = u64::from_le_bytes(read_bstack!(self.stack, head_off => u64));
        if head == Self::SENTINEL {
            return Ok(None);
        }
        let next = u64::from_le_bytes(read_bstack!(self.stack, head + Self::OVERHEAD => u64));
        self.stack.set(head_off, next.to_le_bytes())?;
        Ok(Some(head))
    }

    /// Pop the head of `class`, returning its block-start offset or `None`.
    ///
    /// Drives one [`BStack::process_gen`] holding the write lock across
    /// read-head → read-next → advance-head, closing the ABA window. The claim
    /// (overhead flip + data scrub) is a separate write on the now-detached
    /// block; a crash between leaks ≤ 1 block, reclaimed by `recover`.
    #[cfg(feature = "atomic")]
    fn pop_class(&self, class: u64) -> io::Result<Option<u64>> {
        let head_off = Self::head_off(class);
        let mut head_buf = [0u8; 8];
        let mut next_buf = [0u8; 8];
        let mut step = 0u32;
        let mut popped: Option<u64> = None;
        self.stack.process_gen(|| {
            let op = match step {
                0 => Some(BStackGenOp::Read {
                    offset: head_off,
                    // SAFETY: `head_buf` outlives this `process_gen` call.
                    buf: bstack_unsafe_reborrow_mut!(&mut head_buf[..]),
                }),
                1 => {
                    let head = u64::from_le_bytes(head_buf);
                    if head == Self::SENTINEL {
                        None
                    } else {
                        popped = Some(head);
                        Some(BStackGenOp::Read {
                            offset: head + Self::OVERHEAD,
                            // SAFETY: `next_buf` outlives this `process_gen` call.
                            buf: bstack_unsafe_reborrow_mut!(&mut next_buf[..]),
                        })
                    }
                }
                2 => Some(BStackGenOp::Write {
                    offset: head_off,
                    // SAFETY: `next_buf` outlives this `process_gen` call.
                    data: bstack_unsafe_reborrow!(&next_buf[..]),
                }),
                _ => None,
            };
            step += 1;
            op
        })?;
        Ok(popped)
    }

    /// Pop the oversized head if its stored physical size is **≥ `need`**,
    /// returning `(block_start, actual_size)`; a head smaller than `need` is left
    /// in place (the O(1) pop-if-head-fits rule — no search). The caller uses the
    /// first `need` bytes and carves any excess (`actual_size − need`).
    ///
    /// The method is **not** thread-safe and must be externally synchronised if the
    /// allocator is used concurrently. Since this only issues one `bstack` write, it
    /// is trivially crash-safe.
    #[cfg(not(feature = "atomic"))]
    fn pop_oversized(&self, need: u64) -> io::Result<Option<(u64, u64)>> {
        let head_off = Self::head_off(Self::OVERSIZED_CLASS);
        let head = u64::from_le_bytes(read_bstack!(self.stack, head_off => u64));
        if head == Self::SENTINEL {
            return Ok(None);
        }
        // overhead ‖ next_free are contiguous: fetch both in one 16-byte read.
        let buf = read_bstack!(self.stack, head => 16);
        let word = read_buf_le!(buf, 0 => u64);
        // Free head must have the high bit clear; its size is word << 4.
        let size = word << 4;
        if word & Self::IN_USE_BIT != 0 || size < need {
            return Ok(None);
        }
        let next = read_buf_le!(buf, 8 => u64);
        self.stack.set(head_off, next.to_le_bytes())?;
        Ok(Some((head, size)))
    }

    /// Pop the oversized head if its stored physical size is **≥ `need`**,
    /// returning `(block_start, actual_size)`; a head smaller than `need` is left
    /// in place (the O(1) pop-if-head-fits rule — no search). The caller uses the
    /// first `need` bytes and carves any excess (`actual_size − need`).
    ///
    /// A single [`BStack::process_gen`] holds the write lock across read-head →
    /// read-(overhead‖next) → advance-head, removing any ABA window. The head
    /// block's overhead word and inline `next_free` are contiguous, so they are
    /// fetched in one 16-byte read.
    #[cfg(feature = "atomic")]
    fn pop_oversized(&self, need: u64) -> io::Result<Option<(u64, u64)>> {
        let head_off = Self::head_off(Self::OVERSIZED_CLASS);
        let mut head_buf = [0u8; 8];
        // overhead ‖ next_free of the head block, read as one 16-byte op.
        let mut oh_next_buf = [0u8; 16];
        let mut step = 0usize;
        let mut head = 0u64;
        let mut size = 0u64;
        let mut popped: Option<(u64, u64)> = None;
        self.stack.process_gen(|| {
            let op = match step {
                0 => Some(BStackGenOp::Read {
                    offset: head_off,
                    // SAFETY: `head_buf` outlives this call.
                    buf: bstack_unsafe_reborrow_mut!(&mut head_buf[..]),
                }),
                1 => {
                    head = u64::from_le_bytes(head_buf);
                    if head == Self::SENTINEL {
                        None
                    } else {
                        Some(BStackGenOp::Read {
                            offset: head,
                            // SAFETY: `oh_next_buf` outlives this call.
                            buf: bstack_unsafe_reborrow_mut!(&mut oh_next_buf[..]),
                        })
                    }
                }
                2 => {
                    let word = read_buf_le!(oh_next_buf, 0 => u64);
                    // Free head must have the high bit clear; its size is word << 4.
                    size = word << 4;
                    if word & Self::IN_USE_BIT != 0 || size < need {
                        None
                    } else {
                        popped = Some((head, size));
                        // head[oversized] ← next_free (the second half of the read).
                        Some(BStackGenOp::Write {
                            offset: head_off,
                            // SAFETY: `oh_next_buf` outlives this call; its [8..16]
                            // half is untouched after step 1's read resolved.
                            data: bstack_unsafe_reborrow!(&oh_next_buf[8..]),
                        })
                    }
                }
                _ => None,
            };
            step += 1;
            op
        })?;
        Ok(popped)
    }

    /// Build the claim buffer (`in_use | (block >> 4)` overhead recording the
    /// block's physical size, `copy_from` prefix read straight in) without
    /// writing it. `block` is the physical extent being claimed and the size the
    /// overhead word records — the caller's visible length is never stored here.
    ///
    /// With [`true`] the buffer is the full `block` bytes, so writing it
    /// also scrubs everything past the copied prefix. With [`false`] it
    /// stops right after the prefix (`OVERHEAD + n` bytes, just `OVERHEAD` when
    /// there is nothing to copy): every consumer either writes it in place — in
    /// which case the untouched tail keeps the previous occupant's bytes — or
    /// hands it to a sparse grow, where the tail reads back as zero. Both are
    /// permitted "unspecified" contents, and neither costs an extra
    /// [`BStack`] call.
    ///
    /// Shared by both `atomic` and non-`atomic` paths.
    fn claim_buf(
        &self,
        block: u64,
        copy_from: Option<(u64, u64)>,
        init: bool,
    ) -> io::Result<Vec<u8>> {
        let copied = copy_from.map_or(0, |(_, n)| n);
        let buf_len = if init { block } else { Self::OVERHEAD + copied };
        let mut buf = vec![0u8; buf_len as usize];
        write_buf!(Self::IN_USE_BIT | (block >> 4) => buf, 0);
        if let Some((src, n)) = copy_from {
            self.stack.get_into(src, &mut buf[8..8 + n as usize])?;
        }
        Ok(buf)
    }

    /// Core allocation: place a `len`-byte block whose payload begins with `n`
    /// bytes copied from payload offset `src` (`copy_from = Some((src, n))`) and
    /// return its data pointer (`block_start + OVERHEAD`). A free-list hit reads
    /// the source straight into the single claim buffer; a miss extends a
    /// zero-filled block and writes overhead (plus the copied prefix), relying on
    /// `extend`'s zero-fill for the tail. `n` must not exceed the class payload
    /// capacity for `len` (callers pass a prefix `≤ len`).
    ///
    /// With [`true`] the payload past the copied prefix is zero; with
    /// [`false`] it is left unspecified — a reused block keeps its previous
    /// occupant's bytes, a freshly extended one still reads back as zero. The
    /// [`BStack`] call sequence is the same either way, only the claim buffer is
    /// shorter, so crash behaviour is unchanged.
    fn alloc_raw(&self, len: u64, copy_from: Option<(u64, u64)>, init: bool) -> io::Result<u64> {
        let need = Self::phys_need(len)?;
        let block = Self::class_blocksize(need);
        if block <= Self::MAX_CLASS {
            let class = Self::classify(block);
            if let Some(bs) = self.pop_class(class)? {
                // In both atomic and non-atomic paths, a failure between the pop
                // and the claim leaves the block free-tagged and reachable from
                // the head, so a crash is recoverable by `recover`.
                let buf = self.claim_buf(block, copy_from, init)?;
                self.stack.set(bs, buf)?;
                return Ok(bs + Self::OVERHEAD);
            }
        } else if let Some((bs, actual)) = self.pop_oversized(block)? {
            let excess = actual - block;
            if excess < Self::SPLIT_MIN {
                // Retain the whole popped block: claim `actual` bytes and record
                // `actual` as the physical size. One `set`, no carve. The block
                // stays in the (heterogeneous) oversized bucket, so retaining a
                // non-class size is sound. A crash before the claim lands leaves
                // it free-tagged and reachable, so `recover` reclaims it.
                let buf = self.claim_buf(actual, copy_from, init)?;
                self.stack.set(bs, buf)?;
            } else {
                // Reclaim the excess above the threshold: claim `block` bytes and
                // carve the rest, all as one crash-atomic transaction (claim
                // buffer as the prefix).
                let buf = self.claim_buf(block, copy_from, init)?;
                self.commit_carve(bs, &buf, bs + block, excess)?;
            }
            return Ok(bs + Self::OVERHEAD);
        }
        // Miss: eagerly grow the whole zero-filled block in one sparse write.
        // The write prefix is copied in before the tail is left zero by the
        // sparse grow, so we do not need a separate `set` for the remainder.
        let buf = self.claim_buf(block, copy_from, init)?;
        let bs = self.stack.extend_sparse(&buf, block)?;
        Ok(bs + Self::OVERHEAD)
    }

    /// Push `block_start` (physical size `size`, head index `class`) onto its
    /// free list, bundling the overhead flip.
    fn push(&self, block_start: u64, size: u64, class: u64) -> io::Result<()> {
        let head_off = Self::head_off(class);
        let start_bytes = block_start.to_le_bytes();
        // overhead || next_free: contiguous fields (OVERHEAD == 8 == size_of head),
        // so both are staged in one 16-byte buffer and written in a single op.
        let mut overhead_buf = [0u8; 16];
        write_buf!(size >> 4 => overhead_buf, 0); // free tag: high bit clear
        #[cfg(not(feature = "atomic"))]
        {
            // Non-atomic path: read head, write overhead+next_free, write head.
            let head = u64::from_le_bytes(read_bstack!(self.stack, head_off => u64));
            write_buf!(head => overhead_buf, 8);
            self.stack.set(block_start, overhead_buf)?;
            // A crash between these two writes leaves the block free-tagged so it is
            // recoverable by `recover`.
            self.stack.set(head_off, start_bytes)
        }
        #[cfg(feature = "atomic")]
        let mut step = 0u32;
        #[cfg(feature = "atomic")]
        self.stack.inplace_gen(|_res| {
            let op = match step {
                // Read the current head into next_free's half (no writes staged
                // yet ⇒ committed value).
                0 => Some(BStackGenOp::Read {
                    offset: head_off,
                    // SAFETY: `overhead_buf` outlives this call.
                    buf: bstack_unsafe_reborrow_mut!(&mut overhead_buf[8..]),
                }),
                // overhead ← free | size; next_free ← old head
                1 => Some(BStackGenOp::Write {
                    offset: block_start,
                    // SAFETY: `overhead_buf` outlives this call and is not
                    // mutated after step 0's read resolved.
                    data: bstack_unsafe_reborrow!(&overhead_buf[..]),
                }),
                // head[class] ← block_start
                2 => Some(BStackGenOp::Write {
                    offset: head_off,
                    // SAFETY: `start_bytes` outlives this call.
                    data: bstack_unsafe_reborrow!(&start_bytes[..]),
                }),
                _ => None,
            };
            step += 1;
            op
        })
    }

    /// Commit a `prefix` write and free a contiguous `region` as **one**
    /// crash-atomic [`BStack::inplace_gen`] transaction.
    ///
    /// `region` is greedily decomposed into class free blocks — the largest class
    /// `≤` the remainder, repeated (a region `> MAX_CLASS` becomes one oversized
    /// block). Every piece is a distinct class (greedy remainders strictly
    /// shrink), so each free-list head is read once and rewritten once: the
    /// transaction reads every involved head, writes `prefix`, writes each
    /// piece's overhead + `next_free` (= that class's old head), and repoints each
    /// head — all together. Bundling the `prefix` (the shrunk block's overhead,
    /// or the used oversized block) with the carve means a crash leaves the block
    /// either wholly un-shrunk or fully shrunk-and-freed, never a mid-arena gap.
    ///
    /// `region_size` must be a multiple of `QUANTUM` (`0` is a valid no-op that
    /// just writes `prefix`); pieces number ≤ 3 for any classed region.
    fn commit_carve(
        &self,
        prefix_off: u64,
        prefix: &[u8],
        region_start: u64,
        region_size: u64,
    ) -> io::Result<()> {
        // Greedy decomposition into ≤ MAX_CARVE_PIECES pieces, held in fixed
        // stack buffers (no heap on the carve path).
        const N: usize = SegregatedBStackAllocator::MAX_CARVE_PIECES;
        let mut block_offs = [0u64; N]; // piece block starts
        let mut head_offs = [0u64; N]; // free-list head slot per piece
        let mut blockoff_bytes = [[0u8; 8]; N]; // block start LE, for head writes
        // Per-piece 16-byte buffer: [0..8]=overhead (free|size), [8..16]=next_free (old head)
        let mut overhead_next = [[0u8; 16]; N];
        let mut k = 0usize;
        let mut off = region_start;
        let mut rem = region_size;
        while rem > 0 {
            let ps = if rem > Self::MAX_CLASS {
                rem // one oversized block absorbs the whole remainder
            } else {
                Self::largest_class_le(rem)
            };
            debug_assert!(k < N, "greedy carve exceeded MAX_CARVE_PIECES");
            block_offs[k] = off;
            head_offs[k] = Self::head_off(Self::classify(ps));
            write_buf!(ps >> 4 => overhead_next[k], 0);
            blockoff_bytes[k] = off.to_le_bytes();
            off += ps;
            rem -= ps;
            k += 1;
        }

        // Non-atomic path: lay down every freed piece *before* the prefix. Until
        // the prefix is written the carved region is not yet exposed as separate
        // blocks — it still sits inside the block whose header `prefix_off` will
        // change (a free block for the sole non-atomic caller, oversized non-exact
        // reuse) — so these writes are invisible to `recover`, and the single
        // prefix write is the commit point that exposes the already-valid pieces
        // atomically. A fault before it leaves the whole region reclaimable as it
        // was. This ordering is only fault-safe because that region is free excess,
        // never live caller data (non-tail shrink, which owns its tail, takes the
        // move path without `atomic`).
        #[cfg(not(feature = "atomic"))]
        {
            for i in 0..k {
                // Copy the prefilled overhead into a local buffer, then read the
                // old head directly into the latter half before writing both.
                let mut shared = overhead_next[i];
                // next_free ← current head of this class (read straight in).
                self.stack.get_into(head_offs[i], &mut shared[8..])?;
                // overhead || next_free, then head ← this block.
                self.stack.set(block_offs[i], shared)?;
                self.stack.set(head_offs[i], blockoff_bytes[i])?;
            }
            // Commit: expose the pieces (and, for a claim, mark the block in use).
            self.stack.set(prefix_off, prefix)?;
            Ok(())
        }

        // Atomic path with `inplace_gen` transaction: read each head, write the prefix,
        // then write each piece's overhead, next_free, and head. A crash leaves the
        // block either wholly un-shrunk or fully shrunk-and-freed, never a mid-arena gap.
        // Steps: [0, k) read each head; k writes the prefix; then 3 writes per
        // piece (overhead, next_free, head); then None commits the batch.
        #[cfg(feature = "atomic")]
        {
            let mut step = 0usize;
            // `overhead_next` already holds the per-piece overhead in its first
            // 8 bytes; we will read each head directly into its second half.
            self.stack.inplace_gen(|_res| {
                let op = if step < k {
                    // Read the committed head of piece `step`'s class (no head writes
                    // staged yet ⇒ this is the current head, captured as next_free).
                    Some(BStackGenOp::Read {
                        offset: head_offs[step],
                        // SAFETY: `overhead_next` outlives this call; we read the
                        // old head straight into its upper 8 bytes so a later write
                        // can emit both overhead and next_free together.
                        buf: bstack_unsafe_reborrow_mut!(&mut overhead_next[step][8..]),
                    })
                } else if step == k {
                    Some(BStackGenOp::Write {
                        offset: prefix_off,
                        // SAFETY: `prefix` outlives this call.
                        data: bstack_unsafe_reborrow!(prefix),
                    })
                } else if step < k + 1 + 2 * k {
                    let j = step - (k + 1);
                    let i = j / 2;
                    Some(match j % 2 {
                        // overhead || next_free (combined 16-byte write)
                        0 => BStackGenOp::Write {
                            offset: block_offs[i],
                            // SAFETY: `overhead_next` outlives this call.
                            data: bstack_unsafe_reborrow!(&overhead_next[i][..]),
                        },
                        // head[class] ← this block
                        _ => BStackGenOp::Write {
                            offset: head_offs[i],
                            // SAFETY: `blockoff_bytes` outlives this call.
                            data: bstack_unsafe_reborrow!(&blockoff_bytes[i][..]),
                        },
                    })
                } else {
                    None
                };
                step += 1;
                op
            })
        }
    }
}

#[cfg(feature = "set")]
impl fmt::Debug for SegregatedBStackAllocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SegregatedBStackAllocator")
            .field("num_classes", &Self::NUM_CLASSES)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "set")]
impl BStackAllocator for SegregatedBStackAllocator {
    type Error = io::Error;
    type Allocated<'a> = BStackOwnedSlice<'a, Self>;

    #[inline]
    fn stack(&self) -> &BStack {
        &self.stack
    }

    #[inline]
    fn into_stack(self) -> BStack {
        self.stack
    }

    /// Allocate `len` bytes. Computes the class, pops its head, else extends a
    /// fresh class block; oversized requests reuse an exact-size head or extend.
    #[inline]
    fn alloc(&self, len: u64) -> io::Result<BStackOwnedSlice<'_, Self>> {
        self.alloc_impl(len, true)
    }

    /// Resize the region described by `slice` to `new_len` bytes.
    ///
    /// | Case | Strategy |
    /// |------|----------|
    /// | Fits the current block (`phys_need(new_len) ≤ size`) | retain in place — no metadata write; zero the newly-exposed tail on a visible grow |
    /// | Grow past the block, at the tail | extend the tail in place (zero-filled), then record the new physical size |
    /// | Shrink, reclaimable excess (`≥ SPLIT_MIN`) | `atomic`: drop the excess — tail `Len` + `Atrunc`, else an in-place carve — recording the new size in one transaction; without `atomic`, retain the excess in place |
    /// | Shrink, excess below `SPLIT_MIN` | retain in place — no write |
    /// | Non-tail grow past the block | alloc new class, copy, dealloc old |
    ///
    /// The visible length lives in the returned handle, not on disk, so a resize
    /// that fits (or a shrink whose excess is retained) touches no metadata at
    /// all. Every path only ever *leaks* on a mid-op failure (never corrupts): the
    /// tail grow records the new physical size leak-preferring so a crash leaves
    /// an orphaned zero tail that [`recover`](Self::recover) reclaims, and an
    /// `atomic` shrink commits the new size together with the truncation (resp.
    /// the carve) as a single transaction, so a crash leaves the block wholly
    /// un-shrunk or fully shrunk — never a recorded size disagreeing with the
    /// block's physical extent, which would make the recovery scan mis-stride.
    #[inline]
    fn realloc<'a>(
        &'a self,
        slice: BStackOwnedSlice<'a, Self>,
        new_len: u64,
    ) -> Result<BStackOwnedSlice<'a, Self>, BStackAllocError<'a, Self>> {
        self.realloc_impl(slice, new_len, true)
    }

    /// Release the region described by `slice`.
    ///
    /// Reads the overhead at `slice.start() − 8`: a clear high bit is a
    /// double-free. The block's physical size comes straight from the word; the
    /// handle's length is trusted (rejecting only a length too large for the
    /// block). Otherwise an oversized block at the tail is discarded in one call,
    /// and every other block is spliced onto its class head via one crash-atomic
    /// [`BStack::inplace_gen`] transaction.
    fn dealloc<'a>(
        &'a self,
        slice: BStackOwnedSlice<'a, Self>,
    ) -> Result<(), BStackAllocError<'a, Self>> {
        let slice = ensure_own_handle(self, slice, "SegregatedBStackAllocator::dealloc")?;
        let start = slice.start();
        let len = slice.len();
        if slice.is_empty() {
            return Ok(());
        }
        // Set once a free-list splice is attempted: a torn/failed atomic push
        // must not hand back a handle that could double-free.
        let mut lost = false;
        let result = (|| -> io::Result<()> {
            let block_start = Self::block_start_of(start)?;
            let word = u64::from_le_bytes(read_bstack!(self.stack, block_start => u64));
            if word & Self::IN_USE_BIT == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "double free: block is already free",
                ));
            }
            let size = (word & !Self::IN_USE_BIT) << 4;
            if Self::phys_need(len)? > size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot free a mismatched slice",
                ));
            }
            let class = Self::classify(size);

            if size > Self::MAX_CLASS {
                let end = block_start.checked_add(size).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "block end overflows u64")
                })?;
                // Oversized tail block: hand its bytes back to the stack instead of
                // the free list. Under `atomic`, `try_discard` fuses the tail check
                // and the drop; otherwise check `len` then `discard`.
                #[cfg(feature = "atomic")]
                if self.stack.try_discard(end, size)? {
                    return Ok(());
                }
                #[cfg(not(feature = "atomic"))]
                if end == self.stack.len()? {
                    self.stack.discard(size)?;
                    return Ok(());
                }
            }
            lost = true;
            self.push(block_start, size, class)
        })();
        result.map_err(|source| BStackAllocError {
            source,
            handle: if lost {
                None
            } else {
                // SAFETY: (start, len) still describes the caller's live block.
                Some(unsafe { BStackOwnedSlice::from_raw_parts(self, start, len) })
            },
        })
    }
}

/// Claiming a block without scrubbing the payload past the copied prefix.
///
/// A freed block keeps its previous occupant's bytes — `dealloc` rewrites only
/// the 8-byte overhead tag and the inline `next_free` link — so
/// [`alloc`](BStackAllocator::alloc) stages a full `block`-byte claim buffer and
/// writes it, fusing the overhead, any copied prefix, and a scrub of everything
/// after it into one [`BStack`] call.
///
/// [`alloc_uninit`](BStackUninitAllocator::alloc_uninit) stops the claim buffer
/// right after the prefix: the same single call, but only `OVERHEAD + copied`
/// bytes and no block-sized heap staging. A reused block therefore hands back
/// its previous contents past the prefix; a block carved fresh off the tail
/// still reads back as zero, since the sparse grow realises it with one
/// `set_len`.
///
/// [`realloc_uninit`](BStackUninitAllocator::realloc_uninit) additionally drops
/// the two `zero` calls that scrub bytes newly exposed inside a block the caller
/// already owns — one on a same-class grow, one for the old block's slack on a
/// tail grow — each a whole durable sync.
///
/// Neither method changes the [`BStack`] call sequence of its initialised
/// counterpart, so the leak-preferring crash model is identical. Recovery is
/// unaffected: its linear scan reads only the overhead word at each block start
/// and strides over live blocks by their recorded length, never inspecting a
/// live block's payload.
#[cfg(feature = "set")]
impl SegregatedBStackAllocator {
    /// Shared body of [`alloc`](BStackAllocator::alloc) and
    /// [`alloc_uninit`](BStackUninitAllocator::alloc_uninit); the two differ only
    /// in the length of the claim buffer [`alloc_raw`](Self::alloc_raw) writes.
    #[inline]
    fn alloc_impl(&self, len: u64, init: bool) -> io::Result<BStackOwnedSlice<'_, Self>> {
        if len == 0 {
            return Ok(BStackOwnedSlice::empty(self));
        }
        let ptr = self.alloc_raw(len, None, init)?;
        // SAFETY: `ptr` is the data start of a freshly allocated `len`-byte block.
        Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, ptr, len) })
    }

    /// Shared body of [`realloc`](BStackAllocator::realloc) and
    /// [`realloc_uninit`](BStackUninitAllocator::realloc_uninit).
    ///
    /// A clear `init` drops the two [`BStack::zero`] calls that exist purely to
    /// scrub bytes newly exposed inside a block the caller already owns — the
    /// same-class grow and the old block's slack on a tail grow — and shortens
    /// the move path's claim buffer. Every write that maintains block metadata
    /// (the overhead word, the carve, the tail extend/discard) is unconditional,
    /// so the leak-preferring failure model documented on
    /// [`realloc`](BStackAllocator::realloc) is unchanged.
    fn realloc_impl<'a>(
        &'a self,
        slice: BStackOwnedSlice<'a, Self>,
        new_len: u64,
        init: bool,
    ) -> Result<BStackOwnedSlice<'a, Self>, BStackAllocError<'a, Self>> {
        let slice = ensure_own_handle(self, slice, "SegregatedBStackAllocator::realloc")?;
        if slice.is_empty() {
            // Nothing backs an empty handle: realloc is just a fresh alloc.
            return self.alloc_impl(new_len, init).map_err(|source| {
                BStackAllocError::with_handle(source, BStackOwnedSlice::empty(self))
            });
        }
        if new_len == 0 {
            // dealloc consumes `slice`; its BStackAllocError propagates unchanged.
            self.dealloc(slice)?;
            return Ok(BStackOwnedSlice::empty(self));
        }
        let start = slice.start();
        let old_len = slice.len();
        // Validate the pointer up front so every path below (including the
        // no-op resize) trusts a real block start.
        let block_start = match Self::block_start_of(start) {
            Ok(bs) => bs,
            Err(source) => {
                // SAFETY: the region is untouched and still owned by the caller.
                let handle = unsafe { BStackOwnedSlice::from_raw_parts(self, start, old_len) };
                return Err(BStackAllocError::with_handle(source, handle));
            }
        };
        if new_len == old_len {
            // SAFETY: unchanged region.
            return Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, start, old_len) });
        }

        // The allocation to hand back on failure. Starts as the original block;
        // becomes the new region once a move has committed and copied it (both
        // are always distinct live regions safe to return).
        let mut recovered = (start, old_len);
        let result = (|| -> io::Result<BStackOwnedSlice<'a, Self>> {
            let word = u64::from_le_bytes(read_bstack!(self.stack, block_start => u64));
            if word & Self::IN_USE_BIT == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot realloc a freed block",
                ));
            }
            let old_size = (word & !Self::IN_USE_BIT) << 4;
            if Self::phys_need(old_len)? > old_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot realloc a mismatched slice",
                ));
            }
            let new_need = Self::phys_need(new_len)?;
            let new_size = Self::class_blocksize(new_need);

            if new_need <= old_size {
                // The request fits the current physical block — no bigger block
                // needed.
                if new_len > old_len {
                    // Visible grow within the block: zero the newly-exposed bytes
                    // (a prior shrink may have left stale data there) and retain.
                    // The length lives in the handle, so there is no metadata
                    // write and nothing to order the zero against for `recover`.
                    if init {
                        self.stack.zero(start + old_len, new_len - old_len)?;
                    }
                    // SAFETY: same physical block, larger visible length.
                    return Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, start, new_len) });
                }

                // Visible shrink (`new_len < old_len`; the `==` case returned
                // early above). No new bytes are exposed. Reclaim the excess only
                // above the split threshold, and only under `atomic`: a shrink's
                // freed tail overlaps still-live caller bytes, so recording the
                // smaller size and dropping the excess must commit together in one
                // transaction — the non-`atomic` build cannot fuse them (either
                // ordering leaves a window `recover` mis-parses, and a rollback
                // would hand back a block whose tail the carve already clobbered),
                // so it retains the excess in place (zero writes, no move).
                #[cfg(feature = "atomic")]
                if new_size < old_size && old_size - new_size >= Self::SPLIT_MIN {
                    let old_end = block_start + old_size; // block exists ⇒ ≤ stack_len

                    // Tail shrink: replace the whole block with its shrunk self in
                    // ONE crash-atomic transaction. `Len` confirms the tail under
                    // `process_gen`'s held write lock and a single `Atrunc` cuts
                    // the old block and re-appends the new one (overhead recording
                    // `new_size`, surviving prefix, zero pad) at the same offset.
                    // Recording the size and dropping the excess as separate calls
                    // would leave a crash window where the recorded size disagrees
                    // with the physical extent and `recover` mis-strides.
                    //
                    // The replacement buffer is always full-length regardless of
                    // `init`: `Atrunc` re-appends exactly these bytes, so their
                    // count *is* the new block's physical extent. A shrink adds no
                    // new bytes, so there is no uninitialised region to hand back.
                    let buf = self.claim_buf(new_size, Some((start, new_len)), true)?;
                    let mut cur_len = 0u64;
                    let cur_ptr: *mut u64 = &mut cur_len;
                    let mut phase = 0u8;
                    let mut truncated = false;
                    self.stack.process_gen(|| match phase {
                        0 => {
                            phase = 1;
                            // SAFETY: `process_gen` invokes this closure strictly
                            // sequentially and finishes writing `out` before the
                            // next call, so the `&mut` never aliases the read below.
                            Some(BStackGenOp::Len {
                                out: unsafe { &mut *cur_ptr },
                            })
                        }
                        1 => {
                            phase = 2;
                            // SAFETY: the `Len` write above has completed.
                            if old_end == unsafe { *cur_ptr } {
                                truncated = true;
                                Some(BStackGenOp::Atrunc {
                                    n: old_size,
                                    data: &buf[..],
                                })
                            } else {
                                None
                            }
                        }
                        _ => None,
                    })?;
                    if truncated {
                        // SAFETY: block replaced in place at the same offset,
                        // shrunk to the new class with its prefix preserved.
                        return Ok(unsafe {
                            BStackOwnedSlice::from_raw_parts(self, start, new_len)
                        });
                    }

                    // Non-tail: keep the block at the new class and free the excess
                    // tail *in place* as one crash-atomic carve — no move, no copy,
                    // and `recovered` stays the untouched original (a fault leaves
                    // the block un-shrunk). The prefix commit records `new_size`.
                    let prefix = (Self::IN_USE_BIT | (new_size >> 4)).to_le_bytes();
                    self.commit_carve(
                        block_start,
                        &prefix,
                        block_start + new_size,
                        old_size - new_size,
                    )?;
                    // SAFETY: block shrunk in place; the freed tail is now free.
                    return Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, start, new_len) });
                }

                // Retain in place: the block records its own size and a shrink
                // exposes no new bytes, so this is zero `BStack` writes.
                // SAFETY: same physical block, smaller visible length.
                return Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, start, new_len) });
            }

            // The request needs a physically larger block (`new_need > old_size`,
            // so also `new_size > old_size`). Grow at the tail: extend the block in
            // place, only when it ends at the payload tail. Extend first
            // (leak-preferring: a failure after it only leaks the extension, never
            // corrupts), then zero the old slack, then record the new size. Under
            // `atomic`, `try_extend_zeros` fuses the tail check and the grow into
            // one locked critical section; otherwise we check `len` then `extend`
            // (which zero-fills the new region via `set_len`).
            let old_end = block_start + old_size; // block exists ⇒ ≤ stack_len
            let grew = {
                #[cfg(feature = "atomic")]
                {
                    self.stack.try_extend_zeros(old_end, new_size - old_size)?
                }
                #[cfg(not(feature = "atomic"))]
                {
                    old_end == self.stack.len()? && {
                        self.stack.extend(new_size - old_size)?;
                        true
                    }
                }
            };
            if grew {
                // Old block's slack [start+old_len, old_end) may hold stale bytes
                // from a prior shrink; the extension past old_end is already zero.
                let slack = (old_size - Self::OVERHEAD) - old_len;
                if slack > 0 && init {
                    self.stack.zero(start + old_len, slack)?;
                }
                self.stack.set(
                    block_start,
                    (Self::IN_USE_BIT | (new_size >> 4)).to_le_bytes(),
                )?;
                // SAFETY: block extended in place to the new class at the tail.
                return Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, start, new_len) });
            }

            // Move: allocate the new class, having it read the surviving prefix
            // straight from the old block into its claim buffer (no separate copy
            // buffer or write), then free the old block. Reached only by a grow
            // past the current block that is not at the tail (a shrink always fits
            // the block, so it never moves). Each step is individually atomic; a
            // mid-move failure leaks (never corrupts).
            let copy_len = old_len.min(new_len);
            let new_ptr = self.alloc_raw(new_len, Some((start, copy_len)), init)?;
            // New region committed and populated; it is now the survivor.
            recovered = (new_ptr, new_len);
            // SAFETY: (start, old_len) still names the caller's live old block.
            let old = unsafe { BStackOwnedSlice::from_raw_parts(self, start, old_len) };
            self.dealloc(old).map_err(|e| e.source)?;
            // SAFETY: `new_ptr` is the data start of the freshly populated block.
            Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, new_ptr, new_len) })
        })();
        result.map_err(|source| BStackAllocError {
            source,
            // SAFETY: `recovered` names a live region owned by the caller.
            handle: Some(unsafe {
                BStackOwnedSlice::from_raw_parts(self, recovered.0, recovered.1)
            }),
        })
    }
}

#[cfg(feature = "set")]
impl BStackUninitAllocator for SegregatedBStackAllocator {
    #[inline]
    fn alloc_uninit(&self, len: u64) -> io::Result<BStackOwnedSlice<'_, Self>> {
        self.alloc_impl(len, false)
    }

    #[inline]
    fn realloc_uninit<'a>(
        &'a self,
        slice: BStackOwnedSlice<'a, Self>,
        new_len: u64,
    ) -> Result<BStackOwnedSlice<'a, Self>, BStackAllocError<'a, Self>> {
        self.realloc_impl(slice, new_len, false)
    }
}

#[cfg(all(test, feature = "set"))]
mod _assertions {
    use super::SegregatedBStackAllocator;
    fn _send()
    where
        SegregatedBStackAllocator: Send,
    {
    }
    #[cfg(feature = "atomic")]
    fn _sync()
    where
        SegregatedBStackAllocator: Sync,
    {
    }
}

#[cfg(all(test, feature = "set"))]
mod tests {
    use super::SegregatedBStackAllocator as Seg;
    use crate::BStack;
    use crate::alloc::{BStackAllocator, BStackUninitAllocator};
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Guard(std::path::PathBuf);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    fn temp_path() -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        std::env::temp_dir().join(format!("bstack_seg_{pid}_{id}.bin"))
    }
    fn new_alloc() -> (Seg, Guard) {
        let path = temp_path();
        let g = Guard(path.clone());
        (Seg::new(BStack::open(&path).unwrap()).unwrap(), g)
    }

    // ── alloc_uninit / realloc_uninit ────────────────────────────────────────

    #[test]
    fn seg_alloc_uninit_returns_a_usable_region() {
        let (a, _g) = new_alloc();
        let mut s = a.alloc_uninit(100).unwrap();
        assert_eq!(s.len(), 100);
        s.write([0xABu8; 100]).unwrap();
        assert_eq!(s.read().unwrap(), vec![0xABu8; 100]);
    }

    #[test]
    fn seg_alloc_uninit_of_fresh_tail_growth_still_reads_zero() {
        // A miss grows the block with one sparse `set_len`, so the payload past
        // the claim buffer reads back as zero even with nothing written to it.
        let (a, _g) = new_alloc();
        let s = a.alloc_uninit(100).unwrap();
        assert_eq!(s.read().unwrap(), vec![0u8; 100]);
    }

    #[test]
    fn seg_alloc_uninit_hands_back_a_recycled_block_unscrubbed() {
        // White-box: proves the claim buffer really stops after the overhead.
        // The trait's contract is only that the bytes are unspecified.
        let (a, _g) = new_alloc();
        let mut x = a.alloc(100).unwrap();
        x.write([0x5Au8; 100]).unwrap();
        let start = x.start();
        a.dealloc(x).unwrap();

        let y = a.alloc_uninit(100).unwrap();
        assert_eq!(y.start(), start, "the freed block must be the one reused");
        // dealloc rewrites the overhead and the inline next_free link, which sits
        // in the first 8 payload bytes; the rest is the previous occupant's data.
        assert_eq!(&y.read().unwrap()[8..], &[0x5Au8; 92]);
    }

    #[test]
    fn seg_alloc_still_scrubs_a_recycled_block() {
        let (a, _g) = new_alloc();
        let mut x = a.alloc(100).unwrap();
        x.write([0x5Au8; 100]).unwrap();
        let start = x.start();
        a.dealloc(x).unwrap();

        let y = a.alloc(100).unwrap();
        assert_eq!(y.start(), start);
        assert_eq!(y.read().unwrap(), vec![0u8; 100]);
    }

    #[test]
    fn seg_alloc_uninit_block_survives_recover() {
        // The overhead tag is still written, so the linear scan classifies the
        // block as live and accounts for the whole arena.
        let (a, _g) = new_alloc();
        let x = a.alloc(100).unwrap();
        a.dealloc(x).unwrap();
        let mut y = a.alloc_uninit(100).unwrap();
        y.write([0x77u8; 100]).unwrap();
        assert_eq!(unsafe { a.recover() }.unwrap(), 0);
        assert_eq!(y.read().unwrap(), vec![0x77u8; 100]);
    }

    #[test]
    fn seg_realloc_uninit_preserves_existing_bytes_on_grow() {
        let (a, _g) = new_alloc();
        let mut x = a.alloc(100).unwrap();
        x.write([0x11u8; 100]).unwrap();
        // Keep `x` off the tail so the grow has to move it.
        let _pin = a.alloc(100).unwrap();

        let grown = a.realloc_uninit(x, 1000).unwrap();
        assert_eq!(grown.len(), 1000);
        assert_eq!(&grown.read().unwrap()[..100], &[0x11u8; 100]);
    }

    #[test]
    fn seg_realloc_uninit_preserves_existing_bytes_on_shrink() {
        let (a, _g) = new_alloc();
        let mut x = a.alloc(1000).unwrap();
        x.write([0x22u8; 1000]).unwrap();
        let shrunk = a.realloc_uninit(x, 100).unwrap();
        assert_eq!(shrunk.len(), 100);
        assert_eq!(shrunk.read().unwrap(), vec![0x22u8; 100]);
    }

    // ── classification math ──────────────────────────────────────────────────

    #[test]
    fn class_scheme_constants() {
        assert_eq!(Seg::LINEAR_CLASSES, 16);
        assert_eq!(Seg::GEO_CLASSES, 16);
        assert_eq!(Seg::NUM_CLASSES, 33);
        assert_eq!(Seg::OVERSIZED_CLASS, 32);
        assert_eq!(Seg::ARENA_START, 304);
    }

    #[test]
    fn classify_boundaries() {
        assert_eq!(Seg::classify(16), 0);
        assert_eq!(Seg::classify(256), 15); // last linear
        assert_eq!(Seg::classify(320), 16); // first geometric
        assert_eq!(Seg::classify(512), 19); // top of octave 8
        assert_eq!(Seg::classify(640), 20); // first of octave 9
        assert_eq!(Seg::classify(4096), 31); // last geometric
        assert_eq!(Seg::classify(4112), Seg::OVERSIZED_CLASS); // oversized
    }

    #[test]
    fn class_blocksize_snaps() {
        // len 200 → need 208 → linear 208 (exact fit, data 200)
        assert_eq!(Seg::class_blocksize(Seg::phys_need(200).unwrap()), 208);
        // len 500 → need 512 → octave (256,512] → 512
        assert_eq!(Seg::class_blocksize(Seg::phys_need(500).unwrap()), 512);
        // len 8 → need 16 → minimum block
        assert_eq!(Seg::class_blocksize(Seg::phys_need(8).unwrap()), 16);
        // oversized passes through as a raw multiple of 16
        assert_eq!(Seg::class_blocksize(Seg::phys_need(5000).unwrap()), 5008);
    }

    #[test]
    fn class_blocksize_roundtrips_through_classify() {
        for len in [1u64, 8, 9, 24, 100, 200, 255, 300, 500, 1000, 3000, 4000] {
            let size = Seg::class_blocksize(Seg::phys_need(len).unwrap());
            // The block is large enough and its class tag round-trips.
            assert!(size >= len + Seg::OVERHEAD, "len {len} size {size}");
            assert_eq!(size % Seg::QUANTUM, 0);
            assert!(Seg::classify(size) < Seg::OVERSIZED_CLASS);
        }
    }

    // ── behaviour ────────────────────────────────────────────────────────────

    #[test]
    fn seg_new_initialises_header() {
        let (a, _g) = new_alloc();
        assert_eq!(a.stack().len().unwrap(), Seg::ARENA_START);
    }

    #[test]
    fn seg_alloc_zero_is_empty() {
        let (a, _g) = new_alloc();
        assert!(a.alloc(0).unwrap().is_empty());
    }

    #[test]
    fn seg_dealloc_then_alloc_reuses_same_class_block() {
        let (a, _g) = new_alloc();
        // 100 and 104 both round up to need 112 → class 6 (block 112).
        let s1 = a.alloc(100).unwrap();
        let off1 = s1.start();
        a.dealloc(s1).unwrap();
        let s2 = a.alloc(104).unwrap();
        assert_eq!(
            s2.start(),
            off1,
            "block should be reused from the free list"
        );
    }

    #[test]
    fn seg_distinct_classes_do_not_alias() {
        let (a, _g) = new_alloc();
        let s1 = a.alloc(100).unwrap(); // block 112, class 6
        let off1 = s1.start();
        a.dealloc(s1).unwrap();
        let s2 = a.alloc(500).unwrap(); // block 512, class 19 — different list
        assert_ne!(s2.start(), off1);
    }

    #[test]
    fn seg_write_read_round_trip_and_reopen() {
        let path = temp_path();
        let _g = Guard(path.clone());
        let off = {
            let a = Seg::new(BStack::open(&path).unwrap()).unwrap();
            let mut s = a.alloc(300).unwrap();
            s.write(b"segregated allocator payload").unwrap();
            let off = s.start();
            drop(a);
            off
        };
        let a = Seg::new(BStack::open(&path).unwrap()).unwrap();
        let s = unsafe { crate::alloc::BStackSlice::from_raw_parts(a.stack(), off, 28) };
        assert_eq!(s.read().unwrap(), b"segregated allocator payload");
    }

    // ── realloc ──────────────────────────────────────────────────────────────

    #[test]
    fn seg_realloc_same_class_grow_zeros_and_preserves() {
        let (a, _g) = new_alloc();
        // 90 and 100 both map to block 112 (class 6): a same-class resize.
        let mut s = a.alloc(90).unwrap();
        s.write([0xABu8; 90]).unwrap();
        let off = s.start();
        let s = a.realloc(s, 100).unwrap();
        assert_eq!(s.start(), off, "same-class grow stays in place");
        let data = s.read().unwrap();
        assert_eq!(&data[..90], &[0xABu8; 90], "prefix preserved");
        assert_eq!(&data[90..], &[0u8; 10], "grown tail zeroed");
    }

    #[test]
    fn seg_realloc_same_class_shrink_then_grow_has_no_stale_bytes() {
        let (a, _g) = new_alloc();
        let mut s = a.alloc(100).unwrap(); // block 112
        s.write([0xCDu8; 100]).unwrap();
        let s = a.realloc(s, 90).unwrap(); // same class, shrink (stale [90,100))
        let s = a.realloc(s, 100).unwrap(); // same class, grow back
        let data = s.read().unwrap();
        assert_eq!(&data[..90], &[0xCDu8; 90]);
        assert_eq!(
            &data[90..],
            &[0u8; 10],
            "re-grown bytes must be zero, not stale"
        );
    }

    #[test]
    fn seg_realloc_tail_grow_in_place() {
        let (a, _g) = new_alloc();
        let mut s = a.alloc(100).unwrap(); // block 112, at the tail
        s.write([7u8; 100]).unwrap();
        let off = s.start();
        let len_before = a.stack().len().unwrap();
        let s = a.realloc(s, 200).unwrap(); // need 208 (class 12) > 112: cross-class
        assert_eq!(s.start(), off, "tail grow extends in place");
        assert!(a.stack().len().unwrap() > len_before);
        let data = s.read().unwrap();
        assert_eq!(&data[..100], &[7u8; 100]);
        assert_eq!(&data[100..], &[0u8; 100], "grown region zeroed");
    }

    #[test]
    fn seg_realloc_cross_class_grow_non_tail_moves() {
        let (a, _g) = new_alloc();
        let mut s = a.alloc(100).unwrap(); // block 112
        s.write([9u8; 100]).unwrap();
        let off = s.start();
        let _pin = a.alloc(100).unwrap(); // pins the tail so `s` is interior
        let s = a.realloc(s, 300).unwrap(); // cross-class, non-tail → move
        assert_ne!(s.start(), off, "interior grow moves to a new block");
        let data = s.read().unwrap();
        assert_eq!(&data[..100], &[9u8; 100]);
        assert_eq!(&data[100..], &[0u8; 200]);
    }

    // A shrink whose excess is below `SPLIT_MIN` is retained inside the live
    // block (in either build): no metadata write, no move, and the block keeps
    // its larger physical size so a later grow back fits in place.
    #[test]
    fn seg_realloc_small_shrink_excess_retained_in_place() {
        let (a, _g) = new_alloc();
        let mut s = a.alloc(200).unwrap(); // block 208 (class 12)
        s.write([0x5Au8; 200]).unwrap();
        let off = s.start();
        let _pin = a.alloc(100).unwrap(); // pins the tail so `s` is interior
        let s = a.realloc(s, 100).unwrap(); // block 112 needed; excess 96 < SPLIT_MIN → retain
        assert_eq!(s.start(), off, "small shrink retains in place, no move");
        assert_eq!(
            s.read().unwrap(),
            vec![0x5Au8; 100],
            "surviving prefix preserved"
        );
        // The block still physically spans 208 bytes, so growing back to 200 fits
        // in place (no move) with the re-exposed tail zeroed.
        let s = a.realloc(s, 200).unwrap();
        assert_eq!(s.start(), off, "grow back into the retained block, no move");
        let data = s.read().unwrap();
        assert_eq!(&data[..100], &[0x5Au8; 100], "prefix preserved");
        assert_eq!(&data[100..], &[0u8; 100], "re-exposed tail zeroed");
        assert_eq!(unsafe { a.recover() }.unwrap(), 0, "arena accounted for");
    }

    #[test]
    fn seg_realloc_to_zero_frees_and_from_empty_allocs() {
        let (a, _g) = new_alloc();
        let s = a.alloc(64).unwrap();
        let empty = a.realloc(s, 0).unwrap();
        assert!(empty.is_empty());
        let grown = a.realloc(empty, 48).unwrap();
        assert_eq!(grown.len(), 48);
        assert!(!grown.is_empty());
    }

    #[test]
    fn seg_realloc_rejects_malformed_pointer_even_on_noop() {
        let (a, _g) = new_alloc();
        // A misaligned pointer with new_len == old_len must still be rejected
        // (the no-op path validates), handle returned intact.
        let bad = unsafe {
            crate::alloc::BStackOwnedSlice::from_raw_parts(
                &a,
                Seg::ARENA_START + Seg::OVERHEAD + 1,
                16,
            )
        };
        let err = a.realloc(bad, 16).unwrap_err();
        assert_eq!(err.source.kind(), std::io::ErrorKind::InvalidInput);
        assert!(err.handle.is_some());
    }

    /// With `atomic` a tail shrink whose excess reaches `SPLIT_MIN` is one
    /// `Len` + `Atrunc` transaction: the block is replaced by its shrunk self at
    /// the same offset and the excess goes back to the stack.
    #[cfg(feature = "atomic")]
    #[test]
    fn seg_realloc_tail_shrink_in_place() {
        let (a, _g) = new_alloc();
        let mut s = a.alloc(500).unwrap(); // block 512, at the tail
        s.write([0x3Cu8; 500]).unwrap();
        let off = s.start();
        let len_before = a.stack().len().unwrap();
        let s = a.realloc(s, 100).unwrap(); // new class 112 < 512, excess 400 ≥ SPLIT_MIN, at tail → atrunc
        assert_eq!(s.start(), off, "tail shrink stays in place");
        assert!(
            a.stack().len().unwrap() < len_before,
            "excess discarded from tail"
        );
        assert_eq!(s.read().unwrap(), vec![0x3Cu8; 100]);
    }

    /// Without `atomic`, recording the smaller size and dropping the excess
    /// cannot be fused, and either ordering leaves a crash window that desyncs
    /// `recover` (see the `realloc` docs), so a shrink retains the excess in place
    /// instead: the block keeps its physical size and offset with no move, and a
    /// later grow back into that retained span fits without moving.
    #[cfg(not(feature = "atomic"))]
    #[test]
    fn seg_realloc_tail_shrink_retains_in_place() {
        let (a, _g) = new_alloc();
        let mut s = a.alloc(500).unwrap(); // block 512, at the tail
        s.write([0x3Cu8; 500]).unwrap();
        let off = s.start();
        let len_before = a.stack().len().unwrap();
        let s = a.realloc(s, 100).unwrap(); // new class 112 < 512 → retain, no move
        assert_eq!(
            s.start(),
            off,
            "non-atomic shrink retains the block in place"
        );
        assert_eq!(
            a.stack().len().unwrap(),
            len_before,
            "nothing discarded: the excess is retained"
        );
        assert_eq!(s.read().unwrap(), vec![0x3Cu8; 100]);
        // The retained 512-byte block absorbs a grow back to 400 with no move.
        let s = a.realloc(s, 400).unwrap();
        assert_eq!(s.start(), off, "grow back into the retained block, no move");
        assert_eq!(&s.read().unwrap()[..100], &[0x3Cu8; 100]);
    }

    // ── recover ──────────────────────────────────────────────────────────────

    #[test]
    fn seg_recover_relinks_leaked_free_block() {
        let (a, _g) = new_alloc();
        let a1 = a.alloc(100).unwrap(); // class 6 (block 112)
        let off1 = a1.start();
        let _b = a.alloc(100).unwrap(); // pins a second class-6 block
        a.dealloc(a1).unwrap(); // head[6] → a1
        // Simulate a leak: clear head[6] so a1 is free-tagged but unreachable.
        a.stack().set(Seg::head_off(6), 0u64.to_le_bytes()).unwrap();
        assert_eq!(
            unsafe { a.recover() }.unwrap(),
            0,
            "arena fully accounted for"
        );
        // a1 is relinked, so the next same-class alloc reuses it.
        let r = a.alloc(90).unwrap();
        assert_eq!(r.start(), off1, "recover relinked the leaked block");
    }

    #[test]
    fn seg_recover_discards_crashed_extend_tail() {
        let (a, _g) = new_alloc();
        let _a1 = a.alloc(100).unwrap();
        let base = a.stack().len().unwrap();
        // Simulate a crashed tail extend: a zero-filled block whose overhead
        // write never landed (word == 0 at `base`).
        a.stack().extend(128).unwrap();
        assert_eq!(a.stack().len().unwrap(), base + 128);
        assert_eq!(unsafe { a.recover() }.unwrap(), 0);
        assert_eq!(
            a.stack().len().unwrap(),
            base,
            "orphaned zero tail discarded"
        );
    }

    #[test]
    fn seg_recover_clean_arena_preserves_data_and_free_list() {
        let (a, _g) = new_alloc();
        let mut keep = a.alloc(300).unwrap();
        keep.write(b"survives recover").unwrap();
        let freed = a.alloc(64).unwrap();
        let freed_off = freed.start();
        a.dealloc(freed).unwrap();
        assert_eq!(unsafe { a.recover() }.unwrap(), 0);
        assert_eq!(&keep.read().unwrap()[..16], b"survives recover");
        // The free list still works: the freed block is reused.
        let r = a.alloc(60).unwrap();
        assert_eq!(r.start(), freed_off);
    }

    #[test]
    fn seg_largest_class_le() {
        assert_eq!(Seg::largest_class_le(16), 16);
        assert_eq!(Seg::largest_class_le(256), 256);
        assert_eq!(Seg::largest_class_le(272), 256); // just above the linear top
        assert_eq!(Seg::largest_class_le(704), 640);
        assert_eq!(Seg::largest_class_le(896), 896); // itself a class
        assert_eq!(Seg::largest_class_le(4080), 3584);
        assert_eq!(Seg::largest_class_le(4096), 4096);
    }

    // In-place non-tail-shrink carve is atomic-only (see the sibling test above).
    #[cfg(feature = "atomic")]
    #[test]
    fn seg_realloc_non_tail_shrink_carves_reusable_blocks() {
        let (a, _g) = new_alloc();
        let mut s = a.alloc(1000).unwrap(); // block 1024 (class 23)
        s.write([0x77u8; 1000]).unwrap();
        let off = s.start();
        let base = off - Seg::OVERHEAD;
        let _pin = a.alloc(200).unwrap(); // class 12 — pins the tail, s is interior
        let s = a.realloc(s, 300).unwrap(); // block 320; gap 704 → carve 640 + 64
        assert_eq!(s.start(), off, "non-tail shrink keeps the block in place");
        assert_eq!(
            &s.read().unwrap()[..300],
            &[0x77u8; 300],
            "prefix preserved"
        );
        // Carved 640 (class 20) at base+320, 64 (class 3) at base+960 — reusable.
        let r640 = a.alloc(632).unwrap(); // class 20
        assert_eq!(r640.start(), base + 320 + Seg::OVERHEAD);
        let r64 = a.alloc(56).unwrap(); // class 3
        assert_eq!(r64.start(), base + 960 + Seg::OVERHEAD);
        assert_eq!(
            unsafe { a.recover() }.unwrap(),
            0,
            "arena fully accounted for after carve"
        );
    }

    #[test]
    fn seg_oversized_non_exact_reuse_carves_excess() {
        let (a, _g) = new_alloc();
        let x = a.alloc(5000).unwrap(); // oversized, block 5008
        let off_x = x.start();
        let base = off_x - Seg::OVERHEAD;
        let _pin = a.alloc(50).unwrap(); // pins the tail so X is interior
        a.dealloc(x).unwrap(); // X → oversized free list (size 5008)
        // Y needs block 4112 (oversized); reuses X (5008 ≥ 4112), carves 896.
        let y = a.alloc(4090).unwrap();
        assert_eq!(y.start(), off_x, "oversized reuse hands back X's block");
        // The 896 excess is itself class 22 (greedy → one block, not 7×128).
        let z = a.alloc(888).unwrap(); // class 22 (block 896)
        assert_eq!(z.start(), base + 4112 + Seg::OVERHEAD);
        assert_eq!(unsafe { a.recover() }.unwrap(), 0);
    }

    #[test]
    fn seg_oversized_reuse_retains_small_excess_whole() {
        let (a, _g) = new_alloc();
        let x = a.alloc(5000).unwrap(); // oversized, block 5008
        let off_x = x.start();
        let _pin = a.alloc(50).unwrap(); // pins the tail so X is interior
        a.dealloc(x).unwrap(); // X → oversized free list (size 5008)
        // Y needs block 4864 (oversized); reuses X whole (excess 144 < SPLIT_MIN).
        let y = a.alloc(4850).unwrap();
        assert_eq!(y.start(), off_x, "oversized reuse hands back X's block");
        assert_eq!(
            unsafe { a.recover() }.unwrap(),
            0,
            "block strides its full size"
        );
        // Freed, the retained block returns to the oversized list at its full size.
        a.dealloc(y).unwrap();
        let z = a.alloc(5000).unwrap();
        assert_eq!(z.start(), off_x, "retained 5008 block recycled");
    }

    #[test]
    fn seg_double_free_detected() {
        let (a, _g) = new_alloc();
        let s = a.alloc(64).unwrap();
        let (start, len) = (s.start(), s.len());
        a.dealloc(s).unwrap();
        let dup = unsafe { crate::alloc::BStackOwnedSlice::from_raw_parts(&a, start, len) };
        let err = a.dealloc(dup).unwrap_err();
        assert_eq!(err.source.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn seg_dealloc_rejects_malformed_pointer() {
        let (a, _g) = new_alloc();
        // Each malformed handle must be rejected without panic, with the handle
        // handed back intact: (a) underflowing/header-range start, and
        // (b) a mid-block start whose block_start is not QUANTUM-aligned.
        for start in [4u64, Seg::ARENA_START + Seg::OVERHEAD + 1] {
            let bogus = unsafe { crate::alloc::BStackOwnedSlice::from_raw_parts(&a, start, 16) };
            let err = a.dealloc(bogus).unwrap_err();
            assert_eq!(err.source.kind(), std::io::ErrorKind::InvalidInput);
            assert!(
                err.handle.is_some(),
                "malformed free must return the handle"
            );
        }
    }

    #[test]
    fn seg_oversized_tail_dealloc_shrinks() {
        let (a, _g) = new_alloc();
        let base = a.stack().len().unwrap();
        let s = a.alloc(5000).unwrap(); // oversized, at the tail
        assert!(a.stack().len().unwrap() > base);
        a.dealloc(s).unwrap();
        assert_eq!(
            a.stack().len().unwrap(),
            base,
            "tail oversized block discarded"
        );
    }

    // ── Foreign handles ───────────────────────────────────────────────────

    #[test]
    fn seg_dealloc_and_realloc_reject_a_handle_from_another_instance() {
        let (a1, _g1) = new_alloc();
        let (a2, _g2) = new_alloc();

        let h = a1.alloc(64).unwrap();
        assert!(h.is_from(&a1));
        assert!(!h.is_from(&a2));
        let range = h.as_range();

        let err = a2.dealloc(h).expect_err("a2 must refuse a1's handle");
        assert_eq!(err.source.kind(), std::io::ErrorKind::InvalidInput);
        let h = err
            .handle
            .expect("a refused handle is returned, not leaked");
        assert_eq!(h.as_range(), range);

        let err = a2.realloc(h, 128).expect_err("a2 must refuse a1's handle");
        assert_eq!(err.source.kind(), std::io::ErrorKind::InvalidInput);
        let h = err
            .handle
            .expect("a refused handle is returned, not leaked");
        assert_eq!(h.as_range(), range);

        // `a2`'s bookkeeping never saw the foreign block, so it still
        // round-trips its own allocations, and `a1` can still free the region.
        let own = a2.alloc(64).unwrap();
        a2.dealloc(own).map_err(|e| e.source).unwrap();
        a1.dealloc(h).map_err(|e| e.source).unwrap();
    }
}
