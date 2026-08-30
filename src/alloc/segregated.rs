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
//! Implemented: `new`/`open`, `alloc`, `dealloc`, `realloc`, `realloc_inplace`
//! (back-edge only — the fixed block base rules out front moves; a back grow uses
//! the same tail extend as `realloc`), and `recover` (linear-scan free-list
//! rebuild + leak reclaim). Still pending: the background coalescer.
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
//! under `atomic`, where one atomic operation fuses recording the new physical size
//! with dropping the excess; the non-`atomic` build cannot fuse them without a
//! crash window `recover` mis-parses, so it simply **retains** the excess inside
//! the still-recorded larger block (zero extra writes, no move).

use super::{
    BStackAllocError, BStackAllocator, BStackInPlaceResizeAllocator, BStackOwnedSlice,
    BStackUninitAllocator, ensure_own_handle,
};
#[cfg(feature = "atomic")]
use super::{BStackBulkAllocError, BStackBulkAllocator, ensure_own_handles};
use crate::BStack;
#[cfg(feature = "atomic")]
use crate::BStackGenOp;
#[cfg(feature = "atomic")]
use crate::{bstack_unsafe_reborrow, bstack_unsafe_reborrow_mut};
#[cfg(not(feature = "atomic"))]
use std::cell::Cell;
#[cfg(feature = "atomic")]
use std::collections::HashSet;
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
            .ok_or_else(|| io_error!(InvalidInput, "allocation length overflows u64"))?;
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
            return Err(io_error!(
                InvalidInput,
                "slice start is not a valid block pointer"
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
            return Err(io_error!(
                UnexpectedEof,
                "stack too short to contain allocator header"
            ));
        }

        let mut magic = [0u8; 8];
        stack.get_into(Self::OFFSET_SIZE, &mut magic)?;
        if magic[..ALSG_MAGIC_PREFIX.len()] != ALSG_MAGIC_PREFIX {
            return Err(io_error!(
                InvalidData,
                "invalid magic: not a SegregatedBStackAllocator file of the expected version"
            ));
        }
        if stack_len < Self::ARENA_START {
            // Make a zeroed free_head array
            let needed = Self::ARENA_START - stack_len;
            let _ = stack.extend(needed)?;
        } else if (stack_len - Self::ARENA_START) % Self::QUANTUM != 0 {
            // Every block is a multiple of QUANTUM, so the arena byte count is too.
            return Err(io_error!(
                UnexpectedEof,
                "arena is not a multiple of the block quantum"
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
                // carve the rest, all as one crash-atomic operation (claim
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
    /// crash-atomic [`BStack::inplace_gen`] operation.
    ///
    /// `region` is greedily decomposed into class free blocks — the largest class
    /// `≤` the remainder, repeated (a region `> MAX_CLASS` becomes one oversized
    /// block). Every piece is a distinct class (greedy remainders strictly
    /// shrink), so each free-list head is read once and rewritten once: the
    /// operation reads every involved head, writes `prefix`, writes each
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

        // Atomic path with an `inplace_gen` batch: read each head, write the prefix,
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
    /// | Shrink, reclaimable excess (`≥ SPLIT_MIN`) | `atomic`: drop the excess — tail `Len` + `Atrunc`, else an in-place carve — recording the new size in one operation; without `atomic`, retain the excess in place |
    /// | Shrink, excess below `SPLIT_MIN` | retain in place — no write |
    /// | Non-tail grow past the block | alloc new class, copy, dealloc old |
    ///
    /// The visible length lives in the returned handle, not on disk, so a resize
    /// that fits (or a shrink whose excess is retained) touches no metadata at
    /// all. Every path only ever *leaks* on a mid-op failure (never corrupts): the
    /// tail grow records the new physical size leak-preferring so a crash leaves
    /// an orphaned zero tail that [`recover`](Self::recover) reclaims, and an
    /// `atomic` shrink commits the new size together with the truncation (resp.
    /// the carve) as a single operation, so a crash leaves the block wholly
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
    /// [`BStack::inplace_gen`] operation.
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
                return Err(io_error!(
                    InvalidInput,
                    "double free: block is already free"
                ));
            }
            let size = (word & !Self::IN_USE_BIT) << 4;
            if Self::phys_need(len)? > size {
                return Err(io_error!(InvalidInput, "cannot free a mismatched slice"));
            }
            let class = Self::classify(size);

            if size > Self::MAX_CLASS {
                let end = block_start
                    .checked_add(size)
                    .ok_or_else(|| io_error!(InvalidInput, "block end overflows u64"))?;
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

#[cfg(feature = "atomic")]
impl SegregatedBStackAllocator {
    /// Pop up to `want[c]` blocks from every classed free list `c` in **one**
    /// crash-atomic [`BStack::inplace_gen`], advancing each touched `head[c]` past
    /// its last popped block. Every head advance commits together: on any error
    /// nothing is popped — no class is left with blocks detached while another
    /// keeps its head (the O(n) cross-class leak a per-class chase would risk).
    ///
    /// The generator runs in two phases: a **read phase** chases every classed
    /// list and computes each touched class's new head, then a **write phase**
    /// emits those head advances. Because no write is staged until all reads have
    /// completed, a read failure or a detected cycle can bail with `None` (which
    /// commits the — still empty — batch), so no `Abort` is needed and no head
    /// slot is mutated after a write is staged (the reborrow aliasing rule). The
    /// oversized class is skipped (matched separately, and `want[OVERSIZED]` is
    /// never set). Returns the popped block starts flattened in ascending class
    /// order, plus the per-class count actually popped (fewer than `want[c]` if
    /// that list ran out). Popped blocks are left **free-tagged and detached** —
    /// the caller claims them — and a crash before the claim leaves them
    /// reclaimable by [`recover`](Self::recover).
    fn pop_all_classes(
        &self,
        want: &[usize],
    ) -> io::Result<(Vec<u64>, [usize; Self::NUM_CLASSES as usize])> {
        let num = Self::NUM_CLASSES as usize;
        let mut counts = [0usize; Self::NUM_CLASSES as usize];
        // Total blocks wanted across all classes (the oversized bucket never has
        // `want > 0`). Zero means nothing to pop.
        let total_want: usize = want.iter().take(num).sum();
        if total_want == 0 {
            return Ok((Vec::new(), counts));
        }
        let mut popped: Vec<u64> = Vec::with_capacity(total_want);
        let mut seen: HashSet<u64> = HashSet::with_capacity(total_want);
        let mut head_buf = [0u8; 8];
        let mut next_buf = [0u8; 8];
        // Each touched class's new head value, all computed in the read phase and
        // only emitted in the write phase: a head slot is never mutated after any
        // write is staged, so the reborrow is sound. `advance[c]` marks the
        // classes that actually popped a block (an empty list advances nothing).
        let mut head_writes = [[0u8; 8]; Self::NUM_CLASSES as usize];
        let mut advance = [false; Self::NUM_CLASSES as usize];
        let mut err: Option<io::Error> = None;

        #[derive(Clone, Copy)]
        enum St {
            // Read phase: chase class `c`'s chain (empty `want[c]` skipped).
            Chase(usize),
            ConsumeHead(usize),
            ReadNode(usize, u64),
            ConsumeNode(usize, u64),
            // Write phase: emit the advanced heads, then finish.
            Emit(usize),
            Done,
        }
        let mut st = St::Chase(0);
        let mut in_class = 0usize; // blocks popped so far from the current class

        self.stack.inplace_gen(|res| {
            // A prior read failed. All writes live in the write phase, entered
            // only once every read has completed, so nothing is staged yet:
            // ending with `None` commits the empty batch (pops nothing).
            if let Err(e) = res {
                err = Some(e);
                return None;
            }
            loop {
                match st {
                    // --- read phase ---
                    St::Chase(c) => {
                        if c >= num {
                            st = St::Emit(0);
                            continue;
                        }
                        if want[c] == 0 {
                            st = St::Chase(c + 1);
                            continue;
                        }
                        in_class = 0;
                        st = St::ConsumeHead(c);
                        return Some(BStackGenOp::Read {
                            offset: Self::head_off(c as u64),
                            // SAFETY: `head_buf` outlives this call.
                            buf: bstack_unsafe_reborrow_mut!(&mut head_buf[..]),
                        });
                    }
                    St::ConsumeHead(c) => {
                        let head = u64::from_le_bytes(head_buf);
                        if head == Self::SENTINEL {
                            st = St::Chase(c + 1); // empty list: no head advance
                        } else {
                            st = St::ReadNode(c, head);
                        }
                    }
                    St::ReadNode(c, cursor) => {
                        // A revisited block means a cycle: bail popping nothing.
                        // No write is staged yet, so `None` commits nothing.
                        if !seen.insert(cursor) {
                            err = Some(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "alloc_bulk: free-list cycle detected",
                            ));
                            return None;
                        }
                        st = St::ConsumeNode(c, cursor);
                        return Some(BStackGenOp::Read {
                            offset: cursor + Self::OVERHEAD,
                            // SAFETY: `next_buf` outlives this call.
                            buf: bstack_unsafe_reborrow_mut!(&mut next_buf[..]),
                        });
                    }
                    St::ConsumeNode(c, cursor) => {
                        popped.push(cursor);
                        counts[c] += 1;
                        in_class += 1;
                        let next = u64::from_le_bytes(next_buf);
                        if in_class == want[c] || next == Self::SENTINEL {
                            head_writes[c] = next.to_le_bytes();
                            advance[c] = true;
                            st = St::Chase(c + 1);
                        } else {
                            st = St::ReadNode(c, next);
                        }
                    }
                    // --- write phase ---
                    St::Emit(c) => {
                        let mut c = c;
                        while c < num && !advance[c] {
                            c += 1;
                        }
                        if c >= num {
                            st = St::Done;
                            return None; // commit the accumulated head advances
                        }
                        st = St::Emit(c + 1);
                        return Some(BStackGenOp::Write {
                            offset: Self::head_off(c as u64),
                            // SAFETY: `head_writes[c]` outlives this call and was
                            // set in the read phase, never mutated after any write.
                            data: bstack_unsafe_reborrow!(&head_writes[c][..]),
                        });
                    }
                    St::Done => return None,
                }
            }
        })?;
        if let Some(e) = err {
            return Err(e);
        }
        Ok((popped, counts))
    }

    /// Best-effort: return free-tagged detached blocks to their class lists after
    /// an `alloc_bulk` failure, grouped by class so each touched class is spliced
    /// back in **one** [`splice_class_chain`](Self::splice_class_chain) (one
    /// `set_batched` + one `cross_exchange`) rather than one `push` per block.
    /// Errors are swallowed — a block that cannot be re-pushed is a free-tagged
    /// leak `recover` reclaims.
    fn repush_detached(&self, blocks: &[(u64, u64)]) {
        if blocks.is_empty() {
            return;
        }
        // Bucket by class without a Vec-per-class: sort one working copy by class
        // (classify recomputed on demand, so no `(class, off, size)` triple), then
        // splice each contiguous run straight from a sub-slice.
        let mut blocks = blocks.to_vec();
        blocks.sort_unstable_by_key(|&(_, size)| Self::classify(size));
        let mut i = 0usize;
        while i < blocks.len() {
            let class = Self::classify(blocks[i].1);
            let mut j = i + 1;
            while j < blocks.len() && Self::classify(blocks[j].1) == class {
                j += 1;
            }
            let _ = self.splice_class_chain(class, &blocks[i..j]);
            i = j;
        }
    }

    /// Detach the **entire** oversized free list in one [`BStack::process_gen`]:
    /// chase every block collecting `(block_start, size)` and set the oversized
    /// head to the sentinel as the single terminating write. The returned blocks
    /// are left free-tagged (their overhead is untouched) and detached; the caller
    /// matches them against oversized requests and re-splices the unmatched ones.
    /// A crash after the head write leaves them reclaimable by
    /// [`recover`](Self::recover). Trusts the list to be acyclic, as the rest of
    /// the allocator does.
    fn detach_oversized_all(&self) -> io::Result<Vec<(u64, u64)>> {
        let head_off = Self::head_off(Self::OVERSIZED_CLASS);
        let mut blocks: Vec<(u64, u64)> = Vec::new();
        let mut seen: HashSet<u64> = HashSet::new();
        let mut head_buf = [0u8; 8];
        let mut node_buf = [0u8; 16]; // [0..8] overhead (free tag), [8..16] next
        let sentinel = [0u8; 8];
        // Set if the (unbounded) chase revisits a block; without this a cyclic
        // free list would loop forever under the write lock. Abort with no write.
        let mut cycle = false;

        #[derive(Clone, Copy)]
        enum St {
            ReadHead,
            ConsumeHead,
            ReadNode(u64),
            ConsumeNode(u64),
            Done,
        }
        let mut st = St::ReadHead;

        self.stack.process_gen(|| {
            loop {
                match st {
                    St::ReadHead => {
                        st = St::ConsumeHead;
                        return Some(BStackGenOp::Read {
                            offset: head_off,
                            // SAFETY: `head_buf` outlives this call.
                            buf: bstack_unsafe_reborrow_mut!(&mut head_buf[..]),
                        });
                    }
                    St::ConsumeHead => {
                        let head = u64::from_le_bytes(head_buf);
                        if head == Self::SENTINEL {
                            return None; // empty: nothing detached, no write
                        }
                        st = St::ReadNode(head);
                    }
                    St::ReadNode(cursor) => {
                        if !seen.insert(cursor) {
                            cycle = true;
                            return None;
                        }
                        st = St::ConsumeNode(cursor);
                        return Some(BStackGenOp::Read {
                            offset: cursor,
                            // SAFETY: `node_buf` outlives this call.
                            buf: bstack_unsafe_reborrow_mut!(&mut node_buf[..]),
                        });
                    }
                    St::ConsumeNode(cursor) => {
                        let size = read_buf_le!(node_buf, 0 => u64) << 4;
                        let next = read_buf_le!(node_buf, 8 => u64);
                        blocks.push((cursor, size));
                        if next == Self::SENTINEL {
                            st = St::Done;
                            return Some(BStackGenOp::Write {
                                offset: head_off,
                                // SAFETY: `sentinel` outlives this call.
                                data: bstack_unsafe_reborrow!(&sentinel[..]),
                            });
                        }
                        st = St::ReadNode(next);
                    }
                    St::Done => return None,
                }
            }
        })?;
        if cycle {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "alloc_bulk: oversized free-list cycle detected",
            ));
        }
        Ok(blocks)
    }

    /// Splice a run of blocks onto `class`'s free list as one chain: stage each
    /// block's `[free overhead (size>>4) | next]` with **one**
    /// [`BStack::set_batched`] (unreachable until spliced), then splice the whole
    /// run with one atomic [`BStack::cross_exchange`] — so a concurrent push/pop
    /// cannot be lost. Every `blocks[i]` must map to `class` (all classed blocks of
    /// one class share a size; the oversized bucket holds any size). No-op if empty.
    fn splice_class_chain(&self, class: u64, blocks: &[(u64, u64)]) -> io::Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }
        let mut batch: Vec<(u64, [u8; 16])> = Vec::with_capacity(blocks.len());
        for (i, &(block, size)) in blocks.iter().enumerate() {
            // Last block's next is the placeholder `blocks[0]` that cross_exchange
            // replaces with the old head.
            let next = if i + 1 < blocks.len() {
                blocks[i + 1].0
            } else {
                blocks[0].0
            };
            let mut buf = [0u8; 16];
            write_buf!(size >> 4 => buf, 0);
            write_buf!(next => buf, 8);
            batch.push((block, buf));
        }
        self.stack.set_batched(batch)?;
        self.stack.cross_exchange(
            blocks[blocks.len() - 1].0 + Self::OVERHEAD,
            Self::head_off(class),
            8,
        )
    }
}

/// How the final `alloc_bulk` claim writes one block.
#[cfg(feature = "atomic")]
#[derive(Clone, Copy)]
enum ClaimKind {
    /// Reused block (popped or oversized-retained): mark in-use, scrub the body.
    Reuse,
    /// Freshly extended block: mark in-use (its body is already sparse-zero).
    Fresh,
    /// Carve remainder: mark free so [`recover`](SegregatedBStackAllocator::recover) relinks it.
    CarveFree,
}

/// One claim write's payload: an 8-byte overhead word carried by value, or a
/// body-scrub slice borrowing the shared zero buffer. Both are `AsRef<[u8]>`, so
/// a single [`BStack::set_batched`] iterator can mix them without a per-block
/// heap buffer for the overhead.
#[cfg(feature = "atomic")]
enum ClaimBuf<'z> {
    Oh([u8; 8]),
    Body(&'z [u8]),
}

#[cfg(feature = "atomic")]
impl AsRef<[u8]> for ClaimBuf<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            ClaimBuf::Oh(oh) => oh,
            ClaimBuf::Body(b) => b,
        }
    }
}

/// [`SegregatedBStackAllocator`] batches whole runs across its size classes.
///
/// Work is bounded by the number of *distinct classes touched* (≤ `NUM_CLASSES`),
/// not by the request count: each class contributes one free-list chase, the
/// misses share one tail [`BStack::extend`], and every claim commits in one
/// [`BStack::set_batched`]. Requires `atomic` (the batching rides
/// `process_gen` / `set_batched` / `cross_exchange`); without it, use the
/// single-item [`alloc`](BStackAllocator::alloc) / [`dealloc`](BStackAllocator::dealloc).
#[cfg(feature = "atomic")]
impl BStackBulkAllocator for SegregatedBStackAllocator {
    /// Allocate one independently-freeable region per requested length.
    ///
    /// Classed requests (`phys_need ≤ MAX_CLASS`) are counted per class and every
    /// touched class's free list is drained together in **one** atomic
    /// [`pop_all_classes`](Self::pop_all_classes) — a failure pops nothing, so no
    /// class is left with blocks detached while another keeps its head. Oversized
    /// requests reuse the oversized free list: it is detached whole and each free
    /// block is assigned largest-request-first to the largest request it fits; a
    /// block whose slack clears [`SPLIT_MIN`](Self::SPLIT_MIN) is carved and the
    /// remainder freed (matching single [`alloc`](BStackAllocator::alloc)), the
    /// rest re-spliced back. Every remaining miss is served from **one**
    /// [`BStack::extend_sparse_batched`] that writes each fresh block's *free*
    /// overhead: the grown region is self-describing (never a mid-arena zero hole
    /// that [`recover`](Self::recover) would mistake for an orphaned tail and
    /// truncate, dropping a concurrent thread's blocks beyond it). Reused blocks
    /// are then scrubbed and marked in-use, fresh blocks flipped to in-use, and
    /// carve remainders written free — all in **one** [`BStack::set_batched`] with
    /// buffers shared per size — after which the free remainders and unmatched
    /// oversized blocks are spliced onto their class lists. Zero-length requests
    /// yield the null sentinel slice.
    ///
    /// # Atomicity
    ///
    /// Every block is detached / extended **free-tagged** and only flipped in-use
    /// by the final `set_batched`, so a crash before it leaves them reclaimable by
    /// [`recover`](Self::recover). On an I/O failure the fresh tail is discarded
    /// first and the detached blocks re-pushed to their class lists, both
    /// best-effort; a fresh tail a concurrent extend has grown past stays
    /// free-tagged and is reclaimed by `recover`.
    fn alloc_bulk(
        &self,
        lengths: impl AsRef<[u64]>,
    ) -> Result<Vec<Self::Allocated<'_>>, Self::Error> {
        let lengths = lengths.as_ref();
        if lengths.is_empty() {
            return Ok(Vec::new());
        }
        let n = lengths.len();
        let num = Self::NUM_CLASSES as usize;

        // Per-request physical block size (0 marks a zero-length/null request) and
        // its class. Classed (non-oversized) requests are counted per class; the
        // oversized ones are collected for separate matching. No `Vec<Vec>`.
        let mut block_of = vec![0u64; n];
        let mut class_of = vec![0u64; n];
        // Per-class counts and offsets are bounded by `NUM_CLASSES` (a constant),
        // so they live on the stack, not the heap.
        let mut want = [0usize; Self::NUM_CLASSES as usize];
        let mut oversized_reqs: Vec<usize> = Vec::new();
        for (i, &len) in lengths.iter().enumerate() {
            if len == 0 {
                continue;
            }
            let block = Self::class_blocksize(Self::phys_need(len)?);
            block_of[i] = block;
            let class = Self::classify(block);
            class_of[i] = class;
            if class == Self::OVERSIZED_CLASS {
                oversized_reqs.push(i);
            } else {
                want[class as usize] += 1;
            }
        }

        // Counting-sort classed request indices into `order`, grouped by class,
        // with `req_base[c]` the start of class `c`'s run.
        let mut req_base = [0usize; Self::NUM_CLASSES as usize + 1];
        for c in 0..num {
            req_base[c + 1] = req_base[c] + want[c];
        }
        let mut order = vec![0usize; req_base[num]];
        {
            let mut cur = req_base; // Copy: a per-class write cursor.
            for (i, &len) in lengths.iter().enumerate() {
                if len == 0 {
                    continue;
                }
                let c = class_of[i] as usize;
                if c != Self::OVERSIZED_CLASS as usize {
                    order[cur[c]] = i;
                    cur[c] += 1;
                }
            }
        }

        // Every free-tagged block we detached/created, for rollback repush.
        let mut detached: Vec<(u64, u64)> = Vec::new();
        let mut ext_base = 0u64;
        let mut ext_bytes = 0u64;

        let build = (|| -> io::Result<Vec<BStackOwnedSlice<'_, Self>>> {
            // Block starts are always ≥ ARENA_START > 0, so 0 marks an unassigned
            // request; zero-length requests keep 0 and become null slices.
            let mut assign: Vec<u64> = vec![0u64; n];
            // Every claim the final `set_batched` applies, tagged by kind.
            let mut claims: Vec<(u64, u64, ClaimKind)> = Vec::new(); // (offset, size, kind)
            let mut to_splice: Vec<(u64, u64)> = Vec::new(); // free blocks to relink (best-effort)

            // 1. Atomic classed pop across all touched classes. `popped` is
            //    already flattened in ascending class order, so one running index
            //    walks it — no per-class base array.
            let (popped, popped_counts) = self.pop_all_classes(&want)?;
            let mut pi = 0usize;
            for c in 0..num {
                for k in 0..popped_counts[c] {
                    let i = order[req_base[c] + k];
                    let bs = popped[pi];
                    pi += 1;
                    let size = block_of[i]; // classed: block == class block size
                    assign[i] = bs;
                    detached.push((bs, size));
                    claims.push((bs, size, ClaimKind::Reuse));
                }
                // order[req_base[c]+popped_counts[c] .. req_base[c]+want[c]] are
                // misses served by the extend below.
            }

            // 2. Oversized matching (largest request first), with SPLIT_MIN carve.
            if !oversized_reqs.is_empty() {
                let free_blocks = self.detach_oversized_all()?;
                // Match largest request first; `oversized_reqs` is not used again,
                // so sort it in place rather than cloning.
                let reqs = &mut oversized_reqs;
                reqs.sort_unstable_by(|&x, &y| block_of[y].cmp(&block_of[x]));
                let mut matched = vec![false; reqs.len()];
                for &(boff, bsize) in &free_blocks {
                    let mut used = false;
                    for (ri, &req) in reqs.iter().enumerate() {
                        if !matched[ri] && block_of[req] <= bsize {
                            let block = block_of[req];
                            assign[req] = boff;
                            matched[ri] = true;
                            // Rollback frees the whole block (nothing is carved
                            // until the claim commits).
                            detached.push((boff, bsize));
                            let excess = bsize - block;
                            if excess >= Self::SPLIT_MIN {
                                // Carve: claim `block`, free the remainder greedily
                                // into class blocks (their free overhead is written
                                // by the claim, so the region stays recover-safe).
                                claims.push((boff, block, ClaimKind::Reuse));
                                let mut off = boff + block;
                                let mut rem = excess;
                                while rem > 0 {
                                    let ps = if rem > Self::MAX_CLASS {
                                        rem
                                    } else {
                                        Self::largest_class_le(rem)
                                    };
                                    claims.push((off, ps, ClaimKind::CarveFree));
                                    to_splice.push((off, ps));
                                    off += ps;
                                    rem -= ps;
                                }
                            } else {
                                // Retain the whole block (a non-class size is sound
                                // in the heterogeneous oversized bucket).
                                claims.push((boff, bsize, ClaimKind::Reuse));
                            }
                            used = true;
                            break;
                        }
                    }
                    if !used {
                        // Unmatched: relink on success, repush on rollback.
                        detached.push((boff, bsize));
                        to_splice.push((boff, bsize));
                    }
                }
            }

            // 3. Misses (classed short + unmatched oversized): one extend that
            //    writes each fresh block's free overhead (self-describing tail).
            let mut misses: Vec<usize> = Vec::new();
            let mut total_ext = 0u64;
            for i in 0..n {
                if block_of[i] > 0 && assign[i] == 0 {
                    total_ext = total_ext.checked_add(block_of[i]).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "alloc_bulk: total allocation size overflows u64",
                        )
                    })?;
                    misses.push(i);
                }
            }
            if total_ext > 0 {
                // Each fresh block's free overhead, laid out back-to-back from the
                // extend base. `[u8; 8]` is `AsRef<[u8]>`, so the writes are yielded
                // by value — no `rels`/`oh_bufs` parallel Vecs.
                let writes = misses.iter().scan(0u64, |rel, &i| {
                    let off = *rel;
                    let size = block_of[i];
                    *rel += size;
                    let mut oh = [0u8; 8];
                    write_buf!(size >> 4 => oh, 0); // free tag: high bit clear
                    Some((off, oh))
                });
                ext_base = self.stack.extend_sparse_batched(writes, total_ext)?;
                ext_bytes = total_ext;
                let mut rel = 0u64;
                for &i in &misses {
                    let abs = ext_base + rel;
                    assign[i] = abs;
                    claims.push((abs, block_of[i], ClaimKind::Fresh)); // flip in-use
                    rel += block_of[i];
                }
            }

            // 4. Claim: one crash-atomic `set_batched`. Each block contributes its
            //    8-byte overhead word (carried by value — in-use for `Reuse`/`Fresh`,
            //    free for `CarveFree`); a `Reuse` block additionally scrubs its
            //    body, all of which is zero, so every scrub borrows one shared zero
            //    buffer sub-sliced to its length (no per-block heap buffer).
            let max_scrub = claims
                .iter()
                .filter(|(_, _, k)| matches!(k, ClaimKind::Reuse))
                .map(|&(_, s, _)| s)
                .max()
                .unwrap_or(0);
            let zeros = vec![0u8; max_scrub.saturating_sub(Self::OVERHEAD) as usize];
            let mut batch: Vec<(u64, ClaimBuf)> = Vec::with_capacity(claims.len() * 2);
            for &(off, size, kind) in &claims {
                let word = match kind {
                    ClaimKind::Reuse | ClaimKind::Fresh => Self::IN_USE_BIT | (size >> 4),
                    ClaimKind::CarveFree => size >> 4, // free tag: high bit clear
                };
                let mut oh = [0u8; 8];
                write_buf!(word => oh, 0);
                batch.push((off, ClaimBuf::Oh(oh)));
                if matches!(kind, ClaimKind::Reuse) {
                    let body = (size - Self::OVERHEAD) as usize;
                    if body > 0 {
                        // Adjacent to the overhead, non-overlapping: a body scrub.
                        batch.push((off + Self::OVERHEAD, ClaimBuf::Body(&zeros[..body])));
                    }
                }
            }
            if !batch.is_empty() {
                self.stack.set_batched(batch)?;
            }

            // 5. Relink the now-free carve remainders and unmatched oversized
            //    blocks. Best-effort: the allocations are already valid, and a
            //    block left unspliced stays free-tagged (reclaimed by `recover`).
            self.repush_detached(&to_splice);

            // Build the result in input order (null slices for zero-length).
            let mut result = Vec::with_capacity(n);
            for (i, &len) in lengths.iter().enumerate() {
                if len == 0 {
                    result.push(BStackOwnedSlice::empty(self));
                } else {
                    // Every non-zero request was assigned a nonzero block start.
                    let block_start = assign[i];
                    // SAFETY: a live block just popped or extended; its data region
                    // begins at `block_start + OVERHEAD` and spans `len` bytes.
                    result.push(unsafe {
                        BStackOwnedSlice::from_raw_parts(self, block_start + Self::OVERHEAD, len)
                    });
                }
            }
            Ok(result)
        })();

        build.inspect_err(|_| {
            // Best-effort rollback: drop the fresh tail first, then return the
            // detached blocks to their lists (a fresh tail a concurrent extend
            // grew past stays free-tagged and is reclaimed by `recover`).
            if ext_bytes > 0 {
                let _ = self.stack.try_discard(ext_base + ext_bytes, ext_bytes);
            }
            self.repush_detached(&detached);
        })
    }

    /// Free every handle in one batch.
    ///
    /// Each handle's overhead is validated (a clear in-use bit, a length larger
    /// than the block, or a block repeated within the batch is rejected before any
    /// write, returning all handles). Freed blocks are chained per class in **one**
    /// pass — a `prev[class]` cursor links each block to the previous one of its
    /// class, so no grouping sort or per-class buffer is needed — then staged into
    /// the freed block bodies with **one** [`BStack::set_batched`] (unreachable
    /// until spliced) and spliced onto each class head with one atomic
    /// [`BStack::cross_exchange`], so a concurrent push/pop is never lost. Null
    /// sentinel handles are ignored.
    ///
    /// Unlike the single-item [`dealloc`](BStackAllocator::dealloc), an oversized
    /// block at the tail is **not** discarded — every block goes to its free list.
    ///
    /// # Atomicity
    ///
    /// The staging batch is crash-atomic and every class splice is attempted even
    /// if one fails, so a partial splice failure still relinks the classes it can;
    /// any class left unspliced (or a crash after staging) leaves its blocks
    /// free-tagged and reclaimed by `recover`. Once staging begins no handle can be
    /// returned, so [`BStackBulkAllocError::handles`] is empty.
    fn dealloc_bulk<'a>(
        &'a self,
        handles: impl IntoIterator<Item = Self::Allocated<'a>>,
    ) -> Result<(), BStackBulkAllocError<'a, Self>> {
        let slices: Vec<BStackOwnedSlice<'a, Self>> = handles.into_iter().collect();
        let slices = ensure_own_handles(self, slices, "SegregatedBStackAllocator::dealloc_bulk")?;

        let mut freeing = false;
        let result = (|| -> io::Result<()> {
            // Validate every handle and build the per-class chains in one pass.
            // `prev[class]` holds the batch index of the previous freed block of
            // that class (linked to the current one as we go); `first[class]` its
            // chain tail (seeded to the chain head once the pass ends). Reject a bad
            // batch whole before any write.
            let mut batch: Vec<(u64, [u8; 16])> = Vec::new(); // (block, [free oh | next])
            // Per-class state is bounded by `NUM_CLASSES` (a constant), so it lives
            // on the stack. `touched[..ntouched]` lists the classes that got a
            // block, in first-touch order.
            let mut prev = [None::<usize>; Self::NUM_CLASSES as usize];
            let mut first = [Self::SENTINEL; Self::NUM_CLASSES as usize];
            let mut touched = [0usize; Self::NUM_CLASSES as usize];
            let mut ntouched = 0usize;
            let mut seen: HashSet<u64> = HashSet::new();
            for s in &slices {
                if s.is_empty() {
                    continue;
                }
                let block_start = Self::block_start_of(s.start())?;
                let word = u64::from_le_bytes(read_bstack!(self.stack, block_start => u64));
                if word & Self::IN_USE_BIT == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "dealloc_bulk: double free: block is already free",
                    ));
                }
                let size = (word & !Self::IN_USE_BIT) << 4;
                if Self::phys_need(s.len())? > size {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "dealloc_bulk: cannot free a mismatched slice",
                    ));
                }
                if !seen.insert(block_start) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "dealloc_bulk: double free: block appears twice",
                    ));
                }
                let class = Self::classify(size) as usize;
                let idx = batch.len();
                let mut buf = [0u8; 16]; // [0..8] free overhead (size>>4), [8..16] next
                write_buf!(size >> 4 => buf, 0);
                batch.push((block_start, buf));
                if let Some(p) = prev[class] {
                    // Link the previous block of this class to this one.
                    write_buf!(block_start => batch[p].1, 8);
                } else {
                    first[class] = block_start;
                    touched[ntouched] = class;
                    ntouched += 1;
                }
                prev[class] = Some(idx);
            }
            if batch.is_empty() {
                return Ok(());
            }

            // Close each class chain: the last block's next is seeded to the chain
            // head (`first`), the placeholder `cross_exchange` swaps with the head.
            let mut splices = [(0u64, 0u64); Self::NUM_CLASSES as usize];
            for (s, &class) in touched[..ntouched].iter().enumerate() {
                let last = prev[class].expect("touched class has a last block");
                let last_off = batch[last].0;
                write_buf!(first[class] => batch[last].1, 8);
                splices[s] = (last_off, class as u64);
            }

            freeing = true;
            self.stack.set_batched(batch)?;
            // Attempt every class splice even if one fails; an unspliced class is
            // left free-tagged (reclaimed by `recover`). Surface the first error.
            let mut first_err: Option<io::Error> = None;
            for &(last_block, class) in &splices[..ntouched] {
                if let Err(e) =
                    self.stack
                        .cross_exchange(last_block + Self::OVERHEAD, Self::head_off(class), 8)
                {
                    first_err.get_or_insert(e);
                }
            }
            match first_err {
                Some(e) => Err(e),
                None => Ok(()),
            }
        })();
        result.map_err(|source| BStackBulkAllocError {
            source,
            handles: if freeing { Vec::new() } else { slices },
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
                return Err(io_error!(InvalidInput, "cannot realloc a freed block"));
            }
            let old_size = (word & !Self::IN_USE_BIT) << 4;
            if Self::phys_need(old_len)? > old_size {
                return Err(io_error!(InvalidInput, "cannot realloc a mismatched slice"));
            }
            let new_need = Self::phys_need(new_len)?;
            let new_size = Self::class_blocksize(new_need);

            if new_need <= old_size {
                // The request fits the current physical block — no bigger block
                // needed, and never a move. Shared with `realloc_inplace`, whose
                // append-only in-block case is exactly this path.
                return self.resize_in_block(block_start, start, old_len, new_len, old_size, init);
            }

            // The request needs a physically larger block (`new_need > old_size`,
            // so also `new_size > old_size`). Grow at the tail in place when the
            // block ends at the payload tail; shared with `realloc_inplace`.
            if self.grow_tail_inplace(block_start, start, old_len, old_size, new_size, init)? {
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

    /// Resize a block whose request already fits its recorded physical extent
    /// (`new_need ≤ old_size`, so `new_size ≤ old_size`) — never a bigger block
    /// and never a move. Shared by [`realloc`](BStackAllocator::realloc) (its
    /// fits-the-block arm) and [`realloc_inplace`](BStackInPlaceResizeAllocator::realloc_inplace)
    /// (whose append-only in-block case is exactly this).
    ///
    /// * Visible grow (`new_len > old_len`): the newly-exposed bytes may hold
    ///   stale data from a prior shrink, so scrub them when `init`; the length
    ///   lives in the handle, so nothing else is written.
    /// * Visible shrink (`new_len < old_len`): under `atomic`, reclaim the excess
    ///   when it clears `SPLIT_MIN` — a tail `Len` + `Atrunc`, else an in-place
    ///   carve — recording the new size in the same crash-atomic operation.
    ///   Otherwise (below threshold, or non-`atomic`) retain it in place with zero
    ///   writes. The caller's original `(start, old_len)` block stays intact on a
    ///   mid-op failure, so it is always safe to return as the surviving handle.
    ///
    /// `new_len == old_len` is handled by the callers before they reach here.
    fn resize_in_block<'a>(
        &'a self,
        block_start: u64,
        start: u64,
        old_len: u64,
        new_len: u64,
        old_size: u64,
        init: bool,
    ) -> io::Result<BStackOwnedSlice<'a, Self>> {
        // `block_start` and `old_size` drive only the `atomic` shrink reclaim
        // below; without `atomic` a shrink always retains in place.
        #[cfg(not(feature = "atomic"))]
        let _ = (block_start, old_size);
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

        // Visible shrink (`new_len < old_len`; the `==` case is handled by the
        // callers). No new bytes are exposed. Reclaim the excess only
        // above the split threshold, and only under `atomic`: a shrink's
        // freed tail overlaps still-live caller bytes, so recording the
        // smaller size and dropping the excess must commit together in one
        // operation — the non-`atomic` build cannot fuse them (either
        // ordering leaves a window `recover` mis-parses, and a rollback
        // would hand back a block whose tail the carve already clobbered),
        // so it retains the excess in place (zero writes, no move).
        #[cfg(feature = "atomic")]
        {
            let new_size = Self::class_blocksize(Self::phys_need(new_len)?);
            if new_size < old_size && old_size - new_size >= Self::SPLIT_MIN {
                let old_end = block_start + old_size; // block exists ⇒ ≤ stack_len

                // Tail shrink: replace the whole block with its shrunk self in
                // ONE crash-atomic operation. `Len` confirms the tail under
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
                    return Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, start, new_len) });
                }

                // Non-tail: keep the block at the new class and free the excess
                // tail *in place* as one crash-atomic carve — no move, no copy,
                // and the caller's original handle stays the untouched survivor (a
                // fault leaves the block un-shrunk). The prefix commit records `new_size`.
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
        }

        // Retain in place: the block records its own size and a shrink
        // exposes no new bytes, so this is zero `BStack` writes.
        // SAFETY: same physical block, smaller visible length.
        Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, start, new_len) })
    }

    /// Grow the block at `block_start` from `old_size` to `new_size`
    /// (`new_size > old_size`) by extending the stack tail **in place** — only
    /// when the block ends at the payload tail. Returns `Ok(true)` on success
    /// (extended, old slack scrubbed when `init`, the new size recorded) and
    /// `Ok(false)` when the block is not at the tail, leaving it untouched so the
    /// caller can decide between a move ([`realloc`](BStackAllocator::realloc)) and
    /// `Unsupported` ([`realloc_inplace`](BStackInPlaceResizeAllocator::realloc_inplace)).
    ///
    /// Shared by both. Extend first (leak-preferring: a failure after it only
    /// leaks the extension, never corrupts), then zero the old slack, then record
    /// the new size. Under `atomic`, `try_extend_zeros` fuses the tail check and
    /// the grow into one locked critical section; otherwise we check `len` then
    /// `extend` (which zero-fills the new region via `set_len`). The caller's
    /// original `(start, old_len)` block stays intact on any failure.
    fn grow_tail_inplace(
        &self,
        block_start: u64,
        start: u64,
        old_len: u64,
        old_size: u64,
        new_size: u64,
        init: bool,
    ) -> io::Result<bool> {
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
        if !grew {
            return Ok(false);
        }
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
        Ok(true)
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

#[cfg(feature = "set")]
impl BStackInPlaceResizeAllocator for SegregatedBStackAllocator {
    /// Back-edge in-place resize for the segregated arena. The front edge is fixed
    /// (the overhead word sits immediately before the payload and the block base is
    /// `payload − OVERHEAD`, which both the class free lists and `recover`'s linear
    /// scan key on), so this only ever moves the tail.
    ///
    /// | `(prepend, append)`                     | Behaviour                                                     |
    /// |-----------------------------------------|---------------------------------------------------------------|
    /// | `prepend != 0` (either sign)            | `Unsupported` — moving the front edge would relocate the block base |
    /// | `prepend == 0`, `append == 0`           | identity — the block is untouched, the handle is returned as-is |
    /// | `prepend == 0`, back grow that fits the recorded block | succeed with no metadata write; zero the newly-exposed bytes (a prior shrink may have left stale data there) |
    /// | `prepend == 0`, back grow past the recorded block, block at the stack tail | extend the tail in place (zero-filled), then record the new physical size |
    /// | `prepend == 0`, back grow past the recorded block, block not at the tail | `Unsupported` — a free-neighbour merge is the coalescer's job, and relocating would break the position guarantee |
    /// | `prepend == 0`, back shrink             | retain the excess in place, or (above `SPLIT_MIN`, under `atomic`) reclaim it in one crash-atomic operation |
    /// | `prepend == 0`, shrink to zero          | free the block (delegates to [`dealloc`](BStackAllocator::dealloc)) |
    ///
    /// The visible length lives in the returned handle, not on disk, so a grow that
    /// fits (past the scrub) and a shrink that retains its excess both touch no
    /// metadata at all; only a tail grow past the recorded block records a new size.
    /// Every *resize* path leaves the caller's original block intact on a mid-op
    /// failure — the in-block grow's scrub adds no new block, the tail extend is
    /// leak-preferring (a crash leaks the extension, which [`recover`](Self::recover)
    /// reclaims, and the size is recorded last), and the `atomic` shrink reclaim
    /// commits the new size together with the truncation (or the carve) as a single
    /// operation — so those always return `handle: Some`. The
    /// sole exception is the shrink-to-zero free, which delegates to
    /// [`dealloc`](BStackAllocator::dealloc): a torn free-list splice there can drop
    /// the handle (`handle: None`), leaving the block reclaimable only through
    /// [`recover`](Self::recover), exactly as a direct `dealloc` would.
    fn realloc_inplace<'a>(
        &'a self,
        slice: BStackOwnedSlice<'a, Self>,
        prepend: i64,
        append: i64,
    ) -> Result<BStackOwnedSlice<'a, Self>, BStackAllocError<'a, Self>> {
        // Reject a handle from another allocator instance before any logic runs
        // (see the module's "Foreign handles" section).
        let slice = ensure_own_handle(self, slice, "SegregatedBStackAllocator::realloc_inplace")?;
        // An empty handle anchors no block; resizing it in place is never
        // supported for any `(prepend, append)` (see the trait's "Empty handles").
        if slice.is_empty() {
            return Err(BStackAllocError::with_handle(
                io_error!(
                    Unsupported,
                    "realloc_inplace: cannot resize an empty handle in place"
                ),
                slice,
            ));
        }

        // Resulting length, validated before any I/O. Guard the cast and both
        // additions so a hostile handle or delta cannot wrap into a bogus length.
        let old_len = slice.len();
        let new_len = match i64::try_from(old_len)
            .ok()
            .and_then(|l| l.checked_add(prepend))
            .and_then(|l| l.checked_add(append))
        {
            Some(n) if n >= 0 => n as u64,
            _ => {
                return Err(BStackAllocError::with_handle(
                    io_error!(
                        InvalidInput,
                        "realloc_inplace: resulting length is negative or overflows"
                    ),
                    slice,
                ));
            }
        };

        // The front edge is anchored to the block base, so any nonzero `prepend`
        // would relocate the block — never supported here (see the trait docs).
        if prepend != 0 {
            return Err(BStackAllocError::with_handle(
                io_error!(
                    Unsupported,
                    "realloc_inplace: cannot move the front edge of a segregated block in place"
                ),
                slice,
            ));
        }

        // Shrinking to nothing frees the region; delegate to dealloc, which owns
        // the handle-return contract on failure.
        if new_len == 0 {
            return self.dealloc(slice).map(|()| BStackOwnedSlice::empty(self));
        }

        let start = slice.start();
        // Validate the pointer up front (mirrors realloc/dealloc).
        let block_start = match Self::block_start_of(start) {
            Ok(bs) => bs,
            Err(source) => {
                // SAFETY: the region is untouched and still owned by the caller.
                let handle = unsafe { BStackOwnedSlice::from_raw_parts(self, start, old_len) };
                return Err(BStackAllocError::with_handle(source, handle));
            }
        };
        if new_len == old_len {
            // `(0, 0)` on a non-empty handle: a no-op that returns the handle
            // exactly as it was, per the position guarantee.
            // SAFETY: unchanged region.
            return Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, start, old_len) });
        }

        // Every supported path below leaves the caller's original block intact on
        // failure, so the surviving handle is always `(start, old_len)`.
        let result = (|| -> io::Result<BStackOwnedSlice<'a, Self>> {
            let word = u64::from_le_bytes(read_bstack!(self.stack, block_start => u64));
            if word & Self::IN_USE_BIT == 0 {
                return Err(io_error!(InvalidInput, "cannot realloc a freed block"));
            }
            let old_size = (word & !Self::IN_USE_BIT) << 4;
            if Self::phys_need(old_len)? > old_size {
                return Err(io_error!(InvalidInput, "cannot realloc a mismatched slice"));
            }
            let new_need = Self::phys_need(new_len)?;
            if new_need > old_size {
                // Back grow past the recorded physical block. Extend the stack
                // tail in place when the block ends there — the position
                // guarantee holds, since the retained bytes never move. A block
                // that is *not* at the tail could only grow by merging a free
                // neighbour (the coalescer's job) or by relocating (which the
                // guarantee forbids), so it is `Unsupported`.
                let new_size = Self::class_blocksize(new_need);
                if self.grow_tail_inplace(block_start, start, old_len, old_size, new_size, true)? {
                    // SAFETY: block extended in place to the new class at the tail.
                    return Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, start, new_len) });
                }
                return Err(io_error!(
                    Unsupported,
                    "realloc_inplace: back grow would relocate a non-tail block"
                ));
            }
            self.resize_in_block(block_start, start, old_len, new_len, old_size, true)
        })();
        result.map_err(|source| BStackAllocError {
            source,
            // SAFETY: (start, old_len) still names the caller's live, intact block.
            handle: Some(unsafe { BStackOwnedSlice::from_raw_parts(self, start, old_len) }),
        })
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
    use crate::alloc::{BStackAllocator, BStackInPlaceResizeAllocator, BStackUninitAllocator};
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
    /// `Len` + `Atrunc` operation: the block is replaced by its shrunk self at
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

    // ── realloc_inplace (BStackInPlaceResizeAllocator) ───────────────────────

    // A back grow that still fits the recorded physical block succeeds in place,
    // never moving, and scrubs the re-exposed tail (a prior shrink left stale
    // data there) while keeping the surviving prefix.
    #[test]
    fn seg_realloc_inplace_grow_scrubs_stale_tail() {
        let (a, _g) = new_alloc();
        let mut s = a.alloc(100).unwrap(); // block 112
        s.write([0xCDu8; 100]).unwrap();
        let off = s.start();
        let s = a.realloc_inplace(s, 0, -10).unwrap(); // len 90, stale [90,100), no move
        assert_eq!(s.start(), off);
        assert_eq!(s.len(), 90);
        let s = a.realloc_inplace(s, 0, 10).unwrap(); // len 100, grow back within block
        assert_eq!(s.start(), off, "grow within recorded block never moves");
        let data = s.read().unwrap();
        assert_eq!(&data[..90], &[0xCDu8; 90], "prefix preserved");
        assert_eq!(&data[90..], &[0u8; 10], "re-exposed tail scrubbed");
    }

    // A back grow past the recorded block, when the block is at the stack tail,
    // extends the tail in place (same as `realloc`): the block stays at its
    // offset, the grown region is zeroed, and the stack lengthens.
    #[test]
    fn seg_realloc_inplace_tail_grow_past_block_extends() {
        let (a, _g) = new_alloc();
        let mut s = a.alloc(100).unwrap(); // block 112 (cap 104), at the tail
        s.write([7u8; 100]).unwrap();
        let off = s.start();
        let len_before = a.stack().len().unwrap();
        let s = a.realloc_inplace(s, 0, 100).unwrap(); // need 208 (class 12) > 112
        assert_eq!(s.start(), off, "tail grow extends in place, no move");
        assert_eq!(s.len(), 200);
        assert!(a.stack().len().unwrap() > len_before, "stack extended");
        let data = s.read().unwrap();
        assert_eq!(&data[..100], &[7u8; 100], "prefix preserved");
        assert_eq!(&data[100..], &[0u8; 100], "grown region zeroed");
        assert_eq!(unsafe { a.recover() }.unwrap(), 0, "arena accounted for");
    }

    // A back grow past the recorded block, when the block is NOT at the tail, is
    // `Unsupported` (a move would break the position guarantee). The handle comes
    // back intact and the stack is untouched.
    #[test]
    fn seg_realloc_inplace_interior_grow_past_block_is_unsupported() {
        let (a, _g) = new_alloc();
        let mut s = a.alloc(100).unwrap(); // block 112 (cap 104)
        s.write([7u8; 100]).unwrap();
        let off = s.start();
        let _pin = a.alloc(100).unwrap(); // pin the tail so `s` is interior
        let len_before = a.stack().len().unwrap();
        let err = a.realloc_inplace(s, 0, 100).unwrap_err(); // need 208 > 112, not at tail
        assert_eq!(err.source.kind(), std::io::ErrorKind::Unsupported);
        let s = err.handle.expect("handle returned intact");
        assert_eq!(s.start(), off);
        assert_eq!(s.len(), 100);
        assert_eq!(a.stack().len().unwrap(), len_before, "stack untouched");
        assert_eq!(s.read().unwrap(), vec![7u8; 100], "data intact");
    }

    // A grow that lands exactly on the recorded block capacity fits with no
    // metadata write; one byte past it, when the block is interior, is
    // `Unsupported`.
    #[test]
    fn seg_realloc_inplace_grow_to_block_capacity_fits() {
        let (a, _g) = new_alloc();
        let mut s = a.alloc(90).unwrap(); // block 112, cap 104
        s.write([1u8; 90]).unwrap();
        let off = s.start();
        let _pin = a.alloc(100).unwrap(); // pin the tail so `s` cannot tail-extend
        let s = a.realloc_inplace(s, 0, 14).unwrap(); // len 104 == cap, phys_need 112 ≤ 112
        assert_eq!(s.start(), off);
        assert_eq!(s.len(), 104);
        // One byte more must spill to the next class; interior → Unsupported.
        let err = a.realloc_inplace(s, 0, 1).unwrap_err();
        assert_eq!(err.source.kind(), std::io::ErrorKind::Unsupported);
    }

    // Any nonzero `prepend` is `Unsupported` (the front edge is anchored to the
    // block base); the handle is returned intact.
    #[test]
    fn seg_realloc_inplace_prepend_is_unsupported() {
        let (a, _g) = new_alloc();
        let mut s = a.alloc(100).unwrap();
        s.write([0x11u8; 100]).unwrap();
        let off = s.start();
        for (p, ap) in [(8i64, 0i64), (-8, 0), (8, -8), (-8, 8)] {
            let err = a.realloc_inplace(s, p, ap).unwrap_err();
            assert_eq!(
                err.source.kind(),
                std::io::ErrorKind::Unsupported,
                "prepend {p} append {ap}"
            );
            s = err.handle.expect("handle returned");
            assert_eq!(s.start(), off);
        }
        assert_eq!(s.read().unwrap(), vec![0x11u8; 100], "block untouched");
    }

    // An empty handle is `Unsupported` for every `(prepend, append)`, including
    // the `(0, 0)` no-op.
    #[test]
    fn seg_realloc_inplace_empty_handle_is_unsupported() {
        let (a, _g) = new_alloc();
        for (p, ap) in [(0i64, 0i64), (0, 8), (8, 0), (0, -8)] {
            let empty = crate::alloc::BStackOwnedSlice::empty(&a);
            let err = a.realloc_inplace(empty, p, ap).unwrap_err();
            assert_eq!(
                err.source.kind(),
                std::io::ErrorKind::Unsupported,
                "prepend {p} append {ap}"
            );
            assert!(err.handle.is_some());
        }
    }

    // `(0, 0)` on a non-empty handle is an identity no-op that returns the handle
    // exactly as it was.
    #[test]
    fn seg_realloc_inplace_identity_noop() {
        let (a, _g) = new_alloc();
        let mut s = a.alloc(64).unwrap();
        s.write([0x22u8; 64]).unwrap();
        let off = s.start();
        let s = a.realloc_inplace(s, 0, 0).unwrap();
        assert_eq!(s.start(), off);
        assert_eq!(s.len(), 64);
        assert_eq!(s.read().unwrap(), vec![0x22u8; 64]);
    }

    // A back shrink whose excess is below `SPLIT_MIN` retains the excess in place
    // (no move, no metadata write) in either build; the block keeps its physical
    // size so a later grow back fits in place.
    #[test]
    fn seg_realloc_inplace_small_shrink_retained() {
        let (a, _g) = new_alloc();
        let mut s = a.alloc(200).unwrap(); // block 208
        s.write([0x5Au8; 200]).unwrap();
        let off = s.start();
        let _pin = a.alloc(100).unwrap(); // pin the tail so `s` is interior
        let s = a.realloc_inplace(s, 0, -100).unwrap(); // len 100, excess 96 < SPLIT_MIN → retain
        assert_eq!(s.start(), off, "small shrink retains in place");
        assert_eq!(s.read().unwrap(), vec![0x5Au8; 100]);
        let s = a.realloc_inplace(s, 0, 100).unwrap(); // grow back within retained block
        assert_eq!(s.start(), off);
        let data = s.read().unwrap();
        assert_eq!(&data[..100], &[0x5Au8; 100]);
        assert_eq!(&data[100..], &[0u8; 100], "re-exposed tail scrubbed");
        assert_eq!(unsafe { a.recover() }.unwrap(), 0, "arena accounted for");
    }

    // With `atomic`, a back shrink whose excess clears `SPLIT_MIN` at the tail is
    // reclaimed in one crash-atomic operation: the block stays at its offset and
    // the excess returns to the stack.
    #[cfg(feature = "atomic")]
    #[test]
    fn seg_realloc_inplace_tail_shrink_reclaims() {
        let (a, _g) = new_alloc();
        let mut s = a.alloc(500).unwrap(); // block 512, at the tail
        s.write([0x3Cu8; 500]).unwrap();
        let off = s.start();
        let len_before = a.stack().len().unwrap();
        let s = a.realloc_inplace(s, 0, -400).unwrap(); // len 100, excess 400 ≥ SPLIT_MIN, tail
        assert_eq!(s.start(), off, "tail shrink stays in place");
        assert!(
            a.stack().len().unwrap() < len_before,
            "excess discarded from tail"
        );
        assert_eq!(s.read().unwrap(), vec![0x3Cu8; 100]);
        assert_eq!(unsafe { a.recover() }.unwrap(), 0, "arena accounted for");
    }

    // Shrinking to zero frees the block and yields an empty handle.
    #[test]
    fn seg_realloc_inplace_shrink_to_zero_frees() {
        let (a, _g) = new_alloc();
        let s = a.alloc(64).unwrap();
        let empty = a.realloc_inplace(s, 0, -64).unwrap();
        assert!(empty.is_empty());
        // The freed block is reusable and the arena stays accounted for.
        assert_eq!(unsafe { a.recover() }.unwrap(), 0);
    }

    // A malformed pointer is rejected with `InvalidInput` and the handle returned,
    // even though it never reaches the resize logic.
    #[test]
    fn seg_realloc_inplace_rejects_malformed_pointer() {
        let (a, _g) = new_alloc();
        let bad = unsafe {
            crate::alloc::BStackOwnedSlice::from_raw_parts(
                &a,
                Seg::ARENA_START + Seg::OVERHEAD + 1,
                16,
            )
        };
        let err = a.realloc_inplace(bad, 0, 8).unwrap_err();
        assert_eq!(err.source.kind(), std::io::ErrorKind::InvalidInput);
        assert!(err.handle.is_some());
    }

    // A resulting length that underflows below zero is `InvalidInput`, handle kept.
    #[test]
    fn seg_realloc_inplace_negative_result_is_invalid() {
        let (a, _g) = new_alloc();
        let s = a.alloc(64).unwrap();
        let err = a.realloc_inplace(s, 0, -65).unwrap_err(); // 64 - 65 < 0
        assert_eq!(err.source.kind(), std::io::ErrorKind::InvalidInput);
        assert!(err.handle.is_some());
    }

    // A handle from another allocator instance is rejected before any logic runs.
    #[test]
    fn seg_realloc_inplace_rejects_foreign_handle() {
        let (a, _ga) = new_alloc();
        let (b, _gb) = new_alloc();
        let s = a.alloc(64).unwrap();
        let err = b.realloc_inplace(s, 0, 8).unwrap_err();
        assert_eq!(err.source.kind(), std::io::ErrorKind::InvalidInput);
        assert!(err.handle.is_some());
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

// ── Bulk allocation (feature = "atomic") ──────────────────────────────────
#[cfg(all(test, feature = "set", feature = "atomic"))]
mod bulk_tests {
    use super::SegregatedBStackAllocator as Seg;
    use crate::BStack;
    use crate::alloc::{BStackAllocator, BStackBulkAllocator};
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
        std::env::temp_dir().join(format!("bstack_seg_bulk_{pid}_{id}.bin"))
    }
    fn new_alloc() -> (Seg, Guard) {
        let path = temp_path();
        let g = Guard(path.clone());
        (Seg::new(BStack::open(&path).unwrap()).unwrap(), g)
    }

    #[test]
    fn alloc_bulk_empty_returns_empty() {
        let (a, _g) = new_alloc();
        assert!(a.alloc_bulk([]).unwrap().is_empty());
    }

    #[test]
    fn alloc_bulk_distinct_usable_regions_of_requested_len() {
        let (a, _g) = new_alloc();
        let mut slices = a.alloc_bulk([24u64, 100, 500, 24]).unwrap();
        assert_eq!(
            slices.iter().map(|s| s.len()).collect::<Vec<_>>(),
            [24, 100, 500, 24]
        );
        for (i, s) in slices.iter_mut().enumerate() {
            let v = vec![i as u8 + 1; s.len() as usize];
            s.write(v).unwrap();
        }
        for (i, s) in slices.iter().enumerate() {
            assert_eq!(s.read().unwrap(), vec![i as u8 + 1; s.len() as usize]);
        }
        // Distinct block starts.
        let mut starts: Vec<u64> = slices.iter().map(|s| s.start()).collect();
        starts.sort_unstable();
        starts.dedup();
        assert_eq!(starts.len(), 4);
    }

    #[test]
    fn alloc_bulk_zero_length_entries_are_null_slices() {
        let (a, _g) = new_alloc();
        let slices = a.alloc_bulk([0u64, 100, 0]).unwrap();
        assert!(slices[0].is_empty());
        assert!(!slices[1].is_empty());
        assert!(slices[2].is_empty());
    }

    #[test]
    fn alloc_bulk_reuses_freed_blocks_per_class() {
        let (a, _g) = new_alloc();
        // Two blocks in the same class (24 and 30 both → 32-byte block).
        let first = a.alloc_bulk([24u64, 30]).unwrap();
        let mut freed: Vec<u64> = first.iter().map(|s| s.start()).collect();
        freed.sort_unstable();
        a.dealloc_bulk(first).unwrap();
        // Same class again: must reuse the freed blocks.
        let second = a.alloc_bulk([24u64, 30]).unwrap();
        let mut reused: Vec<u64> = second.iter().map(|s| s.start()).collect();
        reused.sort_unstable();
        assert_eq!(reused, freed, "same-class blocks must be recycled");
    }

    #[test]
    fn alloc_bulk_recycled_blocks_read_back_zero() {
        let (a, _g) = new_alloc();
        let mut first = a.alloc_bulk([100u64, 100, 100]).unwrap();
        for s in &mut first {
            s.write([0x5Au8; 100]).unwrap();
        }
        a.dealloc_bulk(first).unwrap();
        let second = a.alloc_bulk([100u64, 100, 100]).unwrap();
        for s in &second {
            assert_eq!(
                s.read().unwrap(),
                vec![0u8; 100],
                "recycled block not scrubbed"
            );
        }
    }

    #[test]
    fn alloc_bulk_oversized_requests_are_usable_and_freeable() {
        let (a, _g) = new_alloc();
        // > MAX_CLASS (4096) → oversized bucket.
        let mut slices = a.alloc_bulk([5000u64, 100, 8000]).unwrap();
        assert_eq!(slices[0].len(), 5000);
        assert_eq!(slices[2].len(), 8000);
        slices[0].write([0xABu8; 5000]).unwrap();
        assert_eq!(slices[0].read().unwrap(), vec![0xABu8; 5000]);
        a.dealloc_bulk(slices).unwrap();
        assert_eq!(unsafe { a.recover() }.unwrap(), 0, "arena fully accounted");
    }

    #[test]
    fn dealloc_bulk_frees_and_recover_reports_clean() {
        let (a, _g) = new_alloc();
        let slices = a.alloc_bulk([24u64, 100, 500, 5000]).unwrap();
        a.dealloc_bulk(slices).unwrap();
        assert_eq!(unsafe { a.recover() }.unwrap(), 0, "no blocks leaked");
    }

    #[test]
    fn dealloc_bulk_empty_and_null_handles_are_noops() {
        let (a, _g) = new_alloc();
        a.dealloc_bulk([]).unwrap();
        let z = a.alloc(0).unwrap();
        a.dealloc_bulk([z]).unwrap();
    }

    #[test]
    fn dealloc_bulk_rejects_double_free_and_returns_all_handles() {
        let (a, _g) = new_alloc();
        let x = a.alloc(100).unwrap();
        let y = a.alloc(100).unwrap();
        let x_start = x.start();
        a.dealloc(x).unwrap();
        let stale = unsafe { crate::alloc::BStackOwnedSlice::from_raw_parts(&a, x_start, 100) };
        let err = a
            .dealloc_bulk([y, stale])
            .expect_err("double free must be rejected");
        assert_eq!(err.source.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(err.handles.len(), 2);
    }

    #[test]
    fn dealloc_bulk_rejects_a_batch_with_a_foreign_handle() {
        let (a1, _g1) = new_alloc();
        let (a2, _g2) = new_alloc();
        let own = a2.alloc(100).unwrap();
        let foreign = a1.alloc(100).unwrap();
        let err = a2
            .dealloc_bulk([own, foreign])
            .expect_err("a2 must refuse a1's handle");
        assert_eq!(err.source.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(err.handles.len(), 2);
    }

    #[test]
    fn bulk_round_trips_survive_reopen() {
        let path = temp_path();
        let _g = Guard(path.clone());
        let offsets: Vec<(u64, u64)>;
        {
            let a = Seg::new(BStack::open(&path).unwrap()).unwrap();
            let mut slices = a.alloc_bulk([50u64, 300, 5000]).unwrap();
            offsets = slices.iter().map(|s| (s.start(), s.len())).collect();
            for (i, s) in slices.iter_mut().enumerate() {
                let v = vec![i as u8 + 7; s.len() as usize];
                s.write(v).unwrap();
            }
        }
        let a2 = Seg::new(BStack::open(&path).unwrap()).unwrap();
        for (i, &(off, len)) in offsets.iter().enumerate() {
            let v = unsafe { crate::alloc::BStackSlice::from_raw_parts(a2.stack(), off, len) };
            assert_eq!(v.read().unwrap(), vec![i as u8 + 7; len as usize]);
        }
    }

    #[test]
    fn concurrent_alloc_bulk_dealloc_bulk_stay_consistent() {
        use std::collections::HashSet;
        use std::sync::{Arc, Mutex};
        use std::thread;

        const THREADS: usize = 8;
        const ROUNDS: usize = 80;
        const SIZES: [u64; 4] = [24, 100, 500, 5000];

        let path = temp_path();
        let _g = Guard(path.clone());
        let alloc = Arc::new(Seg::new(BStack::open(&path).unwrap()).unwrap());
        let live: Arc<Mutex<HashSet<u64>>> = Arc::new(Mutex::new(HashSet::new()));

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let alloc = Arc::clone(&alloc);
                let live = Arc::clone(&live);
                thread::spawn(move || {
                    let a: &Seg = &alloc;
                    for _ in 0..ROUNDS {
                        let mut slices = a.alloc_bulk(SIZES).unwrap();
                        {
                            let mut set = live.lock().unwrap();
                            for s in &slices {
                                assert!(
                                    set.insert(s.start()),
                                    "duplicate live offset {}",
                                    s.start()
                                );
                            }
                        }
                        for s in &mut slices {
                            let v = vec![0xC3u8; s.len() as usize];
                            s.write(v).unwrap();
                        }
                        for s in &slices {
                            assert_eq!(s.read().unwrap(), vec![0xC3u8; s.len() as usize]);
                        }
                        {
                            let mut set = live.lock().unwrap();
                            for s in &slices {
                                set.remove(&s.start());
                            }
                        }
                        a.dealloc_bulk(slices).unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(unsafe { alloc.recover() }.unwrap(), 0);
    }

    #[test]
    fn alloc_bulk_reuses_oversized_blocks_via_matching() {
        let (a, _g) = new_alloc();
        // Three oversized blocks of distinct sizes on the free list.
        let first = a.alloc_bulk([5000u64, 6000, 7000]).unwrap();
        let mut freed: Vec<u64> = first.iter().map(|s| s.start()).collect();
        freed.sort_unstable();
        a.dealloc_bulk(first).unwrap();
        // The same three requests must be satisfied entirely by matching.
        let second = a.alloc_bulk([5000u64, 6000, 7000]).unwrap();
        let mut reused: Vec<u64> = second.iter().map(|s| s.start()).collect();
        reused.sort_unstable();
        assert_eq!(
            reused, freed,
            "oversized requests must reuse the freed blocks"
        );
    }

    #[test]
    fn alloc_bulk_oversized_block_goes_to_largest_fitting_request() {
        let (a, _g) = new_alloc();
        // One free oversized block big enough for both requests. Pin it away from
        // the tail so `dealloc` routes it to the free list instead of discarding.
        let big = a.alloc(8000).unwrap();
        let big_start = big.start();
        let _pin = a.alloc(100).unwrap();
        a.dealloc(big).unwrap();
        // Requests [5000, 6000] (both oversized): the single block fits both but is
        // assigned to the larger one (6000); the 5000 request falls to a fresh extend.
        let slices = a.alloc_bulk([5000u64, 6000]).unwrap();
        assert_eq!(
            slices[1].start(),
            big_start,
            "the block must go to the largest fitting request"
        );
        assert_ne!(slices[0].start(), big_start);
        // The retained-whole block still frees cleanly and leaves no leak.
        a.dealloc_bulk(slices).unwrap();
        assert_eq!(unsafe { a.recover() }.unwrap(), 0);
    }

    #[test]
    fn alloc_bulk_carves_oversized_block_when_excess_reaches_split_min() {
        let (a, _g) = new_alloc();
        // A large free oversized block (physical size 9008), pinned off the tail so
        // `dealloc` routes it to the free list instead of discarding.
        let big = a.alloc(9000).unwrap();
        let big_start = big.start() - 8; // block_start (OVERHEAD == 8)
        let big_end = big_start + 9008;
        let _pin = a.alloc(100).unwrap();
        a.dealloc(big).unwrap();

        // One oversized request far smaller than the free block (block 4112, excess
        // 4896 ≥ SPLIT_MIN): the block is carved, not retained whole.
        let mut slices = a.alloc_bulk([4100u64]).unwrap();
        assert_eq!(slices[0].len(), 4100);
        assert_eq!(
            slices[0].start() - 8,
            big_start,
            "the carved request keeps the block start"
        );
        slices[0].write(&[0xABu8; 4100]).unwrap();
        assert_eq!(slices[0].read().unwrap(), vec![0xABu8; 4100]);

        // The carved-off remainder is a reusable free block inside the original
        // block — proof the excess was split off, not wasted with the whole block.
        let rem = a.alloc(4090).unwrap(); // block 4112 (> MAX_CLASS) fits the remainder
        let rem_start = rem.start() - 8;
        assert!(
            rem_start >= big_start + 4112 && rem_start < big_end,
            "the remainder must be reused from the carve (got {rem_start})"
        );

        a.dealloc(rem).unwrap();
        a.dealloc_bulk(slices).unwrap();
        assert_eq!(unsafe { a.recover() }.unwrap(), 0);
    }
}

// Bulk-allocation fault-injection test (`atomic`): a failed `extend` on the
// alloc_bulk path returns the blocks already popped from the free lists rather
// than leaking them.
#[cfg(all(
    test,
    debug_assertions,
    feature = "fault-injection",
    feature = "set",
    feature = "atomic"
))]
mod bulk_fault_tests {
    use super::SegregatedBStackAllocator as Seg;
    use crate::BStack;
    use crate::alloc::{BStackAllocator, BStackBulkAllocator};
    use crate::alloc_fuzz::common::{Guard, policies::FailOpAt, temp_path};
    use crate::fault::FaultPolicy;
    use std::io::ErrorKind;
    use std::sync::Arc;

    #[test]
    fn alloc_bulk_extend_fault_reclaims_popped_blocks() {
        let path = temp_path("seg_bulk_extend");
        let _g = Guard(path.clone());
        let alloc = Seg::new(BStack::open(&path).unwrap()).unwrap();

        // Seed one class (len 24 → 32-byte block) with two free blocks.
        let seed = alloc.alloc_bulk([24u64, 24]).unwrap();
        let mut freed: Vec<u64> = seed.iter().map(|s| s.start()).collect();
        freed.sort_unstable();
        alloc.dealloc_bulk(seed).unwrap();

        // Three same-class requests: two are served from the free list, the third
        // forces an extend; fault it (bulk grows via `extend_sparse_batched`, which
        // writes each fresh block's free overhead).
        let policy: Arc<dyn FaultPolicy> =
            Arc::new(FailOpAt::new("extend_sparse_batched", 0, ErrorKind::Other));
        alloc.stack().set_fault_policy(Some(policy));
        let err = alloc
            .alloc_bulk([24u64, 24, 24])
            .expect_err("extend fault must fail alloc_bulk");
        alloc.stack().set_fault_policy(None);
        assert_eq!(err.kind(), ErrorKind::Other);

        // The two popped blocks were returned to the class free list.
        let r1 = alloc.alloc(24).unwrap();
        let r2 = alloc.alloc(24).unwrap();
        let mut reused = [r1.start(), r2.start()];
        reused.sort_unstable();
        assert_eq!(reused, freed[..], "popped blocks were not reclaimed");
    }
}
