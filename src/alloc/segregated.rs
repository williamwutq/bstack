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
//! This is the initial *core* implementation: `new`/`open`, `alloc`, and
//! `dealloc`. `realloc` and the background coalescer are not yet implemented,
//! and `recover` is a stub (see the method docs).
//!
//! # Feature flags
//!
//! Requires `set` + `atomic` (the type is `Send + Sync`). The non-`atomic`
//! degraded path is deferred to a later pass.

use super::{BStackAllocError, BStackAllocator, BStackOwnedSlice};
use crate::{BStack, BStackGenOp};
use std::sync::Mutex;
use std::{fmt, io};

/// Magic: `ALSG` + major 0 + minor 1; the version encodes the fixed class scheme.
const ALSG_MAGIC: [u8; 8] = *b"ALSG\x00\x01\x00\x00";
/// Compatibility prefix checked on open (`ALSG` + major 0 + minor 1).
const ALSG_MAGIC_PREFIX: [u8; 6] = *b"ALSG\x00\x01";

/// A segregated free-list allocator implementing [`BStackAllocator`] on top of a
/// [`BStack`].
///
/// # On-disk layout
///
/// ```text
/// offset  0  reserved (user)                24 B
/// offset 24  magic  "ALSG\x00\x01\x00\x00"   8 B
/// offset 32  flags                           4 B   # bit0 = recovery_needed
/// offset 36  _reserved                       4 B
/// offset 40  free_head[NUM_CLASSES] : u64          # last entry = oversized list
///            (pad to 32-B alignment)
/// arena start (32-B aligned)
/// ```
///
/// Every arena block is `[ overhead(8) | data(block − 8) ]`; the caller pointer
/// is the data start (`block_start + 8`). The overhead is a single tagged word:
/// high bit set ⇒ in use, low 63 bits = the caller's exact slice length; high
/// bit clear ⇒ free, low 63 bits = physical block size `>> 4` (which doubles as
/// the class tag). A free block stores its `next_free` offset inline at the data
/// start, so live allocations carry no space overhead beyond the 8-byte word.
///
/// # Thread safety
///
/// Always `Send + Sync`: `alloc`/`dealloc` drive [`BStack::process_gen`] /
/// [`BStack::inplace_gen`] sequences that hold `BStack`'s write lock across the
/// dependent read/modify/write, so no allocator-level lock is taken. The
/// internal [`Mutex`] serialises [`recover`](Self::recover) against itself only.
#[cfg(all(feature = "set", feature = "atomic"))]
pub struct SegregatedBStackAllocator {
    stack: BStack,
    /// Serialises [`recover`](Self::recover) against itself; ordinary
    /// alloc/dealloc never take it.
    lock: Mutex<()>,
}

#[cfg(all(feature = "set", feature = "atomic"))]
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
    /// Offset of the flags word (bit0 = recovery_needed). Written by the
    /// multi-transaction paths (realloc, coalescer) added in a later pass.
    #[allow(dead_code)]
    const FLAGS_OFFSET: u64 = 32;
    /// Offset of `free_head[0]`.
    const FREE_HEAD_BASE: u64 = 40;
    /// Payload offset of the first arena block: header rounded up to 32 B.
    /// `40 + 33*8 = 304 → 320`.
    const ARENA_START: u64 = (Self::FREE_HEAD_BASE + Self::NUM_CLASSES * 8 + 31) & !31;

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

    /// Payload offset of the free-list head for `class`.
    #[inline]
    fn head_off(class: u64) -> u64 {
        Self::FREE_HEAD_BASE + class * core::mem::size_of::<u64>() as u64
    }

    /// Initialise a new allocator over an empty `stack`, writing the header.
    ///
    /// # Errors
    ///
    /// * [`io::ErrorKind::InvalidInput`] — `stack` is not empty (use
    ///   [`open`](Self::open) to reopen an existing file).
    /// * Any [`io::Error`] from the underlying [`BStack`] operations.
    pub fn new(stack: BStack) -> io::Result<Self> {
        if !stack.is_empty()? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "stack is not empty; use SegregatedBStackAllocator::open to reopen",
            ));
        }
        const OFFSET_OFFSET: usize = SegregatedBStackAllocator::OFFSET_SIZE as usize;
        let mut hdr = [0u8; OFFSET_OFFSET + 8];
        hdr[OFFSET_OFFSET..].copy_from_slice(&ALSG_MAGIC);
        // flags, reserved, and every free_head remain 0.
        let _ = stack.extend_sparse(&hdr, Self::ARENA_START)?;
        Ok(Self {
            stack,
            lock: Mutex::new(()),
        })
    }

    /// Open an existing allocator from a non-empty `stack`, validating the magic
    /// and arena alignment and running [`recover`](Self::recover).
    ///
    /// # Errors
    ///
    /// * [`io::ErrorKind::InvalidInput`] — `stack` is empty (use
    ///   [`new`](Self::new)).
    /// * [`io::ErrorKind::InvalidData`] — wrong magic or a misaligned arena.
    /// * Any [`io::Error`] from the underlying [`BStack`] operations.
    pub fn open(stack: BStack) -> io::Result<Self> {
        if stack.is_empty()? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "stack is empty; use SegregatedBStackAllocator::new to create",
            ));
        }
        let stack_len = stack.len()?;
        if stack_len < Self::ARENA_START {
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
        // Every block is a multiple of QUANTUM, so the arena byte count is too.
        if (stack_len - Self::ARENA_START) % Self::QUANTUM != 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "arena is not a multiple of the block quantum",
            ));
        }
        let allocator = Self {
            stack,
            lock: Mutex::new(()),
        };
        allocator.recover()?;
        Ok(allocator)
    }

    /// Reclaim blocks leaked by an unclean shutdown and return the count that
    /// could not be classified with certainty (`0` = fully accounted for).
    ///
    /// **Core-pass stub:** the linear arena scan is not yet implemented, so this
    /// always returns `0`. The single-flight lock is taken so the contract (and
    /// the `open` call site) is already wired.
    pub fn recover(&self) -> io::Result<u64> {
        let _guard = self.lock.lock().unwrap();
        // TODO: linear arena scan — stride by classify_blocksize(len) at live
        // blocks and by the stored size at free blocks, relinking leaks.
        Ok(0)
    }

    /// Pop the head of `class`, returning its block-start offset or `None`.
    ///
    /// Drives one [`BStack::process_gen`] holding the write lock across
    /// read-head → read-next → advance-head, closing the ABA window. The claim
    /// (overhead flip + data scrub) is a separate write on the now-detached
    /// block; a crash between leaks ≤ 1 block, reclaimed by `recover`.
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
                    buf: unsafe { core::mem::transmute::<&mut [u8], &mut [u8]>(&mut head_buf[..]) },
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
                            buf: unsafe {
                                core::mem::transmute::<&mut [u8], &mut [u8]>(&mut next_buf[..])
                            },
                        })
                    }
                }
                2 => Some(BStackGenOp::Write {
                    offset: head_off,
                    // SAFETY: `next_buf` outlives this `process_gen` call.
                    data: unsafe { core::mem::transmute::<&[u8], &[u8]>(&next_buf[..]) },
                }),
                _ => None,
            };
            step += 1;
            op
        })?;
        Ok(popped)
    }

    /// Pop the oversized head **only if** its stored physical size equals `need`
    /// exactly, preserving the `physical == classify_blocksize(len)` invariant.
    /// A non-exact head is left in place (the caller extends a fresh block).
    fn pop_oversized(&self, need: u64) -> io::Result<Option<u64>> {
        let head_off = Self::head_off(Self::OVERSIZED_CLASS);
        let mut head_buf = [0u8; 8];
        let mut oh_buf = [0u8; 8];
        let mut next_buf = [0u8; 8];
        let mut step = 0usize;
        let mut head = 0u64;
        let mut popped: Option<u64> = None;
        self.stack.process_gen(|| {
            let op = match step {
                0 => Some(BStackGenOp::Read {
                    offset: head_off,
                    // SAFETY: `head_buf` outlives this call.
                    buf: unsafe { core::mem::transmute::<&mut [u8], &mut [u8]>(&mut head_buf[..]) },
                }),
                1 => {
                    head = u64::from_le_bytes(head_buf);
                    if head == Self::SENTINEL {
                        None
                    } else {
                        Some(BStackGenOp::Read {
                            offset: head,
                            // SAFETY: `oh_buf` outlives this call.
                            buf: unsafe {
                                core::mem::transmute::<&mut [u8], &mut [u8]>(&mut oh_buf[..])
                            },
                        })
                    }
                }
                2 => {
                    let word = u64::from_le_bytes(oh_buf);
                    // Free head must have the high bit clear; its size is word << 4.
                    if word & Self::IN_USE_BIT != 0 || (word << 4) != need {
                        None
                    } else {
                        Some(BStackGenOp::Read {
                            offset: head + Self::OVERHEAD,
                            // SAFETY: `next_buf` outlives this call.
                            buf: unsafe {
                                core::mem::transmute::<&mut [u8], &mut [u8]>(&mut next_buf[..])
                            },
                        })
                    }
                }
                3 => {
                    popped = Some(head);
                    Some(BStackGenOp::Write {
                        offset: head_off,
                        // SAFETY: `next_buf` outlives this call.
                        data: unsafe { core::mem::transmute::<&[u8], &[u8]>(&next_buf[..]) },
                    })
                }
                _ => None,
            };
            step += 1;
            op
        })?;
        Ok(popped)
    }

    /// Scrub a freshly popped block: write `in_use | len` overhead and zero the
    /// rest of the data (which still holds the free block's stale bytes). One
    /// `set` of `block` bytes.
    fn claim(&self, block_start: u64, block: u64, len: u64) -> io::Result<()> {
        let mut buf = vec![0u8; block as usize];
        buf[..8].copy_from_slice(&(Self::IN_USE_BIT | len).to_le_bytes());
        self.stack.set(block_start, buf)
    }

    /// Push `block_start` (physical size `size`, head index `class`) onto its
    /// free list, bundling the overhead flip, `next_free`, and head-slot write
    /// into one crash-atomic [`BStack::inplace_gen`] transaction.
    fn push(&self, block_start: u64, size: u64, class: u64) -> io::Result<()> {
        let head_off = Self::head_off(class);
        let free_word = (size >> 4).to_le_bytes(); // free tag: high bit clear
        let start_bytes = block_start.to_le_bytes();
        let mut head_buf = [0u8; 8];
        let mut step = 0u32;
        self.stack.inplace_gen(|_res| {
            let op = match step {
                // Read the current head (no writes staged yet ⇒ committed value).
                0 => Some(BStackGenOp::Read {
                    offset: head_off,
                    // SAFETY: `head_buf` outlives this call.
                    buf: unsafe { core::mem::transmute::<&mut [u8], &mut [u8]>(&mut head_buf[..]) },
                }),
                // overhead ← free | size
                1 => Some(BStackGenOp::Write {
                    offset: block_start,
                    // SAFETY: `free_word` outlives this call.
                    data: unsafe { core::mem::transmute::<&[u8], &[u8]>(&free_word[..]) },
                }),
                // next_free ← old head
                2 => Some(BStackGenOp::Write {
                    offset: block_start + Self::OVERHEAD,
                    // SAFETY: `head_buf` outlives this call and is not mutated
                    // after step 0's read resolved.
                    data: unsafe { core::mem::transmute::<&[u8], &[u8]>(&head_buf[..]) },
                }),
                // head[class] ← block_start
                3 => Some(BStackGenOp::Write {
                    offset: head_off,
                    // SAFETY: `start_bytes` outlives this call.
                    data: unsafe { core::mem::transmute::<&[u8], &[u8]>(&start_bytes[..]) },
                }),
                _ => None,
            };
            step += 1;
            op
        })
    }
}

#[cfg(all(feature = "set", feature = "atomic"))]
impl fmt::Debug for SegregatedBStackAllocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SegregatedBStackAllocator")
            .field("num_classes", &Self::NUM_CLASSES)
            .finish_non_exhaustive()
    }
}

#[cfg(all(feature = "set", feature = "atomic"))]
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
    fn alloc(&self, len: u64) -> io::Result<BStackOwnedSlice<'_, Self>> {
        if len == 0 {
            return Ok(BStackOwnedSlice::empty(self));
        }
        let need = Self::phys_need(len)?;
        let block = Self::class_blocksize(need);

        if block <= Self::MAX_CLASS {
            let class = Self::classify(block);
            if let Some(bs) = self.pop_class(class)? {
                self.claim(bs, block, len)?;
                // SAFETY: `bs` is a freshly claimed class-`block` region.
                return Ok(unsafe {
                    BStackOwnedSlice::from_raw_parts(self, bs + Self::OVERHEAD, len)
                });
            }
        } else if let Some(bs) = self.pop_oversized(block)? {
            self.claim(bs, block, len)?;
            // SAFETY: `bs` is an exact-size oversized block from the free list.
            return Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, bs + Self::OVERHEAD, len) });
        }

        // Miss: extend a fresh block. `extend` zero-fills, so only the overhead
        // word needs writing.
        let bs = self.stack.extend(block)?;
        self.stack.set(bs, (Self::IN_USE_BIT | len).to_le_bytes())?;
        // SAFETY: `bs` is a fresh tail extension of exactly `block` bytes.
        Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, bs + Self::OVERHEAD, len) })
    }

    /// `realloc` is not yet implemented in the core pass; returns
    /// [`io::ErrorKind::Unsupported`] with the original handle intact.
    fn realloc<'a>(
        &'a self,
        slice: BStackOwnedSlice<'a, Self>,
        _new_len: u64,
    ) -> Result<BStackOwnedSlice<'a, Self>, BStackAllocError<'a, Self>> {
        let (start, len) = (slice.start(), slice.len());
        Err(BStackAllocError::with_handle(
            io::Error::new(
                io::ErrorKind::Unsupported,
                "realloc not yet implemented for SegregatedBStackAllocator",
            ),
            // SAFETY: the region is untouched and still owned by the caller.
            unsafe { BStackOwnedSlice::from_raw_parts(self, start, len) },
        ))
    }

    /// Release the region described by `slice`.
    ///
    /// Reads the overhead at `slice.start() − 8`: a clear high bit is a
    /// double-free; a stored length that disagrees with `slice.len()` is a
    /// partial/erroneous free. Otherwise an oversized block at the tail is
    /// discarded in one call, and every other block is spliced onto its class
    /// head via one crash-atomic [`BStack::inplace_gen`] transaction.
    fn dealloc<'a>(
        &'a self,
        slice: BStackOwnedSlice<'a, Self>,
    ) -> Result<(), BStackAllocError<'a, Self>> {
        let start = slice.start();
        let len = slice.len();
        if slice.is_empty() {
            return Ok(());
        }
        // Set once a free-list splice is attempted: a torn/failed atomic push
        // must not hand back a handle that could double-free.
        let mut lost = false;
        let result = (|| -> io::Result<()> {
            // `start` is the data pointer (block_start + OVERHEAD); a valid
            // block_start is >= ARENA_START and QUANTUM-aligned (ARENA_START is
            // itself 16-aligned and every block is a multiple of 16). Reject a
            // header-range, underflowing, or mid-block pointer — otherwise a
            // crafted `start` would let dealloc reinterpret interior bytes as an
            // overhead word.
            let block_start = start
                .checked_sub(Self::OVERHEAD)
                .filter(|bs| *bs >= Self::ARENA_START && *bs % Self::QUANTUM == 0)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "slice start is not a valid block pointer",
                    )
                })?;
            let word = u64::from_le_bytes(read_bstack!(self.stack, block_start => u64));
            if word & Self::IN_USE_BIT == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "double free: block is already free",
                ));
            }
            if word & !Self::IN_USE_BIT != len {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot free a partial or mismatched slice",
                ));
            }
            let size = Self::class_blocksize(Self::phys_need(len)?);
            let class = Self::classify(size);

            if size > Self::MAX_CLASS {
                let end = block_start.checked_add(size).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "block end overflows u64")
                })?;
                if self.stack.try_discard(end, size)? {
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

#[cfg(all(test, feature = "set", feature = "atomic"))]
mod _assertions {
    use super::SegregatedBStackAllocator;
    fn _send()
    where
        SegregatedBStackAllocator: Send,
    {
    }
    fn _sync()
    where
        SegregatedBStackAllocator: Sync,
    {
    }
}

#[cfg(all(test, feature = "set", feature = "atomic"))]
mod tests {
    use super::SegregatedBStackAllocator as Seg;
    use crate::BStack;
    use crate::alloc::BStackAllocator;
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

    // ── classification math ──────────────────────────────────────────────────

    #[test]
    fn class_scheme_constants() {
        assert_eq!(Seg::LINEAR_CLASSES, 16);
        assert_eq!(Seg::GEO_CLASSES, 16);
        assert_eq!(Seg::NUM_CLASSES, 33);
        assert_eq!(Seg::OVERSIZED_CLASS, 32);
        assert_eq!(Seg::ARENA_START, 320);
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
        let a = Seg::open(BStack::open(&path).unwrap()).unwrap();
        let s = unsafe { crate::alloc::BStackSlice::from_raw_parts(a.stack(), off, 28) };
        assert_eq!(s.read().unwrap(), b"segregated allocator payload");
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
            assert!(err.handle.is_some(), "malformed free must return the handle");
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
}
