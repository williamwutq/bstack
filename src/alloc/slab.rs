//! Fixed-block slab allocator for [`BStack`]-backed storage.
//!
//! Provides [`SlabBStackAllocator`], which implements [`BStackAllocator`] with
//! O(1) alloc and dealloc by keeping all blocks the same size and tracking
//! freed blocks in an intrusive singly-linked free list.

use super::{BStackAllocator, BStackOwnedSlice};
use crate::BStack;
#[cfg(feature = "atomic")]
use crate::BStackGenOp;
#[cfg(not(feature = "atomic"))]
use core::cell::Cell;
#[cfg(not(feature = "atomic"))]
use core::marker::PhantomData;
use core::num::NonZeroU64;
use std::{fmt, io};

#[cfg(feature = "set")]
const ALSL_MAGIC: [u8; 8] = *b"ALSL\x00\x01\x01\x00";

/// Compatibility prefix checked on open: `ALSL` + major 0 + minor 1.
/// Any file whose first 6 bytes match is considered compatible.
#[cfg(feature = "set")]
const ALSL_MAGIC_PREFIX: [u8; 6] = *b"ALSL\x00\x01";

/// A fixed-block slab allocator implementing [`BStackAllocator`] on top of a
/// [`BStack`].
///
/// All blocks in the arena are exactly `block_size` bytes with **no** per-block
/// header or footer. When a block is free its first 8 bytes hold the payload
/// offset of the next free block (little-endian `u64`, sentinel `0`); when
/// live those bytes belong entirely to the caller.
///
/// # On-disk layout
///
/// ```text
/// [ reserved(24) | magic[8] | block_size[8] | free_head[8] | arena ... ]
///   ^               ^
///   offset 0        offset 24 (allocator header start)
///   user data       offset 48 (arena start)
/// ```
///
/// # Allocation policy
///
/// * `len == 0` — returns a zero-length sentinel slice (`offset = 0, len = 0`).
/// * `len <= block_size` — pops from the free list if available; otherwise
///   extends the stack tail by exactly `block_size` bytes.
/// * `len > block_size` — always extends the tail by
///   `len.div_ceil(block_size) * block_size` bytes.
///
/// # Deallocation policy
///
/// * Oversized block at the tail — `BStack::discard` (single call, crash-safe
///   by inheritance).
/// * All other cases — each `block_size` chunk is prepended to the free list.
///
/// # Crash consistency
///
/// Operations that touch `free_head` consist of two `BStack` calls. In both
/// `push_free_block` and `pop_free_block` the block payload is written before
/// the header is updated, so a crash between the two calls at worst leaks the
/// block being operated on; the rest of the free list remains consistent and
/// the file can be used without recovery.
///
/// With the `atomic` feature, free-list push and pop instead use
/// [`BStack::cross_exchange`] and [`BStack::process_gen`] (see
/// [Thread safety](#thread-safety)); the call counts in the table below are
/// for the non-`atomic` implementation, but the atomicity class of each
/// operation is unchanged or improved.
///
/// # Method safety
///
/// | Method                               | Atomicity       | `BStack` op      | Crash effect.             |
/// |--------------------------------------|-----------------|------------------|---------------------------|
/// | `new`                                | Atomic          | Yes (`push`)     | —                         |
/// | `open`                               | N/A (read-only) | Yes (`get_into`) | —                         |
/// | `block_size`                         | N/A (no I/O)    | —                | —                         |
/// | `into_stack`                         | N/A (no I/O)    | —                | —                         |
/// | `alloc(0)`                           | N/A (no I/O)    | —                | —                         |
/// | `alloc(≤ block_size)`, free list hit | Partial         | No (4)           | Popped block leaked       |
/// | `alloc(≤ block_size)`, tail extend   | Atomic          | Yes (`extend`)   | —                         |
/// | `alloc(> block_size)`                | Atomic          | Yes (`extend`)   | —                         |
/// | `dealloc(null)`                      | N/A (no I/O)    | —                | —                         |
/// | `dealloc`, oversized tail            | Atomic          | Yes (`discard`)  | —                         |
/// | `dealloc`, other blocks              | Partial         | No (3)           | Entire freed batch leaked |
/// | `realloc`, same block count          | Atomic          | No (0–1)         | —                         |
/// | `realloc`, tail grow                 | Atomic          | Yes (`extend`)   | —                         |
/// | `realloc`, tail shrink               | Atomic          | Yes (`discard`)  | —                         |
/// | `realloc`, shrink non-tail           | Partial         | No (3)           | Freed blocks leaked       |
/// | `realloc`, grow non-tail             | Partial         | No (4–5)         | Old blocks leaked         |
///
/// **Atomicity key:** *Atomic* — crash leaves the file fully consistent (no partial writes visible);
/// *Partial* — crash keeps the free list consistent but may leak ≤ 1 block or batch;
/// *N/A* — operation performs no I/O.
///
/// # Thread safety
///
/// `SlabBStackAllocator` is always **`Send`** — ownership can be transferred
/// to another thread.
///
/// Without the `atomic` feature it is **not `Sync`**: free-list mutations
/// require a read then a write of `free_head` as separate `BStack` calls — a
/// TOCTOU race under concurrent `&self` access that can result in two callers
/// receiving the same block.
///
/// With the `atomic` feature it **is `Sync`** with no allocator-level lock.
/// Free-list push uses [`BStack::cross_exchange`] to splice a block (or a
/// whole freed run) onto the head in one atomic step; free-list pop drives a
/// single [`BStack::process_gen`] sequence that holds `BStack`'s write lock
/// across the read of `free_head`, the read of the popped block's `next`
/// pointer, and the write that advances `free_head`. Tail grow/shrink paths
/// use [`BStack::try_extend_zeros`] / [`BStack::try_discard`] to perform
/// check-and-act atomically under `BStack`'s write lock. Every concurrent
/// `&self` operation is therefore safe without any `Mutex`.
/// ```
/// fn assert_send<T: Send>() {}
/// assert_send::<bstack::SlabBStackAllocator>();
/// ```
///
/// Without `atomic` the type is `!Sync` (this fails to compile); with `atomic`
/// `BStack`'s own interior mutability makes it `Sync` (this compiles):
///
#[cfg_attr(not(feature = "atomic"), doc = "```compile_fail")]
#[cfg_attr(feature = "atomic", doc = "```")]
/// fn assert_sync<T: Sync>() {}
/// assert_sync::<bstack::SlabBStackAllocator>();
/// ```
///
/// # Feature flags
///
/// Requires both the `alloc` and `set` Cargo features:
///
/// ```toml
/// bstack = { version = "0.2", features = ["alloc", "set"] }
/// ```
#[cfg(feature = "set")]
pub struct SlabBStackAllocator {
    stack: BStack,
    /// Cached from the on-disk header; fixed for the lifetime of the allocator.
    block_size: u64,
    #[cfg(not(feature = "atomic"))]
    _not_sync: PhantomData<Cell<()>>,
}

#[cfg(feature = "set")]
impl SlabBStackAllocator {
    /// Bytes before the allocator header reserved for caller use.
    const OFFSET_SIZE: u64 = 24;
    /// Allocator header size: `magic[8] + block_size[8] + free_head[8]`.
    const HEADER_SIZE: u64 = 24;
    /// Payload offset of the first arena block.
    const ARENA_START: u64 = Self::OFFSET_SIZE + Self::HEADER_SIZE;
    /// Payload offset of the `free_head` field inside the header.
    const FREE_HEAD_OFFSET: u64 = Self::OFFSET_SIZE + 16;
    /// Minimum legal `block_size`: must fit at least one free-list pointer.
    const MIN_BLOCK_SIZE: u64 = 8;
    /// Free-list sentinel meaning "no next block".
    const SENTINEL: u64 = 0;

    /// Initialise a new `SlabBStackAllocator` over an empty `stack`.
    ///
    /// Writes the 48-byte allocator header (24 reserved bytes, magic,
    /// `block_size`, and `free_head = 0`) using a single `BStack::push`
    /// and returns a ready allocator.
    ///
    /// # Errors
    ///
    /// * [`io::ErrorKind::InvalidInput`] — `block_size < 8`, or `stack` is not
    ///   empty (use [`SlabBStackAllocator::open`] to reopen an existing file).
    /// * Any [`io::Error`] propagated from the underlying [`BStack`] operations.
    pub fn new(stack: BStack, block_size: u64) -> io::Result<Self> {
        if !stack.is_empty()? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "stack is not empty; use SlabBStackAllocator::open to reopen an existing allocator",
            ));
        }
        if block_size < Self::MIN_BLOCK_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "block_size ({block_size}) must be >= {}",
                    Self::MIN_BLOCK_SIZE
                ),
            ));
        }
        if usize::try_from(block_size).is_err() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "block_size is too large for this platform",
            ));
        }
        let mut hdr = [0u8; Self::ARENA_START as usize];
        let off = Self::OFFSET_SIZE as usize;
        hdr[off..off + 8].copy_from_slice(&ALSL_MAGIC);
        hdr[off + 8..off + 16].copy_from_slice(&block_size.to_le_bytes());
        // free_head at off+16 remains 0 (SENTINEL)
        stack.push(hdr)?;
        Ok(Self {
            stack,
            block_size,
            #[cfg(not(feature = "atomic"))]
            _not_sync: PhantomData,
        })
    }

    /// Open an existing `SlabBStackAllocator` from a non-empty `stack`.
    ///
    /// Validates the `ALSL 0.1.x` magic prefix and reads `block_size` from
    /// the stored header.
    ///
    /// # Errors
    ///
    /// * [`io::ErrorKind::InvalidInput`] — `stack` is empty (use
    ///   [`SlabBStackAllocator::new`] to create a new allocator).
    /// * [`io::ErrorKind::InvalidData`] — wrong magic, invalid stored
    ///   `block_size`, or invalid `free_head`.
    /// * Any [`io::Error`] propagated from the underlying [`BStack`] operations.
    pub fn open(stack: BStack) -> io::Result<Self> {
        if stack.is_empty()? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "stack is empty; use SlabBStackAllocator::new to create a new allocator",
            ));
        }

        let stack_len = stack.len()?;
        if stack_len < Self::ARENA_START {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "stack too short to contain allocator header",
            ));
        }

        let mut header = [0u8; Self::HEADER_SIZE as usize];
        stack.get_into(Self::OFFSET_SIZE, &mut header)?;

        if header[..ALSL_MAGIC_PREFIX.len()] != ALSL_MAGIC_PREFIX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid magic: not a SlabBStackAllocator file",
            ));
        }

        let stored_block_size = u64::from_le_bytes(header[8..16].try_into().unwrap());
        if stored_block_size < Self::MIN_BLOCK_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("stored block_size ({stored_block_size}) is invalid"),
            ));
        }
        if usize::try_from(stored_block_size).is_err() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "stored block_size is too large for this platform",
            ));
        }
        let stored_free_head = u64::from_le_bytes(header[16..24].try_into().unwrap());
        if stored_free_head != Self::SENTINEL
            && (stored_free_head < Self::ARENA_START
                || (stored_free_head - Self::ARENA_START) % stored_block_size != 0
                || stored_free_head >= stack_len)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("stored free_head ({stored_free_head}) is not a valid block offset"),
            ));
        }
        let arena_bytes = stack_len.checked_sub(Self::ARENA_START).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "stack too short to contain allocator header",
            )
        })?;
        if arena_bytes % stored_block_size != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "stack tail is not aligned to block_size",
            ));
        }

        Ok(Self {
            stack,
            block_size: stored_block_size,
            #[cfg(not(feature = "atomic"))]
            _not_sync: PhantomData,
        })
    }

    /// Return the `block_size` this allocator was created with.
    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    /// Pop the head block from the free list. Returns its payload offset, or `None`.
    ///
    /// # `atomic` feature
    ///
    /// Drives a single [`BStack::process_gen`] sequence — read `free_head`,
    /// read its `next` pointer, write `next` back into `free_head` — under one
    /// held write lock, so the read-read-write is indivisible with respect to
    /// any other thread's free-list operations. The freed block is zeroed in a
    /// separate call afterwards, since by then it is exclusively owned by the
    /// caller.
    #[cfg(feature = "atomic")]
    fn pop_free_block(&self) -> io::Result<Option<NonZeroU64>> {
        let mut head_buf = [0u8; 8];
        let mut next_buf = [0u8; 8];
        let mut step = 0usize;
        let mut popped: Option<u64> = None;

        self.stack.process_gen(|| {
            let op = match step {
                // Step 0: read the current free-list head.
                0 => Some(BStackGenOp::Read {
                    offset: Self::FREE_HEAD_OFFSET,
                    // SAFETY: `head_buf` outlives this `process_gen` call.
                    buf: unsafe { core::mem::transmute::<&mut [u8], &mut [u8]>(&mut head_buf[..]) },
                }),
                // Step 1: an empty list ends the sequence with no write;
                // otherwise read the head block's next-pointer.
                1 => {
                    let head = u64::from_le_bytes(head_buf);
                    if head == Self::SENTINEL {
                        None
                    } else {
                        popped = Some(head);
                        Some(BStackGenOp::Read {
                            offset: head,
                            // SAFETY: `next_buf` outlives this `process_gen` call.
                            buf: unsafe {
                                core::mem::transmute::<&mut [u8], &mut [u8]>(&mut next_buf[..])
                            },
                        })
                    }
                }
                // Step 2: advance free_head to the popped block's next pointer,
                // still under the lock acquired for step 0's read.
                2 => Some(BStackGenOp::Write {
                    offset: Self::FREE_HEAD_OFFSET,
                    // SAFETY: `next_buf` outlives this `process_gen` call.
                    data: unsafe { core::mem::transmute::<&[u8], &[u8]>(&next_buf[..]) },
                }),
                _ => None,
            };
            step += 1;
            op
        })?;

        let Some(head) = popped else {
            return Ok(None);
        };
        self.stack.zero(head, self.block_size)?;
        // SAFETY: head is not zero since we checked for the SENTINEL case above, so it is a valid NonZeroU64
        Ok(Some(head.try_into().unwrap()))
    }

    /// Pop the head block from the free list. Returns its payload offset, or `None`.
    #[cfg(not(feature = "atomic"))]
    fn pop_free_block(&self) -> io::Result<Option<NonZeroU64>> {
        let head = u64::from_le_bytes(read_bstack!(self.stack, Self::FREE_HEAD_OFFSET => u64));
        if head == Self::SENTINEL {
            return Ok(None);
        }
        self.stack.set(
            Self::FREE_HEAD_OFFSET,
            read_bstack!(self.stack, head => u64),
        )?;
        self.stack.zero(head, self.block_size)?;
        // SAFETY: head is not zero since we checked for the SENTINEL case above, so it is a valid NonZeroU64
        Ok(Some(head.try_into().unwrap()))
    }

    /// Prepend the block at `block_start` to the free list.
    ///
    /// # `atomic` feature
    ///
    /// Lock-free splice via [`BStack::cross_exchange`]: `block_start` is first
    /// seeded with a self-pointer placeholder, then atomically swapped with
    /// `free_head` under one write lock — `free_head` becomes `block_start` and
    /// `block_start`'s next-pointer becomes the old head, in a single
    /// indivisible step. A crash between the two calls leaks `block_start`
    /// rather than corrupting the list.
    #[cfg(feature = "atomic")]
    fn push_free_block(&self, block_start: u64) -> io::Result<()> {
        self.stack.set(block_start, block_start.to_le_bytes())?;
        self.stack
            .cross_exchange(block_start, Self::FREE_HEAD_OFFSET, 8)
    }

    /// Prepend the block at `block_start` to the free list.
    #[cfg(not(feature = "atomic"))]
    fn push_free_block(&self, block_start: u64) -> io::Result<()> {
        // Write the next-pointer into the block before updating free_head: a
        // crash after this write but before the header update leaks the block
        // rather than corrupting the list.
        self.stack.set(
            block_start,
            read_bstack!(self.stack, Self::FREE_HEAD_OFFSET => u64),
        )?;
        self.stack
            .set(Self::FREE_HEAD_OFFSET, block_start.to_le_bytes())
    }

    /// Prepend `count` contiguous blocks starting at `first_block` to the free list.
    ///
    /// Requires that count * block_size does not overflow u64 and
    /// first_block + count * block_size does not overflow u64 and is a valid offset
    /// on the stack by the caller.
    ///
    /// # `atomic` feature
    ///
    /// Generalises [`push_free_block`](Self::push_free_block) to a whole run:
    /// the chain `first_block -> first_block + block_size -> ... -> last_block`
    /// is built in one buffer, with `last_block`'s next-pointer set to the
    /// placeholder `first_block`. A single bulk [`BStack::set`] writes the
    /// chain (still unreachable from `free_head`), then
    /// [`BStack::cross_exchange`] atomically swaps `last_block`'s next-pointer
    /// with `free_head` — `free_head` becomes `first_block` and `last_block`'s
    /// next-pointer becomes the old head, splicing the whole run in under one
    /// write lock. For `count == 1`, `first_block == last_block` and this is
    /// exactly `push_free_block`.
    fn push_free_blocks(&self, first_block: u64, count: u64) -> io::Result<()> {
        if count == 0 {
            return Ok(());
        }
        if count == 1 {
            return self.push_free_block(first_block);
        }
        let total_bytes = count.checked_mul(self.block_size).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "freed region size overflows u64",
            )
        })?;
        first_block.checked_add(total_bytes).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "freed region end offset overflows u64",
            )
        })?;
        let buf_size = usize::try_from(total_bytes).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "freed region exceeds platform pointer size",
            )
        })?;
        let mut buf = vec![0u8; buf_size];
        for i in 0..count {
            let next = if i + 1 < count {
                first_block
                    .checked_add((i + 1).checked_mul(self.block_size).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "next block index multiplication overflows u64",
                        )
                    })?)
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "next block offset overflows u64",
                        )
                    })?
            } else {
                #[cfg(feature = "atomic")]
                {
                    // Placeholder: replaced with the old free_head by
                    // cross_exchange below.
                    first_block
                }
                #[cfg(not(feature = "atomic"))]
                {
                    u64::from_le_bytes(read_bstack!(self.stack, Self::FREE_HEAD_OFFSET => u64))
                }
            };
            let off = usize::try_from(i.checked_mul(self.block_size).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "free-list offset overflows u64",
                )
            })?)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "free-list offset overflows usize",
                )
            })?;
            buf[off..off + 8].copy_from_slice(&next.to_le_bytes());
        }
        self.stack.set(first_block, buf)?;

        #[cfg(feature = "atomic")]
        {
            let last_block = first_block
                .checked_add((count - 1).checked_mul(self.block_size).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "last free-list offset overflows u64",
                    )
                })?)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "last block offset overflows u64",
                    )
                })?;
            self.stack
                .cross_exchange(last_block, Self::FREE_HEAD_OFFSET, 8)
        }
        #[cfg(not(feature = "atomic"))]
        {
            self.stack
                .set(Self::FREE_HEAD_OFFSET, first_block.to_le_bytes())
        }
    }

    /// Number of `block_size` blocks required to back `len` bytes.
    fn blocks_needed(&self, len: u64) -> u64 {
        if len == 0 {
            0
        } else if len <= self.block_size {
            1
        } else {
            len.div_ceil(self.block_size)
        }
    }
}

#[cfg(feature = "set")]
impl fmt::Debug for SlabBStackAllocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlabBStackAllocator")
            .field("block_size", &self.block_size)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "set")]
impl BStackAllocator for SlabBStackAllocator {
    type Error = io::Error;
    type Allocated<'a> = BStackOwnedSlice<'a, Self>;

    fn stack(&self) -> &BStack {
        &self.stack
    }

    fn into_stack(self) -> BStack {
        self.stack
    }

    /// Allocate `len` bytes.
    ///
    /// # Crash consistency
    ///
    /// | Path | Calls | Safety |
    /// |------|-------|--------|
    /// | `len == 0` | 0 | trivially safe |
    /// | slab, free list hit | 4 (2× `get_into` + `set` + `zero`) | crash may leak popped block |
    /// | slab, tail extend | 1 (`extend`) | crash-safe by inheritance |
    /// | oversized | 1 (`extend`) | crash-safe by inheritance |
    fn alloc(&self, len: u64) -> io::Result<BStackOwnedSlice<'_, Self>> {
        if len == 0 {
            return Ok(BStackOwnedSlice::empty(self));
        }

        if len <= self.block_size {
            if let Some(block) = self.pop_free_block()? {
                // SAFETY: block is a valid block_size region from pop_free_block
                return Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, block.into(), len) });
            }
            let offset = self.stack.extend(self.block_size)?;
            // SAFETY: offset from a fresh tail extension of block_size bytes
            return Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, offset, len) });
        }

        let n = len.div_ceil(self.block_size);
        let total = n.checked_mul(self.block_size).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "allocation size overflows u64")
        })?;
        let offset = self.stack.extend(total)?;
        // SAFETY: offset from a fresh tail extension of n * block_size bytes
        Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, offset, len) })
    }

    /// Release the region described by `slice`.
    ///
    /// # Crash consistency
    ///
    /// | Path | Calls | Safety |
    /// |------|-------|--------|
    /// | null slice | 0 | trivially safe |
    /// | oversized tail | 1 (`discard`) | crash-safe by inheritance |
    /// | slab / oversized non-tail | 3 total (`get_into` + bulk `set` + `set`) | crash leaks entire freed batch |
    ///
    /// Double-freeing a slice corrupts the free list; this allocator does not guard against it.
    fn dealloc(&self, slice: BStackOwnedSlice<'_, Self>) -> io::Result<()> {
        if slice.is_empty() && slice.start() == Self::SENTINEL {
            return Ok(());
        }

        let n_blocks = self.blocks_needed(slice.len());
        let backing_size = n_blocks.checked_mul(self.block_size).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "deallocation size overflows u64",
            )
        })?;
        let slice_end = slice.start().checked_add(backing_size).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "deallocation end offset overflows u64",
            )
        })?;

        // Tail discard path: only for oversized allocations (> 1 block).
        // try_discard atomically checks tail == slice_end and removes backing_size
        // bytes under BStack's own write lock — no allocator lock needed.
        #[cfg(feature = "atomic")]
        if slice.len() > self.block_size && self.stack.try_discard(slice_end, backing_size)? {
            return Ok(());
        }

        #[cfg(not(feature = "atomic"))]
        if slice.len() > self.block_size && slice_end == self.stack.len()? {
            return self.stack.discard(backing_size);
        }

        // Not at tail (or single-block): push to the free list.
        self.push_free_blocks(slice.start(), n_blocks)
    }

    /// Resize the region described by `slice` to `new_len` bytes.
    ///
    /// # Resize strategies
    ///
    /// | Case | Strategy |
    /// |------|----------|
    /// | Same block count | Adjust visible length only (no I/O) |
    /// | Slice at tail | Extend or discard tail (single `BStack` call) |
    /// | Shrink, non-tail | Recycle excess blocks into the free list |
    /// | Grow, non-tail | Allocate fresh region, copy, release old |
    fn realloc<'a>(
        &'a self,
        slice: BStackOwnedSlice<'a, Self>,
        new_len: u64,
    ) -> io::Result<BStackOwnedSlice<'a, Self>> {
        if slice.is_empty() && slice.start() == Self::SENTINEL {
            return self.alloc(new_len);
        }
        if new_len == 0 {
            self.dealloc(slice)?;
            return Ok(BStackOwnedSlice::empty(self));
        }
        if new_len == slice.len() {
            return Ok(slice);
        }

        let old_n = self.blocks_needed(slice.len());
        let new_n = self.blocks_needed(new_len);

        if old_n == new_n {
            // Same backing blocks: zero newly-exposed bytes then adjust visible length.
            // Integer safety: old and new slice length are both valid u64 values and they could not differ
            // by more than block_size by bytes, so new_len - slice.len() will not overflow.
            if new_len > slice.len() {
                self.stack.zero(slice.end(), new_len - slice.len())?;
            }
            // SAFETY: new_len still fits within the same block_size-aligned region
            return Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, slice.start(), new_len) });
        }

        let old_backing = old_n.checked_mul(self.block_size).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "old allocation size overflows u64",
            )
        })?;
        let new_backing = new_n.checked_mul(self.block_size).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "new allocation size overflows u64",
            )
        })?;

        let checked_len = slice.start().checked_add(old_backing).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "tail check overflows u64")
        })?;

        if new_n > old_n {
            // Grow path.
            // With `atomic`: try_extend_zeros atomically checks tail == checked_len
            // and appends the delta — no allocator lock needed.
            // Without `atomic`: plain len() check then extend (single-threaded).
            #[cfg(feature = "atomic")]
            if self
                .stack
                .try_extend_zeros(checked_len, new_backing - old_backing)?
            {
                if new_len > slice.len() {
                    self.stack.zero(slice.end(), new_len - slice.len())?;
                }
                // SAFETY: slice extended in place at the tail
                return Ok(unsafe {
                    BStackOwnedSlice::from_raw_parts(self, slice.start(), new_len)
                });
            }

            #[cfg(not(feature = "atomic"))]
            if checked_len == self.stack.len()? {
                self.stack.extend(new_backing - old_backing)?;
                if new_len > slice.len() {
                    self.stack.zero(slice.end(), new_len - slice.len())?;
                }
                // SAFETY: slice extended in place at the tail
                return Ok(unsafe {
                    BStackOwnedSlice::from_raw_parts(self, slice.start(), new_len)
                });
            }

            // Grow non-tail: copy data into a fresh region, then free the old blocks.
            // get_into and push need no lock; push_free_blocks mutates the free list.
            let buf_len = usize::try_from(new_backing).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "reallocation too large for this platform",
                )
            })?;
            let mut data_buf = vec![0u8; buf_len];
            let old_visible_len = usize::try_from(slice.len()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "existing allocation too large for this platform",
                )
            })?;
            self.stack
                .get_into(slice.start(), &mut data_buf[..old_visible_len])?;
            let new_ptr = self.stack.push(data_buf)?;
            self.push_free_blocks(slice.start(), old_n)?;
            // SAFETY: new_len fits within the new_n blocks of the newly pushed region
            return Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, new_ptr, new_len) });
        }

        // Shrink path (new_n < old_n).
        // With `atomic`: try_discard atomically checks tail == checked_len and removes
        // the excess — no lock needed. On failure the slice is not at the tail;
        // fall through to shrink non-tail.
        // Without `atomic`: plain len() check then discard (single-threaded).
        #[cfg(feature = "atomic")]
        if self
            .stack
            .try_discard(checked_len, old_backing - new_backing)?
        {
            // SAFETY: slice shrunk in place at the tail
            return Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, slice.start(), new_len) });
        }

        #[cfg(not(feature = "atomic"))]
        if checked_len == self.stack.len()? {
            self.stack.discard(old_backing - new_backing)?;
            // SAFETY: slice shrunk in place at the tail
            return Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, slice.start(), new_len) });
        }

        // Shrink non-tail: recycle excess blocks into the free list.
        let free_start = slice
            .start()
            .checked_add(new_n.checked_mul(self.block_size).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "free start multiplication overflows u64",
                )
            })?)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "free start overflows u64")
            })?;
        self.push_free_blocks(free_start, old_n - new_n)?;
        // SAFETY: new_len fits within the first new_n retained blocks
        Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, slice.start(), new_len) })
    }
}

#[cfg(all(test, feature = "set"))]
mod _assertions {
    use super::SlabBStackAllocator;
    fn _send()
    where
        SlabBStackAllocator: Send,
    {
    }
    #[cfg(feature = "atomic")]
    fn _sync()
    where
        SlabBStackAllocator: Sync,
    {
    }
}

#[cfg(all(test, feature = "set"))]
mod tests {
    use super::SlabBStackAllocator;
    use crate::BStack;
    use crate::alloc::{BStackAllocator, BStackSlice};
    use std::io::ErrorKind;
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
        std::env::temp_dir().join(format!("bstack_slab_{pid}_{id}.bin"))
    }

    fn empty_stack() -> (BStack, std::path::PathBuf) {
        let path = temp_path();
        (BStack::open(&path).unwrap(), path)
    }

    // ── new() ─────────────────────────────────────────────────────────────────

    #[test]
    fn new_initialises_header_and_reports_block_size() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = SlabBStackAllocator::new(stack, 16).unwrap();
        assert_eq!(alloc.block_size(), 16);
        // ARENA_START = OFFSET_SIZE(24) + HEADER_SIZE(24) = 48
        assert_eq!(alloc.stack().len().unwrap(), 48);
    }

    #[test]
    fn new_rejects_block_size_below_minimum() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let err = SlabBStackAllocator::new(stack, 7).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn new_rejects_nonempty_stack() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        stack.push(b"data").unwrap();
        let err = SlabBStackAllocator::new(stack, 8).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    // ── open() ────────────────────────────────────────────────────────────────

    #[test]
    fn open_rejects_empty_stack() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let err = SlabBStackAllocator::open(stack).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn open_rejects_stack_too_short() {
        let (stack, path) = empty_stack();
        let _g = Guard(path.clone());
        stack.push([0u8; 24]).unwrap(); // only 24 bytes, need >= 48
        drop(stack);
        let err = SlabBStackAllocator::open(BStack::open(&path).unwrap()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn open_rejects_bad_magic() {
        let (stack, path) = empty_stack();
        let _g = Guard(path.clone());
        stack.push([0u8; 48]).unwrap(); // 48 bytes of zeros — no ALSL magic
        drop(stack);
        let err = SlabBStackAllocator::open(BStack::open(&path).unwrap()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn open_rejects_invalid_stored_block_size() {
        let (stack, path) = empty_stack();
        let _g = Guard(path.clone());
        // Craft a header with valid magic but block_size = 1 (< MIN_BLOCK_SIZE = 8).
        let mut hdr = [0u8; 48];
        hdr[24..32].copy_from_slice(b"ALSL\x00\x01\x00\x00");
        hdr[32..40].copy_from_slice(&1u64.to_le_bytes());
        stack.push(hdr).unwrap();
        drop(stack);
        let err = SlabBStackAllocator::open(BStack::open(&path).unwrap()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn open_rejects_misaligned_tail() {
        let (stack, path) = empty_stack();
        let _g = Guard(path.clone());
        SlabBStackAllocator::new(stack, 8).unwrap();
        let reopen = BStack::open(&path).unwrap();
        reopen.extend(1).unwrap();
        drop(reopen);
        let err = SlabBStackAllocator::open(BStack::open(&path).unwrap()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn open_succeeds_and_restores_block_size() {
        let (stack, path) = empty_stack();
        let _g = Guard(path.clone());
        SlabBStackAllocator::new(stack, 32).unwrap();
        let alloc = SlabBStackAllocator::open(BStack::open(&path).unwrap()).unwrap();
        assert_eq!(alloc.block_size(), 32);
    }

    // ── allocation behaviour ──────────────────────────────────────────────────

    #[test]
    fn zero_alloc_returns_empty_slice() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = SlabBStackAllocator::new(stack, 8).unwrap();
        let s = alloc.alloc(0).unwrap();
        assert!(s.is_empty());
    }

    #[test]
    fn dealloc_pushes_to_free_list_and_next_alloc_reuses_block() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = SlabBStackAllocator::new(stack, 8).unwrap();

        let s1 = alloc.alloc(8).unwrap();
        let offset1 = s1.start();
        alloc.dealloc(s1).unwrap();

        let s2 = alloc.alloc(8).unwrap();
        assert_eq!(s2.start(), offset1);
    }

    #[test]
    fn free_list_recycles_all_dealloc_d_blocks() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = SlabBStackAllocator::new(stack, 8).unwrap();

        let a = alloc.alloc(8).unwrap();
        let b = alloc.alloc(8).unwrap();
        let c = alloc.alloc(8).unwrap();
        let mut original = [a.start(), b.start(), c.start()];
        alloc.dealloc(a).unwrap();
        alloc.dealloc(b).unwrap();
        alloc.dealloc(c).unwrap();

        let r1 = alloc.alloc(8).unwrap();
        let r2 = alloc.alloc(8).unwrap();
        let r3 = alloc.alloc(8).unwrap();
        let mut reused = [r1.start(), r2.start(), r3.start()];

        original.sort();
        reused.sort();
        assert_eq!(reused, original);
    }

    #[test]
    fn oversized_tail_dealloc_shrinks_stack() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = SlabBStackAllocator::new(stack, 8).unwrap();

        // 17 bytes needs 3 blocks (3 × 8 = 24 bytes backing).
        let s = alloc.alloc(17).unwrap();
        let tail_before = alloc.stack().len().unwrap();
        assert_eq!(
            s.start() + 24,
            tail_before,
            "allocation must be at the tail"
        );

        alloc.dealloc(s).unwrap();
        assert_eq!(alloc.stack().len().unwrap(), tail_before - 24);
    }

    #[test]
    fn write_and_read_round_trip() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = SlabBStackAllocator::new(stack, 16).unwrap();
        let mut s = alloc.alloc(12).unwrap();
        s.write(b"hello world!").unwrap();
        assert_eq!(s.read().unwrap(), b"hello world!");
    }

    #[test]
    fn data_survives_reopen() {
        let (stack, path) = empty_stack();
        let _g = Guard(path.clone());
        let alloc = SlabBStackAllocator::new(stack, 16).unwrap();
        let mut s = alloc.alloc(5).unwrap();
        let offset = s.start();
        s.write(b"hello").unwrap();
        drop(alloc);

        let alloc2 = SlabBStackAllocator::open(BStack::open(&path).unwrap()).unwrap();
        let s2 = unsafe { BStackSlice::from_raw_parts(alloc2.stack(), offset, 5) };
        assert_eq!(s2.read().unwrap(), b"hello");
    }

    // ── concurrent (feature = "atomic") ───────────────────────────────────────

    #[cfg(feature = "atomic")]
    #[test]
    fn concurrent_alloc_dealloc_no_live_duplicates() {
        use std::collections::HashSet;
        use std::sync::{Arc, Mutex};
        use std::thread;

        // Verify that concurrent alloc/dealloc never hands the same block to
        // two callers simultaneously.  Each thread claims a block, inserts its
        // offset into a shared live-set (asserting uniqueness), writes and
        // reads back its thread id, then removes the offset and deallocates.
        // Slab has no per-block overhead, so a free-list race would silently
        // produce a duplicate offset rather than an error; the HashSet catches
        // that.
        const THREADS: usize = 8;
        const ROUNDS: usize = 200;

        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = Arc::new(SlabBStackAllocator::new(stack, 16).unwrap());
        let live: Arc<Mutex<HashSet<u64>>> = Arc::new(Mutex::new(HashSet::new()));

        let handles: Vec<_> = (0..THREADS)
            .map(|tid| {
                let alloc = Arc::clone(&alloc);
                let live = Arc::clone(&live);
                thread::spawn(move || {
                    let a: &SlabBStackAllocator = &alloc;
                    for _ in 0..ROUNDS {
                        let mut slice = a.alloc(16).unwrap();
                        let off = slice.start();
                        {
                            let mut set = live.lock().unwrap();
                            assert!(set.insert(off), "duplicate live offset {off}");
                        }
                        slice.write(&[tid as u8; 16]).unwrap();
                        let data = slice.read().unwrap();
                        assert_eq!(data, vec![tid as u8; 16]);
                        {
                            let mut set = live.lock().unwrap();
                            set.remove(&off);
                        }
                        a.dealloc(slice).unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn concurrent_realloc_hammers_tail_paths() {
        use std::sync::Arc;
        use std::thread;

        // T threads each own one allocation and repeatedly grow then shrink it.
        // Whichever allocation sits at the tail exercises try_extend_zeros /
        // try_discard; the others hit the non-tail copy-grow / block-recycle
        // paths.  Both branches are exercised on every round because threads
        // race for the tail.  Verify each thread's data survives every round.
        //
        // With block_size = 16:
        //   alloc(12) → 1 block; alloc(17) → 2 blocks; alloc(33) → 3 blocks.
        const THREADS: usize = 6;
        const ROUNDS: usize = 150;
        const SMALL: u64 = 12; // fits in 1 block (block_size = 16)
        const LARGE: u64 = 33; // needs 3 blocks

        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = Arc::new(SlabBStackAllocator::new(stack, 16).unwrap());

        let handles: Vec<_> = (0..THREADS)
            .map(|tid| {
                let alloc = Arc::clone(&alloc);
                thread::spawn(move || {
                    let a: &SlabBStackAllocator = &alloc;
                    let mut slice = a.alloc(SMALL).unwrap();
                    slice
                        .as_slice_mut()
                        .write(&[tid as u8; SMALL as usize])
                        .unwrap();

                    for _ in 0..ROUNDS {
                        // Grow: tail → try_extend_zeros; non-tail → copy to new region.
                        slice = a.realloc(slice, LARGE).unwrap();
                        let data = slice.read().unwrap();
                        assert_eq!(
                            &data[..SMALL as usize],
                            &[tid as u8; SMALL as usize],
                            "data corrupted after grow (tid {tid})",
                        );

                        // Shrink: tail → try_discard; non-tail → recycle excess blocks.
                        slice = a.realloc(slice, SMALL).unwrap();
                        let data = slice.read().unwrap();
                        assert_eq!(
                            data,
                            vec![tid as u8; SMALL as usize],
                            "data corrupted after shrink (tid {tid})",
                        );
                    }

                    a.dealloc(slice).unwrap();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }
}
