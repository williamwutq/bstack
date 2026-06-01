//! Crash-recoverable fixed-block slab allocator for [`BStack`]-backed storage.
//!
//! Provides [`CheckedSlabBStackAllocator`], a variant of
//! [`SlabBStackAllocator`](super::SlabBStackAllocator) that prefixes every block
//! with an 8-byte overhead field encoding whether the block is free or in use.
//! The overhead makes leaked blocks recoverable by a linear scan after a crash
//! and lets `dealloc` detect double-free at runtime before the free list can be
//! corrupted.

use super::{BStackAllocator, BStackSlice};
use crate::BStack;
use core::{cell::Cell, marker::PhantomData};
use std::{fmt, io};

#[cfg(feature = "set")]
const ALCK_MAGIC: [u8; 8] = *b"ALCK\x00\x01\x00\x00";

/// Compatibility prefix checked on open: `ALCK` + major 0 + minor 1.
/// Any file whose first 6 bytes match is considered compatible.
#[cfg(feature = "set")]
const ALCK_MAGIC_PREFIX: [u8; 6] = *b"ALCK\x00\x01";

/// A crash-recoverable fixed-block slab allocator implementing
/// [`BStackAllocator`] on top of a [`BStack`].
///
/// Unlike [`SlabBStackAllocator`](super::SlabBStackAllocator), every block in
/// the arena carries an 8-byte **overhead** prefix that records the block's
/// state. The slice handed to the caller covers only the `data` region that
/// follows the overhead, i.e. `block_size − 8` usable bytes in a single block.
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
/// Every block within the arena has the shape:
///
/// ```text
/// [ overhead(8) | data ... ]
/// ```
///
/// The overhead field encodes block state:
///
/// | Value | Meaning |
/// |---|---|
/// | `0x0000_0000_0000_0000` | Block is free. `data[0..8]` holds the next free block offset (little-endian `u64`, sentinel `0`). |
/// | `0x8NNN_NNNN_NNNN_NNNN` | Block is in use; `NNN…` is the allocation size in number of blocks; the high bit is always 1. |
///
/// Because the minimum block size is 16 bytes, the maximum allocation size in
/// blocks is `2^63 / 16 = 2^59`, so the 2nd–5th hex digits of an in-use
/// overhead are always zero. These bits are reserved for future metadata and
/// are not validated.
///
/// A multi-block allocation stores its overhead **only in the first block**;
/// the remaining `block_size − 8` bytes of the first block and every subsequent
/// block are one contiguous `data` region. A linear recovery scan therefore
/// advances by `num_blocks` blocks at a live allocation and by one block at a
/// free block, so it never reads allocation-interior bytes as overhead.
///
/// # Allocation policy
///
/// * `len == 0` — returns a zero-length sentinel slice (`offset = 0, len = 0`).
/// * `num_blocks == 1` (`len ≤ block_size − 8`) — pops from the free list if
///   available; otherwise extends the tail by exactly `block_size` bytes.
/// * `num_blocks > 1` — always extends the tail by `num_blocks × block_size`
///   bytes. Multi-block allocations require a contiguous run and so never draw
///   from the (single-block) free list.
///
/// # Deallocation policy
///
/// * Already-free block (overhead high bit clear) — returns a double-free error
///   without modifying any list.
/// * Multi-block allocation at the tail — reclaimed with a single
///   [`BStack::discard`].
/// * All other cases — each `block_size` chunk becomes a free-list node.
///
/// # Crash consistency
///
/// Every operation that mutates the free list writes block payloads before the
/// `free_head` header pointer, and only flips a block's overhead high bit (the
/// "live" marker) as the last step that makes a region visible. A crash at any
/// intermediate point therefore leaks at most the block or batch being operated
/// on; the remaining free list stays consistent, and a linear scan over the
/// arena can reconstruct a valid free list from scratch if desired.
///
/// # Thread safety
///
/// Like [`SlabBStackAllocator`](super::SlabBStackAllocator), this allocator is
/// `Send` but not `Sync`: concurrent `&self` access must be externally
/// synchronized, because free-list mutation reads then writes `free_head` as
/// separate [`BStack`] calls.
///
/// # Feature flags
///
/// Requires both the `alloc` and `set` Cargo features:
///
/// ```toml
/// bstack = { version = "0.2", features = ["alloc", "set"] }
/// ```
#[cfg(feature = "set")]
pub struct CheckedSlabBStackAllocator {
    stack: BStack,
    /// Cached from the on-disk header; fixed for the lifetime of the allocator.
    /// Covers the full block including the 8-byte overhead; must be `≥ 16`.
    block_size: u64,
    // Mark as !Sync to prevent concurrent access to the free list.
    _not_sync: PhantomData<Cell<()>>,
}

#[cfg(feature = "set")]
impl CheckedSlabBStackAllocator {
    /// Bytes before the allocator header reserved for caller use.
    const OFFSET_SIZE: u64 = 24;
    /// Allocator header size: `magic[8] + block_size[8] + free_head[8]`.
    const HEADER_SIZE: u64 = 24;
    /// Payload offset of the first arena block.
    const ARENA_START: u64 = Self::OFFSET_SIZE + Self::HEADER_SIZE;
    /// Payload offset of the `free_head` field inside the header.
    const FREE_HEAD_OFFSET: u64 = Self::OFFSET_SIZE + 16;
    /// Per-block overhead prefix size in bytes.
    const OVERHEAD: u64 = 8;
    /// Minimum legal `block_size`: 8 overhead + 8 minimum usable.
    const MIN_BLOCK_SIZE: u64 = 16;
    /// Free-list sentinel meaning "no next block". 0 is safe because all blocks
    /// start at ARENA_START (48) or later and no valid block offset is 0.
    const SENTINEL: u64 = 0;
    /// High bit of the overhead field: set when a block is in use.
    const IN_USE_BIT: u64 = 0x8000_0000_0000_0000;
    /// Mask extracting the block-count field from an in-use overhead value.
    const BLOCKS_MASK: u64 = !Self::IN_USE_BIT;

    /// Initialise a new `CheckedSlabBStackAllocator` over an empty `stack`.
    ///
    /// Writes the 48-byte allocator header (24 reserved bytes, magic,
    /// `block_size`, and `free_head = 0`) using a single
    /// [`BStack::push`] and returns a ready allocator.
    ///
    /// # Errors
    ///
    /// * [`io::ErrorKind::InvalidInput`] — `block_size < 16`, or `stack` is not
    ///   empty (use [`CheckedSlabBStackAllocator::open`] to reopen an existing
    ///   file).
    /// * Any [`io::Error`] propagated from the underlying [`BStack`] operations.
    pub fn new(stack: BStack, block_size: u64) -> io::Result<Self> {
        if !stack.is_empty()? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "stack is not empty; use CheckedSlabBStackAllocator::open to reopen an existing allocator",
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
        hdr[off..off + 8].copy_from_slice(&ALCK_MAGIC);
        hdr[off + 8..off + 16].copy_from_slice(&block_size.to_le_bytes());
        // free_head at off+16 remains 0 (SENTINEL)
        stack.push(hdr)?;
        Ok(Self {
            stack,
            block_size,
            _not_sync: PhantomData,
        })
    }

    /// Open an existing `CheckedSlabBStackAllocator` from a non-empty `stack`.
    ///
    /// Validates the `ALCK 0.1.x` magic prefix, reads `block_size`, and checks
    /// that `free_head` is either the sentinel or points to a block-aligned
    /// offset whose overhead is zero (free).
    ///
    /// # Errors
    ///
    /// * [`io::ErrorKind::InvalidInput`] — `stack` is empty (use
    ///   [`CheckedSlabBStackAllocator::new`] to create a new allocator).
    /// * [`io::ErrorKind::InvalidData`] — wrong magic, invalid stored
    ///   `block_size`, misaligned arena, or an invalid `free_head`.
    /// * Any [`io::Error`] propagated from the underlying [`BStack`] operations.
    pub fn open(stack: BStack) -> io::Result<Self> {
        if stack.is_empty()? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "stack is empty; use CheckedSlabBStackAllocator::new to create a new allocator",
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

        if header[..ALCK_MAGIC_PREFIX.len()] != ALCK_MAGIC_PREFIX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid magic: not a CheckedSlabBStackAllocator file",
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
        // Checked: the free-list head must itself be a free block.
        if stored_free_head != Self::SENTINEL {
            let mut prefix = [0u8; 8];
            stack.get_into(stored_free_head, &mut prefix)?;
            if u64::from_le_bytes(prefix) != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "free-list head block is marked in use; free list corrupt",
                ));
            }
        }

        Ok(Self {
            stack,
            block_size: stored_block_size,
            _not_sync: PhantomData,
        })
    }

    /// Return the `block_size` this allocator was created with.
    ///
    /// This covers the full block including the 8-byte overhead; a single block
    /// holds `block_size − 8` usable bytes.
    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    /// Read the overhead word stored at the start of the block at `block_start`.
    fn read_overhead(&self, block_start: u64) -> io::Result<u64> {
        Ok(u64::from_le_bytes(
            read_bstack!(self.stack, block_start => u64),
        ))
    }

    /// Write the overhead word at the start of the block at `block_start`.
    fn write_overhead(&self, block_start: u64, value: u64) -> io::Result<()> {
        self.stack.set(block_start, value.to_le_bytes())
    }

    /// Number of `block_size` blocks required to back `len` usable bytes,
    /// accounting for the 8-byte overhead prefix.
    fn blocks_needed(&self, len: u64) -> io::Result<u64> {
        if len == 0 {
            return Ok(0);
        }
        let total = len.checked_add(Self::OVERHEAD).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "allocation length overflows u64",
            )
        })?;
        Ok(total.div_ceil(self.block_size))
    }

    /// Pop the head block off the free list, returning its block start offset.
    ///
    /// Detaches the block by advancing `free_head` to the popped block's next
    /// pointer; the block is left with its overhead still zero (free-looking),
    /// so a crash before the caller marks it in use merely leaks it. Verifies
    /// the popped block's overhead is zero and errors otherwise.
    fn pop_free_block(&self) -> io::Result<Option<u64>> {
        let head = u64::from_le_bytes(read_bstack!(self.stack, Self::FREE_HEAD_OFFSET => u64));
        if head == Self::SENTINEL {
            return Ok(None);
        }
        let mut prefix = [0u8; 16];
        self.stack.get_into(head, &mut prefix)?;
        let overhead = u64::from_le_bytes(prefix[0..8].try_into().unwrap());
        if overhead != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "free-list block at {head} has non-zero overhead {overhead:#018x}; free list corrupt"
                ),
            ));
        }
        // The next pointer lives in data[0..8] (offset head + 8).
        let next = &prefix[8..16];
        self.stack.set(Self::FREE_HEAD_OFFSET, next)?;
        Ok(Some(head))
    }

    /// Write the block prefixes for a run of `count` contiguous free blocks
    /// starting at `first_block`, linking them into a chain whose tail points at
    /// the current `free_head`. Does **not** update `free_head`.
    ///
    /// Each block's overhead is set to zero and its `data[0..8]` to the next
    /// block's offset (or the existing `free_head` for the last block). All
    /// other data bytes in the run are zeroed. The single bulk
    /// [`BStack::set`] makes this crash-safe: until `free_head` is repointed the
    /// whole run is simply unreachable.
    fn write_free_run(&self, first_block: u64, count: u64) -> io::Result<()> {
        debug_assert!(count > 0);
        let old_head = read_bstack!(self.stack, Self::FREE_HEAD_OFFSET => u64);
        let total = count.checked_mul(self.block_size).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "freed region size overflows u64",
            )
        })?;
        first_block.checked_add(total).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "freed region end offset overflows u64",
            )
        })?;
        let buf_size = usize::try_from(total).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "freed region exceeds platform pointer size",
            )
        })?;
        let mut buf = vec![0u8; buf_size];
        for i in 0..count {
            let base = usize::try_from(i.checked_mul(self.block_size).ok_or_else(|| {
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
            let next_bytes: [u8; 8] = if i + 1 < count {
                let next = first_block
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
                    })?;
                next.to_le_bytes()
            } else {
                old_head
            };
            // Overhead at buf[base..base+8] stays zero; next pointer at data[0..8].
            buf[base + 8..base + 16].copy_from_slice(&next_bytes);
        }
        self.stack.set(first_block, buf)
    }

    /// Prepend `count` contiguous blocks starting at `first_block` to the free
    /// list. The blocks' overhead bytes are cleared as part of the operation,
    /// so this also transitions a live allocation into free blocks.
    fn push_free_blocks(&self, first_block: u64, count: u64) -> io::Result<()> {
        if count == 0 {
            return Ok(());
        }
        self.write_free_run(first_block, count)?;
        self.stack
            .set(Self::FREE_HEAD_OFFSET, first_block.to_le_bytes())
    }
}

#[cfg(feature = "set")]
impl fmt::Debug for CheckedSlabBStackAllocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CheckedSlabBStackAllocator")
            .field("block_size", &self.block_size)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "set")]
impl BStackAllocator for CheckedSlabBStackAllocator {
    type Error = io::Error;
    type Allocated<'a> = BStackSlice<'a, Self>;

    fn stack(&self) -> &BStack {
        &self.stack
    }

    fn into_stack(self) -> BStack {
        self.stack
    }

    /// Allocate `len` bytes.
    ///
    /// The returned slice covers the `data` region only; the 8-byte overhead
    /// prefix is written transparently. `len == 0` yields the empty sentinel
    /// slice. Single-block requests reuse a free-list block when available and
    /// otherwise extend the tail; multi-block requests always extend the tail.
    fn alloc(&self, len: u64) -> io::Result<BStackSlice<'_, Self>> {
        if len == 0 {
            return Ok(BStackSlice::empty(self));
        }

        let num_blocks = self.blocks_needed(len)?;
        if num_blocks == 1 {
            if let Some(block_start) = self.pop_free_block()? {
                // Zero the data region, then flip the overhead to in-use last so
                // a crash before the marker leaves the block free-looking (and
                // merely leaked, since it is already detached from the list).
                self.stack.zero(
                    block_start + Self::OVERHEAD,
                    self.block_size - Self::OVERHEAD,
                )?;
                self.write_overhead(block_start, Self::IN_USE_BIT | 1)?;
                // SAFETY: block_start is a valid block from pop_free_block; data
                // begins after the 8-byte overhead and spans block_size - 8 >= len.
                return Ok(unsafe {
                    BStackSlice::from_raw_parts(self, block_start + Self::OVERHEAD, len)
                });
            }
            let block_start = self.stack.extend(self.block_size)?;
            self.write_overhead(block_start, Self::IN_USE_BIT | 1)?;
            // SAFETY: block_start from a fresh, zeroed tail extension of block_size bytes.
            return Ok(unsafe {
                BStackSlice::from_raw_parts(self, block_start + Self::OVERHEAD, len)
            });
        }

        let total = num_blocks.checked_mul(self.block_size).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "allocation size overflows u64")
        })?;
        let block_start = self.stack.extend(total)?;
        self.write_overhead(block_start, Self::IN_USE_BIT | num_blocks)?;
        // SAFETY: block_start from a fresh, zeroed tail extension of num_blocks * block_size bytes.
        Ok(unsafe { BStackSlice::from_raw_parts(self, block_start + Self::OVERHEAD, len) })
    }

    /// Release the region described by `slice`.
    ///
    /// Reads the overhead at the block start (`slice.start() − 8`). If the
    /// high bit is clear the block is already free and a double-free error is
    /// returned without touching any list. A multi-block allocation at the tail
    /// is reclaimed with a single [`BStack::discard`]; otherwise every block is
    /// prepended to the free list.
    fn dealloc(&self, slice: BStackSlice<'_, Self>) -> io::Result<()> {
        if slice.is_empty() && slice.start() == 0 {
            return Ok(());
        }

        let block_start = slice.start().checked_sub(Self::OVERHEAD).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "slice start is below the overhead prefix; not a valid allocation",
            )
        })?;
        let overhead = self.read_overhead(block_start)?;
        if overhead & Self::IN_USE_BIT == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("double free detected: block at {block_start} is not marked in use"),
            ));
        }
        let num_blocks = overhead & Self::BLOCKS_MASK;
        if num_blocks == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "in-use block records a zero block count; metadata corrupt",
            ));
        }

        let backing = num_blocks.checked_mul(self.block_size).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "deallocation size overflows u64",
            )
        })?;
        let current_tail = self.stack.len()?;
        let slice_end = block_start.checked_add(backing).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "deallocation end offset overflows u64",
            )
        })?;

        if num_blocks > 1 && slice_end == current_tail {
            return self.stack.discard(backing);
        }

        self.push_free_blocks(block_start, num_blocks)
    }

    /// Resize the region described by `slice` to `new_len` bytes.
    ///
    /// # Resize strategies
    ///
    /// | Case | Strategy |
    /// |------|----------|
    /// | Same block count | Adjust visible length, zeroing newly-exposed bytes on grow |
    /// | Slice at tail | Extend or discard the tail and update the overhead count |
    /// | Shrink, non-tail | Recycle excess blocks into the free list, then shrink the overhead |
    /// | Grow, non-tail | Allocate a fresh region, copy, release the old |
    fn realloc<'a>(
        &'a self,
        slice: BStackSlice<'a, Self>,
        new_len: u64,
    ) -> io::Result<BStackSlice<'a, Self>> {
        if slice.is_empty() && slice.start() == 0 {
            return self.alloc(new_len);
        }
        if new_len == 0 {
            self.dealloc(slice)?;
            return Ok(BStackSlice::empty(self));
        }
        if new_len == slice.len() {
            return Ok(slice);
        }

        let block_start = slice.start().checked_sub(Self::OVERHEAD).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "slice start is below the overhead prefix; not a valid allocation",
            )
        })?;
        let overhead = self.read_overhead(block_start)?;
        if overhead & Self::IN_USE_BIT == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("realloc of a freed or invalid block at {block_start}"),
            ));
        }
        let old_n = overhead & Self::BLOCKS_MASK;
        let new_n = self.blocks_needed(new_len)?;

        if old_n == new_n {
            // Same backing blocks: zero newly-exposed bytes then adjust length.
            if new_len > slice.len() {
                self.stack.zero(slice.end(), new_len - slice.len())?;
            }
            // SAFETY: new_len still fits within the same block-aligned region.
            return Ok(unsafe { BStackSlice::from_raw_parts(self, slice.start(), new_len) });
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
        let current_tail = self.stack.len()?;
        let is_tail = block_start.checked_add(old_backing).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "tail check overflows u64")
        })? == current_tail;

        if is_tail {
            if new_n > old_n {
                self.stack.extend(new_backing - old_backing)?;
                if new_len > slice.len() {
                    self.stack.zero(slice.end(), new_len - slice.len())?;
                }
                self.write_overhead(block_start, Self::IN_USE_BIT | new_n)?;
            } else {
                // Shrink the overhead first: a crash before the discard leaves an
                // orphaned (but safely unreferenced) tail region rather than an
                // overhead that claims more blocks than the file contains.
                self.write_overhead(block_start, Self::IN_USE_BIT | new_n)?;
                self.stack.discard(old_backing - new_backing)?;
            }
            // SAFETY: slice extended or shrunk in place at the tail.
            return Ok(unsafe { BStackSlice::from_raw_parts(self, slice.start(), new_len) });
        }

        if new_n < old_n {
            // Shrink non-tail: recycle the excess blocks into the free list.
            //
            // Ordering matters. The excess block prefixes (overhead = 0) are
            // written first, while the still-live first block hides them, then
            // the first block's count is shrunk (the commit point), and only
            // then is free_head repointed. A crash before the commit leaves the
            // original allocation intact; a crash after it leaks the excess.
            let excess_start = block_start
                .checked_add(new_n.checked_mul(self.block_size).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "free start multiplication overflows u64",
                    )
                })?)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "free start overflows u64")
                })?;
            self.write_free_run(excess_start, old_n - new_n)?;
            self.write_overhead(block_start, Self::IN_USE_BIT | new_n)?;
            self.stack
                .set(Self::FREE_HEAD_OFFSET, excess_start.to_le_bytes())?;
            // SAFETY: new_len fits within the first new_n retained blocks.
            return Ok(unsafe { BStackSlice::from_raw_parts(self, slice.start(), new_len) });
        }

        // Grow non-tail: allocate a fresh region, copy data, release the old.
        let new_slice = self.alloc(new_len)?;
        let data = slice.read()?;
        new_slice.write(&data)?;
        self.dealloc(slice)?;
        Ok(new_slice)
    }
}

#[cfg(all(test, feature = "set"))]
mod tests {
    use super::CheckedSlabBStackAllocator;
    use crate::BStack;
    use crate::alloc::BStackAllocator;
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
        std::env::temp_dir().join(format!("bstack_checked_slab_{pid}_{id}.bin"))
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
        let alloc = CheckedSlabBStackAllocator::new(stack, 16).unwrap();
        assert_eq!(alloc.block_size(), 16);
        // ARENA_START = OFFSET_SIZE(24) + HEADER_SIZE(24) = 48
        assert_eq!(alloc.stack().len().unwrap(), 48);
    }

    #[test]
    fn new_rejects_block_size_below_minimum() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let err = CheckedSlabBStackAllocator::new(stack, 15).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn new_rejects_nonempty_stack() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        stack.push(b"data").unwrap();
        let err = CheckedSlabBStackAllocator::new(stack, 16).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    // ── open() ────────────────────────────────────────────────────────────────

    #[test]
    fn open_rejects_empty_stack() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let err = CheckedSlabBStackAllocator::open(stack).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn open_rejects_stack_too_short() {
        let (stack, path) = empty_stack();
        let _g = Guard(path.clone());
        stack.push([0u8; 24]).unwrap(); // only 24 bytes, need >= 48
        drop(stack);
        let err = CheckedSlabBStackAllocator::open(BStack::open(&path).unwrap()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn open_rejects_bad_magic() {
        let (stack, path) = empty_stack();
        let _g = Guard(path.clone());
        stack.push([0u8; 48]).unwrap(); // 48 bytes of zeros — no ALCK magic
        drop(stack);
        let err = CheckedSlabBStackAllocator::open(BStack::open(&path).unwrap()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn open_rejects_invalid_stored_block_size() {
        let (stack, path) = empty_stack();
        let _g = Guard(path.clone());
        // Valid magic but block_size = 8 (< MIN_BLOCK_SIZE = 16).
        let mut hdr = [0u8; 48];
        hdr[24..32].copy_from_slice(b"ALCK\x00\x01\x00\x00");
        hdr[32..40].copy_from_slice(&8u64.to_le_bytes());
        // free_head at [40..48] stays 0 (SENTINEL)
        stack.push(hdr).unwrap();
        drop(stack);
        let err = CheckedSlabBStackAllocator::open(BStack::open(&path).unwrap()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn open_rejects_misaligned_tail() {
        let (stack, path) = empty_stack();
        let _g = Guard(path.clone());
        CheckedSlabBStackAllocator::new(stack, 16).unwrap();
        let reopen = BStack::open(&path).unwrap();
        reopen.extend(1).unwrap();
        drop(reopen);
        let err = CheckedSlabBStackAllocator::open(BStack::open(&path).unwrap()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn open_succeeds_and_restores_block_size() {
        let (stack, path) = empty_stack();
        let _g = Guard(path.clone());
        CheckedSlabBStackAllocator::new(stack, 32).unwrap();
        let alloc = CheckedSlabBStackAllocator::open(BStack::open(&path).unwrap()).unwrap();
        assert_eq!(alloc.block_size(), 32);
    }

    // ── allocation behaviour ──────────────────────────────────────────────────

    #[test]
    fn zero_alloc_returns_empty_slice() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = CheckedSlabBStackAllocator::new(stack, 16).unwrap();
        let s = alloc.alloc(0).unwrap();
        assert!(s.is_empty());
    }

    #[test]
    fn dealloc_pushes_to_free_list_and_next_alloc_reuses_block() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = CheckedSlabBStackAllocator::new(stack, 16).unwrap();

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
        let alloc = CheckedSlabBStackAllocator::new(stack, 16).unwrap();

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
        let alloc = CheckedSlabBStackAllocator::new(stack, 16).unwrap();

        // 17 bytes needs ceil((17 + 8) / 16) = 2 blocks (32 bytes backing).
        let s = alloc.alloc(17).unwrap();
        let tail_before = alloc.stack().len().unwrap();
        assert_eq!(tail_before, 48 + 32);

        alloc.dealloc(s).unwrap();
        assert_eq!(alloc.stack().len().unwrap(), 48);
    }

    #[test]
    fn double_free_returns_error() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = CheckedSlabBStackAllocator::new(stack, 16).unwrap();

        let s = alloc.alloc(8).unwrap();
        let s_copy = s; // BStackSlice is Copy
        alloc.dealloc(s).unwrap();
        let err = alloc.dealloc(s_copy).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn write_and_read_round_trip() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = CheckedSlabBStackAllocator::new(stack, 24).unwrap();
        let s = alloc.alloc(12).unwrap();
        s.write(b"hello world!").unwrap();
        assert_eq!(s.read().unwrap(), b"hello world!");
    }

    #[test]
    fn data_survives_reopen() {
        let (stack, path) = empty_stack();
        let _g = Guard(path.clone());
        let alloc = CheckedSlabBStackAllocator::new(stack, 16).unwrap();
        let s = alloc.alloc(5).unwrap();
        let offset = s.start();
        s.write(b"hello").unwrap();
        drop(alloc);

        let alloc2 = CheckedSlabBStackAllocator::open(BStack::open(&path).unwrap()).unwrap();
        let s2 = unsafe { crate::alloc::BStackSlice::from_raw_parts(&alloc2, offset, 5) };
        assert_eq!(s2.read().unwrap(), b"hello");
    }

    #[test]
    fn multiblock_alloc_round_trip_and_free_reuse() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = CheckedSlabBStackAllocator::new(stack, 16).unwrap();

        // 40 bytes needs ceil((40 + 8) / 16) = 3 blocks.
        let s = alloc.alloc(40).unwrap();
        let payload: Vec<u8> = (0..40u8).collect();
        s.write(&payload).unwrap();
        assert_eq!(s.read().unwrap(), payload);

        // Free, then a single-block alloc should reuse the first freed block.
        let start = s.start();
        alloc.dealloc(s).unwrap();
        let reused = alloc.alloc(8).unwrap();
        assert_eq!(reused.start(), start);
    }

    // ── realloc ───────────────────────────────────────────────────────────────

    #[test]
    fn realloc_same_block_count_grows_in_place_and_zeroes() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        // block_size 32 -> 24 usable per block.
        let alloc = CheckedSlabBStackAllocator::new(stack, 32).unwrap();
        let s = alloc.alloc(8).unwrap();
        s.write(b"abcdefgh").unwrap();
        let s2 = alloc.realloc(s, 16).unwrap();
        assert_eq!(s2.start(), s.start());
        let data = s2.read().unwrap();
        assert_eq!(&data[..8], b"abcdefgh");
        assert_eq!(&data[8..], &[0u8; 8]); // newly-exposed bytes are zeroed
    }

    #[test]
    fn realloc_grow_preserves_data_across_blocks() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = CheckedSlabBStackAllocator::new(stack, 16).unwrap();
        let s = alloc.alloc(8).unwrap();
        s.write(b"abcdefgh").unwrap();
        // Grow to 40 bytes -> 3 blocks.
        let s2 = alloc.realloc(s, 40).unwrap();
        assert_eq!(s2.len(), 40);
        let data = s2.read().unwrap();
        assert_eq!(&data[..8], b"abcdefgh");
        assert_eq!(&data[8..], &[0u8; 32]);
    }

    #[test]
    fn realloc_shrink_non_tail_recycles_excess() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = CheckedSlabBStackAllocator::new(stack, 16).unwrap();

        // Allocate a 3-block region, then a guard block so the first is non-tail.
        let s = alloc.alloc(40).unwrap(); // 3 blocks
        let payload: Vec<u8> = (0..40u8).collect();
        s.write(&payload).unwrap();
        let _guard_block = alloc.alloc(8).unwrap();

        // Shrink to a single block; the two freed blocks become reusable.
        let s2 = alloc.realloc(s, 8).unwrap();
        assert_eq!(s2.start(), s.start());
        assert_eq!(s2.read().unwrap(), &payload[..8]);

        // The recycled blocks are now served from the free list.
        let r = alloc.alloc(8).unwrap();
        assert_eq!(r.start(), s.start() + 16);
    }

    #[test]
    fn realloc_to_zero_deallocs() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = CheckedSlabBStackAllocator::new(stack, 16).unwrap();
        let s = alloc.alloc(8).unwrap();
        let start = s.start();
        let s2 = alloc.realloc(s, 0).unwrap();
        assert!(s2.is_empty());
        // The block was freed and is reused on the next alloc.
        let r = alloc.alloc(8).unwrap();
        assert_eq!(r.start(), start);
    }

    #[test]
    fn realloc_from_empty_allocates() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = CheckedSlabBStackAllocator::new(stack, 16).unwrap();
        let empty = alloc.alloc(0).unwrap();
        let s = alloc.realloc(empty, 8).unwrap();
        assert_eq!(s.len(), 8);
        assert!(!s.is_empty());
    }
}
