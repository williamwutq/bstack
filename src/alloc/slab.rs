//! Fixed-block slab allocator for [`BStack`]-backed storage.
//!
//! Provides [`SlabBStackAllocator`], which implements [`BStackAllocator`] with
//! O(1) alloc and dealloc by keeping all blocks the same size and tracking
//! freed blocks in an intrusive singly-linked free list.

use super::{BStackAllocator, BStackSlice};
use crate::BStack;
use core::{cell::Cell, marker::PhantomData, num::NonZeroU64};
use std::{fmt, io};

#[cfg(feature = "set")]
const ALSL_MAGIC: [u8; 8] = *b"ALSL\x00\x01\x00\x00";

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
/// # Thread safety
///
/// `SlabBStackAllocator` is only `Send` but not `Sync`: concurrent access to
/// the same allocator must be externally synchronized. This is because the
/// free list head is cached in memory and updated non-atomically, so concurrent
/// modifications could lead to lost updates and list corruption.
///
/// # Feature flags
///
/// Requires both the `alloc` and `set` Cargo features:
///
/// ```toml
/// bstack = { version = "0.1", features = ["alloc", "set"] }
/// ```
#[cfg(feature = "set")]
pub struct SlabBStackAllocator {
    stack: BStack,
    /// Cached from the on-disk header; fixed for the lifetime of the allocator.
    block_size: u64,
    // Mark as !Sync to prevent concurrent access to the free list.
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

    /// Open or initialise a `SlabBStackAllocator` over `stack`.
    ///
    /// * **Empty stack** — writes the 48-byte allocator header (24 reserved
    ///   bytes, magic, `block_size`, and `free_head = 0`) using a single
    ///   `BStack::push` and returns a ready allocator. `block_size` must be
    ///   `>= 8`.
    /// * **Non-empty stack** — validates the `ALSL 0.1.x` magic prefix and
    ///   checks that the stored `block_size` matches the provided value.
    ///
    /// # Errors
    ///
    /// * [`io::ErrorKind::InvalidInput`] — `block_size < 8` on a new stack
    ///   (returns an error rather than panicking so callers can recover or
    ///   report the constraint programmatically).
    /// * [`io::ErrorKind::InvalidData`] — wrong magic, invalid stored
    ///   `block_size`, or mismatch between the stored and provided value.
    /// * Any [`io::Error`] propagated from the underlying [`BStack`] operations.
    pub fn new(stack: BStack, block_size: u64) -> io::Result<Self> {
        if stack.is_empty()? {
            if block_size < Self::MIN_BLOCK_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "block_size ({block_size}) must be >= {}",
                        Self::MIN_BLOCK_SIZE
                    ),
                ));
            }
            let mut hdr = [0u8; Self::ARENA_START as usize];
            let off = Self::OFFSET_SIZE as usize;
            hdr[off..off + 8].copy_from_slice(&ALSL_MAGIC);
            hdr[off + 8..off + 16].copy_from_slice(&block_size.to_le_bytes());
            // free_head at off+16 remains 0 (SENTINEL)
            stack.push(hdr)?;
            return Ok(Self {
                stack,
                block_size,
                _not_sync: PhantomData,
            });
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
        if block_size != stored_block_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("block_size mismatch: stored {stored_block_size}, provided {block_size}"),
            ));
        }

        Ok(Self {
            stack,
            block_size: stored_block_size,
            _not_sync: PhantomData,
        })
    }

    /// Return the `block_size` this allocator was created with.
    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    /// Pop the head block from the free list. Returns its payload offset, or `None`.
    fn pop_free_block(&self) -> io::Result<Option<NonZeroU64>> {
        let head = u64::from_le_bytes(read_bstack!(self.stack, Self::FREE_HEAD_OFFSET => u64));
        if head == Self::SENTINEL {
            return Ok(None);
        }
        self.stack.set(
            Self::FREE_HEAD_OFFSET,
            read_bstack!(self.stack, head => u64),
        )?;
        // Zeroing. If this fail, the free list is still consistent and the popped block is leaked
        // but not corrupted, so we can ignore the error.
        let _ = self.stack.zero(head, self.block_size);
        // SAFETY: head is not zero since we checked for the SENTINEL case above, so it is a valid NonZeroU64
        Ok(Some(head.try_into().unwrap()))
    }

    /// Prepend the block at `block_start` to the free list.
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
    /// Uses exactly 3 IO calls regardless of `count`: one read of `free_head`,
    /// one bulk write of all next-pointers into the freed region, and one write
    /// of the new `free_head`. Crash behaviour matches `push_free_block`: a
    /// crash after the bulk write but before the `free_head` update leaks the
    /// entire batch rather than corrupting the list.
    ///
    /// Requires that count * block_size does not overflow u64 and
    /// first_block + count * block_size does not overflow u64 and is a valid offset
    /// on the stack by the caller.
    fn push_free_blocks(&self, first_block: u64, count: u64) -> io::Result<()> {
        if count == 0 {
            return Ok(());
        }
        if count == 1 {
            return self.push_free_block(first_block);
        }
        let old_head = read_bstack!(self.stack, Self::FREE_HEAD_OFFSET => u64);
        let buf_size = (count * self.block_size) as usize;
        let mut buf = vec![0u8; buf_size];
        for i in 0..count - 1 {
            let next = first_block + (i + 1) * self.block_size;
            let off = (i * self.block_size) as usize;
            buf[off..off + 8].copy_from_slice(&next.to_le_bytes());
        }
        let last_off = ((count - 1) * self.block_size) as usize;
        buf[last_off..last_off + 8].copy_from_slice(&old_head);
        self.stack.set(first_block, buf)?;
        self.stack
            .set(Self::FREE_HEAD_OFFSET, first_block.to_le_bytes())
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
    type Allocated<'a> = BStackSlice<'a, Self>;

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
    /// | slab, free list hit | 2 (`get_into` + `set`) | crash leaves block at list head |
    /// | slab, tail extend | 1 (`extend`) | crash-safe by inheritance |
    /// | oversized | 1 (`extend`) | crash-safe by inheritance |
    fn alloc(&self, len: u64) -> io::Result<BStackSlice<'_, Self>> {
        if len == 0 {
            return Ok(BStackSlice::empty(self));
        }

        if len <= self.block_size {
            if let Some(block) = self.pop_free_block()? {
                // SAFETY: block is a valid block_size region from pop_free_block
                return Ok(unsafe { BStackSlice::from_raw_parts(self, block.into(), len) });
            }
            let offset = self.stack.extend(self.block_size)?;
            // SAFETY: offset from a fresh tail extension of block_size bytes
            return Ok(unsafe { BStackSlice::from_raw_parts(self, offset, len) });
        }

        let n = len.div_ceil(self.block_size);
        let total = n.checked_mul(self.block_size).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "allocation size overflows u64")
        })?;
        let offset = self.stack.extend(total)?;
        // SAFETY: offset from a fresh tail extension of n * block_size bytes
        Ok(unsafe { BStackSlice::from_raw_parts(self, offset, len) })
    }

    /// Release the region described by `slice`.
    ///
    /// # Crash consistency
    ///
    /// | Path | Calls | Safety |
    /// |------|-------|--------|
    /// | null slice | 0 | trivially safe |
    /// | oversized tail | 1 (`discard`) | crash-safe by inheritance |
    /// | slab / oversized non-tail | 2 per block (`set` + `set`) | crash leaks the block being added |
    fn dealloc(&self, slice: BStackSlice<'_, Self>) -> io::Result<()> {
        if slice.is_empty() && slice.start() == Self::SENTINEL {
            return Ok(());
        }

        let n_blocks = self.blocks_needed(slice.len());
        let backing_size = n_blocks * self.block_size;
        let current_tail = self.stack.len()?;

        if slice.len() > self.block_size && slice.start() + backing_size == current_tail {
            return self.stack.discard(backing_size);
        }

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
        slice: BStackSlice<'a, Self>,
        new_len: u64,
    ) -> io::Result<BStackSlice<'a, Self>> {
        if slice.is_empty() && slice.start() == Self::SENTINEL {
            return self.alloc(new_len);
        }
        if new_len == 0 {
            self.dealloc(slice)?;
            return Ok(BStackSlice::empty(self));
        }
        if new_len == slice.len() {
            return Ok(slice);
        }

        let old_n = self.blocks_needed(slice.len());
        let new_n = self.blocks_needed(new_len);

        if old_n == new_n {
            // Same backing blocks: zero newly-exposed bytes then adjust visible length.
            // Interget safety: old and new slice length are both valid u64 values and they could not differ
            // by more than block_size by bytes, so new_len - slice.len() will not overflow.
            if new_len > slice.len() {
                self.stack.zero(slice.end(), new_len - slice.len())?;
            }
            // SAFETY: new_len still fits within the same block_size-aligned region
            return Ok(unsafe { BStackSlice::from_raw_parts(self, slice.start(), new_len) });
        }

        let old_backing = old_n * self.block_size;
        let new_backing = new_n * self.block_size;
        let current_tail = self.stack.len()?;
        let is_tail = slice.start() + old_backing == current_tail;

        if is_tail {
            if new_n > old_n {
                self.stack.extend(new_backing - old_backing)?;
            } else {
                self.stack.discard(old_backing - new_backing)?;
            }
            if new_len > slice.len() {
                self.stack.zero(slice.end(), new_len - slice.len())?;
            }
            // SAFETY: slice extended or shrunk in place at the tail
            return Ok(unsafe { BStackSlice::from_raw_parts(self, slice.start(), new_len) });
        }

        if new_n < old_n {
            // Shrink non-tail: recycle excess blocks into the free list.
            self.push_free_blocks(slice.start() + new_n * self.block_size, old_n - new_n)?;
            // SAFETY: new_len fits within the first new_n retained blocks
            return Ok(unsafe { BStackSlice::from_raw_parts(self, slice.start(), new_len) });
        }

        // Grow non-tail: allocate a new region, copy data, release old.
        // TODO: since block is too big, we definitely need a tail grow and there is no need to call
        // alloc since we can just extend the tail directly.
        let new_slice = self.alloc(new_len)?;
        let new_start = new_slice.start();
        if !slice.is_empty() {
            let data = self.stack.get(slice.start(), slice.start() + slice.len())?;
            self.stack.set(new_start, &data)?;
        }
        self.dealloc(slice)?;
        // SAFETY: new_start from a fresh allocation covering new_len bytes
        Ok(unsafe { BStackSlice::from_raw_parts(self, new_start, new_len) })
    }
}
