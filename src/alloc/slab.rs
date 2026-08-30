//! Fixed-block slab allocator for [`BStack`]-backed storage.
//!
//! Provides [`SlabBStackAllocator`], which implements [`BStackAllocator`] with
//! O(1) alloc and dealloc by keeping all blocks the same size and tracking
//! freed blocks in an intrusive singly-linked free list.

use super::{
    BStackAllocError, BStackAllocator, BStackOwnedSlice, BStackUninitAllocator, ensure_own_handle,
};
#[cfg(feature = "atomic")]
use super::{BStackBulkAllocError, BStackBulkAllocator, ensure_own_handles};
use crate::BStack;
#[cfg(feature = "atomic")]
use crate::BStackGenOp;
#[cfg(feature = "atomic")]
use crate::{bstack_unsafe_reborrow, bstack_unsafe_reborrow_mut};
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
/// | `alloc_bulk` *(needs `atomic`)*      | Partial         | Yes (`process_gen`+`extend`+`set_batched`) | Detached/extended blocks leaked |
/// | `dealloc_bulk` *(needs `atomic`)*    | Partial         | Yes (`set_batched`+`cross_exchange`) | Entire freed batch leaked |
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
    #[inline]
    #[must_use]
    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    /// Pop the head block from the free list. Returns its payload offset, or `None`.
    ///
    /// `init` is the caller's zero-initialisation guarantee: when set, the
    /// popped block is scrubbed before it is returned; when clear, the scrub is
    /// skipped and the block keeps whatever its previous occupant left in it
    /// (including the stale free-list `next` pointer in its first 8 bytes).
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
    fn pop_free_block(&self, init: bool) -> io::Result<Option<NonZeroU64>> {
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
                    buf: bstack_unsafe_reborrow_mut!(&mut head_buf[..]),
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
                            buf: bstack_unsafe_reborrow_mut!(&mut next_buf[..]),
                        })
                    }
                }
                // Step 2: advance free_head to the popped block's next pointer,
                // still under the lock acquired for step 0's read.
                2 => Some(BStackGenOp::Write {
                    offset: Self::FREE_HEAD_OFFSET,
                    // SAFETY: `next_buf` outlives this `process_gen` call.
                    data: bstack_unsafe_reborrow!(&next_buf[..]),
                }),
                _ => None,
            };
            step += 1;
            op
        })?;

        let Some(head) = popped else {
            return Ok(None);
        };
        if init {
            self.stack.zero(head, self.block_size)?;
        }
        // SAFETY: head is not zero since we checked for the SENTINEL case above, so it is a valid NonZeroU64
        Ok(Some(head.try_into().unwrap()))
    }

    /// Pop the head block from the free list. Returns its payload offset, or `None`.
    ///
    /// See the `atomic` variant for the meaning of `init`.
    #[cfg(not(feature = "atomic"))]
    fn pop_free_block(&self, init: bool) -> io::Result<Option<NonZeroU64>> {
        let head = u64::from_le_bytes(read_bstack!(self.stack, Self::FREE_HEAD_OFFSET => u64));
        if head == Self::SENTINEL {
            return Ok(None);
        }
        self.stack.set(
            Self::FREE_HEAD_OFFSET,
            read_bstack!(self.stack, head => u64),
        )?;
        if init {
            self.stack.zero(head, self.block_size)?;
        }
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

    /// Shared body of [`alloc`](BStackAllocator::alloc) and
    /// [`alloc_uninit`](BStackUninitAllocator::alloc_uninit).
    ///
    /// The two differ only in the free-list-reuse path: `init` set scrubs the
    /// popped block, `init` clear skips that whole [`BStack::zero`] call.
    /// The tail-extend paths are identical — [`BStack::extend`] is a `set_len` on
    /// a sparse file, so their zero-fill is already free.
    fn alloc_impl(&self, len: u64, init: bool) -> io::Result<BStackOwnedSlice<'_, Self>> {
        if len == 0 {
            return Ok(BStackOwnedSlice::empty(self));
        }

        if len <= self.block_size {
            if let Some(block) = self.pop_free_block(init)? {
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

    /// Shared body of [`realloc`](BStackAllocator::realloc) and
    /// [`realloc_uninit`](BStackUninitAllocator::realloc_uninit).
    ///
    /// A clear `init` drops every [`BStack::zero`] of newly-exposed bytes (all
    /// of which sit inside blocks the caller already owns, so nothing but the
    /// caller-facing zero guarantee depends on them) and, on the non-tail grow,
    /// writes only the surviving prefix instead of a full-size block image.
    /// Block bookkeeping — the tail extend/discard and the free-list splices —
    /// is identical in both modes.
    fn realloc_impl<'a>(
        &'a self,
        slice: BStackOwnedSlice<'a, Self>,
        new_len: u64,
        init: bool,
    ) -> Result<BStackOwnedSlice<'a, Self>, BStackAllocError<'a, Self>> {
        let slice = ensure_own_handle(self, slice, "SlabBStackAllocator::realloc")?;
        if slice.is_empty() && slice.start() == Self::SENTINEL {
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
        if new_len == old_len {
            // SAFETY: unchanged region.
            return Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, start, old_len) });
        }

        // The surviving allocation to hand back on failure. Starts as the
        // original block; updated once a move commits a new region or a shrink
        // commits the retained region (both distinct from any blocks being
        // freed, so the handle is always safe to return).
        let mut recovered = (start, old_len);
        let result = (|| -> io::Result<BStackOwnedSlice<'a, Self>> {
            let old_n = self.blocks_needed(slice.len());
            let new_n = self.blocks_needed(new_len);

            if old_n == new_n {
                // Same backing blocks: zero newly-exposed bytes then adjust visible length.
                // Integer safety: old and new slice length are both valid u64 values and they could not differ
                // by more than block_size by bytes, so new_len - slice.len() will not overflow.
                if new_len > slice.len() && init {
                    self.stack.zero(slice.end(), new_len - slice.len())?;
                }
                // SAFETY: new_len still fits within the same block_size-aligned region
                return Ok(unsafe {
                    BStackOwnedSlice::from_raw_parts(self, slice.start(), new_len)
                });
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
                    if new_len > slice.len() && init {
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
                    if new_len > slice.len() && init {
                        self.stack.zero(slice.end(), new_len - slice.len())?;
                    }
                    // SAFETY: slice extended in place at the tail
                    return Ok(unsafe {
                        BStackOwnedSlice::from_raw_parts(self, slice.start(), new_len)
                    });
                }

                // Grow non-tail: copy data into a fresh region, then free the old blocks.
                // get_into and push need no lock; push_free_blocks mutates the free list.
                let old_visible_len = usize::try_from(slice.len()).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "existing allocation too large for this platform",
                    )
                })?;
                let new_ptr = if init {
                    // The whole backing region is written, so it must fit in memory.
                    let buf_len = usize::try_from(new_backing).map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "reallocation too large for this platform",
                        )
                    })?;
                    let mut data_buf = vec![0u8; buf_len];
                    self.stack
                        .get_into(slice.start(), &mut data_buf[..old_visible_len])?;
                    self.stack.push(data_buf)?
                } else {
                    // Only the surviving prefix has to be written: `extend_sparse`
                    // realises the whole backing region with one `set_len`, so the
                    // bytes past the prefix cost no write I/O. They read back as
                    // zero, which is a permitted "unspecified" value, and no
                    // full-size buffer is needed.
                    let mut data_buf = vec![0u8; old_visible_len];
                    self.stack.get_into(slice.start(), &mut data_buf)?;
                    self.stack.extend_sparse(data_buf, new_backing)?
                };
                // New region committed and populated; it is now the survivor, so a
                // failure freeing the old blocks returns the new region instead.
                recovered = (new_ptr, new_len);
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
                return Ok(unsafe {
                    BStackOwnedSlice::from_raw_parts(self, slice.start(), new_len)
                });
            }

            #[cfg(not(feature = "atomic"))]
            if checked_len == self.stack.len()? {
                self.stack.discard(old_backing - new_backing)?;
                // SAFETY: slice shrunk in place at the tail
                return Ok(unsafe {
                    BStackOwnedSlice::from_raw_parts(self, slice.start(), new_len)
                });
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
            // The first new_n blocks are retained regardless of the outcome, so the
            // resized region is the survivor if freeing the excess blocks fails.
            recovered = (slice.start(), new_len);
            self.push_free_blocks(free_start, old_n - new_n)?;
            // SAFETY: new_len fits within the first new_n retained blocks
            Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, slice.start(), new_len) })
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

    #[inline]
    fn stack(&self) -> &BStack {
        &self.stack
    }

    #[inline]
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
    #[inline]
    fn alloc(&self, len: u64) -> io::Result<BStackOwnedSlice<'_, Self>> {
        self.alloc_impl(len, true)
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
    fn dealloc<'a>(
        &'a self,
        slice: BStackOwnedSlice<'a, Self>,
    ) -> Result<(), BStackAllocError<'a, Self>> {
        let slice = ensure_own_handle(self, slice, "SlabBStackAllocator::dealloc")?;
        let start = slice.start();
        let len = slice.len();
        // Set once the caller's blocks may have been partially freed, after
        // which returning the handle for retry would risk a double-free.
        let mut lost = false;
        let result = (|| -> io::Result<()> {
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

            // Not at tail (or single-block): push to the free list. This mutates
            // multiple block links, so a mid-way failure may leave the blocks
            // partially freed — the handle can no longer be safely returned.
            lost = true;
            self.push_free_blocks(slice.start(), n_blocks)
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
    #[inline]
    fn realloc<'a>(
        &'a self,
        slice: BStackOwnedSlice<'a, Self>,
        new_len: u64,
    ) -> Result<BStackOwnedSlice<'a, Self>, BStackAllocError<'a, Self>> {
        self.realloc_impl(slice, new_len, true)
    }
}

/// Reusing a free-list block without scrubbing it.
///
/// A freed block keeps its previous occupant's bytes on disk — `dealloc` writes
/// only the intrusive `next` pointer into the block's first 8 bytes — so
/// [`alloc`](BStackAllocator::alloc) has to issue a whole-block
/// [`BStack::zero`] before handing a recycled block back.  That is an entire
/// extra durable sync per reused block, and pure waste for a caller that
/// overwrites the region immediately.
///
/// [`alloc_uninit`](BStackUninitAllocator::alloc_uninit) drops it: a free-list
/// hit becomes a single `process_gen` pop (`atomic`) or a read plus one `set`
/// (non-`atomic`) with no scrub at all, and the returned bytes hold the old
/// contents — including the stale free-list pointer in the first 8 bytes.  The
/// tail-extend and oversized paths are unchanged, since
/// [`BStack::extend`] zero-fills by `set_len` on a sparse file at no cost.
///
/// [`realloc_uninit`](BStackUninitAllocator::realloc_uninit) likewise drops the
/// `zero` of bytes newly exposed inside the existing backing blocks, and writes
/// only the surviving prefix on a non-tail grow ([`BStack::extend_sparse`]
/// rather than a full-size [`BStack::push`]).  All block bookkeeping — tail
/// extend/discard, free-list splices — is identical to
/// [`realloc`](BStackAllocator::realloc), so the crash-consistency table above
/// applies unchanged apart from the dropped `zero` calls.
#[cfg(feature = "set")]
impl BStackUninitAllocator for SlabBStackAllocator {
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

#[cfg(feature = "atomic")]
impl SlabBStackAllocator {
    /// Bulk free-list pop, used only by the [`alloc_bulk`](BStackBulkAllocator::alloc_bulk)
    /// extend path.
    ///
    /// Generalises [`pop_free_block`](SlabBStackAllocator::pop_free_block) to a
    /// run: chases up to `want` blocks along the singly-linked chain through the
    /// block bodies under **one** [`BStack::process_gen`] sequence (the chase is
    /// inherently sequential but happens in one locked critical section), then
    /// advances `free_head` once — past the last popped block — as the single
    /// terminating write. Returns the popped block offsets in pop order (head
    /// first); fewer than `want` if the list is exhausted first, and an empty
    /// `Vec` if the list is empty. The popped blocks are detached but **not**
    /// scrubbed; the caller stages any zero-fill separately, since by then each
    /// block is exclusively owned.
    fn pop_free_blocks_bulk(&self, want: usize) -> io::Result<Vec<u64>> {
        if want == 0 {
            return Ok(Vec::new());
        }
        let mut popped: Vec<u64> = Vec::with_capacity(want);
        let mut head_buf = [0u8; 8];
        // Holds the last-read node's next-pointer; on the terminating write it is
        // exactly the value `free_head` must advance to, so no separate buffer is
        // needed. Declared here so it outlives the `process_gen` borrow.
        let mut next_buf = [0u8; 8];

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
                    // Read the current free-list head.
                    St::ReadHead => {
                        st = St::ConsumeHead;
                        return Some(BStackGenOp::Read {
                            offset: Self::FREE_HEAD_OFFSET,
                            // SAFETY: `head_buf` outlives this `process_gen` call.
                            buf: bstack_unsafe_reborrow_mut!(&mut head_buf[..]),
                        });
                    }
                    St::ConsumeHead => {
                        let head = u64::from_le_bytes(head_buf);
                        if head == Self::SENTINEL {
                            // Empty list: end with no write, nothing popped.
                            return None;
                        }
                        st = St::ReadNode(head);
                    }
                    // Read the next-pointer of the block at `cursor`.
                    St::ReadNode(cursor) => {
                        st = St::ConsumeNode(cursor);
                        return Some(BStackGenOp::Read {
                            offset: cursor,
                            // SAFETY: `next_buf` outlives this `process_gen` call.
                            buf: bstack_unsafe_reborrow_mut!(&mut next_buf[..]),
                        });
                    }
                    St::ConsumeNode(cursor) => {
                        popped.push(cursor);
                        let next = u64::from_le_bytes(next_buf);
                        if popped.len() == want || next == Self::SENTINEL {
                            // Advance free_head to the last-read next-pointer
                            // (`next_buf` already holds those bytes), still under
                            // the lock acquired for the head read. `Write` ends
                            // the sequence.
                            st = St::Done;
                            return Some(BStackGenOp::Write {
                                offset: Self::FREE_HEAD_OFFSET,
                                // SAFETY: `next_buf` outlives this call and holds
                                // the next-pointer bytes to store.
                                data: bstack_unsafe_reborrow!(&next_buf[..]),
                            });
                        }
                        st = St::ReadNode(next);
                    }
                    // `Write` is terminal, so this is defensive only.
                    St::Done => return None,
                }
            }
        })?;

        Ok(popped)
    }

    /// No-extend fast path for [`alloc_bulk`](BStackBulkAllocator::alloc_bulk):
    /// serve an all-single-block request entirely from the free list in one
    /// crash-atomic step.
    ///
    /// One [`BStack::inplace_gen`] chases up to `singles` blocks and, **only if**
    /// the list can supply them all, advances `free_head` and scrubs every
    /// recycled block in that same commit — so the pop and the claim land
    /// together. Returns `Some(handles)` on success. If the list is too short it
    /// returns `None` having written nothing (the closure ends before any
    /// write), and the caller falls back to the extend path. Because the whole
    /// operation is one atomic commit, a failure or crash leaks nothing:
    /// `free_head` and every block are left exactly as they were.
    ///
    /// Only called when no request is oversized, so every `len > 0` needs exactly
    /// one block and `singles > 0`.
    fn alloc_bulk_from_freelist_atomic(
        &self,
        lengths: &[u64],
        singles: usize,
    ) -> io::Result<Option<Vec<BStackOwnedSlice<'_, Self>>>> {
        debug_assert!(singles > 0);
        let bs = self.block_size;
        let zero_block = vec![0u8; bs as usize];
        let mut popped: Vec<u64> = Vec::with_capacity(singles);
        let mut head_buf = [0u8; 8];
        let mut node_buf = [0u8; 8];
        let mut enough = false;

        #[derive(Clone, Copy)]
        enum St {
            ReadHead,
            ConsumeHead,
            ReadNode(u64),
            ConsumeNode(u64),
            WriteHead,
            WriteClaim(usize),
            Abort,
        }
        let mut st = St::ReadHead;

        self.stack.inplace_gen(|_prev| {
            loop {
                match st {
                    // ── Chase phase: reads only, so they see committed bytes. ──
                    St::ReadHead => {
                        st = St::ConsumeHead;
                        return Some(BStackGenOp::Read {
                            offset: Self::FREE_HEAD_OFFSET,
                            // SAFETY: `head_buf` outlives this `inplace_gen` call.
                            buf: bstack_unsafe_reborrow_mut!(&mut head_buf[..]),
                        });
                    }
                    St::ConsumeHead => {
                        let head = u64::from_le_bytes(head_buf);
                        if head == Self::SENTINEL {
                            // Empty list, cannot cover the request: abort.
                            st = St::Abort;
                        } else {
                            st = St::ReadNode(head);
                        }
                    }
                    St::ReadNode(cursor) => {
                        st = St::ConsumeNode(cursor);
                        return Some(BStackGenOp::Read {
                            offset: cursor,
                            // SAFETY: `node_buf` outlives this `inplace_gen` call.
                            buf: bstack_unsafe_reborrow_mut!(&mut node_buf[..]),
                        });
                    }
                    St::ConsumeNode(cursor) => {
                        popped.push(cursor);
                        let next = u64::from_le_bytes(node_buf);
                        if popped.len() == singles {
                            // Got them all: commit the pop + claims atomically.
                            enough = true;
                            st = St::WriteHead;
                        } else if next == Self::SENTINEL {
                            // List exhausted early: abort, extend path handles it.
                            st = St::Abort;
                        } else {
                            st = St::ReadNode(next);
                        }
                    }
                    // ── Write phase: reached only when the whole request is
                    // covered, so every write commits together. ──
                    St::WriteHead => {
                        st = St::WriteClaim(0);
                        return Some(BStackGenOp::Write {
                            offset: Self::FREE_HEAD_OFFSET,
                            // SAFETY: `node_buf` outlives this call; it holds the
                            // last-read next-pointer, and no read follows.
                            data: bstack_unsafe_reborrow!(&node_buf[..]),
                        });
                    }
                    // `i` indexes `popped`, which was filled during the chase, so
                    // every index below `popped.len()` is in bounds; the guard
                    // ends the sequence exactly once all blocks are scrubbed.
                    St::WriteClaim(i) => {
                        let Some(&off) = popped.get(i) else {
                            return None; // commit
                        };
                        st = St::WriteClaim(i + 1);
                        return Some(BStackGenOp::Write {
                            offset: off,
                            // SAFETY: `zero_block` outlives this call and is never
                            // mutated; every write aliases it read-only.
                            data: bstack_unsafe_reborrow!(&zero_block[..]),
                        });
                    }
                    // Nothing written yet, so the sequence commits nothing.
                    St::Abort => return None,
                }
            }
        })?;

        if !enough {
            return Ok(None);
        }
        // Map the popped blocks onto the requests in order (all single-block).
        let mut popped_iter = popped.into_iter();
        let mut result = Vec::with_capacity(lengths.len());
        for &len in lengths {
            if len == 0 {
                result.push(BStackOwnedSlice::empty(self));
            } else {
                let block_start = popped_iter.next().unwrap();
                // SAFETY: a live `block_size`-aligned block just popped and
                // scrubbed; `len ≤ block_size`.
                result.push(unsafe { BStackOwnedSlice::from_raw_parts(self, block_start, len) });
            }
        }
        Ok(Some(result))
    }

    /// Scrub each block in `blocks` to zero in one crash-atomic multi-write.
    ///
    /// Streaming the writes through [`BStack::inplace_gen`] lets every block reuse
    /// a single zero buffer, so peak memory is one `block_size` buffer regardless
    /// of how many blocks are scrubbed.
    fn scrub_blocks(&self, blocks: &[u64]) -> io::Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }
        let zero_block = vec![0u8; self.block_size as usize];
        let mut i = 0usize;
        self.stack.inplace_gen(|_prev| {
            // `i` walks `blocks` (collected before this call), so `get` is in
            // bounds until it runs off the end, where `?` ends the sequence.
            let off = *blocks.get(i)?;
            i += 1;
            Some(BStackGenOp::Write {
                offset: off,
                // SAFETY: `zero_block` outlives this call and is never mutated.
                data: bstack_unsafe_reborrow!(&zero_block[..]),
            })
        })
    }

    /// Splice `blocks` (in any address order) onto the free list as one chain.
    ///
    /// The chain `block[0] -> block[1] -> ... -> block[k-1]` is written entirely
    /// into the block bodies with one [`BStack::set_batched`] (unreachable from
    /// `free_head` until the splice, so staging touches nothing live), then the
    /// whole run is spliced with one atomic [`BStack::cross_exchange`] — the same
    /// discipline as [`push_free_blocks`](SlabBStackAllocator::push_free_blocks),
    /// so a concurrent push/pop cannot be lost. Used by
    /// [`dealloc_bulk`](BStackBulkAllocator::dealloc_bulk) and by the `alloc_bulk`
    /// rollback that returns popped blocks after a failed extend.
    fn splice_blocks_onto_free_list(&self, blocks: &[u64]) -> io::Result<()> {
        let k = blocks.len();
        if k == 0 {
            return Ok(());
        }
        // Thread the chain: the last block's next-pointer is seeded to the
        // placeholder `block[0]` that cross_exchange replaces with the old head.
        let mut batch: Vec<(u64, [u8; 8])> = Vec::with_capacity(k);
        for i in 0..k {
            let next = if i + 1 < k { blocks[i + 1] } else { blocks[0] };
            batch.push((blocks[i], next.to_le_bytes()));
        }
        self.stack.set_batched(batch)?;
        self.stack
            .cross_exchange(blocks[k - 1], Self::FREE_HEAD_OFFSET, 8)
    }
}

/// [`SlabBStackAllocator`] batches whole runs of fixed-size blocks.
///
/// A slab block is `block_size` bytes and every allocation is a whole number of
/// blocks, so a bulk request is served block-by-block and — unlike
/// [`GhostTreeBstackAllocator`](super::ghost_tree::GhostTreeBstackAllocator) —
/// the result is **not** one contiguous block sliced up: each slab block must
/// stay independently deallocatable, so per-request handles are returned.
///
/// Requires the `atomic` feature: both methods commit through
/// [`BStack::inplace_gen`] / [`BStack::set_batched`] / [`BStack::cross_exchange`],
/// which exist only with `atomic`. Without it, use the single-item
/// [`alloc`](BStackAllocator::alloc) / [`dealloc`](BStackAllocator::dealloc).
#[cfg(feature = "atomic")]
impl BStackBulkAllocator for SlabBStackAllocator {
    /// Allocate one independently-freeable region per requested length.
    ///
    /// When every request is single-block (`0 < len ≤ block_size`) and the free
    /// list can cover them all, the whole batch is served in **one** crash-atomic
    /// [`BStack::inplace_gen`] that pops the blocks and scrubs them together —
    /// nothing is leaked even on failure. Otherwise the free list supplies what
    /// it can (one [`process_gen`](BStack::process_gen) chase), the remainder
    /// (every oversized request plus any single-block overflow) comes from **one**
    /// [`BStack::extend`], and the recycled blocks are scrubbed in one
    /// [`BStack::inplace_gen`]; freshly extended blocks read back as zero for free
    /// (a sparse `set_len`). Zero-length requests yield the null sentinel slice.
    ///
    /// # Atomicity
    ///
    /// The all-free-list path is a single atomic commit — no window, no leak. On
    /// the extend path, if the `extend` or the scrub fails, the popped blocks are
    /// spliced back onto the free list and any fresh tail is discarded on a
    /// best-effort basis; only if that rollback itself fails are those blocks
    /// leaked (the free list stays consistent).
    fn alloc_bulk(
        &self,
        lengths: impl AsRef<[u64]>,
    ) -> Result<Vec<Self::Allocated<'_>>, Self::Error> {
        let lengths = lengths.as_ref();
        if lengths.is_empty() {
            return Ok(Vec::new());
        }
        let bs = self.block_size;

        // Per-request block counts, the single-block (free-list eligible) tally,
        // and whether any request needs a contiguous multi-block run.
        // `blocks_needed` returns 0 for a zero-length request.
        let mut counts: Vec<u64> = Vec::with_capacity(lengths.len());
        let mut total_blocks: u64 = 0;
        let mut singles: usize = 0;
        let mut oversized = false;
        for &len in lengths {
            let n = self.blocks_needed(len);
            total_blocks = total_blocks.checked_add(n).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "alloc_bulk: total block count overflows u64",
                )
            })?;
            if n == 1 {
                singles += 1;
            } else if n > 1 {
                oversized = true;
            }
            counts.push(n);
        }

        // All zero-length: null slices, no I/O.
        if total_blocks == 0 {
            return Ok(lengths
                .iter()
                .map(|_| BStackOwnedSlice::empty(self))
                .collect());
        }

        // Reject a request too large to ever back, before touching the free list.
        total_blocks.checked_mul(bs).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "alloc_bulk: total allocation size overflows u64",
            )
        })?;

        // Fast path: no oversized request, so try to serve the whole batch from
        // the free list in one atomic commit. `Some` means it fit; `None` means
        // the list was too short and nothing was written — fall through.
        if !oversized
            && let Some(result) = self.alloc_bulk_from_freelist_atomic(lengths, singles)?
        {
            return Ok(result);
        }

        // Extend path: pop what the free list has, extend the rest.
        let popped = self.pop_free_blocks_bulk(singles)?;
        let remainder = total_blocks - popped.len() as u64;
        let (ext_base, ext_bytes) = if remainder > 0 {
            let bytes = remainder * bs; // ≤ total_blocks * bs, checked above
            match self.stack.extend(bytes) {
                Ok(base) => (base, bytes),
                Err(e) => {
                    // Best-effort: return the detached blocks to the free list.
                    let _ = self.splice_blocks_onto_free_list(&popped);
                    return Err(e);
                }
            }
        } else {
            (0, 0)
        };

        // Assign a region to each request in order. Popped blocks (all consumed
        // by single-block requests) still hold stale bytes and must be scrubbed.
        let mut popped_iter = popped.iter().copied();
        let mut ext_cursor = ext_base;
        let mut result = Vec::with_capacity(lengths.len());
        for (&len, &n) in lengths.iter().zip(counts.iter()) {
            if n == 0 {
                result.push(BStackOwnedSlice::empty(self));
                continue;
            }
            let block_start = if n == 1 {
                if let Some(b) = popped_iter.next() {
                    b
                } else {
                    let b = ext_cursor;
                    ext_cursor += bs; // within the extended tail; cannot overflow
                    b
                }
            } else {
                let b = ext_cursor;
                ext_cursor += n * bs; // within the extended tail; cannot overflow
                b
            };
            // SAFETY: `block_start` names a live `block_size`-aligned region just
            // popped or extended; `len ≤ n * block_size`.
            result.push(unsafe { BStackOwnedSlice::from_raw_parts(self, block_start, len) });
        }

        // Scrub the recycled blocks. On failure, roll back best-effort: return the
        // popped blocks to the free list and discard the fresh tail.
        if let Err(e) = self.scrub_blocks(&popped) {
            let _ = self.splice_blocks_onto_free_list(&popped);
            if ext_bytes > 0 {
                let _ = self.stack.try_discard(ext_base + ext_bytes, ext_bytes);
            }
            return Err(e);
        }
        Ok(result)
    }

    /// Free every handle in one batch.
    ///
    /// Each allocation's blocks are threaded into one chain and spliced onto the
    /// free list with a single [`BStack::set_batched`] plus one atomic
    /// [`BStack::cross_exchange`] (see
    /// [`splice_blocks_onto_free_list`](SlabBStackAllocator::splice_blocks_onto_free_list)),
    /// so a concurrent push/pop cannot be lost. Null sentinel handles are ignored.
    ///
    /// Unlike the single-item [`dealloc`](BStackAllocator::dealloc), a run that
    /// happens to sit at the tail is **not** discarded — every block goes to the
    /// free list. Freed blocks stay reusable by later single-block requests.
    ///
    /// # Atomicity
    ///
    /// The staging batch and the splice are each crash-atomic. A crash or I/O
    /// failure after staging begins leaks the whole freed batch (the free list
    /// stays consistent); no handle can then be safely returned, so
    /// [`BStackBulkAllocError::handles`] is empty. A failure before any write
    /// (a foreign or arithmetic-overflow rejection) returns every handle.
    fn dealloc_bulk<'a>(
        &'a self,
        handles: impl IntoIterator<Item = Self::Allocated<'a>>,
    ) -> Result<(), BStackBulkAllocError<'a, Self>> {
        let slices: Vec<BStackOwnedSlice<'a, Self>> = handles.into_iter().collect();
        let slices = ensure_own_handles(self, slices, "SlabBStackAllocator::dealloc_bulk")?;
        let bs = self.block_size;

        // Set once the chain build has begun. Before this point every handle is
        // still fully owned and returned intact on failure; after it a mid-way
        // failure has clobbered block bodies with next-pointers and no handle can
        // be safely returned.
        let mut freeing = false;
        let result = (|| -> io::Result<()> {
            // Expand every allocation into its constituent block offsets.
            let mut blocks: Vec<u64> = Vec::new();
            for s in &slices {
                if s.is_empty() {
                    continue;
                }
                let n = self.blocks_needed(s.len());
                let backing = n.checked_mul(bs).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "dealloc_bulk: deallocation size overflows u64",
                    )
                })?;
                s.start().checked_add(backing).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "dealloc_bulk: deallocation end offset overflows u64",
                    )
                })?;
                for i in 0..n {
                    blocks.push(s.start() + i * bs);
                }
            }
            if blocks.is_empty() {
                return Ok(());
            }

            freeing = true;
            self.splice_blocks_onto_free_list(&blocks)
        })();
        result.map_err(|source| BStackBulkAllocError {
            source,
            handles: if freeing { Vec::new() } else { slices },
        })
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
    use crate::alloc::{BStackAllocator, BStackSlice, BStackUninitAllocator};
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

    // ── alloc_uninit / realloc_uninit ─────────────────────────────────────────

    #[test]
    fn alloc_uninit_returns_a_usable_region() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = SlabBStackAllocator::new(stack, 32).unwrap();
        let mut s = alloc.alloc_uninit(32).unwrap();
        assert_eq!(s.len(), 32);
        s.write([0xABu8; 32]).unwrap();
        assert_eq!(s.read().unwrap(), vec![0xABu8; 32]);
    }

    #[test]
    fn alloc_uninit_of_a_fresh_block_still_reads_zero() {
        // Tail growth is a sparse `set_len`, so a block that was never occupied
        // reads back as zero even though nothing was written to scrub it.
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = SlabBStackAllocator::new(stack, 32).unwrap();
        let s = alloc.alloc_uninit(32).unwrap();
        assert_eq!(s.read().unwrap(), vec![0u8; 32]);
    }

    #[test]
    fn alloc_uninit_hands_back_a_recycled_block_unscrubbed() {
        // White-box: proves the whole-block `zero` really is skipped. The
        // trait's contract is only that the bytes are unspecified.
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = SlabBStackAllocator::new(stack, 32).unwrap();

        let mut a = alloc.alloc(32).unwrap();
        a.write([0x5Au8; 32]).unwrap();
        let start = a.start();
        alloc.dealloc(a).unwrap();

        let b = alloc.alloc_uninit(32).unwrap();
        assert_eq!(b.start(), start, "the freed block must be the one reused");
        // dealloc overwrites the first 8 bytes with the free-list next pointer;
        // everything after it is the previous occupant's data, untouched.
        assert_eq!(&b.read().unwrap()[8..], &[0x5Au8; 24]);
    }

    #[test]
    fn alloc_still_scrubs_a_recycled_block() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = SlabBStackAllocator::new(stack, 32).unwrap();

        let mut a = alloc.alloc(32).unwrap();
        a.write([0x5Au8; 32]).unwrap();
        let start = a.start();
        alloc.dealloc(a).unwrap();

        let b = alloc.alloc(32).unwrap();
        assert_eq!(b.start(), start);
        assert_eq!(b.read().unwrap(), vec![0u8; 32]);
    }

    #[test]
    fn realloc_uninit_preserves_existing_bytes_on_grow() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = SlabBStackAllocator::new(stack, 32).unwrap();

        // Two blocks so the first is not at the tail: forces the move path.
        let mut a = alloc.alloc(16).unwrap();
        a.write([0x11u8; 16]).unwrap();
        let _pin = alloc.alloc(16).unwrap();

        let grown = alloc.realloc_uninit(a, 100).unwrap();
        assert_eq!(grown.len(), 100);
        assert_eq!(&grown.read().unwrap()[..16], &[0x11u8; 16]);
    }

    #[test]
    fn realloc_uninit_preserves_existing_bytes_on_shrink() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = SlabBStackAllocator::new(stack, 32).unwrap();

        let mut a = alloc.alloc(100).unwrap();
        a.write([0x22u8; 100]).unwrap();
        let shrunk = alloc.realloc_uninit(a, 20).unwrap();
        assert_eq!(shrunk.len(), 20);
        assert_eq!(shrunk.read().unwrap(), vec![0x22u8; 20]);
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
                        slice.write([tid as u8; 16]).unwrap();
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
                        .write([tid as u8; SMALL as usize])
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

    // ── Foreign handles ───────────────────────────────────────────────────

    #[test]
    fn dealloc_and_realloc_reject_a_handle_from_another_instance() {
        let (s1, p1) = empty_stack();
        let _g1 = Guard(p1);
        let (s2, p2) = empty_stack();
        let _g2 = Guard(p2);
        let a1 = SlabBStackAllocator::new(s1, 64).unwrap();
        let a2 = SlabBStackAllocator::new(s2, 64).unwrap();

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
    use super::SlabBStackAllocator;
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
        std::env::temp_dir().join(format!("bstack_slab_bulk_{pid}_{id}.bin"))
    }

    fn empty_stack() -> (BStack, std::path::PathBuf) {
        let path = temp_path();
        (BStack::open(&path).unwrap(), path)
    }

    #[test]
    fn alloc_bulk_empty_returns_empty() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = SlabBStackAllocator::new(stack, 16).unwrap();
        let out = alloc.alloc_bulk([]).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn alloc_bulk_returns_distinct_usable_regions_of_requested_len() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = SlabBStackAllocator::new(stack, 16).unwrap();
        let mut slices = alloc.alloc_bulk([8u64, 16, 12]).unwrap();
        assert_eq!(slices.len(), 3);
        assert_eq!(
            slices.iter().map(|s| s.len()).collect::<Vec<_>>(),
            [8, 16, 12]
        );
        // Distinct starts.
        let starts: Vec<u64> = slices.iter().map(|s| s.start()).collect();
        assert_ne!(starts[0], starts[1]);
        assert_ne!(starts[1], starts[2]);
        assert_ne!(starts[0], starts[2]);
        // Each is independently writable/readable without disturbing the others.
        for (i, s) in slices.iter_mut().enumerate() {
            let n = s.len() as usize;
            s.write(vec![i as u8 + 1; n]).unwrap();
        }
        for (i, s) in slices.iter().enumerate() {
            assert_eq!(s.read().unwrap(), vec![i as u8 + 1; s.len() as usize]);
        }
    }

    #[test]
    fn alloc_bulk_zero_length_entries_are_null_slices() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = SlabBStackAllocator::new(stack, 16).unwrap();
        let slices = alloc.alloc_bulk([0u64, 16, 0]).unwrap();
        assert_eq!(slices.len(), 3);
        assert!(slices[0].is_empty());
        assert!(!slices[1].is_empty());
        assert!(slices[2].is_empty());
    }

    #[test]
    fn alloc_bulk_all_zero_touches_nothing() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = SlabBStackAllocator::new(stack, 16).unwrap();
        let before = alloc.stack().len().unwrap();
        let slices = alloc.alloc_bulk([0u64, 0]).unwrap();
        assert!(slices.iter().all(|s| s.is_empty()));
        assert_eq!(alloc.stack().len().unwrap(), before);
    }

    #[test]
    fn alloc_bulk_oversized_entries_are_contiguous_and_independently_freeable() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = SlabBStackAllocator::new(stack, 16).unwrap();
        // 40 bytes needs 3 blocks (3×16); 16 needs 1.
        let mut slices = alloc.alloc_bulk([40u64, 16]).unwrap();
        assert_eq!(slices[0].len(), 40);
        assert_eq!(slices[1].len(), 16);
        // The oversized region is a single contiguous run.
        slices[0].write([0xAAu8; 40]).unwrap();
        assert_eq!(slices[0].read().unwrap(), vec![0xAAu8; 40]);
        // Each handle frees independently.
        alloc.dealloc(slices.pop().unwrap()).unwrap();
        alloc.dealloc(slices.pop().unwrap()).unwrap();
    }

    #[test]
    fn alloc_bulk_recycled_blocks_read_back_zero() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = SlabBStackAllocator::new(stack, 16).unwrap();
        // Fill three blocks with a pattern, then bulk-free them.
        let mut a = alloc.alloc_bulk([16u64, 16, 16]).unwrap();
        for s in &mut a {
            s.write([0x5Au8; 16]).unwrap();
        }
        alloc.dealloc_bulk(a).unwrap();
        // Bulk-alloc again: recycled blocks must honour the zero-init guarantee.
        let b = alloc.alloc_bulk([16u64, 16, 16]).unwrap();
        for s in &b {
            assert_eq!(
                s.read().unwrap(),
                vec![0u8; 16],
                "recycled block not scrubbed"
            );
        }
    }

    #[test]
    fn alloc_bulk_prefers_free_list_then_extends() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = SlabBStackAllocator::new(stack, 16).unwrap();
        // Seed the free list with two blocks.
        let a = alloc.alloc_bulk([16u64, 16]).unwrap();
        let mut freed: Vec<u64> = a.iter().map(|s| s.start()).collect();
        freed.sort_unstable();
        alloc.dealloc_bulk(a).unwrap();

        // Request four single blocks: two reuse the free list, two extend.
        let b = alloc.alloc_bulk([16u64, 16, 16, 16]).unwrap();
        let mut reused: Vec<u64> = b.iter().map(|s| s.start()).collect();
        reused.sort_unstable();
        // The two freed offsets must reappear among the four handed out.
        assert!(freed.iter().all(|f| reused.contains(f)));
        // Four distinct blocks.
        reused.dedup();
        assert_eq!(reused.len(), 4);
    }

    #[test]
    fn dealloc_bulk_returns_all_blocks_to_the_free_list() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = SlabBStackAllocator::new(stack, 16).unwrap();
        let a = alloc.alloc_bulk([16u64, 16, 16]).unwrap();
        let mut original: Vec<u64> = a.iter().map(|s| s.start()).collect();
        original.sort_unstable();
        alloc.dealloc_bulk(a).unwrap();

        // Every freed block is reusable.
        let mut reused = Vec::new();
        for _ in 0..3 {
            reused.push(alloc.alloc(16).unwrap().start());
        }
        reused.sort_unstable();
        assert_eq!(reused, original);
    }

    #[test]
    fn dealloc_bulk_empty_and_null_handles_are_noops() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = SlabBStackAllocator::new(stack, 16).unwrap();
        alloc.dealloc_bulk([]).unwrap();
        let z = alloc.alloc(0).unwrap();
        alloc.dealloc_bulk([z]).unwrap();
    }

    #[test]
    fn bulk_round_trips_survive_reopen() {
        let (stack, path) = empty_stack();
        let _g = Guard(path.clone());
        let alloc = SlabBStackAllocator::new(stack, 16).unwrap();
        let mut slices = alloc.alloc_bulk([10u64, 40, 16]).unwrap();
        let offsets: Vec<(u64, u64)> = slices.iter().map(|s| (s.start(), s.len())).collect();
        for (i, s) in slices.iter_mut().enumerate() {
            let n = s.len() as usize;
            s.write(vec![i as u8 + 7; n]).unwrap();
        }
        drop(slices);
        drop(alloc);

        let alloc2 = SlabBStackAllocator::open(BStack::open(&path).unwrap()).unwrap();
        for (i, &(off, len)) in offsets.iter().enumerate() {
            let v = unsafe { crate::alloc::BStackSlice::from_raw_parts(alloc2.stack(), off, len) };
            assert_eq!(v.read().unwrap(), vec![i as u8 + 7; len as usize]);
        }
    }

    #[test]
    fn dealloc_bulk_rejects_a_batch_with_a_foreign_handle() {
        let (s1, p1) = empty_stack();
        let _g1 = Guard(p1);
        let (s2, p2) = empty_stack();
        let _g2 = Guard(p2);
        let a1 = SlabBStackAllocator::new(s1, 32).unwrap();
        let a2 = SlabBStackAllocator::new(s2, 32).unwrap();

        let own = a2.alloc(32).unwrap();
        let foreign = a1.alloc(32).unwrap();
        let err = a2
            .dealloc_bulk([own, foreign])
            .expect_err("a2 must refuse a batch containing a1's handle");
        assert_eq!(err.source.kind(), std::io::ErrorKind::InvalidInput);
        // Both handles come back (nothing was freed).
        assert_eq!(err.handles.len(), 2);
    }

    #[test]
    fn concurrent_alloc_bulk_dealloc_bulk_no_live_duplicates() {
        use std::collections::HashSet;
        use std::sync::{Arc, Mutex};
        use std::thread;

        const THREADS: usize = 8;
        const ROUNDS: usize = 100;
        const SIZES: [u64; 3] = [16, 40, 16];

        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = Arc::new(SlabBStackAllocator::new(stack, 16).unwrap());
        let live: Arc<Mutex<HashSet<u64>>> = Arc::new(Mutex::new(HashSet::new()));

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let alloc = Arc::clone(&alloc);
                let live = Arc::clone(&live);
                thread::spawn(move || {
                    let a: &SlabBStackAllocator = &alloc;
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
                            let n = s.len() as usize;
                            s.write(vec![0xC3u8; n]).unwrap();
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
    }
}

// Bulk-allocation fault-injection tests (`atomic`): prove the alloc_bulk leak
// avoidance the reviews asked for — the all-free-list fast path commits nothing
// on a fault (no leak), and the extend path returns its popped blocks to the
// free list when the extend fails (best-effort reclaim).
#[cfg(all(
    test,
    debug_assertions,
    feature = "fault-injection",
    feature = "set",
    feature = "atomic"
))]
mod bulk_fault_tests {
    use super::SlabBStackAllocator;
    use crate::BStack;
    use crate::alloc::{BStackAllocator, BStackBulkAllocator};
    use crate::alloc_fuzz::common::{Guard, policies::FailOpAt, temp_path};
    use crate::fault::FaultPolicy;
    use std::io::ErrorKind;
    use std::sync::Arc;

    const BLOCK: u64 = 16;

    fn arm(alloc: &SlabBStackAllocator, policy: FailOpAt) {
        let policy: Arc<dyn FaultPolicy> = Arc::new(policy);
        alloc.stack().set_fault_policy(Some(policy));
    }
    fn disarm(alloc: &SlabBStackAllocator) {
        alloc.stack().set_fault_policy(None);
    }

    // Seed the free list with two blocks and return their (sorted) offsets.
    fn seed_two(alloc: &SlabBStackAllocator) -> [u64; 2] {
        let a = alloc.alloc(BLOCK).unwrap();
        let b = alloc.alloc(BLOCK).unwrap();
        let mut freed = [a.start(), b.start()];
        alloc.dealloc(a).unwrap();
        alloc.dealloc(b).unwrap();
        freed.sort_unstable();
        freed
    }

    fn reuse_two(alloc: &SlabBStackAllocator) -> [u64; 2] {
        let r1 = alloc.alloc(BLOCK).unwrap();
        let r2 = alloc.alloc(BLOCK).unwrap();
        let mut reused = [r1.start(), r2.start()];
        reused.sort_unstable();
        reused
    }

    // A failed `extend` on the extend path returns the blocks already popped from
    // the free list rather than leaking them.
    #[test]
    fn alloc_bulk_extend_fault_reclaims_popped_blocks() {
        let path = temp_path("slab_bulk_extend");
        let _g = Guard(path.clone());
        let alloc = SlabBStackAllocator::new(BStack::open(&path).unwrap(), BLOCK).unwrap();
        let freed = seed_two(&alloc);

        // Two singles (drain the list) plus an oversized run that forces an
        // extend; fault the extend.
        arm(&alloc, FailOpAt::new("extend", 0, ErrorKind::Other));
        let err = alloc
            .alloc_bulk([BLOCK, BLOCK, BLOCK * 3])
            .expect_err("extend fault must fail alloc_bulk");
        disarm(&alloc);
        assert_eq!(err.kind(), ErrorKind::Other);

        // The two popped blocks were returned to the free list.
        assert_eq!(
            reuse_two(&alloc),
            freed,
            "popped blocks were not returned to the free list"
        );
    }

    // A fault during the all-free-list fast path (one `inplace_gen`) commits
    // nothing, so the free list is untouched.
    #[test]
    fn alloc_bulk_freelist_atomic_fault_leaks_nothing() {
        let path = temp_path("slab_bulk_atomic");
        let _g = Guard(path.clone());
        let alloc = SlabBStackAllocator::new(BStack::open(&path).unwrap(), BLOCK).unwrap();
        let freed = seed_two(&alloc);

        // Two single-block requests → the all-free-list fast path via inplace_gen.
        arm(&alloc, FailOpAt::new("inplace_gen", 0, ErrorKind::Other));
        let err = alloc
            .alloc_bulk([BLOCK, BLOCK])
            .expect_err("inplace_gen fault must fail alloc_bulk");
        disarm(&alloc);
        assert_eq!(err.kind(), ErrorKind::Other);

        assert_eq!(
            reuse_two(&alloc),
            freed,
            "fast-path fault must leave the free list intact"
        );
    }
}

// Fault-injection failure tests (non-`atomic` white-box; op-agnostic fuzz covers
// the `atomic` build). Slab has no recovery routine — consistency rests entirely
// on write ordering (payload before `free_head`), so a fault during a free-list
// push leaks at most the in-flight block and never corrupts the list. The tests
// pin the handle contract for the retained (oversized tail) and lost (free-list)
// paths and confirm a reopen sees an intact, usable arena.
#[cfg(all(
    test,
    debug_assertions,
    feature = "fault-injection",
    feature = "set",
    not(feature = "atomic")
))]
mod fault_tests {
    use super::SlabBStackAllocator;
    use crate::BStack;
    use crate::alloc::BStackAllocator;
    use crate::alloc_fuzz::common::{Guard, policies::FailOpAt, temp_path};
    use crate::fault::FaultPolicy;
    use std::io::ErrorKind;
    use std::sync::Arc;

    const BLOCK: u64 = 64;

    fn arm(alloc: &SlabBStackAllocator, policy: FailOpAt) {
        let policy: Arc<dyn FaultPolicy> = Arc::new(policy);
        alloc.stack().set_fault_policy(Some(policy));
    }
    fn disarm(alloc: &SlabBStackAllocator) {
        alloc.stack().set_fault_policy(None);
    }

    // A fresh block is grown with a single `extend`; a fault there surfaces
    // cleanly and leaves the allocator usable.
    #[test]
    fn alloc_extend_fault_surfaces_cleanly() {
        let path = temp_path("slab_alloc");
        let _g = Guard(path.clone());
        let alloc = SlabBStackAllocator::new(BStack::open(&path).unwrap(), BLOCK).unwrap();

        arm(&alloc, FailOpAt::new("extend", 0, ErrorKind::Other));
        let err = alloc
            .alloc(BLOCK)
            .expect_err("alloc must fail when extend faults");
        disarm(&alloc);
        assert_eq!(err.kind(), ErrorKind::Other);

        let mut s = alloc.alloc(BLOCK).unwrap();
        s.write([7u8; BLOCK as usize]).unwrap();
        assert_eq!(s.read().unwrap(), vec![7u8; BLOCK as usize]);
    }

    // Freeing an oversized tail allocation is a single `discard` with no `lost`
    // marking; a fault there leaves the region live, so the handle comes back and
    // a retry succeeds.
    #[test]
    fn dealloc_oversized_tail_discard_fault_returns_handle() {
        let path = temp_path("slab_tail");
        let _g = Guard(path.clone());
        let alloc = SlabBStackAllocator::new(BStack::open(&path).unwrap(), BLOCK).unwrap();

        let mut s = alloc.alloc(200).unwrap(); // oversized (4 blocks), at the tail
        s.write([3u8; 200]).unwrap();
        let (start, len) = (s.start(), s.len());

        arm(&alloc, FailOpAt::new("discard", 0, ErrorKind::Other));
        let err = alloc
            .dealloc(s)
            .expect_err("tail dealloc must fail when discard faults");
        disarm(&alloc);

        let handle = err
            .handle
            .expect("oversized-tail dealloc leaves the region live");
        assert_eq!((handle.start(), handle.len()), (start, len));
        assert_eq!(
            handle.read().unwrap(),
            vec![3u8; 200],
            "data must be intact"
        );
        alloc.dealloc(handle).unwrap();
    }

    // A single-block, non-tail free enters the free-list push, which sets `lost`
    // before touching any links. Faulting its first `set` reports the block lost
    // (handle `None`); because the fault fires before the write, `free_head` is
    // unchanged, so the list stays valid and a reopen sees an intact arena.
    #[test]
    fn dealloc_freelist_fault_is_lost_and_list_stays_valid() {
        let path = temp_path("slab_lost");
        let _g = Guard(path.clone());

        let b_start = {
            let alloc = SlabBStackAllocator::new(BStack::open(&path).unwrap(), BLOCK).unwrap();
            let a = alloc.alloc(BLOCK).unwrap();
            let mut b = alloc.alloc(BLOCK).unwrap();
            b.write([5u8; BLOCK as usize]).unwrap();
            let b_start = b.start();

            arm(&alloc, FailOpAt::new("set", 0, ErrorKind::Other));
            let err = alloc
                .dealloc(a)
                .expect_err("free-list dealloc must fail when set faults");
            disarm(&alloc);
            assert!(
                err.handle.is_none(),
                "past the lost point the handle must be None"
            );
            drop(alloc);
            b_start
        };

        // Reopen: Slab::open validates the header/free_head; the list must be
        // intact and the surviving allocation preserved.
        let alloc = SlabBStackAllocator::open(BStack::open(&path).unwrap()).unwrap();
        assert_eq!(
            alloc.stack().get(b_start, b_start + BLOCK).unwrap(),
            vec![5u8; BLOCK as usize]
        );
        let mut c = alloc.alloc(BLOCK).unwrap();
        c.write([6u8; BLOCK as usize]).unwrap();
        assert_eq!(c.read().unwrap(), vec![6u8; BLOCK as usize]);
    }
}
