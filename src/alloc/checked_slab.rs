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
use std::{collections::HashSet, fmt, io};

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
/// follows the overhead, i.e. `data_size` usable bytes per block.
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
/// * `num_blocks == 1` (`len ≤ data_size`) — pops from the free list if
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

/// How a single block looks to the recovery scan.
#[cfg(feature = "set")]
enum BlockClass {
    /// `overhead == 0` and the block is reachable from `free_head`.
    Free,
    /// `overhead == 0` but the block is **not** in the free list (a leak).
    Leaked,
    /// A valid in-use marker spanning this many blocks.
    InUse(u64),
    /// Neither a clean free block nor a valid in-use marker.
    Suspicious,
}

/// Outcome of attempting to resynchronise the scan after a suspicious block
/// when no free-list block remains as an anchor.
#[cfg(feature = "set")]
enum ResyncOutcome {
    /// A later block boundary cleanly tiles to the tail; resume there. The gap
    /// is mid-arena garbage and is left leaked.
    Resync(u64),
    /// Nothing valid follows; the suspect region is an orphaned tail (a failed
    /// `realloc` truncation) and should be discarded.
    DiscardTail,
    /// The region is too large to analyse within the memory cap; leave it
    /// leaked rather than risk an unbounded allocation.
    LeaveLeaked,
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
    /// Minimum legal `block_size` (internal): `OVERHEAD + MIN_DATA_SIZE`.
    const MIN_BLOCK_SIZE: u64 = 16;
    /// Minimum usable bytes per block exposed via `new`.
    const MIN_DATA_SIZE: u64 = Self::MIN_BLOCK_SIZE - Self::OVERHEAD;
    /// Free-list sentinel meaning "no next block". 0 is safe because all blocks
    /// start at ARENA_START (48) or later and no valid block offset is 0.
    const SENTINEL: u64 = 0;
    /// High bit of the overhead field: set when a block is in use.
    const IN_USE_BIT: u64 = 0x8000_0000_0000_0000;
    /// Mask extracting the block-count field from an in-use overhead value.
    const BLOCKS_MASK: u64 = !Self::IN_USE_BIT;

    /// Initialise a new `CheckedSlabBStackAllocator` over an empty `stack`.
    ///
    /// `data_size` is the number of usable bytes per slab block (excluding the
    /// 8-byte overhead prefix). The on-disk `block_size` stored in the header is
    /// `data_size + 8`. Writes the 48-byte allocator header using a single
    /// [`BStack::push`] and returns a ready allocator.
    ///
    /// # Errors
    ///
    /// * [`io::ErrorKind::InvalidInput`] — `data_size < 8`, or `stack` is not
    ///   empty (use [`CheckedSlabBStackAllocator::open`] to reopen an existing
    ///   file).
    /// * Any [`io::Error`] propagated from the underlying [`BStack`] operations.
    pub fn new(stack: BStack, data_size: u64) -> io::Result<Self> {
        if !stack.is_empty()? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "stack is not empty; use CheckedSlabBStackAllocator::open to reopen an existing allocator",
            ));
        }
        if data_size < Self::MIN_DATA_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("data_size ({data_size}) must be >= {}", Self::MIN_DATA_SIZE),
            ));
        }
        let block_size = data_size.checked_add(Self::OVERHEAD).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "data_size is too large (overflows u64)",
            )
        })?;
        if usize::try_from(block_size).is_err() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "data_size is too large for this platform",
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

        let allocator = Self {
            stack,
            block_size: stored_block_size,
            _not_sync: PhantomData,
        };
        // Reclaim leaks and repair a failed tail truncation left by an unclean
        // shutdown. Best-effort: the residual unsure-block count is discarded
        // here; call [`recover`](Self::recover) explicitly to inspect it.
        allocator.recover()?;
        Ok(allocator)
    }

    /// Return the usable bytes per slab block (the `data_size` passed to [`new`](Self::new)).
    pub fn data_size(&self) -> u64 {
        self.block_size - Self::OVERHEAD
    }

    /// Upper bound on the number of blocks the reach-oracle will analyse in one
    /// pass. Past this, an ambiguous region is left leaked rather than risk an
    /// unbounded allocation. Only reached on a corrupt/garbage arena.
    const MAX_RECOVER_REGION: usize = 1 << 26;

    /// Repair the allocator after an unclean shutdown and return the number of
    /// blocks that remain leaked or could not be classified with certainty
    /// (`0` means the arena is fully accounted for).
    ///
    /// Recovery is a two-phase, crash-safe, idempotent operation. [`open`](Self::open)
    /// runs it automatically; it is exposed so callers can inspect the residual
    /// count or re-run it on demand.
    ///
    /// # Phase 1 — scan (read-only)
    ///
    /// The free list is walked into a sorted set (with cycle detection), then
    /// the arena is scanned linearly. A valid in-use block advances the cursor
    /// by its block count; a zero-overhead block not in the free list is a leak
    /// and is queued for reclaim; anything else is *suspicious*. At a suspicious
    /// block the scan resynchronises on the next free-list block if one follows
    /// (the intervening gap is left leaked); otherwise a backward reachability
    /// pass decides between a mid-arena gap (left leaked) and an orphaned tail
    /// from a failed `realloc` truncation (discarded).
    ///
    /// # Phase 2 — apply (one block at a time)
    ///
    /// Any orphaned tail is discarded, then each reclaimed block is prepended to
    /// the existing free list individually — the list is never rebuilt. Every
    /// step is an atomic [`BStack`] operation, so a crash mid-recovery simply
    /// leaves the remaining leaks to be re-found on the next run.
    ///
    /// # Safety of destructive steps
    ///
    /// A tail is discarded only when no free-list block lies at or beyond it, so
    /// `free_head` and every free block survive. If the free-list walk itself
    /// hits corruption, reclaim and tail-discard are both suppressed for that
    /// run and the uncertain blocks are merely counted — an unreliable free list
    /// never authorises relinking or truncation.
    pub fn recover(&self) -> io::Result<u64> {
        let stack_len = self.stack.len()?;
        if stack_len <= Self::ARENA_START {
            return Ok(0);
        }
        let bs = self.block_size;
        let (free, free_corrupt) = self.scan_free_list(stack_len)?;

        let mut reclaim: Vec<u64> = Vec::new();
        let mut unsure: u64 = 0;
        let mut tailcut: Option<u64> = None;

        let mut p = Self::ARENA_START;
        'scan: while p < stack_len {
            match self.classify(p, stack_len, &free)? {
                BlockClass::Free => p += bs,
                BlockClass::Leaked => {
                    // A bare zero-overhead block is only trustworthy as a leak
                    // while the free list walked cleanly; a corrupt list means
                    // it might already be linked, so leave it leaked.
                    if free_corrupt {
                        unsure += 1;
                    } else {
                        reclaim.push(p);
                    }
                    p += bs;
                }
                BlockClass::InUse(n) => p += n * bs,
                BlockClass::Suspicious => {
                    // Prefer a known-free block as a reliable resync anchor.
                    let idx = free.partition_point(|&x| x <= p);
                    if let Some(&f) = free.get(idx) {
                        unsure += (f - p) / bs;
                        p = f;
                    } else {
                        match self.resync_tail(p, stack_len, &free)? {
                            ResyncOutcome::Resync(q) => {
                                unsure += (q - p) / bs;
                                p = q;
                            }
                            ResyncOutcome::DiscardTail => {
                                // Only safe when the free list is trusted.
                                if free_corrupt {
                                    unsure += (stack_len - p) / bs;
                                } else {
                                    tailcut = Some(p);
                                }
                                break 'scan;
                            }
                            ResyncOutcome::LeaveLeaked => {
                                unsure += (stack_len - p) / bs;
                                break 'scan;
                            }
                        }
                    }
                }
            }
        }

        if let Some(t) = tailcut {
            self.stack.discard(stack_len - t)?;
        }
        for b in reclaim {
            self.push_free_blocks(b, 1)?;
        }
        Ok(unsure)
    }

    /// Walk the free list from `free_head` into a sorted set of block offsets.
    ///
    /// Stops at the first structural problem — a misaligned or out-of-bounds
    /// pointer, a head whose overhead is non-zero, or a cycle (detected with a
    /// visited set bounded by the arena block count) — and reports it via the
    /// returned flag (`true` = the walk was cut short by corruption).
    fn scan_free_list(&self, stack_len: u64) -> io::Result<(Vec<u64>, bool)> {
        let mut free = Vec::new();
        let mut seen: HashSet<u64> = HashSet::new();
        let arena_blocks = (stack_len - Self::ARENA_START) / self.block_size;
        let mut head = u64::from_le_bytes(read_bstack!(self.stack, Self::FREE_HEAD_OFFSET => u64));
        let mut corrupt = false;
        while head != Self::SENTINEL {
            if head < Self::ARENA_START
                || (head - Self::ARENA_START) % self.block_size != 0
                || head >= stack_len
                || !seen.insert(head)
                || seen.len() as u64 > arena_blocks
            {
                corrupt = true;
                break;
            }
            let mut prefix = [0u8; 16];
            self.stack.get_into(head, &mut prefix)?;
            if u64::from_le_bytes(prefix[0..8].try_into().unwrap()) != 0 {
                corrupt = true;
                break;
            }
            free.push(head);
            head = u64::from_le_bytes(prefix[8..16].try_into().unwrap());
        }
        free.sort_unstable();
        Ok((free, corrupt))
    }

    /// Classify the block at `p` for the recovery scan.
    fn classify(&self, p: u64, stack_len: u64, free: &[u64]) -> io::Result<BlockClass> {
        let overhead = self.read_overhead(p)?;
        if overhead == 0 {
            return Ok(if free.binary_search(&p).is_ok() {
                BlockClass::Free
            } else {
                BlockClass::Leaked
            });
        }
        Ok(match self.valid_in_use(overhead, p, stack_len, free) {
            Some(n) => BlockClass::InUse(n),
            None => BlockClass::Suspicious,
        })
    }

    /// If `overhead` at `p` is a valid in-use marker, return its block span.
    ///
    /// Rejects markers whose count is zero, whose span overflows or runs past
    /// `stack_len`, or whose extent engulfs a known free block — a free block
    /// can never lie inside a live allocation.
    fn valid_in_use(&self, overhead: u64, p: u64, stack_len: u64, free: &[u64]) -> Option<u64> {
        if overhead & Self::IN_USE_BIT == 0 {
            return None;
        }
        let n = overhead & Self::BLOCKS_MASK;
        if n == 0 {
            return None;
        }
        let span = n.checked_mul(self.block_size)?;
        let end = p.checked_add(span)?;
        if end > stack_len {
            return None;
        }
        // Engulf check: the first free block past `p` must not fall inside `end`.
        if let Some(&f) = free.get(free.partition_point(|&x| x <= p)) {
            if f < end {
                return None;
            }
        }
        Some(n)
    }

    /// Decide what to do with a suspicious region `[p, stack_len)` when no
    /// free-list block follows it.
    ///
    /// A backward reachability pass marks each boundary from which a strict
    /// clean walk (only free or valid in-use blocks) lands exactly on
    /// `stack_len`. The smallest such interior boundary is a mid-arena gap to
    /// resync on; if none exists the region is an orphaned tail to discard.
    fn resync_tail(&self, p: u64, stack_len: u64, free: &[u64]) -> io::Result<ResyncOutcome> {
        let bs = self.block_size;
        let m = match usize::try_from((stack_len - p) / bs) {
            Ok(v) if v <= Self::MAX_RECOVER_REGION => v,
            _ => return Ok(ResyncOutcome::LeaveLeaked),
        };
        // reach[j]: a clean walk starting at block j reaches stack_len exactly.
        let mut reach = vec![false; m + 1];
        reach[m] = true;
        for j in (0..m).rev() {
            let off = p + (j as u64) * bs;
            let overhead = self.read_overhead(off)?;
            reach[j] = if overhead == 0 {
                reach[j + 1]
            } else if let Some(n) = self.valid_in_use(overhead, off, stack_len, free) {
                // valid_in_use guarantees off + n*bs <= stack_len, so j + n <= m.
                reach[j + n as usize]
            } else {
                false
            };
        }
        for j in 1..m {
            if reach[j] {
                return Ok(ResyncOutcome::Resync(p + (j as u64) * bs));
            }
        }
        Ok(ResyncOutcome::DiscardTail)
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

    /// Pop the head block off the free list, mark it in use with `num_blocks`,
    /// and return its block start offset, or `None` if the list is empty.
    ///
    /// Advances `free_head` then writes the full block in one call: the overhead
    /// is set to `IN_USE_BIT | num_blocks` and the data bytes are zeroed. A
    /// crash between the two writes merely leaks the detached block.
    fn pop_and_claim_block(&self, num_blocks: u64) -> io::Result<Option<u64>> {
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
        // Advance free_head to the next block (stored in data[0..8]).
        self.stack.set(Self::FREE_HEAD_OFFSET, &prefix[8..16])?;
        // Mark in-use and zero data in one write.
        let mut block_buf = vec![0u8; self.block_size as usize]; // safe: validated in new/open
        block_buf[..8].copy_from_slice(&(Self::IN_USE_BIT | num_blocks).to_le_bytes());
        self.stack.set(head, block_buf)?;
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
            .field("data_size", &self.data_size())
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
            if let Some(block_start) = self.pop_and_claim_block(1)? {
                // SAFETY: block_start is a valid block_size region; data begins after the overhead.
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

        if slice_end == current_tail {
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
            // Ordering matters, and the commit must come first. Shrinking the
            // first block's count is the commit point: before it the old view
            // (old_n blocks, original payload) is fully intact; after it the new
            // view (new_n blocks) is in force. Only once committed do we write
            // free-list metadata into the excess blocks (which clobbers their old
            // payload) and repoint free_head. A crash before the commit leaves
            // the original allocation untouched; a crash after it leaks the
            // excess blocks but never corrupts a live allocation. Writing the
            // free run first would shred the tail payload while the header still
            // claims old_n, leaving a recovered allocation that is neither
            // cleanly old nor cleanly new.
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
            self.write_overhead(block_start, Self::IN_USE_BIT | new_n)?;
            self.write_free_run(excess_start, old_n - new_n)?;
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
    fn new_initialises_header_and_reports_data_size() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = CheckedSlabBStackAllocator::new(stack, 8).unwrap();
        assert_eq!(alloc.data_size(), 8);
        // ARENA_START = OFFSET_SIZE(24) + HEADER_SIZE(24) = 48
        assert_eq!(alloc.stack().len().unwrap(), 48);
    }

    #[test]
    fn new_rejects_data_size_below_minimum() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let err = CheckedSlabBStackAllocator::new(stack, 7).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn new_rejects_nonempty_stack() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        stack.push(b"data").unwrap();
        let err = CheckedSlabBStackAllocator::new(stack, 8).unwrap_err();
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
        CheckedSlabBStackAllocator::new(stack, 8).unwrap();
        let reopen = BStack::open(&path).unwrap();
        reopen.extend(1).unwrap();
        drop(reopen);
        let err = CheckedSlabBStackAllocator::open(BStack::open(&path).unwrap()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn open_succeeds_and_restores_data_size() {
        let (stack, path) = empty_stack();
        let _g = Guard(path.clone());
        CheckedSlabBStackAllocator::new(stack, 24).unwrap();
        let alloc = CheckedSlabBStackAllocator::open(BStack::open(&path).unwrap()).unwrap();
        assert_eq!(alloc.data_size(), 24);
    }

    // ── allocation behaviour ──────────────────────────────────────────────────

    #[test]
    fn zero_alloc_returns_empty_slice() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = CheckedSlabBStackAllocator::new(stack, 8).unwrap();
        let s = alloc.alloc(0).unwrap();
        assert!(s.is_empty());
    }

    #[test]
    fn dealloc_pushes_to_free_list_and_next_alloc_reuses_block() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = CheckedSlabBStackAllocator::new(stack, 8).unwrap();

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
        let alloc = CheckedSlabBStackAllocator::new(stack, 8).unwrap();

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
        let alloc = CheckedSlabBStackAllocator::new(stack, 8).unwrap();

        // data_size=8, block_size=16: 17 bytes needs ceil((17+8)/16) = 2 blocks = 32 bytes.
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
        let alloc = CheckedSlabBStackAllocator::new(stack, 8).unwrap();

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
        let alloc = CheckedSlabBStackAllocator::new(stack, 16).unwrap();
        let s = alloc.alloc(12).unwrap();
        s.write(b"hello world!").unwrap();
        assert_eq!(s.read().unwrap(), b"hello world!");
    }

    #[test]
    fn data_survives_reopen() {
        let (stack, path) = empty_stack();
        let _g = Guard(path.clone());
        let alloc = CheckedSlabBStackAllocator::new(stack, 8).unwrap();
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
        let alloc = CheckedSlabBStackAllocator::new(stack, 8).unwrap();

        // data_size=8, block_size=16: 40 bytes needs ceil((40+8)/16) = 3 blocks.
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
        // data_size 24: alloc(8) and realloc(16) both fit in 1 block.
        let alloc = CheckedSlabBStackAllocator::new(stack, 24).unwrap();
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
        let alloc = CheckedSlabBStackAllocator::new(stack, 8).unwrap();
        let s = alloc.alloc(8).unwrap();
        s.write(b"abcdefgh").unwrap();
        // Grow to 40 bytes -> ceil((40+8)/16) = 3 blocks.
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
        let alloc = CheckedSlabBStackAllocator::new(stack, 8).unwrap();

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
        let alloc = CheckedSlabBStackAllocator::new(stack, 8).unwrap();
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
        let alloc = CheckedSlabBStackAllocator::new(stack, 8).unwrap();
        let empty = alloc.alloc(0).unwrap();
        let s = alloc.realloc(empty, 8).unwrap();
        assert_eq!(s.len(), 8);
        assert!(!s.is_empty());
    }

    // ── recover() ─────────────────────────────────────────────────────────────

    /// In-use overhead value for an allocation spanning `n` blocks.
    const fn in_use(n: u64) -> u64 {
        0x8000_0000_0000_0000u64 | n
    }

    #[test]
    fn recover_clean_allocator_returns_zero() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = CheckedSlabBStackAllocator::new(stack, 8).unwrap(); // block_size 16
        let _a = alloc.alloc(8).unwrap(); // block 48
        let b = alloc.alloc(8).unwrap(); // block 64
        let _c = alloc.alloc(16).unwrap(); // blocks 80,96 (2-block)
        alloc.dealloc(b).unwrap(); // b -> free list
        // Nothing leaked or orphaned: a clean scan accounts for every block.
        assert_eq!(alloc.recover().unwrap(), 0);
    }

    #[test]
    fn recover_reclaims_leaked_free_block() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = CheckedSlabBStackAllocator::new(stack, 8).unwrap(); // block_size 16
        let _a = alloc.alloc(8).unwrap(); // block 48
        let b = alloc.alloc(8).unwrap(); // block 64, slice start 72
        let _c = alloc.alloc(8).unwrap(); // block 80
        alloc.dealloc(b).unwrap(); // free_head = 64
        // Simulate a pop crash: advance free_head past b without claiming it.
        alloc.stack().set(40, 0u64.to_le_bytes()).unwrap();
        // Block 64 is now leaked (overhead 0, not in the free list).
        assert_eq!(alloc.recover().unwrap(), 0);
        // The reclaimed block is handed back out on the next allocation.
        let reused = alloc.alloc(8).unwrap();
        assert_eq!(reused.start(), 72);
    }

    #[test]
    fn recover_discards_orphan_tail() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = CheckedSlabBStackAllocator::new(stack, 8).unwrap(); // block_size 16
        let s = alloc.alloc(40).unwrap(); // 3 blocks: block 48, stack -> 96
        s.write(&[0xFFu8; 40]).unwrap(); // fill so orphan blocks read as garbage
        assert_eq!(alloc.stack().len().unwrap(), 96);
        // Simulate a realloc tail-shrink crash: commit new_n=1, skip the discard.
        alloc.stack().set(48, in_use(1).to_le_bytes()).unwrap();
        // Blocks 64 and 80 are now orphaned tail garbage.
        assert_eq!(alloc.recover().unwrap(), 0);
        assert_eq!(alloc.stack().len().unwrap(), 64); // tail truncated away
    }

    #[test]
    fn recover_leaves_mid_arena_garbage_leaked() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = CheckedSlabBStackAllocator::new(stack, 8).unwrap(); // block_size 16
        let _a = alloc.alloc(8).unwrap(); // block 48
        let _b = alloc.alloc(8).unwrap(); // block 64
        let c = alloc.alloc(8).unwrap(); // block 80, slice start 88
        c.write(b"keepkeep").unwrap();
        // Corrupt the middle block into garbage; the live block after it remains.
        alloc.stack().set(64, u64::MAX.to_le_bytes()).unwrap();
        // No free-list anchor follows, so the reach-oracle must resync on block 80.
        assert_eq!(alloc.recover().unwrap(), 1); // one garbage block left leaked
        assert_eq!(alloc.stack().len().unwrap(), 96); // nothing discarded
        assert_eq!(c.read().unwrap(), b"keepkeep"); // live data preserved
    }

    #[test]
    fn open_auto_recovers_orphan_tail() {
        let (stack, path) = empty_stack();
        let _g = Guard(path.clone());
        {
            let alloc = CheckedSlabBStackAllocator::new(stack, 8).unwrap();
            let s = alloc.alloc(40).unwrap(); // 3 blocks, stack -> 96
            s.write(&[0xFFu8; 40]).unwrap();
            alloc.stack().set(48, in_use(1).to_le_bytes()).unwrap(); // crash sim
        }
        let reopened = CheckedSlabBStackAllocator::open(BStack::open(&path).unwrap()).unwrap();
        assert_eq!(reopened.stack().len().unwrap(), 64); // open() ran recovery
    }

    #[test]
    fn recover_is_idempotent() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = CheckedSlabBStackAllocator::new(stack, 8).unwrap();
        let _a = alloc.alloc(8).unwrap();
        let b = alloc.alloc(8).unwrap();
        let _c = alloc.alloc(8).unwrap();
        alloc.dealloc(b).unwrap();
        alloc.stack().set(40, 0u64.to_le_bytes()).unwrap(); // leak block 64
        assert_eq!(alloc.recover().unwrap(), 0);
        let len_after = alloc.stack().len().unwrap();
        // A second run finds nothing further and changes nothing.
        assert_eq!(alloc.recover().unwrap(), 0);
        assert_eq!(alloc.stack().len().unwrap(), len_after);
    }
}
