//! Crash-recoverable fixed-block slab allocator for [`BStack`]-backed storage.
//!
//! Provides [`CheckedSlabBStackAllocator`], a variant of
//! [`SlabBStackAllocator`](super::SlabBStackAllocator) that prefixes every block
//! with an 8-byte overhead field encoding whether the block is free or in use.
//! The overhead makes leaked blocks recoverable by a linear scan after a crash
//! and lets `dealloc` detect double-free at runtime before the free list can be
//! corrupted.

use super::{BStackAllocError, BStackAllocator, BStackOwnedSlice};
use crate::BStack;
#[cfg(feature = "atomic")]
use crate::BStackGenOp;
#[cfg(not(feature = "atomic"))]
use core::cell::Cell;
#[cfg(not(feature = "atomic"))]
use core::marker::PhantomData;
#[cfg(feature = "atomic")]
use std::sync::Mutex;
use std::{collections::HashSet, fmt, io};

#[cfg(feature = "set")]
const ALCK_MAGIC: [u8; 8] = *b"ALCK\x00\x01\x01\x00";

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
/// `CheckedSlabBStackAllocator` is always **`Send`** — ownership can be
/// transferred to another thread.
///
/// Without the `atomic` feature it is **not `Sync`**: free-list mutations read
/// then write `free_head` as separate [`BStack`] calls — a TOCTOU race under
/// concurrent `&self` access.
///
/// With the `atomic` feature it **is `Sync`**. Free-list push and pop —
/// [`alloc`](Self::alloc), [`dealloc`](Self::dealloc), and the free-list paths
/// of [`realloc`](Self::realloc) — use [`BStack::cross_exchange`] and
/// [`BStack::process_gen`] exactly as described for
/// [`SlabBStackAllocator`](super::SlabBStackAllocator), and need no
/// allocator-level lock. Tail grow/shrink paths use
/// [`BStack::try_extend_zeros`] / [`BStack::try_discard`] to perform
/// check-and-act atomically under `BStack`'s write lock, also without a lock.
///
/// An internal [`Mutex`] is retained solely to make [`recover`](Self::recover)
/// (and the automatic `recover` call in [`open`](Self::open)) single-flight:
/// the recovery *scan* itself is serialised against alloc/dealloc by the
/// [`BStack`] write lock it holds across one [`BStack::process_gen`] sequence,
/// while the `Mutex` only prevents two concurrent `recover` runs from each
/// reclaiming the same leaked block. It plays no part in ordinary
/// alloc/dealloc/realloc.
///
/// ```
/// fn assert_send<T: Send>() {}
/// assert_send::<bstack::CheckedSlabBStackAllocator>();
/// ```
///
/// Without `atomic` the type is `!Sync` (this fails to compile); with `atomic`
/// the internal `Mutex` makes it `Sync` (this compiles):
///
/// # Method safety
///
/// | Method | Atomicity | `BStack` op | Crash effect |
/// |--------|-----------|-------------|-------------|
/// | `new` | Atomic | Yes (`push`) | — |
/// | `open` | N/A (read-only + `recover`) | No | see `recover` |
/// | `recover` | Partial | No (multiple) | remaining leaks re-found on next `open` |
/// | `alloc(0)` | N/A (no I/O) | — | — |
/// | `alloc`, free-list hit | Partial | No (2) | popped block leaked |
/// | `alloc`, tail extend | Partial | No (2) | extended block leaked |
/// | `dealloc(null)` | N/A (no I/O) | — | — |
/// | `dealloc`, tail | Atomic | Yes (`discard`) | — |
/// | `dealloc`, free list | Partial | No (2) | freed blocks leaked |
/// | `realloc`, same block count | Partial | No (0–1) | — |
/// | `realloc`, tail grow | Partial | No (2) | capacity extension leaked |
/// | `realloc`, tail shrink | Partial | No (2) | orphaned tail left |
/// | `realloc`, shrink non-tail | Partial | No (3) | excess blocks leaked |
/// | `realloc`, grow non-tail | Partial | No (4–5) | old block leaked |
///
/// **Atomicity key:** *Atomic* — crash leaves the file fully consistent;
/// *Partial* — crash keeps the free list consistent but may leak ≤ 1 block or batch;
/// *N/A* — operation performs no I/O.
///
#[cfg_attr(not(feature = "atomic"), doc = "```compile_fail")]
#[cfg_attr(feature = "atomic", doc = "```")]
/// fn assert_sync<T: Sync>() {}
/// assert_sync::<bstack::CheckedSlabBStackAllocator>();
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
pub struct CheckedSlabBStackAllocator {
    stack: BStack,
    /// Cached from the on-disk header; fixed for the lifetime of the allocator.
    /// Covers the full block including the 8-byte overhead; must be `≥ 16`.
    block_size: u64,
    /// Serialises [`recover`](Self::recover) against itself when `atomic` is
    /// enabled, so two recovery runs cannot both reclaim the same leaked block.
    /// Ordinary alloc/dealloc/realloc never take it — they stay lock-free.
    #[cfg(feature = "atomic")]
    lock: Mutex<()>,
    #[cfg(not(feature = "atomic"))]
    _not_sync: PhantomData<Cell<()>>,
}

/// How a single block looks to the recovery scan.
///
/// Only used by the non-`atomic` recovery path; the `atomic` path inlines the
/// equivalent classification inside its `process_gen` state machine.
#[cfg(all(feature = "set", not(feature = "atomic")))]
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
///
/// Only used by the non-`atomic` recovery path; the `atomic` path inlines the
/// equivalent decision inside its `process_gen` state machine.
#[cfg(all(feature = "set", not(feature = "atomic")))]
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
            #[cfg(feature = "atomic")]
            lock: Mutex::new(()),
            #[cfg(not(feature = "atomic"))]
            _not_sync: PhantomData,
        })
    }

    /// Open an existing `CheckedSlabBStackAllocator` from a non-empty `stack`.
    ///
    /// Validates the `ALCK 0.1.x` magic prefix, reads `block_size`, and checks
    /// that `free_head` is either the sentinel or points to a block-aligned
    /// offset whose overhead is zero (free). On success, [`recover`](Self::recover)
    /// is called automatically to reclaim any blocks leaked by a previous unclean
    /// shutdown and to discard any orphaned tail left by a failed `realloc`
    /// truncation; this may rewrite free-list entries and truncate the backing
    /// stack before the allocator is returned.
    ///
    /// Returns a ready `CheckedSlabBStackAllocator` backed by `stack`.
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
        let arena_bytes = stack_len - Self::ARENA_START; // stack_len >= ARENA_START guaranteed above
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
            #[cfg(feature = "atomic")]
            lock: Mutex::new(()),
            #[cfg(not(feature = "atomic"))]
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
    /// # Phase 1 — scan (single locked pass)
    ///
    /// The free-list walk (with cycle detection) and the linear arena scan run
    /// as **one** [`BStack::process_gen`] sequence: every read happens under one
    /// held `BStack` write lock, so a concurrent [`alloc`](Self::alloc) or
    /// [`dealloc`](Self::dealloc) cannot mutate the free list between the moment
    /// it is walked and the moment a block is classified against it. A valid
    /// in-use block advances the cursor by its block count; a zero-overhead
    /// block not in the free list is a leak and is queued for reclaim; anything
    /// else is *suspicious*. At a suspicious block the scan resynchronises on the
    /// next free-list block if one follows (the intervening gap is left leaked);
    /// otherwise a backward reachability pass decides between a mid-arena gap
    /// (left leaked) and an orphaned tail from a failed `realloc` truncation. The
    /// orphaned-tail discard is the single mutation permitted in the sequence,
    /// expressed as a terminating [`BStackGenOp::Pop`]; because it is performed
    /// under the same lock as the scan that chose it, no concurrent `alloc` can
    /// extend the tail into the region being discarded.
    ///
    /// # Phase 2 — reclaim (one block at a time, lock-free)
    ///
    /// After the locked scan, each reclaimed block is prepended to the free
    /// list individually with `push_free_blocks` — the list is never
    /// rebuilt. This runs **outside** the scan lock and may overlap a
    /// concurrent `alloc`/`dealloc`: a reclaimed block is a *leak*, reachable by
    /// neither (no live slice points at it and it is absent from the free list),
    /// so its state cannot change between the scan and the splice, and each
    /// splice is itself an atomic [`BStack::cross_exchange`]. A crash mid-reclaim
    /// simply leaves the remaining leaks to be re-found on the next run.
    ///
    /// # Concurrency
    ///
    /// The allocator-level [`Mutex`] is held for the whole call. It no longer
    /// guards individual free-list reads — Phase 1 does that with the `BStack`
    /// write lock — but it serialises `recover` against **itself**: two
    /// overlapping runs could each observe the same block as leaked and splice
    /// it into the free list twice, corrupting the list. The lock makes recovery
    /// single-flight; ordinary `alloc`/`dealloc` never take it.
    ///
    /// # Safety of destructive steps
    ///
    /// A tail is discarded only when no free-list block lies at or beyond it, so
    /// `free_head` and every free block survive. If the free-list walk itself
    /// hits corruption, reclaim and tail-discard are both suppressed for that
    /// run and the uncertain blocks are merely counted — an unreliable free list
    /// never authorises relinking or truncation.
    ///
    /// # Errors
    ///
    /// * Any [`io::Error`] propagated from the underlying [`BStack`] operations
    ///   during the arena scan or while applying reclaim / tail-discard steps.
    #[cfg(feature = "atomic")]
    pub fn recover(&self) -> io::Result<u64> {
        // Held for the entire call. Phase 1's reads are serialised against
        // alloc/dealloc by the `BStack` write lock inside `process_gen`; this
        // lock instead prevents two `recover` runs from overlapping, which would
        // let both reclaim the same leaked block and double-link it.
        let _guard = self.lock.lock().unwrap();

        // Cheap early-out for an empty allocator. Only a hint: the authoritative
        // payload size is taken under the `process_gen` lock below via `Len`.
        if self.stack.len()? <= Self::ARENA_START {
            return Ok(0);
        }
        let bs = self.block_size;

        // Results produced by the locked scan, consumed after it returns.
        let mut reclaim: Vec<u64> = Vec::new();
        let mut unsure: u64 = 0;
        let mut discard_from: Option<u64> = None;

        // Resume points for the pull-driven `process_gen` state machine. Each
        // variant carries the cursor it operates on; shared working state
        // (free set, reach DP) lives in the captured locals below.
        #[derive(Clone, Copy)]
        enum St {
            ReadLen,
            ReadFreeHead,
            ConsumeFreeHead,
            WalkHead(u64),
            ConsumeNode(u64),
            ArenaAt(u64),
            ConsumeArena(u64),
            ConsumeResync,
            Finish,
            Done,
        }
        let mut st = St::ReadLen;
        // Authoritative payload size, read under the lock via `Len`.
        let mut stack_len: u64 = 0;
        // Sorted free-block offsets and whether the walk was cut short.
        let mut free: Vec<u64> = Vec::new();
        let mut free_corrupt = false;
        let mut seen: HashSet<u64> = HashSet::new();
        // Backward reach DP over a suspicious tail region `[resync_p, stack_len)`.
        let mut reach: Vec<bool> = Vec::new();
        let mut resync_p: u64 = 0;
        let mut rj: usize = 0;

        // Buffers filled by `Len`/`Read`; declared here so they outlive the
        // `process_gen` borrow. The transmutes detach the borrow from these
        // locals (see `pop_and_claim_block` for the same idiom); each is safe
        // because the local outlives the call and is never aliased while an op
        // holds the reference — `process_gen` resolves each op fully before it
        // calls the closure again.
        let mut len_out: u64 = 0;
        let mut head_buf = [0u8; 8];
        let mut node_buf = [0u8; 16];
        let mut oh_buf = [0u8; 8];

        self.stack.process_gen(|| {
            loop {
                match st {
                    // Read the authoritative payload size first.
                    St::ReadLen => {
                        st = St::ReadFreeHead;
                        return Some(BStackGenOp::Len {
                            // SAFETY: `len_out` outlives this `process_gen` call.
                            out: unsafe {
                                core::mem::transmute::<&mut u64, &mut u64>(&mut len_out)
                            },
                        });
                    }
                    St::ReadFreeHead => {
                        stack_len = len_out;
                        if stack_len <= Self::ARENA_START {
                            st = St::Finish;
                            continue;
                        }
                        st = St::ConsumeFreeHead;
                        return Some(BStackGenOp::Read {
                            offset: Self::FREE_HEAD_OFFSET,
                            // SAFETY: `head_buf` outlives this `process_gen` call.
                            buf: unsafe {
                                core::mem::transmute::<&mut [u8], &mut [u8]>(&mut head_buf[..])
                            },
                        });
                    }
                    St::ConsumeFreeHead => {
                        st = St::WalkHead(u64::from_le_bytes(head_buf));
                        continue;
                    }
                    // Free-list walk: validate `head`, then read its node.
                    St::WalkHead(head) => {
                        if head == Self::SENTINEL {
                            free.sort_unstable();
                            st = St::ArenaAt(Self::ARENA_START);
                            continue;
                        }
                        if head < Self::ARENA_START
                            || (head - Self::ARENA_START) % bs != 0
                            || head >= stack_len
                            || !seen.insert(head)
                        {
                            free_corrupt = true;
                            free.sort_unstable();
                            st = St::ArenaAt(Self::ARENA_START);
                            continue;
                        }
                        st = St::ConsumeNode(head);
                        return Some(BStackGenOp::Read {
                            offset: head,
                            // SAFETY: `node_buf` outlives this `process_gen` call.
                            buf: unsafe {
                                core::mem::transmute::<&mut [u8], &mut [u8]>(&mut node_buf[..])
                            },
                        });
                    }
                    St::ConsumeNode(head) => {
                        let overhead = u64::from_le_bytes(node_buf[0..8].try_into().unwrap());
                        if overhead != 0 {
                            free_corrupt = true;
                            free.sort_unstable();
                            st = St::ArenaAt(Self::ARENA_START);
                            continue;
                        }
                        free.push(head);
                        let next = u64::from_le_bytes(node_buf[8..16].try_into().unwrap());
                        st = St::WalkHead(next);
                        continue;
                    }
                    // Linear arena scan: read the overhead at `p`.
                    St::ArenaAt(p) => {
                        if p >= stack_len {
                            st = St::Finish;
                            continue;
                        }
                        st = St::ConsumeArena(p);
                        return Some(BStackGenOp::Read {
                            offset: p,
                            // SAFETY: `oh_buf` outlives this `process_gen` call.
                            buf: unsafe {
                                core::mem::transmute::<&mut [u8], &mut [u8]>(&mut oh_buf[..])
                            },
                        });
                    }
                    St::ConsumeArena(p) => {
                        let overhead = u64::from_le_bytes(oh_buf);
                        if overhead == 0 {
                            if free.binary_search(&p).is_ok() {
                                // Free: reachable from free_head.
                            } else if free_corrupt {
                                // A bare zero-overhead block is only trustworthy
                                // as a leak while the free list walked cleanly; a
                                // corrupt list means it might already be linked.
                                unsure += 1;
                            } else {
                                reclaim.push(p);
                            }
                            st = St::ArenaAt(p + bs);
                            continue;
                        }
                        if let Some(n) = self.valid_in_use(overhead, p, stack_len, &free) {
                            // valid_in_use guarantees p + n*bs <= stack_len.
                            st = St::ArenaAt(p + n * bs);
                            continue;
                        }
                        // Suspicious: prefer a known-free block as resync anchor.
                        let idx = free.partition_point(|&x| x <= p);
                        if let Some(&f) = free.get(idx) {
                            unsure += (f - p) / bs;
                            st = St::ArenaAt(f);
                            continue;
                        }
                        // No anchor follows: set up the backward reach DP over
                        // `[p, stack_len)` (see the non-atomic `resync_tail`).
                        let span = stack_len - p;
                        match usize::try_from(span / bs) {
                            Ok(m) if m <= Self::MAX_RECOVER_REGION => {
                                // m >= 1 since p < stack_len and both are aligned.
                                reach = vec![false; m + 1];
                                reach[m] = true;
                                resync_p = p;
                                rj = m - 1;
                                st = St::ConsumeResync;
                                return Some(BStackGenOp::Read {
                                    offset: resync_p + (rj as u64) * bs,
                                    // SAFETY: `oh_buf` outlives this call.
                                    buf: unsafe {
                                        core::mem::transmute::<&mut [u8], &mut [u8]>(
                                            &mut oh_buf[..],
                                        )
                                    },
                                });
                            }
                            // Region too large to analyse safely: leave leaked.
                            _ => {
                                unsure += span / bs;
                                st = St::Finish;
                                continue;
                            }
                        }
                    }
                    St::ConsumeResync => {
                        let overhead = u64::from_le_bytes(oh_buf);
                        let off = resync_p + (rj as u64) * bs;
                        reach[rj] = if overhead == 0 {
                            reach[rj + 1]
                        } else if let Some(n) = self.valid_in_use(overhead, off, stack_len, &free) {
                            // valid_in_use guarantees rj + n <= m.
                            reach[rj + n as usize]
                        } else {
                            false
                        };
                        if rj != 0 {
                            rj -= 1;
                            st = St::ConsumeResync;
                            return Some(BStackGenOp::Read {
                                offset: resync_p + (rj as u64) * bs,
                                // SAFETY: `oh_buf` outlives this call.
                                buf: unsafe {
                                    core::mem::transmute::<&mut [u8], &mut [u8]>(&mut oh_buf[..])
                                },
                            });
                        }
                        // DP complete. The smallest interior boundary that tiles
                        // cleanly to the tail is a mid-arena gap to resync on;
                        // j=0 is excluded (it would contradict the Suspicious
                        // classification that brought us here).
                        let m = reach.len() - 1;
                        if let Some(jr) = (1..m).find(|&j| reach[j]) {
                            unsure += jr as u64;
                            st = St::ArenaAt(resync_p + (jr as u64) * bs);
                            continue;
                        }
                        // Orphaned tail. Only safe to discard when the free list
                        // is trusted; otherwise leave it leaked.
                        if free_corrupt {
                            unsure += (stack_len - resync_p) / bs;
                        } else {
                            discard_from = Some(resync_p);
                        }
                        st = St::Finish;
                        continue;
                    }
                    // Emit the single permitted mutation: the tail discard.
                    // `Discard` drops the bytes without reading them, so no
                    // throwaway buffer is needed.
                    St::Finish => {
                        // No tail discard chosen: end the sequence with no write.
                        let t = discard_from?;
                        st = St::Done;
                        return Some(BStackGenOp::Discard { len: stack_len - t });
                    }
                    // `Discard` is terminal, so this is defensive only.
                    St::Done => return None,
                }
            }
        })?;

        // Phase 2: splice reclaimed leaks onto the free list, lock-free. Each
        // block is unreachable by alloc/dealloc, so its leaked state is stable
        // across the unlocked gap, and `push_free_blocks` splices atomically.
        for b in reclaim {
            self.push_free_blocks(b, 1)?;
        }
        Ok(unsure)
    }

    /// Repair the allocator after an unclean shutdown and return the number of
    /// blocks that remain leaked or could not be classified with certainty
    /// (`0` means the arena is fully accounted for).
    ///
    /// Single-threaded (non-`atomic`) variant: the free-list walk and arena
    /// scan run as ordinary sequential [`BStack`] reads, then any orphaned tail
    /// is discarded and each reclaimed block is prepended to the free list. See
    /// the `atomic` build for the crash- and concurrency-safety contract; here
    /// `&self` access is assumed exclusive.
    ///
    /// # Errors
    ///
    /// * Any [`io::Error`] propagated from the underlying [`BStack`] operations
    ///   during the arena scan or while applying reclaim / tail-discard steps.
    #[cfg(not(feature = "atomic"))]
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
    ///
    /// The `atomic` recovery path performs the equivalent walk inline inside
    /// its `process_gen` scan, so this helper is only compiled without `atomic`.
    #[cfg(not(feature = "atomic"))]
    fn scan_free_list(&self, stack_len: u64) -> io::Result<(Vec<u64>, bool)> {
        let mut free = Vec::new();
        let mut seen: HashSet<u64> = HashSet::new();
        let mut head = u64::from_le_bytes(read_bstack!(self.stack, Self::FREE_HEAD_OFFSET => u64));
        let mut corrupt = false;
        while head != Self::SENTINEL {
            if head < Self::ARENA_START
                || (head - Self::ARENA_START) % self.block_size != 0
                || head >= stack_len
                || !seen.insert(head)
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
    ///
    /// The `atomic` recovery path inlines this classification inside its
    /// `process_gen` scan, so this helper is only compiled without `atomic`.
    #[cfg(not(feature = "atomic"))]
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
        // Engulf check: the first free block strictly after `p` must not fall
        // inside `[p, end)`. A Suspicious block has non-zero overhead so it
        // cannot also be in `free` (scan_free_list rejects non-zero overhead),
        // which means `p` itself is never found by partition_point here.
        if let Some(&f) = free.get(free.partition_point(|&x| x <= p))
            && f < end
        {
            return None;
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
    ///
    /// The `atomic` recovery path runs the equivalent reach DP inline inside
    /// its `process_gen` scan, so this helper is only compiled without `atomic`.
    #[cfg(not(feature = "atomic"))]
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
        // j=0 is excluded: reach[0]=true would mean the block at `p` is itself
        // valid, contradicting the Suspicious classification that called us.
        if let Some(j) = (1..m).find(|&j| reach[j]) {
            return Ok(ResyncOutcome::Resync(p + (j as u64) * bs));
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
    ///
    /// # `atomic` feature
    ///
    /// The read of `free_head`, the read of the popped block's overhead and
    /// `next` pointer (`data[0..8]`), and the write that advances `free_head`
    /// run as a single [`BStack::process_gen`] sequence under one held write
    /// lock — see [`SlabBStackAllocator::pop_free_block`](super::SlabBStackAllocator).
    /// The final claim write (overhead + zeroed data) happens afterwards on
    /// the now-detached block, which is exclusively owned by this call.
    #[cfg(feature = "atomic")]
    fn pop_and_claim_block(&self, num_blocks: u64) -> io::Result<Option<u64>> {
        let mut head_buf = [0u8; 8];
        let mut prefix_buf = [0u8; 16];
        let mut step = 0usize;
        let mut head_opt: Option<u64> = None;
        let mut corrupt: Option<u64> = None;

        self.stack.process_gen(|| {
            let op = match step {
                // Step 0: read the current free-list head.
                0 => Some(BStackGenOp::Read {
                    offset: Self::FREE_HEAD_OFFSET,
                    // SAFETY: `head_buf` outlives this `process_gen` call.
                    buf: unsafe { core::mem::transmute::<&mut [u8], &mut [u8]>(&mut head_buf[..]) },
                }),
                // Step 1: an empty list ends the sequence with no write;
                // otherwise read the head block's overhead and next-pointer.
                1 => {
                    let head = u64::from_le_bytes(head_buf);
                    if head == Self::SENTINEL {
                        None
                    } else {
                        head_opt = Some(head);
                        Some(BStackGenOp::Read {
                            offset: head,
                            // SAFETY: `prefix_buf` outlives this `process_gen` call.
                            buf: unsafe {
                                core::mem::transmute::<&mut [u8], &mut [u8]>(&mut prefix_buf[..])
                            },
                        })
                    }
                }
                // Step 2: a non-zero overhead means the free list is corrupt —
                // end the sequence with no write and report the error after.
                // Otherwise advance free_head to the popped block's next
                // pointer, still under the lock acquired for step 0's read.
                2 => {
                    let overhead = u64::from_le_bytes(prefix_buf[0..8].try_into().unwrap());
                    if overhead != 0 {
                        corrupt = Some(overhead);
                        None
                    } else {
                        Some(BStackGenOp::Write {
                            offset: Self::FREE_HEAD_OFFSET,
                            // SAFETY: `prefix_buf` outlives this `process_gen` call.
                            data: unsafe {
                                core::mem::transmute::<&[u8], &[u8]>(&prefix_buf[8..16])
                            },
                        })
                    }
                }
                _ => None,
            };
            step += 1;
            op
        })?;

        let Some(head) = head_opt else {
            return Ok(None);
        };
        if let Some(overhead) = corrupt {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "free-list block at {head} has non-zero overhead {overhead:#018x}; free list corrupt"
                ),
            ));
        }
        // Mark in-use and zero data in one write.
        let mut block_buf = vec![0u8; self.block_size as usize]; // safe: validated in new/open
        block_buf[..8].copy_from_slice(&(Self::IN_USE_BIT | num_blocks).to_le_bytes());
        self.stack.set(head, block_buf)?;
        Ok(Some(head))
    }

    /// Pop the head block off the free list, mark it in use with `num_blocks`,
    /// and return its block start offset, or `None` if the list is empty.
    ///
    /// Advances `free_head` then writes the full block in one call: the overhead
    /// is set to `IN_USE_BIT | num_blocks` and the data bytes are zeroed. A
    /// crash between the two writes merely leaks the detached block.
    #[cfg(not(feature = "atomic"))]
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
    /// starting at `first_block`, linking them into a chain. All blocks'
    /// overhead is cleared (transitioning a live allocation into free blocks)
    /// and `data[0..8]` is set to the next block's offset.
    ///
    /// # `atomic` feature
    ///
    /// The last block's `data[0..8]` is set to the placeholder `first_block`;
    /// [`push_free_blocks`](Self::push_free_blocks) splices the whole run onto
    /// `free_head` with [`BStack::cross_exchange`] afterwards. Does **not**
    /// touch `free_head`.
    ///
    /// # non-`atomic`
    ///
    /// The last block's `data[0..8]` is set to the current `free_head`, whose
    /// value the caller then writes into `free_head` directly. Does **not**
    /// update `free_head`.
    ///
    /// The single bulk [`BStack::set`] makes this crash-safe: until the run is
    /// spliced in (or `free_head` is repointed), it is simply unreachable.
    fn write_free_run(&self, first_block: u64, count: u64) -> io::Result<()> {
        debug_assert!(count > 0);
        #[cfg(not(feature = "atomic"))]
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
                #[cfg(feature = "atomic")]
                {
                    // Placeholder: replaced with the old free_head by
                    // cross_exchange in push_free_blocks.
                    first_block.to_le_bytes()
                }
                #[cfg(not(feature = "atomic"))]
                {
                    old_head
                }
            };
            // Overhead at buf[base..base+8] stays zero; next pointer at data[0..8].
            buf[base + 8..base + 16].copy_from_slice(&next_bytes);
        }
        self.stack.set(first_block, buf)
    }

    /// Prepend `count` contiguous blocks starting at `first_block` to the free
    /// list. The blocks' overhead bytes are cleared as part of the operation,
    /// so this also transitions a live allocation into free blocks.
    ///
    /// # `atomic` feature
    ///
    /// [`write_free_run`](Self::write_free_run) writes the chain with the last
    /// block's `data[0..8]` set to the placeholder `first_block`. A single
    /// [`BStack::cross_exchange`] then atomically swaps that slot with
    /// `free_head`: `free_head` becomes `first_block` (the new head) and the
    /// last block's next-pointer becomes the old head — splicing the whole run
    /// in under one write lock, lock-free. For `count == 1`, the first and last
    /// block are the same, exactly mirroring
    /// [`SlabBStackAllocator::push_free_block`](super::SlabBStackAllocator).
    fn push_free_blocks(&self, first_block: u64, count: u64) -> io::Result<()> {
        if count == 0 {
            return Ok(());
        }
        self.write_free_run(first_block, count)?;

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
                .cross_exchange(last_block + Self::OVERHEAD, Self::FREE_HEAD_OFFSET, 8)
        }
        #[cfg(not(feature = "atomic"))]
        {
            self.stack
                .set(Self::FREE_HEAD_OFFSET, first_block.to_le_bytes())
        }
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
    type Allocated<'a> = BStackOwnedSlice<'a, Self>;

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
    ///
    /// # Crash consistency
    ///
    /// | Path | Calls | Safety |
    /// |------|-------|--------|
    /// | `len == 0` | 0 | trivially safe |
    /// | free-list hit | 2 (`set` + `set`) | crash may leak popped block |
    /// | tail extend, single block | 2 (`extend` + `set`) | crash may leak extended block |
    /// | tail extend, multi-block | 2 (`extend` + `set`) | crash may leak extended blocks |
    fn alloc(&self, len: u64) -> io::Result<BStackOwnedSlice<'_, Self>> {
        if len == 0 {
            return Ok(BStackOwnedSlice::empty(self));
        }

        let num_blocks = self.blocks_needed(len)?;

        if num_blocks == 1
            && let Some(block_start) = self.pop_and_claim_block(1)?
        {
            // SAFETY:
            // 1. No overflow: `block_start` is a valid in-bounds arena offset;
            //    `block_start + OVERHEAD + len ≤ block_start + block_size ≤ stack_len ≤ u64::MAX`
            //    because `blocks_needed(len) == 1` implies `len ≤ data_size = block_size − OVERHEAD`.
            // 2. In bounds: the block was just popped from the free list and
            //    marked in-use by `pop_and_claim_block`; the full `block_size`
            //    region is part of the arena and present in the stack payload.
            // 3. Alloc origin: the slice spans exactly the data region of this
            //    allocation and may safely be passed to `dealloc`/`realloc`.
            return Ok(unsafe {
                BStackOwnedSlice::from_raw_parts(self, block_start + Self::OVERHEAD, len)
            });
        }

        // Tail-extend path (single-block with no free block, and all multi-block
        // allocations). No lock needed: BStack::extend is internally serialised by
        // its write lock and returns a distinct region to each concurrent caller;
        // write_overhead then writes only to that exclusively-owned region.
        let total = num_blocks.checked_mul(self.block_size).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "allocation size overflows u64")
        })?;
        let block_start = self.stack.extend(total)?;
        self.write_overhead(block_start, Self::IN_USE_BIT | num_blocks)?;
        // SAFETY:
        // 1. No overflow: `block_start + OVERHEAD + len ≤ block_start + total ≤ new stack_len ≤ u64::MAX`
        //    because `total = num_blocks * block_size` and `OVERHEAD + len ≤ num_blocks * block_size`
        //    by the definition of `blocks_needed`.
        // 2. In bounds: `stack.extend` just appended exactly `total` zeroed bytes;
        //    the full range is within the stack payload.
        // 3. Alloc origin: the slice covers the data region of this fresh
        //    allocation and may safely be passed to `dealloc`/`realloc`.
        Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, block_start + Self::OVERHEAD, len) })
    }

    /// Release the region described by `slice`.
    ///
    /// Reads the overhead at the block start (`slice.start() − 8`). If the
    /// high bit is clear the block is already free and a double-free error is
    /// returned without touching any list. A multi-block allocation at the tail
    /// is reclaimed with a single [`BStack::discard`]; otherwise every block is
    /// prepended to the free list.
    ///
    /// Passing the null/empty sentinel slice (`start == 0, len == 0`) is a
    /// no-op that returns `Ok(())`.
    ///
    /// # Errors
    ///
    /// * [`io::ErrorKind::InvalidInput`] — `slice.start()` is below the
    ///   overhead prefix offset, or the block's overhead high bit is clear
    ///   (double-free detected).
    /// * [`io::ErrorKind::InvalidData`] — the in-use overhead records a zero
    ///   block count (metadata corrupt).
    /// * Any [`io::Error`] propagated from the underlying [`BStack`] operations.
    ///
    /// # Crash consistency
    ///
    /// | Path | Calls | Safety |
    /// |------|-------|--------|
    /// | null slice | 0 | trivially safe |
    /// | tail (any block count) | 1 (`discard`) | crash-safe by inheritance |
    /// | free list | 2 (`set` + `set`) | crash leaks freed blocks; double-free guard unaffected |
    fn dealloc<'a>(
        &'a self,
        slice: BStackOwnedSlice<'a, Self>,
    ) -> Result<(), BStackAllocError<'a, Self>> {
        let start = slice.start();
        let len = slice.len();
        // Set once the caller's blocks may have been partially freed, after
        // which returning the handle for retry would risk a double-free.
        let mut lost = false;
        let result = (|| -> io::Result<()> {
            if slice.is_empty() && slice.start() == 0 {
                return Ok(());
            }

            let block_start = slice.start().checked_sub(Self::OVERHEAD).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "slice start is below the overhead prefix; not a valid allocation",
                )
            })?;
            // read_overhead is a single BStack read from a block owned by the caller;
            // no lock required here.
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

            let slice_end = block_start.checked_add(backing).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "deallocation end offset overflows u64",
                )
            })?;

            // Tail path: try_discard atomically checks tail == slice_end and removes
            // backing bytes under BStack's own write lock — no allocator lock needed.
            #[cfg(feature = "atomic")]
            if self.stack.try_discard(slice_end, backing)? {
                return Ok(());
            }

            // Non-atomic tail path.
            #[cfg(not(feature = "atomic"))]
            if slice_end == self.stack.len()? {
                return self.stack.discard(backing);
            }

            // Not at tail: push to the free list. This mutates the block
            // overhead and multiple free-list links, so a mid-way failure may
            // leave the blocks partially freed — the handle can no longer be
            // safely returned.
            lost = true;
            self.push_free_blocks(block_start, num_blocks)
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
    /// Returns a (possibly different) slice covering the resized region. If
    /// `slice` is the null/empty sentinel (`start == 0, len == 0`), delegates
    /// to [`alloc`](Self::alloc). If `new_len == 0`, deallocates `slice` and
    /// returns the null sentinel.
    ///
    /// # Resize strategies
    ///
    /// | Case | Strategy |
    /// |------|----------|
    /// | Same block count | Adjust visible length, zeroing newly-exposed bytes on grow |
    /// | Slice at tail | Extend or discard the tail and update the overhead count |
    /// | Shrink, non-tail | Recycle excess blocks into the free list, then shrink the overhead |
    /// | Grow, non-tail | Allocate a fresh region, copy, release the old |
    ///
    /// # Errors
    ///
    /// * [`io::ErrorKind::InvalidInput`] — `slice.start()` is below the
    ///   overhead prefix offset, or the block's overhead high bit is clear
    ///   (realloc of a freed or invalid block).
    /// * Any [`io::Error`] propagated from the underlying [`BStack`] operations.
    ///
    /// # Crash consistency
    ///
    /// | Case | Calls | Safety |
    /// |------|-------|--------|
    /// | same block count | 0–1 (`zero`) | trivially safe |
    /// | tail shrink | 2 (`set` + `discard`) | crash before `discard` leaves orphaned tail; reclaimed by `recover` |
    /// | tail grow | 2 (`extend` + `set`) | crash leaks capacity extension; original data intact |
    /// | shrink non-tail | 3 (`set` + `set` + `set`) | crash leaks excess blocks; no live allocation corrupted |
    /// | grow non-tail | 4–5 (`alloc` + copy + `dealloc`) | crash leaks old block; new allocation is consistent |
    fn realloc<'a>(
        &'a self,
        slice: BStackOwnedSlice<'a, Self>,
        new_len: u64,
    ) -> Result<BStackOwnedSlice<'a, Self>, BStackAllocError<'a, Self>> {
        if slice.is_empty() && slice.start() == 0 {
            return self.alloc(new_len).map_err(|source| {
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
        // original block, becomes the new region once a non-tail grow commits
        // it, and becomes the shrunk view once the overhead commit point is
        // written (see below). Every failure therefore has a valid handle.
        let mut recovered = (start, old_len);
        let result = (|| -> io::Result<BStackOwnedSlice<'a, Self>> {
            // Overhead read, validation, and same-block-count handling are lock-free:
            // read_overhead is a single BStack read from a caller-owned block, and
            // stack.zero writes to caller-owned bytes — no shared state is touched.
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
            if old_n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "in-use block records a zero block count; metadata corrupt",
                ));
            }
            let new_n = self.blocks_needed(new_len)?;

            if old_n == new_n {
                // Same backing blocks: zero newly-exposed bytes then adjust length.
                if new_len > slice.len() {
                    self.stack.zero(slice.end(), new_len - slice.len())?;
                }
                // SAFETY:
                // 1. No overflow: `slice.start() + new_len ≤ slice.start() + old_n * block_size − OVERHEAD ≤ u64::MAX`
                //    because `old_n == new_n` and `new_len ≤ new_n * block_size − OVERHEAD` by `blocks_needed`.
                // 2. In bounds: same backing blocks are retained; the range is within the payload.
                // 3. Alloc origin: `slice.start()` is the original allocation data offset;
                //    the slice may safely be passed to `dealloc`/`realloc`.
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
            // Precompute the expected tail for this allocation; used by both paths.
            let sentinel = block_start.checked_add(old_backing).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "tail check overflows u64")
            })?;

            if new_n > old_n {
                // Grow path.
                //
                // With `atomic`: try_extend_zeros atomically checks tail == sentinel and
                // appends the delta under BStack's own write lock, so no allocator lock
                // is needed. write_overhead then writes only to the exclusively-owned
                // newly-extended region. If try_extend_zeros returns false the tail has
                // moved and we are no longer the tail block — fall through to grow non-tail.
                //
                // Without `atomic`: a plain len() check followed by extend is safe in a
                // single-threaded context.
                #[cfg(feature = "atomic")]
                if self
                    .stack
                    .try_extend_zeros(sentinel, new_backing - old_backing)?
                {
                    if new_len > slice.len() {
                        self.stack.zero(slice.end(), new_len - slice.len())?;
                    }
                    self.write_overhead(block_start, Self::IN_USE_BIT | new_n)?;
                    // SAFETY:
                    // 1. No overflow: slice.start() + new_len ≤ block_start + OVERHEAD + new_n * block_size − OVERHEAD ≤ u64::MAX
                    //    because new_n * block_size ≤ new_backing ≤ stack_len.
                    // 2. In bounds: try_extend_zeros just extended the tail by new_backing − old_backing bytes.
                    // 3. Alloc origin: slice.start() is unchanged; overhead records new_n.
                    return Ok(unsafe {
                        BStackOwnedSlice::from_raw_parts(self, slice.start(), new_len)
                    });
                }

                #[cfg(not(feature = "atomic"))]
                if sentinel == self.stack.len()? {
                    self.stack.extend(new_backing - old_backing)?;
                    if new_len > slice.len() {
                        self.stack.zero(slice.end(), new_len - slice.len())?;
                    }
                    self.write_overhead(block_start, Self::IN_USE_BIT | new_n)?;
                    // SAFETY: same invariants as the atomic path above.
                    return Ok(unsafe {
                        BStackOwnedSlice::from_raw_parts(self, slice.start(), new_len)
                    });
                }

                // Not at tail (or tail moved under atomic): grow non-tail.
                // alloc and dealloc each handle their own free-list and tail
                // operations independently.
                let mut new_slice = self.alloc(new_len)?;
                let data = slice.read()?;
                new_slice.write(&data)?;
                // New region committed and populated; it is now the survivor, so a
                // failure freeing the old block returns the new region instead.
                recovered = (new_slice.start(), new_len);
                self.dealloc(slice).map_err(|e| e.source)?;
                return Ok(new_slice);
            }

            // Shrink path (new_n < old_n).
            // Overhead is the commit point for both tail and non-tail paths: write
            // it first so a crash after this point leaves an orphaned (but safely
            // unreferenced) tail region or leaked blocks that recover() can reclaim,
            // rather than an overhead that claims more blocks than the file contains.
            self.write_overhead(block_start, Self::IN_USE_BIT | new_n)?;
            // This is the shrink commit point: the block now records new_n blocks,
            // so the resized view is the survivor for any subsequent failure.
            recovered = (start, new_len);

            // Tail shrink: try_discard atomically checks tail == sentinel and
            // removes the excess under bstack's write lock, so no other thread can
            // race between the check and the truncation. On failure the slice is
            // not at the tail; fall through to recycle the excess blocks.
            #[cfg(feature = "atomic")]
            if self
                .stack
                .try_discard(sentinel, old_backing - new_backing)?
            {
                // SAFETY:
                // 1. No overflow: slice.start() + new_len ≤ block_start + OVERHEAD + new_n * block_size − OVERHEAD ≤ u64::MAX
                //    because new_n * block_size ≤ new_backing ≤ stack_len.
                // 2. In bounds: tail discarded down to new_backing.
                // 3. Alloc origin: slice.start() is unchanged; overhead records new_n.
                return Ok(unsafe {
                    BStackOwnedSlice::from_raw_parts(self, slice.start(), new_len)
                });
            }

            #[cfg(not(feature = "atomic"))]
            if sentinel == self.stack.len()? {
                self.stack.discard(old_backing - new_backing)?;
                // SAFETY:
                // 1. No overflow: slice.start() + new_len ≤ block_start + OVERHEAD + new_n * block_size − OVERHEAD ≤ u64::MAX
                //    because new_n * block_size ≤ new_backing ≤ stack_len.
                // 2. In bounds: tail discarded down to new_backing.
                // 3. Alloc origin: slice.start() is unchanged; overhead records new_n.
                return Ok(unsafe {
                    BStackOwnedSlice::from_raw_parts(self, slice.start(), new_len)
                });
            }

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
            //
            // Overhead was already written above (the commit point).
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
            self.push_free_blocks(excess_start, old_n - new_n)?;
            // SAFETY:
            // 1. No overflow: slice.start() + new_len ≤ block_start + OVERHEAD + new_n * block_size − OVERHEAD ≤ u64::MAX
            //    because new_n * block_size ≤ old_backing ≤ stack_len.
            // 2. In bounds: the first new_n blocks are still live in the stack payload.
            // 3. Alloc origin: slice.start() is unchanged; overhead records new_n.
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

#[cfg(all(test, feature = "set"))]
mod _assertions {
    use super::CheckedSlabBStackAllocator;
    fn _send()
    where
        CheckedSlabBStackAllocator: Send,
    {
    }
    #[cfg(feature = "atomic")]
    fn _sync()
    where
        CheckedSlabBStackAllocator: Sync,
    {
    }
}

#[cfg(all(test, feature = "set"))]
mod tests {
    use super::CheckedSlabBStackAllocator;
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
        let start = s.start();
        let len = s.len();
        alloc.dealloc(s).unwrap();
        // Deliberately construct a duplicate handle to test double-free detection.
        // SAFETY: testing runtime double-free detection; the block is already freed.
        let s2 = unsafe { crate::alloc::BStackOwnedSlice::from_raw_parts(&alloc, start, len) };
        let err = alloc.dealloc(s2).unwrap_err();
        assert_eq!(err.source.kind(), ErrorKind::InvalidInput);
        // Double-free is caught before any freeing, so the handle is returned.
        assert!(err.handle.is_some());
    }

    #[test]
    fn write_and_read_round_trip() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = CheckedSlabBStackAllocator::new(stack, 16).unwrap();
        let mut s = alloc.alloc(12).unwrap();
        s.write(b"hello world!").unwrap();
        assert_eq!(s.read().unwrap(), b"hello world!");
    }

    #[test]
    fn data_survives_reopen() {
        let (stack, path) = empty_stack();
        let _g = Guard(path.clone());
        let alloc = CheckedSlabBStackAllocator::new(stack, 8).unwrap();
        let mut s = alloc.alloc(5).unwrap();
        let offset = s.start();
        s.write(b"hello").unwrap();
        drop(alloc);

        let alloc2 = CheckedSlabBStackAllocator::open(BStack::open(&path).unwrap()).unwrap();
        let s2 = unsafe { BStackSlice::from_raw_parts(alloc2.stack(), offset, 5) };
        assert_eq!(s2.read().unwrap(), b"hello");
    }

    #[test]
    fn multiblock_alloc_round_trip_and_free_reuse() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = CheckedSlabBStackAllocator::new(stack, 8).unwrap();

        // data_size=8, block_size=16: 40 bytes needs ceil((40+8)/16) = 3 blocks.
        let mut s = alloc.alloc(40).unwrap();
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
        let mut s = alloc.alloc(8).unwrap();
        s.write(b"abcdefgh").unwrap();
        let s_start = s.start();
        let s2 = alloc.realloc(s, 16).unwrap();
        assert_eq!(s2.start(), s_start);
        let data = s2.read().unwrap();
        assert_eq!(&data[..8], b"abcdefgh");
        assert_eq!(&data[8..], &[0u8; 8]); // newly-exposed bytes are zeroed
    }

    #[test]
    fn realloc_grow_preserves_data_across_blocks() {
        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = CheckedSlabBStackAllocator::new(stack, 8).unwrap();
        let mut s = alloc.alloc(8).unwrap();
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
        let mut s = alloc.alloc(40).unwrap(); // 3 blocks
        let payload: Vec<u8> = (0..40u8).collect();
        s.write(&payload).unwrap();
        let s_start = s.start();
        let _guard_block = alloc.alloc(8).unwrap();

        // Shrink to a single block; the two freed blocks become reusable.
        let s2 = alloc.realloc(s, 8).unwrap();
        assert_eq!(s2.start(), s_start);
        assert_eq!(s2.read().unwrap(), &payload[..8]);

        // The recycled blocks are now served from the free list.
        let r = alloc.alloc(8).unwrap();
        assert_eq!(r.start(), s_start + 16);
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
        let mut s = alloc.alloc(40).unwrap(); // 3 blocks: block 48, stack -> 96
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
        let mut c = alloc.alloc(8).unwrap(); // block 80, slice start 88
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
            let mut s = alloc.alloc(40).unwrap(); // 3 blocks, stack -> 96
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

    // ── concurrent (feature = "atomic") ───────────────────────────────────────

    #[cfg(feature = "atomic")]
    #[test]
    fn concurrent_alloc_dealloc_no_live_duplicates() {
        use std::collections::HashSet;
        use std::sync::{Arc, Mutex};
        use std::thread;

        // Verify that concurrent alloc/dealloc calls never hand the same block
        // to two callers simultaneously.  Each thread claims a block, inserts
        // its offset into a shared live-set (asserting uniqueness), writes its
        // thread id, reads back and verifies the data, then removes the offset
        // and deallocates.  The free-list lock is what makes this safe; a bug
        // there would produce a duplicate entry in the set.
        const THREADS: usize = 8;
        const ROUNDS: usize = 200;

        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = Arc::new(CheckedSlabBStackAllocator::new(stack, 8).unwrap());
        let live: Arc<Mutex<HashSet<u64>>> = Arc::new(Mutex::new(HashSet::new()));

        let handles: Vec<_> = (0..THREADS)
            .map(|tid| {
                let alloc = Arc::clone(&alloc);
                let live = Arc::clone(&live);
                thread::spawn(move || {
                    let a: &CheckedSlabBStackAllocator = &alloc;
                    for _ in 0..ROUNDS {
                        let mut slice = a.alloc(8).unwrap();
                        let off = slice.start();
                        {
                            let mut set = live.lock().unwrap();
                            assert!(set.insert(off), "duplicate live offset {off}");
                        }
                        slice.write(&[tid as u8; 8]).unwrap();
                        let data = slice.read().unwrap();
                        assert_eq!(data, vec![tid as u8; 8]);
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

        // All threads done: the allocator should be fully consistent.
        assert_eq!(alloc.recover().unwrap(), 0);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn concurrent_realloc_hammers_tail_paths() {
        use std::sync::Arc;
        use std::thread;

        // T threads each own one allocation and repeatedly grow then shrink it.
        // Whichever allocation sits at the tail exercises try_extend_zeros /
        // try_discard; the others hit the non-tail copy-grow / block-recycle
        // paths.  Because threads race, both branches are exercised on every
        // round.  Verify that each thread's data survives every round intact.
        //
        // With data_size = 8 (block_size = 16):
        //   SMALL = 8  → blocks_needed = ceil((8+8)/16)  = 1 block
        //   LARGE = 32 → blocks_needed = ceil((32+8)/16) = 3 blocks
        const THREADS: usize = 6;
        const ROUNDS: usize = 150;
        const SMALL: u64 = 8;
        const LARGE: u64 = 32;

        let (stack, path) = empty_stack();
        let _g = Guard(path);
        let alloc = Arc::new(CheckedSlabBStackAllocator::new(stack, 8).unwrap());

        let handles: Vec<_> = (0..THREADS)
            .map(|tid| {
                let alloc = Arc::clone(&alloc);
                thread::spawn(move || {
                    let a: &CheckedSlabBStackAllocator = &alloc;
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

        assert_eq!(alloc.recover().unwrap(), 0);
    }
}
