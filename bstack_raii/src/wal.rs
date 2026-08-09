//! Write-ahead log data model for atomic multi-slice transactions.
//!
//! ## The categories
//!
//! An on-disk **slice** `S = (ptr, len)` is allocated/freed by the adjoint
//! functors `Alloc`/`Dealloc`. Because `Alloc ∘ Dealloc` and `Dealloc ∘ Alloc`
//! are identities, slices form a groupoid — which is what licenses the
//! [reduction](reduce) optimisation (reuse a freed slice for an equal-length
//! allocation instead of freeing then re-allocating).
//!
//! But `Alloc` is *non-deterministic in address*: two requests for the same
//! length yield different slices. So we split the "slice" notion into a
//! requirement category `R = (len)` and a slice category `S`, bridged by an
//! address-preserving `R' = (id, len)`:
//!
//! ```text
//! R --Choice--> R' --Alloc--> S       S --Dealloc--> R' --ForgetAddress--> R
//! ```
//!
//! `Alloc: R' → S` and `Dealloc: S → R'` are adjoint, and equal-length slices are
//! interchangeable (`ForgetAddress ∘ Dealloc ∘ Alloc ∘ Choice = id`).
//!
//! ## The log
//!
//! A traditional durable-monoid WAL can't work here: `Alloc`/`Dealloc` aren't
//! idempotent, so operations can't be replayed. Instead each operation carries a
//! [`WalStatus`] from the ordered set `{None < Pending < Complete}` (plus the
//! recovery sink `Abandon`), with two monotonic maps:
//!
//! * normal progress [`advance`](WalStatus::advance): `None → Pending → Complete`;
//! * [`recover`](WalStatus::recover): `Pending → Abandon`, else identity.
//!
//! An operation is thus `(slice, Alloc|Dealloc) × Status`, and the WAL is the
//! functor `wal_append` mapping operations into disk state ([`WalLog`]). On disk
//! **both** an `Alloc` and a `Dealloc` store their slice `S = (ptr, len)`; the
//! `op` marks the *recovery polarity* (which outcome orphans the slice).
//! ([`AllocReq`] / [`reduce`] are the pre-allocation planning form, `R' = (id,
//! len)`, used before an address exists.)
//!
//! Recovery semantics ([`finish`]), driven by the transaction-level
//! `txn_status`, **reclaims** rather than merely staying consistent:
//!
//! * **committed** → free each `Pending` `Dealloc` (the old blocks the op
//!   unlinked) — roll forward;
//! * **abandoned** → free each `Pending` `Alloc` (the new blocks a crashed op
//!   allocated but never linked) — reclaim the orphans.
//!
//! Each entry self-brackets (`persist Complete → free`), so a second crash mid-
//! completion never double-frees; no separate cursor is needed.

use core::mem::size_of;
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex, OnceLock};

use bstack::{BStackAllocator, BStackOwnedSliceAllocator, BStackRange};
use bytemuck::{Pod, Zeroable};

use crate::teardown::dealloc_range;

/// `R'`: an allocation requirement carrying identity — a length whose address has
/// been "forgotten", plus an `id` that keeps equal-length requirements distinct.
/// The `id` is a wrapping autoincrement (see [`WalLog::fresh_id`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AllocReq {
    pub id: u64,
    pub len: u64,
}

/// The status lifecycle of a WAL operation: the ordered set
/// `{None < Pending < Complete}` plus the recovery sink `Abandon`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum WalStatus {
    None = 0,
    Pending = 1,
    Complete = 2,
    Abandon = 3,
}

impl WalStatus {
    /// Normal monotonic progress: `None → Pending → Complete` (idempotent at
    /// `Complete`; `Abandon` is terminal).
    pub fn advance(self) -> Self {
        match self {
            WalStatus::None => WalStatus::Pending,
            WalStatus::Pending => WalStatus::Complete,
            other => other,
        }
    }

    /// Recovery monotonic map: `Pending → Abandon` (an in-flight op is abandoned,
    /// its slice leaked rather than re-run); `None` and `Complete` are unchanged.
    pub fn recover(self) -> Self {
        match self {
            WalStatus::Pending => WalStatus::Abandon,
            other => other,
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            0 => WalStatus::None,
            1 => WalStatus::Pending,
            2 => WalStatus::Complete,
            // Any other byte (incl. 3 and corruption) is treated as `Abandon` —
            // the safe sink: never run, leak.
            _ => WalStatus::Abandon,
        }
    }
}

/// The two morphisms of the slice groupoid, as recorded in the log.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum WalOp {
    Alloc = 0,
    Dealloc = 1,
}

impl WalOp {
    fn from_u8(v: u8) -> Self {
        match v {
            1 => WalOp::Dealloc,
            _ => WalOp::Alloc,
        }
    }
}

/// One on-disk WAL entry: `(status, op, payload)`.
///
/// `status` and `op` are **separate** fields — never packed into one byte. The two
/// payload words are `R' = (id, len)` for an [`Alloc`](WalOp::Alloc) and
/// `S = (ptr, len)` for a [`Dealloc`](WalOp::Dealloc). 24 bytes, 8-aligned, `Pod`.
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct WalEntry {
    status: u8,
    op: u8,
    _pad: [u8; 6],
    /// `Alloc`: requirement `id`. `Dealloc`: slice `ptr` (start offset).
    word_a: u64,
    /// `Alloc` / `Dealloc`: `len`.
    word_b: u64,
}

impl WalEntry {
    /// An `Alloc` entry recording a freshly allocated slice `S = (ptr, len)`.
    /// Recovery frees it iff the transaction is **abandoned** (the block is an
    /// orphan of a crashed op); a committed transaction keeps it.
    pub fn alloc(status: WalStatus, slice: BStackRange) -> Self {
        WalEntry {
            status: status as u8,
            op: WalOp::Alloc as u8,
            _pad: [0; 6],
            word_a: slice.start(),
            word_b: slice.len(),
        }
    }

    /// A `Dealloc` entry recording a concrete slice `S = (ptr, len)`.
    pub fn dealloc(status: WalStatus, slice: BStackRange) -> Self {
        WalEntry {
            status: status as u8,
            op: WalOp::Dealloc as u8,
            _pad: [0; 6],
            word_a: slice.start(),
            word_b: slice.len(),
        }
    }

    pub fn status(&self) -> WalStatus {
        WalStatus::from_u8(self.status)
    }

    pub fn op(&self) -> WalOp {
        WalOp::from_u8(self.op)
    }

    pub fn set_status(&mut self, status: WalStatus) {
        self.status = status as u8;
    }

    /// The recorded slice `S`, if this is an `Alloc` entry (to be freed on abandon).
    pub fn as_alloc(&self) -> Option<BStackRange> {
        match self.op() {
            WalOp::Alloc => Some(BStackRange::new(self.word_a, self.word_b)),
            WalOp::Dealloc => None,
        }
    }

    /// The recorded slice `S`, if this is a `Dealloc` entry.
    pub fn as_dealloc(&self) -> Option<BStackRange> {
        match self.op() {
            WalOp::Dealloc => Some(BStackRange::new(self.word_a, self.word_b)),
            WalOp::Alloc => None,
        }
    }
}

/// In-memory write-ahead log: a vector of [`WalEntry`] plus the `R'` id counter.
///
/// [`with_capacity`](Self::with_capacity) pre-reserves to the known operation
/// count (a transaction knows how many allocs/deallocs it will log up front), so
/// [`append`](Self::append) never reallocates mid-transaction.
pub struct WalLog {
    entries: Vec<WalEntry>,
    next_id: u64,
}

impl WalLog {
    /// A log pre-reserved for `ops` entries.
    pub fn with_capacity(ops: usize) -> Self {
        WalLog {
            entries: Vec::with_capacity(ops),
            next_id: 0,
        }
    }

    /// The next `R'` identity — a wrapping autoincrement.
    pub fn fresh_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        id
    }

    /// Append an operation to the log (`wal_append`).
    pub fn append(&mut self, entry: WalEntry) {
        self.entries.push(entry);
    }

    pub fn entries(&self) -> &[WalEntry] {
        &self.entries
    }

    pub fn entries_mut(&mut self) -> &mut [WalEntry] {
        &mut self.entries
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The log's on-disk image (a packed array of [`WalEntry`]).
    pub fn as_bytes(&self) -> &[u8] {
        bytemuck::cast_slice(&self.entries)
    }

    /// Parse a log image back into entries (unaligned-safe, so a raw disk buffer
    /// works). Trailing bytes shorter than one entry are ignored.
    pub fn entries_from_bytes(bytes: &[u8]) -> Vec<WalEntry> {
        let sz = size_of::<WalEntry>();
        let count = bytes.len() / sz;
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            out.push(bytemuck::pod_read_unaligned(&bytes[i * sz..(i + 1) * sz]));
        }
        out
    }
}

/// The result of [`reduce`]: allocation requirements that were satisfied by
/// repurposing a freed slice (`reused`), and the physical operations that remain.
#[derive(Debug, Default)]
pub struct Reduced {
    /// `(requirement, repurposed slice)` — no physical alloc *or* dealloc needed.
    pub reused: Vec<(AllocReq, BStackRange)>,
    /// Requirements still needing a physical `Alloc`.
    pub allocs: Vec<AllocReq>,
    /// Slices still needing a physical `Dealloc`.
    pub deallocs: Vec<BStackRange>,
}

/// The groupoid reduction: cancel each allocation requirement against a to-be-freed
/// slice of **equal length**, handing that slice's storage straight to the new
/// allocation (`ForgetAddress ∘ Dealloc ∘ Alloc ∘ Choice = id`). Only the unpaired
/// remainder becomes physical work.
pub fn reduce(allocs: Vec<AllocReq>, mut deallocs: Vec<BStackRange>) -> Reduced {
    let mut reused = Vec::new();
    let mut rem_allocs = Vec::new();
    for req in allocs {
        if let Some(pos) = deallocs.iter().position(|d| d.len() == req.len) {
            reused.push((req, deallocs.remove(pos)));
        } else {
            rem_allocs.push(req);
        }
    }
    Reduced {
        reused,
        allocs: rem_allocs,
        deallocs,
    }
}

// ---------------------------------------------------------------------------
// On-disk WAL block + completion runtime.
//
// The WAL block is **persistent and reused** (Vec-like), not allocated per
// transaction: `[WalHeader | WalEntry × capacity]`, reached through a stable
// anchor slot (see [`BStackWalAnchor`]) that holds the block's offset (`0` = not
// yet created — lazily allocated on first use). The header's `txn_status` doubles
// as the in-use flag:
//
//   * `None`     — idle: no transaction in flight (the block is free to reuse);
//   * `Pending`  — a transaction is staged but not committed (abandon on recover);
//   * `Complete` — committed, deallocs may be unfinished (roll forward on recover).
//
// A transaction reuses the block in place (growing it — free old, alloc bigger —
// only when it needs more than `capacity` entries), sets its status back to
// `None` when done, and never frees the block. Concurrent transactions on the
// same file are serialized by an in-memory mutex ([`wal_lock_for`]); the on-disk
// `txn_status` is purely for crash recovery, not live mutual exclusion.
// ---------------------------------------------------------------------------

const WAL_MAGIC: u64 = 0x6273_7461_636b_5741; // "bstackWA"

/// Minimum entry capacity of the persistent WAL block; larger transactions grow
/// it to the next power of two.
const WAL_MIN_CAP: u64 = 8;

/// On-disk header of the persistent WAL block. `txn_status` is both the
/// transaction-level commit marker and the idle/in-use flag (`None` = idle);
/// `capacity` is the number of [`WalEntry`] slots the block was allocated for.
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct WalHeader {
    magic: u64,
    txn_status: u8,
    _pad: [u8; 7],
    count: u64,
    capacity: u64,
}

impl WalHeader {
    fn txn_status(&self) -> WalStatus {
        WalStatus::from_u8(self.txn_status)
    }
}

impl WalLog {
    /// The used prefix image `[WalHeader | entries]` at the given transaction
    /// status, for a block allocated with `capacity` entry slots. Only the header
    /// and the `count` live entries are written; the spare capacity is left as-is.
    pub fn block_image(&self, txn_status: WalStatus, capacity: u64) -> Vec<u8> {
        let header = WalHeader {
            magic: WAL_MAGIC,
            txn_status: txn_status as u8,
            _pad: [0; 7],
            count: self.entries.len() as u64,
            capacity,
        };
        let mut img =
            Vec::with_capacity(size_of::<WalHeader>() + self.entries.len() * size_of::<WalEntry>());
        img.extend_from_slice(bytemuck::bytes_of(&header));
        img.extend_from_slice(bytemuck::cast_slice(&self.entries));
        img
    }
}

/// The allocator capability the whole crate is bound on: a
/// [`BStackOwnedSliceAllocator`] that names a stable on-disk slot for the WAL
/// block pointer — or `None` to opt out of crash-reclamation.
///
/// Making this the uniform bound is what lets `try_clone_in` / `bstack_drop`
/// reclaim orphaned allocations on the next open **automatically**, with no
/// separate opt-in call: they read [`wal_anchor`](Self::wal_anchor) directly.
/// `None` (the default) means "no reclamation" — the op behaves exactly as before.
/// Every bstack-provided allocator implements this; a custom allocator adds a
/// one-line `unsafe impl BStackWalAnchor for MyAlloc {}` (defaulting to `None`,
/// or returning `Some(slot)` if it reserves one).
///
/// # Safety
///
/// An implementor returning `Some(off)` asserts that `[off, off + 8)` is a
/// stable, persistent 8-byte region the allocator **never** hands out via `alloc`
/// and **never** uses for its own metadata, and that survives across open/close.
/// `bstack_raii` stores the current WAL block's offset there (`0` = none).
/// Returning `None` asserts nothing.
pub unsafe trait BStackWalAnchor: BStackOwnedSliceAllocator {
    fn wal_anchor(&self) -> Option<u64> {
        None
    }
}

/// Anchor offset for the bstack-provided freeing allocators: the second `u64`
/// word of the user-reserved region every one of them keeps at payload offset 0
/// and never hands out (FirstFit reserves 16 B there, GhostTree 32 B, Slab and
/// CheckedSlab 24 B — all ≥ 16). Payload offset 0 is left as `bstack_raii`'s null
/// niche, so the anchor is the *next* word, `[8, 16)`.
pub const STD_WAL_ANCHOR: u64 = 8;

// SAFETY: each of these allocators documents a user-reserved region at payload
// offset 0 (≥ 16 bytes) that it never allocates from and never writes to; the
// `[8, 16)` slot sits inside it and persists across open/close.
unsafe impl BStackWalAnchor for bstack::FirstFitBStackAllocator {
    fn wal_anchor(&self) -> Option<u64> {
        Some(STD_WAL_ANCHOR)
    }
}
unsafe impl BStackWalAnchor for bstack::GhostTreeBstackAllocator {
    fn wal_anchor(&self) -> Option<u64> {
        Some(STD_WAL_ANCHOR)
    }
}
unsafe impl BStackWalAnchor for bstack::SlabBStackAllocator {
    fn wal_anchor(&self) -> Option<u64> {
        Some(STD_WAL_ANCHOR)
    }
}
unsafe impl BStackWalAnchor for bstack::CheckedSlabBStackAllocator {
    fn wal_anchor(&self) -> Option<u64> {
        Some(STD_WAL_ANCHOR)
    }
}
// `LinearBStackAllocator`'s `dealloc` is a no-op (nothing to reclaim), so it opts
// out via the default `None` — but it still needs the impl to satisfy the bound.
unsafe impl BStackWalAnchor for bstack::LinearBStackAllocator {}

// ---------------------------------------------------------------------------
// In-memory serialization of WAL transactions.
//
// The persistent WAL block + anchor slot are single-writer per file: one
// transaction may be staged there at a time. Since automatic teardown/clone run
// concurrently, an in-memory mutex (keyed by the file's `BStack` identity)
// serializes the whole staging→commit→finish critical section. Different files
// use different locks and never contend. The lock is process-local, matching
// bstack's single-process write model; the on-disk `txn_status` handles the
// orthogonal job of crash recovery across a restart.
// ---------------------------------------------------------------------------

/// Per-file WAL mutex registry, keyed by the address of the file's [`BStack`].
static WAL_LOCKS: OnceLock<Mutex<HashMap<usize, Arc<Mutex<()>>>>> = OnceLock::new();

/// The WAL mutex for `allocator`'s file (created on first use). Hold its guard
/// across a whole WAL transaction.
pub(crate) fn wal_lock_for<A: BStackOwnedSliceAllocator>(allocator: &A) -> Arc<Mutex<()>> {
    let key = core::ptr::from_ref(allocator.stack()) as usize;
    let reg = WAL_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = reg.lock().unwrap_or_else(|e| e.into_inner());
    map.entry(key)
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

/// Read the anchor slot: the persistent WAL block's offset, or `None` if one has
/// not been created yet.
fn read_anchor<A: BStackOwnedSliceAllocator>(
    allocator: &A,
    anchor: u64,
) -> io::Result<Option<u64>> {
    let mut buf = [0u8; 8];
    allocator.stack().get_into(anchor, &mut buf)?;
    let off = u64::from_le_bytes(buf);
    Ok((off != 0).then_some(off))
}

/// Ensure the persistent WAL block exists and holds at least `needed` entry
/// slots, returning `(block_offset, capacity)`. Lazily allocates it on first use;
/// grows it (free old, allocate a larger one — its contents are transient between
/// transactions) when a transaction needs more capacity. The header is
/// (re)initialized `None` (idle) whenever the block is created or grown.
fn wal_ensure_block<A: BStackOwnedSliceAllocator>(
    allocator: &A,
    anchor: u64,
    needed: u64,
) -> io::Result<(u64, u64)> {
    let stack = allocator.stack();
    let hsz = size_of::<WalHeader>() as u64;
    let esz = size_of::<WalEntry>() as u64;

    if let Some(off) = read_anchor(allocator, anchor)? {
        let mut hbuf = [0u8; size_of::<WalHeader>()];
        stack.get_into(off, &mut hbuf)?;
        let header: WalHeader = bytemuck::pod_read_unaligned(&hbuf);
        if header.magic == WAL_MAGIC {
            if header.capacity >= needed {
                return Ok((off, header.capacity));
            }
            // Too small: free the old block (its content is not needed across the
            // grow) and fall through to allocate a bigger one.
            let old = BStackRange::new(off, hsz + header.capacity * esz);
            unsafe { dealloc_range(allocator, old)? };
        }
    }

    let capacity = needed.max(WAL_MIN_CAP).next_power_of_two();
    let mut slice = allocator.alloc(hsz + capacity * esz)?;
    let off = slice.as_range().start();
    let header = WalHeader {
        magic: WAL_MAGIC,
        txn_status: WalStatus::None as u8,
        _pad: [0; 7],
        count: 0,
        capacity,
    };
    if let Err(e) = slice.write_range(0, bytemuck::bytes_of(&header)) {
        let _ = allocator.dealloc(slice);
        return Err(e);
    }
    stack.set(anchor, off.to_le_bytes())?;
    Ok((off, capacity))
}

/// Stage `log` into the persistent WAL block at transaction status `txn_status`,
/// (lazily) creating or growing the block as needed. Returns the block's range.
/// The caller must hold the file's WAL lock (see [`wal_lock_for`]).
pub fn persist_at<A: BStackOwnedSliceAllocator>(
    allocator: &A,
    anchor: u64,
    log: &WalLog,
    txn_status: WalStatus,
) -> io::Result<BStackRange> {
    let (off, capacity) = wal_ensure_block(allocator, anchor, log.entries().len() as u64)?;
    let image = log.block_image(txn_status, capacity);
    allocator.stack().set(off, &image)?;
    let hsz = size_of::<WalHeader>() as u64;
    let esz = size_of::<WalEntry>() as u64;
    Ok(BStackRange::new(off, hsz + capacity * esz))
}

/// Read the staged transaction in the persistent WAL block, if any. Returns the
/// block range, header, and the `count` live entries; `None` if no block exists.
fn load_at<A: BStackOwnedSliceAllocator>(
    allocator: &A,
    anchor: u64,
) -> io::Result<Option<(BStackRange, WalHeader, Vec<WalEntry>)>> {
    let stack = allocator.stack();
    let wal_off = match read_anchor(allocator, anchor)? {
        Some(off) => off,
        None => return Ok(None),
    };
    let mut hbuf = [0u8; size_of::<WalHeader>()];
    stack.get_into(wal_off, &mut hbuf)?;
    let header: WalHeader = bytemuck::pod_read_unaligned(&hbuf);
    if header.magic != WAL_MAGIC {
        return Ok(None);
    }
    let ebytes = header.count as usize * size_of::<WalEntry>();
    let mut ebuf = vec![0u8; ebytes];
    stack.get_into(wal_off + size_of::<WalHeader>() as u64, &mut ebuf)?;
    let entries = WalLog::entries_from_bytes(&ebuf);
    let block_size = size_of::<WalHeader>() as u64 + header.capacity * size_of::<WalEntry>() as u64;
    Ok(Some((
        BStackRange::new(wal_off, block_size),
        header,
        entries,
    )))
}

/// Mark the persistent WAL block idle (`txn_status := None`) — a transaction is
/// complete and the block is free to reuse. The block itself is **not** freed.
pub(crate) fn wal_set_idle<A: BStackOwnedSliceAllocator>(
    allocator: &A,
    block_off: u64,
) -> io::Result<()> {
    // `txn_status` is the byte right after the u64 magic (offset 8).
    allocator
        .stack()
        .set(block_off + 8, [WalStatus::None as u8])
}

/// **Complete** a staged transaction by reclaiming exactly the slices its outcome
/// orphaned, then marking the block idle. Assumes the file's WAL lock is already
/// held (used on the failure/recovery paths that run under the transaction lock).
///
/// * **Committed** (`txn_status == Complete`): roll forward — free each still-
///   `Pending` `Dealloc` (the old blocks the committed op unlinked).
/// * **Uncommitted** (`txn_status == Pending`): abandon — free each still-
///   `Pending` `Alloc` (the new blocks a crashed op allocated but never linked).
///
/// Each freed entry is persisted `Complete` **before** its slice is freed, so a
/// second crash mid-completion never double-frees. The persistent block is then
/// marked idle (`None`) and kept for reuse. Returns the number of slices freed.
pub(crate) fn finish_at_locked<A: BStackOwnedSliceAllocator>(
    allocator: &A,
    anchor: u64,
) -> io::Result<usize> {
    let (wal_range, header, entries) = match load_at(allocator, anchor)? {
        Some(x) => x,
        None => return Ok(0),
    };
    let stack = allocator.stack();
    let txn = header.txn_status();
    if txn == WalStatus::None {
        // Idle block, nothing staged.
        return Ok(0);
    }
    let committed = txn == WalStatus::Complete;
    let base = wal_range.start() + size_of::<WalHeader>() as u64;
    let mut completed = 0usize;

    for (i, e) in entries.iter().enumerate() {
        if e.status() != WalStatus::Pending {
            continue;
        }
        // Committed: the `Dealloc`s (old blocks) must go. Abandoned: the `Alloc`s
        // (new orphans) must go. Everything else is kept.
        let slice = if committed {
            e.as_dealloc()
        } else {
            e.as_alloc()
        };
        if let Some(slice) = slice {
            // Persist Complete for this entry (its status is byte 0), THEN free —
            // so a second crash can't double-free it.
            let entry_off = base + (i * size_of::<WalEntry>()) as u64;
            stack.set(entry_off, [WalStatus::Complete as u8])?;
            unsafe { dealloc_range(allocator, slice)? };
            completed += 1;
        }
    }

    // Mark the persistent block idle (kept for reuse); do not free it.
    wal_set_idle(allocator, wal_range.start())?;
    Ok(completed)
}

/// **Complete** a crash-left transaction at `anchor`: reclaim the slices its
/// outcome orphaned and mark the persistent block idle. Returns the number of
/// slices reclaimed. This is what a caller runs once after `open` — a
/// *completion*, not a leaky recovery. Acquires the file's WAL lock.
pub fn finish_at<A: BStackOwnedSliceAllocator>(allocator: &A, anchor: u64) -> io::Result<usize> {
    let lock = wal_lock_for(allocator);
    let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
    finish_at_locked(allocator, anchor)
}

/// Like [`finish_at`], using the allocator's own [`BStackWalAnchor`] slot.
/// An allocator that opts out of reclamation (`wal_anchor() == None`) has no WAL
/// to complete, so this is a no-op returning `0`.
pub fn finish<A: BStackWalAnchor>(allocator: &A) -> io::Result<usize> {
    match allocator.wal_anchor() {
        Some(anchor) => finish_at(allocator, anchor),
        None => Ok(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wal_status_maps() {
        // Normal progress: N → P → C → C; Abandon terminal.
        assert_eq!(WalStatus::None.advance(), WalStatus::Pending);
        assert_eq!(WalStatus::Pending.advance(), WalStatus::Complete);
        assert_eq!(WalStatus::Complete.advance(), WalStatus::Complete);
        assert_eq!(WalStatus::Abandon.advance(), WalStatus::Abandon);
        // Recovery: only Pending → Abandon.
        assert_eq!(WalStatus::None.recover(), WalStatus::None);
        assert_eq!(WalStatus::Pending.recover(), WalStatus::Abandon);
        assert_eq!(WalStatus::Complete.recover(), WalStatus::Complete);
    }

    #[test]
    fn wal_entry_roundtrip() {
        let a = WalEntry::alloc(WalStatus::Pending, BStackRange::new(0x1000, 256));
        assert_eq!(a.op(), WalOp::Alloc);
        assert_eq!(a.status(), WalStatus::Pending);
        assert_eq!(a.as_alloc(), Some(BStackRange::new(0x1000, 256)));
        assert_eq!(a.as_dealloc(), None);

        let d = WalEntry::dealloc(WalStatus::Complete, BStackRange::new(0x6CD4, 256));
        assert_eq!(d.op(), WalOp::Dealloc);
        assert_eq!(d.as_dealloc(), Some(BStackRange::new(0x6CD4, 256)));
        assert_eq!(d.as_alloc(), None);
    }

    #[test]
    fn wal_entry_is_24_bytes_and_pod_roundtrips() {
        assert_eq!(size_of::<WalEntry>(), 24);
        let mut log = WalLog::with_capacity(2);
        log.append(WalEntry::alloc(
            WalStatus::Pending,
            BStackRange::new(8192, 64),
        ));
        log.append(WalEntry::dealloc(
            WalStatus::Pending,
            BStackRange::new(4096, 64),
        ));
        let bytes = log.as_bytes().to_vec();
        let back = WalLog::entries_from_bytes(&bytes);
        assert_eq!(back.len(), 2);
        assert_eq!(back[0].as_alloc(), Some(BStackRange::new(8192, 64)));
        assert_eq!(back[1].as_dealloc(), Some(BStackRange::new(4096, 64)));
    }

    #[test]
    fn wal_fresh_id_wraps() {
        let mut log = WalLog::with_capacity(0);
        assert_eq!(log.fresh_id(), 0);
        assert_eq!(log.fresh_id(), 1);
        log.next_id = u64::MAX;
        assert_eq!(log.fresh_id(), u64::MAX);
        assert_eq!(log.fresh_id(), 0); // wrapped
    }

    #[test]
    fn reduce_cancels_equal_length_pairs() {
        // (Alloc 256, Alloc 600, Dealloc 256) → reuse the 256 slice, Alloc 600 left.
        let allocs = vec![AllocReq { id: 0, len: 256 }, AllocReq { id: 1, len: 600 }];
        let deallocs = vec![BStackRange::new(0x1FF0, 256)];
        let r = reduce(allocs, deallocs);
        assert_eq!(r.reused.len(), 1);
        assert_eq!(r.reused[0].0, AllocReq { id: 0, len: 256 });
        assert_eq!(r.reused[0].1, BStackRange::new(0x1FF0, 256));
        assert_eq!(r.allocs, vec![AllocReq { id: 1, len: 600 }]);
        assert!(r.deallocs.is_empty());
    }

    #[test]
    fn reduce_leaves_unpaired_on_both_sides() {
        let allocs = vec![AllocReq { id: 0, len: 100 }];
        let deallocs = vec![BStackRange::new(8, 200)];
        let r = reduce(allocs, deallocs);
        assert!(r.reused.is_empty());
        assert_eq!(r.allocs.len(), 1);
        assert_eq!(r.deallocs.len(), 1);
    }
}
