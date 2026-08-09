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
use std::io;

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
// A WAL block is `[WalHeader | WalEntry × count]`, allocated from the allocator
// and reached through a stable anchor slot (see [`BStackWalAnchor`]) that holds
// the block's offset (`0` = none). The header's `txn_status` is the
// transaction-level commit marker: `Complete` = committed (roll forward on the
// next open), `Pending` = uncommitted (abandon).
// ---------------------------------------------------------------------------

const WAL_MAGIC: u64 = 0x6273_7461_636b_5741; // "bstackWA"

/// On-disk header of a WAL block. `txn_status` is the transaction-level commit
/// marker (`Pending` = uncommitted, `Complete` = committed).
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct WalHeader {
    magic: u64,
    txn_status: u8,
    _pad: [u8; 7],
    count: u64,
}

impl WalHeader {
    fn txn_status(&self) -> WalStatus {
        WalStatus::from_u8(self.txn_status)
    }
}

impl WalLog {
    /// The full WAL-block image `[WalHeader | entries]` at the given
    /// transaction-level status, ready to write to an allocated block.
    pub fn block_image(&self, txn_status: WalStatus) -> Vec<u8> {
        let header = WalHeader {
            magic: WAL_MAGIC,
            txn_status: txn_status as u8,
            _pad: [0; 7],
            count: self.entries.len() as u64,
        };
        let mut img =
            Vec::with_capacity(size_of::<WalHeader>() + self.entries.len() * size_of::<WalEntry>());
        img.extend_from_slice(bytemuck::bytes_of(&header));
        img.extend_from_slice(bytemuck::cast_slice(&self.entries));
        img
    }
}

/// A [`BStackOwnedSliceAllocator`] that can point `bstack_raii` at a stable
/// on-disk slot for its WAL block pointer.
///
/// # Safety
///
/// The implementor asserts that `[wal_anchor(), wal_anchor() + 8)` is a stable,
/// persistent 8-byte region that the allocator **never** hands out via `alloc`
/// and **never** uses for its own metadata, and that survives across open/close.
/// `bstack_raii` stores the current WAL block's offset there (`0` = none).
pub unsafe trait BStackWalAnchor: BStackOwnedSliceAllocator {
    fn wal_anchor(&self) -> u64;
}

/// Anchor offset for the bstack-provided freeing allocators: the second `u64`
/// word of the user-reserved region every one of them keeps at payload offset 0
/// and never hands out (FirstFit reserves 16 B there, GhostTree 32 B, Slab and
/// CheckedSlab 24 B — all ≥ 16). Payload offset 0 is left as `bstack_raii`'s null
/// niche, so the anchor is the *next* word, `[8, 16)`.
pub const STD_WAL_ANCHOR: u64 = 8;

// SAFETY: each of these allocators documents a user-reserved region at payload
// offset 0 (≥ 16 bytes) that it never allocates from and never writes to; the
// `[8, 16)` slot sits inside it and persists across open/close. `LinearBStack-
// Allocator` is intentionally excluded — its `dealloc` is a no-op, so there is
// nothing for the WAL to reclaim.
unsafe impl BStackWalAnchor for bstack::FirstFitBStackAllocator {
    fn wal_anchor(&self) -> u64 {
        STD_WAL_ANCHOR
    }
}
unsafe impl BStackWalAnchor for bstack::GhostTreeBstackAllocator {
    fn wal_anchor(&self) -> u64 {
        STD_WAL_ANCHOR
    }
}
unsafe impl BStackWalAnchor for bstack::SlabBStackAllocator {
    fn wal_anchor(&self) -> u64 {
        STD_WAL_ANCHOR
    }
}
unsafe impl BStackWalAnchor for bstack::CheckedSlabBStackAllocator {
    fn wal_anchor(&self) -> u64 {
        STD_WAL_ANCHOR
    }
}

// Note: WAL reclamation is **opt-in**, not automatic. A generic op bounded on
// `BStackOwnedSliceAllocator` cannot detect at that call whether the concrete `A`
// also implements `BStackWalAnchor` — autoref "specialization" only resolves at a
// concrete call site, and stable Rust has no real specialization. So the WAL is
// exposed through explicit entry points ([`crate::wal_clone_in`], [`crate::wal_drop`])
// that take a concrete `A: BStackWalAnchor` and call [`wal_anchor`](BStackWalAnchor::wal_anchor)
// directly; the plain generic `try_clone_in` / `bstack_drop` are unchanged.

/// Write `log` as a WAL block with transaction status `txn_status`, allocate the
/// block, and point the anchor slot at it. Returns the block's range.
pub fn persist_at<A: BStackOwnedSliceAllocator>(
    allocator: &A,
    anchor: u64,
    log: &WalLog,
    txn_status: WalStatus,
) -> io::Result<BStackRange> {
    let image = log.block_image(txn_status);
    let mut slice = allocator.alloc(image.len() as u64)?;
    let range = slice.as_range();
    if let Err(e) = slice.write_range(0, &image) {
        let _ = allocator.dealloc(slice);
        return Err(e);
    }
    allocator.stack().set(anchor, range.start().to_le_bytes())?;
    Ok(range)
}

/// Read the WAL block referenced by the anchor slot, if any (and valid).
fn load_at<A: BStackOwnedSliceAllocator>(
    allocator: &A,
    anchor: u64,
) -> io::Result<Option<(BStackRange, WalHeader, Vec<WalEntry>)>> {
    let stack = allocator.stack();
    let mut buf = [0u8; 8];
    stack.get_into(anchor, &mut buf)?;
    let wal_off = u64::from_le_bytes(buf);
    if wal_off == 0 {
        return Ok(None);
    }
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
    let block_size = size_of::<WalHeader>() as u64 + ebytes as u64;
    Ok(Some((
        BStackRange::new(wal_off, block_size),
        header,
        entries,
    )))
}

/// **Complete** a crash-left transaction referenced by the anchor slot at
/// `anchor` by reclaiming exactly the slices the transaction's outcome orphaned.
///
/// * **Committed** (`txn_status == Complete`): roll forward — free each still-
///   `Pending` `Dealloc` (the old blocks the committed op unlinked).
/// * **Uncommitted** (`txn_status == Pending`): abandon — free each still-
///   `Pending` `Alloc` (the new blocks the crashed op allocated but never linked).
///
/// Each freed entry is persisted `Complete` **before** its slice is freed, so a
/// second crash mid-completion never double-frees. Either way the WAL block is
/// then cleared (anchor `:= 0`) and freed. Returns the number of slices
/// reclaimed. This is what a caller runs once after `open` — a *completion*, not
/// a leaky recovery.
pub fn finish_at<A: BStackOwnedSliceAllocator>(allocator: &A, anchor: u64) -> io::Result<usize> {
    let (wal_range, header, entries) = match load_at(allocator, anchor)? {
        Some(x) => x,
        None => return Ok(0),
    };
    let stack = allocator.stack();
    let committed = header.txn_status() == WalStatus::Complete;
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

    // Clear the anchor, then free the WAL block.
    stack.set(anchor, 0u64.to_le_bytes())?;
    unsafe { dealloc_range(allocator, wal_range)? };
    Ok(completed)
}

/// Like [`finish_at`], using the allocator's own [`BStackWalAnchor`] slot.
pub fn finish<A: BStackWalAnchor>(allocator: &A) -> io::Result<usize> {
    finish_at(allocator, allocator.wal_anchor())
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
