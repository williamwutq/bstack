//! Disk-level recursive destruction, fully decoupled from Rust's `Drop`.
//!
//! [`BStackDrop`] is implemented by every `#[bstack_block]` type (frees the
//! block and recurses into its owned children) and by the small child-handle
//! types in [`crate::handle`]. It takes `self` (a *without-allocator* handle)
//! plus an explicit allocator, so it is generic over all handle-like types.

use core::cell::RefCell;
use core::mem::ManuallyDrop;
use core::ops::Deref;
use std::io;

use bstack::{BStackGenOp, BStackOwnedSlice, BStackOwnedSliceAllocator, BStackRange};

use crate::wal::{BStackWalAnchor, WalEntry, WalLog, WalStatus, finish_at, persist_at};

thread_local! {
    /// While a WAL-backed teardown is in progress, the collector that
    /// [`dealloc_range`] funnels every subtree slice into *instead of* freeing it
    /// eagerly. The root driver ([`wal_teardown`]) installs it; the generated
    /// recursion and nested handle `bstack_drop`s see it transparently (they all
    /// go through `dealloc_range`), so no allocator/sink parameter has to be
    /// threaded through the whole teardown.
    static TEARDOWN_SINK: RefCell<Option<Vec<BStackRange>>> = const { RefCell::new(None) };
}

/// Tear down `handle` (a whole owned subtree) as one crash-atomic batch of frees,
/// so a crash mid-teardown is completed — not leaked — by `finish` on the next
/// open. The WAL-backed, opt-in counterpart to [`BStackDrop::bstack_drop`];
/// requires an anchored allocator ([`BStackWalAnchor`]).
///
/// While the sink is installed, every [`dealloc_range`] in the (ordinary,
/// generic) teardown recursion *collects* its slice rather than freeing it;
/// afterwards the whole set commits as one `Dealloc` transaction and is executed
/// via [`finish_at`] (the same path crash recovery takes). Nested owned frees
/// (e.g. a collection freeing its values through `BStackOwned::bstack_drop`) see
/// the sink already set and just collect, so exactly one transaction wraps the
/// outermost teardown.
pub fn wal_drop<A: BStackWalAnchor, T: BStackDrop>(handle: T, allocator: &A) -> io::Result<()> {
    // Nested (shouldn't happen at a public entry, but be safe): the outer driver
    // owns the sink; frees already collect.
    if TEARDOWN_SINK.with(|s| s.borrow().is_some()) {
        return handle.bstack_drop(allocator);
    }
    let anchor = allocator.wal_anchor();
    TEARDOWN_SINK.with(|s| *s.borrow_mut() = Some(Vec::new()));
    let result = handle.bstack_drop(allocator);
    let slices = TEARDOWN_SINK
        .with(|s| s.borrow_mut().take())
        .unwrap_or_default();
    wal_free_all(allocator, anchor, slices)?;
    result
}

/// Commit `slices` as one committed `Dealloc` transaction and execute the frees.
/// A crash mid-free leaves a `Complete` WAL that `finish` rolls forward on reopen.
fn wal_free_all<A: BStackOwnedSliceAllocator>(
    allocator: &A,
    anchor: u64,
    slices: Vec<BStackRange>,
) -> io::Result<()> {
    if slices.is_empty() {
        return Ok(());
    }
    let mut log = WalLog::with_capacity(slices.len());
    for s in &slices {
        log.append(WalEntry::dealloc(WalStatus::Pending, *s));
    }
    // Stage the transaction `Pending`, then commit it by flipping `txn_status` to
    // `Complete` in one atomic `inplace_gen` — the single commit point (a crash
    // before it abandons; after it, `finish` rolls the frees forward). With the
    // sink now cleared, `finish_at` executes the frees + reclaims the WAL block.
    let wal_range = persist_at(allocator, anchor, &log, WalStatus::Pending)?;
    let flip = [WalStatus::Complete as u8];
    let mut done = false;
    allocator.stack().inplace_gen(|_feedback| {
        if done {
            None
        } else {
            done = true;
            // SAFETY: `flip` outlives the call; the `txn_status` byte follows the
            // u64 magic at offset 8 in `WalHeader`.
            let data: &[u8] = unsafe { core::mem::transmute::<&[u8], _>(&flip[..]) };
            Some(BStackGenOp::Write {
                offset: wal_range.start() + 8,
                data,
            })
        }
    })?;
    finish_at(allocator, anchor)?;
    Ok(())
}

/// Recursively free a block and all of its owned children.
///
/// Because a bare [`BStackRange`] carries no allocator, freeing is done by
/// reconstructing a [`BStackOwnedSlice`] and handing it to the allocator's
/// `dealloc` — see [`dealloc_range`]. There is deliberately no `dealloc_range`
/// method on the allocator trait itself.
///
/// The allocator is bound to [`BStackOwnedSliceAllocator`] rather than the bare
/// `BStackAllocator`: that supertrait pins `Allocated<'a> = BStackOwnedSlice<'a,
/// A>` (so a reconstructed owned slice is the accepted `dealloc` handle) and
/// `Error = io::Error` (so the layer speaks [`io::Result`]).
pub trait BStackDrop: Sized {
    fn bstack_drop<A: BStackOwnedSliceAllocator>(self, allocator: &A) -> io::Result<()>;
}

/// Free a raw block range by reconstructing an owned slice and delegating to the
/// allocator. The central sink the generated `bstack_drop` code funnels through,
/// since ranges carry no allocator of their own.
///
/// # Safety
/// `range` must be a live allocation owned by `allocator` that no other live
/// handle will also free.
pub unsafe fn dealloc_range<A: BStackOwnedSliceAllocator>(
    allocator: &A,
    range: BStackRange,
) -> io::Result<()> {
    // Inside a WAL-backed teardown, defer the free: collect the slice so the whole
    // subtree commits (and frees) as one crash-atomic transaction.
    let deferred = TEARDOWN_SINK.with(|s| match s.borrow_mut().as_mut() {
        Some(sink) => {
            sink.push(range);
            true
        }
        None => false,
    });
    if deferred {
        return Ok(());
    }
    let owned: BStackOwnedSlice<'_, A> =
        unsafe { BStackOwnedSlice::from_raw_range(allocator, range) };
    allocator.dealloc(owned).map_err(|e| e.source)
}

/// A guard that runs [`BStackDrop::bstack_drop`] on its inner handle when it goes
/// out of scope, bridging fallible on-disk teardown to Rust's `Drop`.
///
/// It is the *one* place that calls `bstack_drop` from a `Drop` impl: every
/// allocator-bound handle that wants automatic cleanup is (or embeds) an
/// `AutoDrop`, rather than hand-writing its own `Drop`. A bare [`BStackDrop`]
/// handle that is not wrapped frees nothing on its own — its `bstack_drop` is
/// invoked explicitly, or runs as a child of a parent block's recursive
/// teardown.
///
/// It is a newtype over `(ManuallyDrop<T>, &'a A)`; `bstack_move!` and the raw
/// accessors defuse it via [`into_raw_parts`](Self::into_raw_parts) so no
/// parallel destruction path exists.
pub struct AutoDrop<'a, T: BStackDrop, A: BStackOwnedSliceAllocator> {
    inner: ManuallyDrop<T>,
    allocator: &'a A,
}

impl<'a, T: BStackDrop, A: BStackOwnedSliceAllocator> AutoDrop<'a, T, A> {
    /// Pair an inner handle with its allocator into an auto-dropping guard.
    ///
    /// # Safety
    /// The caller asserts `inner` describes a live allocation owned by
    /// `allocator` and that no other handle will also free it.
    pub unsafe fn from_raw(inner: T, allocator: &'a A) -> Self {
        Self {
            inner: ManuallyDrop::new(inner),
            allocator,
        }
    }

    /// Split into the raw inner handle and allocator **without** running the
    /// disk-level `Drop`. The caller takes over responsibility for the
    /// allocation (e.g. `bstack_move!`, which frees only the parent shell).
    pub fn into_raw_parts(self) -> (T, &'a A) {
        // Wrapping `self` in ManuallyDrop defuses our own `Drop`, so
        // `bstack_drop` is not called; then move the inner `T` out.
        let mut me = ManuallyDrop::new(self);
        let inner = unsafe { ManuallyDrop::take(&mut me.inner) };
        (inner, me.allocator)
    }

    /// The allocator this handle is bound to.
    pub fn allocator(&self) -> &'a A {
        self.allocator
    }

    /// Borrow the underlying handle, e.g. to call generated field accessors:
    /// `owned.handle().field(stack)`.
    pub fn handle(&self) -> &T {
        &self.inner
    }
}

impl<'a, T: BStackDrop, A: BStackOwnedSliceAllocator> Deref for AutoDrop<'a, T, A> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<'a, T: BStackDrop, A: BStackOwnedSliceAllocator> Drop for AutoDrop<'a, T, A> {
    fn drop(&mut self) {
        let inner = unsafe { ManuallyDrop::take(&mut self.inner) };
        // Errors are swallowed, matching the contract of Rust's `Drop`.
        let _ = inner.bstack_drop(self.allocator);
    }
}
