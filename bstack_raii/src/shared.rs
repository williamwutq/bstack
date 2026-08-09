//! [`BStackRc`] + [`BStackWeak`]: the with-allocator shared handles.
//!
//! Neither hand-writes a `Drop`. Each embeds an [`AutoDrop`] over a
//! without-allocator *drop core* ([`StrongCore`] / [`WeakRef`]) whose
//! [`BStackDrop`] performs the refcount release; the embedded guard runs it on
//! scope exit.

use core::mem::size_of;
use std::io;

use crate::BStackRaiiAllocator;
use bstack::BStackRange;

use crate::block::{BStackBlock, BStackMove, BStackMoveExpr, BStackWeakable};
use crate::clone::TryClone;
use crate::handle::{StrongRef, WeakRef, strong_release_ctrl};
use crate::layout;
use crate::owned::BStackOwned;
use crate::refcount;
use crate::reference::BStackRef;
use crate::teardown::{AutoDrop, BStackDrop, dealloc_range};

/// The without-allocator drop core of a [`BStackRc`]: the data ref plus the
/// optional control-block range.
///
/// `ctrl` distinguishes the two block kinds at runtime — `None` for a plain
/// `(rc)` block (inline refcount), `Some(range)` for an `(rc, weak)` block
/// (control block). Its [`BStackDrop`] is the strong release, so a `BStackRc`'s
/// embedded [`AutoDrop`] runs it automatically and the handle needs no
/// hand-written `Drop`.
pub(crate) struct StrongCore<T: BStackBlock> {
    data: BStackRef<T>,
    ctrl: Option<BStackRange>,
}

impl<T: BStackBlock> BStackDrop for StrongCore<T> {
    fn bstack_drop<A: BStackRaiiAllocator>(self, allocator: &A) -> io::Result<()> {
        match self.ctrl {
            None => StrongRef(self.data).bstack_drop(allocator),
            Some(ctrl) => strong_release_ctrl::<T, A>(allocator, self.data.into_range(), ctrl),
        }
    }
}

/// A shared, refcounted, allocator-bound handle.
///
/// Serves **both** block kinds via its [`StrongCore`]'s runtime `ctrl`:
/// * `None` — a plain `#[bstack_block(rc)]` block, whose refcount lives inline
///   in the data block at [`layout::RC_REFCOUNT_OFFSET`].
/// * `Some(range)` — an `#[bstack_block(rc, weak)]` block, whose `strong`/`weak`
///   counters live in a separate control block at `range`.
///
/// Carrying this as a runtime `Option` (rather than a type-level split via an
/// associated `Strong` handle) keeps `BStackRc<'a, T, A>`'s public signature
/// fixed at three parameters; a zero-cost representation can replace it later
/// without breaking callers.
///
/// **Invariant:** for a `T: BStackWeakable` block, `ctrl` is always `Some` — such
/// blocks are only ever constructed through the control-block paths
/// ([`BStackWeak::upgrade`], `bstack_move!`). `downgrade` relies on this.
pub struct BStackRc<'a, T: BStackBlock, A: BStackRaiiAllocator> {
    inner: AutoDrop<'a, StrongCore<T>, A>,
}

impl<'a, T: BStackBlock, A: BStackRaiiAllocator> BStackRc<'a, T, A> {
    /// Reconstruct a shared handle from its raw parts.
    ///
    /// # Safety
    /// The refs must describe a live `(rc)` / `(rc, weak)` block owned by
    /// `allocator`, and this handle must account for a strong count the caller
    /// has already established (e.g. the allocation's initial `strong = 1`, or a
    /// count bumped by `upgrade`). `ctrl` must be `Some` iff the block is
    /// `(rc, weak)`.
    pub unsafe fn from_raw(
        data: BStackRef<T>,
        ctrl: Option<BStackRange>,
        allocator: &'a A,
    ) -> Self {
        Self {
            inner: unsafe { AutoDrop::from_raw(StrongCore { data, ctrl }, allocator) },
        }
    }

    fn data(&self) -> BStackRef<T> {
        self.inner.handle().data
    }

    fn ctrl(&self) -> Option<BStackRange> {
        self.inner.handle().ctrl
    }

    fn allocator(&self) -> &'a A {
        self.inner.allocator()
    }

    /// The underlying typed handle, e.g. to call generated field accessors:
    /// `rc.handle().field(stack)`. Cheap: it just re-wraps the data ref and does
    /// not touch the refcount.
    pub fn handle(&self) -> T {
        <T as BStackBlock>::from_range(self.data().into_range())
    }

    /// Consume the handle into its raw parts **without** decrementing the strong
    /// count — the count is transferred to the caller (e.g. into a parent's
    /// `#[bstack_strong]` field). `ctrl` is `Some` for `(rc, weak)` blocks.
    pub fn into_raw(self) -> (BStackRef<T>, Option<BStackRange>) {
        let (core, _) = self.inner.into_raw_parts();
        (core.data, core.ctrl)
    }

    /// Byte offset of the strong counter for this handle's block kind.
    fn strong_offset(&self) -> u64 {
        match self.ctrl() {
            None => self.data().into_range().start() + layout::RC_REFCOUNT_OFFSET,
            Some(ctrl) => ctrl.start() + layout::CTRL_STRONG_OFFSET,
        }
    }
}

/// Cloning a strong handle bumps the block's strong count and returns another
/// handle to the **same** block — sharing, not copying (like `Rc::clone`). This
/// is the clone semantics for a shared block; there is deliberately no
/// deep-copy-to-owned (`TryCloneIn`) for one.
impl<'a, T: BStackBlock, A: BStackRaiiAllocator> TryClone for BStackRc<'a, T, A> {
    fn try_clone(&self) -> io::Result<Self> {
        refcount::fetch_add(self.allocator().stack(), self.strong_offset(), 1)?;
        // SAFETY: the fetch_add above established the strong count this clone
        // accounts for.
        Ok(unsafe { Self::from_raw(self.data(), self.ctrl(), self.allocator()) })
    }
}

impl<'a, T: BStackWeakable, A: BStackRaiiAllocator> BStackRc<'a, T, A> {
    /// Create a weak handle to the same block by incrementing `ctrl.weak`.
    ///
    /// Available only for `(rc, weak)` blocks (`T: BStackWeakable`), so a plain
    /// `(rc)` block's `BStackRc` has no `downgrade` at all — a compile error, not
    /// a runtime hazard.
    pub fn downgrade(&self) -> io::Result<BStackWeak<'a, T, A>> {
        // Invariant: a weakable block's `BStackRc` always carries a control ref.
        let ctrl_range = self
            .ctrl()
            .expect("BStackRc<T: BStackWeakable> always has a control block");
        let weak_off = ctrl_range.start() + layout::CTRL_WEAK_OFFSET;
        refcount::fetch_add(self.allocator().stack(), weak_off, 1)?;
        let ctrl = unsafe { BStackRef::<T::Control>::from_range(ctrl_range) };
        // SAFETY: the fetch_add above established the weak count this handle holds.
        Ok(unsafe { BStackWeak::from_raw(ctrl, self.allocator()) })
    }
}

impl<'a, T: BStackMove, A: BStackRaiiAllocator> BStackRc<'a, T, A> {
    /// `Rc::try_unwrap` + destructure: if this handle is the **sole strong
    /// owner**, move every field out (freeing only the data shell) and return
    /// them; otherwise hand the handle back in `Err`.
    ///
    /// The check-and-take is an atomic CAS `strong: 1 -> 0`, so a concurrent
    /// clone or `upgrade` makes it fail cleanly rather than tearing a shared
    /// block apart. Works for both `(rc)` (inline count) and `(rc, weak)` (the
    /// control block's phantom weak is released, freeing it if no weak handles
    /// remain). This is what `bstack_move!` calls on a `BStackRc`.
    pub fn try_move(self) -> io::Result<Result<T::Fields<'a, A>, Self>> {
        let strong_off = self.strong_offset();
        let stack = self.allocator().stack();

        // Atomic try-unwrap: succeed only if the strong count is exactly 1.
        if !refcount::cas(stack, strong_off, 1, 0)? {
            return Ok(Err(self));
        }

        // Strong is now 0 — no concurrent upgrade can revive the data block, so
        // it is safe to move the fields out and free the data shell. Defuse the
        // embedded guard so it does not double-free.
        let (core, allocator) = self.inner.into_raw_parts();
        let StrongCore { data, ctrl } = core;
        let owned =
            unsafe { BStackOwned::from_raw(<T as BStackBlock>::from_range(data.into_range())) };
        let fields = T::bstack_move(owned, allocator)?;

        // `(rc, weak)`: release the phantom weak; free the control block at zero.
        if let Some(ctrl) = ctrl {
            let weak_off = ctrl.start() + layout::CTRL_WEAK_OFFSET;
            if refcount::fetch_sub(allocator.stack(), weak_off, 1)? == 1 {
                unsafe { dealloc_range(allocator, ctrl)? };
            }
        }
        Ok(Ok(fields))
    }
}

impl<'a, T: BStackMove, A: BStackRaiiAllocator> BStackMoveExpr for BStackRc<'a, T, A> {
    type Output = io::Result<Result<T::Fields<'a, A>, Self>>;
    fn bstack_move(self) -> Self::Output {
        self.try_move()
    }
}

/// A non-owning weak handle to an `(rc, weak)` block's control block.
///
/// Obtained from [`BStackRc::downgrade`] or [`TryClone::try_clone`]. It keeps the
/// control block alive (so [`upgrade`](BStackWeak::upgrade) can check liveness)
/// but never pins the data block. Its drop core is a [`WeakRef`], whose
/// [`BStackDrop`] decrements `ctrl.weak` and frees the control block at zero.
pub struct BStackWeak<'a, T: BStackWeakable, A: BStackRaiiAllocator> {
    inner: AutoDrop<'a, WeakRef<T>, A>,
}

impl<'a, T: BStackWeakable, A: BStackRaiiAllocator> BStackWeak<'a, T, A> {
    /// Reconstruct a weak handle from its raw control ref.
    ///
    /// # Safety
    /// `ctrl` must describe a live control block owned by `allocator`, and this
    /// handle must account for a weak count the caller has already established.
    pub unsafe fn from_raw(ctrl: BStackRef<T::Control>, allocator: &'a A) -> Self {
        Self {
            inner: unsafe { AutoDrop::from_raw(WeakRef(ctrl), allocator) },
        }
    }

    fn ctrl(&self) -> BStackRef<T::Control> {
        self.inner.handle().0
    }

    fn allocator(&self) -> &'a A {
        self.inner.allocator()
    }

    /// Consume the handle into its raw control ref **without** decrementing the
    /// weak count — the count is transferred to the caller.
    pub fn into_raw(self) -> BStackRef<T::Control> {
        let (weak, _) = self.inner.into_raw_parts();
        weak.0
    }

    /// Attempt to promote to a strong handle. Succeeds iff `ctrl.strong` is
    /// currently non-zero (CAS-increment-if-nonzero), reading `ctrl.x` to recover
    /// the data ref. Returns `None` if the data block is already gone.
    pub fn upgrade(&self) -> io::Result<Option<BStackRc<'a, T, A>>> {
        let allocator = self.allocator();
        let stack = allocator.stack();
        let ctrl_range = self.ctrl().into_range();
        let strong_off = ctrl_range.start() + layout::CTRL_STRONG_OFFSET;
        if refcount::increment_if_nonzero(stack, strong_off)?.is_none() {
            return Ok(None);
        }
        // Strong is now claimed; recover the data ref from the forward pointer.
        let data_pos = ctrl_range.start() + layout::CTRL_DATA_OFFSET;
        let mut bytes = [0u8; 8];
        stack.get_into(data_pos, &mut bytes)?;
        let data_range = BStackRange::new(u64::from_le_bytes(bytes), size_of::<T::OnDisk>() as u64);
        let data = unsafe { BStackRef::<T>::from_range(data_range) };
        // SAFETY: the increment above claimed the strong count this handle holds.
        Ok(Some(unsafe {
            BStackRc::from_raw(data, Some(ctrl_range), allocator)
        }))
    }
}

/// Cloning a weak handle bumps the control block's weak count and returns
/// another weak handle to the **same** control block. This is the *only* sound
/// meaning of a weak clone: a weak reference observes a live object's control
/// block, and a copy that observed anything else would not be observing what the
/// original does. So a weak clone shares the observation (a count bump) rather
/// than deep-copying — there is no `TryCloneIn` for a weak reference.
impl<'a, T: BStackWeakable, A: BStackRaiiAllocator> TryClone for BStackWeak<'a, T, A> {
    fn try_clone(&self) -> io::Result<Self> {
        let weak_off = self.ctrl().into_range().start() + layout::CTRL_WEAK_OFFSET;
        refcount::fetch_add(self.allocator().stack(), weak_off, 1)?;
        // SAFETY: the fetch_add above established the weak count this clone holds.
        Ok(unsafe { Self::from_raw(self.ctrl(), self.allocator()) })
    }
}
