//! Without-allocator inner handles: small `Copy` types constructed transiently
//! during teardown, each encapsulating one field annotation's destruction logic.
//!
//! Keeping the per-annotation logic here (rather than in generated block code)
//! means `#[bstack_block]` can emit a flat, uniform sequence of
//! `.bstack_drop(allocator)?` calls. The with-allocator wrappers
//! ([`crate::BStackOwned`], [`crate::BStackRc`], [`crate::BStackWeak`]) hold one
//! of these plus an allocator reference.
//!
//! ## Open design note — what generic teardown needs from a block type
//!
//! The `todo!()` bodies below all need layout facts that only the concrete block
//! type knows, and which the macro can generate. Before implementing them, we
//! must decide how the block-type traits expose:
//!
//! * **Plain `(rc)`**: the byte offset of the inline `refcount` within `OnDisk`
//!   (for [`StrongRef`]).
//! * **`(rc, weak)`**: the `ctrl` back-pointer range out of the data `OnDisk`,
//!   the `x` forward-pointer range out of the `Control` block, and the byte
//!   offsets of `strong` / `weak` within `Control` (for [`StrongWeakRef`] /
//!   [`WeakRef`] / `upgrade`).
//!
//! These are additive trait members (associated `const`s + small accessors) on
//! [`crate::BStackBlock`] / [`crate::BStackWeakable`]; they are intentionally
//! left out until we settle the shape, to avoid baking in surface prematurely.

use std::io;

use bstack::BStackOwnedSliceAllocator;

use crate::block::{BStackBlock, BStackWeakable};
use crate::reference::BStackRef;
use crate::teardown::BStackDrop;

/// `#[bstack_owned]`: an exclusively-owned child.
#[derive(Clone, Copy)]
pub struct OwnedRef<T>(pub BStackRef<T>);

/// `#[bstack_strong]` on a plain `(rc)` `T`: holds just the data ref; teardown
/// decrements the inline refcount and frees at zero.
#[derive(Clone, Copy)]
pub struct StrongRef<T>(pub BStackRef<T>);

/// `#[bstack_strong]` on an `(rc, weak)` `T`: holds the data ref and the control
/// ref; teardown runs the two-phase strong-then-weak release.
#[derive(Clone, Copy)]
pub struct StrongWeakRef<T: BStackWeakable>(pub BStackRef<T>, pub BStackRef<T::Control>);

/// `#[bstack_weak]` on an `(rc, weak)` `T`: holds only the control ref; teardown
/// decrements `ctrl.weak` and frees the control block at zero. The data block is
/// never touched.
#[derive(Clone, Copy)]
pub struct WeakRef<T: BStackWeakable>(pub BStackRef<T::Control>);

impl<T: BStackBlock> BStackDrop for OwnedRef<T> {
    fn bstack_drop<A: BStackOwnedSliceAllocator>(self, allocator: &A) -> io::Result<()> {
        // An owned child is freed by running the block's own recursive teardown,
        // which frees its children (post-order) and then deallocs the block.
        T::from_range(self.0.into_range()).bstack_drop(allocator)
    }
}

impl<T: BStackBlock> BStackDrop for StrongRef<T> {
    fn bstack_drop<A: BStackOwnedSliceAllocator>(self, allocator: &A) -> io::Result<()> {
        // CAS-decrement the inline refcount; at zero, run T's teardown.
        todo!("decrement inline refcount (needs refcount offset); free at zero")
    }
}

impl<T: BStackWeakable> StrongWeakRef<T> {
    /// Resolve the control ref from the data block's on-disk `ctrl` back-pointer
    /// with a single read, then pair it with the data ref.
    pub fn from_disk<A: BStackOwnedSliceAllocator>(
        data_ref: BStackRef<T>,
        allocator: &A,
    ) -> io::Result<Self> {
        todo!("read T::OnDisk, extract ctrl back-pointer, pair with data_ref")
    }
}

impl<T: BStackWeakable> BStackDrop for StrongWeakRef<T> {
    fn bstack_drop<A: BStackOwnedSliceAllocator>(self, allocator: &A) -> io::Result<()> {
        // Phase 1: decrement ctrl.strong. At zero, free the data block (children
        // + shell), then release the phantom weak by decrementing ctrl.weak;
        // if that hits zero, free the control block too.
        todo!("two-phase strong release; see RAII.md 'Two-Phase Teardown'")
    }
}

impl<T: BStackWeakable> WeakRef<T> {
    /// Resolve the control ref from the data block's on-disk `ctrl` back-pointer
    /// with a single read.
    pub fn from_disk<A: BStackOwnedSliceAllocator>(
        data_ref: BStackRef<T>,
        allocator: &A,
    ) -> io::Result<Self> {
        todo!("read T::OnDisk, extract ctrl back-pointer")
    }
}

impl<T: BStackWeakable> BStackDrop for WeakRef<T> {
    fn bstack_drop<A: BStackOwnedSliceAllocator>(self, allocator: &A) -> io::Result<()> {
        // Decrement ctrl.weak; free the control block when it reaches zero.
        todo!("decrement ctrl.weak; free control block at zero")
    }
}
