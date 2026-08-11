//! [`BStackOwned`]: a without-allocator, uniquely-owned block handle.
//!
//! `BStackOwned<T>` is an *ownership marker* over an inner [`BStackDrop`] handle
//! (typically a `#[bstack_block]` type). Its own [`BStackDrop`] recursively frees
//! the block — but, being a bare handle, it frees **nothing on scope exit**:
//! teardown is explicit ([`bstack_drop`](BStackDrop::bstack_drop)) or automatic
//! only once wrapped in an [`AutoDrop`] (`owned.auto(alloc)`), whose Rust `Drop`
//! runs it. This keeps a persistent root from being silently deleted when its
//! handle drops.
//!
//! `X::new(..)` and `bstack_move!`'d owned children hand back a bare
//! `BStackOwned<X>`; the caller decides when (and whether) it dies.

use core::ops::Deref;
use std::io;

use crate::BStackRaiiAllocator;
use crate::block::{BStackMove, BStackMoveExpr};
use crate::teardown::{AutoDrop, BStackDrop, wal_teardown};

/// A uniquely-owned handle to a block: an ownership marker over an inner
/// [`BStackDrop`] handle whose teardown recursively frees the block on disk.
///
/// Carries no allocator (unlike an [`AutoDrop`]-wrapped handle), so it never
/// frees itself on `Drop`. Wrap it via [`auto`](Self::auto) for RAII, or free it
/// explicitly with [`BStackDrop::bstack_drop`].
pub struct BStackOwned<T: BStackDrop>(T);

impl<T: BStackDrop> BStackOwned<T> {
    /// Mark `inner` as uniquely owned.
    ///
    /// # Safety
    /// The caller asserts `inner` describes a live allocation that no other
    /// handle will also free.
    pub unsafe fn from_raw(inner: T) -> Self {
        BStackOwned(inner)
    }

    /// Unwrap to the inner handle, dropping the ownership marker without freeing
    /// anything (the caller takes over responsibility). Used to read a child's
    /// offset when transferring it into a parent field.
    pub fn into_inner(self) -> T {
        self.0
    }

    /// Borrow the inner handle, e.g. to call generated field accessors:
    /// `owned.handle().get_field(stack)`.
    pub fn handle(&self) -> &T {
        &self.0
    }

    /// Attach an allocator to make an auto-freeing [`AutoDrop`] guard: dropping
    /// the returned value runs this handle's recursive teardown.
    pub fn auto<A: BStackRaiiAllocator>(self, allocator: &A) -> AutoDrop<'_, Self, A> {
        // SAFETY: a `BStackOwned` asserts sole ownership of a live block at
        // construction, exactly the invariant `AutoDrop::from_raw` requires.
        unsafe { AutoDrop::from_raw(self, allocator) }
    }
}

impl<T: BStackDrop> Deref for BStackOwned<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T: BStackDrop> BStackDrop for BStackOwned<T> {
    fn bstack_drop<A: BStackRaiiAllocator>(self, allocator: &A) -> io::Result<()> {
        // Recursively free the owned block (and its children) as one crash-atomic,
        // leak-reclaiming batch — automatically, whenever the allocator names a WAL
        // anchor. `wal_teardown` collects the whole subtree's frees and commits
        // them as one transaction (and is a plain teardown when there is no anchor,
        // or when this runs nested inside an outer teardown that owns the sink).
        wal_teardown(self.0, allocator)
    }
}

/// `bstack_move!` on an `AutoDrop`-wrapped owned handle: defuse the guard and
/// destructure via the block's [`BStackMove`], passing the recovered allocator.
///
/// (A *bare* `BStackOwned<X>` carries no allocator, so it is moved with the
/// explicit two-argument form `bstack_move!(owned, allocator)` instead.)
impl<'a, X: BStackMove, A: BStackRaiiAllocator> BStackMoveExpr for AutoDrop<'a, BStackOwned<X>, A> {
    type Output = io::Result<X::Fields<'a, A>>;
    fn bstack_move(self) -> Self::Output {
        let (owned, allocator) = self.into_raw_parts();
        X::bstack_move(owned, allocator)
    }
}
