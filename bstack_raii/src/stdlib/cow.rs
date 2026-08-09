//! [`BStackCow<T>`]: clone-on-write ownership of a block.
//!
//! The on-disk analogue of [`std::borrow::Cow`]. A `BStackCow<T>` is *either* a
//! non-owning [`BStackRef<T>`] into a block someone else owns, *or* a
//! [`BStackOwned<T>`] block it owns outright. Reads work identically through
//! both; the first time the caller needs to *own* the block — [`into_owned`] or
//! [`to_mut`] — a borrowed `Cow` deep-copies the referenced block into a fresh
//! owned one (via [`TryCloneIn`]) and becomes owned. An already-owned `Cow`
//! pays nothing.
//!
//! This is the persistent-storage version of the borrow-until-you-mutate
//! pattern: hand out a cheap `Borrowed` view of a shared block, and only spend
//! an allocation + deep copy at the point a mutation actually needs a private
//! copy.
//!
//! [`into_owned`]: BStackCow::into_owned
//! [`to_mut`]: BStackCow::to_mut

use std::io;

use crate::wal::BStackWalAnchor;
use bstack::{BStackOwnedSliceAllocator, BStackRange};

use crate::block::BStackBlock;
use crate::clone::TryCloneIn;
use crate::owned::BStackOwned;
use crate::reference::BStackRef;
use crate::teardown::{AutoDrop, BStackDrop};

/// Clone-on-write ownership of a block of type `T`.
///
/// * [`Borrowed`](BStackCow::Borrowed) — a non-owning [`BStackRef<T>`]. Dropping
///   it frees nothing; the owner lives elsewhere.
/// * [`Owned`](BStackCow::Owned) — a [`BStackOwned<T>`] this handle owns.
///   Dropping it (via [`bstack_drop`](BStackDrop::bstack_drop) or an
///   [`AutoDrop`] guard) recursively frees the block.
///
/// The write path ([`into_owned`](Self::into_owned) / [`to_mut`](Self::to_mut))
/// requires `T: TryCloneIn`, i.e. a **plain** (uniquely-owned) block — the same
/// blocks that can be deep-copied. Construction and all read access need only
/// `T: BStackBlock`, so a borrowed `Cow` over any block kind is fine as long as
/// you never ask it to become owned.
pub enum BStackCow<T: BStackBlock> {
    /// A non-owning reference to a block owned elsewhere.
    Borrowed(BStackRef<T>),
    /// A block this handle owns outright.
    Owned(BStackOwned<T>),
}

impl<T: BStackBlock> BStackCow<T> {
    /// Wrap a non-owning reference: a `Borrowed` `Cow` that frees nothing on
    /// teardown and deep-copies on first write.
    pub fn borrowed(reference: BStackRef<T>) -> Self {
        BStackCow::Borrowed(reference)
    }

    /// Wrap an owned block: an `Owned` `Cow` that already holds a private copy,
    /// so the write path is free.
    pub fn owned(owned: BStackOwned<T>) -> Self {
        BStackCow::Owned(owned)
    }

    /// `true` if this is a [`Borrowed`](Self::Borrowed) reference (no private
    /// copy yet).
    pub fn is_borrowed(&self) -> bool {
        matches!(self, BStackCow::Borrowed(_))
    }

    /// `true` if this already [`Owned`](Self::Owned)s its block.
    pub fn is_owned(&self) -> bool {
        matches!(self, BStackCow::Owned(_))
    }

    /// A non-owning [`BStackRef<T>`] to the current block, whichever variant is
    /// held — the uniform read handle. Cheap (a copied range); it does **not**
    /// change ownership.
    pub fn as_ref(&self) -> BStackRef<T> {
        match self {
            // SAFETY: an owned block is a live allocation of type `T`, exactly
            // what `BStackRef::from_range` asserts.
            BStackCow::Owned(o) => unsafe { BStackRef::from_range(o.handle().range()) },
            BStackCow::Borrowed(r) => *r,
        }
    }

    /// The underlying block range, whichever variant is held.
    pub fn range(&self) -> BStackRange {
        self.as_ref().into_range()
    }

    /// Materialize a fresh, bare `T` handle over the current block for calling
    /// the block's generated field accessors — e.g.
    /// `cow.handle().field(stack)`. Works for both variants; carries no
    /// ownership (dropping it frees nothing).
    pub fn handle(&self) -> T {
        <T as BStackBlock>::from_range(self.range())
    }

    /// Collapse to an owned block, deep-copying if currently borrowed.
    ///
    /// * `Owned` — returned as-is; no I/O.
    /// * `Borrowed` — the referenced block is deep-cloned into a fresh
    ///   independent [`BStackOwned<T>`] allocated with `allocator`.
    pub fn into_owned<A: BStackWalAnchor>(self, allocator: &A) -> io::Result<BStackOwned<T>>
    where
        T: TryCloneIn,
    {
        match self {
            BStackCow::Owned(o) => Ok(o),
            BStackCow::Borrowed(r) => {
                <T as BStackBlock>::from_range(r.into_range()).try_clone_in(allocator)
            }
        }
    }

    /// Ensure this `Cow` owns its block and return a mutable handle to it,
    /// deep-copying first if it was borrowed.
    ///
    /// After this call the `Cow` is [`Owned`](Self::Owned); mutations applied
    /// through the returned handle (the block's setters + `allocator`) never
    /// touch the originally borrowed block. A no-op (beyond the ownership
    /// check) when already owned.
    pub fn to_mut<A: BStackWalAnchor>(&mut self, allocator: &A) -> io::Result<&mut BStackOwned<T>>
    where
        T: TryCloneIn,
    {
        if let BStackCow::Borrowed(r) = self {
            // `BStackRef` is `Copy`; take the range out before we overwrite it.
            let owned =
                <T as BStackBlock>::from_range((*r).into_range()).try_clone_in(allocator)?;
            *self = BStackCow::Owned(owned);
        }
        match self {
            BStackCow::Owned(o) => Ok(o),
            // The block above converted any `Borrowed` into `Owned`.
            BStackCow::Borrowed(_) => unreachable!("to_mut just ensured Owned"),
        }
    }

    /// Attach an allocator to make an auto-freeing [`AutoDrop`] guard: dropping
    /// the returned value runs this `Cow`'s teardown (a no-op when borrowed).
    pub fn auto<A: BStackWalAnchor>(self, allocator: &A) -> AutoDrop<'_, Self, A> {
        // SAFETY: an `Owned` variant asserts sole ownership of a live block; a
        // `Borrowed` variant frees nothing, so the assertion is trivially met.
        unsafe { AutoDrop::from_raw(self, allocator) }
    }
}

impl<T: BStackBlock> BStackDrop for BStackCow<T> {
    /// Free the block **only** when owned; a borrowed `Cow` has no claim on its
    /// target and frees nothing.
    fn bstack_drop<A: BStackWalAnchor>(self, allocator: &A) -> io::Result<()> {
        match self {
            BStackCow::Owned(o) => o.bstack_drop(allocator),
            BStackCow::Borrowed(_) => Ok(()),
        }
    }
}

impl<T: BStackBlock> From<BStackOwned<T>> for BStackCow<T> {
    fn from(owned: BStackOwned<T>) -> Self {
        BStackCow::Owned(owned)
    }
}

impl<T: BStackBlock> From<BStackRef<T>> for BStackCow<T> {
    fn from(reference: BStackRef<T>) -> Self {
        BStackCow::Borrowed(reference)
    }
}
