//! # `bstack_raii`
//!
//! A typed, RAII-style ownership, lifetime, and on-disk-layout layer built on
//! top of the mainline `bstack` `alloc` primitives ([`bstack::BStackRange`],
//! [`bstack::BStackSlice`], [`bstack::BStackOwnedSlice`]). It decouples
//! disk-level destruction ([`BStackDrop`]) from Rust's process-scoped `Drop`,
//! providing persistent-storage ownership semantics with C++-style
//! `unique_ptr` / `shared_ptr` / `weak_ptr` conveniences.
//!
//! The full design lives in `RAII.md` at the repository root. This crate is the
//! implementation of that document. It is intentionally kept as a separate
//! crate (rather than a `bstack` feature flag) because it introduces a large,
//! not-yet-stable ABI surface (block layouts, control blocks, refcounting) that
//! must not gate `bstack`'s own ABI stability.
//!
//! ## Layout of this crate
//!
//! * This crate ([`bstack_raii`]) holds the **runtime**: the [`BStackDrop`],
//!   [`BStackWeakable`], and [`BStackCast`] traits, the on-disk [`BlockHeader`]
//!   / [`EightCC`], the typed handle types ([`BStackRef`], and — once
//!   implemented — `BStackOwned` / `BStackRc` / `BStackWeak`), and the small
//!   `Copy` child-handle types that carry the recursive/atomic teardown logic.
//! * The nested [`bstack_raii_derive`] proc-macro crate holds the **code
//!   generators**: [`macro@bstack_block`], [`bstack_move`], and
//!   [`bstack_cast`]. They emit calls into the runtime defined here.
//!
//! ## Status
//!
//! Scaffold only. The types and traits below establish the shape described in
//! `RAII.md`; method bodies marked `todo!()` are the work to be filled in.

#![allow(dead_code)]

use core::marker::PhantomData;
use std::io;

use bstack::{BStackOwnedSlice, BStackOwnedSliceAllocator, BStackRange};

// Re-export the procedural macros so callers only depend on `bstack_raii`.
pub use bstack_raii_derive::{bstack_block, bstack_cast, bstack_move};

// ---------------------------------------------------------------------------
// On-disk header
// ---------------------------------------------------------------------------

/// An 8-byte type tag stored in every block header.
///
/// Used instead of the traditional 4-byte `FourCC` because `bstack` offsets are
/// 64-bit, so 8-byte alignment is natural. Derived from the block type's name at
/// `#[bstack_block]` expansion time and compared during safe downcasts
/// ([`BStackCast`]).
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct EightCC(pub [u8; 8]);

impl EightCC {
    /// Construct a tag from a raw 8-byte array.
    pub const fn new(tag: [u8; 8]) -> Self {
        Self(tag)
    }
}

/// The header prefixing every on-disk block. 16 bytes, `#[repr(C, packed)]`.
///
/// The `size` field is the payload length in bytes; `tag` is the [`EightCC`]
/// discriminant written by the allocator at block creation.
#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct BlockHeader {
    pub size: u64,
    pub tag: EightCC,
}

// ---------------------------------------------------------------------------
// Typed reference: BStackRef<T>
// ---------------------------------------------------------------------------

/// A typed, non-owning wrapper over a [`BStackRange`].
///
/// Like `BStackRange`, it carries no backing reference and performs no I/O on
/// its own — it is the serialization form of a typed pointer and is `Copy`.
/// Resolving it into a live handle requires an allocator or stack supplied
/// externally. On disk it occupies the same bytes as a `BStackRange`.
#[repr(transparent)]
pub struct BStackRef<T> {
    range: BStackRange,
    _marker: PhantomData<fn() -> T>,
}

impl<T> BStackRef<T> {
    /// Wrap a raw range as a typed reference.
    ///
    /// # Safety
    /// The caller asserts that `range` refers to a validly allocated block of
    /// type `T` (or will, by the time it is resolved).
    pub const unsafe fn from_range(range: BStackRange) -> Self {
        Self { range, _marker: PhantomData }
    }

    /// The underlying untyped range.
    pub const fn into_range(self) -> BStackRange {
        self.range
    }
}

impl<T> Clone for BStackRef<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for BStackRef<T> {}

// ---------------------------------------------------------------------------
// Core traits
// ---------------------------------------------------------------------------

/// Disk-level recursive destruction, fully decoupled from Rust's `Drop`.
///
/// Implemented by every `#[bstack_block]` type (destroys the block and recurses
/// into its owned children) and by the small child-handle types below. Takes
/// `self` (the *without-allocator* handle) plus an explicit allocator, so it is
/// generic over all handle-like types rather than tied to `BStackOwnedSlice`.
///
/// Because a bare [`BStackRange`] carries no allocator, freeing a block is done
/// by reconstructing a [`BStackOwnedSlice`] from the range via
/// [`BStackOwnedSlice::from_raw_range`] (`unsafe`) and handing it to the
/// allocator's `dealloc` — there is deliberately no `dealloc_range` on the
/// allocator itself; see the free [`dealloc_range`] helper.
///
/// The allocator is bound to [`BStackOwnedSliceAllocator`] rather than the bare
/// `BStackAllocator`. That supertrait pins `Allocated<'a> = BStackOwnedSlice<'a,
/// A>` (so a reconstructed owned slice is the accepted `dealloc` handle) and
/// `Error = io::Error` (so the whole layer speaks [`io::Result`], like the rest
/// of `bstack`).
pub trait BStackDrop: Sized {
    fn bstack_drop<A: BStackOwnedSliceAllocator>(self, allocator: &A) -> io::Result<()>;
}

/// Marker implemented only for blocks declared `#[bstack_block(rc, weak)]`.
///
/// Its presence is what lets the generated `BStackRc<T>` carry a `ctrl` field
/// and expose `downgrade`/`upgrade`. Plain `#[bstack_block(rc)]` blocks do not
/// implement it, so weak references to them are a compile error rather than a
/// runtime hazard.
pub trait BStackWeakable: BStackDrop {
    /// The generated on-disk control-block type (`XOnDiskRef`) holding the
    /// `strong`/`weak` counters.
    type Control;
}

/// Downcast discriminant. Implemented by every `#[bstack_block]` type.
///
/// The returned [`EightCC`] must match the tag in a block's [`BlockHeader`] for
/// a safe downcast to succeed.
pub trait BStackCast {
    fn eightcc() -> EightCC;
}

// ---------------------------------------------------------------------------
// Child handle types (small, Copy, constructed transiently during teardown)
//
// Each maps to one field annotation and encapsulates its own destruction logic
// so that the code generated per block type stays a flat, uniform sequence of
// `.bstack_drop(allocator)?` calls. See RAII.md "Child Handle Types".
// ---------------------------------------------------------------------------

/// `#[bstack_owned]`: an exclusively-owned child. Teardown recurses via
/// `T::bstack_drop`.
#[derive(Clone, Copy)]
pub struct OwnedRef<T>(pub BStackRef<T>);

/// `#[bstack_strong]` on a plain `(rc)` `T`. Teardown decrements the inline
/// refcount and calls `T::bstack_drop` at zero.
#[derive(Clone, Copy)]
pub struct StrongRef<T>(pub BStackRef<T>);

/// `#[bstack_strong]` on an `(rc, weak)` `T`. Holds both the data ref and the
/// control-block ref; teardown runs the two-phase strong-then-weak decrement.
#[derive(Clone, Copy)]
pub struct StrongWeakRef<T: BStackWeakable>(pub BStackRef<T>, pub BStackRef<T::Control>);

/// `#[bstack_weak]` on an `(rc, weak)` `T`. Holds only the control-block ref;
/// teardown decrements `ctrl.weak` and frees the control block at zero. The data
/// block is never touched.
#[derive(Clone, Copy)]
pub struct WeakRef<T: BStackWeakable>(pub BStackRef<T::Control>);

impl<T: BStackDrop> BStackDrop for OwnedRef<T> {
    fn bstack_drop<A: BStackOwnedSliceAllocator>(self, _allocator: &A) -> io::Result<()> {
        // Recurse into T, then reconstruct a BStackOwnedSlice from the range and
        // dealloc it. See RAII.md "Generated bstack_drop".
        todo!("recurse T::bstack_drop, then dealloc_range")
    }
}

impl<T: BStackDrop> BStackDrop for StrongRef<T> {
    fn bstack_drop<A: BStackOwnedSliceAllocator>(self, _allocator: &A) -> io::Result<()> {
        todo!("CAS-decrement inline refcount; at zero, T::bstack_drop then dealloc_range")
    }
}

impl<T: BStackWeakable> BStackDrop for StrongWeakRef<T> {
    fn bstack_drop<A: BStackOwnedSliceAllocator>(self, _allocator: &A) -> io::Result<()> {
        todo!("decrement ctrl.strong; at zero free data block + release phantom weak")
    }
}

impl<T: BStackWeakable> BStackDrop for WeakRef<T> {
    fn bstack_drop<A: BStackOwnedSliceAllocator>(self, _allocator: &A) -> io::Result<()> {
        todo!("decrement ctrl.weak; free control block at zero")
    }
}

/// Free a raw block range by reconstructing an owned slice and delegating to the
/// allocator. Central helper the generated `bstack_drop` code funnels through,
/// since ranges carry no allocator of their own.
///
/// The [`BStackOwnedSliceAllocator`] bound makes this well-typed: it pins
/// `Allocated<'a> = BStackOwnedSlice<'a, A>` (so the reconstructed slice is the
/// handle `dealloc` accepts) and `Error = io::Error` (so the failure maps
/// straight through `BStackAllocError::source`).
///
/// # Safety
/// `range` must be a live allocation owned by `allocator` that no other live
/// handle will also free.
pub unsafe fn dealloc_range<A: BStackOwnedSliceAllocator>(
    allocator: &A,
    range: BStackRange,
) -> io::Result<()> {
    let owned: BStackOwnedSlice<'_, A> =
        unsafe { BStackOwnedSlice::from_raw_range(allocator, range) };
    allocator.dealloc(owned).map_err(|e| e.source)
}
