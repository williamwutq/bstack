//! Lifetime-extending reborrows for generator call sites.
//!
//! [`BStack::process_gen`](crate::BStack::process_gen) and
//! [`BStack::inplace_gen`](crate::BStack::inplace_gen) take a `FnMut` closure
//! that yields [`BStackGenOp<'a>`](crate::BStackGenOp) values, where `'a` is a
//! lifetime chosen by the *caller* and outlives the whole call.  A generator
//! that hands the ops a short-lived scratch buffer — an 8-byte free-list head,
//! say — therefore needs a `&'a mut [u8]` derived from a local the closure
//! captured, and that is exactly what the borrow checker refuses:
//!
//! ```text
//! error[E0521]: borrowed data escapes outside of closure
//!   ...captured variable cannot escape `FnMut` closure body
//! ```
//!
//! The rejection is a limitation of the closure model, not a real hazard: a
//! `FnMut` body only ever gets `&mut Env` for the duration of one call, so
//! nothing reborrowed out of a capture can be *proved* to outlive the call,
//! even when the referent is a stack local that plainly does.  The same
//! pattern is rejected under Polonius Alpha and is outside the stated scope of
//! Full Polonius, so the workaround is not going away on its own.
//!
//! [`bstack_unsafe_reborrow!`](crate::bstack_unsafe_reborrow) and
//! [`bstack_unsafe_reborrow_mut!`](crate::bstack_unsafe_reborrow_mut) package
//! that workaround: they extend a borrow's lifetime — and *only* its lifetime,
//! the referent type is preserved by construction — so the call site reads as
//! one greppable, visibly-unsafe operation instead of an open-coded
//! [`transmute`](core::mem::transmute).  The `unsafe` in each macro's *name*
//! carries the marking, so the call site does not write an `unsafe` block of
//! its own; it still owes the safety obligations below, and should still carry
//! a `// SAFETY:` comment saying why they hold.
//!
//! ```
//! # #[cfg(all(feature = "set", feature = "atomic"))]
//! # fn demo(stack: &bstack::BStack, head_off: u64) -> std::io::Result<()> {
//! use bstack::{BStackGenOp, bstack_unsafe_reborrow, bstack_unsafe_reborrow_mut};
//!
//! let mut head_buf = [0u8; 8];
//! let mut next_buf = [0u8; 8];
//! let mut step = 0u32;
//! stack.process_gen(|| {
//!     let op = match step {
//!         // Read the free-list head into a scratch buffer.
//!         // SAFETY: `head_buf` is declared before this call and never moved,
//!         // so it outlives the whole sequence; the op is consumed before the
//!         // closure runs again, so no other access to `head_buf` overlaps it.
//!         0 => Some(BStackGenOp::Read {
//!             offset: head_off,
//!             buf: bstack_unsafe_reborrow_mut!(&mut head_buf[..]),
//!         }),
//!         // Step 0's read has resolved, so `head_buf` can be inspected here.
//!         1 => {
//!             next_buf = u64::from_le_bytes(head_buf).wrapping_add(1).to_le_bytes();
//!             // SAFETY: as above; `next_buf` is not touched again after this
//!             // step hands it to the write.
//!             Some(BStackGenOp::Write {
//!                 offset: head_off,
//!                 data: bstack_unsafe_reborrow!(&next_buf[..]),
//!             })
//!         }
//!         _ => None,
//!     };
//!     step += 1;
//!     op
//! })
//! # }
//! # fn main() {}
//! ```
//!
//! # Safety contract
//!
//! Both macros are `unsafe`: they produce a reference whose lifetime the
//! compiler can no longer check, and neither the macro nor the crate can
//! verify any of the following.  The caller must guarantee, for every
//! extended reference:
//!
//! 1. **The referent outlives the call.**  It must live at least until
//!    `process_gen` / `inplace_gen` returns — in practice, a local declared
//!    *before* the call and not moved, reallocated, or dropped during it.  A
//!    buffer created inside the closure body, or a `Vec` that may reallocate
//!    mid-sequence, does not qualify.
//! 2. **No overlapping access while the callee holds it.**  Between the moment
//!    an op is yielded and the moment the callee finishes with it, the referent
//!    must not be read or written through any other path, including the
//!    closure's own captures.  For `process_gen` this is automatic for the
//!    common shape: each op is consumed and dropped before the closure is
//!    called again, so inspecting a buffer at a later step — after the `Read`
//!    that filled it has resolved — is fine.
//! 3. **`inplace_gen` retains write payloads.**  Unlike `process_gen`, an
//!    `inplace_gen` `Write` is *not* applied immediately: the borrowed `data`
//!    is staged and held until the batch commits at the end of the call, and
//!    later `Read`s are served from that staging overlay.  A buffer handed to a
//!    `Write` must therefore be treated as frozen — no mutation, no
//!    overlapping mutable reborrow — for the remainder of the call, not merely
//!    for that one step.
//!
//! Rule 3 is the one most easily missed, and the only one whose violation is
//! undefined behaviour outright rather than a dangling read: mutating memory
//! behind a live shared reference is UB regardless of what the callee does
//! with it.
//!
//! These macros are a documentation and auditability device, not a proof.  New
//! code that can be expressed without them should be.

#[doc(inline)]
pub use crate::{bstack_unsafe_reborrow, bstack_unsafe_reborrow_mut};

/// Extends a shared borrow's lifetime, preserving the referent type.
///
/// Implementation detail of [`bstack_unsafe_reborrow!`](crate::bstack_unsafe_reborrow);
/// not part of the public API.
///
/// # Safety
///
/// See the [module documentation](self#safety-contract).  The referent must
/// outlive `'long` and must not be mutated or mutably reborrowed while the
/// returned reference is live.
#[doc(hidden)]
#[inline(always)]
#[must_use]
pub const unsafe fn __extend<'long, T: ?Sized>(r: &T) -> &'long T {
    // SAFETY: a lifetime-only change; the referent type is fixed by the
    // signature. The caller guarantees the referent outlives `'long`.
    unsafe { &*core::ptr::from_ref(r) }
}

/// Extends a mutable borrow's lifetime, preserving the referent type.
///
/// Implementation detail of [`bstack_unsafe_reborrow_mut!`](crate::bstack_unsafe_reborrow_mut);
/// not part of the public API.
///
/// # Safety
///
/// See the [module documentation](self#safety-contract).  The referent must
/// outlive `'long` and must not be accessed through any other path while the
/// returned reference is live.
#[doc(hidden)]
#[inline(always)]
#[must_use]
pub const unsafe fn __extend_mut<'long, T: ?Sized>(r: &mut T) -> &'long mut T {
    // SAFETY: a lifetime-only change; the referent type is fixed by the
    // signature. The caller guarantees the referent outlives `'long` and that
    // the returned reference is the only live path to it.
    unsafe { &mut *core::ptr::from_mut(r) }
}

/// Extends a shared borrow so it can escape a `process_gen` / `inplace_gen`
/// generator closure.
///
/// Takes a borrow expression and returns the same reference with a caller-chosen
/// (inferred) lifetime — the referent type is preserved by construction, so this
/// cannot silently reinterpret the pointee the way an open-coded
/// [`transmute`](core::mem::transmute) with an inferred target can.  An optional
/// second argument ascribes the resulting reference type for readability:
///
/// ```
/// # use bstack::bstack_unsafe_reborrow;
/// let buf = [0u8; 8];
/// // SAFETY: `buf` outlives every use of `long` below.
/// let long: &[u8] = bstack_unsafe_reborrow!(&buf[..]);
/// // SAFETY: as above.
/// let also: &[u8] = bstack_unsafe_reborrow!(&buf[..], &[u8]);
/// assert_eq!(long, also);
/// ```
///
/// # Safety
///
/// See the [`reborrow` module documentation](crate::reborrow#safety-contract)
/// for the full contract.  In short: the referent must outlive the entire
/// enclosing `process_gen` / `inplace_gen` call, and must not be mutated while
/// the extended reference is live — which, for a buffer handed to an
/// `inplace_gen` `Write`, means for the rest of the call, since staged writes
/// are held until the batch commits.
#[macro_export]
macro_rules! bstack_unsafe_reborrow {
    ($borrow:expr $(,)?) => {{
        // The caller's expression is evaluated in a safe context, so the
        // `unsafe` below covers the lifetime extension and nothing else.
        let __bstack_borrow = $borrow;
        // SAFETY: delegated to the caller by the macro's contract; `unsafe` in
        // the macro's own name is what marks the obligation at the call site.
        unsafe { $crate::reborrow::__extend(__bstack_borrow) }
    }};
    ($borrow:expr, $ty:ty $(,)?) => {{
        let __bstack_reborrowed: $ty = $crate::bstack_unsafe_reborrow!($borrow);
        __bstack_reborrowed
    }};
}

/// Extends a mutable borrow so it can escape a `process_gen` / `inplace_gen`
/// generator closure.
///
/// The mutable counterpart of
/// [`bstack_unsafe_reborrow!`](crate::bstack_unsafe_reborrow); see that macro
/// for the argument forms.
///
/// ```
/// # use bstack::bstack_unsafe_reborrow_mut;
/// let mut buf = [0u8; 8];
/// // SAFETY: `buf` outlives every use of `long`, which is the only live path
/// // to it while it is in scope.
/// let long: &mut [u8] = bstack_unsafe_reborrow_mut!(&mut buf[..]);
/// long[0] = 1;
/// assert_eq!(buf[0], 1);
/// ```
///
/// # Safety
///
/// See the [`reborrow` module documentation](crate::reborrow#safety-contract)
/// for the full contract.  In short: the referent must outlive the entire
/// enclosing `process_gen` / `inplace_gen` call, and while the extended
/// reference is live it must be the *only* live path to the referent — no
/// reads, writes, or reborrows through the original binding.
#[macro_export]
macro_rules! bstack_unsafe_reborrow_mut {
    ($borrow:expr $(,)?) => {{
        // The caller's expression is evaluated in a safe context, so the
        // `unsafe` below covers the lifetime extension and nothing else.
        let __bstack_borrow = $borrow;
        // SAFETY: delegated to the caller by the macro's contract; `unsafe` in
        // the macro's own name is what marks the obligation at the call site.
        unsafe { $crate::reborrow::__extend_mut(__bstack_borrow) }
    }};
    ($borrow:expr, $ty:ty $(,)?) => {{
        let __bstack_reborrowed: $ty = $crate::bstack_unsafe_reborrow_mut!($borrow);
        __bstack_reborrowed
    }};
}
