use super::{BStackAllocator, BStackSlice};
use crate::BStack;
use std::io;

/// A singleton allocator that carries no underlying [`BStack`] and never
/// allocates.
///
/// `ManualAllocator` is for code that wants the [`BStackSlice`] type — a
/// typed `(offset, len)` handle with the standard slice interface — but
/// manages positions on the backing [`BStack`] directly, without delegating
/// region tracking to an allocator.
///
/// # Behaviour
///
/// | Operation                                         | Behaviour                    |
/// |---------------------------------------------------|------------------------------|
/// | [`alloc`](BStackAllocator::alloc)                 | Returns `Err(Unsupported)`   |
/// | [`realloc`](BStackAllocator::realloc)             | Returns `Err(Unsupported)`   |
/// | [`dealloc`](BStackAllocator::dealloc)             | Returns `Ok(())` (no-op)     |
/// | [`len`](BStackAllocator::len)                     | Returns `Err(Unsupported)`   |
/// | [`is_empty`](BStackAllocator::is_empty)           | Returns `Err(Unsupported)`   |
/// | [`stack`](BStackAllocator::stack)                 | **Panics** — no backing stack |
/// | [`into_stack`](BStackAllocator::into_stack)       | **Panics** — no backing stack |
///
/// # Singleton
///
/// Obtain the allocator via [`ManualAllocator::get`], which always returns the
/// same `&'static ManualAllocator`.  Direct construction is intentionally
/// prevented to make the singleton nature explicit and to discourage treating
/// the type as a regular value.
///
/// # Creating slices
///
/// Use [`BStackSlice::from_raw_parts`] to create a slice at a known position:
///
/// ```rust,ignore
/// let slice = unsafe { BStackSlice::from_raw_parts(ManualAllocator::get(), offset, len) };
/// ```
///
/// Or reconstruct one from a serialised token:
///
/// ```rust,ignore
/// let slice = unsafe { BStackSlice::from_bytes(ManualAllocator::get(), token) };
/// ```
///
/// Because there is no backing [`BStack`], calling [`BStackSlice::read`] or
/// [`BStackSlice::write`] on such a slice will **panic**.  The slice is useful
/// as a typed coordinate — serialised, compared, sorted, stored alongside
/// other data — while all I/O is performed directly on the [`BStack`] via
/// [`BStack::get`] / [`BStack::set`] at the offsets the slice describes.
///
/// # Manual position management
///
/// Since no allocator tracks ownership, the caller is entirely responsible for
/// ensuring that every `(offset, len)` pair describes a valid, live region
/// within the backing [`BStack`], that regions do not overlap unless
/// intentional, and that the [`BStack`] is not truncated beneath a live slice.
/// No bookkeeping or validation is performed on construction.
pub struct ManualAllocator(());

static INSTANCE: ManualAllocator = ManualAllocator(());

impl ManualAllocator {
    /// Returns a reference to the global singleton instance.
    pub fn get() -> &'static Self {
        &INSTANCE
    }
}

impl BStackAllocator for ManualAllocator {
    type Error = io::Error;
    type Allocated<'a> = BStackSlice<'a, Self>;

    /// Always panics — `ManualAllocator` has no backing [`BStack`].
    ///
    /// Use the [`BStack`] directly for all I/O.
    fn stack(&self) -> &BStack {
        panic!("ManualAllocator has no backing BStack; perform I/O directly on the BStack")
    }

    /// Always panics — `ManualAllocator` has no backing [`BStack`] to return.
    fn into_stack(self) -> BStack {
        panic!("ManualAllocator has no backing BStack")
    }

    /// Always returns `Err(Unsupported)`.
    ///
    /// Construct slices manually with [`BStackSlice::from_raw_parts`] instead.
    fn alloc(&self, _len: u64) -> io::Result<BStackSlice<'_, Self>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "ManualAllocator does not allocate; construct slices with BStackSlice::from_raw_parts",
        ))
    }

    /// Always returns `Err(Unsupported)`.
    ///
    /// Manage slice positions manually by creating new [`BStackSlice`] handles.
    fn realloc<'a>(
        &'a self,
        _slice: BStackSlice<'a, Self>,
        _new_len: u64,
    ) -> io::Result<BStackSlice<'a, Self>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "ManualAllocator does not reallocate; manage positions manually",
        ))
    }

    /// Always returns `Ok(())`.
    ///
    /// There is nothing to free — ownership of the region is the caller's
    /// responsibility.
    fn dealloc(&self, _slice: BStackSlice<'_, Self>) -> io::Result<()> {
        Ok(())
    }

    /// Always returns `Err(Unsupported)` — there is no backing [`BStack`].
    fn len(&self) -> io::Result<u64> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "ManualAllocator has no backing BStack",
        ))
    }

    /// Always returns `Err(Unsupported)` — there is no backing [`BStack`].
    fn is_empty(&self) -> io::Result<bool> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "ManualAllocator has no backing BStack",
        ))
    }
}
