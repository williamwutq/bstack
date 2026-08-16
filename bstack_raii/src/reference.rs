//! [`BStackRef<T>`]: a typed, non-owning wrapper over a [`bstack::BStackRange`].

use core::marker::PhantomData;
use std::io;

use bstack::{BStack, BStackRange, BStackSlice};

use crate::block::BStackBlock;

/// A typed reference to a block of type `T`.
///
/// Like [`BStackRange`], it carries no backing reference and performs no I/O of
/// its own — it is the serialization form of a typed pointer and is `Copy`.
/// Resolving it into a live handle requires an allocator or stack supplied
/// externally.
///
/// The in-memory form wraps a [`BStackRange`]. The *on-disk* encoding of a ref
/// (little-endian, fixed width) is a separate `Pod` representation the macro
/// emits inside `XOnDisk`; it is not this type, because `BStackRange` is not
/// itself `bytemuck::Pod`.
///
/// # Not fully qualified: pair it with its own file
///
/// A `BStackRef` names only a **location** (an offset within *some* file) — never a
/// file identity. Resolving it ([`read_on_disk`](Self::read_on_disk), or a generated
/// field accessor) supplies the file as the `stack`/allocator argument, and nothing
/// checks that the file you pass is the one this ref actually lives in. Reading it
/// against the wrong file returns wrong data or an [`io::Error`] (out-of-bounds I/O is
/// bounds-checked) — never undefined behaviour, so this stays a safe API — but it is a
/// silent-wrong-data footgun: **always pair a `BStackRef` with the file it came from.**
/// For a *self-qualifying* pointer that carries its own file identity and resolves
/// through the registry, use [`Foreign<T>`](crate::Foreign) instead.
///
/// A `BStackRef` is **read-only** (only [`read_on_disk`](Self::read_on_disk)); writes go
/// through a file — the generated `set_<field>`/`replace_<field>` mutators (which take
/// the `stack`) or the `unsafe raw_<field>_slice` place — so an unqualified ref can
/// never *corrupt* a file, only read the wrong one.
#[repr(transparent)]
pub struct BStackRef<T> {
    range: BStackRange,
    _marker: PhantomData<fn() -> T>,
}

impl<T> BStackRef<T> {
    /// Wrap a raw range as a typed reference.
    ///
    /// # Safety
    /// The caller asserts `range` refers to a validly allocated block of type
    /// `T` (or will, by the time it is resolved).
    pub const unsafe fn from_range(range: BStackRange) -> Self {
        Self {
            range,
            _marker: PhantomData,
        }
    }

    /// The underlying untyped range.
    pub const fn into_range(self) -> BStackRange {
        self.range
    }

    /// Reinterpret this reference as pointing at a different type `U`, keeping
    /// the same range. Used to move between a data ref and its control-block ref.
    ///
    /// # Safety
    /// The caller asserts the range is valid for `U`.
    pub const unsafe fn cast<U>(self) -> BStackRef<U> {
        BStackRef {
            range: self.range,
            _marker: PhantomData,
        }
    }
}

impl<T: BStackBlock> BStackRef<T> {
    /// Read this block's on-disk payload into `buf` and reinterpret it.
    ///
    /// Buffer-based (no zero-copy without `mmap`): `buf` must be at least
    /// `size_of::<T::OnDisk>()` bytes. The returned reference borrows `buf`.
    pub fn read_on_disk<'b>(self, stack: &BStack, buf: &'b mut [u8]) -> io::Result<&'b T::OnDisk> {
        let size = core::mem::size_of::<T::OnDisk>();
        if buf.len() < size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("read_on_disk: buffer of {} < OnDisk size {size}", buf.len()),
            ));
        }
        let dst = &mut buf[..size];
        // `OnDisk` is `#[repr(C, packed)]` (alignment 1), so any buffer address is
        // adequately aligned and `from_bytes` will not panic on alignment.
        // `read_into` fills `min(dst.len(), block.len())`; for a fixed-size block
        // those are equal.
        let slice = unsafe { BStackSlice::from_raw_range(stack, self.range) };
        slice.read_into(dst)?;
        Ok(bytemuck::from_bytes(dst))
    }
}

impl<T> Clone for BStackRef<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for BStackRef<T> {}
