//! [`BStackVec`]: a persistent, growable POD vector reachable through a
//! **fixed-size** field.
//!
//! A block field can only store a fixed-size value, but a vector's backing store
//! must be able to grow — and `BStackByteVec` **moves** its block on realloc. The
//! fix is one level of indirection:
//!
//! ```text
//! parent field (u64) ── points to ──▶ descriptor block (fixed, never moves)
//!                                          │ { data_off, data_size }
//!                                          └── points to ──▶ BStackByteVec data
//!                                                            block (may realloc/move)
//! ```
//!
//! The **descriptor** is a fixed 16-byte block that holds the current offset and
//! size of the data block. When the data grows and moves, only the descriptor's
//! pointer is rewritten; the parent's pointer to the descriptor is stable. So a
//! `BStackVec` field is identified by its descriptor offset, which never changes.
//!
//! Elements are `bytemuck::Pod`; bytes are stored/read unaligned, so any element
//! type works. `u8` (i.e. `Vec<u8>` / `String` fields) is the common case.
//!
//! > **Growth reallocates**, so use a realloc-safe allocator (see the crate
//! > docs) to avoid corruption on a torn realloc.

use core::marker::PhantomData;
use std::io;

use bstack::{BStackByteVec, BStackOwnedSlice, BStackOwnedSliceAllocator, BStackRange};
use bytemuck::Pod;

use crate::teardown::dealloc_range;

/// Byte size of a descriptor block: `{ data_off: u64, data_size: u64 }`.
pub(crate) const DESCRIPTOR_SIZE: u64 = 16;

/// A persistent, growable vector of POD elements, addressed by a stable
/// descriptor block. Backs `#[bstack_owned] Vec<T>` / `String` fields.
pub struct BStackVec<'a, T, A: BStackOwnedSliceAllocator> {
    /// The descriptor block (`data_off`, `data_size`) — a stable identity.
    desc: BStackRange,
    allocator: &'a A,
    _marker: PhantomData<fn() -> T>,
}

impl<'a, T, A: BStackOwnedSliceAllocator> BStackVec<'a, T, A> {
    /// Reconstruct a handle from its descriptor block offset (e.g. a field
    /// accessor, which stores just that offset).
    ///
    /// # Safety
    /// `desc_off` must be the offset of a live descriptor block written by this
    /// type.
    pub unsafe fn from_descriptor(desc_off: u64, allocator: &'a A) -> Self {
        Self {
            desc: BStackRange::new(desc_off, DESCRIPTOR_SIZE),
            allocator,
            _marker: PhantomData,
        }
    }

    /// The descriptor block's range — the vector's stable on-disk identity.
    pub fn descriptor(&self) -> BStackRange {
        self.desc
    }

    fn read_desc(&self) -> io::Result<(u64, u64)> {
        let mut buf = [0u8; DESCRIPTOR_SIZE as usize];
        self.allocator
            .stack()
            .get_into(self.desc.start(), &mut buf)?;
        let off = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let size = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        Ok((off, size))
    }

    fn write_desc(&self, data_off: u64, data_size: u64) -> io::Result<()> {
        let mut buf = [0u8; DESCRIPTOR_SIZE as usize];
        buf[0..8].copy_from_slice(&data_off.to_le_bytes());
        buf[8..16].copy_from_slice(&data_size.to_le_bytes());
        self.allocator.stack().set(self.desc.start(), buf)
    }

    /// Reconstruct the `BStackByteVec` over the current data block.
    fn bytes(&self) -> io::Result<BStackByteVec<'a, A>> {
        let (off, size) = self.read_desc()?;
        let block = unsafe {
            BStackOwnedSlice::from_raw_range(self.allocator, BStackRange::new(off, size))
        };
        Ok(unsafe { BStackByteVec::from_raw_block(block) })
    }

    /// Free the data block and the descriptor. Consumes the handle.
    pub fn bstack_drop(self) -> io::Result<()> {
        let (off, size) = self.read_desc()?;
        unsafe {
            dealloc_range(self.allocator, BStackRange::new(off, size))?;
            dealloc_range(self.allocator, self.desc)?;
        }
        Ok(())
    }
}

impl<'a, T: Pod, A: BStackOwnedSliceAllocator> BStackVec<'a, T, A> {
    /// Create a vector holding `data`, allocating the data block then a
    /// descriptor pointing at it.
    pub fn from_slice(allocator: &'a A, data: &[T]) -> io::Result<Self> {
        let data_range = BStackByteVec::from_slice(bytemuck::cast_slice(data), allocator)?
            .into_raw_block()
            .as_range();

        let mut desc = match allocator.alloc(DESCRIPTOR_SIZE) {
            Ok(d) => d,
            Err(e) => {
                let _ = unsafe { dealloc_range(allocator, data_range) };
                return Err(e);
            }
        };
        let mut buf = [0u8; DESCRIPTOR_SIZE as usize];
        buf[0..8].copy_from_slice(&data_range.start().to_le_bytes());
        buf[8..16].copy_from_slice(&data_range.len().to_le_bytes());
        if let Err(e) = desc.write_range(0, buf) {
            let _ = allocator.dealloc(desc);
            let _ = unsafe { dealloc_range(allocator, data_range) };
            return Err(e);
        }
        Ok(Self {
            desc: desc.as_range(),
            allocator,
            _marker: PhantomData,
        })
    }

    /// Create an empty vector.
    pub fn new(allocator: &'a A) -> io::Result<Self> {
        Self::from_slice(allocator, &[])
    }

    /// Number of elements.
    pub fn len(&self) -> io::Result<u64> {
        Ok(self.bytes()?.len()? / core::mem::size_of::<T>() as u64)
    }

    /// Whether the vector is empty.
    pub fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Read all elements into a `Vec<T>` (unaligned reads, so any `T` is fine).
    pub fn to_vec(&self) -> io::Result<Vec<T>> {
        let bytes = self.bytes()?.read_bytes()?;
        let esz = core::mem::size_of::<T>();
        Ok(bytes
            .chunks_exact(esz)
            .map(bytemuck::pod_read_unaligned::<T>)
            .collect())
    }

    /// Append an element, growing the data block if needed (which may move it —
    /// the descriptor is rewritten to follow).
    pub fn push(&mut self, value: T) -> io::Result<()> {
        let (off, size) = self.read_desc()?;
        let block = unsafe {
            BStackOwnedSlice::from_raw_range(self.allocator, BStackRange::new(off, size))
        };
        let mut bytevec = unsafe { BStackByteVec::from_raw_block(block) };
        for &b in bytemuck::bytes_of(&value) {
            bytevec.push(b)?;
        }
        let new_range = bytevec.into_raw_block().as_range();
        if new_range.start() != off || new_range.len() != size {
            self.write_desc(new_range.start(), new_range.len())?;
        }
        Ok(())
    }
}
