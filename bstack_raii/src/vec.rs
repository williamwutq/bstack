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
use core::mem::size_of;
use std::io;

use bstack::{BStackByteVec, BStackOwnedSlice, BStackOwnedSliceAllocator, BStackRange};
use bytemuck::Pod;

use crate::block::{BStackBlock, BStackShared, BStackWeakable};
use crate::handle::WeakRef;
use crate::owned::BStackOwned;
use crate::reference::BStackRef;
use crate::shared::{BStackRc, BStackWeak};
use crate::teardown::{BStackDrop, dealloc_range};

/// Byte size of a descriptor block: `{ data_off: u64, data_size: u64 }`.
pub(crate) const DESCRIPTOR_SIZE: u64 = 16;

/// The without-allocator drop core of a [`BStackVec`]: just its descriptor
/// range. Its [`BStackDrop`] frees the data block and then the descriptor, so a
/// vector field's teardown (and [`crate::AutoDrop`]) frees it uniformly with the
/// other handle kinds — without carrying the element type or an allocator.
#[derive(Clone, Copy)]
pub struct VecRef(pub BStackRange);

impl VecRef {
    /// Build from a descriptor block offset (its length is the fixed descriptor
    /// size).
    pub fn from_descriptor(desc_off: u64) -> Self {
        VecRef(BStackRange::new(desc_off, DESCRIPTOR_SIZE))
    }
}

impl BStackDrop for VecRef {
    fn bstack_drop<A: BStackOwnedSliceAllocator>(self, allocator: &A) -> io::Result<()> {
        let mut buf = [0u8; DESCRIPTOR_SIZE as usize];
        allocator.stack().get_into(self.0.start(), &mut buf)?;
        let off = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let size = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        unsafe {
            dealloc_range(allocator, BStackRange::new(off, size))?;
            dealloc_range(allocator, self.0)?;
        }
        Ok(())
    }
}

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

    /// The allocator this vector is bound to.
    pub fn allocator(&self) -> &'a A {
        self.allocator
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

    /// Free the data block and the descriptor. Consumes the handle. Delegates to
    /// the without-allocator [`VecRef`] core, the same teardown a vector field
    /// runs during its parent's recursive drop.
    pub fn bstack_drop(self) -> io::Result<()> {
        VecRef(self.desc).bstack_drop(self.allocator)
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

/// A persistent, growable vector of **owned block children**, addressed by a
/// stable descriptor.
///
/// Like [`BStackVec`], but each element is a `u64` offset to a
/// separately-allocated `#[bstack_block]` child that this vector *owns*: the
/// backing data block stores the child offsets, and dropping the vector
/// recursively frees every child (post-order) plus the offset array and the
/// descriptor. Backs `#[bstack_owned] Vec<Thing>` fields.
///
/// > Like [`BStackVec`], growth reallocates the offset array, so use a
/// > realloc-safe allocator.
pub struct BStackBlockVec<'a, T: BStackBlock, A: BStackOwnedSliceAllocator> {
    /// The offset array, stored as a POD `u64` vector behind the same descriptor
    /// indirection. Its identity (the descriptor) is the field's stable pointer.
    offsets: BStackVec<'a, u64, A>,
    _marker: PhantomData<fn() -> T>,
}

impl<'a, T: BStackBlock, A: BStackOwnedSliceAllocator> BStackBlockVec<'a, T, A> {
    /// Reconstruct a handle from its descriptor block offset.
    ///
    /// # Safety
    /// `desc_off` must be the offset of a live descriptor block written by this
    /// type, over an array of offsets to live `T` blocks this vector owns.
    pub unsafe fn from_descriptor(desc_off: u64, allocator: &'a A) -> Self {
        Self {
            offsets: unsafe { BStackVec::from_descriptor(desc_off, allocator) },
            _marker: PhantomData,
        }
    }

    /// The descriptor block's range — the vector's stable on-disk identity.
    pub fn descriptor(&self) -> BStackRange {
        self.offsets.descriptor()
    }

    /// Number of elements.
    pub fn len(&self) -> io::Result<u64> {
        self.offsets.len()
    }

    /// Whether the vector is empty.
    pub fn is_empty(&self) -> io::Result<bool> {
        self.offsets.is_empty()
    }

    /// The child block's range, recovered from a stored offset and `T`'s fixed
    /// on-disk size.
    fn elem_range(off: u64) -> BStackRange {
        BStackRange::new(off, size_of::<T::OnDisk>() as u64)
    }

    /// Read all element handles (non-owning views; the vector still owns them).
    pub fn to_vec(&self) -> io::Result<Vec<T>> {
        Ok(self
            .offsets
            .to_vec()?
            .into_iter()
            .map(|off| T::from_range(Self::elem_range(off)))
            .collect())
    }

    /// The element at index `i`, or `None` if out of range.
    pub fn get(&self, i: u64) -> io::Result<Option<T>> {
        Ok(self
            .offsets
            .to_vec()?
            .get(i as usize)
            .map(|&off| T::from_range(Self::elem_range(off))))
    }

    /// Build from a list of owned children (each consumed, its ownership moved
    /// into the vector).
    pub fn from_handles(allocator: &'a A, children: Vec<BStackOwned<T>>) -> io::Result<Self> {
        let offs: Vec<u64> = children
            .into_iter()
            .map(|c| c.into_inner().range().start())
            .collect();
        Ok(Self {
            offsets: BStackVec::from_slice(allocator, &offs)?,
            _marker: PhantomData,
        })
    }

    /// Create an empty vector.
    pub fn new(allocator: &'a A) -> io::Result<Self> {
        Self::from_handles(allocator, Vec::new())
    }

    /// Append an owned child, transferring its ownership into the vector (the
    /// offset array may realloc/move; the descriptor follows).
    pub fn push_owned(&mut self, child: BStackOwned<T>) -> io::Result<()> {
        let off = child.into_inner().range().start();
        self.offsets.push(off)
    }

    /// Recursively free every owned child (post-order), then the offset array and
    /// the descriptor. Consumes the handle.
    pub fn bstack_drop(self) -> io::Result<()> {
        let allocator = self.offsets.allocator();
        for off in self.offsets.to_vec()? {
            T::from_range(Self::elem_range(off)).bstack_drop(allocator)?;
        }
        self.offsets.bstack_drop()
    }
}

/// A persistent, growable vector of **strong references** to shared block
/// children (`(rc)` / `(rc, weak)` blocks), behind a stable descriptor.
///
/// Each element holds one strong reference (contributes 1 to the child's strong
/// count); dropping the vector releases every one (freeing a child when its count
/// hits zero) and frees the offset array + descriptor. Backs
/// `#[bstack_strong] Vec<Thing>` fields.
pub struct BStackStrongVec<'a, T: BStackShared, A: BStackOwnedSliceAllocator> {
    offsets: BStackVec<'a, u64, A>,
    _marker: PhantomData<fn() -> T>,
}

impl<'a, T: BStackShared, A: BStackOwnedSliceAllocator> BStackStrongVec<'a, T, A> {
    /// # Safety
    /// `desc_off` must be a live descriptor over an array of data offsets to
    /// live `T` blocks, each accounting for one strong reference this vector owns.
    pub unsafe fn from_descriptor(desc_off: u64, allocator: &'a A) -> Self {
        Self {
            offsets: unsafe { BStackVec::from_descriptor(desc_off, allocator) },
            _marker: PhantomData,
        }
    }

    /// The vector's stable on-disk identity.
    pub fn descriptor(&self) -> BStackRange {
        self.offsets.descriptor()
    }

    /// Number of elements.
    pub fn len(&self) -> io::Result<u64> {
        self.offsets.len()
    }

    /// Whether the vector is empty.
    pub fn is_empty(&self) -> io::Result<bool> {
        self.offsets.is_empty()
    }

    fn elem_range(off: u64) -> BStackRange {
        BStackRange::new(off, size_of::<T::OnDisk>() as u64)
    }

    /// Read all element handles (non-owning views — the vector still holds the
    /// strong references).
    pub fn to_vec(&self) -> io::Result<Vec<T>> {
        Ok(self
            .offsets
            .to_vec()?
            .into_iter()
            .map(|off| T::from_range(Self::elem_range(off)))
            .collect())
    }

    /// The element at index `i`, or `None` if out of range.
    pub fn get(&self, i: u64) -> io::Result<Option<T>> {
        Ok(self
            .offsets
            .to_vec()?
            .get(i as usize)
            .map(|&off| T::from_range(Self::elem_range(off))))
    }

    /// Build from a list of strong handles (each consumed, its strong count moved
    /// into the vector).
    pub fn from_handles(allocator: &'a A, elems: Vec<BStackRc<'a, T, A>>) -> io::Result<Self> {
        let offs: Vec<u64> = elems
            .into_iter()
            .map(|rc| {
                let (data, _ctrl) = rc.into_raw();
                data.into_range().start()
            })
            .collect();
        Ok(Self {
            offsets: BStackVec::from_slice(allocator, &offs)?,
            _marker: PhantomData,
        })
    }

    /// Append a strong reference (consumed, its count moved into the vector).
    pub fn push_strong(&mut self, elem: BStackRc<'a, T, A>) -> io::Result<()> {
        let (data, _ctrl) = elem.into_raw();
        self.offsets.push(data.into_range().start())
    }

    /// Release every strong reference (freeing children that reach zero), then
    /// free the offset array and descriptor. Consumes the handle.
    pub fn bstack_drop(self) -> io::Result<()> {
        let allocator = self.offsets.allocator();
        for off in self.offsets.to_vec()? {
            let data = unsafe { BStackRef::<T>::from_range(Self::elem_range(off)) };
            T::drop_strong_ref(data, allocator)?;
        }
        self.offsets.bstack_drop()
    }
}

/// A persistent, growable vector of **weak references** to `(rc, weak)` block
/// children, behind a stable descriptor.
///
/// Each element holds one weak reference (a stored control-block offset).
/// Dropping the vector releases every weak count (freeing a control block when
/// it reaches zero) and frees the offset array + descriptor. Backs
/// `#[bstack_weak] Vec<Thing>` fields.
pub struct BStackWeakVec<'a, T: BStackWeakable, A: BStackOwnedSliceAllocator> {
    offsets: BStackVec<'a, u64, A>,
    _marker: PhantomData<fn() -> T>,
}

impl<'a, T: BStackWeakable, A: BStackOwnedSliceAllocator> BStackWeakVec<'a, T, A> {
    /// # Safety
    /// `desc_off` must be a live descriptor over an array of control-block
    /// offsets, each accounting for one weak reference this vector owns.
    pub unsafe fn from_descriptor(desc_off: u64, allocator: &'a A) -> Self {
        Self {
            offsets: unsafe { BStackVec::from_descriptor(desc_off, allocator) },
            _marker: PhantomData,
        }
    }

    /// The vector's stable on-disk identity.
    pub fn descriptor(&self) -> BStackRange {
        self.offsets.descriptor()
    }

    /// Number of elements.
    pub fn len(&self) -> io::Result<u64> {
        self.offsets.len()
    }

    /// Whether the vector is empty.
    pub fn is_empty(&self) -> io::Result<bool> {
        self.offsets.is_empty()
    }

    fn ctrl_ref(off: u64) -> BStackRef<T::Control> {
        unsafe { BStackRef::from_range(BStackRange::new(off, size_of::<T::Control>() as u64)) }
    }

    /// Attempt to upgrade the element at index `i` to a strong handle. `None` if
    /// out of range, or if the target's strong count has already reached zero.
    pub fn upgrade(&self, i: u64) -> io::Result<Option<BStackRc<'a, T, A>>> {
        let offs = self.offsets.to_vec()?;
        let Some(&off) = offs.get(i as usize) else {
            return Ok(None);
        };
        // Borrow a weak over the element's control ref just long enough to
        // upgrade; consume it via `into_raw` so the vector's own weak count is
        // untouched.
        let allocator = self.offsets.allocator();
        let weak = unsafe { BStackWeak::from_raw(Self::ctrl_ref(off), allocator) };
        let result = weak.upgrade();
        let _ = weak.into_raw();
        result
    }

    /// Build from a list of weak handles (each consumed, its weak count moved
    /// into the vector).
    pub fn from_handles(allocator: &'a A, elems: Vec<BStackWeak<'a, T, A>>) -> io::Result<Self> {
        let offs: Vec<u64> = elems
            .into_iter()
            .map(|w| w.into_raw().into_range().start())
            .collect();
        Ok(Self {
            offsets: BStackVec::from_slice(allocator, &offs)?,
            _marker: PhantomData,
        })
    }

    /// Append a weak reference (consumed, its count moved into the vector).
    pub fn push_weak(&mut self, elem: BStackWeak<'a, T, A>) -> io::Result<()> {
        self.offsets.push(elem.into_raw().into_range().start())
    }

    /// Release every weak reference (freeing control blocks that reach zero),
    /// then free the offset array and descriptor. Consumes the handle.
    pub fn bstack_drop(self) -> io::Result<()> {
        let allocator = self.offsets.allocator();
        for off in self.offsets.to_vec()? {
            WeakRef::<T>(Self::ctrl_ref(off)).bstack_drop(allocator)?;
        }
        self.offsets.bstack_drop()
    }
}

/// A persistent, growable vector of **raw references** to block children, behind
/// a stable descriptor.
///
/// Elements carry no ownership: dropping the vector frees only the offset array
/// and descriptor, never the targets. Backs `#[bstack_ref] Vec<Thing>` fields.
pub struct BStackRefVec<'a, T: BStackBlock, A: BStackOwnedSliceAllocator> {
    offsets: BStackVec<'a, u64, A>,
    _marker: PhantomData<fn() -> T>,
}

impl<'a, T: BStackBlock, A: BStackOwnedSliceAllocator> BStackRefVec<'a, T, A> {
    /// # Safety
    /// `desc_off` must be a live descriptor over an array of offsets to `T`
    /// blocks (which this vector does not own).
    pub unsafe fn from_descriptor(desc_off: u64, allocator: &'a A) -> Self {
        Self {
            offsets: unsafe { BStackVec::from_descriptor(desc_off, allocator) },
            _marker: PhantomData,
        }
    }

    /// The vector's stable on-disk identity.
    pub fn descriptor(&self) -> BStackRange {
        self.offsets.descriptor()
    }

    /// Number of elements.
    pub fn len(&self) -> io::Result<u64> {
        self.offsets.len()
    }

    /// Whether the vector is empty.
    pub fn is_empty(&self) -> io::Result<bool> {
        self.offsets.is_empty()
    }

    fn elem_range(off: u64) -> BStackRange {
        BStackRange::new(off, size_of::<T::OnDisk>() as u64)
    }

    /// Read all element handles (non-owning views).
    pub fn to_vec(&self) -> io::Result<Vec<T>> {
        Ok(self
            .offsets
            .to_vec()?
            .into_iter()
            .map(|off| T::from_range(Self::elem_range(off)))
            .collect())
    }

    /// The element at index `i`, or `None` if out of range.
    pub fn get(&self, i: u64) -> io::Result<Option<T>> {
        Ok(self
            .offsets
            .to_vec()?
            .get(i as usize)
            .map(|&off| T::from_range(Self::elem_range(off))))
    }

    /// Build from a list of raw references.
    pub fn from_handles(allocator: &'a A, elems: Vec<BStackRef<T>>) -> io::Result<Self> {
        let offs: Vec<u64> = elems.into_iter().map(|r| r.into_range().start()).collect();
        Ok(Self {
            offsets: BStackVec::from_slice(allocator, &offs)?,
            _marker: PhantomData,
        })
    }

    /// Append a raw reference.
    pub fn push_ref(&mut self, elem: BStackRef<T>) -> io::Result<()> {
        self.offsets.push(elem.into_range().start())
    }

    /// Free only the offset array and descriptor (elements are not owned).
    /// Consumes the handle.
    pub fn bstack_drop(self) -> io::Result<()> {
        self.offsets.bstack_drop()
    }
}
