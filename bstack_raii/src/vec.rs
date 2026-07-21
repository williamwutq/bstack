//! Persistent, growable vectors reached through a **fixed-size** field, with the
//! descriptor stored **inline** in the owning struct.
//!
//! A block field can only store a fixed-size value, but a vector's backing store
//! must grow — and `BStackByteVec` **moves** its block on realloc. The fix is a
//! small [`VecDesc`] (`{ data_off, data_size }`) that names the current data
//! block; the field stores it, and on growth only the descriptor is rewritten:
//!
//! ```text
//! struct field: [ data_off, data_size ] ── points to ──▶ data block (may realloc/move)
//! ```
//!
//! Because a struct **uniquely owns** its vector, the descriptor lives *inline*
//! in the field — there is no separate descriptor block, no extra indirection or
//! allocation. A vector not resident in a field (built by [`BStackVec::from_slice`]
//! or handed out by `bstack_move!`) carries its descriptor **in memory** in the
//! handle, and becomes persistent only when written into a field (which stamps
//! the inline descriptor). A field handle remembers its inline location and
//! rewrites it whenever a push reallocates.
//!
//! Elements are `bytemuck::Pod`; bytes are stored/read unaligned. `u8` (`Vec<u8>`
//! / `String` fields) is the common case. Block-element vectors
//! ([`BStackBlockVec`] / [`BStackStrongVec`] / [`BStackWeakVec`] /
//! [`BStackRefVec`]) store a `u64` offset per element in the same way.
//!
//! > **Growth reallocates**, so use a realloc-safe allocator (see the crate
//! > docs) to avoid corruption on a torn realloc.

use core::marker::PhantomData;
use core::mem::size_of;
use std::io;

use bstack::{BStack, BStackByteVec, BStackOwnedSlice, BStackOwnedSliceAllocator, BStackRange};
use bytemuck::{Pod, Zeroable};

use crate::block::{BStackBlock, BStackShared, BStackWeakable};
use crate::handle::WeakRef;
use crate::owned::BStackOwned;
use crate::reference::BStackRef;
use crate::shared::{BStackRc, BStackWeak};
use crate::teardown::{BStackDrop, dealloc_range};

/// The inline, fixed-size descriptor of a persistent vector: the current offset
/// and byte size of its (reallocating) data block.
///
/// Stored **inline** in the owning struct's field — there is no separate
/// descriptor block, since the struct uniquely owns the vector. `Pod`, so it
/// embeds directly in a generated `XOnDisk`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Pod, Zeroable)]
pub struct VecDesc {
    pub data_off: u64,
    pub data_size: u64,
}

/// Read a [`VecDesc`] from an absolute on-disk offset (its inline field location).
fn read_vecdesc(stack: &BStack, loc: u64) -> io::Result<VecDesc> {
    let mut buf = [0u8; size_of::<VecDesc>()];
    stack.get_into(loc, &mut buf)?;
    Ok(VecDesc {
        data_off: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
        data_size: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
    })
}

/// Write a [`VecDesc`] to an absolute on-disk offset (its inline field location).
fn write_vecdesc(stack: &BStack, loc: u64, desc: VecDesc) -> io::Result<()> {
    let mut buf = [0u8; size_of::<VecDesc>()];
    buf[0..8].copy_from_slice(&desc.data_off.to_le_bytes());
    buf[8..16].copy_from_slice(&desc.data_size.to_le_bytes());
    stack.set(loc, buf)
}

/// A persistent, growable vector of POD elements. Backs un-annotated `Vec<T>`
/// (`T: Pod`) / `String` fields.
///
/// The handle carries the descriptor in memory (`data`), plus the inline field
/// location to persist it to (`writeback`) when field-resident — `None` for a
/// detached vector (from [`from_slice`](Self::from_slice) or `bstack_move!`).
pub struct BStackVec<'a, T, A: BStackOwnedSliceAllocator> {
    /// The current data block range (the live descriptor).
    data: BStackRange,
    /// Where to persist descriptor changes on realloc (the inline field). `None`
    /// for a detached vector (in-memory descriptor only).
    writeback: Option<BStackRange>,
    allocator: &'a A,
    _marker: PhantomData<fn() -> T>,
}

impl<'a, T, A: BStackOwnedSliceAllocator> BStackVec<'a, T, A> {
    /// Reconstruct a **field-resident** handle from its inline descriptor's
    /// absolute on-disk location (what a field accessor passes). Reads the
    /// current descriptor and remembers the location for write-back.
    ///
    /// # Safety
    /// `loc` must be the offset of a live inline [`VecDesc`] written by this type.
    pub unsafe fn from_field(loc: u64, allocator: &'a A) -> io::Result<Self> {
        let desc = read_vecdesc(allocator.stack(), loc)?;
        Ok(Self {
            data: BStackRange::new(desc.data_off, desc.data_size),
            writeback: Some(BStackRange::new(loc, size_of::<VecDesc>() as u64)),
            allocator,
            _marker: PhantomData,
        })
    }

    /// Reconstruct a **detached** handle from a descriptor value (no write-back;
    /// the descriptor lives only in memory). Used by `bstack_move!`.
    pub fn from_desc(desc: VecDesc, allocator: &'a A) -> Self {
        Self {
            data: BStackRange::new(desc.data_off, desc.data_size),
            writeback: None,
            allocator,
            _marker: PhantomData,
        }
    }

    /// The current descriptor value — what a field stores inline.
    pub fn descriptor(&self) -> VecDesc {
        VecDesc {
            data_off: self.data.start(),
            data_size: self.data.len(),
        }
    }

    /// The allocator this vector is bound to.
    pub fn allocator(&self) -> &'a A {
        self.allocator
    }

    /// Persist the current descriptor to the inline field, if field-resident.
    fn persist(&self) -> io::Result<()> {
        if let Some(loc) = self.writeback {
            write_vecdesc(self.allocator.stack(), loc.start(), self.descriptor())?;
        }
        Ok(())
    }

    /// Reconstruct the `BStackByteVec` over the current data block.
    fn bytes(&self) -> io::Result<BStackByteVec<'a, A>> {
        let block = unsafe { BStackOwnedSlice::from_raw_range(self.allocator, self.data) };
        Ok(unsafe { BStackByteVec::from_raw_block(block) })
    }

    /// Free the data block. Consumes the handle. (There is no descriptor block;
    /// a field's inline descriptor is freed with the owning struct's block.)
    pub fn bstack_drop(self) -> io::Result<()> {
        unsafe { dealloc_range(self.allocator, self.data) }
    }
}

impl<'a, T: Pod, A: BStackOwnedSliceAllocator> BStackVec<'a, T, A> {
    /// Create a **detached** vector holding `data`, allocating only the data
    /// block. It becomes persistent when written into a struct field.
    pub fn from_slice(allocator: &'a A, data: &[T]) -> io::Result<Self> {
        let data_range = BStackByteVec::from_slice(bytemuck::cast_slice(data), allocator)?
            .into_raw_block()
            .as_range();
        Ok(Self {
            data: data_range,
            writeback: None,
            allocator,
            _marker: PhantomData,
        })
    }

    /// Create an empty detached vector.
    pub fn new(allocator: &'a A) -> io::Result<Self> {
        Self::from_slice(allocator, &[])
    }

    /// Number of elements.
    pub fn len(&self) -> io::Result<u64> {
        Ok(self.bytes()?.len()? / size_of::<T>() as u64)
    }

    /// Whether the vector is empty.
    pub fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Read all elements into a `Vec<T>` (unaligned reads, so any `T` is fine).
    pub fn to_vec(&self) -> io::Result<Vec<T>> {
        let bytes = self.bytes()?.read_bytes()?;
        let esz = size_of::<T>();
        Ok(bytes
            .chunks_exact(esz)
            .map(bytemuck::pod_read_unaligned::<T>)
            .collect())
    }

    /// Append an element, growing the data block if needed (which may move it —
    /// the inline descriptor is rewritten to follow, if field-resident).
    pub fn push(&mut self, value: T) -> io::Result<()> {
        let mut bytevec = self.bytes()?;
        for &b in bytemuck::bytes_of(&value) {
            bytevec.push(b)?;
        }
        self.data = bytevec.into_raw_block().as_range();
        self.persist()
    }
}

// ---------------------------------------------------------------------------
// Block-element vectors: one `u64` offset per element, stored the same way. The
// field annotation states the elements' ownership; the descriptor + offset array
// are always owned by the enclosing struct.
// ---------------------------------------------------------------------------

/// A persistent, growable vector of **owned block children**.
///
/// Each element is a `u64` offset to a separately-allocated `#[bstack_block]`
/// child this vector *owns*; dropping the vector recursively frees every child
/// (post-order) plus the offset array. Backs `#[bstack_owned] Vec<Thing>` fields.
pub struct BStackBlockVec<'a, T: BStackBlock, A: BStackOwnedSliceAllocator> {
    offsets: BStackVec<'a, u64, A>,
    _marker: PhantomData<fn() -> T>,
}

impl<'a, T: BStackBlock, A: BStackOwnedSliceAllocator> BStackBlockVec<'a, T, A> {
    /// # Safety
    /// `loc` must be a live inline descriptor over an array of data offsets to
    /// live `T` blocks this vector owns.
    pub unsafe fn from_field(loc: u64, allocator: &'a A) -> io::Result<Self> {
        Ok(Self {
            offsets: unsafe { BStackVec::from_field(loc, allocator)? },
            _marker: PhantomData,
        })
    }

    /// Reconstruct a detached handle from a descriptor value. Used by `bstack_move!`.
    pub fn from_desc(desc: VecDesc, allocator: &'a A) -> Self {
        Self {
            offsets: BStackVec::from_desc(desc, allocator),
            _marker: PhantomData,
        }
    }

    /// The current descriptor value — what a field stores inline.
    pub fn descriptor(&self) -> VecDesc {
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

    /// Build a detached vector from a list of owned children (each consumed).
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

    /// Create an empty detached vector.
    pub fn new(allocator: &'a A) -> io::Result<Self> {
        Self::from_handles(allocator, Vec::new())
    }

    /// Append an owned child, transferring its ownership into the vector.
    pub fn push_owned(&mut self, child: BStackOwned<T>) -> io::Result<()> {
        self.offsets.push(child.into_inner().range().start())
    }

    /// Recursively free every owned child (post-order), then the offset array.
    /// Consumes the handle.
    pub fn bstack_drop(self) -> io::Result<()> {
        let allocator = self.offsets.allocator();
        for off in self.offsets.to_vec()? {
            T::from_range(Self::elem_range(off)).bstack_drop(allocator)?;
        }
        self.offsets.bstack_drop()
    }
}

/// A persistent, growable vector of **strong references** to shared block
/// children (`(rc)` / `(rc, weak)` blocks).
///
/// Each element holds one strong reference; dropping the vector releases every
/// one (freeing a child when its count hits zero) and frees the offset array.
/// Backs `#[bstack_strong] Vec<Thing>` fields.
pub struct BStackStrongVec<'a, T: BStackShared, A: BStackOwnedSliceAllocator> {
    offsets: BStackVec<'a, u64, A>,
    _marker: PhantomData<fn() -> T>,
}

impl<'a, T: BStackShared, A: BStackOwnedSliceAllocator> BStackStrongVec<'a, T, A> {
    /// # Safety
    /// `loc` must be a live inline descriptor over an array of data offsets to
    /// live `T` blocks, each accounting for one strong reference this vector owns.
    pub unsafe fn from_field(loc: u64, allocator: &'a A) -> io::Result<Self> {
        Ok(Self {
            offsets: unsafe { BStackVec::from_field(loc, allocator)? },
            _marker: PhantomData,
        })
    }

    /// Reconstruct a detached handle from a descriptor value. Used by `bstack_move!`.
    pub fn from_desc(desc: VecDesc, allocator: &'a A) -> Self {
        Self {
            offsets: BStackVec::from_desc(desc, allocator),
            _marker: PhantomData,
        }
    }

    /// The current descriptor value — what a field stores inline.
    pub fn descriptor(&self) -> VecDesc {
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

    /// Build a detached vector from a list of strong handles (each consumed, its
    /// strong count moved into the vector).
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
    /// free the offset array. Consumes the handle.
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
/// children.
///
/// Each element holds one weak reference (a stored control-block offset).
/// Dropping the vector releases every weak count (freeing a control block when
/// it reaches zero) and frees the offset array. Backs `#[bstack_weak] Vec<Thing>`
/// fields.
pub struct BStackWeakVec<'a, T: BStackWeakable, A: BStackOwnedSliceAllocator> {
    offsets: BStackVec<'a, u64, A>,
    _marker: PhantomData<fn() -> T>,
}

impl<'a, T: BStackWeakable, A: BStackOwnedSliceAllocator> BStackWeakVec<'a, T, A> {
    /// # Safety
    /// `loc` must be a live inline descriptor over an array of control-block
    /// offsets, each accounting for one weak reference this vector owns.
    pub unsafe fn from_field(loc: u64, allocator: &'a A) -> io::Result<Self> {
        Ok(Self {
            offsets: unsafe { BStackVec::from_field(loc, allocator)? },
            _marker: PhantomData,
        })
    }

    /// Reconstruct a detached handle from a descriptor value. Used by `bstack_move!`.
    pub fn from_desc(desc: VecDesc, allocator: &'a A) -> Self {
        Self {
            offsets: BStackVec::from_desc(desc, allocator),
            _marker: PhantomData,
        }
    }

    /// The current descriptor value — what a field stores inline.
    pub fn descriptor(&self) -> VecDesc {
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

    /// Build a detached vector from a list of weak handles (each consumed, its
    /// weak count moved into the vector).
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
    /// then free the offset array. Consumes the handle.
    pub fn bstack_drop(self) -> io::Result<()> {
        let allocator = self.offsets.allocator();
        for off in self.offsets.to_vec()? {
            WeakRef::<T>(Self::ctrl_ref(off)).bstack_drop(allocator)?;
        }
        self.offsets.bstack_drop()
    }
}

/// A persistent, growable vector of **raw references** to block children.
///
/// Elements carry no ownership: dropping the vector frees only the offset array,
/// never the targets. Backs `#[bstack_ref] Vec<Thing>` fields.
pub struct BStackRefVec<'a, T: BStackBlock, A: BStackOwnedSliceAllocator> {
    offsets: BStackVec<'a, u64, A>,
    _marker: PhantomData<fn() -> T>,
}

impl<'a, T: BStackBlock, A: BStackOwnedSliceAllocator> BStackRefVec<'a, T, A> {
    /// # Safety
    /// `loc` must be a live inline descriptor over an array of offsets to `T`
    /// blocks (which this vector does not own).
    pub unsafe fn from_field(loc: u64, allocator: &'a A) -> io::Result<Self> {
        Ok(Self {
            offsets: unsafe { BStackVec::from_field(loc, allocator)? },
            _marker: PhantomData,
        })
    }

    /// Reconstruct a detached handle from a descriptor value. Used by `bstack_move!`.
    pub fn from_desc(desc: VecDesc, allocator: &'a A) -> Self {
        Self {
            offsets: BStackVec::from_desc(desc, allocator),
            _marker: PhantomData,
        }
    }

    /// The current descriptor value — what a field stores inline.
    pub fn descriptor(&self) -> VecDesc {
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

    /// Build a detached vector from a list of raw references.
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

    /// Free only the offset array (elements are not owned). Consumes the handle.
    pub fn bstack_drop(self) -> io::Result<()> {
        self.offsets.bstack_drop()
    }
}
