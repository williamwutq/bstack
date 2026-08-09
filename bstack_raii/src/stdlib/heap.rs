//! [`BStackBinaryHeap<K, V>`]: an owned priority queue (binary min-heap).
//!
//! A priority queue keyed by a `Pod + Ord` priority `K`, carrying a block value
//! `V` the heap owns. [`pop`](BStackBinaryHeap::pop) always returns the entry
//! with the **smallest** key.
//!
//! # Array-backed, pointer-free
//!
//! Despite being a tree, a binary heap needs **no pointers**: it is a single
//! contiguous array with the tree structure implicit in the indices — the
//! children of slot `i` are `2i+1` and `2i+2`. So, like [`crate::BStackDeque`],
//! the entries live in one contiguous block (`[ (K, value_ref) ; cap ]`); only
//! the block pointer, capacity, and length live in the fixed handle, and growth
//! reallocates just that array. Each slot is the inline priority followed by a
//! `u64` reference to the owned value block.
//!
//! # Atomicity — single-writer
//!
//! `push` sifts a new element up and `pop` sifts the last element down; both
//! touch an `O(log n)` path of slots whose shape depends on key comparisons. Each
//! operation reads that path, then commits *all* of its slot moves plus the
//! length change as one crash-atomic [`bstack::BStack::set_batched`] batch — so a
//! crash never leaves the heap half-sifted (the ordering invariant is preserved
//! all-or-nothing). Because the sift is computed from reads taken before the
//! commit, the heap is **single-writer / multi-reader**: concurrent writers need
//! external synchronization; concurrent readers (and `peek`) are always fine.
//! Growth reallocates the array and swaps the descriptor atomically.

use core::marker::PhantomData;
use core::mem::size_of;
use std::io;

use crate::wal::BStackWalAnchor;
use bstack::{BStack, BStackOwnedSliceAllocator, BStackRange};
use bytemuck::{Pod, Zeroable};

use super::util::{alloc_image, read_fields, read_u64};
use crate::block::{BStackBlock, BStackCast};
use crate::clone::{ClonePlan, TryCloneIn};
use crate::layout::{BlockHeader, EightCC, HEADER_SIZE, get_u64};
use crate::owned::BStackOwned;
use crate::teardown::{AutoDrop, BStackDrop, dealloc_range};

/// The on-disk image of a [`BStackBinaryHeap`]: header, array-block pointer
/// (`0` = none), capacity in slots, and element count. Non-generic.
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub struct HeapOnDisk {
    /// The 16-byte block header (size + type tag).
    pub header: BlockHeader,
    /// Offset of the `[(K, value_ref); cap]` array block, or `0` when unallocated.
    pub data: u64,
    /// Number of slots in the array.
    pub cap: u64,
    /// Number of elements currently held.
    pub len: u64,
}

const DATA_OFF: u64 = HEADER_SIZE; // 16
const CAP_OFF: u64 = HEADER_SIZE + 8; // 24
const LEN_OFF: u64 = HEADER_SIZE + 16; // 32
const HEAP_SIZE: u64 = size_of::<HeapOnDisk>() as u64;
const MIN_CAP: u64 = 4;

/// An owned binary min-heap of `(K, V)` entries.
pub struct BStackBinaryHeap<K: Pod + Ord, V: BStackBlock> {
    range: BStackRange,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K: Pod + Ord, V: BStackBlock> BStackBinaryHeap<K, V> {
    fn ksize() -> usize {
        size_of::<K>()
    }
    /// Bytes per slot: inline priority `K` + a `u64` value reference.
    fn stride() -> u64 {
        Self::ksize() as u64 + 8
    }

    fn value_size() -> u64 {
        size_of::<<V as BStackBlock>::OnDisk>() as u64
    }
    fn value_at(off: u64) -> V {
        <V as BStackBlock>::from_range(BStackRange::new(off, Self::value_size()))
    }
    fn read_key(slot: &[u8]) -> K {
        bytemuck::pod_read_unaligned::<K>(&slot[..Self::ksize()])
    }
    fn slot_val(slot: &[u8]) -> u64 {
        get_u64(&slot[Self::ksize()..Self::ksize() + 8])
    }

    /// Read `(data, cap, len)` — the three contiguous handle fields — in one I/O.
    fn read_meta(stack: &BStack, handle: u64) -> io::Result<(u64, u64, u64)> {
        let [data, cap, len] = read_fields::<3>(stack, handle + DATA_OFF)?;
        Ok((data, cap, len))
    }

    /// Read the `stride` raw bytes of slot `i`.
    fn read_slot(stack: &BStack, data: u64, i: u64) -> io::Result<Vec<u8>> {
        let mut buf = vec![0u8; Self::stride() as usize];
        stack.get_into(data + i * Self::stride(), &mut buf)?;
        Ok(buf)
    }

    /// Allocate an empty heap (no array until the first push).
    pub fn new<A: BStackWalAnchor>(allocator: &A) -> io::Result<BStackOwned<Self>> {
        Self::with_image(allocator, 0, 0)
    }

    /// Allocate an empty heap with room for `cap` elements pre-reserved.
    pub fn with_capacity<A: BStackWalAnchor>(
        allocator: &A,
        cap: u64,
    ) -> io::Result<BStackOwned<Self>> {
        if cap == 0 {
            return Self::new(allocator);
        }
        let data = allocator.alloc(cap * Self::stride())?.as_range().start();
        match Self::with_image(allocator, data, cap) {
            Ok(o) => Ok(o),
            Err(e) => {
                // SAFETY: the array was just allocated, linked to nothing.
                let _ = unsafe {
                    dealloc_range(allocator, BStackRange::new(data, cap * Self::stride()))
                };
                Err(e)
            }
        }
    }

    fn with_image<A: BStackWalAnchor>(
        allocator: &A,
        data: u64,
        cap: u64,
    ) -> io::Result<BStackOwned<Self>> {
        let od = HeapOnDisk {
            header: BlockHeader {
                size: HEAP_SIZE,
                tag: Self::eightcc(),
            },
            data,
            cap,
            len: 0,
        };
        let range = alloc_image(allocator, bytemuck::bytes_of(&od))?;
        // SAFETY: a freshly allocated block owned by no other handle.
        Ok(unsafe { BStackOwned::from_raw(Self::from_range(range)) })
    }

    /// Number of elements.
    pub fn len(&self, stack: &BStack) -> io::Result<u64> {
        read_u64(stack, self.range.start() + LEN_OFF)
    }

    /// Whether the heap is empty.
    pub fn is_empty(&self, stack: &BStack) -> io::Result<bool> {
        Ok(self.len(stack)? == 0)
    }

    /// Current array capacity.
    pub fn capacity(&self, stack: &BStack) -> io::Result<u64> {
        read_u64(stack, self.range.start() + CAP_OFF)
    }

    /// A **borrowed** view of the minimum entry (no ownership), or `None` if
    /// empty.
    pub fn peek(&self, stack: &BStack) -> io::Result<Option<(K, V)>> {
        let (data, _cap, len) = Self::read_meta(stack, self.range.start())?;
        if len == 0 {
            return Ok(None);
        }
        let slot = Self::read_slot(stack, data, 0)?;
        Ok(Some((
            Self::read_key(&slot),
            Self::value_at(Self::slot_val(&slot)),
        )))
    }

    /// Insert `key -> value`, taking ownership of the value block.
    ///
    /// Sifts the new element up and commits the whole path atomically.
    pub fn push<A: BStackWalAnchor>(
        &self,
        allocator: &A,
        key: K,
        value: BStackOwned<V>,
    ) -> io::Result<()> {
        let handle = self.range.start();
        let stride = Self::stride();
        let val_ref = value.into_inner().range().start();
        let key_bytes = bytemuck::bytes_of(&key).to_vec();

        loop {
            let (data, cap, len) = Self::read_meta(allocator.stack(), handle)?;
            if data == 0 || len >= cap {
                self.grow(allocator)?;
                continue;
            }
            // The new element's slot bytes: priority then value ref.
            let mut new_slot = Vec::with_capacity(stride as usize);
            new_slot.extend_from_slice(&key_bytes);
            new_slot.extend_from_slice(&val_ref.to_le_bytes());

            // Sift up: walk toward the root, moving greater parents down into the
            // hole, until the new key is `>=` its parent.
            let mut hole = len;
            let mut writes: Vec<(u64, Vec<u8>)> = Vec::new();
            while hole > 0 {
                let parent = (hole - 1) / 2;
                let parent_slot = Self::read_slot(allocator.stack(), data, parent)?;
                if Self::read_key(&parent_slot) > key {
                    writes.push((data + hole * stride, parent_slot));
                    hole = parent;
                } else {
                    break;
                }
            }
            writes.push((data + hole * stride, new_slot));
            writes.push((handle + LEN_OFF, (len + 1).to_le_bytes().to_vec()));
            allocator.stack().set_batched(writes)?;
            return Ok(());
        }
    }

    /// Remove and return the minimum entry (its value block owned), or `None` if
    /// empty. Sifts the last element down and commits the whole path atomically.
    pub fn pop<A: BStackWalAnchor>(
        &self,
        allocator: &A,
    ) -> io::Result<Option<(K, BStackOwned<V>)>> {
        let handle = self.range.start();
        let stride = Self::stride();
        let (data, _cap, len) = Self::read_meta(allocator.stack(), handle)?;
        if len == 0 {
            return Ok(None);
        }
        let min_slot = Self::read_slot(allocator.stack(), data, 0)?;
        let min_key = Self::read_key(&min_slot);
        let min_val = Self::slot_val(&min_slot);

        if len == 1 {
            allocator
                .stack()
                .set(handle + LEN_OFF, 0u64.to_le_bytes())?;
            // SAFETY: the value block's ownership transfers to the caller.
            return Ok(Some((min_key, unsafe {
                BStackOwned::from_raw(Self::value_at(min_val))
            })));
        }

        // Re-place the last element from the root down.
        let last_slot = Self::read_slot(allocator.stack(), data, len - 1)?;
        let last_key = Self::read_key(&last_slot);
        let newlen = len - 1;
        let mut hole = 0u64;
        let mut writes: Vec<(u64, Vec<u8>)> = Vec::new();
        loop {
            let mut child = 2 * hole + 1;
            if child >= newlen {
                break;
            }
            let mut smaller = Self::read_slot(allocator.stack(), data, child)?;
            let mut smaller_key = Self::read_key(&smaller);
            if child + 1 < newlen {
                let right = Self::read_slot(allocator.stack(), data, child + 1)?;
                let right_key = Self::read_key(&right);
                if right_key < smaller_key {
                    smaller = right;
                    smaller_key = right_key;
                    child += 1;
                }
            }
            if smaller_key < last_key {
                writes.push((data + hole * stride, smaller));
                hole = child;
            } else {
                break;
            }
        }
        writes.push((data + hole * stride, last_slot));
        writes.push((handle + LEN_OFF, newlen.to_le_bytes().to_vec()));
        allocator.stack().set_batched(writes)?;

        // SAFETY: the value block's ownership transfers to the caller.
        Ok(Some((min_key, unsafe {
            BStackOwned::from_raw(Self::value_at(min_val))
        })))
    }

    /// Grow the array to at least double its capacity, copying the elements and
    /// atomically swapping the descriptor.
    fn grow<A: BStackWalAnchor>(&self, allocator: &A) -> io::Result<()> {
        let handle = self.range.start();
        let stride = Self::stride();
        let (data, cap, len) = Self::read_meta(allocator.stack(), handle)?;
        let newcap = if cap == 0 { MIN_CAP } else { cap * 2 };
        let newdata = allocator.alloc(newcap * stride)?.as_range().start();

        // Copy the live elements into the new (orphan) array.
        if len > 0 {
            let mut buf = vec![0u8; (len * stride) as usize];
            allocator.stack().get_into(data, &mut buf)?;
            allocator.stack().set(newdata, buf)?;
        }
        // Swap the descriptor's `data`/`cap` (contiguous) in one atomic write.
        let mut meta = [0u8; 16];
        meta[0..8].copy_from_slice(&newdata.to_le_bytes());
        meta[8..16].copy_from_slice(&newcap.to_le_bytes());
        allocator.stack().set(handle + DATA_OFF, meta)?;

        if data != 0 {
            // SAFETY: the descriptor no longer points at the old array.
            let _ = unsafe { dealloc_range(allocator, BStackRange::new(data, cap * stride)) };
        }
        Ok(())
    }

    /// Attach an allocator to make an auto-freeing [`AutoDrop`] guard.
    pub fn auto<A: BStackWalAnchor>(self, allocator: &A) -> AutoDrop<'_, Self, A> {
        // SAFETY: sole ownership was asserted when the heap was created.
        unsafe { AutoDrop::from_raw(self, allocator) }
    }
}

impl<K: Pod + Ord, V: BStackBlock> BStackCast for BStackBinaryHeap<K, V> {
    /// A `"Hep"` prefix perturbed by the key size and value type's tag.
    fn eightcc() -> EightCC {
        const BASE: EightCC = EightCC::new([b'H', b'e', b'p', 0x80, 0x81, 0x82, 0x83, 0x84]);
        BASE.mix(EightCC::new((size_of::<K>() as u64).to_le_bytes()))
            .mix(<V as BStackCast>::eightcc())
    }
}

impl<K: Pod + Ord, V: BStackBlock> BStackBlock for BStackBinaryHeap<K, V> {
    type OnDisk = HeapOnDisk;

    fn from_range(range: BStackRange) -> Self {
        BStackBinaryHeap {
            range,
            _marker: PhantomData,
        }
    }

    fn range(&self) -> BStackRange {
        self.range
    }

    /// Recursively free every value block and the array, **without** freeing the
    /// handle block itself.
    fn __bstack_drop_children<A: BStackWalAnchor>(
        range: BStackRange,
        allocator: &A,
    ) -> io::Result<()> {
        let handle = range.start();
        let (data, cap, len) = Self::read_meta(allocator.stack(), handle)?;
        for i in 0..len {
            let slot = Self::read_slot(allocator.stack(), data, i)?;
            let v = Self::slot_val(&slot);
            if v != 0 {
                // SAFETY: the heap solely owns each value block.
                let owned = unsafe { BStackOwned::from_raw(Self::value_at(v)) };
                owned.bstack_drop(allocator)?;
            }
        }
        if data != 0 {
            // SAFETY: the heap solely owns its array block.
            unsafe { dealloc_range(allocator, BStackRange::new(data, cap * Self::stride()))? };
        }
        Ok(())
    }

    /// Deep-clone: pack the elements into a fresh, exactly-sized array with each
    /// value deep-cloned, and stage the handle, in the parent plan's atomic commit.
    fn __bstack_clone_into<A: BStackWalAnchor>(
        &self,
        allocator: &A,
        plan: &mut ClonePlan,
    ) -> io::Result<BStackRange> {
        let handle = self.range.start();
        let stride = Self::stride();
        let ksize = Self::ksize();
        let (data, _cap, len) = Self::read_meta(allocator.stack(), handle)?;

        let (new_data, new_cap) = if len > 0 {
            let mut image = vec![0u8; (len * stride) as usize];
            allocator.stack().get_into(data, &mut image)?;
            // Deep-clone each value and repoint its ref in the copy (heap order
            // is preserved, so the array stays a valid heap).
            for i in 0..len as usize {
                let vo = i * stride as usize + ksize;
                let vref = get_u64(&image[vo..vo + 8]);
                let cloned = Self::value_at(vref)
                    .__bstack_clone_into(allocator, plan)?
                    .start();
                image[vo..vo + 8].copy_from_slice(&cloned.to_le_bytes());
            }
            let dst = plan.alloc_raw(allocator, len * stride)?;
            plan.write(dst.start(), image);
            (dst.start(), len)
        } else {
            (0, 0)
        };

        let handle_dst = plan.alloc_raw(allocator, HEAP_SIZE)?;
        let od = HeapOnDisk {
            header: BlockHeader {
                size: HEAP_SIZE,
                tag: Self::eightcc(),
            },
            data: new_data,
            cap: new_cap,
            len,
        };
        plan.write(handle_dst.start(), bytemuck::bytes_of(&od).to_vec());
        Ok(handle_dst)
    }
}

impl<K: Pod + Ord, V: BStackBlock> BStackDrop for BStackBinaryHeap<K, V> {
    fn bstack_drop<A: BStackWalAnchor>(self, allocator: &A) -> io::Result<()> {
        Self::__bstack_drop_children(self.range, allocator)?;
        // SAFETY: sole ownership of the handle block was asserted at construction.
        unsafe { dealloc_range(allocator, self.range) }
    }
}

impl<K: Pod + Ord, V: BStackBlock> TryCloneIn for BStackBinaryHeap<K, V> {
    fn try_clone_in<A: BStackWalAnchor>(&self, allocator: &A) -> io::Result<BStackOwned<Self>> {
        let mut plan = ClonePlan::new();
        let dst = match self.__bstack_clone_into(allocator, &mut plan) {
            Ok(range) => range,
            Err(e) => {
                plan.rollback(allocator);
                return Err(e);
            }
        };
        plan.commit(allocator)?;
        // SAFETY: `dst` is a fresh block owned by nobody else.
        Ok(unsafe { BStackOwned::from_raw(Self::from_range(dst)) })
    }
}
