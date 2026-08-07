//! [`BStackDeque<T>`]: an owned double-ended queue over a contiguous ring.
//!
//! The on-disk answer to [`std::collections::VecDeque`], and the container most
//! callers reaching for [`crate::BStackLinkedList`] actually want. Its elements'
//! references live in **one contiguous ring block** — `[u64; cap]` slots indexed
//! circularly — so traversing the structure is a single sequential scan rather
//! than a pointer chase per element (each *value* still lives in its own block, so
//! resolving a value seeks once, but finding the next element does not).
//!
//! Push/pop at **both** ends are O(1) amortized. The ring's `head`/`len`/`cap`
//! and its data pointer live in the fixed handle block, so the handle never
//! moves; growth reallocates only the ring.
//!
//! # Single-ref slots, non-generic ring
//!
//! Like [`crate::BStackLinkedList`], each slot is a **single `u64` reference** to
//! an ordinary `T` block the deque owns — not `T` inlined. So the ring's on-disk
//! shape is the same for every `T` (a plain `u64` array), the handle layout
//! [`DequeOnDisk`] is non-generic, and only the tag varies by element type.
//!
//! # Atomicity
//!
//! Every push/pop is atomic per call and external-lock-free on the fast path: the
//! `head`/`len`/`cap`/`data` metadata and the target slot are read *and* written
//! inside one [`bstack::BStack::inplace_gen`] run (see
//! [`atomic_update`](super::util::atomic_update)), so a concurrent writer never
//! observes a torn ring and a crash never corrupts it. **Growth** is also atomic
//! and consistent: the new ring is allocated first (an orphan), then one
//! `inplace_gen` snapshots every live element, copies it into the new ring, and
//! swaps the descriptor — all under bstack's write lock, so it composes correctly
//! with concurrent pushes (a push that finds the ring full simply grows and
//! retries). A crash mid-growth leaks a ring, never tears the deque.

use core::cell::Cell;
use core::marker::PhantomData;
use core::mem::size_of;
use std::io;

use bstack::{BStack, BStackOwnedSliceAllocator, BStackRange};
use bytemuck::{Pod, Zeroable};

use super::util::{alloc_image, atomic_update, read_fields, read_u64};
use crate::block::{BStackBlock, BStackCast};
use crate::clone::{ClonePlan, TryCloneIn};
use crate::layout::{BlockHeader, EightCC, HEADER_SIZE};
use crate::owned::BStackOwned;
use crate::teardown::{AutoDrop, BStackDrop, dealloc_range};

/// The on-disk image of a [`BStackDeque`]: the block header, a pointer to the
/// ring data block (`0` = none), its capacity in slots, and the circular
/// `head`/`len`. `#[repr(C)]` with only `u64` fields after the header, so it is
/// padding-free and **non-generic** — the same layout for every element type.
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub struct DequeOnDisk {
    /// The 16-byte block header (size + type tag).
    pub header: BlockHeader,
    /// Offset of the ring data block (`[u64; cap]`), or `0` when unallocated.
    pub data: u64,
    /// Number of slots in the ring.
    pub cap: u64,
    /// Index (into the ring) of the front element.
    pub head: u64,
    /// Number of elements currently held.
    pub len: u64,
}

// Field offsets within the handle block.
const DATA_OFF: u64 = HEADER_SIZE; // 16
const CAP_OFF: u64 = HEADER_SIZE + 8; // 24
const HEAD_OFF: u64 = HEADER_SIZE + 16; // 32
const LEN_OFF: u64 = HEADER_SIZE + 24; // 40

const DEQUE_SIZE: u64 = size_of::<DequeOnDisk>() as u64;
/// The capacity a freshly grown empty ring starts at.
const MIN_CAP: u64 = 4;

/// An owned double-ended queue of `T` blocks.
///
/// A typed handle (a newtype over a [`BStackRange`]); [`new`](Self::new) returns
/// a bare [`BStackOwned<BStackDeque<T>>`] that frees nothing on scope exit — free
/// it with [`bstack_drop`](BStackDrop::bstack_drop) or wrap it
/// ([`AutoDrop`] / [`crate::BStackCow`]).
///
/// The deque owns its elements' blocks: pushing takes a [`BStackOwned<T>`],
/// popping hands one back, and teardown recursively frees every element and the
/// ring.
pub struct BStackDeque<T: BStackBlock> {
    range: BStackRange,
    _marker: PhantomData<fn() -> T>,
}

impl<T: BStackBlock> BStackDeque<T> {
    fn value_size() -> u64 {
        size_of::<<T as BStackBlock>::OnDisk>() as u64
    }

    /// A `T`-value handle over the block at `off` (fixed-size-block model).
    fn value_at(off: u64) -> T {
        <T as BStackBlock>::from_range(BStackRange::new(off, Self::value_size()))
    }

    /// Read the four `(head, len, cap, data)` metadata fields of the handle at
    /// `handle` in a single I/O (the on-disk order is `data, cap, head, len`).
    fn read_meta(stack: &BStack, handle: u64) -> io::Result<(u64, u64, u64, u64)> {
        let [data, cap, head, len] = read_fields::<4>(stack, handle + DATA_OFF)?;
        Ok((head, len, cap, data))
    }

    /// Allocate an empty deque (no ring is allocated until the first push).
    pub fn new<A: BStackOwnedSliceAllocator>(allocator: &A) -> io::Result<BStackOwned<Self>> {
        Self::with_image(allocator, 0, 0)
    }

    /// Allocate an empty deque with room for `cap` elements pre-reserved (so the
    /// first `cap` pushes never grow). `cap == 0` behaves like [`new`](Self::new).
    pub fn with_capacity<A: BStackOwnedSliceAllocator>(
        allocator: &A,
        cap: u64,
    ) -> io::Result<BStackOwned<Self>> {
        if cap == 0 {
            return Self::new(allocator);
        }
        // Allocate the ring first (an orphan); its slots are empty (len == 0), so
        // their contents are never read before being written.
        let ring = allocator.alloc(cap * 8)?.as_range().start();
        match Self::with_image(allocator, ring, cap) {
            Ok(owned) => Ok(owned),
            Err(e) => {
                // SAFETY: the ring was just allocated and linked to nothing.
                let _ = unsafe { dealloc_range(allocator, BStackRange::new(ring, cap * 8)) };
                Err(e)
            }
        }
    }

    fn with_image<A: BStackOwnedSliceAllocator>(
        allocator: &A,
        data: u64,
        cap: u64,
    ) -> io::Result<BStackOwned<Self>> {
        let od = DequeOnDisk {
            header: BlockHeader {
                size: DEQUE_SIZE,
                tag: Self::eightcc(),
            },
            data,
            cap,
            head: 0,
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

    /// Whether the deque has no elements.
    pub fn is_empty(&self, stack: &BStack) -> io::Result<bool> {
        Ok(self.len(stack)? == 0)
    }

    /// Current ring capacity (elements storable before the next growth).
    pub fn capacity(&self, stack: &BStack) -> io::Result<u64> {
        read_u64(stack, self.range.start() + CAP_OFF)
    }

    /// Append a value to the back, taking ownership of its block. Grows the ring
    /// (once) if it is full, then commits the slot write + length bump atomically.
    pub fn push_back<A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &A,
        value: BStackOwned<T>,
    ) -> io::Result<()> {
        let handle = self.range.start();
        let val_off = value.into_inner().range().start();
        loop {
            let full = Cell::new(false);
            atomic_update(
                allocator,
                &[
                    handle + HEAD_OFF,
                    handle + LEN_OFF,
                    handle + CAP_OFF,
                    handle + DATA_OFF,
                ],
                |_v1| Vec::new(),
                |v1, _v2| {
                    let (head, len, cap, data) = (v1[0], v1[1], v1[2], v1[3]);
                    if len < cap {
                        let slot = data + ((head + len) % cap) * 8;
                        vec![
                            (slot, val_off.to_le_bytes().to_vec()),
                            (handle + LEN_OFF, (len + 1).to_le_bytes().to_vec()),
                        ]
                    } else {
                        full.set(true);
                        Vec::new()
                    }
                },
            )?;
            if !full.get() {
                return Ok(());
            }
            self.grow(allocator)?;
        }
    }

    /// Prepend a value to the front, taking ownership of its block.
    pub fn push_front<A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &A,
        value: BStackOwned<T>,
    ) -> io::Result<()> {
        let handle = self.range.start();
        let val_off = value.into_inner().range().start();
        loop {
            let full = Cell::new(false);
            atomic_update(
                allocator,
                &[
                    handle + HEAD_OFF,
                    handle + LEN_OFF,
                    handle + CAP_OFF,
                    handle + DATA_OFF,
                ],
                |_v1| Vec::new(),
                |v1, _v2| {
                    let (head, len, cap, data) = (v1[0], v1[1], v1[2], v1[3]);
                    if len < cap {
                        let idx = (head + cap - 1) % cap;
                        let slot = data + idx * 8;
                        vec![
                            (slot, val_off.to_le_bytes().to_vec()),
                            (handle + HEAD_OFF, idx.to_le_bytes().to_vec()),
                            (handle + LEN_OFF, (len + 1).to_le_bytes().to_vec()),
                        ]
                    } else {
                        full.set(true);
                        Vec::new()
                    }
                },
            )?;
            if !full.get() {
                return Ok(());
            }
            self.grow(allocator)?;
        }
    }

    /// Remove and return the last element (as an owned value block), or `None` if
    /// empty. Atomic: the slot is read and the length decremented in one commit;
    /// the value block's ownership transfers to the caller (its ring slot is left
    /// stale and reused by a later push).
    pub fn pop_back<A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &A,
    ) -> io::Result<Option<BStackOwned<T>>> {
        let handle = self.range.start();
        let got = Cell::new(false);
        let val = Cell::new(0u64);
        atomic_update(
            allocator,
            &[
                handle + HEAD_OFF,
                handle + LEN_OFF,
                handle + CAP_OFF,
                handle + DATA_OFF,
            ],
            |v1| {
                let (head, len, cap, data) = (v1[0], v1[1], v1[2], v1[3]);
                if len == 0 {
                    Vec::new()
                } else {
                    vec![data + ((head + len - 1) % cap) * 8]
                }
            },
            |v1, v2| {
                let len = v1[1];
                if len == 0 {
                    return Vec::new();
                }
                got.set(true);
                val.set(v2[0]);
                vec![(handle + LEN_OFF, (len - 1).to_le_bytes().to_vec())]
            },
        )?;
        if !got.get() {
            return Ok(None);
        }
        // SAFETY: the value block's ownership transfers to the caller.
        Ok(Some(unsafe {
            BStackOwned::from_raw(Self::value_at(val.get()))
        }))
    }

    /// Remove and return the first element (as an owned value block), or `None`
    /// if empty.
    pub fn pop_front<A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &A,
    ) -> io::Result<Option<BStackOwned<T>>> {
        let handle = self.range.start();
        let got = Cell::new(false);
        let val = Cell::new(0u64);
        atomic_update(
            allocator,
            &[
                handle + HEAD_OFF,
                handle + LEN_OFF,
                handle + CAP_OFF,
                handle + DATA_OFF,
            ],
            |v1| {
                let (head, len, cap, data) = (v1[0], v1[1], v1[2], v1[3]);
                if len == 0 {
                    Vec::new()
                } else {
                    vec![data + (head % cap) * 8]
                }
            },
            |v1, v2| {
                let (head, len, cap) = (v1[0], v1[1], v1[2]);
                if len == 0 {
                    return Vec::new();
                }
                got.set(true);
                val.set(v2[0]);
                vec![
                    (handle + HEAD_OFF, ((head + 1) % cap).to_le_bytes().to_vec()),
                    (handle + LEN_OFF, (len - 1).to_le_bytes().to_vec()),
                ]
            },
        )?;
        if !got.get() {
            return Ok(None);
        }
        // SAFETY: the value block's ownership transfers to the caller.
        Ok(Some(unsafe {
            BStackOwned::from_raw(Self::value_at(val.get()))
        }))
    }

    /// Grow the ring to at least double its capacity, atomically snapshotting and
    /// re-basing the live elements. A no-op (beyond a wasted allocation, freed
    /// again) if another thread already made room.
    fn grow<A: BStackOwnedSliceAllocator>(&self, allocator: &A) -> io::Result<()> {
        let handle = self.range.start();
        let cap0 = read_u64(allocator.stack(), handle + CAP_OFF)?;
        let newcap = if cap0 == 0 { MIN_CAP } else { cap0 * 2 };
        // Allocate the new ring up front (an orphan until the commit swaps to it).
        let newring = allocator.alloc(newcap * 8)?.as_range().start();

        let grown = Cell::new(false);
        let old_ring = Cell::new(0u64);
        let old_cap = Cell::new(0u64);

        // Abort if, at commit time, the ring already has room or is already at
        // least this big (another thread grew it) — then our `newring` is wasted.
        let abort = |head: u64, len: u64, cap: u64| (cap != 0 && len < cap) || newcap <= cap;

        atomic_update(
            allocator,
            &[
                handle + HEAD_OFF,
                handle + LEN_OFF,
                handle + CAP_OFF,
                handle + DATA_OFF,
            ],
            |v1| {
                let (head, len, cap, data) = (v1[0], v1[1], v1[2], v1[3]);
                if abort(head, len, cap) {
                    Vec::new()
                } else {
                    // The live elements, in logical order.
                    (0..len).map(|i| data + ((head + i) % cap) * 8).collect()
                }
            },
            |v1, v2| {
                let (head, len, cap, data) = (v1[0], v1[1], v1[2], v1[3]);
                if abort(head, len, cap) {
                    return Vec::new();
                }
                grown.set(true);
                old_ring.set(data);
                old_cap.set(cap);
                let mut w = Vec::with_capacity(v2.len() + 3);
                // Copy every live element to the front of the new ring.
                for (i, &r) in v2.iter().enumerate() {
                    w.push((newring + (i as u64) * 8, r.to_le_bytes().to_vec()));
                }
                // Swap the descriptor to the new ring, re-based at head 0.
                w.push((handle + DATA_OFF, newring.to_le_bytes().to_vec()));
                w.push((handle + CAP_OFF, newcap.to_le_bytes().to_vec()));
                w.push((handle + HEAD_OFF, 0u64.to_le_bytes().to_vec()));
                w
            },
        )?;

        if grown.get() {
            // The old ring is now unreferenced; free it (leak-only on crash).
            if old_cap.get() > 0 {
                // SAFETY: the descriptor no longer points at the old ring.
                let _ = unsafe {
                    dealloc_range(
                        allocator,
                        BStackRange::new(old_ring.get(), old_cap.get() * 8),
                    )
                };
            }
        } else {
            // Growth was unnecessary; reclaim the unused new ring.
            // SAFETY: `newring` was never linked into the descriptor.
            let _ = unsafe { dealloc_range(allocator, BStackRange::new(newring, newcap * 8)) };
        }
        Ok(())
    }

    /// A **borrowed** handle to the front value (no ownership), or `None` if empty.
    pub fn front(&self, stack: &BStack) -> io::Result<Option<T>> {
        let (head, len, cap, data) = Self::read_meta(stack, self.range.start())?;
        if len == 0 {
            return Ok(None);
        }
        Ok(Some(Self::value_at(read_u64(
            stack,
            data + (head % cap) * 8,
        )?)))
    }

    /// A **borrowed** handle to the back value (no ownership), or `None` if empty.
    pub fn back(&self, stack: &BStack) -> io::Result<Option<T>> {
        let (head, len, cap, data) = Self::read_meta(stack, self.range.start())?;
        if len == 0 {
            return Ok(None);
        }
        Ok(Some(Self::value_at(read_u64(
            stack,
            data + ((head + len - 1) % cap) * 8,
        )?)))
    }

    /// Collect **borrowed** handles to every value, front to back. The handles
    /// alias the deque's blocks — do not free them; they stay valid only while the
    /// deque does.
    pub fn to_vec(&self, stack: &BStack) -> io::Result<Vec<T>> {
        let (head, len, cap, data) = Self::read_meta(stack, self.range.start())?;
        let mut out = Vec::with_capacity(len as usize);
        for i in 0..len {
            out.push(Self::value_at(read_u64(
                stack,
                data + ((head + i) % cap) * 8,
            )?));
        }
        Ok(out)
    }

    /// Attach an allocator to make an auto-freeing [`AutoDrop`] guard.
    pub fn auto<A: BStackOwnedSliceAllocator>(self, allocator: &A) -> AutoDrop<'_, Self, A> {
        // SAFETY: sole ownership was asserted when the deque was created.
        unsafe { AutoDrop::from_raw(self, allocator) }
    }
}

impl<T: BStackBlock> BStackCast for BStackDeque<T> {
    /// A `"Deq"` prefix over hash bytes perturbed by `T`'s tag, so deques of
    /// different element types never share a discriminant despite the identical
    /// on-disk layout.
    fn eightcc() -> EightCC {
        const BASE: EightCC = EightCC::new([b'D', b'e', b'q', 0x80, 0x81, 0x82, 0x83, 0x84]);
        BASE.mix(<T as BStackCast>::eightcc())
    }
}

impl<T: BStackBlock> BStackBlock for BStackDeque<T> {
    type OnDisk = DequeOnDisk;

    fn from_range(range: BStackRange) -> Self {
        BStackDeque {
            range,
            _marker: PhantomData,
        }
    }

    fn range(&self) -> BStackRange {
        self.range
    }

    /// Recursively free every element block and the ring, **without** freeing the
    /// handle block itself.
    fn __bstack_drop_children<A: BStackOwnedSliceAllocator>(
        range: BStackRange,
        allocator: &A,
    ) -> io::Result<()> {
        let (head, len, cap, data) = Self::read_meta(allocator.stack(), range.start())?;
        for i in 0..len {
            let r = read_u64(allocator.stack(), data + ((head + i) % cap) * 8)?;
            if r != 0 {
                // SAFETY: the deque solely owns each element block.
                let owned = unsafe { BStackOwned::from_raw(Self::value_at(r)) };
                owned.bstack_drop(allocator)?;
            }
        }
        if data != 0 {
            // SAFETY: the deque solely owns its ring block.
            unsafe { dealloc_range(allocator, BStackRange::new(data, cap * 8))? };
        }
        Ok(())
    }

    /// Deep-clone the deque into `plan`: every element is deep-cloned (via `T`'s
    /// own clone hook) and packed into a fresh, compacted ring (`head = 0`,
    /// `cap = len`); the handle block is staged — all in the parent plan's single
    /// atomic commit.
    fn __bstack_clone_into<A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &A,
        plan: &mut ClonePlan,
    ) -> io::Result<BStackRange> {
        let (head, len, cap, data) = Self::read_meta(allocator.stack(), self.range.start())?;

        // Deep-clone each element (in logical order) into the plan.
        let mut dsts = Vec::with_capacity(len as usize);
        for i in 0..len {
            let r = read_u64(allocator.stack(), data + ((head + i) % cap) * 8)?;
            let dst = if r != 0 {
                Self::value_at(r)
                    .__bstack_clone_into(allocator, plan)?
                    .start()
            } else {
                0
            };
            dsts.push(dst);
        }

        // Pack the cloned refs into a fresh, exactly-sized ring.
        let (new_data, new_cap) = if len > 0 {
            let ring = plan.alloc_raw(allocator, len * 8)?;
            let mut bytes = Vec::with_capacity(dsts.len() * 8);
            for d in &dsts {
                bytes.extend_from_slice(&d.to_le_bytes());
            }
            plan.write(ring.start(), bytes);
            (ring.start(), len)
        } else {
            (0, 0)
        };

        let handle_dst = plan.alloc_raw(allocator, DEQUE_SIZE)?;
        let od = DequeOnDisk {
            header: BlockHeader {
                size: DEQUE_SIZE,
                tag: Self::eightcc(),
            },
            data: new_data,
            cap: new_cap,
            head: 0,
            len,
        };
        plan.write(handle_dst.start(), bytemuck::bytes_of(&od).to_vec());
        Ok(handle_dst)
    }
}

impl<T: BStackBlock> BStackDrop for BStackDeque<T> {
    fn bstack_drop<A: BStackOwnedSliceAllocator>(self, allocator: &A) -> io::Result<()> {
        Self::__bstack_drop_children(self.range, allocator)?;
        // SAFETY: sole ownership of the handle block was asserted at construction.
        unsafe { dealloc_range(allocator, self.range) }
    }
}

impl<T: BStackBlock> TryCloneIn for BStackDeque<T> {
    fn try_clone_in<A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &A,
    ) -> io::Result<BStackOwned<Self>> {
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
