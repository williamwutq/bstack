//! [`BStackLinkedList<T>`]: an owned, doubly-linked list of block values.
//!
//! # Prefer a vector unless you actually need a list
//!
//! On disk a linked list is usually the *wrong* choice. Every traversal step
//! chases a pointer to a physically unrelated block — a random on-disk seek per
//! element — whereas a [`crate::BStackBlockVec`] keeps its element offsets in one
//! contiguous block and its values need no per-step indirection. For iteration,
//! indexing, and bulk reads a vector is faster and denser; reach for a linked
//! list only when you genuinely need O(1) splice / push / pop at *both* ends
//! without disturbing the other elements' identities (their on-disk offsets stay
//! put across insert/remove, which a vector cannot promise).
//!
//! # Non-intrusive, single-ref nodes
//!
//! This list is deliberately **not** intrusive: the links do not live inside `T`.
//! An intrusive list would have to weave `prev`/`next` into each value type,
//! which for a generic `T` means the codegen must understand `T`'s layout and
//! inject fields — a lot of per-`T` machinery for no real payoff here. Instead
//! every node is its own small block holding just `{ prev, next, value }`, where
//! `value` is a **single `u64` reference** to an ordinary, unmodified `T` block.
//!
//! The payoff of the single ref is that a node's on-disk layout is
//! [`NodeOnDisk`] — three `u64`s after the header — **identical for every `T`**.
//! There is no generic on-disk struct, the tag is the only thing that varies by
//! `T`, and the read/write/teardown/clone code is one fixed shape rather than a
//! monomorphized family. The list *owns* its nodes and, through each node's
//! single ref, the value blocks: teardown frees both, and a deep clone
//! reproduces the whole chain with freshly deep-cloned values.

use core::marker::PhantomData;
use core::mem::size_of;
use std::io;

use bstack::{BStack, BStackOwnedSliceAllocator, BStackRange};
use bytemuck::{Pod, Zeroable};

use crate::block::{BStackBlock, BStackCast};
use crate::clone::{ClonePlan, TryCloneIn};
use crate::layout::{BlockHeader, EightCC, HEADER_SIZE};
use crate::owned::BStackOwned;
use crate::teardown::{BStackDrop, dealloc_range};

/// The on-disk image of a [`BStackLinkedList`]: the block header followed by the
/// `head`/`tail` node offsets (`0` = empty) and the element count. `#[repr(C)]`
/// with only `u64` fields after a 16-byte header, so it is naturally padding-free
/// and **non-generic** — the same layout for every element type.
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub struct ListOnDisk {
    /// The 16-byte block header (size + type tag).
    pub header: BlockHeader,
    /// Offset of the first node, or `0` when the list is empty.
    pub head: u64,
    /// Offset of the last node, or `0` when the list is empty.
    pub tail: u64,
    /// Number of elements.
    pub len: u64,
}

/// The on-disk image of one list node: the block header followed by the
/// `prev`/`next` node offsets (`0` = none) and a **single** `u64` reference to
/// the value block. Non-generic: the same layout for every element type.
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub struct NodeOnDisk {
    /// The 16-byte block header (size + type tag).
    pub header: BlockHeader,
    /// Offset of the previous node, or `0` at the head.
    pub prev: u64,
    /// Offset of the next node, or `0` at the tail.
    pub next: u64,
    /// The single reference to this node's value block.
    pub value: u64,
}

// Field offsets within a list block.
const HEAD_OFF: u64 = HEADER_SIZE; // 16
const TAIL_OFF: u64 = HEADER_SIZE + 8; // 24
const LEN_OFF: u64 = HEADER_SIZE + 16; // 32
// Field offsets within a node block (same shape, different meaning).
const NPREV_OFF: u64 = HEADER_SIZE; // 16
const NNEXT_OFF: u64 = HEADER_SIZE + 8; // 24
const NVAL_OFF: u64 = HEADER_SIZE + 16; // 32

const LIST_SIZE: u64 = size_of::<ListOnDisk>() as u64;
const NODE_SIZE: u64 = size_of::<NodeOnDisk>() as u64;

/// Read a little-endian `u64` at absolute offset `off`.
fn get_u64(stack: &BStack, off: u64) -> io::Result<u64> {
    let mut b = [0u8; 8];
    stack.get_into(off, &mut b)?;
    Ok(u64::from_le_bytes(b))
}

/// Write a little-endian `u64` at absolute offset `off`.
fn set_u64<A: BStackOwnedSliceAllocator>(allocator: &A, off: u64, val: u64) -> io::Result<()> {
    allocator.stack().set(off, val.to_le_bytes())
}

/// Allocate a block and write `bytes` as its whole image (one write; released
/// without leaking on write failure).
fn alloc_image<A: BStackOwnedSliceAllocator>(
    allocator: &A,
    bytes: &[u8],
) -> io::Result<BStackRange> {
    let mut slice = allocator.alloc(bytes.len() as u64)?;
    if let Err(e) = slice.write_range(0, bytes) {
        let _ = allocator.dealloc(slice);
        return Err(e);
    }
    Ok(slice.as_range())
}

/// An owned, doubly-linked list of `T` blocks.
///
/// A typed handle (a newtype over a [`BStackRange`], like every block handle),
/// carrying no allocator. [`new`](Self::new) returns a bare
/// [`BStackOwned<BStackLinkedList<T>>`] that frees nothing on scope exit; free it
/// with [`bstack_drop`](BStackDrop::bstack_drop) or wrap it
/// ([`crate::AutoDrop`] / [`crate::BStackCow`]).
///
/// The list owns its nodes and their value blocks: pushing takes a
/// [`BStackOwned<T>`] (transferring ownership into a node), popping hands one
/// back, and teardown recursively frees every value and node.
pub struct BStackLinkedList<T: BStackBlock> {
    range: BStackRange,
    _marker: PhantomData<fn() -> T>,
}

impl<T: BStackBlock> BStackLinkedList<T> {
    /// The fixed on-disk size of one `T` value block.
    fn value_size() -> u64 {
        size_of::<<T as BStackBlock>::OnDisk>() as u64
    }

    /// The tag stamped on this list's internal node blocks — a `"LNd"` prefix
    /// perturbed by `T`'s tag, so a node is never mistaken for another type.
    fn node_tag() -> EightCC {
        const BASE: EightCC = EightCC::new([b'L', b'N', b'd', 0x80, 0x81, 0x82, 0x83, 0x84]);
        BASE.mix(<T as BStackCast>::eightcc())
    }

    /// A `T`-value handle over the block at `off` (fixed-size-block model).
    fn value_at(off: u64) -> T {
        <T as BStackBlock>::from_range(BStackRange::new(off, Self::value_size()))
    }

    /// Build a node image with the given links and value ref.
    fn node_image(prev: u64, next: u64, value: u64) -> NodeOnDisk {
        NodeOnDisk {
            header: BlockHeader {
                size: NODE_SIZE,
                tag: Self::node_tag(),
            },
            prev,
            next,
            value,
        }
    }

    /// Allocate an empty list.
    pub fn new<A: BStackOwnedSliceAllocator>(allocator: &A) -> io::Result<BStackOwned<Self>> {
        let od = ListOnDisk {
            header: BlockHeader {
                size: LIST_SIZE,
                tag: Self::eightcc(),
            },
            head: 0,
            tail: 0,
            len: 0,
        };
        let range = alloc_image(allocator, bytemuck::bytes_of(&od))?;
        // SAFETY: a freshly allocated block owned by no other handle.
        Ok(unsafe { BStackOwned::from_raw(Self::from_range(range)) })
    }

    /// Number of elements.
    pub fn len(&self, stack: &BStack) -> io::Result<u64> {
        get_u64(stack, self.range.start() + LEN_OFF)
    }

    /// Whether the list has no elements.
    pub fn is_empty(&self, stack: &BStack) -> io::Result<bool> {
        Ok(self.len(stack)? == 0)
    }

    /// Append a value to the back, taking ownership of its block.
    pub fn push_back<A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &A,
        value: BStackOwned<T>,
    ) -> io::Result<()> {
        let list = self.range.start();
        let val_off = value.into_inner().range().start();
        let old_tail = get_u64(allocator.stack(), list + TAIL_OFF)?;

        let node = alloc_image(
            allocator,
            bytemuck::bytes_of(&Self::node_image(old_tail, 0, val_off)),
        )?
        .start();

        if old_tail != 0 {
            set_u64(allocator, old_tail + NNEXT_OFF, node)?;
        } else {
            set_u64(allocator, list + HEAD_OFF, node)?;
        }
        set_u64(allocator, list + TAIL_OFF, node)?;
        let len = get_u64(allocator.stack(), list + LEN_OFF)?;
        set_u64(allocator, list + LEN_OFF, len + 1)
    }

    /// Prepend a value to the front, taking ownership of its block.
    pub fn push_front<A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &A,
        value: BStackOwned<T>,
    ) -> io::Result<()> {
        let list = self.range.start();
        let val_off = value.into_inner().range().start();
        let old_head = get_u64(allocator.stack(), list + HEAD_OFF)?;

        let node = alloc_image(
            allocator,
            bytemuck::bytes_of(&Self::node_image(0, old_head, val_off)),
        )?
        .start();

        if old_head != 0 {
            set_u64(allocator, old_head + NPREV_OFF, node)?;
        } else {
            set_u64(allocator, list + TAIL_OFF, node)?;
        }
        set_u64(allocator, list + HEAD_OFF, node)?;
        let len = get_u64(allocator.stack(), list + LEN_OFF)?;
        set_u64(allocator, list + LEN_OFF, len + 1)
    }

    /// Remove and return the last element (as an owned value block), or `None`
    /// if the list is empty. The node shell is freed; the value block is handed
    /// back to the caller.
    pub fn pop_back<A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &A,
    ) -> io::Result<Option<BStackOwned<T>>> {
        let list = self.range.start();
        let tail = get_u64(allocator.stack(), list + TAIL_OFF)?;
        if tail == 0 {
            return Ok(None);
        }
        let prev = get_u64(allocator.stack(), tail + NPREV_OFF)?;
        let val = get_u64(allocator.stack(), tail + NVAL_OFF)?;

        if prev != 0 {
            set_u64(allocator, prev + NNEXT_OFF, 0)?;
            set_u64(allocator, list + TAIL_OFF, prev)?;
        } else {
            set_u64(allocator, list + HEAD_OFF, 0)?;
            set_u64(allocator, list + TAIL_OFF, 0)?;
        }
        let len = get_u64(allocator.stack(), list + LEN_OFF)?;
        set_u64(allocator, list + LEN_OFF, len - 1)?;

        // SAFETY: the node was our block; nothing else references it now.
        unsafe { dealloc_range(allocator, BStackRange::new(tail, NODE_SIZE))? };
        // SAFETY: the value block's ownership transfers to the caller.
        Ok(Some(unsafe { BStackOwned::from_raw(Self::value_at(val)) }))
    }

    /// Remove and return the first element (as an owned value block), or `None`
    /// if the list is empty.
    pub fn pop_front<A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &A,
    ) -> io::Result<Option<BStackOwned<T>>> {
        let list = self.range.start();
        let head = get_u64(allocator.stack(), list + HEAD_OFF)?;
        if head == 0 {
            return Ok(None);
        }
        let next = get_u64(allocator.stack(), head + NNEXT_OFF)?;
        let val = get_u64(allocator.stack(), head + NVAL_OFF)?;

        if next != 0 {
            set_u64(allocator, next + NPREV_OFF, 0)?;
            set_u64(allocator, list + HEAD_OFF, next)?;
        } else {
            set_u64(allocator, list + HEAD_OFF, 0)?;
            set_u64(allocator, list + TAIL_OFF, 0)?;
        }
        let len = get_u64(allocator.stack(), list + LEN_OFF)?;
        set_u64(allocator, list + LEN_OFF, len - 1)?;

        // SAFETY: the node was our block; nothing else references it now.
        unsafe { dealloc_range(allocator, BStackRange::new(head, NODE_SIZE))? };
        // SAFETY: the value block's ownership transfers to the caller.
        Ok(Some(unsafe { BStackOwned::from_raw(Self::value_at(val)) }))
    }

    /// A **borrowed** handle to the first value (no ownership; frees nothing), or
    /// `None` if empty.
    pub fn front(&self, stack: &BStack) -> io::Result<Option<T>> {
        let head = get_u64(stack, self.range.start() + HEAD_OFF)?;
        if head == 0 {
            return Ok(None);
        }
        Ok(Some(Self::value_at(get_u64(stack, head + NVAL_OFF)?)))
    }

    /// A **borrowed** handle to the last value (no ownership; frees nothing), or
    /// `None` if empty.
    pub fn back(&self, stack: &BStack) -> io::Result<Option<T>> {
        let tail = get_u64(stack, self.range.start() + TAIL_OFF)?;
        if tail == 0 {
            return Ok(None);
        }
        Ok(Some(Self::value_at(get_u64(stack, tail + NVAL_OFF)?)))
    }

    /// Collect **borrowed** handles to every value, front to back. The handles
    /// alias the list's blocks — do not free them; they stay valid only while the
    /// list does.
    pub fn to_vec(&self, stack: &BStack) -> io::Result<Vec<T>> {
        let mut out = Vec::new();
        let mut cur = get_u64(stack, self.range.start() + HEAD_OFF)?;
        while cur != 0 {
            out.push(Self::value_at(get_u64(stack, cur + NVAL_OFF)?));
            cur = get_u64(stack, cur + NNEXT_OFF)?;
        }
        Ok(out)
    }

    /// Attach an allocator to make an auto-freeing [`crate::AutoDrop`] guard.
    pub fn auto<A: BStackOwnedSliceAllocator>(
        self,
        allocator: &A,
    ) -> crate::teardown::AutoDrop<'_, Self, A> {
        // SAFETY: sole ownership was asserted when the list was created.
        unsafe { crate::teardown::AutoDrop::from_raw(self, allocator) }
    }
}

impl<T: BStackBlock> BStackCast for BStackLinkedList<T> {
    /// A `"List"` prefix over hash bytes perturbed by `T`'s tag, so lists of
    /// different element types never share a discriminant even though their
    /// on-disk layout is identical.
    fn eightcc() -> EightCC {
        const BASE: EightCC = EightCC::new([b'L', b'i', b's', b't', 0x80, 0x81, 0x82, 0x83]);
        BASE.mix(<T as BStackCast>::eightcc())
    }
}

impl<T: BStackBlock> BStackBlock for BStackLinkedList<T> {
    type OnDisk = ListOnDisk;

    fn from_range(range: BStackRange) -> Self {
        BStackLinkedList {
            range,
            _marker: PhantomData,
        }
    }

    fn range(&self) -> BStackRange {
        self.range
    }

    /// Recursively free every value block and node, **without** freeing the list
    /// block itself (its embedding parent, or [`bstack_drop`](BStackDrop), does
    /// that).
    fn __bstack_drop_children<A: BStackOwnedSliceAllocator>(
        range: BStackRange,
        allocator: &A,
    ) -> io::Result<()> {
        let mut cur = get_u64(allocator.stack(), range.start() + HEAD_OFF)?;
        while cur != 0 {
            let next = get_u64(allocator.stack(), cur + NNEXT_OFF)?;
            let val = get_u64(allocator.stack(), cur + NVAL_OFF)?;
            if val != 0 {
                // Recursively free the value block (its own children, then it).
                // SAFETY: the list solely owns each value block.
                let owned = unsafe { BStackOwned::from_raw(Self::value_at(val)) };
                owned.bstack_drop(allocator)?;
            }
            // SAFETY: the list solely owns each node block.
            unsafe { dealloc_range(allocator, BStackRange::new(cur, NODE_SIZE))? };
            cur = next;
        }
        Ok(())
    }

    /// Deep-clone the whole chain into `plan`: every value is deep-cloned (via
    /// `T`'s own clone hook), fresh nodes are allocated and wired, and the list
    /// block is staged — all as part of the parent plan's single atomic commit.
    fn __bstack_clone_into<A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &A,
        plan: &mut ClonePlan,
    ) -> io::Result<BStackRange> {
        let src = self.range.start();

        // 1. Gather the source value offsets in order.
        let mut vals = Vec::new();
        let mut cur = get_u64(allocator.stack(), src + HEAD_OFF)?;
        while cur != 0 {
            vals.push(get_u64(allocator.stack(), cur + NVAL_OFF)?);
            cur = get_u64(allocator.stack(), cur + NNEXT_OFF)?;
        }
        let n = vals.len();

        // 2. Deep-clone each value into the plan.
        let mut val_dsts = Vec::with_capacity(n);
        for &v in &vals {
            let dst = if v != 0 {
                Self::value_at(v).__bstack_clone_into(allocator, plan)?.start()
            } else {
                0
            };
            val_dsts.push(dst);
        }

        // 3. Reserve the node blocks up front so their offsets are known for wiring.
        let mut node_dsts = Vec::with_capacity(n);
        for _ in 0..n {
            node_dsts.push(plan.alloc_raw(allocator, NODE_SIZE)?.start());
        }

        // 4. Stage each node image with links resolved.
        for i in 0..n {
            let prev = if i > 0 { node_dsts[i - 1] } else { 0 };
            let next = if i + 1 < n { node_dsts[i + 1] } else { 0 };
            let od = Self::node_image(prev, next, val_dsts[i]);
            plan.write(node_dsts[i], bytemuck::bytes_of(&od).to_vec());
        }

        // 5. Stage the list block.
        let list_dst = plan.alloc_raw(allocator, LIST_SIZE)?;
        let od = ListOnDisk {
            header: BlockHeader {
                size: LIST_SIZE,
                tag: Self::eightcc(),
            },
            head: node_dsts.first().copied().unwrap_or(0),
            tail: node_dsts.last().copied().unwrap_or(0),
            len: n as u64,
        };
        plan.write(list_dst.start(), bytemuck::bytes_of(&od).to_vec());
        Ok(list_dst)
    }
}

impl<T: BStackBlock> BStackDrop for BStackLinkedList<T> {
    fn bstack_drop<A: BStackOwnedSliceAllocator>(self, allocator: &A) -> io::Result<()> {
        Self::__bstack_drop_children(self.range, allocator)?;
        // SAFETY: sole ownership of the list block was asserted at construction.
        unsafe { dealloc_range(allocator, self.range) }
    }
}

impl<T: BStackBlock> TryCloneIn for BStackLinkedList<T> {
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
