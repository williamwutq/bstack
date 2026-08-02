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

use core::cell::Cell;
use core::marker::PhantomData;
use core::mem::size_of;
use std::io;

use bstack::{BStack, BStackGenOp, BStackOwnedSliceAllocator, BStackRange};
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

/// Commit an atomic, **external-lock-free** read-modify-write to the list's
/// on-disk pointers via [`BStack::inplace_gen`].
///
/// `reads1` are absolute offsets of `u64` slots read in a first round; the values
/// are handed to `reads2` to compute a second round of offsets that may *depend*
/// on the first (e.g. the `prev`/`value` slots of the node found via the tail
/// pointer). `plan` then turns both read rounds into the writes to commit.
///
/// The point of routing every mutator through this: all reads happen **inside**
/// the generator, under bstack's single write lock, so the values reflect the
/// committed state at the one commit point and no other thread can interleave
/// between the reads and the dependent writes — no external lock, no torn
/// structure. Every write lands as one crash-atomic batch (all-or-nothing).
///
/// Only in-place reads/writes ride the generator; allocations and frees, which
/// change the stack's size, are done by the caller *around* it (a freshly
/// allocated node is an orphan until the commit links it; a freed node is already
/// unlinked), so a crash can at worst leak, never tear the list.
fn atomic_update<A, R2, W>(allocator: &A, reads1: &[u64], reads2: R2, plan: W) -> io::Result<()>
where
    A: BStackOwnedSliceAllocator,
    R2: FnOnce(&[u64]) -> Vec<u64>,
    W: FnOnce(&[u64], &[u64]) -> Vec<(u64, Vec<u8>)>,
{
    // Buffers that must outlive the whole `inplace_gen` call (bstack's documented
    // generator pattern): read-back values and the computed writes.
    let mut buf1: Vec<[u8; 8]> = vec![[0u8; 8]; reads1.len()];
    let mut vals1: Vec<u64> = Vec::new();
    let mut offs2: Vec<u64> = Vec::new();
    let mut buf2: Vec<[u8; 8]> = Vec::new();
    let mut writes: Vec<(u64, Vec<u8>)> = Vec::new();

    let mut reads2 = Some(reads2);
    let mut plan = Some(plan);

    let mut r1 = 0usize;
    let mut did_a = false;
    let mut r2 = 0usize;
    let mut did_b = false;
    let mut w = 0usize;

    allocator.stack().inplace_gen(|_feedback| {
        // Round 1 — read the fixed offsets.
        if r1 < reads1.len() {
            let i = r1;
            r1 += 1;
            // SAFETY: `buf1` outlives this call; one read op uses slot `i` at a time.
            let b: &mut [u8] = unsafe { core::mem::transmute::<&mut [u8], _>(&mut buf1[i][..]) };
            return Some(BStackGenOp::Read {
                offset: reads1[i],
                buf: b,
            });
        }
        // Transition A — compute the (possibly dependent) round-2 offsets.
        if !did_a {
            did_a = true;
            vals1 = buf1.iter().map(|x| u64::from_le_bytes(*x)).collect();
            offs2 = (reads2.take().unwrap())(&vals1);
            buf2 = vec![[0u8; 8]; offs2.len()];
        }
        // Round 2 — read the dependent offsets.
        if r2 < offs2.len() {
            let i = r2;
            r2 += 1;
            // SAFETY: `buf2` outlives this call and is not resized after Transition A.
            let b: &mut [u8] = unsafe { core::mem::transmute::<&mut [u8], _>(&mut buf2[i][..]) };
            return Some(BStackGenOp::Read {
                offset: offs2[i],
                buf: b,
            });
        }
        // Transition B — compute the writes from both read rounds.
        if !did_b {
            did_b = true;
            let vals2: Vec<u64> = buf2.iter().map(|x| u64::from_le_bytes(*x)).collect();
            writes = (plan.take().unwrap())(&vals1, &vals2);
        }
        // Commit phase — emit every write; they land together atomically.
        if w < writes.len() {
            let i = w;
            w += 1;
            let (off, ref bytes) = writes[i];
            // SAFETY: `writes` outlives this call and is not mutated after Transition B.
            let d: &[u8] = unsafe { core::mem::transmute::<&[u8], _>(bytes.as_slice()) };
            return Some(BStackGenOp::Write { offset: off, data: d });
        }
        None
    })
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
    ///
    /// Atomic and external-lock-free: the node is allocated first (an orphan),
    /// then the tail read, node-image write, tail/`prev.next` relink and length
    /// bump all commit as one crash-atomic [`atomic_update`]. A crash before the
    /// commit leaks the orphan node; it never tears the list.
    pub fn push_back<A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &A,
        value: BStackOwned<T>,
    ) -> io::Result<()> {
        let list = self.range.start();
        let val_off = value.into_inner().range().start();
        // Allocate the node up front; it stays an orphan until the commit links it.
        let node = allocator.alloc(NODE_SIZE)?.as_range().start();

        let res = atomic_update(
            allocator,
            &[list + TAIL_OFF, list + LEN_OFF],
            |_v1| Vec::new(),
            |v1, _v2| {
                let (old_tail, len) = (v1[0], v1[1]);
                let mut w = Vec::with_capacity(4);
                // The node's full image (with `prev` wired to the read tail).
                w.push((
                    node,
                    bytemuck::bytes_of(&Self::node_image(old_tail, 0, val_off)).to_vec(),
                ));
                // Link the old tail (or the head, if the list was empty) to it.
                let link = if old_tail != 0 {
                    old_tail + NNEXT_OFF
                } else {
                    list + HEAD_OFF
                };
                w.push((link, node.to_le_bytes().to_vec()));
                w.push((list + TAIL_OFF, node.to_le_bytes().to_vec()));
                w.push((list + LEN_OFF, (len + 1).to_le_bytes().to_vec()));
                w
            },
        );
        if res.is_err() {
            // The node was never linked in; reclaim the orphan.
            // SAFETY: freshly allocated, referenced by nobody.
            let _ = unsafe { dealloc_range(allocator, BStackRange::new(node, NODE_SIZE)) };
        }
        res
    }

    /// Prepend a value to the front, taking ownership of its block. Atomic and
    /// external-lock-free (see [`push_back`](Self::push_back)).
    pub fn push_front<A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &A,
        value: BStackOwned<T>,
    ) -> io::Result<()> {
        let list = self.range.start();
        let val_off = value.into_inner().range().start();
        let node = allocator.alloc(NODE_SIZE)?.as_range().start();

        let res = atomic_update(
            allocator,
            &[list + HEAD_OFF, list + LEN_OFF],
            |_v1| Vec::new(),
            |v1, _v2| {
                let (old_head, len) = (v1[0], v1[1]);
                let mut w = Vec::with_capacity(4);
                w.push((
                    node,
                    bytemuck::bytes_of(&Self::node_image(0, old_head, val_off)).to_vec(),
                ));
                let link = if old_head != 0 {
                    old_head + NPREV_OFF
                } else {
                    list + TAIL_OFF
                };
                w.push((link, node.to_le_bytes().to_vec()));
                w.push((list + HEAD_OFF, node.to_le_bytes().to_vec()));
                w.push((list + LEN_OFF, (len + 1).to_le_bytes().to_vec()));
                w
            },
        );
        if res.is_err() {
            // SAFETY: freshly allocated, referenced by nobody.
            let _ = unsafe { dealloc_range(allocator, BStackRange::new(node, NODE_SIZE)) };
        }
        res
    }

    /// Remove and return the last element (as an owned value block), or `None`
    /// if the list is empty. The node shell is freed; the value block is handed
    /// back to the caller.
    ///
    /// Atomic and external-lock-free: the tail is read, the target node's
    /// `prev`/`value` read (a dependent second round), and the relink + length
    /// decrement commit as one [`atomic_update`]. Only *after* the node is
    /// unlinked is its shell freed — a crash between leaks the shell, never a
    /// dangling link.
    pub fn pop_back<A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &A,
    ) -> io::Result<Option<BStackOwned<T>>> {
        let list = self.range.start();
        let node = Cell::new(0u64);
        let val = Cell::new(0u64);

        atomic_update(
            allocator,
            &[list + TAIL_OFF, list + LEN_OFF],
            |v1| {
                let tail = v1[0];
                if tail == 0 {
                    Vec::new()
                } else {
                    vec![tail + NPREV_OFF, tail + NVAL_OFF]
                }
            },
            |v1, v2| {
                let (tail, len) = (v1[0], v1[1]);
                if tail == 0 {
                    return Vec::new();
                }
                let (prev, value) = (v2[0], v2[1]);
                node.set(tail);
                val.set(value);
                let mut w = Vec::with_capacity(3);
                if prev != 0 {
                    w.push((prev + NNEXT_OFF, 0u64.to_le_bytes().to_vec()));
                    w.push((list + TAIL_OFF, prev.to_le_bytes().to_vec()));
                } else {
                    w.push((list + HEAD_OFF, 0u64.to_le_bytes().to_vec()));
                    w.push((list + TAIL_OFF, 0u64.to_le_bytes().to_vec()));
                }
                w.push((list + LEN_OFF, (len - 1).to_le_bytes().to_vec()));
                w
            },
        )?;

        if node.get() == 0 {
            return Ok(None);
        }
        // Unlinked above; free the (now unreachable) node shell.
        // SAFETY: the node is unlinked and solely ours.
        unsafe { dealloc_range(allocator, BStackRange::new(node.get(), NODE_SIZE))? };
        // SAFETY: the value block's ownership transfers to the caller.
        Ok(Some(unsafe { BStackOwned::from_raw(Self::value_at(val.get())) }))
    }

    /// Remove and return the first element (as an owned value block), or `None`
    /// if the list is empty. Atomic and external-lock-free (see
    /// [`pop_back`](Self::pop_back)).
    pub fn pop_front<A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &A,
    ) -> io::Result<Option<BStackOwned<T>>> {
        let list = self.range.start();
        let node = Cell::new(0u64);
        let val = Cell::new(0u64);

        atomic_update(
            allocator,
            &[list + HEAD_OFF, list + LEN_OFF],
            |v1| {
                let head = v1[0];
                if head == 0 {
                    Vec::new()
                } else {
                    vec![head + NNEXT_OFF, head + NVAL_OFF]
                }
            },
            |v1, v2| {
                let (head, len) = (v1[0], v1[1]);
                if head == 0 {
                    return Vec::new();
                }
                let (next, value) = (v2[0], v2[1]);
                node.set(head);
                val.set(value);
                let mut w = Vec::with_capacity(3);
                if next != 0 {
                    w.push((next + NPREV_OFF, 0u64.to_le_bytes().to_vec()));
                    w.push((list + HEAD_OFF, next.to_le_bytes().to_vec()));
                } else {
                    w.push((list + HEAD_OFF, 0u64.to_le_bytes().to_vec()));
                    w.push((list + TAIL_OFF, 0u64.to_le_bytes().to_vec()));
                }
                w.push((list + LEN_OFF, (len - 1).to_le_bytes().to_vec()));
                w
            },
        )?;

        if node.get() == 0 {
            return Ok(None);
        }
        // SAFETY: the node is unlinked and solely ours.
        unsafe { dealloc_range(allocator, BStackRange::new(node.get(), NODE_SIZE))? };
        // SAFETY: the value block's ownership transfers to the caller.
        Ok(Some(unsafe { BStackOwned::from_raw(Self::value_at(val.get())) }))
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
