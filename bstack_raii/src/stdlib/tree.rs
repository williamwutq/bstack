//! [`BStackBTreeMap<K, V>`]: an owned, ordered map backed by a copy-on-write
//! B-tree.
//!
//! # Why a B-tree, not a binary tree
//!
//! On disk, pointer chasing is the enemy: every edge you follow is a seek to a
//! physically unrelated block. A red-black or AVL tree stores **one** key per
//! node, so a lookup in a million-entry tree chases ~20 pointers. A B-tree packs
//! many keys into each wide, **contiguous** node, so the same lookup reads only a
//! handful of nodes (with minimum degree `T = 8`, up to 15 keys per node, height
//! ~5 at a million entries). Same `O(log n)`, far fewer seeks — the on-disk
//! ordered-map you actually want, giving sorted iteration and range scans that a
//! [`crate::BStackHashMap`] cannot.
//!
//! Keys are **`Pod + Ord`** (`K`): stored inline in the node and compared by
//! value; values are blocks (`V: BStackBlock`) the tree owns via a `u64` ref.
//!
//! # Copy-on-write, single-writer
//!
//! Mutation is **path-copying** (the LMDB model): an insert rewrites only the
//! root-to-leaf path into freshly allocated nodes (splitting as needed), leaving
//! every untouched subtree shared, then commits all the new nodes **and** the new
//! root pointer as one atomic [`bstack::BStack::set_batched`] batch. The commit
//! point is the root swap: before it the tree is entirely the old version, after
//! it entirely the new one — so it is crash-atomic, and any number of readers
//! traversing the old root are unaffected.
//!
//! Unlike the lock-free [`crate::BStackDeque`] / [`crate::BStackHashMap`]
//! mutators, a B-tree write reads a whole path to build the new one, so
//! **concurrent writers need external synchronization** (one writer at a time);
//! concurrent *readers* are always fine. This is the same trade LMDB makes, and
//! it keeps each write atomic and crash-safe. (A path is short — a handful of
//! nodes — so the copy cost is small.)
//!
//! Not yet implemented: `remove` (B-tree deletion with rebalancing is the natural
//! next step; the path-copy + `set_batched` commit machinery here carries over).

use core::cmp::Ordering;
use core::marker::PhantomData;
use core::mem::size_of;
use std::io;

use bstack::{BStack, BStackOwnedSliceAllocator, BStackRange};
use bytemuck::{Pod, Zeroable};

use super::util::{Scratch, alloc_image, read_u64};
use crate::block::{BStackBlock, BStackCast};
use crate::clone::{ClonePlan, TryCloneIn};
use crate::layout::{BlockHeader, EightCC, HEADER_SIZE, get_u64};
use crate::owned::BStackOwned;
use crate::teardown::{AutoDrop, BStackDrop, dealloc_range};

/// The on-disk image of a [`BStackBTreeMap`]: header, root node pointer (`0` =
/// empty), and entry count. Non-generic.
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub struct TreeOnDisk {
    /// The 16-byte block header (size + type tag).
    pub header: BlockHeader,
    /// Offset of the root node, or `0` when the tree is empty.
    pub root: u64,
    /// Number of entries.
    pub len: u64,
}

const ROOT_OFF: u64 = HEADER_SIZE; // 16
const LEN_OFF: u64 = HEADER_SIZE + 8; // 24
const TREE_SIZE: u64 = size_of::<TreeOnDisk>() as u64;

/// Minimum degree: a node holds `T-1..=2T-1` keys (the root may hold fewer).
const T: usize = 8;
const MAXKEYS: usize = 2 * T - 1; // 15
const MAXCHILDREN: usize = 2 * T; // 16

// Node field offsets (keys/vals/children arrays follow, sized by `K`).
const NKEYS_OFF: usize = HEADER_SIZE as usize; // 16
const LEAF_OFF: usize = HEADER_SIZE as usize + 8; // 24
const KEYS_OFF: usize = HEADER_SIZE as usize + 16; // 32

/// A node decoded for building/mutation: keys as raw `K` bytes, value refs, and
/// (for an internal node) child node offsets (`keys.len() + 1` of them).
struct BNode {
    leaf: bool,
    keys: Vec<Vec<u8>>,
    vals: Vec<u64>,
    children: Vec<u64>,
}

/// A median lifted out of a split child: its key/value plus the new right node.
struct Split {
    key: Vec<u8>,
    val: u64,
    right: u64,
}

/// Accumulates a path-copy insert's new-node writes and the old path nodes to
/// free, so the whole insert commits as one [`BStack::set_batched`] batch.
struct Build<'a, A: BStackOwnedSliceAllocator> {
    allocator: &'a A,
    node_size: u64,
    ksize: usize,
    vals_off: usize,
    children_off: usize,
    /// New node images `(offset, bytes)`, committed together.
    writes: Vec<(u64, Vec<u8>)>,
    /// Old path nodes, freed after the commit succeeds.
    freed: Vec<u64>,
}

impl<'a, A: BStackOwnedSliceAllocator> Build<'a, A> {
    /// Serialize `nb` and allocate a fresh block for it (an orphan until the
    /// commit links it), returning its offset.
    fn emit(&mut self, nb: &BNode) -> io::Result<u64> {
        let mut b = vec![0u8; self.node_size as usize];
        b[NKEYS_OFF..NKEYS_OFF + 8].copy_from_slice(&(nb.keys.len() as u64).to_le_bytes());
        b[LEAF_OFF..LEAF_OFF + 8].copy_from_slice(&(nb.leaf as u64).to_le_bytes());
        for (i, k) in nb.keys.iter().enumerate() {
            let ko = KEYS_OFF + i * self.ksize;
            b[ko..ko + self.ksize].copy_from_slice(k);
        }
        for (i, v) in nb.vals.iter().enumerate() {
            let vo = self.vals_off + i * 8;
            b[vo..vo + 8].copy_from_slice(&v.to_le_bytes());
        }
        for (i, c) in nb.children.iter().enumerate() {
            let co = self.children_off + i * 8;
            b[co..co + 8].copy_from_slice(&c.to_le_bytes());
        }
        let off = self.allocator.alloc(self.node_size)?.as_range().start();
        self.writes.push((off, b));
        Ok(off)
    }
}

/// An owned, ordered map backed by a copy-on-write B-tree.
///
/// A typed handle (a newtype over a [`BStackRange`]); [`new`](Self::new) returns a
/// bare [`BStackOwned<BStackBTreeMap<K, V>>`] that frees nothing on scope exit —
/// free it with [`bstack_drop`](BStackDrop::bstack_drop) or wrap it
/// ([`AutoDrop`] / [`crate::BStackCow`]).
pub struct BStackBTreeMap<K: Pod + Ord, V: BStackBlock> {
    range: BStackRange,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K: Pod + Ord, V: BStackBlock> BStackBTreeMap<K, V> {
    const fn ksize() -> usize {
        size_of::<K>()
    }
    const fn vals_off() -> usize {
        KEYS_OFF + MAXKEYS * Self::ksize()
    }
    const fn children_off() -> usize {
        Self::vals_off() + MAXKEYS * 8
    }
    const fn node_size() -> u64 {
        (Self::children_off() + MAXCHILDREN * 8) as u64
    }

    fn value_size() -> u64 {
        size_of::<<V as BStackBlock>::OnDisk>() as u64
    }
    fn value_at(off: u64) -> V {
        <V as BStackBlock>::from_range(BStackRange::new(off, Self::value_size()))
    }

    fn read_key(bytes: &[u8]) -> K {
        bytemuck::pod_read_unaligned::<K>(&bytes[..Self::ksize()])
    }

    /// Allocate an empty tree.
    pub fn new<A: BStackOwnedSliceAllocator>(allocator: &A) -> io::Result<BStackOwned<Self>> {
        let od = TreeOnDisk {
            header: BlockHeader {
                size: TREE_SIZE,
                tag: Self::eightcc(),
            },
            root: 0,
            len: 0,
        };
        let range = alloc_image(allocator, bytemuck::bytes_of(&od))?;
        // SAFETY: a freshly allocated block owned by no other handle.
        Ok(unsafe { BStackOwned::from_raw(Self::from_range(range)) })
    }

    /// Number of entries.
    pub fn len(&self, stack: &BStack) -> io::Result<u64> {
        read_u64(stack, self.range.start() + LEN_OFF)
    }

    /// Whether the tree has no entries.
    pub fn is_empty(&self, stack: &BStack) -> io::Result<bool> {
        Ok(self.len(stack)? == 0)
    }

    /// Decode the node at `off`.
    fn read_node(stack: &BStack, off: u64) -> io::Result<BNode> {
        let mut b = vec![0u8; Self::node_size() as usize];
        stack.get_into(off, &mut b)?;
        let nkeys = get_u64(&b[NKEYS_OFF..]) as usize;
        let leaf = get_u64(&b[LEAF_OFF..]) != 0;
        let ksize = Self::ksize();
        let vals_off = Self::vals_off();
        let children_off = Self::children_off();
        let mut keys = Vec::with_capacity(nkeys);
        let mut vals = Vec::with_capacity(nkeys);
        for i in 0..nkeys {
            let ko = KEYS_OFF + i * ksize;
            keys.push(b[ko..ko + ksize].to_vec());
            vals.push(get_u64(&b[vals_off + i * 8..]));
        }
        let mut children = Vec::new();
        if !leaf {
            for i in 0..=nkeys {
                children.push(get_u64(&b[children_off + i * 8..]));
            }
        }
        Ok(BNode {
            leaf,
            keys,
            vals,
            children,
        })
    }

    /// Locate `target` among a node's keys: the first index `i` with
    /// `target <= keys[i]`, and whether it is an exact match.
    fn search(nb: &BNode, target: &K) -> (usize, bool) {
        for (j, kb) in nb.keys.iter().enumerate() {
            match target.cmp(&Self::read_key(kb)) {
                Ordering::Less => return (j, false),
                Ordering::Equal => return (j, true),
                Ordering::Greater => {}
            }
        }
        (nb.keys.len(), false)
    }

    /// Split an over-full node (`keys.len() == 2T`) around its median: returns the
    /// left node and the lifted median; the caller emits the right node.
    fn split(mut nb: BNode) -> (BNode, Split, BNode) {
        let m = nb.keys.len() / 2;
        let right_children = if nb.leaf {
            Vec::new()
        } else {
            nb.children.split_off(m + 1)
        };
        let right_keys = nb.keys.split_off(m + 1);
        let right_vals = nb.vals.split_off(m + 1);
        let med_key = nb.keys.pop().unwrap();
        let med_val = nb.vals.pop().unwrap();
        let right = BNode {
            leaf: nb.leaf,
            keys: right_keys,
            vals: right_vals,
            children: right_children,
        };
        let split = Split {
            key: med_key,
            val: med_val,
            right: 0, // filled in by the caller after emitting `right`
        };
        (nb, split, right)
    }

    /// Recursively path-copy the subtree at `off`, inserting `key -> val`.
    /// Returns the new subtree offset, an optional lifted split, whether a new
    /// entry was added, and any replaced value.
    fn insert_rec(
        build: &mut Build<'_, impl BStackOwnedSliceAllocator>,
        stack: &BStack,
        off: u64,
        key: &K,
        key_bytes: &[u8],
        val: u64,
    ) -> io::Result<(u64, Option<Split>, bool, Option<u64>)> {
        let mut nb = Self::read_node(stack, off)?;
        build.freed.push(off);
        let (i, exact) = Self::search(&nb, key);

        if exact {
            let old = nb.vals[i];
            nb.vals[i] = val;
            let new_off = build.emit(&nb)?;
            return Ok((new_off, None, false, Some(old)));
        }

        let (added, old) = if nb.leaf {
            nb.keys.insert(i, key_bytes.to_vec());
            nb.vals.insert(i, val);
            (true, None)
        } else {
            let child = nb.children[i];
            let (new_child, child_split, added, old) =
                Self::insert_rec(build, stack, child, key, key_bytes, val)?;
            nb.children[i] = new_child;
            if let Some(s) = child_split {
                // The child split and already emitted its right node into `s.right`.
                nb.keys.insert(i, s.key);
                nb.vals.insert(i, s.val);
                nb.children.insert(i + 1, s.right);
            }
            (added, old)
        };

        if nb.keys.len() <= MAXKEYS {
            let new_off = build.emit(&nb)?;
            Ok((new_off, None, added, old))
        } else {
            let (left, mut split, right) = Self::split(nb);
            split.right = build.emit(&right)?;
            let left_off = build.emit(&left)?;
            Ok((left_off, Some(split), added, old))
        }
    }

    /// Insert `key -> value`, taking ownership of the value block. Returns the
    /// previously-mapped value (owned) if `key` was already present, else `None`.
    ///
    /// Path-copies the affected path and commits every new node plus the root
    /// swap as one crash-atomic batch. **Single-writer** (see the module docs).
    pub fn insert<A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &A,
        key: K,
        value: BStackOwned<V>,
    ) -> io::Result<Option<BStackOwned<V>>> {
        let handle = self.range.start();
        let stack = allocator.stack();
        let key_bytes = bytemuck::bytes_of(&key).to_vec();
        let val_ref = value.into_inner().range().start();

        let root = read_u64(stack, handle + ROOT_OFF)?;
        let len = read_u64(stack, handle + LEN_OFF)?;

        let mut build = Build {
            allocator,
            node_size: Self::node_size(),
            ksize: Self::ksize(),
            vals_off: Self::vals_off(),
            children_off: Self::children_off(),
            writes: Vec::new(),
            freed: Vec::new(),
        };

        let built: io::Result<(u64, bool, Option<u64>)> = (|| {
            if root == 0 {
                // Empty tree: a single-entry leaf becomes the root.
                let leaf = BNode {
                    leaf: true,
                    keys: vec![key_bytes.clone()],
                    vals: vec![val_ref],
                    children: Vec::new(),
                };
                let new_root = build.emit(&leaf)?;
                return Ok((new_root, true, None));
            }
            let (new_root0, split, added, old) =
                Self::insert_rec(&mut build, stack, root, &key, &key_bytes, val_ref)?;
            let new_root = if let Some(s) = split {
                // Root split: a fresh root holds the median over the two halves.
                let root_node = BNode {
                    leaf: false,
                    keys: vec![s.key],
                    vals: vec![s.val],
                    children: vec![new_root0, s.right],
                };
                build.emit(&root_node)?
            } else {
                new_root0
            };
            Ok((new_root, added, old))
        })();

        match built {
            Ok((new_root, added, old)) => {
                let new_node_offs: Vec<u64> = build.writes.iter().map(|(o, _)| *o).collect();
                let mut writes = core::mem::take(&mut build.writes);
                writes.push((handle + ROOT_OFF, new_root.to_le_bytes().to_vec()));
                if added {
                    writes.push((handle + LEN_OFF, (len + 1).to_le_bytes().to_vec()));
                }
                match stack.set_batched(writes) {
                    Ok(()) => {
                        // Free the old path nodes (leak-only on crash).
                        for off in &build.freed {
                            // SAFETY: replaced by the copy just committed; nothing
                            // else references it (single-writer).
                            let _ = unsafe {
                                dealloc_range(allocator, BStackRange::new(*off, build.node_size))
                            };
                        }
                        Ok(old.map(|o| unsafe { BStackOwned::from_raw(Self::value_at(o)) }))
                    }
                    Err(e) => {
                        // Nothing committed: reclaim the new nodes we allocated.
                        for off in new_node_offs {
                            let _ = unsafe {
                                dealloc_range(allocator, BStackRange::new(off, build.node_size))
                            };
                        }
                        Err(e)
                    }
                }
            }
            Err(e) => {
                for (off, _) in &build.writes {
                    let _ = unsafe {
                        dealloc_range(allocator, BStackRange::new(*off, build.node_size))
                    };
                }
                Err(e)
            }
        }
    }

    /// A **borrowed** handle to the value mapped by `key` (no ownership), or
    /// `None` if absent.
    pub fn get(&self, stack: &BStack, key: &K) -> io::Result<Option<V>> {
        let mut off = read_u64(stack, self.range.start() + ROOT_OFF)?;
        let ksize = Self::ksize();
        let vals_off = Self::vals_off();
        let children_off = Self::children_off();
        // Stack buffer for the node (reused down the descent; no heap alloc for
        // typical key sizes — see `Scratch`).
        let mut scratch = Scratch::new();
        let node_size = Self::node_size() as usize;
        while off != 0 {
            let buf = scratch.buf(node_size);
            stack.get_into(off, buf)?;
            let nkeys = get_u64(&buf[NKEYS_OFF..]) as usize;
            let leaf = get_u64(&buf[LEAF_OFF..]) != 0;
            let mut i = nkeys;
            let mut exact = false;
            for j in 0..nkeys {
                let ko = KEYS_OFF + j * ksize;
                match key.cmp(&Self::read_key(&buf[ko..ko + ksize])) {
                    Ordering::Less => {
                        i = j;
                        break;
                    }
                    Ordering::Equal => {
                        i = j;
                        exact = true;
                        break;
                    }
                    Ordering::Greater => {}
                }
            }
            if exact {
                return Ok(Some(Self::value_at(get_u64(&buf[vals_off + i * 8..]))));
            }
            if leaf {
                return Ok(None);
            }
            off = get_u64(&buf[children_off + i * 8..]);
        }
        Ok(None)
    }

    /// Whether `key` is present.
    pub fn contains_key(&self, stack: &BStack, key: &K) -> io::Result<bool> {
        Ok(self.get(stack, key)?.is_some())
    }

    /// The smallest entry, or `None` if empty. Descends the leftmost path.
    pub fn first(&self, stack: &BStack) -> io::Result<Option<(K, V)>> {
        self.extreme(stack, true)
    }

    /// The largest entry, or `None` if empty. Descends the rightmost path.
    pub fn last(&self, stack: &BStack) -> io::Result<Option<(K, V)>> {
        self.extreme(stack, false)
    }

    fn extreme(&self, stack: &BStack, leftmost: bool) -> io::Result<Option<(K, V)>> {
        let mut off = read_u64(stack, self.range.start() + ROOT_OFF)?;
        if off == 0 {
            return Ok(None);
        }
        loop {
            let nb = Self::read_node(stack, off)?;
            if nb.leaf {
                let i = if leftmost { 0 } else { nb.keys.len() - 1 };
                return Ok(Some((
                    Self::read_key(&nb.keys[i]),
                    Self::value_at(nb.vals[i]),
                )));
            }
            off = if leftmost {
                nb.children[0]
            } else {
                nb.children[nb.keys.len()]
            };
        }
    }

    /// Collect every entry in ascending key order. The value handles are borrowed
    /// (do not free them; valid only while the tree does).
    pub fn to_vec(&self, stack: &BStack) -> io::Result<Vec<(K, V)>> {
        let mut out = Vec::new();
        let root = read_u64(stack, self.range.start() + ROOT_OFF)?;
        Self::collect(stack, root, &mut out)?;
        Ok(out)
    }

    fn collect(stack: &BStack, off: u64, out: &mut Vec<(K, V)>) -> io::Result<()> {
        if off == 0 {
            return Ok(());
        }
        let nb = Self::read_node(stack, off)?;
        for i in 0..nb.keys.len() {
            if !nb.leaf {
                Self::collect(stack, nb.children[i], out)?;
            }
            out.push((Self::read_key(&nb.keys[i]), Self::value_at(nb.vals[i])));
        }
        if !nb.leaf {
            Self::collect(stack, nb.children[nb.keys.len()], out)?;
        }
        Ok(())
    }

    /// Attach an allocator to make an auto-freeing [`AutoDrop`] guard.
    pub fn auto<A: BStackOwnedSliceAllocator>(self, allocator: &A) -> AutoDrop<'_, Self, A> {
        // SAFETY: sole ownership was asserted when the tree was created.
        unsafe { AutoDrop::from_raw(self, allocator) }
    }

    /// Recursively free the subtree at `off` (values then nodes).
    fn drop_subtree<A: BStackOwnedSliceAllocator>(
        stack: &BStack,
        off: u64,
        allocator: &A,
    ) -> io::Result<()> {
        if off == 0 {
            return Ok(());
        }
        let nb = Self::read_node(stack, off)?;
        if !nb.leaf {
            for &c in &nb.children {
                Self::drop_subtree(stack, c, allocator)?;
            }
        }
        for &v in &nb.vals {
            if v != 0 {
                // SAFETY: the tree solely owns each value block.
                let owned = unsafe { BStackOwned::from_raw(Self::value_at(v)) };
                owned.bstack_drop(allocator)?;
            }
        }
        // SAFETY: the tree solely owns each node block.
        unsafe { dealloc_range(allocator, BStackRange::new(off, Self::node_size()))? };
        Ok(())
    }

    /// Recursively deep-clone the subtree at `off` into `plan`, returning the new
    /// subtree offset.
    fn clone_subtree<A: BStackOwnedSliceAllocator>(
        stack: &BStack,
        off: u64,
        allocator: &A,
        plan: &mut ClonePlan,
    ) -> io::Result<u64> {
        if off == 0 {
            return Ok(0);
        }
        let mut buf = vec![0u8; Self::node_size() as usize];
        stack.get_into(off, &mut buf)?;
        let nkeys = get_u64(&buf[NKEYS_OFF..]) as usize;
        let leaf = get_u64(&buf[LEAF_OFF..]) != 0;
        let vals_off = Self::vals_off();
        let children_off = Self::children_off();

        // Deep-clone each value and repoint it in the copy.
        for i in 0..nkeys {
            let vo = vals_off + i * 8;
            let vref = get_u64(&buf[vo..]);
            let cloned = Self::value_at(vref)
                .__bstack_clone_into(allocator, plan)?
                .start();
            buf[vo..vo + 8].copy_from_slice(&cloned.to_le_bytes());
        }
        // Recurse into children and repoint them.
        if !leaf {
            for i in 0..=nkeys {
                let co = children_off + i * 8;
                let child = get_u64(&buf[co..]);
                let new_child = Self::clone_subtree(stack, child, allocator, plan)?;
                buf[co..co + 8].copy_from_slice(&new_child.to_le_bytes());
            }
        }
        let dst = plan.alloc_raw(allocator, Self::node_size())?;
        plan.write(dst.start(), buf);
        Ok(dst.start())
    }
}

impl<K: Pod + Ord, V: BStackBlock> BStackCast for BStackBTreeMap<K, V> {
    /// A `"Tree"` prefix perturbed by the key size and the value type's tag.
    fn eightcc() -> EightCC {
        const BASE: EightCC = EightCC::new([b'T', b'r', b'e', b'e', 0x80, 0x81, 0x82, 0x83]);
        BASE.mix(EightCC::new((size_of::<K>() as u64).to_le_bytes()))
            .mix(<V as BStackCast>::eightcc())
    }
}

impl<K: Pod + Ord, V: BStackBlock> BStackBlock for BStackBTreeMap<K, V> {
    type OnDisk = TreeOnDisk;

    fn from_range(range: BStackRange) -> Self {
        BStackBTreeMap {
            range,
            _marker: PhantomData,
        }
    }

    fn range(&self) -> BStackRange {
        self.range
    }

    /// Recursively free every value block and node, **without** freeing the
    /// handle block itself.
    fn __bstack_drop_children<A: BStackOwnedSliceAllocator>(
        range: BStackRange,
        allocator: &A,
    ) -> io::Result<()> {
        let root = read_u64(allocator.stack(), range.start() + ROOT_OFF)?;
        Self::drop_subtree(allocator.stack(), root, allocator)
    }

    /// Deep-clone the whole tree into `plan`: every node copied, every value
    /// deep-cloned via `V`'s clone hook, the handle staged — all in the parent
    /// plan's single atomic commit.
    fn __bstack_clone_into<A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &A,
        plan: &mut ClonePlan,
    ) -> io::Result<BStackRange> {
        let handle = self.range.start();
        let root = read_u64(allocator.stack(), handle + ROOT_OFF)?;
        let len = read_u64(allocator.stack(), handle + LEN_OFF)?;
        let new_root = Self::clone_subtree(allocator.stack(), root, allocator, plan)?;

        let handle_dst = plan.alloc_raw(allocator, TREE_SIZE)?;
        let od = TreeOnDisk {
            header: BlockHeader {
                size: TREE_SIZE,
                tag: Self::eightcc(),
            },
            root: new_root,
            len,
        };
        plan.write(handle_dst.start(), bytemuck::bytes_of(&od).to_vec());
        Ok(handle_dst)
    }
}

impl<K: Pod + Ord, V: BStackBlock> BStackDrop for BStackBTreeMap<K, V> {
    fn bstack_drop<A: BStackOwnedSliceAllocator>(self, allocator: &A) -> io::Result<()> {
        Self::__bstack_drop_children(self.range, allocator)?;
        // SAFETY: sole ownership of the handle block was asserted at construction.
        unsafe { dealloc_range(allocator, self.range) }
    }
}

impl<K: Pod + Ord, V: BStackBlock> TryCloneIn for BStackBTreeMap<K, V> {
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
