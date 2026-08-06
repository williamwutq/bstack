//! [`BStackBTreeSet<K>`]: an owned ordered set of `Pod + Ord` keys, backed by a
//! copy-on-write B-tree, with an embedded counting Bloom filter front.
//!
//! The set analogue of [`crate::BStackBTreeMap`] — the same wide contiguous
//! nodes and path-copying insert, but each node stores only keys (no value
//! column). It gives sorted iteration and, like the map, is **single-writer /
//! multi-reader** (an insert path-copies the root-to-leaf path and commits the
//! new nodes plus the root swap as one atomic [`bstack::BStack::set_batched`]).
//!
//! # Bloom filter in front
//!
//! Like [`crate::BStackHashSet`], every set embeds a
//! [`crate::BStackCountingBloomFilter`] maintained as an over-approximation of
//! the tree, so [`contains`](BStackBTreeSet::contains) fast-rejects definitely-
//! absent keys without a tree descent. A key is added to the filter only when it
//! is genuinely new (an exact-membership check precedes the insert), so the
//! filter never over-counts and there are never false negatives.
//!
//! Not yet implemented: `remove` (B-tree deletion with rebalancing — the same gap
//! as [`crate::BStackBTreeMap`]).

use core::cmp::Ordering;
use core::marker::PhantomData;
use core::mem::size_of;
use std::io;

use bstack::{BStack, BStackOwnedSliceAllocator, BStackRange};
use bytemuck::{Pod, Zeroable};

use super::bloom::{BStackCountingBloomFilter, BloomOnDisk};
use super::util::{Scratch, alloc_image, read_u64};
use crate::block::{BStackBlock, BStackCast};
use crate::clone::{ClonePlan, TryCloneIn};
use crate::layout::{BlockHeader, EightCC, HEADER_SIZE, get_u64};
use crate::owned::BStackOwned;
use crate::teardown::{AutoDrop, BStackDrop, dealloc_range};

/// The on-disk image of a [`BStackBTreeSet`]: header, root node pointer (`0` =
/// empty), key count, and the embedded Bloom filter's handle offset.
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub struct TreeSetOnDisk {
    /// The 16-byte block header (size + type tag).
    pub header: BlockHeader,
    /// Offset of the root node, or `0` when the set is empty.
    pub root: u64,
    /// Number of keys.
    pub len: u64,
    /// Offset of the embedded counting Bloom filter's handle block.
    pub bloom: u64,
}

const ROOT_OFF: u64 = HEADER_SIZE; // 16
const LEN_OFF: u64 = HEADER_SIZE + 8; // 24
const BLOOM_OFF: u64 = HEADER_SIZE + 16; // 32
const TREESET_SIZE: u64 = size_of::<TreeSetOnDisk>() as u64;
const BLOOM_SIZE: u64 = size_of::<BloomOnDisk>() as u64;

const T: usize = 8;
const MAXKEYS: usize = 2 * T - 1; // 15
const MAXCHILDREN: usize = 2 * T; // 16

const NKEYS_OFF: usize = HEADER_SIZE as usize; // 16
const LEAF_OFF: usize = HEADER_SIZE as usize + 8; // 24
const KEYS_OFF: usize = HEADER_SIZE as usize + 16; // 32

const DEFAULT_ITEMS: u64 = 1024;
const DEFAULT_FP: f64 = 0.01;

/// A node decoded for building: keys as raw bytes and (internal) child offsets.
struct BNode {
    leaf: bool,
    keys: Vec<Vec<u8>>,
    children: Vec<u64>,
}

/// A median key lifted from a split, plus the new right node.
struct Split {
    key: Vec<u8>,
    right: u64,
}

/// Accumulates a path-copy insert's new nodes and the old path nodes to free.
struct Build<'a, A: BStackOwnedSliceAllocator> {
    allocator: &'a A,
    node_size: u64,
    ksize: usize,
    children_off: usize,
    writes: Vec<(u64, Vec<u8>)>,
    freed: Vec<u64>,
}

impl<'a, A: BStackOwnedSliceAllocator> Build<'a, A> {
    fn emit(&mut self, nb: &BNode) -> io::Result<u64> {
        let mut b = vec![0u8; self.node_size as usize];
        b[NKEYS_OFF..NKEYS_OFF + 8].copy_from_slice(&(nb.keys.len() as u64).to_le_bytes());
        b[LEAF_OFF..LEAF_OFF + 8].copy_from_slice(&(nb.leaf as u64).to_le_bytes());
        for (i, k) in nb.keys.iter().enumerate() {
            let ko = KEYS_OFF + i * self.ksize;
            b[ko..ko + self.ksize].copy_from_slice(k);
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

/// An owned ordered set of `Pod + Ord` keys with an embedded Bloom filter.
pub struct BStackBTreeSet<K: Pod + Ord> {
    range: BStackRange,
    _marker: PhantomData<fn() -> K>,
}

impl<K: Pod + Ord> BStackBTreeSet<K> {
    const fn ksize() -> usize {
        size_of::<K>()
    }
    const fn children_off() -> usize {
        KEYS_OFF + MAXKEYS * Self::ksize()
    }
    const fn node_size() -> u64 {
        (Self::children_off() + MAXCHILDREN * 8) as u64
    }

    fn read_key(bytes: &[u8]) -> K {
        bytemuck::pod_read_unaligned::<K>(&bytes[..Self::ksize()])
    }

    fn bloom(&self, stack: &BStack) -> io::Result<BStackCountingBloomFilter<K>> {
        let off = read_u64(stack, self.range.start() + BLOOM_OFF)?;
        Ok(<BStackCountingBloomFilter<K> as BStackBlock>::from_range(
            BStackRange::new(off, BLOOM_SIZE),
        ))
    }

    /// Allocate an empty set with a default-sized Bloom filter.
    pub fn new<A: BStackOwnedSliceAllocator>(allocator: &A) -> io::Result<BStackOwned<Self>> {
        Self::with_capacity(allocator, DEFAULT_ITEMS, DEFAULT_FP)
    }

    /// Allocate an empty set whose Bloom filter is sized for `expected_items` at
    /// false-positive rate `fp_rate`.
    pub fn with_capacity<A: BStackOwnedSliceAllocator>(
        allocator: &A,
        expected_items: u64,
        fp_rate: f64,
    ) -> io::Result<BStackOwned<Self>> {
        let bloom =
            BStackCountingBloomFilter::<K>::with_capacity(allocator, expected_items, fp_rate)?;
        let bloom_off = bloom.into_inner().range().start();
        let od = TreeSetOnDisk {
            header: BlockHeader {
                size: TREESET_SIZE,
                tag: Self::eightcc(),
            },
            root: 0,
            len: 0,
            bloom: bloom_off,
        };
        match alloc_image(allocator, bytemuck::bytes_of(&od)) {
            // SAFETY: a freshly allocated block owned by no other handle.
            Ok(range) => Ok(unsafe { BStackOwned::from_raw(Self::from_range(range)) }),
            Err(e) => {
                let bloom = <BStackCountingBloomFilter<K> as BStackBlock>::from_range(
                    BStackRange::new(bloom_off, BLOOM_SIZE),
                );
                let _ = unsafe { BStackOwned::from_raw(bloom) }.bstack_drop(allocator);
                Err(e)
            }
        }
    }

    /// Number of keys.
    pub fn len(&self, stack: &BStack) -> io::Result<u64> {
        read_u64(stack, self.range.start() + LEN_OFF)
    }

    /// Whether the set is empty.
    pub fn is_empty(&self, stack: &BStack) -> io::Result<bool> {
        Ok(self.len(stack)? == 0)
    }

    fn read_node(stack: &BStack, off: u64) -> io::Result<BNode> {
        let mut b = vec![0u8; Self::node_size() as usize];
        stack.get_into(off, &mut b)?;
        let nkeys = get_u64(&b[NKEYS_OFF..]) as usize;
        let leaf = get_u64(&b[LEAF_OFF..]) != 0;
        let ksize = Self::ksize();
        let children_off = Self::children_off();
        let mut keys = Vec::with_capacity(nkeys);
        for i in 0..nkeys {
            let ko = KEYS_OFF + i * ksize;
            keys.push(b[ko..ko + ksize].to_vec());
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
            children,
        })
    }

    /// First index `i` with `target <= keys[i]`, and whether it is exact.
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

    /// Split an over-full node around its median.
    fn split(mut nb: BNode) -> (BNode, Split, BNode) {
        let m = nb.keys.len() / 2;
        let right_children = if nb.leaf {
            Vec::new()
        } else {
            nb.children.split_off(m + 1)
        };
        let right_keys = nb.keys.split_off(m + 1);
        let med_key = nb.keys.pop().unwrap();
        let right = BNode {
            leaf: nb.leaf,
            keys: right_keys,
            children: right_children,
        };
        (
            nb,
            Split {
                key: med_key,
                right: 0,
            },
            right,
        )
    }

    /// Path-copy the subtree at `off`, inserting a **new** `key` (assumed absent).
    fn insert_rec(
        build: &mut Build<'_, impl BStackOwnedSliceAllocator>,
        stack: &BStack,
        off: u64,
        key: &K,
        key_bytes: &[u8],
    ) -> io::Result<(u64, Option<Split>)> {
        let mut nb = Self::read_node(stack, off)?;
        build.freed.push(off);
        let (i, _exact) = Self::search(&nb, key);

        if nb.leaf {
            nb.keys.insert(i, key_bytes.to_vec());
        } else {
            let child = nb.children[i];
            let (new_child, child_split) = Self::insert_rec(build, stack, child, key, key_bytes)?;
            nb.children[i] = new_child;
            if let Some(s) = child_split {
                nb.keys.insert(i, s.key);
                nb.children.insert(i + 1, s.right);
            }
        }

        if nb.keys.len() <= MAXKEYS {
            Ok((build.emit(&nb)?, None))
        } else {
            let (left, mut split, right) = Self::split(nb);
            split.right = build.emit(&right)?;
            Ok((build.emit(&left)?, Some(split)))
        }
    }

    /// Insert `key`; returns `true` if newly added, `false` if already present.
    pub fn insert<A: BStackOwnedSliceAllocator>(&self, allocator: &A, key: K) -> io::Result<bool> {
        // Exact check first, so the filter is only touched for genuinely new keys.
        let key_bytes = bytemuck::bytes_of(&key).to_vec();
        if self.tree_contains(allocator.stack(), &key, &key_bytes)? {
            return Ok(false);
        }
        self.bloom(allocator.stack())?.insert(allocator, &key)?;

        let handle = self.range.start();
        let stack = allocator.stack();
        let root = read_u64(stack, handle + ROOT_OFF)?;
        let len = read_u64(stack, handle + LEN_OFF)?;

        let mut build = Build {
            allocator,
            node_size: Self::node_size(),
            ksize: Self::ksize(),
            children_off: Self::children_off(),
            writes: Vec::new(),
            freed: Vec::new(),
        };

        let built: io::Result<u64> = (|| {
            if root == 0 {
                let leaf = BNode {
                    leaf: true,
                    keys: vec![key_bytes.clone()],
                    children: Vec::new(),
                };
                return build.emit(&leaf);
            }
            let (new_root0, split) = Self::insert_rec(&mut build, stack, root, &key, &key_bytes)?;
            if let Some(s) = split {
                let root_node = BNode {
                    leaf: false,
                    keys: vec![s.key],
                    children: vec![new_root0, s.right],
                };
                build.emit(&root_node)
            } else {
                Ok(new_root0)
            }
        })();

        match built {
            Ok(new_root) => {
                let new_node_offs: Vec<u64> = build.writes.iter().map(|(o, _)| *o).collect();
                let mut writes = core::mem::take(&mut build.writes);
                writes.push((handle + ROOT_OFF, new_root.to_le_bytes().to_vec()));
                writes.push((handle + LEN_OFF, (len + 1).to_le_bytes().to_vec()));
                match stack.set_batched(writes) {
                    Ok(()) => {
                        for off in &build.freed {
                            // SAFETY: replaced by the copy just committed (single-writer).
                            let _ = unsafe {
                                dealloc_range(allocator, BStackRange::new(*off, build.node_size))
                            };
                        }
                        Ok(true)
                    }
                    Err(e) => {
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

    /// Whether `key` is present. Fast-rejects via the Bloom filter first.
    pub fn contains(&self, stack: &BStack, key: &K) -> io::Result<bool> {
        if !self.bloom(stack)?.contains(stack, key)? {
            return Ok(false);
        }
        self.tree_contains(stack, key, bytemuck::bytes_of(key))
    }

    /// Exact membership descent (no Bloom fast-reject).
    fn tree_contains(&self, stack: &BStack, key: &K, _key_bytes: &[u8]) -> io::Result<bool> {
        let mut off = read_u64(stack, self.range.start() + ROOT_OFF)?;
        let ksize = Self::ksize();
        let children_off = Self::children_off();
        let mut scratch = Scratch::new();
        let node_size = Self::node_size() as usize;
        while off != 0 {
            let buf = scratch.buf(node_size);
            stack.get_into(off, buf)?;
            let nkeys = get_u64(&buf[NKEYS_OFF..]) as usize;
            let leaf = get_u64(&buf[LEAF_OFF..]) != 0;
            let mut i = nkeys;
            for j in 0..nkeys {
                let ko = KEYS_OFF + j * ksize;
                match key.cmp(&Self::read_key(&buf[ko..ko + ksize])) {
                    Ordering::Less => {
                        i = j;
                        break;
                    }
                    Ordering::Equal => return Ok(true),
                    Ordering::Greater => {}
                }
            }
            if leaf {
                return Ok(false);
            }
            off = get_u64(&buf[children_off + i * 8..]);
        }
        Ok(false)
    }

    /// The number of keys in the node at `off`.
    fn child_nkeys(stack: &BStack, off: u64) -> io::Result<usize> {
        let mut b = [0u8; 8];
        stack.get_into(off + NKEYS_OFF as u64, &mut b)?;
        Ok(get_u64(&b) as usize)
    }

    /// The rightmost / leftmost key bytes in the subtree at `off`.
    fn edge_key(stack: &BStack, off: u64, rightmost: bool) -> io::Result<Vec<u8>> {
        let mut nb = Self::read_node(stack, off)?;
        while !nb.leaf {
            let c = if rightmost {
                *nb.children.last().unwrap()
            } else {
                nb.children[0]
            };
            nb = Self::read_node(stack, c)?;
        }
        let i = if rightmost { nb.keys.len() - 1 } else { 0 };
        Ok(nb.keys[i].clone())
    }

    /// Path-copy delete of `key` from the subtree at `off`; returns the new
    /// subtree offset and whether the key was found.
    fn delete_off(
        build: &mut Build<'_, impl BStackOwnedSliceAllocator>,
        stack: &BStack,
        off: u64,
        key: &K,
    ) -> io::Result<(u64, bool)> {
        let nb = Self::read_node(stack, off)?;
        build.freed.push(off);
        let (nb2, found) = Self::delete_bnode(build, stack, nb, key)?;
        Ok((build.emit(&nb2)?, found))
    }

    /// Delete `key` from the in-memory node `nb`, rebalancing children to keep the
    /// B-tree invariant. Returns the modified node (not yet emitted) and found.
    fn delete_bnode(
        build: &mut Build<'_, impl BStackOwnedSliceAllocator>,
        stack: &BStack,
        mut nb: BNode,
        key: &K,
    ) -> io::Result<(BNode, bool)> {
        let (i, found) = Self::search(&nb, key);

        if found {
            if nb.leaf {
                nb.keys.remove(i);
                return Ok((nb, true));
            }
            let yc = Self::child_nkeys(stack, nb.children[i])?;
            let zc = Self::child_nkeys(stack, nb.children[i + 1])?;
            if yc >= T {
                let pk = Self::edge_key(stack, nb.children[i], true)?;
                nb.keys[i] = pk.clone();
                let (new_y, _) =
                    Self::delete_off(build, stack, nb.children[i], &Self::read_key(&pk))?;
                nb.children[i] = new_y;
            } else if zc >= T {
                let sk = Self::edge_key(stack, nb.children[i + 1], false)?;
                nb.keys[i] = sk.clone();
                let (new_z, _) =
                    Self::delete_off(build, stack, nb.children[i + 1], &Self::read_key(&sk))?;
                nb.children[i + 1] = new_z;
            } else {
                let y_off = nb.children[i];
                let z_off = nb.children[i + 1];
                let mut y = Self::read_node(stack, y_off)?;
                build.freed.push(y_off);
                let mut z = Self::read_node(stack, z_off)?;
                build.freed.push(z_off);
                let sk = nb.keys.remove(i);
                nb.children.remove(i + 1);
                y.keys.push(sk);
                y.keys.append(&mut z.keys);
                if !y.leaf {
                    y.children.append(&mut z.children);
                }
                let (y2, _) = Self::delete_bnode(build, stack, y, key)?;
                nb.children[i] = build.emit(&y2)?;
            }
            return Ok((nb, true));
        }

        if nb.leaf {
            return Ok((nb, false));
        }

        if Self::child_nkeys(stack, nb.children[i])? >= T {
            let (new_c, found) = Self::delete_off(build, stack, nb.children[i], key)?;
            nb.children[i] = new_c;
            return Ok((nb, found));
        }

        let n = nb.keys.len();
        if i > 0 && Self::child_nkeys(stack, nb.children[i - 1])? >= T {
            let ci_off = nb.children[i];
            let ls_off = nb.children[i - 1];
            let mut ci = Self::read_node(stack, ci_off)?;
            build.freed.push(ci_off);
            let mut ls = Self::read_node(stack, ls_off)?;
            build.freed.push(ls_off);
            ci.keys.insert(0, nb.keys[i - 1].clone());
            if !ci.leaf {
                ci.children.insert(0, ls.children.pop().unwrap());
            }
            nb.keys[i - 1] = ls.keys.pop().unwrap();
            nb.children[i - 1] = build.emit(&ls)?;
            let (ci2, found) = Self::delete_bnode(build, stack, ci, key)?;
            nb.children[i] = build.emit(&ci2)?;
            return Ok((nb, found));
        }
        if i < n && Self::child_nkeys(stack, nb.children[i + 1])? >= T {
            let ci_off = nb.children[i];
            let rs_off = nb.children[i + 1];
            let mut ci = Self::read_node(stack, ci_off)?;
            build.freed.push(ci_off);
            let mut rs = Self::read_node(stack, rs_off)?;
            build.freed.push(rs_off);
            ci.keys.push(nb.keys[i].clone());
            if !ci.leaf {
                ci.children.push(rs.children.remove(0));
            }
            nb.keys[i] = rs.keys.remove(0);
            nb.children[i + 1] = build.emit(&rs)?;
            let (ci2, found) = Self::delete_bnode(build, stack, ci, key)?;
            nb.children[i] = build.emit(&ci2)?;
            return Ok((nb, found));
        }

        if i < n {
            let ci_off = nb.children[i];
            let rs_off = nb.children[i + 1];
            let mut ci = Self::read_node(stack, ci_off)?;
            build.freed.push(ci_off);
            let mut rs = Self::read_node(stack, rs_off)?;
            build.freed.push(rs_off);
            let sk = nb.keys.remove(i);
            nb.children.remove(i + 1);
            ci.keys.push(sk);
            ci.keys.append(&mut rs.keys);
            if !ci.leaf {
                ci.children.append(&mut rs.children);
            }
            let (ci2, found) = Self::delete_bnode(build, stack, ci, key)?;
            nb.children[i] = build.emit(&ci2)?;
            Ok((nb, found))
        } else {
            let ls_off = nb.children[i - 1];
            let ci_off = nb.children[i];
            let mut ls = Self::read_node(stack, ls_off)?;
            build.freed.push(ls_off);
            let mut ci = Self::read_node(stack, ci_off)?;
            build.freed.push(ci_off);
            let sk = nb.keys.remove(i - 1);
            nb.children.remove(i);
            ls.keys.push(sk);
            ls.keys.append(&mut ci.keys);
            if !ls.leaf {
                ls.children.append(&mut ci.children);
            }
            let (ls2, found) = Self::delete_bnode(build, stack, ls, key)?;
            nb.children[i - 1] = build.emit(&ls2)?;
            Ok((nb, found))
        }
    }

    /// Remove `key`; returns `true` if it was present. Deletes from the tree
    /// first, then decrements the Bloom filter (see the module docs).
    pub fn remove<A: BStackOwnedSliceAllocator>(&self, allocator: &A, key: &K) -> io::Result<bool> {
        let handle = self.range.start();
        let stack = allocator.stack();
        let key_bytes = bytemuck::bytes_of(key).to_vec();
        if !self.tree_contains(stack, key, &key_bytes)? {
            return Ok(false);
        }
        let root = read_u64(stack, handle + ROOT_OFF)?;
        let len = read_u64(stack, handle + LEN_OFF)?;

        let mut build = Build {
            allocator,
            node_size: Self::node_size(),
            ksize: Self::ksize(),
            children_off: Self::children_off(),
            writes: Vec::new(),
            freed: Vec::new(),
        };

        let built: io::Result<u64> = (|| {
            let nb = Self::read_node(stack, root)?;
            build.freed.push(root);
            let (root_nb, _) = Self::delete_bnode(&mut build, stack, nb, key)?;
            let new_root = if root_nb.keys.is_empty() {
                if root_nb.leaf { 0 } else { root_nb.children[0] }
            } else {
                build.emit(&root_nb)?
            };
            Ok(new_root)
        })();

        match built {
            Ok(new_root) => {
                let new_node_offs: Vec<u64> = build.writes.iter().map(|(o, _)| *o).collect();
                let mut writes = core::mem::take(&mut build.writes);
                writes.push((handle + ROOT_OFF, new_root.to_le_bytes().to_vec()));
                writes.push((handle + LEN_OFF, (len - 1).to_le_bytes().to_vec()));
                match stack.set_batched(writes) {
                    Ok(()) => {
                        for off in &build.freed {
                            let _ = unsafe {
                                dealloc_range(allocator, BStackRange::new(*off, build.node_size))
                            };
                        }
                        // Tree updated; now decrement the filter.
                        self.bloom(stack)?.remove(allocator, key)?;
                        Ok(true)
                    }
                    Err(e) => {
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

    /// The smallest key, or `None` if empty.
    pub fn first(&self, stack: &BStack) -> io::Result<Option<K>> {
        self.extreme(stack, true)
    }

    /// The largest key, or `None` if empty.
    pub fn last(&self, stack: &BStack) -> io::Result<Option<K>> {
        self.extreme(stack, false)
    }

    fn extreme(&self, stack: &BStack, leftmost: bool) -> io::Result<Option<K>> {
        let mut off = read_u64(stack, self.range.start() + ROOT_OFF)?;
        if off == 0 {
            return Ok(None);
        }
        loop {
            let nb = Self::read_node(stack, off)?;
            if nb.leaf {
                let i = if leftmost { 0 } else { nb.keys.len() - 1 };
                return Ok(Some(Self::read_key(&nb.keys[i])));
            }
            off = if leftmost {
                nb.children[0]
            } else {
                nb.children[nb.keys.len()]
            };
        }
    }

    /// Collect every key in ascending order.
    pub fn to_vec(&self, stack: &BStack) -> io::Result<Vec<K>> {
        let mut out = Vec::new();
        let root = read_u64(stack, self.range.start() + ROOT_OFF)?;
        Self::collect(stack, root, &mut out)?;
        Ok(out)
    }

    fn collect(stack: &BStack, off: u64, out: &mut Vec<K>) -> io::Result<()> {
        if off == 0 {
            return Ok(());
        }
        let nb = Self::read_node(stack, off)?;
        for i in 0..nb.keys.len() {
            if !nb.leaf {
                Self::collect(stack, nb.children[i], out)?;
            }
            out.push(Self::read_key(&nb.keys[i]));
        }
        if !nb.leaf {
            Self::collect(stack, nb.children[nb.keys.len()], out)?;
        }
        Ok(())
    }

    /// Attach an allocator to make an auto-freeing [`AutoDrop`] guard.
    pub fn auto<A: BStackOwnedSliceAllocator>(self, allocator: &A) -> AutoDrop<'_, Self, A> {
        // SAFETY: sole ownership was asserted when the set was created.
        unsafe { AutoDrop::from_raw(self, allocator) }
    }

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
        // SAFETY: the set solely owns each node block.
        unsafe { dealloc_range(allocator, BStackRange::new(off, Self::node_size()))? };
        Ok(())
    }

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
        let children_off = Self::children_off();
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

impl<K: Pod + Ord> BStackCast for BStackBTreeSet<K> {
    /// A `"TSt"` prefix perturbed by the key size.
    fn eightcc() -> EightCC {
        const BASE: EightCC = EightCC::new([b'T', b'S', b't', 0x80, 0x81, 0x82, 0x83, 0x84]);
        BASE.mix(EightCC::new((size_of::<K>() as u64).to_le_bytes()))
    }
}

impl<K: Pod + Ord> BStackBlock for BStackBTreeSet<K> {
    type OnDisk = TreeSetOnDisk;

    fn from_range(range: BStackRange) -> Self {
        BStackBTreeSet {
            range,
            _marker: PhantomData,
        }
    }

    fn range(&self) -> BStackRange {
        self.range
    }

    /// Recursively free every node and the embedded Bloom filter, **without**
    /// freeing the handle block itself.
    fn __bstack_drop_children<A: BStackOwnedSliceAllocator>(
        range: BStackRange,
        allocator: &A,
    ) -> io::Result<()> {
        let handle = range.start();
        let root = read_u64(allocator.stack(), handle + ROOT_OFF)?;
        Self::drop_subtree(allocator.stack(), root, allocator)?;
        let bloom_off = read_u64(allocator.stack(), handle + BLOOM_OFF)?;
        if bloom_off != 0 {
            // SAFETY: the set solely owns its embedded Bloom filter.
            let bloom = <BStackCountingBloomFilter<K> as BStackBlock>::from_range(
                BStackRange::new(bloom_off, BLOOM_SIZE),
            );
            unsafe { BStackOwned::from_raw(bloom) }.bstack_drop(allocator)?;
        }
        Ok(())
    }

    /// Deep-clone every node and the Bloom filter into `plan`, then stage the
    /// handle.
    fn __bstack_clone_into<A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &A,
        plan: &mut ClonePlan,
    ) -> io::Result<BStackRange> {
        let handle = self.range.start();
        let root = read_u64(allocator.stack(), handle + ROOT_OFF)?;
        let len = read_u64(allocator.stack(), handle + LEN_OFF)?;
        let bloom_off = read_u64(allocator.stack(), handle + BLOOM_OFF)?;

        let new_root = Self::clone_subtree(allocator.stack(), root, allocator, plan)?;
        let bloom = <BStackCountingBloomFilter<K> as BStackBlock>::from_range(BStackRange::new(
            bloom_off, BLOOM_SIZE,
        ));
        let new_bloom = bloom.__bstack_clone_into(allocator, plan)?.start();

        let handle_dst = plan.alloc_raw(allocator, TREESET_SIZE)?;
        let od = TreeSetOnDisk {
            header: BlockHeader {
                size: TREESET_SIZE,
                tag: Self::eightcc(),
            },
            root: new_root,
            len,
            bloom: new_bloom,
        };
        plan.write(handle_dst.start(), bytemuck::bytes_of(&od).to_vec());
        Ok(handle_dst)
    }
}

impl<K: Pod + Ord> BStackDrop for BStackBTreeSet<K> {
    fn bstack_drop<A: BStackOwnedSliceAllocator>(self, allocator: &A) -> io::Result<()> {
        Self::__bstack_drop_children(self.range, allocator)?;
        // SAFETY: sole ownership of the handle block was asserted at construction.
        unsafe { dealloc_range(allocator, self.range) }
    }
}

impl<K: Pod + Ord> TryCloneIn for BStackBTreeSet<K> {
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
