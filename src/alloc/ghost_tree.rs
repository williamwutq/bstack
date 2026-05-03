use super::{BStackAllocator, BStackBulkAllocator, BStackSlice};
use crate::BStack;
use std::io;

const ALGT_MAGIC: [u8; 8] = *b"ALGT\x00\x01\x00\x00";
const ALGT_MAGIC_PREFIX: [u8; 6] = *b"ALGT\x00\x01";

/// Payload offset of the magic number.
const MAGIC_OFFSET: u64 = 32;
/// Payload offset of the AVL root pointer.
const ROOT_OFFSET: u64 = 40;
/// First payload offset managed by the allocator (32-byte aligned on disk).
const ARENA_START: u64 = 48;

/// Minimum allocation size — exactly the size of one AVL node.
const MIN_ALLOC: u64 = 32;

/// Null / absent pointer sentinel stored in AVL node child fields.
const NULL_PTR: u64 = 0;

// Node offsets within a free block (AVL node header fields).
const NODE_SIZE_OFF: u64 = 0;
const NODE_BF_OFF: u64 = 8; // i8 balance factor
const NODE_HEIGHT_OFF: u64 = 9; // u8 height (max ~59 for balanced; slightly more tolerated)
const NODE_LEFT_OFF: u64 = 16;
const NODE_RIGHT_OFF: u64 = 24;

// Read a value of type `$ty` from `$buf` at offset `$off`.
macro_rules! read_buf {
    ($buf:expr, $off:expr => $ty:ty) => {{
        let start = $off as usize;
        let end = start + std::mem::size_of::<$ty>();
        $buf[start..end].try_into().unwrap()
    }};
}

macro_rules! write_buf {
    ($val:expr => $buf:expr, $off:expr) => {{
        let bytes = $val.to_le_bytes();
        let start = $off as usize;
        let end = start + bytes.len();
        $buf[start..end].copy_from_slice(&bytes);
    }};
}

// Read a little-endian value of type `$ty` from `$buf` at offset `$off`.
macro_rules! read_buf_le {
    ($buf:expr, $off:expr => $ty:ty) => {
        <$ty>::from_le_bytes(read_buf!($buf, $off => $ty))
    };
}

/// A pure-AVL general-purpose allocator built on top of a [`BStack`].
///
/// Free blocks store their AVL node inline at offset 0 within the block —
/// live allocations carry **zero** overhead (no headers, no footers).  The tree
/// is keyed on `(size, address)` for a strict total order.  All memory is kept
/// zeroed: the BStack zeroes on extension, and the allocator zeroes on free.
///
/// Implements both [`BStackAllocator`] and [`BStackBulkAllocator`].
///
/// # Operation summary
///
/// | Operation               | Strategy                                          | Crash-safe |
/// |-------------------------|---------------------------------------------------|------------|
/// | `alloc`                 | best-fit from AVL tree, or `extend`               | multi-call |
/// | `alloc_bulk`            | one block for the combined size, then split       | single-call|
/// | `realloc` same block    | in-place length update; zero gap on shrink        | multi-call |
/// | `realloc` shrink (tail) | zero gap, `discard` freed tail                    | multi-call |
/// | `realloc` shrink        | zero gap + freed tail, AVL insert                 | multi-call |
/// | `realloc` grow (tail)   | `extend` in-place — no copy                       | single-call|
/// | `realloc` grow          | alloc new, copy, dealloc old                      | multi-call |
/// | `dealloc` (tail)        | `discard` — O(1), no AVL insert                   | single-call|
/// | `dealloc`               | zero block, AVL insert                            | multi-call |
/// | `dealloc_bulk`          | merge adjacent slices, then tail-truncate/insert  | multi-call |
///
/// # On-disk layout
///
/// ```text
/// ┌─────────────────────────────┐  payload offset 0
/// │   User-reserved (32 bytes)  │
/// ├─────────────────────────────┤  offset 32
/// │   Magic number (8 bytes)    │  "ALGT\x00\x01\x00\x00"
/// ├─────────────────────────────┤  offset 40
/// │   AVL root pointer (8 B)    │  absolute payload offset of the root node
/// ├─────────────────────────────┤  offset 48  ← arena start (32-byte aligned)
/// │   ... heap grows upward ... │
/// └─────────────────────────────┘
/// ```
///
/// # Alignment
///
/// All allocations are aligned to 32 bytes.  The arena starts at payload offset
/// 48, which maps to a 32-byte-aligned disk address because the BStack header
/// is 16 bytes (`16 + 48 = 64 = 2 × 32`).
///
/// # Bulk allocation
///
/// [`BStackBulkAllocator`] is implemented with a single-block strategy: each
/// requested length is rounded up to 32 bytes individually, the sum is
/// allocated as one contiguous block (one AVL remove or one `extend`), and
/// the block is sliced into per-request regions.  When all slices are returned
/// together to `dealloc_bulk`, adjacent slices are merged and freed as a
/// single operation — typically one `discard` if the slices are at the tail.
///
/// # Crash safety
///
/// No write-ahead log, no checksums.  A crash during `dealloc` before the AVL
/// insert permanently loses that block.  A crash during rotation leaves the tree
/// imbalanced — corrected on the next [`GhostTreeBstackAllocator::new`].
pub struct GhostTreeBstackAllocator {
    stack: BStack,
}

impl GhostTreeBstackAllocator {
    /// Open or initialise a `GhostTreeBstackAllocator` on `stack`.
    ///
    /// | BStack payload size        | Action                                             |
    /// |----------------------------|----------------------------------------------------|
    /// | 0                          | Fresh init: extend to `ARENA_START`, write magic   |
    /// | 1 … `ARENA_START` − 1     | **Error** — partial header, unrecoverable          |
    /// | ≥ `ARENA_START`, misaligned | Pad with zeroes to the next 32-byte arena boundary |
    /// | ≥ `ARENA_START`, aligned   | Verify magic, then coalesce and rebalance          |
    ///
    /// The 32 user-reserved bytes at payload offset 0 are never touched.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidData`] if the payload size falls in the
    /// unrecoverable range, or if the magic prefix does not match `ALGT`.
    pub fn new(stack: BStack) -> io::Result<Self> {
        let size = stack.len()?;

        if size == 0 {
            stack.extend(ARENA_START)?;
            stack.set(MAGIC_OFFSET, ALGT_MAGIC)?;
            // ROOT_OFFSET is zeroed by extend — null root pointer.
            return Ok(Self { stack });
        }

        if size < ARENA_START {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "GhostTreeBstackAllocator: payload is {size} B, \
                     too small for the {ARENA_START}-byte header"
                ),
            ));
        }

        // Verify magic prefix.
        let mut magic_buf = [0u8; 6];
        stack.get_into(MAGIC_OFFSET, &mut magic_buf)?;
        if magic_buf != ALGT_MAGIC_PREFIX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "GhostTreeBstackAllocator: magic number mismatch",
            ));
        }

        // Pad to the next 32-byte arena boundary if the tail is misaligned.
        let arena_used = size - ARENA_START;
        let remainder = arena_used % 32;
        if remainder != 0 {
            stack.extend(32 - remainder)?;
        }

        let this = Self { stack };
        this.coalesce_and_rebalance()?;
        Ok(this)
    }
}

impl GhostTreeBstackAllocator {
    /// Read the AVL root pointer from the header.
    #[inline]
    fn read_root(&self) -> io::Result<u64> {
        let buf = &mut [0u8; 8];
        self.stack.get_into(ROOT_OFFSET, buf)?;
        Ok(read_buf_le!(buf, 0 => u64))
    }

    /// Write the AVL root pointer to the header.
    #[inline]
    fn write_root(&self, ptr: u64) -> io::Result<()> {
        let mut buf = [0u8; 8];
        write_buf!(ptr => buf, 0);
        self.stack.set(ROOT_OFFSET, buf)?;
        Ok(())
    }

    /// Read the entire AVL node at `ptr` and return `(size, bf, height, left, right)`.
    fn read_node(&self, ptr: u64) -> io::Result<(u64, i8, u8, u64, u64)> {
        let buf = &mut [0u8; 32];
        self.stack.get_into(ptr, buf)?;
        let size = read_buf_le!(buf, NODE_SIZE_OFF   => u64);
        let bf = read_buf_le!(buf, NODE_BF_OFF     => i8);
        let height = read_buf_le!(buf, NODE_HEIGHT_OFF => u8);
        let left = read_buf_le!(buf, NODE_LEFT_OFF   => u64);
        let right = read_buf_le!(buf, NODE_RIGHT_OFF  => u64);
        Ok((size, bf, height, left, right))
    }

    /// Write a complete AVL node at `ptr`.
    fn write_node(
        &self,
        ptr: u64,
        size: u64,
        bf: i8,
        height: u8,
        left: u64,
        right: u64,
    ) -> io::Result<()> {
        let mut buf = [0u8; 32];
        write_buf!(size   => buf, NODE_SIZE_OFF);
        write_buf!(bf     => buf, NODE_BF_OFF);
        write_buf!(height => buf, NODE_HEIGHT_OFF);
        write_buf!(left   => buf, NODE_LEFT_OFF);
        write_buf!(right  => buf, NODE_RIGHT_OFF);
        self.stack.set(ptr, buf)?;
        Ok(())
    }

    /// Round `ptr` up to the next 32-byte boundary (minimum 32).
    #[inline]
    fn align_up_ptr(ptr: u64) -> u64 {
        ((ptr + 15) & !31) + 16
    }

    /// Round `len` up to the next multiple of 32, with a floor of [`MIN_ALLOC`].
    #[inline]
    fn align_up_len(len: u64) -> u64 {
        ((len + 31) & !31).max(MIN_ALLOC)
    }

    /// Return the stored height of the subtree rooted at `ptr` (0 for [`NULL_PTR`]).
    ///
    /// O(1) — reads the `height` field from the node header.
    #[inline]
    fn avl_height(&self, ptr: u64) -> io::Result<u8> {
        if ptr == NULL_PTR {
            return Ok(0);
        }
        let (_, _, height, _, _) = self.read_node(ptr)?;
        Ok(height)
    }

    /// Write `(size, left, right)` to `ptr`, computing bf and height from the
    /// children's stored heights in one pass.  Returns the balance factor.
    ///
    /// Replaces the `write_node(…, 0, 0, …) + avl_update_bf` pair: instead of
    /// writing stale zeros and reading back, we read the two child heights once,
    /// compute both fields, and write the node exactly once.
    #[inline]
    fn avl_write_and_update(&self, ptr: u64, size: u64, left: u64, right: u64) -> io::Result<i8> {
        let lh = self.avl_height(left)? as i16;
        let rh = self.avl_height(right)? as i16;
        let bf = (rh - lh) as i8;
        let height = (1 + lh.max(rh)) as u8;
        self.write_node(ptr, size, bf, height, left, right)?;
        Ok(bf)
    }

    /// Recompute bf and height for `node` from its children's stored heights,
    /// write both back, and return the balance factor.
    ///
    /// O(1) — delegates to [`avl_write_and_update`](Self::avl_write_and_update).
    #[inline]
    fn avl_update_bf(&self, node: u64) -> io::Result<i8> {
        let (size, _, _, left, right) = self.read_node(node)?;
        self.avl_write_and_update(node, size, left, right)
    }

    /// Right-rotate around `node`; return the new subtree root.
    ///
    /// ```text
    ///     node           pivot
    ///    /    \    →    /     \
    /// pivot    R       L      node
    ///  / \                   /    \
    /// L   M                 M      R
    /// ```
    fn avl_rotate_right(&self, node: u64) -> io::Result<u64> {
        let (node_sz, _, _, pivot, node_r) = self.read_node(node)?;
        let (pivot_sz, _, _, pivot_l, pivot_r) = self.read_node(pivot)?;
        self.avl_write_and_update(node, node_sz, pivot_r, node_r)?;
        self.avl_write_and_update(pivot, pivot_sz, pivot_l, node)?;
        Ok(pivot)
    }

    /// Left-rotate around `node`; return the new subtree root.
    ///
    /// ```text
    ///  node              pivot
    ///  /  \      →      /     \
    /// L   pivot       node     R
    ///     /  \        /  \
    ///    M    R      L    M
    /// ```
    fn avl_rotate_left(&self, node: u64) -> io::Result<u64> {
        let (node_sz, _, _, node_l, pivot) = self.read_node(node)?;
        let (pivot_sz, _, _, pivot_l, pivot_r) = self.read_node(pivot)?;
        self.avl_write_and_update(node, node_sz, node_l, pivot_l)?;
        self.avl_write_and_update(pivot, pivot_sz, node, pivot_r)?;
        Ok(pivot)
    }

    /// Fix imbalance at `node` after an insert or remove, then return the
    /// (possibly new) subtree root.  Children must already be balanced.
    ///
    /// Uses `< -1` / `> 1` rather than `== -2` / `== 2` so that a node whose
    /// balance factor exceeds ±2 (possible after crash recovery) still gets
    /// corrected instead of silently passed over.
    fn avl_rebalance(&self, node: u64) -> io::Result<u64> {
        let bf = self.avl_update_bf(node)?;
        if bf < -1 {
            let (_, _, _, left, _) = self.read_node(node)?;
            let (_, left_bf, _, _, _) = self.read_node(left)?;
            if left_bf > 0 {
                // Left-right case: rotate left child left first.
                let new_left = self.avl_rotate_left(left)?;
                let (node_sz, _, _, _, node_r) = self.read_node(node)?;
                self.avl_write_and_update(node, node_sz, new_left, node_r)?;
            }
            self.avl_rotate_right(node)
        } else if bf > 1 {
            let (_, _, _, _, right) = self.read_node(node)?;
            let (_, right_bf, _, _, _) = self.read_node(right)?;
            if right_bf < 0 {
                // Right-left case: rotate right child right first.
                let new_right = self.avl_rotate_right(right)?;
                let (node_sz, _, _, node_l, _) = self.read_node(node)?;
                self.avl_write_and_update(node, node_sz, node_l, new_right)?;
            }
            self.avl_rotate_left(node)
        } else {
            Ok(node)
        }
    }

    /// Recursive insert into subtree at `root`; return new subtree root.
    fn avl_insert_rec(&self, root: u64, ptr: u64, size: u64) -> io::Result<u64> {
        if root == NULL_PTR {
            self.write_node(ptr, size, 0, 1, NULL_PTR, NULL_PTR)?;
            return Ok(ptr);
        }
        let (root_sz, _, _, left, right) = self.read_node(root)?;
        if (size, ptr) < (root_sz, root) {
            let new_left = self.avl_insert_rec(left, ptr, size)?;
            self.avl_write_and_update(root, root_sz, new_left, right)?;
        } else {
            let new_right = self.avl_insert_rec(right, ptr, size)?;
            self.avl_write_and_update(root, root_sz, left, new_right)?;
        }
        self.avl_rebalance(root)
    }

    /// Insert a free block at `ptr` with `size` bytes into the AVL tree.
    fn avl_insert(&self, ptr: u64, size: u64) -> io::Result<()> {
        let root = self.read_root()?;
        let new_root = self.avl_insert_rec(root, ptr, size)?;
        self.write_root(new_root)
    }

    /// Return `(ptr, size)` of the leftmost (minimum-key) node in `subtree`.
    fn avl_min(&self, subtree: u64) -> io::Result<(u64, u64)> {
        let (size, _, _, left, _) = self.read_node(subtree)?;
        if left == NULL_PTR {
            Ok((subtree, size))
        } else {
            self.avl_min(left)
        }
    }

    /// Recursive remove of `(size, ptr)` from subtree at `root`; return new root.
    fn avl_remove_rec(&self, root: u64, ptr: u64, size: u64) -> io::Result<u64> {
        if root == NULL_PTR {
            return Ok(NULL_PTR);
        }
        let (root_sz, _, _, left, right) = self.read_node(root)?;
        if (size, ptr) < (root_sz, root) {
            let new_left = self.avl_remove_rec(left, ptr, size)?;
            self.avl_write_and_update(root, root_sz, new_left, right)?;
            return self.avl_rebalance(root);
        }
        if (size, ptr) > (root_sz, root) {
            let new_right = self.avl_remove_rec(right, ptr, size)?;
            self.avl_write_and_update(root, root_sz, left, new_right)?;
            return self.avl_rebalance(root);
        }
        // Found the node to remove.
        if left == NULL_PTR {
            return Ok(right);
        }
        if right == NULL_PTR {
            return Ok(left);
        }
        // Two children: replace with the in-order successor (leftmost of right
        // subtree), then delete the successor from the right subtree.
        let (succ, succ_sz) = self.avl_min(right)?;
        let new_right = self.avl_remove_rec(right, succ, succ_sz)?;
        self.avl_write_and_update(succ, succ_sz, left, new_right)?;
        self.avl_rebalance(succ)
    }

    /// Find and remove the best-fit block (smallest block ≥ `min_size`) from
    /// the subtree at `root` in a single O(log n) pass.
    ///
    /// Returns `(new_subtree_root, Option<(found_ptr, found_size)>)`.
    ///
    /// Strategy: when the current node fits, recurse left to try to find a
    /// smaller fit.  If the left subtree yields a candidate, wire the updated
    /// left child back and rebalance — the current node stays in the tree.  If
    /// not, the current node *is* the best fit and is removed using the standard
    /// two-child replacement (in-order successor).
    fn avl_find_best_fit_and_remove_rec(
        &self,
        root: u64,
        min_size: u64,
    ) -> io::Result<(u64, Option<(u64, u64)>)> {
        if root == NULL_PTR {
            return Ok((NULL_PTR, None));
        }
        let (root_sz, _, _, left, right) = self.read_node(root)?;
        if root_sz >= min_size {
            // This node fits — try left for something smaller.
            let (new_left, found) = self.avl_find_best_fit_and_remove_rec(left, min_size)?;
            if let Some(candidate) = found {
                // A smaller fit was found; keep root, update its left child.
                self.avl_write_and_update(root, root_sz, new_left, right)?;
                let new_root = self.avl_rebalance(root)?;
                return Ok((new_root, Some(candidate)));
            }
            // No smaller fit in the left subtree — remove root itself.
            // Use `new_left` (not the stale `left`): even though no node was
            // removed, the recursive call may have rebalanced the left subtree,
            // changing its root pointer.
            let new_root = if new_left == NULL_PTR {
                right
            } else if right == NULL_PTR {
                new_left
            } else {
                let (succ, succ_sz) = self.avl_min(right)?;
                let new_right = self.avl_remove_rec(right, succ, succ_sz)?;
                self.avl_write_and_update(succ, succ_sz, new_left, new_right)?;
                self.avl_rebalance(succ)?
            };
            Ok((new_root, Some((root, root_sz))))
        } else {
            // Too small — only right subtree can have a fit.
            let (new_right, found) = self.avl_find_best_fit_and_remove_rec(right, min_size)?;
            // Only update the tree structure if a node was actually removed.
            // Updating unconditionally would corrupt child pointers on a no-fit
            // path (the recursive call may have rebalanced without removing).
            if found.is_some() {
                self.avl_write_and_update(root, root_sz, left, new_right)?;
                let new_root = self.avl_rebalance(root)?;
                Ok((new_root, found))
            } else {
                Ok((root, None))
            }
        }
    }

    /// Find and remove the best-fit block (smallest block ≥ `min_size`).
    ///
    /// Returns `(ptr, size)`, or `None` if no block fits.
    fn avl_find_best_fit_and_remove(&self, min_size: u64) -> io::Result<Option<(u64, u64)>> {
        let root = self.read_root()?;
        let (new_root, found) = self.avl_find_best_fit_and_remove_rec(root, min_size)?;
        self.write_root(new_root)?;
        Ok(found)
    }

    /// In-order walk of the subtree at `root`, calling `f(ptr, size)` per node.
    /// Tolerates imbalance — visits every reachable node.
    fn avl_walk_inorder(
        &self,
        root: u64,
        f: &mut dyn FnMut(u64, u64) -> io::Result<()>,
    ) -> io::Result<()> {
        if root == NULL_PTR {
            return Ok(());
        }
        let (size, _, _, left, right) = self.read_node(root)?;
        self.avl_walk_inorder(left, f)?;
        f(root, size)?;
        self.avl_walk_inorder(right, f)
    }

    /// Collect all free blocks, merge adjacent ones, and rebuild a balanced AVL
    /// tree.  Called by [`Self::new`] on every open to recover from crashes.
    ///
    /// Free block data beyond their 32-byte headers is already zeroed by
    /// invariant.  When two blocks A and B are merged (A.end == B.ptr), B's
    /// 32-byte header becomes interior bytes of the merged block and must be
    /// zeroed before the tree is rebuilt.
    fn coalesce_and_rebalance(&self) -> io::Result<()> {
        // Step 1: collect all free blocks in key order
        let root = self.read_root()?;
        let mut blocks: Vec<(u64, u64)> = Vec::new(); // (ptr, size)
        self.avl_walk_inorder(root, &mut |ptr, size| {
            blocks.push((ptr, size));
            Ok(())
        })?;

        if blocks.is_empty() {
            return Ok(());
        }

        // Step 2: sort by address
        blocks.sort_by_key(|&(ptr, _)| ptr);

        // Step 3: coalesce adjacent pairs
        // `seams` holds the ptr of every absorbed sub-block whose 32-byte AVL
        // header must be zeroed before the tree is rebuilt.
        let mut coalesced: Vec<(u64, u64)> = Vec::new();
        let mut seams: Vec<u64> = Vec::new();
        for (ptr, size) in blocks {
            if let Some(last) = coalesced.last_mut()
                && last.0 + last.1 == ptr
            {
                seams.push(ptr);
                last.1 += size;
                continue;
            }
            coalesced.push((ptr, size));
        }

        // Zero the absorbed headers so the invariant holds inside merged blocks.
        for seam in seams {
            self.stack.zero(seam, MIN_ALLOC)?;
        }

        // Step 4: rebuild a balanced AVL tree
        // Coalescing sorted by address; now re-sort by the tree's key (size, ptr)
        // so `build` produces a valid BST.  Without this, insert/remove would
        // navigate by (size, ptr) into an address-ordered tree and miss nodes.
        coalesced.sort_by_key(|&(ptr, size)| (size, ptr));

        // Recursive helper: build an optimally balanced BST from a sorted slice,
        // writing each node and returning the root ptr.
        fn build(this: &GhostTreeBstackAllocator, blocks: &[(u64, u64)]) -> io::Result<u64> {
            if blocks.is_empty() {
                return Ok(NULL_PTR);
            }
            let mid = blocks.len() / 2;
            let (ptr, size) = blocks[mid];
            let left = build(this, &blocks[..mid])?;
            let right = build(this, &blocks[mid + 1..])?;
            this.avl_write_and_update(ptr, size, left, right)?;
            Ok(ptr)
        }

        let new_root = build(self, &coalesced)?;
        self.write_root(new_root)
    }
}

impl BStackAllocator for GhostTreeBstackAllocator {
    fn stack(&self) -> &BStack {
        &self.stack
    }

    fn into_stack(self) -> BStack {
        self.stack
    }

    /// Allocate `len` zeroed bytes using best-fit from the AVL tree.
    ///
    /// The returned slice length is `align_up_len(len)` (≥ 32) in the split
    /// case, or the full reclaimed block size when the remainder is too small
    /// to split (< 32 bytes, transparently absorbed into the caller's slice).
    ///
    /// # Crash safety
    ///
    /// Multi-call.  A crash between AVL remove and the split-insert permanently
    /// loses the remainder fragment; a crash between AVL remove and return
    /// loses the entire block.
    fn alloc(&self, len: u64) -> io::Result<BStackSlice<'_, Self>> {
        if len == 0 {
            return Ok(BStackSlice::new(self, 0, 0));
        }
        let aligned = Self::align_up_len(len);
        if let Some((ptr, block_size)) = self.avl_find_best_fit_and_remove(aligned)? {
            let remainder = block_size - aligned;
            if remainder >= MIN_ALLOC {
                // Split: the leading `remainder` bytes become a new free block.
                // The AVL node is written into those bytes by avl_insert.
                // The tail `aligned` bytes are already zeroed by invariant.
                self.avl_insert(ptr, remainder)?;
                Ok(BStackSlice::new(self, ptr + remainder, len))
            } else {
                // No split: give the whole block.  The stale AVL node in the
                // first 32 bytes must be zeroed; the rest is already zeroed.
                // Any bytes beyond `len` (up to `block_size`) are internal
                // padding and will be recovered on dealloc by re-aligning.
                self.stack.zero(ptr, MIN_ALLOC)?;
                Ok(BStackSlice::new(self, ptr, len))
            }
        } else {
            // No free block fits: grow the BStack (returns zeroed bytes).
            let start = self.stack.extend(aligned)?;
            Ok(BStackSlice::new(self, start, len))
        }
    }

    /// Resize `slice` to `new_len` bytes.
    ///
    /// **Shrink:** if the freed tail ≥ 32 bytes, zero it and insert it into the
    /// tree.  If the tail < 32, it is absorbed into the returned slice — the
    /// allocation cannot be shrunk below the next 32-byte boundary.
    ///
    /// **Grow:** allocate a new block, copy contents, free the old block.
    ///
    /// # Crash safety
    ///
    /// Shrink with a splittable tail: multi-call (zero + AVL insert).
    /// Grow: multi-call (alloc + copy + dealloc).
    fn realloc<'a>(
        &'a self,
        slice: BStackSlice<'a, Self>,
        new_len: u64,
    ) -> io::Result<BStackSlice<'a, Self>> {
        if slice.is_empty() {
            return self.alloc(new_len);
        }
        if slice.start() < ARENA_START || slice.start() != Self::align_up_ptr(slice.start()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "realloc: slice origin is not a valid allocator address",
            ));
        }
        if new_len == 0 {
            self.dealloc(slice)?;
            return Ok(BStackSlice::new(self, 0, 0));
        }
        let old_len = slice.len();
        // Re-align to recover the true underlying block sizes.
        let aligned_old = Self::align_up_len(old_len);
        let aligned_new = Self::align_up_len(new_len);

        if aligned_new == aligned_old {
            // Same underlying block — just update the visible length.
            // If it is a shrink, we need to zero the tail to uphold the invariant
            // but we can do that in-place without touching the AVL tree since the block size doesn't change.
            if new_len < old_len {
                let tail_ptr = slice.start() + new_len;
                let tail_len = old_len - new_len;
                self.stack.zero(tail_ptr, tail_len)?;
            }
            return Ok(BStackSlice::new(self, slice.start(), new_len));
        }

        let is_tail = slice.start() + aligned_old == self.stack.len()?;

        if aligned_new < aligned_old {
            // Shrink.
            let freed_tail = aligned_old - aligned_new;
            let tail_ptr = slice.start() + aligned_new;
            if is_tail {
                // Zero the gap [new_len..aligned_new] only; then truncate
                // the BStack rather than recycling the freed tail.
                if new_len < aligned_new {
                    self.stack
                        .zero(slice.start() + new_len, aligned_new - new_len)?;
                }
                self.stack.discard(freed_tail)?;
            } else {
                // Zero [new_len..aligned_old] in one call (gap + freed tail),
                // then insert the freed tail into the AVL tree.
                self.stack
                    .zero(slice.start() + new_len, aligned_old - new_len)?;
                self.avl_insert(tail_ptr, freed_tail)?;
            }
            return Ok(BStackSlice::new(self, slice.start(), new_len));
        }

        if is_tail {
            // Grow at the tail: extend the BStack directly, no copy needed.
            self.stack.extend(aligned_new - aligned_old)?;
            return Ok(BStackSlice::new(self, slice.start(), new_len));
        }

        // Grow (non-tail): allocate new region, copy old data, free old region.
        let new_slice = self.alloc(new_len)?;
        let data = self.stack.get(slice.start(), slice.start() + old_len)?;
        self.stack.set(new_slice.start(), &data)?;
        self.dealloc(slice)?;
        Ok(new_slice)
    }

    /// Release `slice` back to the free pool.
    ///
    /// Zeros the entire region (upholding the zeroed-memory invariant), then
    /// inserts it into the AVL tree.  No coalescing is performed; adjacent free
    /// blocks accumulate until the next [`GhostTreeBstackAllocator::new`] call.
    ///
    /// # Crash safety
    ///
    /// Multi-call: a crash after the zero but before the AVL insert permanently
    /// loses the block.
    fn dealloc(&self, slice: BStackSlice<'_, Self>) -> io::Result<()> {
        if slice.is_empty() {
            return Ok(());
        }
        if slice.start() < ARENA_START || slice.start() != Self::align_up_ptr(slice.start()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "dealloc: slice origin is not a valid allocator address",
            ));
        }
        let ptr = slice.start();
        // Re-align to recover the true block size that alloc carved out.
        // Any bytes beyond the requested length (< 32) are absorbed here.
        let true_len = Self::align_up_len(slice.len());
        // Tail optimisation: if this block sits at the end of the BStack,
        // truncate instead of recycling through the AVL tree.
        if ptr + true_len == self.stack.len()? {
            return self.stack.discard(true_len);
        }
        self.stack.zero(ptr, true_len)?;
        self.avl_insert(ptr, true_len)
    }
}

impl BStackBulkAllocator for GhostTreeBstackAllocator {
    /// Allocate all slices in a single contiguous block.
    ///
    /// Each requested length is rounded up to 32-byte alignment individually;
    /// the sum of those aligned sizes is allocated as one block (either from
    /// the free tree or via a single `BStack::extend`).  The block is then
    /// sliced into per-request regions, each carrying the original requested
    /// length.  Zero-length requests produce null `(0, 0)` slices without
    /// contributing to the block.
    ///
    /// # Atomicity
    ///
    /// One block allocation (one AVL remove or one `extend`) — crash-safe by
    /// construction.
    fn alloc_bulk(&self, lengths: impl AsRef<[u64]>) -> io::Result<Vec<BStackSlice<'_, Self>>> {
        let lengths = lengths.as_ref();
        if lengths.is_empty() {
            return Ok(Vec::new());
        }

        let aligned: Vec<u64> = lengths
            .iter()
            .map(|&l| if l == 0 { 0 } else { Self::align_up_len(l) })
            .collect();

        let total = aligned
            .iter()
            .copied()
            .try_fold(0u64, |acc, a| acc.checked_add(a))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "alloc_bulk: total size overflows u64",
                )
            })?;

        // All zero-length: return null slices without touching the BStack.
        if total == 0 {
            return Ok(lengths
                .iter()
                .map(|_| BStackSlice::new(self, 0, 0))
                .collect());
        }

        // Allocate one contiguous block.  `total` is already a sum of multiples
        // of MIN_ALLOC so no further rounding is needed.
        let block_ptr = if let Some((ptr, _)) = self.avl_find_best_fit_and_remove(total)? {
            // Zero the stale AVL node header; the rest is already zeroed by invariant.
            self.stack.zero(ptr, MIN_ALLOC)?;
            ptr
        } else {
            self.stack.extend(total)?
        };

        // Build per-request slices from the contiguous block.
        let mut result = Vec::with_capacity(lengths.len());
        let mut offset = 0u64;
        for (&len, &al) in lengths.iter().zip(aligned.iter()) {
            if len == 0 {
                result.push(BStackSlice::new(self, 0, 0));
            } else {
                result.push(BStackSlice::new(self, block_ptr + offset, len));
                offset += al;
            }
        }
        Ok(result)
    }

    /// Deallocate multiple slices, merging contiguous ones before freeing.
    ///
    /// Slices are sorted by address and adjacent slices (whose aligned extents
    /// are immediately contiguous) are merged into a single free block.  This
    /// means a set of slices returned by [`alloc_bulk`](Self::alloc_bulk) is
    /// freed in a single operation when given back together.
    fn dealloc_bulk<'a>(&'a self, slices: impl AsRef<[BStackSlice<'a, Self>]>) -> io::Result<()> {
        let slices = slices.as_ref();

        // Collect, validate, and convert to (ptr, aligned_size) pairs.
        let mut entries: Vec<(u64, u64)> = Vec::new();
        for s in slices {
            if s.is_empty() {
                continue;
            }
            if s.start() < ARENA_START || s.start() != Self::align_up_ptr(s.start()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "dealloc_bulk: invalid slice origin",
                ));
            }
            entries.push((s.start(), Self::align_up_len(s.len())));
        }

        if entries.is_empty() {
            return Ok(());
        }

        // Sort by address so adjacent slices are neighbours.
        entries.sort_by_key(|&(ptr, _)| ptr);

        // Merge contiguous (ptr, size) pairs into combined blocks.
        let mut merged: Vec<(u64, u64)> = Vec::new();
        for (ptr, size) in entries {
            if let Some(last) = merged.last_mut()
                && last.0 + last.1 == ptr
            {
                last.1 += size;
            } else {
                merged.push((ptr, size));
            }
        }

        // Free each merged block: tail-truncate when possible, otherwise zero + insert.
        for (ptr, size) in merged {
            if ptr + size == self.stack.len()? {
                self.stack.discard(size)?;
            } else {
                self.stack.zero(ptr, size)?;
                self.avl_insert(ptr, size)?;
            }
        }
        Ok(())
    }
}
