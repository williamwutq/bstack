use super::{BStackAllocator, BStackBulkAllocator, BStackSlice};
use crate::BStack;
#[cfg(not(feature = "atomic"))]
use std::cell::Cell;
use std::fmt;
use std::io;
#[cfg(not(feature = "atomic"))]
use std::marker::PhantomData;
#[cfg(feature = "atomic")]
use std::sync::Mutex;

const ALGT_MAGIC: [u8; 8] = *b"ALGT\x00\x01\x01\x00";
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

/// Maximum recursion depth for AVL tree operations.  A balanced AVL tree never
/// exceeds ~60 levels for any realistic arena size, so 128 gives ample headroom
/// for slightly-imbalanced-but-valid trees while reliably catching cycles left
/// by a partial rotation crash.
const MAX_AVL_DEPTH: u32 = 128;

// Node offsets within a free block (AVL node header fields).
const NODE_SIZE_OFF: u64 = 0;
const NODE_BF_OFF: u64 = 8; // i8 balance factor
const NODE_HEIGHT_OFF: u64 = 9; // u8 height (max ~59 for balanced; slightly more tolerated)
const NODE_LEFT_OFF: u64 = 16;
const NODE_RIGHT_OFF: u64 = 24;

/// A node visited during a downward AVL tree traversal.
///
/// Used by [`avl_insert`](GhostTreeBstackAllocator::avl_insert) and
/// [`avl_find_best_fit_and_remove`](GhostTreeBstackAllocator::avl_find_best_fit_and_remove)
/// to record the path so that balance factors and heights can be updated on
/// the way back up without recursion.
struct PathEntry {
    ptr: u64,
    size: u64,
    left: u64,
    right: u64,
    went_left: bool,
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
/// | `alloc_bulk`            | one block for the combined size, then split       | multi-call |
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
/// │   Magic number (8 bytes)    │  "ALGT\x00\x01\x01\x00"
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
/// No write-ahead log, no checksums.  All multi-call paths can produce space
/// leaks on crash; **user data in live allocations is never lost**.  Specific
/// known leak windows:
///
/// * **`dealloc` (non-tail):** a crash after `zero` but before the AVL insert
///   permanently discards that free block.  The bytes are zeroed but the tree
///   has no entry for them.
/// * **`realloc` grow (non-tail):** a crash after `alloc(new)` but before
///   `dealloc(old)` leaves both the new and old blocks unreachable.  The caller's
///   original data is still intact in `old`, but neither handle is recoverable.
/// * **`realloc` shrink (non-tail):** a crash after `zero` but before the AVL
///   insert for the freed tail fragment leaks that fragment.
/// * **`alloc` split:** a crash after `avl_find_best_fit_and_remove` but before
///   `avl_insert(remainder)` leaks the entire found block.
/// * **Torn AVL rotation:** if the process crashes between the two `write_node`
///   calls of a rotation, the subtree rooted at `pivot` becomes unreachable from
///   the tree root.  `coalesce_and_rebalance` (run on every open) only walks
///   reachable nodes, so the orphaned subtree is a **permanent space leak**.
///   Since the orphaned nodes are free blocks (no live user data), no user data
///   is lost — only allocatable space.  A linear arena scan would recover them
///   but GhostTree carries no per-block `is_free` flag, making such a scan
///   unreliable; the leak is therefore accepted by design.
///
/// # Thread safety
///
/// `GhostTreeBstackAllocator` is always **`Send`** — ownership can be
/// transferred to another thread.
///
/// Without the `atomic` feature it is **not `Sync`**: all allocator operations
/// take `&self` and mutate the on-disk AVL tree through `BStack`, so concurrent
/// shared access from multiple threads would race on that state.  Each instance
/// must be used from at most one thread at a time.
///
/// With the `atomic` feature it **is `Sync`**.  An internal [`Mutex`] serialises
/// all AVL tree mutations and tail-stack operations that are not already
/// serialised by `BStack`'s own locking.
///
/// ```
/// fn assert_send<T: Send>() {}
/// assert_send::<bstack::GhostTreeBstackAllocator>();
/// ```
///
/// Without `atomic` the type is `!Sync` (this fails to compile); with `atomic`
/// the internal `Mutex` makes it `Sync` (this compiles):
///
#[cfg_attr(not(feature = "atomic"), doc = "```compile_fail")]
#[cfg_attr(feature = "atomic", doc = "```")]
/// fn assert_sync<T: Sync>() {}
/// assert_sync::<bstack::GhostTreeBstackAllocator>();
/// ```
pub struct GhostTreeBstackAllocator {
    stack: BStack,
    #[cfg(feature = "atomic")]
    lock: Mutex<()>,
    #[cfg(not(feature = "atomic"))]
    _not_sync: PhantomData<Cell<()>>,
}

impl fmt::Debug for GhostTreeBstackAllocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GhostTreeBstackAllocator")
            .field("stack", &self.stack)
            .finish_non_exhaustive()
    }
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
            return Ok(Self {
                stack,
                #[cfg(feature = "atomic")]
                lock: Mutex::new(()),
                #[cfg(not(feature = "atomic"))]
                _not_sync: PhantomData,
            });
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

        let this = Self {
            stack,
            #[cfg(feature = "atomic")]
            lock: Mutex::new(()),
            #[cfg(not(feature = "atomic"))]
            _not_sync: PhantomData,
        };
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

    /// Insert a free block at `ptr` with `size` bytes into the AVL tree.
    fn avl_insert(&self, ptr: u64, size: u64) -> io::Result<()> {
        let root = self.read_root()?;

        // Down-pass: walk to the insertion position, recording the path.
        let mut path: Vec<PathEntry> = Vec::with_capacity(MAX_AVL_DEPTH as usize);
        let mut current = root;
        while current != NULL_PTR {
            if path.len() >= MAX_AVL_DEPTH as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "AVL insert exceeded maximum depth: corrupted tree (possible cycle)",
                ));
            }
            let (root_sz, _, _, left, right) = self.read_node(current)?;
            let went_left = (size, ptr) < (root_sz, current);
            path.push(PathEntry {
                ptr: current,
                size: root_sz,
                left,
                right,
                went_left,
            });
            current = if went_left { left } else { right };
        }

        // Write the new leaf.
        self.write_node(ptr, size, 0, 1, NULL_PTR, NULL_PTR)?;

        // Up-pass: propagate the new child pointer and rebalance each ancestor.
        let mut child = ptr;
        for entry in path.iter().rev() {
            let (new_left, new_right) = if entry.went_left {
                (child, entry.right)
            } else {
                (entry.left, child)
            };
            self.avl_write_and_update(entry.ptr, entry.size, new_left, new_right)?;
            child = self.avl_rebalance(entry.ptr)?;
        }
        self.write_root(child)
    }

    /// Remove the minimum-key (leftmost) node from the subtree rooted at `root`,
    /// rebalancing the path back up.
    ///
    /// Returns `(min_ptr, min_size, new_subtree_root)`.  The minimum node always
    /// has no left child, so its replacement is its right child (or [`NULL_PTR`]).
    fn avl_remove_min(&self, root: u64) -> io::Result<(u64, u64, u64)> {
        // Walk left, recording (ptr, size, right_child) for each ancestor.
        let mut path: Vec<(u64, u64, u64)> = Vec::with_capacity(MAX_AVL_DEPTH as usize);
        let mut current = root;
        loop {
            let (size, _, _, left, right) = self.read_node(current)?;
            if left == NULL_PTR {
                // `current` is the minimum; replace it with its right child.
                let mut child = right;
                for &(anc_ptr, anc_sz, anc_right) in path.iter().rev() {
                    self.avl_write_and_update(anc_ptr, anc_sz, child, anc_right)?;
                    child = self.avl_rebalance(anc_ptr)?;
                }
                return Ok((current, size, child));
            }
            if path.len() >= MAX_AVL_DEPTH as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "AVL min exceeded maximum depth: corrupted tree (possible cycle)",
                ));
            }
            path.push((current, size, right));
            current = left;
        }
    }

    /// Find and remove the best-fit block (smallest block ≥ `min_size`).
    ///
    /// Returns `(ptr, size)`, or `None` if no block fits.
    ///
    /// Strategy: when the current node fits, go left to try to find a smaller
    /// fit.  The best fit is the last fitting node encountered before the
    /// traversal exhausts the left subtree.  Path entries after that index
    /// searched the best-fit node's left subtree and found nothing — they
    /// require no updates.
    fn avl_find_best_fit_and_remove(&self, min_size: u64) -> io::Result<Option<(u64, u64)>> {
        let root = self.read_root()?;
        if root == NULL_PTR {
            return Ok(None);
        }

        // Down-pass: record the full traversal path and the index of the last
        // node that satisfies size >= min_size (the best fit).
        let mut path: Vec<PathEntry> = Vec::with_capacity(MAX_AVL_DEPTH as usize);
        let mut last_fit_idx: Option<usize> = None;
        let mut current = root;
        while current != NULL_PTR {
            if path.len() >= MAX_AVL_DEPTH as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "AVL find exceeded maximum depth: corrupted tree (possible cycle)",
                ));
            }
            let (root_sz, _, _, left, right) = self.read_node(current)?;
            if root_sz >= min_size {
                last_fit_idx = Some(path.len());
                path.push(PathEntry {
                    ptr: current,
                    size: root_sz,
                    left,
                    right,
                    went_left: true,
                });
                current = left;
            } else {
                path.push(PathEntry {
                    ptr: current,
                    size: root_sz,
                    left,
                    right,
                    went_left: false,
                });
                current = right;
            }
        }

        let fit_idx = match last_fit_idx {
            None => return Ok(None),
            Some(i) => i,
        };

        let found_ptr = path[fit_idx].ptr;
        let found_size = path[fit_idx].size;
        let found_left = path[fit_idx].left;
        let found_right = path[fit_idx].right;

        // Remove the best-fit node.  The left subtree (path[fit_idx+1..]) was
        // searched and yielded nothing, so found_left is returned unchanged.
        let replacement = if found_left == NULL_PTR {
            found_right
        } else if found_right == NULL_PTR {
            found_left
        } else {
            // Two children: replace with in-order successor (min of right subtree).
            let (succ, succ_sz, new_right) = self.avl_remove_min(found_right)?;
            self.avl_write_and_update(succ, succ_sz, found_left, new_right)?;
            self.avl_rebalance(succ)?
        };

        // Up-pass: update path[0..fit_idx] (path[fit_idx] was removed).
        let mut child = replacement;
        for entry in path[..fit_idx].iter().rev() {
            let (new_left, new_right) = if entry.went_left {
                (child, entry.right)
            } else {
                (entry.left, child)
            };
            self.avl_write_and_update(entry.ptr, entry.size, new_left, new_right)?;
            child = self.avl_rebalance(entry.ptr)?;
        }
        self.write_root(child)?;
        Ok(Some((found_ptr, found_size)))
    }

    /// In-order walk of the subtree at `root`, calling `f(ptr, size)` per node.
    /// Tolerates imbalance — visits every reachable node.  Returns `InvalidData`
    /// if the traversal stack exceeds [`MAX_AVL_DEPTH`] (cycle guard).
    fn avl_walk_inorder(
        &self,
        root: u64,
        f: &mut dyn FnMut(u64, u64) -> io::Result<()>,
    ) -> io::Result<()> {
        // Each stack entry is `(ptr, right_child, size)` for a node whose left
        // subtree is currently being visited.
        let mut stack: Vec<(u64, u64, u64)> = Vec::new();
        let mut current = root;
        loop {
            // Descend left, pushing nodes onto the stack.
            while current != NULL_PTR {
                if stack.len() >= MAX_AVL_DEPTH as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "AVL walk exceeded maximum depth: corrupted tree (possible cycle)",
                    ));
                }
                let (size, _, _, left, right) = self.read_node(current)?;
                stack.push((current, right, size));
                current = left;
            }
            // Pop and visit; then follow the right child.
            match stack.pop() {
                None => return Ok(()),
                Some((ptr, right, size)) => {
                    f(ptr, size)?;
                    current = right;
                }
            }
        }
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

        // Step 2: sort by address and deduplicate by ptr.  A partial rotation
        // crash can leave a node reachable from two parents; the in-order walk
        // would visit it twice.  Without dedup the rebuild would write the same
        // AVL node twice, clobbering child pointers written by the first pass.
        blocks.sort_by_key(|&(ptr, _)| ptr);
        blocks.dedup_by_key(|b| b.0);

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
        // so the build produces a valid BST.  Without this, insert/remove would
        // navigate by (size, ptr) into an address-ordered tree and miss nodes.
        coalesced.sort_by_key(|&(ptr, size)| (size, ptr));

        // Iterative balanced BST build using an explicit ops stack.
        //   Enter(lo, hi) — process range [lo, hi): push Combine(mid), then
        //     Enter(mid+1, hi), then Enter(lo, mid) (reverse order so left
        //     executes first).
        //   Combine(i) — pop right_root then left_root from results, write
        //     coalesced[i] as a node, push its ptr onto results.
        enum BuildOp {
            Enter(usize, usize),
            Combine(usize),
        }
        let mut ops: Vec<BuildOp> = vec![BuildOp::Enter(0, coalesced.len())];
        let mut results: Vec<u64> = Vec::new();
        while let Some(op) = ops.pop() {
            match op {
                BuildOp::Enter(lo, hi) => {
                    if lo >= hi {
                        results.push(NULL_PTR);
                    } else {
                        let mid = lo + (hi - lo) / 2;
                        ops.push(BuildOp::Combine(mid));
                        ops.push(BuildOp::Enter(mid + 1, hi));
                        ops.push(BuildOp::Enter(lo, mid));
                    }
                }
                BuildOp::Combine(i) => {
                    let right_root = results.pop().unwrap();
                    let left_root = results.pop().unwrap();
                    let (ptr, size) = coalesced[i];
                    self.avl_write_and_update(ptr, size, left_root, right_root)?;
                    results.push(ptr);
                }
            }
        }
        let new_root = results
            .pop()
            .expect("build invariant: results must have exactly one element");
        debug_assert!(
            results.is_empty(),
            "build invariant: excess elements on results stack"
        );
        self.write_root(new_root)
    }
}

impl BStackAllocator for GhostTreeBstackAllocator {
    type Error = io::Error;
    type Allocated<'a> = BStackSlice<'a, Self>;

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
            // SAFETY: zero-length slice at offset 0 is safe
            return Ok(unsafe { BStackSlice::from_raw_parts(self, 0, 0) });
        }
        let aligned = Self::align_up_len(len);
        {
            #[cfg(feature = "atomic")]
            let guard = self.lock.lock().unwrap();
            if let Some((ptr, block_size)) = self.avl_find_best_fit_and_remove(aligned)? {
                let remainder = block_size - aligned;
                if remainder >= MIN_ALLOC {
                    // Split: the leading `remainder` bytes become a new free block.
                    // The AVL node is written into those bytes by avl_insert.
                    // The tail `aligned` bytes are already zeroed by invariant.
                    self.avl_insert(ptr, remainder)?;
                    // SAFETY: ptr + remainder is the allocated portion after splitting
                    return Ok(unsafe { BStackSlice::from_raw_parts(self, ptr + remainder, len) });
                } else {
                    #[cfg(feature = "atomic")]
                    drop(guard);
                    // No split: give the whole block.  The stale AVL node in the
                    // first 32 bytes must be zeroed; the rest is already zeroed.
                    // Any bytes beyond `len` (up to `block_size`) are internal
                    // padding and will be recovered on dealloc by re-aligning.
                    self.stack.zero(ptr, MIN_ALLOC)?;
                    // SAFETY: ptr from allocated block via avl_find_best_fit_and_remove
                    return Ok(unsafe { BStackSlice::from_raw_parts(self, ptr, len) });
                }
            }
        }
        // No free block fits: lock released; grow the BStack (returns zeroed bytes).
        let start = self.stack.extend(aligned)?;
        // SAFETY: start from fresh allocation via self.stack.extend
        Ok(unsafe { BStackSlice::from_raw_parts(self, start, len) })
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
            // SAFETY: zero-length slice at offset 0 is safe
            return Ok(unsafe { BStackSlice::from_raw_parts(self, 0, 0) });
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
            // SAFETY: same block, just changing visible length
            return Ok(unsafe { BStackSlice::from_raw_parts(self, slice.start(), new_len) });
        }

        if aligned_new < aligned_old {
            // Shrink.
            let freed_tail = aligned_old - aligned_new;
            let tail_ptr = slice.start() + aligned_new;

            // Atomic fast path: discard the tail block without taking the lock.
            #[cfg(feature = "atomic")]
            if self
                .stack
                .try_discard(slice.start() + aligned_old, freed_tail)?
            {
                if new_len < aligned_new {
                    self.stack
                        .zero(slice.start() + new_len, aligned_new - new_len)?;
                }
                return Ok(unsafe { BStackSlice::from_raw_parts(self, slice.start(), new_len) });
            }

            #[cfg(not(feature = "atomic"))]
            if slice.start() + aligned_old == self.stack.len()? {
                if new_len < aligned_new {
                    self.stack
                        .zero(slice.start() + new_len, aligned_new - new_len)?;
                }
                self.stack.discard(freed_tail)?;
                return Ok(unsafe { BStackSlice::from_raw_parts(self, slice.start(), new_len) });
            }

            // Not tail: zero gap + freed tail before taking the lock, then insert.
            self.stack
                .zero(slice.start() + new_len, aligned_old - new_len)?;
            #[cfg(feature = "atomic")]
            let _guard = self.lock.lock().unwrap();
            self.avl_insert(tail_ptr, freed_tail)?;
            return Ok(unsafe { BStackSlice::from_raw_parts(self, slice.start(), new_len) });
        }

        // Grow path.
        // Atomic fast path: extend the tail without taking the lock.
        #[cfg(feature = "atomic")]
        if self
            .stack
            .try_extend_zeros(slice.start() + aligned_old, aligned_new - aligned_old)?
        {
            return Ok(unsafe { BStackSlice::from_raw_parts(self, slice.start(), new_len) });
        }

        #[cfg(not(feature = "atomic"))]
        if slice.start() + aligned_old == self.stack.len()? {
            self.stack.extend(aligned_new - aligned_old)?;
            return Ok(unsafe { BStackSlice::from_raw_parts(self, slice.start(), new_len) });
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
        let true_len = Self::align_up_len(slice.len());

        // Atomic fast path: discard the tail block without taking the lock.
        // try_discard succeeds only if the stack size is still ptr + true_len,
        // making the check-and-discard atomic w.r.t. other threads' pushes.
        // If it fails the block is no longer at the tail; fall through to insert.
        #[cfg(feature = "atomic")]
        if self.stack.try_discard(ptr + true_len, true_len)? {
            return Ok(());
        }

        // Tail optimisation: truncate instead of recycling through the AVL tree.
        #[cfg(not(feature = "atomic"))]
        if ptr + true_len == self.stack.len()? {
            return self.stack.discard(true_len);
        }

        // Note: GhostTree carries no per-block is_free flag and stores no block
        // headers for live allocations, so reliable double-free detection is not
        // possible without false-positives on ordinary user data.

        // Zero before taking the lock: the block is owned by the caller and no
        // other thread will touch it until it appears in the AVL tree.
        self.stack.zero(ptr, true_len)?;
        #[cfg(feature = "atomic")]
        let _guard = self.lock.lock().unwrap();
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
    fn alloc_bulk(
        &self,
        lengths: impl AsRef<[u64]>,
    ) -> Result<Vec<Self::Allocated<'_>>, Self::Error> {
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
                // SAFETY: zero-length slices are safe
                .map(|_| unsafe { BStackSlice::from_raw_parts(self, 0, 0) })
                .collect());
        }

        // Allocate one contiguous block.  `total` is already a sum of multiples
        // of MIN_ALLOC so no further rounding is needed.  The lock is released
        // before extend and before building per-request slices.
        let block_ptr = {
            #[cfg(feature = "atomic")]
            let guard = self.lock.lock().unwrap();
            if let Some((ptr, block_size)) = self.avl_find_best_fit_and_remove(total)? {
                let remainder = block_size - total;
                if remainder >= MIN_ALLOC {
                    // Split: recycle the leading remainder as a new free block,
                    // use the trailing `total` bytes for the allocation.
                    self.avl_insert(ptr, remainder)?;
                    ptr + remainder
                } else {
                    #[cfg(feature = "atomic")]
                    drop(guard); // release lock before zeroing
                    // No split: zero the stale AVL node header; rest already zeroed.
                    self.stack.zero(ptr, MIN_ALLOC)?;
                    ptr
                }
            } else {
                NULL_PTR // sentinel: no free block found, extend after lock is released
            }
        };
        let block_ptr = if block_ptr == NULL_PTR {
            self.stack.extend(total)?
        } else {
            block_ptr
        };

        // Build per-request slices from the contiguous block.
        let mut result = Vec::with_capacity(lengths.len());
        let mut offset = 0u64;
        for (&len, &al) in lengths.iter().zip(aligned.iter()) {
            if len == 0 {
                // SAFETY: zero-length slice is safe
                result.push(unsafe { BStackSlice::from_raw_parts(self, 0, 0) });
            } else {
                // SAFETY: block_ptr + offset is within the bulk allocated block
                result.push(unsafe { BStackSlice::from_raw_parts(self, block_ptr + offset, len) });
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
    fn dealloc_bulk<'a>(
        &'a self,
        slices: impl AsRef<[Self::Allocated<'a>]>,
    ) -> Result<(), Self::Error> {
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

        // Free each merged block.  The highest-address block may be at the tail;
        // attempt a lock-free discard on it first.  All remaining blocks are
        // zeroed outside the lock (each is owned by the caller), then inserted
        // into the AVL tree under the lock in one pass.

        let last = merged.pop().unwrap(); // highest-address block (merged is sorted)

        // Attempt tail-discard on the highest-address block.
        let last_discarded;
        #[cfg(feature = "atomic")]
        {
            last_discarded = self.stack.try_discard(last.0 + last.1, last.1)?;
        }
        #[cfg(not(feature = "atomic"))]
        {
            if last.0 + last.1 == self.stack.len()? {
                self.stack.discard(last.1)?;
                last_discarded = true;
            } else {
                last_discarded = false;
            }
        }

        if !last_discarded {
            merged.push(last);
        }

        // Zero all blocks to be inserted (outside the lock).
        for &(ptr, size) in &merged {
            self.stack.zero(ptr, size)?;
        }

        // Insert all zeroed blocks under the lock.
        if !merged.is_empty() {
            #[cfg(feature = "atomic")]
            let _guard = self.lock.lock().unwrap();
            for (ptr, size) in merged {
                self.avl_insert(ptr, size)?;
            }
        }
        Ok(())
    }
}
