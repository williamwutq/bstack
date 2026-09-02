use super::{
    BStackAllocator, BStackBulkAllocator, BStackSlice, ensure_own_slice, ensure_own_slices,
};
use crate::BStack;
#[cfg(not(feature = "atomic"))]
use std::cell::Cell;
use std::fmt;
use std::io;
#[cfg(not(feature = "atomic"))]
use std::marker::PhantomData;
#[cfg(feature = "atomic")]
use std::sync::Mutex;

const ALGT_MAGIC: [u8; 8] = *b"ALGT\x00\x01\x04\x00";
const ALGT_MAGIC_PREFIX: [u8; 6] = *b"ALGT\x00\x01";

/// Payload offset of the magic number.
const MAGIC_OFFSET: u64 = 32;
/// Payload offset of the AVL root pointer.
const ROOT_OFFSET: u64 = 40;
/// First payload offset managed by the allocator (32-byte aligned on disk).
const ARENA_START: u64 = 48;

/// Minimum allocation size — exactly the size of one AVL node.
const MIN_ALLOC: u64 = 32;

/// Largest length [`GhostTreeBstackAllocator::align_up_len`] will round up.
///
/// Anything larger is rejected as [`io::ErrorKind::InvalidInput`] rather than
/// wrapped. Unchecked, `len + 31` wraps a near-`u64::MAX` request down to a
/// 32-byte block, which `alloc` would hand back as a handle claiming the
/// original length, and which `realloc` would carry into the tail-shrink path
/// as an underflowed padding size.
const MAX_ALLOC: u64 = (u64::MAX - ARENA_START) & !31;

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
// Cached child heights, denormalized so a parent read during a down-pass yields
// the height of its untouched (sibling) child without a separate read of that
// child on the way back up.  Maintained by every node write; rebuilt from
// scratch by `coalesce_and_rebalance` on open, so old-format arenas (whose
// reserved bytes were zero) self-upgrade transparently.
const NODE_LH_OFF: u64 = 10; // u8 cached height of the left child
const NODE_RH_OFF: u64 = 11; // u8 cached height of the right child
const NODE_LEFT_OFF: u64 = 16;
const NODE_RIGHT_OFF: u64 = 24;

/// A node visited during a downward AVL tree traversal.
///
/// Used by [`avl_insert`](GhostTreeBstackAllocator::avl_insert) and
/// [`avl_find_best_fit_and_remove`](GhostTreeBstackAllocator::avl_find_best_fit_and_remove)
/// to record the path so that balance factors and heights can be updated on
/// the way back up without recursion.  Recorded on a fixed-size stack array of
/// [`MAX_AVL_DEPTH`] entries, so it must be `Copy` and cheaply default-able.
#[derive(Clone, Copy, Default)]
struct PathEntry {
    ptr: u64,
    size: u64,
    left: u64,
    right: u64,
    went_left: bool,
    /// This node's cached child heights, read from its denormalized cache during
    /// the down-pass.  The sibling (untouched) child's height is stable across
    /// the operation — nothing off the path is modified — so the up-pass writes
    /// this node with both child heights known and reads neither.
    lh: u8,
    rh: u8,
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
/// │   Magic number (8 bytes)    │  "ALGT\x00\x01\x04\x00"
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

/// `Sync` is removed deliberately by `_not_sync`; `RefUnwindSafe` is collateral.
#[cfg(not(feature = "atomic"))]
impl std::panic::RefUnwindSafe for GhostTreeBstackAllocator {}

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

    /// Read the node at `ptr` for a down-pass, returning `(size, left, right,
    /// lh, rh)` where `lh`/`rh` are the node's cached child heights.  Same single
    /// `get` as [`read_node`](Self::read_node); the cache lets the up-pass skip
    /// re-reading the untouched sibling child.
    fn read_node_hc(&self, ptr: u64) -> io::Result<(u64, u64, u64, u8, u8)> {
        let buf = &mut [0u8; 32];
        self.stack.get_into(ptr, buf)?;
        let size = read_buf_le!(buf, NODE_SIZE_OFF  => u64);
        let left = read_buf_le!(buf, NODE_LEFT_OFF  => u64);
        let right = read_buf_le!(buf, NODE_RIGHT_OFF => u64);
        let lh = read_buf_le!(buf, NODE_LH_OFF     => u8);
        let rh = read_buf_le!(buf, NODE_RH_OFF     => u8);
        Ok((size, left, right, lh, rh))
    }

    /// Write a complete AVL node at `ptr`, including the denormalized child-height
    /// cache (`lh`, `rh` = heights of `left`, `right`).
    #[allow(clippy::too_many_arguments)] // one serialization site; grouping into a struct would not aid clarity
    fn write_node(
        &self,
        ptr: u64,
        size: u64,
        bf: i8,
        height: u8,
        lh: u8,
        rh: u8,
        left: u64,
        right: u64,
    ) -> io::Result<()> {
        let mut buf = [0u8; 32];
        write_buf!(size   => buf, NODE_SIZE_OFF);
        write_buf!(bf     => buf, NODE_BF_OFF);
        write_buf!(height => buf, NODE_HEIGHT_OFF);
        write_buf!(lh     => buf, NODE_LH_OFF);
        write_buf!(rh     => buf, NODE_RH_OFF);
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
    ///
    /// Returns `None` if `len` exceeds [`MAX_ALLOC`], i.e. if the round-up
    /// would overflow `u64`; every caller turns that into
    /// [`io::ErrorKind::InvalidInput`].
    #[inline]
    fn align_up_len(len: u64) -> Option<u64> {
        if len > MAX_ALLOC {
            return None;
        }
        Some(((len + 31) & !31).max(MIN_ALLOC))
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

    /// Write `(size, left, right)` to `ptr`, computing bf and height in one pass,
    /// and return `(bf, height)`.
    ///
    /// A child's height passed as `Some` is used directly — the caller already
    /// knows it, e.g. from the node written in the previous up-pass step or from
    /// a sibling untouched by a rotation — which avoids a `get_into` (lock +
    /// syscall) to re-read that child.  `None` reads the height from the child.
    #[inline]
    fn avl_write_h(
        &self,
        ptr: u64,
        size: u64,
        left: u64,
        right: u64,
        lh: Option<u8>,
        rh: Option<u8>,
    ) -> io::Result<(i8, u8)> {
        let lh = match lh {
            Some(h) => h as i16,
            None => self.avl_height(left)? as i16,
        };
        let rh = match rh {
            Some(h) => h as i16,
            None => self.avl_height(right)? as i16,
        };
        let bf = (rh - lh) as i8;
        let height = (1 + lh.max(rh)) as u8;
        self.write_node(ptr, size, bf, height, lh as u8, rh as u8, left, right)?;
        Ok((bf, height))
    }

    /// Write `(size, left, right)` to `ptr`, reading both child heights, and
    /// return `(bf, height)`.  Thin wrapper over [`avl_write_h`](Self::avl_write_h).
    #[inline]
    fn avl_write_and_update(
        &self,
        ptr: u64,
        size: u64,
        left: u64,
        right: u64,
    ) -> io::Result<(i8, u8)> {
        self.avl_write_h(ptr, size, left, right, None, None)
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
    fn avl_rotate_right(&self, node: u64) -> io::Result<(u64, u8)> {
        // Read both nodes' cached child heights so neither rewrite re-reads a
        // child.  (`node`'s left child is `pivot`, whose height is not needed.)
        let (node_sz, pivot, node_r, _node_lh, node_rh) = self.read_node_hc(node)?;
        let (pivot_sz, pivot_l, pivot_r, pivot_lh, pivot_rh) = self.read_node_hc(pivot)?;
        // `node`'s new children (pivot_r, node_r) → heights (pivot_rh, node_rh).
        let (_, node_h) = self.avl_write_h(
            node,
            node_sz,
            pivot_r,
            node_r,
            Some(pivot_rh),
            Some(node_rh),
        )?;
        // `pivot`'s new children (pivot_l, node) → heights (pivot_lh, node_h).
        let (_, pivot_h) =
            self.avl_write_h(pivot, pivot_sz, pivot_l, node, Some(pivot_lh), Some(node_h))?;
        Ok((pivot, pivot_h))
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
    fn avl_rotate_left(&self, node: u64) -> io::Result<(u64, u8)> {
        // Read both nodes' cached child heights so neither rewrite re-reads a
        // child.  (`node`'s right child is `pivot`, whose height is not needed.)
        let (node_sz, node_l, pivot, node_lh, _node_rh) = self.read_node_hc(node)?;
        let (pivot_sz, pivot_l, pivot_r, pivot_lh, pivot_rh) = self.read_node_hc(pivot)?;
        // `node`'s new children (node_l, pivot_l) → heights (node_lh, pivot_lh).
        let (_, node_h) = self.avl_write_h(
            node,
            node_sz,
            node_l,
            pivot_l,
            Some(node_lh),
            Some(pivot_lh),
        )?;
        // `pivot`'s new children (node, pivot_r) → heights (node_h, pivot_rh).
        let (_, pivot_h) =
            self.avl_write_h(pivot, pivot_sz, node, pivot_r, Some(node_h), Some(pivot_rh))?;
        Ok((pivot, pivot_h))
    }

    /// Fix imbalance at `node` after an insert or remove, then return the
    /// (possibly new) subtree root and its height.  Children must already be
    /// balanced.
    ///
    /// The caller passes the `bf` and `height` already computed by the
    /// [`avl_write_h`](Self::avl_write_h) that installed `node`'s current
    /// children, so the common in-balance case needs no further I/O — it just
    /// returns `(node, height)`.
    ///
    /// Uses `< -1` / `> 1` rather than `== -2` / `== 2` so that a node whose
    /// balance factor exceeds ±2 (possible after crash recovery) still gets
    /// corrected instead of silently passed over.
    fn avl_rebalance(&self, node: u64, bf: i8, height: u8) -> io::Result<(u64, u8)> {
        if bf < -1 {
            let (_, _, _, left, _) = self.read_node(node)?;
            let (_, left_bf, _, _, _) = self.read_node(left)?;
            if left_bf > 0 {
                // Left-right case: rotate left child left first.
                let (new_left, _) = self.avl_rotate_left(left)?;
                let (node_sz, _, _, _, node_r) = self.read_node(node)?;
                self.avl_write_and_update(node, node_sz, new_left, node_r)?;
            }
            self.avl_rotate_right(node)
        } else if bf > 1 {
            let (_, _, _, _, right) = self.read_node(node)?;
            let (_, right_bf, _, _, _) = self.read_node(right)?;
            if right_bf < 0 {
                // Right-left case: rotate right child right first.
                let (new_right, _) = self.avl_rotate_right(right)?;
                let (node_sz, _, _, node_l, _) = self.read_node(node)?;
                self.avl_write_and_update(node, node_sz, node_l, new_right)?;
            }
            self.avl_rotate_left(node)
        } else {
            Ok((node, height))
        }
    }

    /// Insert a free block at `ptr` with `size` bytes into the AVL tree.
    fn avl_insert(&self, ptr: u64, size: u64) -> io::Result<()> {
        let root = self.read_root()?;

        // Down-pass: walk to the insertion position, recording the path on a
        // fixed-size stack array.  `MAX_AVL_DEPTH` is a compile-time bound, so
        // this avoids a heap allocation on every insert under the mutex.
        let mut path = [PathEntry::default(); MAX_AVL_DEPTH as usize];
        let mut path_len = 0usize;
        let mut current = root;
        while current != NULL_PTR {
            if path_len >= MAX_AVL_DEPTH as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "AVL insert exceeded maximum depth: corrupted tree (possible cycle)",
                ));
            }
            let (root_sz, left, right, lh, rh) = self.read_node_hc(current)?;
            let went_left = (size, ptr) < (root_sz, current);
            path[path_len] = PathEntry {
                ptr: current,
                size: root_sz,
                left,
                right,
                went_left,
                lh,
                rh,
            };
            path_len += 1;
            current = if went_left { left } else { right };
        }

        // Write the new leaf (height 1, null children → cached child heights 0).
        self.write_node(ptr, size, 0, 1, 0, 0, NULL_PTR, NULL_PTR)?;

        // Up-pass: install the new child pointer in each ancestor and rebalance.
        // Both child heights are known — the modified child from the previous
        // iteration, the untouched sibling from this node's cache read on the
        // way down — so `avl_write_h` reads neither.  The `(bf, height)` it
        // returns is handed to `avl_rebalance`, so the in-balance case does no
        // further I/O: one write per level, zero reads.
        let mut child = ptr;
        let mut child_h = 1u8; // leaf height
        for entry in path[..path_len].iter().rev() {
            let (new_left, new_right, lh, rh) = if entry.went_left {
                (child, entry.right, Some(child_h), Some(entry.rh))
            } else {
                (entry.left, child, Some(entry.lh), Some(child_h))
            };
            let (bf, h) = self.avl_write_h(entry.ptr, entry.size, new_left, new_right, lh, rh)?;
            let (new_root, new_h) = self.avl_rebalance(entry.ptr, bf, h)?;
            child = new_root;
            child_h = new_h;
        }
        self.write_root(child)
    }

    /// Remove the minimum-key (leftmost) node from the subtree rooted at `root`,
    /// rebalancing the path back up.
    ///
    /// Returns `(min_ptr, min_size, new_subtree_root)`.  The minimum node always
    /// has no left child, so its replacement is its right child (or [`NULL_PTR`]).
    fn avl_remove_min(&self, root: u64) -> io::Result<(u64, u64, u64)> {
        // Walk left, recording (ptr, size, right_child, right_child_height) for
        // each ancestor on a fixed-size stack array (no heap allocation under the
        // mutex).  The cached right-child height lets the up-pass write each
        // ancestor with both child heights known.
        let mut path = [(0u64, 0u64, 0u64, 0u8); MAX_AVL_DEPTH as usize];
        let mut path_len = 0usize;
        let mut current = root;
        loop {
            let (size, left, right, _lh, rh) = self.read_node_hc(current)?;
            if left == NULL_PTR {
                // `current` is the minimum; replace it with its right child, whose
                // height is `current`'s cached right height.  `child` is always
                // the left side going up; both child heights are known each step,
                // so `avl_write_h` reads neither.
                let mut child = right;
                let mut child_h = rh;
                for &(anc_ptr, anc_sz, anc_right, anc_right_h) in path[..path_len].iter().rev() {
                    let (bf, h) = self.avl_write_h(
                        anc_ptr,
                        anc_sz,
                        child,
                        anc_right,
                        Some(child_h),
                        Some(anc_right_h),
                    )?;
                    let (new_root, new_h) = self.avl_rebalance(anc_ptr, bf, h)?;
                    child = new_root;
                    child_h = new_h;
                }
                return Ok((current, size, child));
            }
            if path_len >= MAX_AVL_DEPTH as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "AVL min exceeded maximum depth: corrupted tree (possible cycle)",
                ));
            }
            path[path_len] = (current, size, right, rh);
            path_len += 1;
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

        // Down-pass: record the full traversal path (on a fixed-size stack
        // array, no heap allocation under the mutex) and the index of the last
        // node that satisfies size >= min_size (the best fit).
        let mut path = [PathEntry::default(); MAX_AVL_DEPTH as usize];
        let mut path_len = 0usize;
        let mut last_fit_idx: Option<usize> = None;
        let mut current = root;
        while current != NULL_PTR {
            if path_len >= MAX_AVL_DEPTH as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "AVL find exceeded maximum depth: corrupted tree (possible cycle)",
                ));
            }
            let (root_sz, left, right, lh, rh) = self.read_node_hc(current)?;
            if root_sz >= min_size {
                last_fit_idx = Some(path_len);
                path[path_len] = PathEntry {
                    ptr: current,
                    size: root_sz,
                    left,
                    right,
                    went_left: true,
                    lh,
                    rh,
                };
                path_len += 1;
                current = left;
            } else {
                path[path_len] = PathEntry {
                    ptr: current,
                    size: root_sz,
                    left,
                    right,
                    went_left: false,
                    lh,
                    rh,
                };
                path_len += 1;
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
        // The found node's cached child heights (it was reached via a left
        // descent, so its right subtree — path[fit_idx+1..] — is what the search
        // exhausted; both children are untouched by the removal of this node).
        let found_lh = path[fit_idx].lh;
        let found_rh = path[fit_idx].rh;

        // Remove the best-fit node.  The left subtree (path[fit_idx+1..]) was
        // searched and yielded nothing, so found_left is returned unchanged.
        // `repl_h` is the replacement subtree's height, seeding the up-pass —
        // known in every case now (single child from the cache, successor from
        // its rebalance).
        let (replacement, repl_h): (u64, u8) = if found_left == NULL_PTR {
            (found_right, found_rh)
        } else if found_right == NULL_PTR {
            (found_left, found_lh)
        } else {
            // Two children: replace with in-order successor (min of right subtree).
            let (succ, succ_sz, new_right) = self.avl_remove_min(found_right)?;
            let (bf, h) = self.avl_write_and_update(succ, succ_sz, found_left, new_right)?;
            let (new_root, new_h) = self.avl_rebalance(succ, bf, h)?;
            (new_root, new_h)
        };

        // Up-pass: update path[0..fit_idx] (path[fit_idx] was removed).  Both
        // child heights are known each step — the modified child threaded from
        // below, the untouched sibling from this node's cache — so `avl_write_h`
        // reads neither and the in-balance case does no further I/O.
        let mut child = replacement;
        let mut child_h = repl_h;
        for entry in path[..fit_idx].iter().rev() {
            let (new_left, new_right, lh, rh) = if entry.went_left {
                (child, entry.right, Some(child_h), Some(entry.rh))
            } else {
                (entry.left, child, Some(entry.lh), Some(child_h))
            };
            let (bf, h) = self.avl_write_h(entry.ptr, entry.size, new_left, new_right, lh, rh)?;
            let (new_root, new_h) = self.avl_rebalance(entry.ptr, bf, h)?;
            child = new_root;
            child_h = new_h;
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

    #[inline]
    fn stack(&self) -> &BStack {
        &self.stack
    }

    #[inline]
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
        let aligned = Self::align_up_len(len).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "alloc: length exceeds the maximum allocatable size",
            )
        })?;
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
        ensure_own_slice(self, &slice, "GhostTreeBstackAllocator::realloc")?;
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
        let too_long = || {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "realloc: length exceeds the maximum allocatable size",
            )
        };
        let aligned_old = Self::align_up_len(old_len).ok_or_else(too_long)?;
        let aligned_new = Self::align_up_len(new_len).ok_or_else(too_long)?;

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

            // Atomic fast path: zero the sub-block padding BEFORE discarding the
            // tail. These two steps cannot be fused into one crash-atomic call
            // here (that needs the in-sequence tail-replace primitive), so their
            // order decides what a crash between them leaves behind. Zeroing
            // first means a crash leaves the retained block with a zeroed
            // `[new_len, aligned_new)` padding — the zeroed-memory invariant that
            // a later same-block grow relies on (it does not re-zero) — and an
            // as-yet-unreclaimed tail, which is a benign leak. Discarding first
            // would instead leave stale bytes in that padding, which a later grow
            // would hand back to the caller. The padding is owned exclusively by
            // this allocation, so zeroing it before confirming the tail is safe.
            // `try_discard` then atomically checks tail == sentinel and removes
            // the freed tail under bstack's write lock; on failure the block is
            // not the tail and we fall through to the non-tail shrink below (which
            // re-zeros the region, harmlessly, before the AVL insert).
            #[cfg(feature = "atomic")]
            {
                if new_len < aligned_new {
                    self.stack
                        .zero(slice.start() + new_len, aligned_new - new_len)?;
                }
                if self
                    .stack
                    .try_discard(slice.start() + aligned_old, freed_tail)?
                {
                    return Ok(unsafe {
                        BStackSlice::from_raw_parts(self, slice.start(), new_len)
                    });
                }
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
        ensure_own_slice(self, &slice, "GhostTreeBstackAllocator::dealloc")?;
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
        let true_len = Self::align_up_len(slice.len()).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "dealloc: slice length exceeds the maximum allocatable size",
            )
        })?;

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
            .map(|&l| {
                if l == 0 {
                    Some(0)
                } else {
                    Self::align_up_len(l)
                }
            })
            .collect::<Option<Vec<u64>>>()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "alloc_bulk: length exceeds the maximum allocatable size",
                )
            })?;

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
        ensure_own_slices(self, slices, "GhostTreeBstackAllocator::dealloc_bulk")?;

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
            let true_len = Self::align_up_len(s.len()).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "dealloc_bulk: slice length exceeds the maximum allocatable size",
                )
            })?;
            entries.push((s.start(), true_len));
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

#[cfg(all(test, feature = "alloc", feature = "set"))]
mod tests {
    use super::*;
    use crate::BStack;
    use crate::alloc::{BStackAllocator, BStackBulkAllocator, BStackSlice};
    use std::sync::atomic::{AtomicU64, Ordering};

    fn open_fresh() -> (GhostTreeBstackAllocator, std::path::PathBuf) {
        static CTR: AtomicU64 = AtomicU64::new(0);
        let id = CTR.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("bstack_gt_{pid}_{id}.bin"));
        let alloc = GhostTreeBstackAllocator::new(BStack::open(&path).unwrap()).unwrap();
        (alloc, path)
    }

    struct Guard(std::path::PathBuf);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn reopen(path: &std::path::Path) -> GhostTreeBstackAllocator {
        GhostTreeBstackAllocator::new(BStack::open(path).unwrap()).unwrap()
    }

    // ── basic alloc/dealloc ────────────────────────────────────────────────────

    #[test]
    fn alloc_returns_zeroed_slice() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let s = alloc.alloc(64).unwrap();
        assert_eq!(s.len(), 64);
        assert!(s.read().unwrap().iter().all(|&b| b == 0));
    }

    #[test]
    fn alloc_zero_len_returns_null_slice() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let s = alloc.alloc(0).unwrap();
        assert_eq!(s.len(), 0);
        assert_eq!(s.start(), 0);
    }

    #[test]
    fn dealloc_zero_len_is_noop() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let before = alloc.stack().len().unwrap();
        let s = alloc.alloc(0).unwrap();
        alloc.dealloc(s).unwrap();
        assert_eq!(alloc.stack().len().unwrap(), before);
    }

    #[test]
    fn dealloc_tail_shrinks_stack() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let before = alloc.stack().len().unwrap();
        let s = alloc.alloc(64).unwrap();
        let after_alloc = alloc.stack().len().unwrap();
        assert!(after_alloc > before);
        alloc.dealloc(s).unwrap();
        // Tail block is discarded: stack shrinks back.
        assert_eq!(alloc.stack().len().unwrap(), before);
    }

    #[test]
    fn dealloc_nontail_reuses_on_next_alloc() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let a = alloc.alloc(64).unwrap();
        let b = alloc.alloc(64).unwrap();
        let a_start = a.start();
        alloc.dealloc(a).unwrap();
        // Stack did not shrink (b still at tail).
        let stack_len = alloc.stack().len().unwrap();
        // Next alloc should reuse a's slot from the AVL tree.
        let c = alloc.alloc(64).unwrap();
        assert_eq!(c.start(), a_start);
        assert_eq!(alloc.stack().len().unwrap(), stack_len);
        alloc.dealloc(c).unwrap();
        alloc.dealloc(b).unwrap();
    }

    #[test]
    fn freed_block_is_zeroed() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let a = alloc.alloc(64).unwrap();
        let b = alloc.alloc(64).unwrap();
        let a_start = a.start();
        a.write([0xAAu8; 64]).unwrap();
        alloc.dealloc(a).unwrap();
        // Read the raw bytes where a used to live.
        let raw = alloc.stack().get(a_start, a_start + 64).unwrap();
        // The AVL node header (first 32 bytes) has size/child fields written by
        // avl_insert; the remaining 32 bytes must be zeroed by invariant.
        assert!(raw[32..].iter().all(|&b| b == 0));
        alloc.dealloc(b).unwrap();
    }

    // ── alignment ─────────────────────────────────────────────────────────────

    #[test]
    fn all_pointers_are_32_byte_aligned() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let slices: Vec<_> = (0..16).map(|i| alloc.alloc(i * 7 + 1).unwrap()).collect();
        for s in &slices {
            if !s.is_empty() {
                // Arena starts at payload offset 48; the 16-byte BStack header means
                // all payload offsets ≡ 16 (mod 32) map to 32-byte-aligned disk addresses.
                assert_eq!(
                    s.start() % 32,
                    16,
                    "start {} not 32-byte aligned on disk",
                    s.start()
                );
            }
        }
        for s in slices {
            alloc.dealloc(s).unwrap();
        }
    }

    #[test]
    fn alloc_rounds_up_to_min_alloc() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let before = alloc.stack().len().unwrap();
        let s = alloc.alloc(1).unwrap();
        // Underlying block is MIN_ALLOC (32) bytes even though len is 1.
        assert_eq!(alloc.stack().len().unwrap() - before, MIN_ALLOC);
        alloc.dealloc(s).unwrap();
    }

    // ── split behaviour ───────────────────────────────────────────────────────

    #[test]
    fn large_free_block_is_split() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        // Alloc 128 bytes then free it: one free block of 128 bytes in the tree.
        let a = alloc.alloc(128).unwrap();
        let anchor = alloc.alloc(32).unwrap(); // prevent tail discard of a
        let a_start = a.start();
        alloc.dealloc(a).unwrap();
        // Allocate 32 bytes — should split the 128-byte block, leaving 96 bytes.
        let b = alloc.alloc(32).unwrap();
        // b comes from the tail of a's old block.
        assert_eq!(b.start(), a_start + 96);
        // The 96-byte remainder can be reused.
        let c = alloc.alloc(96).unwrap();
        assert_eq!(c.start(), a_start);
        alloc.dealloc(b).unwrap();
        alloc.dealloc(c).unwrap();
        alloc.dealloc(anchor).unwrap();
    }

    // ── realloc ───────────────────────────────────────────────────────────────

    #[test]
    fn realloc_to_zero_deallocates() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let before = alloc.stack().len().unwrap();
        let s = alloc.alloc(64).unwrap();
        let z = alloc.realloc(s, 0).unwrap();
        assert_eq!(z.len(), 0);
        assert_eq!(alloc.stack().len().unwrap(), before);
    }

    #[test]
    fn realloc_same_aligned_size_preserves_data() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let s = alloc.alloc(32).unwrap();
        s.write([0x5Au8; 32]).unwrap();
        let start = s.start();
        // Realloc to a different len with the same aligned block size.
        let s2 = alloc.realloc(s, 16).unwrap();
        assert_eq!(s2.start(), start);
        let buf = s2.read().unwrap();
        assert!(buf[..16].iter().all(|&b| b == 0x5A));
        // Bytes [16..32] were zeroed by realloc.
        assert!(buf[16..].iter().all(|&b| b == 0));
        alloc.dealloc(s2).unwrap();
    }

    #[test]
    fn realloc_shrink_tail_discards() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let s = alloc.alloc(128).unwrap();
        let start = s.start();
        s.write([0xBBu8; 128]).unwrap();
        let s2 = alloc.realloc(s, 32).unwrap();
        assert_eq!(s2.start(), start);
        assert_eq!(alloc.stack().len().unwrap(), start + 32);
        let buf = s2.read().unwrap();
        assert!(buf[..32].iter().all(|&b| b == 0xBB));
        alloc.dealloc(s2).unwrap();
    }

    #[test]
    fn realloc_tail_shrink_then_grow_reads_zeros() {
        // A tail shrink zeroes the sub-block padding it leaves behind, upholding
        // the zeroed-memory invariant, so a later same-block grow — which does
        // not re-zero, trusting that invariant — never hands back stale bytes.
        // (The atomic path zeroes the padding before discarding the freed tail so
        // that even a crash between the two never strands stale padding.)
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let s = alloc.alloc(64).unwrap();
        let start = s.start();
        s.write([0xEEu8; 64]).unwrap();
        // Shrink into the first 32-byte sub-block: the padding [20, 32) and the
        // freed tail [32, 64) must not survive as live 0xEE bytes.
        let s2 = alloc.realloc(s, 20).unwrap();
        assert_eq!(s2.start(), start);
        // Grow back within the retained block; the newly-exposed [20, 30) must
        // read zero, not the old 0xEE.
        let s3 = alloc.realloc(s2, 30).unwrap();
        assert_eq!(s3.start(), start);
        let buf = s3.read().unwrap();
        assert!(buf[..20].iter().all(|&b| b == 0xEE), "live bytes preserved");
        assert!(
            buf[20..].iter().all(|&b| b == 0),
            "grown region reads zero, not stale padding"
        );
        alloc.dealloc(s3).unwrap();
    }

    #[test]
    fn realloc_shrink_nontail_inserts_remainder() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let s = alloc.alloc(128).unwrap();
        let anchor = alloc.alloc(32).unwrap();
        let start = s.start();
        s.write([0xCCu8; 128]).unwrap();
        let stack_len = alloc.stack().len().unwrap();
        let s2 = alloc.realloc(s, 32).unwrap();
        assert_eq!(s2.start(), start);
        // Stack did not shrink — remainder was inserted into the tree.
        assert_eq!(alloc.stack().len().unwrap(), stack_len);
        // Remainder (96 bytes) can be reused.
        let r = alloc.alloc(96).unwrap();
        assert_eq!(r.start(), start + 32);
        alloc.dealloc(s2).unwrap();
        alloc.dealloc(r).unwrap();
        alloc.dealloc(anchor).unwrap();
    }

    // A length that cannot be rounded up to a 32-byte multiple without
    // overflowing `u64` is rejected up front. Unchecked, `len + 31` wrapped a
    // near-`u64::MAX` request down to a 32-byte block and `alloc` returned a
    // handle claiming the original length.
    #[test]
    fn alloc_rejects_unalignable_length() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        for len in [MAX_ALLOC + 1, u64::MAX - 5, u64::MAX] {
            let err = alloc.alloc(len).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "len {len}");
        }
        // The allocator is untouched and still works.
        let s = alloc.alloc(64).unwrap();
        alloc.dealloc(s).unwrap();
    }

    // `MAX_ALLOC` is the exact boundary, not a rounded-down guess: it is
    // 32-aligned so it rounds up to itself, and it leaves room for a block at
    // `ARENA_START`. One byte more does not.
    #[test]
    fn max_alloc_is_the_exact_alignment_boundary() {
        assert_eq!(MAX_ALLOC % MIN_ALLOC, 0);
        assert!(ARENA_START.checked_add(MAX_ALLOC).is_some());
        assert_eq!(
            GhostTreeBstackAllocator::align_up_len(MAX_ALLOC),
            Some(MAX_ALLOC)
        );
        assert_eq!(GhostTreeBstackAllocator::align_up_len(MAX_ALLOC + 1), None);
    }

    // Same rejection on the realloc path, which reached it via the tail-shrink
    // zeroing: the wrapped `aligned_new` made `aligned_new - new_len` underflow
    // into an out-of-range length. The block is untouched, so the caller's
    // handle stays valid.
    #[test]
    fn realloc_rejects_unalignable_length() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let s = alloc.alloc(128).unwrap();
        let start = s.start();
        let stack_len = alloc.stack().len().unwrap();

        let err = alloc.realloc(s, u64::MAX - 5).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        // The block and the stack are untouched.
        assert_eq!(s.start(), start);
        assert_eq!(s.len(), 128);
        assert_eq!(alloc.stack().len().unwrap(), stack_len);
        alloc.dealloc(s).unwrap();
    }

    #[test]
    fn realloc_grow_tail_extends_in_place() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let s = alloc.alloc(32).unwrap();
        let start = s.start();
        s.write([0xDDu8; 32]).unwrap();
        let s2 = alloc.realloc(s, 96).unwrap();
        assert_eq!(s2.start(), start);
        let buf = s2.read().unwrap();
        assert!(buf[..32].iter().all(|&b| b == 0xDD));
        assert!(buf[32..].iter().all(|&b| b == 0));
        alloc.dealloc(s2).unwrap();
    }

    #[test]
    fn realloc_grow_nontail_copies_data() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let s = alloc.alloc(32).unwrap();
        let anchor = alloc.alloc(32).unwrap();
        s.write([0xEEu8; 32]).unwrap();
        let s2 = alloc.realloc(s, 96).unwrap();
        // s2 is a new allocation (different address from anchor).
        assert_ne!(s2.start(), anchor.start());
        let buf = s2.read().unwrap();
        assert!(buf[..32].iter().all(|&b| b == 0xEE));
        assert!(buf[32..].iter().all(|&b| b == 0));
        alloc.dealloc(s2).unwrap();
        alloc.dealloc(anchor).unwrap();
    }

    // ── invalid input ──────────────────────────────────────────────────────────

    #[test]
    fn dealloc_misaligned_ptr_returns_error() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let s = alloc.alloc(64).unwrap();
        let bad = unsafe { BStackSlice::from_raw_parts(&alloc, s.start() + 1, 32) };
        assert!(alloc.dealloc(bad).is_err());
        alloc.dealloc(s).unwrap();
    }

    #[test]
    fn realloc_misaligned_ptr_returns_error() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let s = alloc.alloc(64).unwrap();
        let bad = unsafe { BStackSlice::from_raw_parts(&alloc, s.start() + 1, 32) };
        assert!(alloc.realloc(bad, 64).is_err());
        alloc.dealloc(s).unwrap();
    }

    // ── alloc_bulk / dealloc_bulk ─────────────────────────────────────────────

    #[test]
    fn alloc_bulk_contiguous() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let slices = alloc.alloc_bulk([32u64, 64, 32]).unwrap();
        assert_eq!(slices.len(), 3);
        assert_eq!(slices[0].len(), 32);
        assert_eq!(slices[1].len(), 64);
        assert_eq!(slices[2].len(), 32);
        // All three must be contiguous in address order.
        assert_eq!(slices[1].start(), slices[0].start() + 32);
        assert_eq!(slices[2].start(), slices[1].start() + 64);
        for s in slices {
            alloc.dealloc(s).unwrap();
        }
    }

    #[test]
    fn alloc_bulk_with_zeros_returns_null_for_zeros() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let slices = alloc.alloc_bulk([0u64, 32, 0]).unwrap();
        assert_eq!(slices[0].len(), 0);
        assert_eq!(slices[0].start(), 0);
        assert_eq!(slices[2].len(), 0);
        assert_eq!(slices[2].start(), 0);
        assert_eq!(slices[1].len(), 32);
        for s in slices {
            alloc.dealloc(s).unwrap();
        }
    }

    #[test]
    fn dealloc_bulk_merges_adjacent_slices() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let before = alloc.stack().len().unwrap();
        let slices = alloc.alloc_bulk([64u64, 64, 64]).unwrap();
        alloc.dealloc_bulk(slices).unwrap();
        // All three were adjacent and at the tail; merged into one discard.
        assert_eq!(alloc.stack().len().unwrap(), before);
    }

    // ── coalesce and rebalance on reopen ───────────────────────────────────────

    #[test]
    fn coalesce_on_reopen_merges_adjacent_free_blocks() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path.clone());
        let a = alloc.alloc(64).unwrap();
        let b = alloc.alloc(64).unwrap();
        let anchor = alloc.alloc(32).unwrap();
        let a_start = a.start();
        let anchor_start = anchor.start();
        alloc.dealloc(a).unwrap();
        alloc.dealloc(b).unwrap();
        // Two separate free blocks of 64 bytes at a_start.
        let _ = anchor;
        drop(alloc.into_stack());

        // On reopen, coalesce_and_rebalance merges them into one 128-byte block.
        let alloc2 = reopen(&path);
        let c = alloc2.alloc(128).unwrap();
        assert_eq!(c.start(), a_start);
        alloc2.dealloc(c).unwrap();
        let anchor2 = unsafe { BStackSlice::from_raw_parts(&alloc2, anchor_start, 32) };
        alloc2.dealloc(anchor2).unwrap();
    }

    #[test]
    fn reopen_rebuilds_stale_child_height_cache() {
        // A legacy 0.1.2 arena has arbitrary bytes where the 0.1.3 child-height
        // cache now lives (those offsets were reserved before the cache existed).
        // `coalesce_and_rebalance` runs on every open and rebuilds the whole tree,
        // recomputing each node's cache from its children's own (always-maintained)
        // `height` field — so a stale or garbage cache is healed on open and never
        // trusted across a reopen. This guards the 0.1.x on-disk compatibility.
        let (alloc, path) = open_fresh();
        let _g = Guard(path.clone());
        // Three free nodes kept non-adjacent by allocated anchors, so they do not
        // coalesce into one on reopen and the rebuilt tree really has >1 node.
        let f1 = alloc.alloc(64).unwrap();
        let a1 = alloc.alloc(64).unwrap();
        let f2 = alloc.alloc(48).unwrap();
        let a2 = alloc.alloc(64).unwrap();
        let f3 = alloc.alloc(80).unwrap();
        let a3 = alloc.alloc(64).unwrap();
        let (s1, s2, s3) = (f1.start(), f2.start(), f3.start());
        alloc.dealloc(f1).unwrap();
        alloc.dealloc(f2).unwrap();
        alloc.dealloc(f3).unwrap();
        // Drop the anchor handles without freeing them: their blocks stay
        // allocated (out of the tree), separating the three free nodes.
        let _ = (a1, a2, a3);

        // Clobber every free node's child-height cache with garbage (0xFF reads as
        // height 255), mimicking a legacy file whose reserved bytes are not a cache.
        let stack = alloc.into_stack();
        for &n in &[s1, s2, s3] {
            stack.set(n + NODE_LH_OFF, [0xFFu8]).unwrap();
            stack.set(n + NODE_RH_OFF, [0xFFu8]).unwrap();
        }
        drop(stack);

        // Reopen heals the cache via the rebuild; the tree stays consistent and the
        // free blocks are reusable with correct round-trips.
        let alloc2 = reopen(&path);
        let r1 = alloc2.alloc(48).unwrap();
        r1.write([0x33u8; 48]).unwrap();
        assert_eq!(r1.read().unwrap(), vec![0x33u8; 48]);
        let r2 = alloc2.alloc(64).unwrap();
        r2.write([0x44u8; 64]).unwrap();
        assert_eq!(r2.read().unwrap(), vec![0x44u8; 64]);
        alloc2.dealloc(r1).unwrap();
        alloc2.dealloc(r2).unwrap();
    }

    #[test]
    fn data_survives_reopen() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path.clone());
        let s = alloc.alloc(64).unwrap();
        let start = s.start();
        s.write([0xABu8; 64]).unwrap();
        drop(alloc.into_stack());

        let alloc2 = reopen(&path);
        let s2 = unsafe { BStackSlice::from_raw_parts(&alloc2, start, 64) };
        assert!(s2.read().unwrap().iter().all(|&b| b == 0xAB));
        alloc2.dealloc(s2).unwrap();
    }

    // ── new() error cases ──────────────────────────────────────────────────────

    #[test]
    fn new_rejects_partial_header() {
        use std::io::ErrorKind;
        static CTR: AtomicU64 = AtomicU64::new(0);
        let id = CTR.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("bstack_gt_partial_{pid}_{id}.bin"));
        let _g = Guard(path.clone());
        let stack = BStack::open(&path).unwrap();
        stack.extend(4).unwrap(); // 4 bytes — too small for header
        let err = GhostTreeBstackAllocator::new(stack).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    // ── concurrent (feature = "atomic") ───────────────────────────────────────

    #[cfg(feature = "atomic")]
    #[test]
    fn concurrent_alloc_dealloc_no_live_duplicates() {
        use std::collections::HashSet;
        use std::sync::{Arc, Mutex};
        use std::thread;

        // Verify that concurrent alloc/dealloc never hands the same block to
        // two callers simultaneously.  Each thread claims a block, inserts its
        // offset into a shared live-set (asserting uniqueness), writes and reads
        // back its thread id, then removes the offset and deallocates.  A bug
        // in the AVL mutex would produce a duplicate entry in the set.
        const THREADS: usize = 8;
        const ROUNDS: usize = 200;

        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let alloc = Arc::new(alloc);
        let live: Arc<Mutex<HashSet<u64>>> = Arc::new(Mutex::new(HashSet::new()));

        let handles: Vec<_> = (0..THREADS)
            .map(|tid| {
                let alloc = Arc::clone(&alloc);
                let live = Arc::clone(&live);
                thread::spawn(move || {
                    let a: &GhostTreeBstackAllocator = &alloc;
                    for _ in 0..ROUNDS {
                        let slice = a.alloc(32).unwrap();
                        let off = slice.start();
                        {
                            let mut set = live.lock().unwrap();
                            assert!(set.insert(off), "duplicate live offset {off}");
                        }
                        slice.write([tid as u8; 32]).unwrap();
                        let data = slice.read().unwrap();
                        assert_eq!(data, vec![tid as u8; 32]);
                        {
                            let mut set = live.lock().unwrap();
                            set.remove(&off);
                        }
                        a.dealloc(slice).unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn concurrent_realloc_hammers_tail_paths() {
        use std::sync::Arc;
        use std::thread;

        // T threads each own one allocation and repeatedly grow then shrink it.
        // Whichever allocation sits at the tail exercises try_extend_zeros /
        // try_discard; the others hit the non-tail copy-grow / AVL-insert paths.
        // Both branches are exercised on every round because threads race for
        // the tail.  Verify each thread's data survives every round intact.
        //
        // All sizes are multiples of 32 (GhostTree's MIN_ALLOC):
        //   SMALL = 32  → 32-byte aligned block
        //   LARGE = 96  → 96-byte aligned block (3 × 32)
        const THREADS: usize = 6;
        const ROUNDS: usize = 150;
        const SMALL: u64 = 32;
        const LARGE: u64 = 96;

        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let alloc = Arc::new(alloc);

        let handles: Vec<_> = (0..THREADS)
            .map(|tid| {
                let alloc = Arc::clone(&alloc);
                thread::spawn(move || {
                    let a: &GhostTreeBstackAllocator = &alloc;
                    let mut slice = a.alloc(SMALL).unwrap();
                    slice.write([tid as u8; SMALL as usize]).unwrap();

                    for _ in 0..ROUNDS {
                        // Grow: tail → try_extend_zeros; non-tail → copy to new region.
                        slice = a.realloc(slice, LARGE).unwrap();
                        let data = slice.read().unwrap();
                        assert_eq!(
                            &data[..SMALL as usize],
                            &[tid as u8; SMALL as usize],
                            "data corrupted after grow (tid {tid})",
                        );

                        // Shrink: tail → try_discard; non-tail → AVL insert of freed tail.
                        slice = a.realloc(slice, SMALL).unwrap();
                        let data = slice.read().unwrap();
                        assert_eq!(
                            data,
                            vec![tid as u8; SMALL as usize],
                            "data corrupted after shrink (tid {tid})",
                        );
                    }

                    a.dealloc(slice).unwrap();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn concurrent_alloc_bulk_dealloc_bulk_no_live_duplicates() {
        use std::collections::HashSet;
        use std::sync::{Arc, Mutex};
        use std::thread;

        // Verify that concurrent alloc_bulk / dealloc_bulk never hand the same
        // block to two callers at once.  Each thread requests three slices per
        // round, inserts all offsets into a shared live-set (asserting
        // uniqueness), writes and reads back a pattern, then bulk-deallocates.
        // A bug in the AVL mutex or bulk-allocation path would produce a
        // duplicate offset in the set.
        const THREADS: usize = 6;
        const ROUNDS: usize = 100;
        const SIZES: [u64; 3] = [32, 64, 32]; // all 32-byte aligned; 128 bytes total

        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let alloc = Arc::new(alloc);
        let live: Arc<Mutex<HashSet<u64>>> = Arc::new(Mutex::new(HashSet::new()));

        let handles: Vec<_> = (0..THREADS)
            .map(|tid| {
                let alloc = Arc::clone(&alloc);
                let live = Arc::clone(&live);
                thread::spawn(move || {
                    let a: &GhostTreeBstackAllocator = &alloc;
                    for _ in 0..ROUNDS {
                        let slices = a.alloc_bulk(SIZES).unwrap();
                        {
                            let mut set = live.lock().unwrap();
                            for s in &slices {
                                assert!(
                                    set.insert(s.start()),
                                    "duplicate live offset {}",
                                    s.start()
                                );
                            }
                        }
                        for (s, &sz) in slices.iter().zip(SIZES.iter()) {
                            s.write(vec![tid as u8; sz as usize]).unwrap();
                            let data = s.read().unwrap();
                            assert_eq!(data, vec![tid as u8; sz as usize]);
                        }
                        {
                            let mut set = live.lock().unwrap();
                            for s in &slices {
                                set.remove(&s.start());
                            }
                        }
                        a.dealloc_bulk(slices).unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }
}
