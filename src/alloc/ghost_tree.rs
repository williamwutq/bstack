use super::{
    BStackAllocError, BStackAllocator, BStackBulkAllocError, BStackBulkAllocator,
    BStackInPlaceResizeAllocator, BStackOwnedSlice, BStackUninitAllocator, ensure_own_handle,
    ensure_own_handles,
};
use crate::BStack;
#[cfg(feature = "atomic")]
use crate::BStackGenOp;
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
        stack.acl_claim_alloc();
        let size = stack.len()?;

        if size == 0 {
            stack.extend(ARENA_START)?;
            stack.set(MAGIC_OFFSET, ALGT_MAGIC)?;
            // ROOT_OFFSET is zeroed by extend — null root pointer.
            // Header (magic + root pointer) stays `Alloc` for the allocator's
            // lifetime; own I/O via the `meta_*` helpers.
            stack.acl_mark_alloc(MAGIC_OFFSET, ARENA_START - MAGIC_OFFSET)?;
            return Ok(Self {
                stack,
                #[cfg(feature = "atomic")]
                lock: Mutex::new(()),
                #[cfg(not(feature = "atomic"))]
                _not_sync: PhantomData,
            });
        }

        if size < ARENA_START {
            return Err(io_error!(
                InvalidData,
                format!(
                    "GhostTreeBstackAllocator: payload is {size} B, \
                     too small for the {ARENA_START}-byte header"
                )
            ));
        }

        // Verify magic prefix.
        let mut magic_buf = [0u8; 6];
        stack.get_into(MAGIC_OFFSET, &mut magic_buf)?;
        if magic_buf != ALGT_MAGIC_PREFIX {
            return Err(io_error!(
                InvalidData,
                "GhostTreeBstackAllocator: magic number mismatch"
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
        // Re-arm the header mark on reopen (policy is not persisted); the root I/O
        // in `coalesce_and_rebalance` below goes through the `meta_*` helpers.
        this.stack
            .acl_mark_alloc(MAGIC_OFFSET, ARENA_START - MAGIC_OFFSET)?;
        this.coalesce_and_rebalance()?;
        Ok(this)
    }
}

impl GhostTreeBstackAllocator {
    /// Read the AVL root pointer from the header.
    #[inline]
    fn read_root(&self) -> io::Result<u64> {
        // Header stays `Alloc`; the root pointer is read as the allocator.
        self.stack.meta_read_u64(ROOT_OFFSET)
    }

    /// Write the AVL root pointer to the header.
    #[inline]
    fn write_root(&self, ptr: u64) -> io::Result<()> {
        let mut buf = [0u8; 8];
        write_buf!(ptr => buf, 0);
        self.stack.meta_set(ROOT_OFFSET, buf)?;
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
                return Err(io_error!(
                    InvalidData,
                    "AVL insert exceeded maximum depth: corrupted tree (possible cycle)"
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
                return Err(io_error!(
                    InvalidData,
                    "AVL min exceeded maximum depth: corrupted tree (possible cycle)"
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
                return Err(io_error!(
                    InvalidData,
                    "AVL find exceeded maximum depth: corrupted tree (possible cycle)"
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
                    return Err(io_error!(
                        InvalidData,
                        "AVL walk exceeded maximum depth: corrupted tree (possible cycle)"
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

impl GhostTreeBstackAllocator {
    /// Shared body of [`alloc`](BStackAllocator::alloc) and
    /// [`alloc_uninit`](BStackUninitAllocator::alloc_uninit).
    ///
    /// Only the no-split reuse branch differs; see the
    /// [`BStackUninitAllocator`] impl for why a clear `init` still scrubs the
    /// part of the stale AVL node that lies beyond the caller's visible length.
    fn alloc_impl(&self, len: u64, init: bool) -> io::Result<BStackOwnedSlice<'_, Self>> {
        if len == 0 {
            return Ok(BStackOwnedSlice::empty(self));
        }
        let aligned = Self::align_up_len(len).ok_or_else(|| {
            io_error!(
                InvalidInput,
                "alloc: length exceeds the maximum allocatable size"
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
                    return Ok(unsafe {
                        BStackOwnedSlice::from_raw_parts(self, ptr + remainder, len)
                    });
                } else {
                    #[cfg(feature = "atomic")]
                    drop(guard);
                    // No split: give the whole block.  The stale AVL node occupies
                    // the first 32 bytes; the rest of the block is already zeroed.
                    // Any bytes beyond `len` (up to `block_size`) are internal
                    // padding and will be recovered on dealloc by re-aligning.
                    //
                    // A set `init` scrubs the whole node.  A clear one may
                    // leave the caller's own bytes dirty, but must still restore
                    // the allocator's invariant that a live block reads zero at
                    // and beyond its visible length: `realloc`'s in-place grow
                    // paths hand those bytes straight to the caller without
                    // zeroing them, and would otherwise leak node bytes out of a
                    // call that promises zeros.  For `len >= MIN_ALLOC` nothing is
                    // left to scrub and the write disappears entirely.
                    let scrub_from = if init { 0 } else { len.min(MIN_ALLOC) };
                    if scrub_from < MIN_ALLOC {
                        self.stack.zero(ptr + scrub_from, MIN_ALLOC - scrub_from)?;
                    }
                    // SAFETY: ptr from allocated block via avl_find_best_fit_and_remove
                    return Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, ptr, len) });
                }
            }
        }
        // No free block fits: lock released; grow the BStack (returns zeroed bytes).
        let start = self.stack.extend(aligned)?;
        // SAFETY: start from fresh allocation via self.stack.extend
        Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, start, len) })
    }

    /// Shared body of [`realloc`](BStackAllocator::realloc) and
    /// [`realloc_uninit`](BStackUninitAllocator::realloc_uninit).
    ///
    /// The in-place paths are identical in both modes: every `zero` they issue
    /// upholds the allocator's own zeroed-memory invariant rather than the
    /// caller-facing guarantee, and skipping one would either corrupt the free
    /// tree or leak stale bytes into a later `realloc` that promises zeros.
    /// `init` therefore only reaches the move path, whose fresh block comes from
    /// [`alloc_impl`](Self::alloc_impl).
    fn realloc_impl<'a>(
        &'a self,
        slice: BStackOwnedSlice<'a, Self>,
        new_len: u64,
        init: bool,
    ) -> Result<BStackOwnedSlice<'a, Self>, BStackAllocError<'a, Self>> {
        let slice = ensure_own_handle(self, slice, "GhostTreeBstackAllocator::realloc")?;
        if slice.is_empty() {
            return self.alloc_impl(new_len, init).map_err(|source| {
                BStackAllocError::with_handle(source, BStackOwnedSlice::empty(self))
            });
        }
        let start = slice.start();
        let old_len = slice.len();
        if start < ARENA_START || start != Self::align_up_ptr(start) {
            // Invalid address: the caller's handle is unchanged, hand it back.
            return Err(BStackAllocError {
                source: io_error!(
                    InvalidInput,
                    "realloc: slice origin is not a valid allocator address"
                ),
                // SAFETY: (start, old_len) is exactly what the caller passed in.
                handle: Some(unsafe { BStackOwnedSlice::from_raw_parts(self, start, old_len) }),
            });
        }
        if new_len == 0 {
            // dealloc consumes `slice`; its BStackAllocError propagates unchanged.
            self.dealloc(slice)?;
            return Ok(BStackOwnedSlice::empty(self));
        }
        // Re-align to recover the true underlying block sizes.
        let (aligned_old, aligned_new) =
            match (Self::align_up_len(old_len), Self::align_up_len(new_len)) {
                (Some(old), Some(new)) => (old, new),
                _ => {
                    return Err(BStackAllocError {
                        source: io_error!(
                            InvalidInput,
                            "realloc: length exceeds the maximum allocatable size"
                        ),
                        // SAFETY: (start, old_len) is exactly what the caller
                        // passed in; the block has not been touched.
                        handle: Some(unsafe {
                            BStackOwnedSlice::from_raw_parts(self, start, old_len)
                        }),
                    });
                }
            };

        // In-place resize paths. Most failures here leave the original block
        // intact at (start, old_len); `Some(_)` means the resize was handled,
        // `None` falls through to the move-to-new-region path below.
        //
        // Exception: the non-tail shrink commits a (non-atomic) AVL insert of
        // the freed tail. Once that begins, a torn insert means the block can no
        // longer be safely returned for a retry (which would re-insert an
        // already-linked node and corrupt the free tree), so `lost` is set and
        // the error carries `None`.
        let mut lost = false;
        let inplace_result = (|| -> io::Result<Option<BStackOwnedSlice<'a, Self>>> {
            if aligned_new == aligned_old {
                // Same underlying block — just update the visible length.
                // If it is a shrink, zero the tail to uphold the invariant; the
                // block size doesn't change so the AVL tree is untouched.
                if new_len < old_len {
                    let tail_ptr = start + new_len;
                    let tail_len = old_len - new_len;
                    self.stack.zero(tail_ptr, tail_len)?;
                }
                // SAFETY: same block, just changing visible length
                return Ok(Some(unsafe {
                    BStackOwnedSlice::from_raw_parts(self, start, new_len)
                }));
            }

            if aligned_new < aligned_old {
                // Shrink.
                let freed_tail = aligned_old - aligned_new;
                let tail_ptr = start + aligned_new;

                // Atomic fast path (lock-free): fuse the tail truncation and the
                // padding zeroing into ONE crash-atomic splice. As two calls
                // (`try_discard` then `zero`) a fault between them shrinks the
                // stack yet still returns `old_len` — an out-of-bounds handle.
                // `Len` confirms the tail under `process_gen`'s held write lock,
                // then `Atrunc` cuts `[start+new_len, start+aligned_old)` and
                // re-appends `aligned_new - new_len` zeros. The fault policy is
                // consulted before any mutation, so a fault leaves the block
                // intact and `map_err` returns `Some(start, old_len)`.
                #[cfg(feature = "atomic")]
                {
                    // Padding is `aligned_new - new_len`, always < MIN_ALLOC (32,
                    // the alignment granularity), so a stack buffer suffices.
                    // Used later in the `process_gen` closure to avoid a heap allocation.
                    let zeros = [0u8; MIN_ALLOC as usize];
                    let mut cur_len = 0u64;
                    let cur_ptr: *mut u64 = &mut cur_len;
                    let mut phase = 0u8;
                    let mut truncated = false;
                    self.stack.process_gen(|| match phase {
                        0 => {
                            phase = 1;
                            // SAFETY: `process_gen` invokes this closure strictly
                            // sequentially and finishes writing `out` before the
                            // next call, so the `&mut` never aliases the reads below.
                            Some(BStackGenOp::Len {
                                out: unsafe { &mut *cur_ptr },
                            })
                        }
                        1 => {
                            phase = 2;
                            // SAFETY: the `Len` write above has completed.
                            if start + aligned_old == unsafe { *cur_ptr } {
                                truncated = true;
                                Some(BStackGenOp::Atrunc {
                                    n: aligned_old - new_len,
                                    data: &zeros[..(aligned_new - new_len) as usize],
                                })
                            } else {
                                None
                            }
                        }
                        _ => None,
                    })?;
                    if truncated {
                        return Ok(Some(unsafe {
                            BStackOwnedSlice::from_raw_parts(self, start, new_len)
                        }));
                    }
                    // Not the tail — fall through to the non-tail shrink below.
                }

                // Non-atomic tail shrink. No `atrunc`/`process_gen` without the
                // `atomic` feature, so discard FIRST (a fault there fires before
                // any mutation → the original is fully intact → `Some(old_len)`),
                // then mark the shrink committed and zero the sub-block padding. A
                // fault at that zero can no longer hand back the (now-shrunk)
                // original, so `lost` makes the caller get `None` rather than a
                // partially-zeroed "original".
                #[cfg(not(feature = "atomic"))]
                if start + aligned_old == self.stack.len()? {
                    self.stack.discard(freed_tail)?;
                    lost = true;
                    if new_len < aligned_new {
                        self.stack.zero(start + new_len, aligned_new - new_len)?;
                    }
                    return Ok(Some(unsafe {
                        BStackOwnedSlice::from_raw_parts(self, start, new_len)
                    }));
                }

                // Not tail: zero gap + freed tail before taking the lock, then insert.
                self.stack.zero(start + new_len, aligned_old - new_len)?;
                #[cfg(feature = "atomic")]
                let _guard = self.lock.lock().unwrap();
                // Past this point a torn AVL insert cannot be safely retried.
                lost = true;
                self.avl_insert(tail_ptr, freed_tail)?;
                return Ok(Some(unsafe {
                    BStackOwnedSlice::from_raw_parts(self, start, new_len)
                }));
            }

            // Grow path.
            // Atomic fast path: extend the tail without taking the lock.
            #[cfg(feature = "atomic")]
            if self
                .stack
                .try_extend_zeros(start + aligned_old, aligned_new - aligned_old)?
            {
                return Ok(Some(unsafe {
                    BStackOwnedSlice::from_raw_parts(self, start, new_len)
                }));
            }

            #[cfg(not(feature = "atomic"))]
            if start + aligned_old == self.stack.len()? {
                self.stack.extend(aligned_new - aligned_old)?;
                return Ok(Some(unsafe {
                    BStackOwnedSlice::from_raw_parts(self, start, new_len)
                }));
            }

            Ok(None)
        })();
        let inplace = inplace_result.map_err(|source| BStackAllocError {
            source,
            handle: if lost {
                None
            } else {
                // SAFETY: (start, old_len) still describes the live original block.
                Some(unsafe { BStackOwnedSlice::from_raw_parts(self, start, old_len) })
            },
        })?;

        if let Some(resized) = inplace {
            return Ok(resized);
        }

        // Grow (non-tail): allocate new region, copy old data, free old region.
        let new_slice = self
            .alloc_impl(new_len, init)
            .map_err(|source| BStackAllocError {
                source,
                // Allocation of the new region failed; the original is untouched.
                // SAFETY: (start, old_len) still describes the live original block.
                handle: Some(unsafe { BStackOwnedSlice::from_raw_parts(self, start, old_len) }),
            })?;
        let new_start = new_slice.start();
        // Move the old payload into the freshly-allocated region. With `atomic`
        // this is a single crash-atomic `BStack::copy` — the source and
        // destination are disjoint, so it stages only the source coordinate
        // (O(1) journal) and never materialises the payload in memory. Without
        // `atomic`, fall back to read-then-write.
        #[cfg(feature = "atomic")]
        let copy_result = self.stack.copy(start, new_start, old_len);
        #[cfg(not(feature = "atomic"))]
        let copy_result = self
            .stack
            .get(start, start + old_len)
            .and_then(|data| self.stack.set(new_start, &data));
        if let Err(source) = copy_result {
            // The new region was allocated but the copy failed. Roll it back
            // (best-effort) so it is not leaked: GhostTree recovery only rebuilds
            // the free tree by walking existing nodes, so an allocated-but-
            // untracked region cannot be reclaimed on reopen and would leak the
            // space permanently. The original still holds the data, so hand it
            // back. If the rollback dealloc also fails the region is genuinely
            // lost (the same I/O fault just prevented reclaiming it).
            let _ = self.dealloc(new_slice);
            return Err(BStackAllocError {
                source,
                // SAFETY: (start, old_len) still describes the live original block.
                handle: Some(unsafe { BStackOwnedSlice::from_raw_parts(self, start, old_len) }),
            });
        }
        // Data copied and the new region is fully committed. Free the old block.
        match self.dealloc(slice) {
            Ok(()) => Ok(new_slice),
            Err(mut e) => {
                // The resize itself succeeded — the new region is valid and
                // populated; only freeing the old block failed. Hand back the
                // new region (the old block leaks until recovery).
                e.handle = Some(new_slice);
                Err(e)
            }
        }
    }

    /// Shrink the block at `start` by trimming `pf` bytes off the front and/or
    /// narrowing the back, returning a handle at `start + pf` with visible length
    /// `new_len`. The retained window never moves; each residue — the front
    /// `[start, start + pf)` and the back `[new_start + retained, start +
    /// aligned_old)` — is zeroed and inserted into the AVL tree as its own free
    /// block, exactly as freeing them one-by-one would (the same per-region
    /// insert `dealloc`/`dealloc_bulk` use).
    ///
    /// `pf` must be `MIN_ALLOC`-aligned; a nonzero `pf` and each nonzero residue
    /// is therefore a full block. Because blocks are exact-size (no header), the
    /// residues are computed as `align_up_len` differences and are always a
    /// `MIN_ALLOC` multiple — `0` (nothing to free) or `>= MIN_ALLOC`. A
    /// misaligned `pf` returns [`io::ErrorKind::Unsupported`].
    ///
    /// # Crash safety
    ///
    /// The retained window's bytes are never touched. A residue zero is the first
    /// write, and a faulted `zero` never writes, so a fault on the first zero
    /// leaves the block untouched and hands back the original (`handle: Some`).
    /// Once a residue has been zeroed, though, the original is no longer intact, so
    /// any later fault — the second residue zero, or an AVL insert — reports
    /// `handle: None` rather than a `Some` original with a half-scrubbed residue. A
    /// `None` block leaks (this headerless arena cannot relink it on reopen), which
    /// the trait permits; the guarantee is that a returned `Some` original is
    /// byte-for-byte intact. A crash between the two inserts frees one residue and
    /// leaks the other, never corrupting the retained window.
    fn shrink_inplace<'a>(
        &'a self,
        start: u64,
        old_len: u64,
        pf: u64,
        new_len: u64,
    ) -> Result<BStackOwnedSlice<'a, Self>, BStackAllocError<'a, Self>> {
        let mut lost = false;
        let result = (|| -> io::Result<BStackOwnedSlice<'a, Self>> {
            // A nonzero front trim must land the retained window on a valid,
            // aligned block boundary. `pf == 0` (pure back shrink) is allowed.
            if !pf.is_multiple_of(MIN_ALLOC) {
                return Err(io_error!(
                    Unsupported,
                    "realloc_inplace: front trim misaligned to carve in place"
                ));
            }
            let too_long = || {
                io_error!(
                    InvalidInput,
                    "realloc_inplace: length exceeds the maximum allocatable size"
                )
            };
            let aligned_old = Self::align_up_len(old_len).ok_or_else(too_long)?;
            let retained = Self::align_up_len(new_len).ok_or_else(too_long)?; // >= MIN_ALLOC
            let new_start = start + pf;
            let back_start = new_start + retained;
            // Back residue. `pf`, `retained`, `aligned_old` are MIN_ALLOC
            // multiples, so this is one too; `new_len <= old_len - pf` guarantees
            // it is non-negative. A `checked_sub` failure would mean a caller
            // passed a length larger than the block, which cannot be done here.
            let back_size = match aligned_old.checked_sub(pf + retained) {
                Some(b) => b,
                None => {
                    return Err(io_error!(
                        Unsupported,
                        "realloc_inplace: shrink leaves an unaccounted block"
                    ));
                }
            };

            // Set `lost` immediately *after* each residue is zeroed. A faulted
            // `zero` never writes, so a fault on the first zero leaves the block
            // untouched and the original is handed back (`handle: Some`). But once
            // a residue has been scrubbed the original is no longer intact, so any
            // later fault — the second residue zero, or an AVL insert below — must
            // report `handle: None`, never a `Some` original with a half-zeroed
            // residue. A `None` block leaks (this headerless arena cannot relink it
            // on reopen), which the trait permits; the retained window is untouched.
            if pf > 0 {
                self.stack.zero(start, pf)?;
                lost = true;
            }
            if back_size > 0 {
                self.stack.zero(back_start, back_size)?;
                lost = true;
            }

            #[cfg(feature = "atomic")]
            let _guard = self.lock.lock().unwrap();
            if pf > 0 {
                self.avl_insert(start, pf)?;
            }
            if back_size > 0 {
                self.avl_insert(back_start, back_size)?;
            }
            // SAFETY: retained window at new_start backs exactly `new_len` bytes.
            Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, new_start, new_len) })
        })();
        result.map_err(|source| BStackAllocError {
            source,
            handle: if lost {
                None
            } else {
                // SAFETY: (start, old_len) still names the live, unmodified block.
                Some(unsafe { BStackOwnedSlice::from_raw_parts(self, start, old_len) })
            },
        })
    }
}

impl BStackInPlaceResizeAllocator for GhostTreeBstackAllocator {
    /// In-place resize for the zero-overhead AVL arena.
    ///
    /// Because blocks carry no header, their size is derived from the handle
    /// length (`align_up_len`), so a *grow* would need a move (there is no
    /// neighbour tag to grow into) and is `Unsupported`. **Shrinking** either or
    /// both edges is supported: the trimmed front and/or back residue is each
    /// inserted into the AVL tree as its own free block, and the retained window
    /// — an exact-size block — never moves. Supported combinations:
    ///
    /// * identity (`prepend == 0 && append == 0`);
    /// * pure front shrink (`prepend < 0 && append == 0`);
    /// * pure back shrink (`prepend == 0 && append < 0`);
    /// * front + back shrink together (`prepend < 0 && append < 0`).
    ///
    /// Any grow (`prepend > 0` or `append > 0`) returns
    /// [`io::ErrorKind::Unsupported`]. A front trim requires a `MIN_ALLOC`-aligned
    /// `prepend`; a misaligned one falls back to a copy in `try_subslice`.
    fn realloc_inplace<'a>(
        &'a self,
        slice: BStackOwnedSlice<'a, Self>,
        prepend: i64,
        append: i64,
    ) -> Result<BStackOwnedSlice<'a, Self>, BStackAllocError<'a, Self>> {
        // Reject a handle from another allocator instance before any logic runs
        // (see the module's "Foreign handles" section).
        let slice = ensure_own_handle(self, slice, "GhostTreeBstackAllocator::realloc_inplace")?;
        // An empty handle anchors no block; resizing it in place is never
        // supported for any `(prepend, append)` (see the trait's "Empty handles").
        if slice.is_empty() {
            return Err(BStackAllocError::with_handle(
                io_error!(
                    Unsupported,
                    "realloc_inplace: cannot resize an empty handle in place"
                ),
                slice,
            ));
        }
        let start = slice.start();
        let old_len = slice.len();

        let new_len = match i64::try_from(old_len)
            .ok()
            .and_then(|l| l.checked_add(prepend))
            .and_then(|l| l.checked_add(append))
        {
            Some(n) if n >= 0 => n as u64,
            _ => {
                return Err(BStackAllocError::with_handle(
                    io_error!(
                        InvalidInput,
                        "realloc_inplace: resulting length is negative or overflows"
                    ),
                    slice,
                ));
            }
        };

        if new_len == 0 {
            return self.dealloc(slice).map(|()| BStackOwnedSlice::empty(self));
        }
        if start < ARENA_START || start != Self::align_up_ptr(start) {
            return Err(BStackAllocError::with_handle(
                io_error!(
                    InvalidInput,
                    "realloc_inplace: slice origin is not a valid allocator address"
                ),
                slice,
            ));
        }

        let unsupported = |msg: &'static str| {
            // SAFETY: (start, old_len) still names the live, unmodified block.
            BStackAllocError::with_handle(io_error!(Unsupported, msg), unsafe {
                BStackOwnedSlice::from_raw_parts(self, start, old_len)
            })
        };

        if prepend == 0 && append == 0 {
            // SAFETY: same region, unchanged length.
            return Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, start, new_len) });
        }
        // A grow at either edge would need a move (no header/neighbour tag).
        if prepend > 0 || append > 0 {
            return Err(unsupported(
                "realloc_inplace: grow not supported by GhostTreeBstackAllocator",
            ));
        }
        // prepend <= 0 and append <= 0 (at least one negative): shrink the front
        // by `pf` and/or the back, freeing each residue into the tree.
        let pf = prepend.unsigned_abs();
        self.shrink_inplace(start, old_len, pf, new_len)
    }
}

impl BStackAllocator for GhostTreeBstackAllocator {
    type Error = io::Error;
    type Allocated<'a> = BStackOwnedSlice<'a, Self>;

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
    #[inline]
    fn alloc(&self, len: u64) -> io::Result<BStackOwnedSlice<'_, Self>> {
        self.alloc_impl(len, true)
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
    #[inline]
    fn realloc<'a>(
        &'a self,
        slice: BStackOwnedSlice<'a, Self>,
        new_len: u64,
    ) -> Result<BStackOwnedSlice<'a, Self>, BStackAllocError<'a, Self>> {
        self.realloc_impl(slice, new_len, true)
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
    fn dealloc<'a>(
        &'a self,
        slice: BStackOwnedSlice<'a, Self>,
    ) -> Result<(), BStackAllocError<'a, Self>> {
        let slice = ensure_own_handle(self, slice, "GhostTreeBstackAllocator::dealloc")?;
        let start = slice.start();
        let len = slice.len();
        if let Err(source) = self.stack.acl_reclaim(start, len) {
            return Err(BStackAllocError::with_handle(source, slice));
        }
        // Set once the (non-atomic) AVL insert has begun. A torn insert leaves
        // the tree inconsistent, and GhostTree has no is_free flag to repair it
        // in-process, so the block can no longer be returned for a retry —
        // re-inserting an already-linked node would corrupt the free tree
        // (self-cycle / duplicate parent link → the same region handed to two
        // allocations). Failures before this point (validation, or the atomic
        // tail discard, which frees nothing on failure) keep the handle.
        let mut lost = false;
        let result = (|| -> io::Result<()> {
            if slice.is_empty() {
                return Ok(());
            }
            if slice.start() < ARENA_START || slice.start() != Self::align_up_ptr(slice.start()) {
                return Err(io_error!(
                    InvalidInput,
                    "dealloc: slice origin is not a valid allocator address"
                ));
            }
            let ptr = slice.start();
            let true_len = Self::align_up_len(slice.len()).ok_or_else(|| {
                io_error!(
                    InvalidInput,
                    "dealloc: slice length exceeds the maximum allocatable size"
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
            lost = true;
            self.avl_insert(ptr, true_len)
        })();
        result.map_err(|source| BStackAllocError {
            source,
            handle: if lost {
                None
            } else {
                // SAFETY: (start, len) still describes the caller's live block.
                Some(unsafe { BStackOwnedSlice::from_raw_parts(self, start, len) })
            },
        })
    }
}

/// Reusing a free block without scrubbing its stale AVL node.
///
/// GhostTree is a zero-on-free allocator: [`dealloc`](BStackAllocator::dealloc)
/// zeroes the whole region before inserting it into the tree, and
/// [`BStack::extend`] zeroes fresh tail growth for free.  A reused block is
/// therefore already zero everywhere except the 32 bytes holding its AVL node —
/// and even those only when the block is handed out whole, since a split hands
/// back the tail while the node stays in the retained head.  So the entire
/// caller-facing zero guarantee costs exactly one [`BStack::zero`] of
/// `MIN_ALLOC` bytes, on one branch.
///
/// [`alloc_uninit`](BStackUninitAllocator::alloc_uninit) removes that call — a
/// whole durable sync — for every request of `MIN_ALLOC` bytes or more.  Below
/// `MIN_ALLOC` it narrows the scrub to `[len, MIN_ALLOC)` rather than dropping
/// it: GhostTree relies on a live block reading zero at and beyond its visible
/// length, because [`realloc`](BStackAllocator::realloc)'s in-place grow paths
/// expose those bytes to the caller without zeroing them.  Leaving node bytes
/// there would let a later plain `realloc` return non-zero "newly added" bytes,
/// breaking a guarantee the caller never opted out of.
///
/// [`realloc_uninit`](BStackUninitAllocator::realloc_uninit) differs from
/// [`realloc`](BStackAllocator::realloc) only in the non-tail grow, which takes
/// its fresh block from `alloc_uninit`.  Its in-place paths issue no
/// caller-facing zero-fill at all — every `zero` there maintains the free-tree
/// invariant — so there is nothing else to skip.
impl BStackUninitAllocator for GhostTreeBstackAllocator {
    #[inline]
    fn alloc_uninit(&self, len: u64) -> io::Result<BStackOwnedSlice<'_, Self>> {
        self.alloc_impl(len, false)
    }

    #[inline]
    fn realloc_uninit<'a>(
        &'a self,
        slice: BStackOwnedSlice<'a, Self>,
        new_len: u64,
    ) -> Result<BStackOwnedSlice<'a, Self>, BStackAllocError<'a, Self>> {
        self.realloc_impl(slice, new_len, false)
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
                io_error!(
                    InvalidInput,
                    "alloc_bulk: length exceeds the maximum allocatable size"
                )
            })?;

        let total = aligned
            .iter()
            .copied()
            .try_fold(0u64, |acc, a| acc.checked_add(a))
            .ok_or_else(|| io_error!(InvalidInput, "alloc_bulk: total size overflows u64"))?;

        // All zero-length: return null slices without touching the BStack.
        if total == 0 {
            return Ok(lengths
                .iter()
                // SAFETY: zero-length slices are safe
                .map(|_| BStackOwnedSlice::empty(self))
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
                result.push(BStackOwnedSlice::empty(self));
            } else {
                // SAFETY: block_ptr + offset is within the bulk allocated block
                result.push(unsafe {
                    BStackOwnedSlice::from_raw_parts(self, block_ptr + offset, len)
                });
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
        slices: impl IntoIterator<Item = Self::Allocated<'a>>,
    ) -> Result<(), BStackBulkAllocError<'a, Self>> {
        let slices: Vec<BStackOwnedSlice<'a, Self>> = slices.into_iter().collect();
        let slices = ensure_own_handles(self, slices, "GhostTreeBstackAllocator::dealloc_bulk")?;
        for s in &slices {
            if let Err(source) = self.stack.acl_reclaimable(s.start(), s.len()) {
                return Err(BStackBulkAllocError::with_handles(source, slices));
            }
        }

        // Set once any block has begun to be freed. This free is progressive
        // (tail discard, then per-block zero + AVL insert), so once it starts a
        // mid-way failure can leave some blocks freed and the merge has erased
        // the original per-handle boundaries — no handle can then be safely
        // returned. Before this point every handle is still fully owned.
        let mut freeing = false;
        let result = (|| -> io::Result<()> {
            // Collect, validate, and convert to (ptr, aligned_size) pairs.
            let mut entries: Vec<(u64, u64)> = Vec::new();
            for s in &slices {
                if s.is_empty() {
                    continue;
                }
                if s.start() < ARENA_START || s.start() != Self::align_up_ptr(s.start()) {
                    return Err(io_error!(
                        InvalidInput,
                        "dealloc_bulk: invalid slice origin"
                    ));
                }
                let true_len = Self::align_up_len(s.len()).ok_or_else(|| {
                    io_error!(
                        InvalidInput,
                        "dealloc_bulk: slice length exceeds the maximum allocatable size"
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

            // Past this point blocks are physically reclaimed.
            freeing = true;

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
        })();
        result.map_err(|source| BStackBulkAllocError {
            source,
            handles: if freeing { Vec::new() } else { slices },
        })
    }
}

#[cfg(all(test, feature = "alloc", feature = "set"))]
mod tests {
    use super::*;
    use crate::BStack;
    use crate::alloc::{
        BStackAllocator, BStackBulkAllocator, BStackOwnedSlice, BStackUninitAllocator,
    };
    use std::io::ErrorKind;
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

    // ── alloc_uninit / realloc_uninit ─────────────────────────────────────────

    #[test]
    fn alloc_uninit_returns_a_usable_region() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let mut s = alloc.alloc_uninit(64).unwrap();
        assert_eq!(s.len(), 64);
        s.write([0xABu8; 64]).unwrap();
        assert_eq!(s.read().unwrap(), vec![0xABu8; 64]);
    }

    #[test]
    fn alloc_uninit_of_fresh_tail_growth_still_reads_zero() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let s = alloc.alloc_uninit(64).unwrap();
        assert_eq!(s.read().unwrap(), vec![0u8; 64]);
    }

    #[test]
    fn alloc_uninit_leaves_the_stale_avl_node_in_a_whole_reused_block() {
        // White-box: proves the 32-byte scrub really is skipped on the no-split
        // reuse path. The trait's contract is only that the bytes are
        // unspecified — here they are the freed block's AVL node.
        let (alloc, path) = open_fresh();
        let _g = Guard(path);

        // Two blocks, so freeing the first inserts it into the tree instead of
        // truncating the tail.
        let a = alloc.alloc(64).unwrap();
        let _pin = alloc.alloc(64).unwrap();
        let start = a.start();
        alloc.dealloc(a).unwrap();

        let b = alloc.alloc_uninit(64).unwrap();
        assert_eq!(b.start(), start, "the freed block must be the one reused");
        let data = b.read().unwrap();
        assert!(
            data[..MIN_ALLOC as usize].iter().any(|&x| x != 0),
            "the stale AVL node must survive into the uninitialised allocation"
        );
        assert_eq!(
            &data[MIN_ALLOC as usize..],
            &[0u8; 32],
            "the rest is zeroed on free"
        );
    }

    #[test]
    fn alloc_still_scrubs_a_whole_reused_block() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);

        let a = alloc.alloc(64).unwrap();
        let _pin = alloc.alloc(64).unwrap();
        let start = a.start();
        alloc.dealloc(a).unwrap();

        let b = alloc.alloc(64).unwrap();
        assert_eq!(b.start(), start);
        assert_eq!(b.read().unwrap(), vec![0u8; 64]);
    }

    #[test]
    fn alloc_uninit_below_min_alloc_keeps_the_block_tail_zeroed() {
        // GhostTree's in-place `realloc` grow hands out bytes past the visible
        // length without zeroing them, trusting that a live block reads zero
        // there. `alloc_uninit` must uphold that even though it skips the scrub
        // of the caller's own bytes, or a later plain `realloc` would return
        // non-zero "newly added" bytes.
        let (alloc, path) = open_fresh();
        let _g = Guard(path);

        let a = alloc.alloc(64).unwrap();
        let _pin = alloc.alloc(64).unwrap();
        alloc.dealloc(a).unwrap();

        // 8 < MIN_ALLOC, so the AVL node overlaps bytes beyond the visible length.
        let small = alloc.alloc_uninit(8).unwrap();
        let grown = alloc.realloc(small, 24).unwrap();
        assert_eq!(
            &grown.read().unwrap()[8..],
            &[0u8; 16],
            "realloc must still hand back zeroed newly-added bytes"
        );
    }

    #[test]
    fn realloc_uninit_preserves_existing_bytes_on_grow() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);

        let mut a = alloc.alloc(64).unwrap();
        a.write([0x11u8; 64]).unwrap();
        // Keep `a` off the tail so the grow has to move it.
        let _pin = alloc.alloc(64).unwrap();

        let grown = alloc.realloc_uninit(a, 200).unwrap();
        assert_eq!(grown.len(), 200);
        assert_eq!(&grown.read().unwrap()[..64], &[0x11u8; 64]);
    }

    #[test]
    fn realloc_uninit_preserves_existing_bytes_on_shrink() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);

        let mut a = alloc.alloc(200).unwrap();
        a.write([0x22u8; 200]).unwrap();
        let shrunk = alloc.realloc_uninit(a, 64).unwrap();
        assert_eq!(shrunk.len(), 64);
        assert_eq!(shrunk.read().unwrap(), vec![0x22u8; 64]);
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
        let mut a = alloc.alloc(64).unwrap();
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
        let mut s = alloc.alloc(32).unwrap();
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
        let mut s = alloc.alloc(128).unwrap();
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
    fn realloc_shrink_nontail_inserts_remainder() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let mut s = alloc.alloc(128).unwrap();
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
    // `process_gen`: the wrapped `aligned_new` made `aligned_new - new_len`
    // underflow, panicking inside the closure while `process_gen` held the
    // write lock — which poisoned the stack's `RwLock` and bricked the handle
    // for every later call. The block is untouched, so the caller's handle
    // comes back intact.
    #[test]
    fn realloc_rejects_unalignable_length() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let s = alloc.alloc(128).unwrap();
        let start = s.start();
        let stack_len = alloc.stack().len().unwrap();

        let err = alloc.realloc(s, u64::MAX - 5).unwrap_err();
        assert_eq!(err.source.kind(), io::ErrorKind::InvalidInput);
        let handle = err.handle.expect("block untouched, handle returned");
        assert_eq!(handle.start(), start);
        assert_eq!(handle.len(), 128);

        // The stack is still usable — the lock was never poisoned.
        assert_eq!(alloc.stack().len().unwrap(), stack_len);
        alloc.dealloc(handle).unwrap();
    }

    #[test]
    fn realloc_grow_tail_extends_in_place() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let mut s = alloc.alloc(32).unwrap();
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
        let mut s = alloc.alloc(32).unwrap();
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
        let bad = unsafe { BStackOwnedSlice::from_raw_parts(&alloc, s.start() + 1, 32) };
        assert!(alloc.dealloc(bad).is_err());
        alloc.dealloc(s).unwrap();
    }

    #[test]
    fn realloc_misaligned_ptr_returns_error() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let s = alloc.alloc(64).unwrap();
        let bad = unsafe { BStackOwnedSlice::from_raw_parts(&alloc, s.start() + 1, 32) };
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
        let anchor2 = unsafe { BStackOwnedSlice::from_raw_parts(&alloc2, anchor_start, 32) };
        alloc2.dealloc(anchor2).unwrap();
    }

    #[test]
    fn data_survives_reopen() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path.clone());
        let mut s = alloc.alloc(64).unwrap();
        let start = s.start();
        s.write([0xABu8; 64]).unwrap();
        drop(alloc.into_stack());

        let alloc2 = reopen(&path);
        let s2 = unsafe { BStackOwnedSlice::from_raw_parts(&alloc2, start, 64) };
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
                        let mut slice = a.alloc(32).unwrap();
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
                    slice
                        .as_slice_mut()
                        .write([tid as u8; SMALL as usize])
                        .unwrap();

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
                        let mut slices = a.alloc_bulk(SIZES).unwrap();
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
                        for (s, &sz) in slices.iter_mut().zip(SIZES.iter()) {
                            s.as_slice_mut()
                                .write(vec![tid as u8; sz as usize])
                                .unwrap();
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

    // ── Foreign handles ───────────────────────────────────────────────────

    #[test]
    fn dealloc_and_realloc_reject_a_handle_from_another_instance() {
        let (a1, p1) = open_fresh();
        let _g1 = Guard(p1);
        let (a2, p2) = open_fresh();
        let _g2 = Guard(p2);

        let h = a1.alloc(64).unwrap();
        assert!(h.is_from(&a1));
        assert!(!h.is_from(&a2));
        let range = h.as_range();

        let err = a2.dealloc(h).expect_err("a2 must refuse a1's handle");
        assert_eq!(err.source.kind(), std::io::ErrorKind::InvalidInput);
        let h = err
            .handle
            .expect("a refused handle is returned, not leaked");
        assert_eq!(h.as_range(), range);

        let err = a2.realloc(h, 128).expect_err("a2 must refuse a1's handle");
        assert_eq!(err.source.kind(), std::io::ErrorKind::InvalidInput);
        let h = err
            .handle
            .expect("a refused handle is returned, not leaked");
        assert_eq!(h.as_range(), range);

        // `a2`'s bookkeeping never saw the foreign block, so it still
        // round-trips its own allocations, and `a1` can still free the region.
        let own = a2.alloc(64).unwrap();
        a2.dealloc(own).map_err(|e| e.source).unwrap();
        a1.dealloc(h).map_err(|e| e.source).unwrap();
    }

    #[test]
    fn dealloc_bulk_rejects_a_batch_containing_a_foreign_handle() {
        let (a1, p1) = open_fresh();
        let _g1 = Guard(p1);
        let (a2, p2) = open_fresh();
        let _g2 = Guard(p2);

        let own = a2.alloc(64).unwrap();
        let foreign = a1.alloc(64).unwrap();

        // One foreign handle poisons the batch: nothing is freed, and every
        // handle comes back — including the one that did belong to `a2`.
        let err = a2
            .dealloc_bulk([own, foreign])
            .expect_err("a2 must refuse a batch holding a1's handle");
        assert_eq!(err.source.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(err.handles.len(), 2);

        let mut handles = err.handles.into_iter();
        let own = handles.next().unwrap();
        let foreign = handles.next().unwrap();
        a2.dealloc(own).map_err(|e| e.source).unwrap();
        a1.dealloc(foreign).map_err(|e| e.source).unwrap();
    }

    // ── realloc_inplace (front shrink) ────────────────────────────────────────

    #[test]
    fn front_shrink_inplace_preserves_retained_and_reuses_front() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let mut h = alloc.alloc(100).unwrap(); // block = align_up_len(100) = 128
        let pat: Vec<u8> = (0..100).map(|i| (i % 251) as u8).collect();
        h.write(&pat).unwrap();
        let start = h.start();

        let r = alloc.realloc_inplace(h, -32, 0).unwrap();
        assert_eq!(r.start(), start + 32);
        assert_eq!(r.len(), 68);
        assert_eq!(r.read().unwrap(), pat[32..100].to_vec());

        // The 32-byte freed front returns to the tree and is reused there.
        let reused = alloc.alloc(32).unwrap();
        assert_eq!(reused.start(), start);
        alloc.dealloc(reused).map_err(|e| e.source).unwrap();
        alloc.dealloc(r).map_err(|e| e.source).unwrap();
    }

    #[test]
    fn realloc_inplace_rejects_misaligned_front_or_any_grow() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let make = || {
            let mut h = alloc.alloc(100).unwrap();
            h.write(vec![0xABu8; 100]).unwrap();
            h
        };
        for (pp, ap, what) in [
            (-16i64, 0i64, "sub-MIN_ALLOC (misaligned) trim"),
            (-48, 0, "misaligned trim"),
            (-16, -8, "misaligned front + back shrink"),
            (0, 8, "back grow"),
            (32, 0, "front grow"),
            (-32, 8, "front shrink + back grow"),
        ] {
            let h = make();
            let err = alloc.realloc_inplace(h, pp, ap).unwrap_err();
            assert_eq!(err.source.kind(), ErrorKind::Unsupported, "{what}");
            let back = err.handle.expect(what);
            alloc.dealloc(back).map_err(|e| e.source).unwrap();
        }
    }

    #[test]
    fn realloc_inplace_empty_handle_is_always_unsupported() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        // Every (prepend, append) on an empty handle, including the no-op, is
        // Unsupported and returns the handle untouched.
        for (p, a) in [(0i64, 0i64), (0, 32), (32, 0), (-32, 32)] {
            let h = alloc.alloc(0).unwrap();
            assert!(h.is_empty());
            let err = alloc.realloc_inplace(h, p, a).unwrap_err();
            assert_eq!(err.source.kind(), ErrorKind::Unsupported);
            assert!(err.handle.is_some());
        }
    }

    #[test]
    fn back_shrink_inplace_frees_back_residue() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let pat: Vec<u8> = (0..100).map(|i| (i % 251) as u8).collect();
        let mut h = alloc.alloc(100).unwrap(); // block = 128
        h.write(&pat).unwrap();
        let start = h.start();

        // Pure back shrink: retained window stays at `start`, back residue freed.
        let r = alloc.realloc_inplace(h, 0, -40).unwrap();
        assert_eq!(r.start(), start);
        assert_eq!(r.len(), 60);
        assert_eq!(r.read().unwrap(), pat[..60].to_vec());

        // The 64-byte back residue (128 - align_up_len(60)=64) returns to the tree.
        let reused = alloc.alloc(64).unwrap();
        assert_eq!(reused.start(), start + 64);
        alloc.dealloc(reused).map_err(|e| e.source).unwrap();
        alloc.dealloc(r).map_err(|e| e.source).unwrap();
    }

    #[test]
    fn front_and_back_shrink_inplace_together() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let pat: Vec<u8> = (0..100).map(|i| (i % 251) as u8).collect();
        let mut h = alloc.alloc(100).unwrap(); // block = 128
        h.write(&pat).unwrap();
        let start = h.start();

        // Trim 32 off the front and narrow to a 58-byte window: front residue 32,
        // retained block 64, back residue 32 (all freed / kept as exact blocks).
        let r = alloc.realloc_inplace(h, -32, -10).unwrap();
        assert_eq!(r.start(), start + 32);
        assert_eq!(r.len(), 58);
        assert_eq!(r.read().unwrap(), pat[32..90].to_vec());

        // Both residues are back in the tree and reusable.
        let a = alloc.alloc(32).unwrap();
        let b = alloc.alloc(32).unwrap();
        assert_eq!(
            [a.start(), b.start()]
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>(),
            [start, start + 96]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
        );
        alloc.dealloc(a).map_err(|e| e.source).unwrap();
        alloc.dealloc(b).map_err(|e| e.source).unwrap();
        alloc.dealloc(r).map_err(|e| e.source).unwrap();
    }

    #[test]
    fn front_and_back_shrink_survives_reopen() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path.clone());
        let pat: Vec<u8> = (0..100).map(|i| (i % 251) as u8).collect();
        let mut h = alloc.alloc(100).unwrap();
        h.write(&pat).unwrap();
        let r = alloc.realloc_inplace(h, -32, -10).unwrap();
        let (r_start, r_len) = (r.start(), r.len());
        drop(alloc);

        // Reopen (coalesce/rebalance walks both freed residues). The retained
        // middle window survives at its carved offset and the allocator works.
        let alloc = reopen(&path);
        assert_eq!(
            alloc.stack().get(r_start, r_start + r_len).unwrap(),
            pat[32..90].to_vec()
        );
        let mut d = alloc.alloc(64).unwrap();
        d.write([9u8; 64]).unwrap();
        assert_eq!(d.read().unwrap(), vec![9u8; 64]);
    }

    #[test]
    fn front_shrink_inplace_survives_reopen() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path.clone());
        let pat: Vec<u8> = (0..100).map(|i| (i % 251) as u8).collect();
        let mut h = alloc.alloc(100).unwrap();
        h.write(&pat).unwrap();
        let r = alloc.realloc_inplace(h, -32, 0).unwrap();
        let r_start = r.start();
        drop(alloc);

        let alloc = reopen(&path);
        // Retained bytes survive the reopen scan at their carved offset.
        assert_eq!(
            alloc.stack().get(r_start, r_start + 68).unwrap(),
            pat[32..100].to_vec()
        );
        let mut d = alloc.alloc(64).unwrap();
        d.write([9u8; 64]).unwrap();
        assert_eq!(d.read().unwrap(), vec![9u8; 64]);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn try_subslice_inplace_for_aligned_front_then_falls_back_for_misaligned() {
        let (alloc, path) = open_fresh();
        let _g = Guard(path);
        let pat: Vec<u8> = (0..100).map(|i| (i % 251) as u8).collect();

        // Aligned front (32) with a back trim: fully in place now.
        let mut a = alloc.alloc(100).unwrap();
        a.write(&pat).unwrap();
        let sub = a.try_subslice_inplace(32, 90).unwrap();
        assert_eq!(sub.read().unwrap(), pat[32..90].to_vec());

        // Misaligned front (10): the in-place carve can't; the fallback copies.
        let mut b = alloc.alloc(100).unwrap();
        b.write(&pat).unwrap();
        let err = b.try_subslice_inplace(10, 90).unwrap_err();
        assert_eq!(err.source.kind(), ErrorKind::Unsupported);
        let b = err.handle.unwrap();
        let sub2 = b.try_subslice(10, 90).unwrap();
        assert_eq!(sub2.read().unwrap(), pat[10..90].to_vec());
    }
}

// Fault-injection failure tests (non-`atomic` white-box; op-agnostic fuzz covers
// the `atomic` build). GhostTree's load-bearing failure path is the non-tail
// `dealloc`, which zeros the block and then commits a non-atomic AVL insert:
// once the insert begins a torn insert cannot be retried, so the handle is lost
// (`None`). The tests pin both the pre-insert (handle retained) and mid-insert
// (handle lost) boundaries.
#[cfg(all(
    test,
    debug_assertions,
    feature = "fault-injection",
    feature = "set",
    not(feature = "atomic")
))]
mod fault_tests {
    use super::GhostTreeBstackAllocator;
    use crate::BStack;
    use crate::alloc::{BStackAllocator, BStackInPlaceResizeAllocator};
    use crate::alloc_fuzz::common::{Guard, policies::FailOpAt, temp_path};
    use crate::fault::FaultPolicy;
    use std::io::ErrorKind;
    use std::sync::Arc;

    fn arm(alloc: &GhostTreeBstackAllocator, policy: FailOpAt) {
        let policy: Arc<dyn FaultPolicy> = Arc::new(policy);
        alloc.stack().set_fault_policy(Some(policy));
    }
    fn disarm(alloc: &GhostTreeBstackAllocator) {
        alloc.stack().set_fault_policy(None);
    }

    // A fresh allocation with no free block grows via a single `extend`; a fault
    // there surfaces cleanly and leaves the allocator usable.
    #[test]
    fn alloc_extend_fault_surfaces_cleanly() {
        let path = temp_path("gt_alloc");
        let _g = Guard(path.clone());
        let alloc = GhostTreeBstackAllocator::new(BStack::open(&path).unwrap()).unwrap();

        arm(&alloc, FailOpAt::new("extend", 0, ErrorKind::Other));
        let err = alloc
            .alloc(64)
            .expect_err("alloc must fail when extend faults");
        disarm(&alloc);
        assert_eq!(err.kind(), ErrorKind::Other);

        let mut s = alloc.alloc(64).unwrap();
        s.write([7u8; 64]).unwrap();
        assert_eq!(s.read().unwrap(), vec![7u8; 64]);
    }

    // Non-tail `dealloc` zeros the block before the AVL insert. Faulting that
    // `zero` fires before any mutation, so the block survives and the handle is
    // returned for a clean retry.
    #[test]
    fn dealloc_nontail_fault_before_insert_returns_handle() {
        let path = temp_path("gt_retain");
        let _g = Guard(path.clone());
        let alloc = GhostTreeBstackAllocator::new(BStack::open(&path).unwrap()).unwrap();

        let mut a = alloc.alloc(64).unwrap();
        a.write([2u8; 64]).unwrap();
        let _b = alloc.alloc(64).unwrap(); // keep a non-tail
        let (a_start, a_len) = (a.start(), a.len());

        arm(&alloc, FailOpAt::new("zero", 0, ErrorKind::Other));
        let err = alloc
            .dealloc(a)
            .expect_err("dealloc must fail when the zero faults");
        disarm(&alloc);

        let handle = err
            .handle
            .expect("fault before the AVL insert must return the surviving handle");
        assert_eq!((handle.start(), handle.len()), (a_start, a_len));
        assert_eq!(handle.read().unwrap(), vec![2u8; 64], "data must be intact");
        alloc.dealloc(handle).unwrap();
    }

    // Non-tail `dealloc` faults the AVL insert's first `set` (after the block is
    // zeroed and the `lost` point is crossed): the block cannot be safely handed
    // back, so the handle is `None`. A reopen must leave surviving allocations
    // intact and the allocator usable.
    #[test]
    fn dealloc_nontail_avl_insert_fault_is_lost() {
        let path = temp_path("gt_lost");
        let _g = Guard(path.clone());

        let b_start = {
            let alloc = GhostTreeBstackAllocator::new(BStack::open(&path).unwrap()).unwrap();
            let a = alloc.alloc(64).unwrap();
            let mut b = alloc.alloc(64).unwrap();
            b.write([5u8; 64]).unwrap();
            let b_start = b.start();

            arm(&alloc, FailOpAt::new("set", 0, ErrorKind::Other));
            let err = alloc
                .dealloc(a)
                .expect_err("dealloc must fail when the AVL insert faults");
            disarm(&alloc);
            assert!(
                err.handle.is_none(),
                "a torn AVL insert must report the block lost (handle None)"
            );
            drop(alloc);
            b_start
        };

        // Reopen (coalesce/rebalance walks reachable nodes). The surviving
        // allocation is intact and the allocator continues to work.
        let alloc = GhostTreeBstackAllocator::new(BStack::open(&path).unwrap()).unwrap();
        assert_eq!(
            alloc.stack().get(b_start, b_start + 64).unwrap(),
            vec![5u8; 64]
        );
        let mut c = alloc.alloc(96).unwrap();
        c.write([6u8; 96]).unwrap();
        assert_eq!(c.read().unwrap(), vec![6u8; 96]);
    }

    // Front-shrink `realloc_inplace` zeros the freed front before the AVL insert.
    // Faulting that `zero` fires before any mutation, so the whole original block
    // survives and the handle is returned.
    #[test]
    fn front_shrink_fault_before_insert_returns_handle() {
        let path = temp_path("gt_fshrink_pre");
        let _g = Guard(path.clone());
        let alloc = GhostTreeBstackAllocator::new(BStack::open(&path).unwrap()).unwrap();

        let mut h = alloc.alloc(100).unwrap();
        h.write([0x3Cu8; 100]).unwrap();
        let (start, len) = (h.start(), h.len());

        arm(&alloc, FailOpAt::new("zero", 0, ErrorKind::Other));
        let err = alloc
            .realloc_inplace(h, -32, 0)
            .expect_err("front shrink must fail when the zero faults");
        disarm(&alloc);

        let handle = err
            .handle
            .expect("fault before the insert must return the handle");
        assert_eq!((handle.start(), handle.len()), (start, len));
        assert_eq!(handle.read().unwrap(), vec![0x3Cu8; 100]);
        alloc.dealloc(handle).map_err(|e| e.source).unwrap();
    }

    // Faulting the front-shrink's AVL insert (after the front is zeroed and the
    // `lost` point is crossed) reports the block lost; a reopen leaves the
    // retained tail intact and the allocator usable.
    #[test]
    fn front_shrink_avl_insert_fault_is_lost() {
        let path = temp_path("gt_fshrink_lost");
        let _g = Guard(path.clone());

        let r_start = {
            let alloc = GhostTreeBstackAllocator::new(BStack::open(&path).unwrap()).unwrap();
            let mut h = alloc.alloc(100).unwrap();
            h.write([0x3Cu8; 100]).unwrap();
            let start = h.start();

            arm(&alloc, FailOpAt::new("set", 0, ErrorKind::Other));
            let err = alloc
                .realloc_inplace(h, -32, 0)
                .expect_err("front shrink must fail when the AVL insert faults");
            disarm(&alloc);
            assert!(
                err.handle.is_none(),
                "a torn AVL insert reports the block lost"
            );
            drop(alloc);
            start + 32
        };

        let alloc = GhostTreeBstackAllocator::new(BStack::open(&path).unwrap()).unwrap();
        // Retained tail bytes [32, 100) survive at the carved offset.
        assert_eq!(
            alloc.stack().get(r_start, r_start + 68).unwrap(),
            vec![0x3Cu8; 68]
        );
        let mut c = alloc.alloc(96).unwrap();
        c.write([6u8; 96]).unwrap();
        assert_eq!(c.read().unwrap(), vec![6u8; 96]);
    }

    // Regression: a front+back shrink zeroes the front residue, then the back.
    // Faulting the *second* zero (after the front is already scrubbed) must report
    // the block lost, not hand back a `Some` original whose front residue is
    // zeroed. Before the fix `lost` was set only after both zeroes, so this
    // returned a corrupted `Some` original.
    #[test]
    fn front_and_back_shrink_second_zero_fault_is_lost() {
        let path = temp_path("gt_fbshrink_zero");
        let _g = Guard(path.clone());
        let alloc = GhostTreeBstackAllocator::new(BStack::open(&path).unwrap()).unwrap();

        let mut h = alloc.alloc(256).unwrap(); // aligned block 256
        h.write([0x3Cu8; 256]).unwrap();

        // Front+back shrink: pf = 32 (front residue), retained 192, back residue
        // 32 — two `zero` calls. The first (index 0) writes and sets `lost`; the
        // second (index 1) faults.
        arm(&alloc, FailOpAt::new("zero", 1, ErrorKind::Other));
        let err = alloc
            .realloc_inplace(h, -32, -32)
            .expect_err("second residue zero faults");
        disarm(&alloc);
        assert_eq!(err.source.kind(), ErrorKind::Other);
        assert!(
            err.handle.is_none(),
            "the front residue was already zeroed, so the original is not intact: \
             must report handle: None, never a corrupted Some"
        );

        // The allocator stays usable after the leak.
        let mut c = alloc.alloc(64).unwrap();
        c.write([9u8; 64]).unwrap();
        assert_eq!(c.read().unwrap(), vec![9u8; 64]);
    }

    // A front+back shrink frees two residues with two AVL inserts. Faulting the
    // first insert's first `set` (after both residues are zeroed, past the `lost`
    // point) reports the block lost; a reopen must still recover a consistent
    // tree with the retained middle window intact.
    #[test]
    fn front_and_back_shrink_avl_insert_fault_is_lost() {
        let path = temp_path("gt_fbshrink_lost");
        let _g = Guard(path.clone());

        let r_start = {
            let alloc = GhostTreeBstackAllocator::new(BStack::open(&path).unwrap()).unwrap();
            let mut h = alloc.alloc(100).unwrap();
            h.write([0x3Cu8; 100]).unwrap();
            let start = h.start();

            arm(&alloc, FailOpAt::new("set", 0, ErrorKind::Other));
            let err = alloc
                .realloc_inplace(h, -32, -10)
                .expect_err("shrink must fail when the AVL insert faults");
            disarm(&alloc);
            assert!(
                err.handle.is_none(),
                "a torn AVL insert reports the block lost"
            );
            drop(alloc);
            start + 32
        };

        let alloc = GhostTreeBstackAllocator::new(BStack::open(&path).unwrap()).unwrap();
        // Retained middle window [32, 90) survives at the carved offset.
        assert_eq!(
            alloc.stack().get(r_start, r_start + 58).unwrap(),
            vec![0x3Cu8; 58]
        );
        let mut c = alloc.alloc(96).unwrap();
        c.write([6u8; 96]).unwrap();
        assert_eq!(c.read().unwrap(), vec![6u8; 96]);
    }

    // `dealloc` reads the payload size to decide truncate-the-tail versus recycle
    // through the AVL tree. A fault there precedes both, so the handle survives.
    #[test]
    fn dealloc_tail_check_read_fault_returns_handle() {
        let path = temp_path("ghost_read");
        let _g = Guard(path.clone());
        let alloc = GhostTreeBstackAllocator::new(BStack::open(&path).unwrap()).unwrap();

        let mut s = alloc.alloc(64).unwrap();
        s.write([5u8; 64]).unwrap();
        let (start, len) = (s.start(), s.len());

        arm(&alloc, FailOpAt::new("len", 0, ErrorKind::Other));
        let err = alloc
            .dealloc(s)
            .expect_err("dealloc must fail when the tail check faults");
        disarm(&alloc);

        assert_eq!(err.source.kind(), ErrorKind::Other);
        let handle = err.handle.expect("the tail check precedes every mutation");
        assert_eq!((handle.start(), handle.len()), (start, len));
        assert_eq!(handle.read().unwrap(), vec![5u8; 64], "data must be intact");
        alloc.dealloc(handle).unwrap();
    }
}
