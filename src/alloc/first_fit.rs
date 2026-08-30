use super::{
    BStackAllocError, BStackAllocator, BStackInPlaceResizeAllocator, BStackOwnedSlice,
    BStackUninitAllocator, ensure_own_handle,
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

/// Full magic for FirstFitBStackAllocator
#[cfg(feature = "set")]
const ALFF_MAGIC: [u8; 8] = *b"ALFF\x00\x01\x03\x00";

/// Compatibility prefix checked on open: `ALFF` + major 0 + minor 1.
/// Any file whose first 6 bytes match is considered compatible.
#[cfg(feature = "set")]
const ALFF_MAGIC_PREFIX: [u8; 6] = *b"ALFF\x00\x01";

/// A persistent first-fit free-list allocator implementing [`BStackAllocator`]
/// on top of a [`BStack`].
///
/// Unlike [`crate::LinearBStackAllocator`], freed regions are tracked on disk in a
/// doubly-linked intrusive free list and reused for future allocations, so the
/// file does not grow without bound.
///
/// # On-disk layout
///
/// The allocator occupies the entire `BStack` payload.  The first 48 payload
/// bytes are the header region, followed immediately by the block arena:
///
/// ```text
/// ┌─────────────────────┬──────────────────────────────────────────────────┐
/// │  reserved (16 B)    │ allocator header (32 B)                          │
/// │  (custom use)       │ magic[8] | flags[4] | _reserved[4] | free_head[8]│
/// └─────────────────────┴──────────────────────────────────────────────────┘
///                        ^                                                 ^
///                   payload offset 16                               offset 48 (arena start)
/// ```
///
/// Every block in the arena is laid out as:
///
/// ```text
/// [ BlockHeader 16 B | payload (size bytes) | BlockFooter 8 B ]
/// ```
///
/// **BlockHeader** (16 bytes) — `size: u64`, `flags: u32` (bit 0 = `is_free`), `_reserved: u32`.
/// **BlockFooter** (8 bytes) — `size: u64` (mirrors the header, used for leftward coalescing).
/// **Free blocks** additionally store `next_free: u64` and `prev_free: u64` in the first
/// 16 bytes of their payload, forming an intrusive doubly-linked list.
///
/// # Minimum allocation size
///
/// Allocations smaller than 16 bytes are rounded up to 16.  All sizes are also
/// rounded up to a multiple of 8, so the first 16 bytes of every free block's
/// payload are always available for the free-list pointers.
///
/// # Free-list policy
///
/// The free list is sorted by insertion order (newest-first / LIFO prepend).
/// `alloc` walks the list from the head and takes the **first block whose size
/// ≥ the aligned request** (first-fit).  If the found block is large enough to
/// split — remaining payload would be ≥ 16 bytes after accounting for the
/// 24-byte per-block overhead — the remainder is kept as a new free block in
/// place; the allocated portion is carved from the back.
///
/// # Coalescing
///
/// [`dealloc`](BStackAllocator::dealloc) merges the freed block with its
/// immediate right and left neighbours if they are free.  If the resulting
/// merged block extends to the stack tail it is discarded immediately.  A
/// cascade check (`cascade_discard_free_tail`) then removes any further free
/// blocks newly exposed at the tail, maintaining the invariant that the tail
/// block is always allocated (or the arena is empty).  This invariant makes
/// tail reclamation inside coalesce unnecessary.
///
/// # Crash consistency
///
/// Any operation that issues more than one [`BStack`] call sets the
/// `recovery_needed` flag in the allocator header immediately before the
/// free-list mutation and clears it once all writes complete.  On the next
/// [`FirstFitBStackAllocator::new`] call, if `recovery_needed` is set, a
/// single linear scan of the arena rebuilds the free list from the `is_free`
/// flags in block headers — no stored pointer values are trusted.  Any
/// partial block at the tail is also truncated.  Recovery is O(n) in arena
/// size and runs at most once per crash event.
///
/// # Thread safety
///
/// `FirstFitBStackAllocator` is always **`Send`** — ownership can be
/// transferred to another thread.
///
/// Without the `atomic` feature it is **not `Sync`**: operations take `&self`
/// and mutate the on-disk free list through `BStack`, so concurrent shared
/// access from multiple threads would race on that state.  Each instance must
/// be used from at most one thread at a time.
///
/// With the `atomic` feature it **is `Sync`**.  An internal [`Mutex`] serialises
/// the two operations that are not already serialised by `BStack`'s own
/// locking: mutating the free list and extending/discarding the stack tail.
/// The `recovery_needed` flag — updated with a compare-and-swap, which costs
/// nothing over the disk write it has to perform anyway — additionally guards
/// against operating on a stack left in a needs-recovery state.  Read-only
/// access within an already-allocated block (e.g. growing in place inside the
/// existing block) is not serialised, as such bytes are owned by the caller
/// and never touched by another thread's free-list walk.
///
/// ```
/// fn assert_send<T: Send>() {}
/// assert_send::<bstack::FirstFitBStackAllocator>();
/// ```
///
/// Without `atomic` the type is `!Sync` (this fails to compile); with `atomic`
/// the internal `Mutex` makes it `Sync` (this compiles):
///
#[cfg_attr(not(feature = "atomic"), doc = "```compile_fail")]
#[cfg_attr(feature = "atomic", doc = "```")]
/// fn assert_sync<T: Sync>() {}
/// assert_sync::<bstack::FirstFitBStackAllocator>();
/// ```
///
/// # Feature flags
///
/// Requires both the `alloc` and `set` Cargo features:
///
/// ```toml
/// bstack = { version = "0.1", features = ["alloc", "set"] }
/// ```
///
/// # Example
///
/// ```no_run
/// use bstack::{BStack, BStackAllocator, FirstFitBStackAllocator};
///
/// # fn main() -> std::io::Result<()> {
/// let alloc = FirstFitBStackAllocator::new(BStack::open("data.bstack")?)?;
///
/// let mut a = alloc.alloc(64)?;
/// let b = alloc.alloc(64)?;
/// a.write(b"hello world")?;
///
/// let a_start = a.start();
/// // On failure `dealloc` returns the handle inside the error; surface just
/// // the underlying error with `.map_err(|e| e.source)`.
/// alloc.dealloc(a).map_err(|e| e.source)?;  // freed; coalesced if adjacent to another free block
///
/// let c = alloc.alloc(64)?;    // reuses a's slot
/// assert_eq!(c.start(), a_start);
///
/// let stack = alloc.into_stack();
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "set")]
pub struct FirstFitBStackAllocator {
    stack: BStack,
    /// Serialises the operations that span multiple [`BStack`] calls and are
    /// therefore not made atomic by `BStack`'s own locking: free-list mutation
    /// and stack extension/discard.  It is **not** taken for general writes
    /// within an already-allocated block, which are owned by the caller and
    /// safe to perform without it.
    #[cfg(feature = "atomic")]
    lock: Mutex<()>,
    #[cfg(not(feature = "atomic"))]
    _not_sync: PhantomData<Cell<()>>,
}

#[cfg(feature = "set")]
impl fmt::Debug for FirstFitBStackAllocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FirstFitBStackAllocator")
            .field("stack", &self.stack)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "set")]
impl FirstFitBStackAllocator {
    const OFFSET_SIZE: u64 = 16;
    const HEADER_SIZE: u64 = 32;
    const BLOCK_HEADER_SIZE: u64 = 16;
    const BLOCK_FOOTER_SIZE: u64 = 8;
    const BLOCK_OVERHEAD_SIZE: u64 = Self::BLOCK_HEADER_SIZE + Self::BLOCK_FOOTER_SIZE;
    const MIN_BLOCK_PAYLOAD_SIZE: u64 = 16;
    // Absolute payload offset of the free_head field in the allocator header:
    // OFFSET_SIZE(16) + magic(8) + flags(4) + _reserved(4) = 32
    const FREE_HEAD_OFFSET: u64 = Self::OFFSET_SIZE + 16;

    /// Open or initialise a `FirstFitBStackAllocator` over `stack`.
    ///
    /// * **Empty stack** — writes the 48-byte allocator header (16 reserved
    ///   bytes followed by the 32-byte header containing the magic, flags, and
    ///   `free_head = 0`) and returns a ready allocator.
    /// * **Non-empty stack** — validates the `ALFF 0.1.x` magic prefix.  If
    ///   the `recovery_needed` flag is set (a crash occurred during a previous
    ///   multi-step operation), runs recovery before returning: the arena is
    ///   scanned linearly, any partial tail block is truncated, and the free
    ///   list is rebuilt from the `is_free` flags in block headers.
    ///
    /// # Errors
    ///
    /// * [`io::ErrorKind::InvalidData`] — the existing payload does not start
    ///   with a valid `ALFF 0.1.x` magic prefix (wrong file or wrong allocator
    ///   type).
    /// * Any [`io::Error`] propagated from the underlying [`BStack`] operations.
    pub fn new(stack: BStack) -> Result<Self, io::Error> {
        // Initialize empty stack with allocator header
        if stack.is_empty()? {
            let mut hdr = [0u8; (Self::OFFSET_SIZE + Self::HEADER_SIZE) as usize];
            hdr[Self::OFFSET_SIZE as usize..Self::OFFSET_SIZE as usize + ALFF_MAGIC.len()]
                .copy_from_slice(&ALFF_MAGIC);
            // flags, _reserved, free_head remain zero
            stack.push(hdr)?;
            return Ok(Self {
                stack,
                #[cfg(feature = "atomic")]
                lock: Mutex::new(()),
                #[cfg(not(feature = "atomic"))]
                _not_sync: PhantomData,
            });
        }
        // Validate header
        let stack_len = stack.len()?;
        if stack_len < Self::OFFSET_SIZE + Self::HEADER_SIZE {
            return Err(io_error!(
                InvalidData,
                "stack too short to contain allocator header"
            ));
        }
        let mut header = [0u8; Self::HEADER_SIZE as usize];
        stack.get_into(Self::OFFSET_SIZE, &mut header)?;
        // Check magic prefix for compatibility with 0.1.x files.
        if header[..ALFF_MAGIC_PREFIX.len()] != ALFF_MAGIC_PREFIX {
            return Err(io_error!(
                InvalidData,
                "invalid magic prefix: expected ALFF\\x00\\x01"
            ));
        }
        // Only bit 0 of flags is recovery_needed; ignore reserved flag bits
        let mut recovery_needed = header[ALFF_MAGIC.len()] & 1 != 0;
        let free_head = u64::from_le_bytes(
            header[ALFF_MAGIC.len() + 8..ALFF_MAGIC.len() + 16]
                .try_into()
                .unwrap(),
        );
        // Check that the free list head is valid (either 0 or a valid payload offset within the stack).
        if free_head != 0 {
            let stack_len = stack.len()?;
            if free_head < Self::OFFSET_SIZE + Self::HEADER_SIZE + Self::BLOCK_HEADER_SIZE
                || free_head >= stack_len
            {
                recovery_needed = true;
            }
        }
        let alloc = Self {
            stack,
            #[cfg(feature = "atomic")]
            lock: Mutex::new(()),
            #[cfg(not(feature = "atomic"))]
            _not_sync: PhantomData,
        };
        if recovery_needed {
            alloc.recovery()?;
        }
        Ok(alloc)
    }

    #[cfg(not(feature = "atomic"))]
    #[inline]
    fn set_recovery_needed(&self) -> io::Result<()> {
        self.stack
            .set(Self::OFFSET_SIZE + 8, 1u32.to_le_bytes().as_slice())
    }

    #[cfg(feature = "atomic")]
    #[inline]
    fn set_recovery_needed(&self) -> io::Result<()> {
        // Set recovery_needed = 1 via CAS, expecting it to currently be 0.
        // Mutual exclusion is provided by `self.lock`; this CAS is a no-cost
        // consistency check layered on the disk write we must do anyway: a
        // failure means the flag was left set by a previously crashed or failed
        // operation, so the stack needs recovery (reopen) before it is safe to
        // mutate, and we surface that as an error rather than proceeding.
        if !self.stack.cas(
            Self::OFFSET_SIZE + 8,
            [0u8; 4].as_slice(),
            1u32.to_le_bytes().as_slice(),
        )? {
            // CAS failed: recovery_needed was already set, so a prior operation
            // crashed or failed mid-mutation and the stack must be recovered.
            Err(io_error!(
                InvalidData,
                "stack needs recovery: recovery_needed already set"
            ))
        } else {
            Ok(())
        }
    }

    #[cfg(not(feature = "atomic"))]
    #[inline]
    fn clear_recovery_needed(&self) -> io::Result<()> {
        self.stack.set(Self::OFFSET_SIZE + 8, [0u8; 4].as_slice())
    }

    #[cfg(feature = "atomic")]
    #[inline]
    fn clear_recovery_needed(&self) -> io::Result<()> {
        // Clear recovery_needed = 0 via CAS, expecting it to currently be 1.
        // As with `set_recovery_needed`, mutual exclusion comes from `self.lock`;
        // this CAS is a no-cost check over the disk write. A failure means the
        // flag was not set when we expected it to be, indicating the paired set
        // was lost or the flag was disturbed out of band.
        if !self.stack.cas(
            Self::OFFSET_SIZE + 8,
            1u32.to_le_bytes().as_slice(),
            [0u8; 4].as_slice(),
        )? {
            // CAS failed: recovery_needed was not set when we expected it to be.
            Err(io_error!(
                InvalidData,
                "recovery_needed was not set when clearing"
            ))
        } else {
            Ok(())
        }
    }

    /// Check if a block size is impossible given the allocator's invariants and the stack length.
    ///
    /// Includes the multiple of 8 alignment invariant
    #[inline]
    fn is_impossible_block_size(&self, size: u64) -> bool {
        size < Self::MIN_BLOCK_PAYLOAD_SIZE || size > self.len().unwrap_or(u64::MAX)
    }

    /// Check if a block start is impossible given the allocator's invariants and the stack length.
    ///
    /// Includes the multiple of 8 alignment invariant
    #[inline]
    fn is_impossible_block_start(&self, start: u64) -> bool {
        !start.is_multiple_of(8)
            || start < Self::OFFSET_SIZE + Self::HEADER_SIZE + Self::BLOCK_HEADER_SIZE
            || start >= self.len().unwrap_or(u64::MAX)
    }

    /// Check if a block end offset is impossible given the allocator's invariants and the stack length.
    ///
    /// Does not include multiple of 8 alignment
    #[inline]
    fn is_impossible_block_end(&self, end: u64) -> bool {
        end < Self::OFFSET_SIZE
            + Self::HEADER_SIZE
            + Self::BLOCK_HEADER_SIZE
            + Self::MIN_BLOCK_PAYLOAD_SIZE
            || end > self.len().unwrap_or(u64::MAX) - Self::BLOCK_FOOTER_SIZE
    }

    /// Align a requested payload length to the allocator's block size and alignment requirements.
    #[inline]
    fn align_len(&self, len: u64) -> u64 {
        len.max(Self::MIN_BLOCK_PAYLOAD_SIZE).next_multiple_of(8)
    }

    /// Remove a free block from the free list by updating its neighbours' pointers.
    /// Does not touch the block's own header or payload.
    fn unlink_from_free_list(&self, payload_start: u64) -> io::Result<()> {
        let mut ptrs = [0u8; 16];
        self.stack.get_into(payload_start, &mut ptrs)?;
        let next = read_buf_le!(ptrs, 0 => u64);
        let prev = read_buf_le!(ptrs, 8 => u64);
        if prev != 0 {
            self.stack.set(prev, next.to_le_bytes())?;
        } else {
            self.stack.set(Self::FREE_HEAD_OFFSET, next.to_le_bytes())?;
        }
        if next != 0 {
            self.stack.set(next + 8, prev.to_le_bytes())?;
        }
        Ok(())
    }

    fn add_to_free_list(&self, block_start: u64) -> io::Result<()> {
        // Add the block at block_start to the head of the free list, coalescing adjacent free
        // neighbours first. This involves:
        //   1. Marking the block as free (crash before coalescing: recovery finds it as free).
        //   2. Absorbing the right neighbour if it is free (right coalesce).
        //   3. Merging into the left neighbour if it is free (left coalesce).
        //   4. Prepend the merged block to the free list.
        //   (Tail reclamation is the caller's responsibility via cascade_discard_free_tail.)

        // Current free list:
        // free_head --------------> next -> ...
        // free_head <-------------- next <- ...

        let stack_len = self.stack.len()?;
        let arena_start = Self::OFFSET_SIZE + Self::HEADER_SIZE;
        let block_header_start = block_start - Self::BLOCK_HEADER_SIZE;

        // Read the current block's payload size from its header
        let mut size_buf = [0u8; 8];
        self.stack.get_into(block_header_start, &mut size_buf)?;
        let mut size = u64::from_le_bytes(size_buf);
        let mut result_header_start = block_header_start;

        // Mark block as free early so recovery can find it even if we crash mid-coalesce
        self.stack.set(block_header_start + 8, 1u32.to_le_bytes())?;

        // Coalesce right: absorb the immediately following block if it is free
        let next_header = block_header_start + Self::BLOCK_OVERHEAD_SIZE + size;
        if next_header + Self::BLOCK_HEADER_SIZE <= stack_len {
            let mut next_hdr = [0u8; 16];
            self.stack.get_into(next_header, &mut next_hdr)?;
            let next_size = read_buf_le!(next_hdr, 0 => u64);
            if next_hdr[8] & 1 != 0
                && next_size >= Self::MIN_BLOCK_PAYLOAD_SIZE
                && next_size % 8 == 0
                && next_header + Self::BLOCK_OVERHEAD_SIZE + next_size <= stack_len
            {
                self.unlink_from_free_list(next_header + Self::BLOCK_HEADER_SIZE)?;
                size += next_size + Self::BLOCK_OVERHEAD_SIZE;
            }
        }

        // Coalesce left: merge into the immediately preceding block if it is free.
        // Use its footer (8 bytes before our header) to locate its header, then cross-check.
        if block_header_start > arena_start {
            let mut prev_footer_buf = [0u8; 8];
            self.stack.get_into(
                block_header_start - Self::BLOCK_FOOTER_SIZE,
                &mut prev_footer_buf,
            )?;
            let prev_size = u64::from_le_bytes(prev_footer_buf);
            if prev_size >= Self::MIN_BLOCK_PAYLOAD_SIZE
                && prev_size % 8 == 0
                && let Some(prev_header) = block_header_start
                    .checked_sub(prev_size + Self::BLOCK_OVERHEAD_SIZE)
                    .filter(|&h| h >= arena_start)
            {
                let mut prev_hdr = [0u8; 16];
                self.stack.get_into(prev_header, &mut prev_hdr)?;
                let prev_hdr_size = read_buf_le!(prev_hdr, 0 => u64);
                // Cross-check: header size must match footer size
                if prev_hdr[8] & 1 != 0 && prev_hdr_size == prev_size {
                    self.unlink_from_free_list(prev_header + Self::BLOCK_HEADER_SIZE)?;
                    size += prev_size + Self::BLOCK_OVERHEAD_SIZE;
                    result_header_start = prev_header;
                }
            }
        }

        let result_start = result_header_start + Self::BLOCK_HEADER_SIZE;

        // Write the merged block's size into its header and footer
        self.stack.set(result_header_start, size.to_le_bytes())?;
        self.stack.set(result_start + size, size.to_le_bytes())?;

        // Mark result block as free and write next_free = old_head, prev_free = 0 in one call.
        // Writes flags(4) + reserved(4) + next_free(8) + prev_free(8) starting at result_start - 8.
        // free_head <- result_block -> next
        // free_head --------------------> next -> ...
        // free_head <------------------- next <- ...
        let mut head_buf = [0u8; 8];
        self.stack.get_into(Self::FREE_HEAD_OFFSET, &mut head_buf)?;
        let next_block = u64::from_le_bytes(head_buf);
        let mut update_buf = [0u8; 24];
        write_buf!(1u32 => update_buf, 0); // is_free = 1
        write_buf!(next_block => update_buf, 8); // next_free = old head
        // update_buf[4..8] = reserved = 0, update_buf[16..24] = prev_free = 0
        self.stack
            .set(result_start - Self::BLOCK_HEADER_SIZE + 8, update_buf)?;

        // Update free_head to point to the result block.
        // free_head <- result_block
        // free_head -> result_block -> next -> ...
        // free_head <------------------ next <- ...
        // If this step fails, the free list is still consistent but the result block is orphaned
        self.stack
            .set(Self::FREE_HEAD_OFFSET, result_start.to_le_bytes())?;

        // After adding result block:
        // free_head -> result_block -> next -> ...
        // free_head <- result_block <- next <- ...
        // If this step fails, the forward links are still consistent but the backward link from next to result_block
        // is missing, which can be detected and fixed in recovery. This is similar to the unlink case in unlink_block
        if next_block != 0 {
            self.stack.set(next_block + 8, result_start.to_le_bytes())?;
        }

        Ok(())
    }

    /// Find the first free block that is large enough to hold `size` bytes of payload.
    ///
    /// Walk the free list starting from the head, checking each block's size until a suitable block
    /// is found or the end of the list is reached.
    ///
    /// Returns the offset of the block's payload if a suitable block is found, or 0 if no such block exists.
    fn find_large_enough_block(&self, size: u64) -> io::Result<(u64, u64)> {
        // Walk the free-list from free_head. For each block, check if block.size >= len
        let mut block_found = 0u64;
        let mut found_size = 0u64;
        let stack_len = self.stack.len()?;
        let arena_start = Self::OFFSET_SIZE + Self::HEADER_SIZE;
        // Upper bound on the number of free blocks; used to detect cycles.
        let max_walk = stack_len.saturating_sub(arena_start)
            / (Self::MIN_BLOCK_PAYLOAD_SIZE + Self::BLOCK_OVERHEAD_SIZE)
            + 1;
        let mut walk_count = 0u64;
        let mut free_head_buf = [0u8; 8];
        self.stack
            .get_into(Self::FREE_HEAD_OFFSET, &mut free_head_buf)?;
        let mut head = u64::from_le_bytes(free_head_buf);
        while head != 0 {
            walk_count += 1;
            if walk_count > max_walk {
                return Err(io_error!(
                    InvalidData,
                    "corrupted free list: cycle detected (walk exceeded maximum block count)"
                ));
            }
            let size_flags_and_ptr_buf = &mut [0u8; Self::BLOCK_HEADER_SIZE as usize + 8];
            self.stack
                .get_into(head - Self::BLOCK_HEADER_SIZE, size_flags_and_ptr_buf)?;
            let block_size = read_buf_le!(size_flags_and_ptr_buf, 0 => u64);
            let is_free = size_flags_and_ptr_buf[8] & 1 != 0;
            debug_assert!(
                is_free,
                "corrupted free list: block at offset {head} is not marked free"
            );
            if !is_free {
                return Err(io_error!(
                    InvalidData,
                    format!("corrupted free list: block at offset {head} is not marked free")
                ));
            } else if self.is_impossible_block_size(block_size) || block_size % 8 != 0 {
                return Err(io_error!(
                    InvalidData,
                    format!(
                        "corrupted free list: block at offset {head} has invalid size {block_size}"
                    )
                ));
            }
            if block_size >= size {
                block_found = head;
                found_size = block_size;
                break;
            }
            head = u64::from_le_bytes(
                size_flags_and_ptr_buf
                    [Self::BLOCK_HEADER_SIZE as usize..(Self::BLOCK_HEADER_SIZE as usize + 8)]
                    .try_into()
                    .unwrap(),
            );
            if head != 0 && self.is_impossible_block_start(head) {
                return Err(io_error!(
                    InvalidData,
                    format!("corrupted free list: next block offset {head} is invalid")
                ));
            }
        }

        Ok((block_found, found_size))
    }

    /// Detach `found_start` from the free list and mark it allocated.
    ///
    /// `content_buffer` is the payload image to lay down over the reclaimed
    /// block: `Self::BLOCK_OVERHEAD_SIZE + requested_size` bytes, whose
    /// `[BLOCK_HEADER_SIZE, len - BLOCK_FOOTER_SIZE)` window is the content.
    /// Passing it lets the block's metadata write and the payload scrub (or a
    /// copied payload) land in a single `set`.
    ///
    /// `None` means the caller wants the payload left alone — the reclaimed
    /// block keeps whatever its previous occupant wrote. The same writes are
    /// issued in the same order, each covering only the metadata words, so the
    /// crash-consistency reasoning below is unchanged.
    fn unlink_block(
        &self,
        found_start: u64,
        found_size: u64,
        requested_size: u64,
        content_buffer: Option<&mut [u8]>,
    ) -> io::Result<()> {
        if found_size >= requested_size + Self::BLOCK_OVERHEAD_SIZE + Self::MIN_BLOCK_PAYLOAD_SIZE {
            // The found block is big enough to split. Split it into an allocated block of the requested size
            // and a smaller free block for the remainder, and add the new free block back to the free list.
            // There is no need to change the pointers but only update block size

            // Structure of the block after split:
            // [ old block header  | ----------------------------------- old block content ----------------------------------- | -- old block footer -- ]
            // [ free block header | free block content | free block footer | allocated block header | allocated block content | allocated block footer ]
            //                     ^ found_start
            //                     | < ------------------------------------- found_size -------------------------------------> |
            // | BLOCK_HEADER_SIZE | < remaining_size > | BLOCK_FOOTER_SIZE | BLOCK_HEADER_SIZE      | <-- requested_size ---> | BLOCK_FOOTER_SIZE      |
            //                                                              | <--------------------------- content_buffer ----------------------------> |
            //
            // | < update 3 > |    |                    | <--------------- update 1 ---------------> | <------------------ update 2 ------------------> |

            let remaining_size = found_size - requested_size - Self::BLOCK_OVERHEAD_SIZE;

            // Update 1
            // Update the footer of the free block and write the header of the allocated block together
            // Flag and reserved bytes are already 0, so the new block is marked as allocated.
            let update_buf = &mut [0u8; Self::BLOCK_OVERHEAD_SIZE as usize];
            write_buf!(remaining_size => update_buf, 0);
            write_buf!(requested_size => update_buf, 8);
            self.stack.set(found_start + remaining_size, update_buf)?;

            // Update 2
            // Update the footer of the allocated block and zero out
            // If this step fails and middle is already updated, nothing bad happens since
            // the middle of a free block is just garbage data
            let payload_start = found_start + remaining_size + Self::BLOCK_OVERHEAD_SIZE;
            match content_buffer {
                Some(content_buffer) => {
                    // Write the footer of the allocated block into the content
                    // buffer so payload and footer land in one call.
                    write_buf!(requested_size => content_buffer, (requested_size + Self::BLOCK_HEADER_SIZE) as usize);
                    self.stack.set(
                        payload_start,
                        &content_buffer[Self::BLOCK_HEADER_SIZE as usize..],
                    )?;
                }
                // Payload left as-is: only the footer has to be written.
                None => {
                    self.stack
                        .set(payload_start + requested_size, requested_size.to_le_bytes())?;
                }
            }

            // Update the size of the free block in the header.
            // If this steps fails, the header is corrupted and should be repaired in recovery
            // Failure cause: header corruption
            self.stack.set(
                found_start - Self::BLOCK_HEADER_SIZE,
                remaining_size.to_le_bytes().as_slice(),
            )?;
            Ok(())
        } else {
            // The found block is not big enough to split, so just remove it from the free list and return it.
            // Read both pointers
            let mut pointers_buf = [0u8; 16];
            self.stack.get_into(found_start, &mut pointers_buf)?;
            let next = read_buf_le!(pointers_buf, 0 => u64);
            let prev = read_buf_le!(pointers_buf, 8 => u64);

            // Commit backward pointer first
            // If fails here, the free list looks like this:
            // free_head -> ... -> prev -> found_block -> next -> ...
            //              ... <- prev <---------------- next <- ...
            // So the forward link is still there
            if prev != 0 {
                self.stack.set(prev, next.to_le_bytes())?;
            } else {
                self.stack.set(Self::FREE_HEAD_OFFSET, next.to_le_bytes())?;
            }

            // Then commit forward pointer
            // If fails here, the block is orphaned but still marked as free, which should be repaired in recovery
            // Failure cause: orphaned block with stale forward link from old head (detectable in recovery) but no backward link
            if next != 0 {
                self.stack.set(next + 8, prev.to_le_bytes())?;
            }

            // Clear is_free flag + reserved and write user data in one call by modifying content_buffer
            // Failure cause: orphaned block
            let flags_offset = found_start - Self::BLOCK_HEADER_SIZE + 8;
            match content_buffer {
                Some(content_buffer) => {
                    content_buffer[8..16].copy_from_slice(&[0u8; 8]);
                    self.stack.set(
                        flags_offset,
                        &content_buffer
                            [8..Self::BLOCK_HEADER_SIZE as usize + requested_size as usize],
                    )?;
                }
                // Payload left as-is: only the flag and reserved words are cleared.
                None => {
                    self.stack.set(flags_offset, [0u8; 8])?;
                }
            }

            Ok(())
        }
    }

    /// Cascade-discard any free blocks sitting at the stack tail.
    ///
    /// Called after every operation that can leave a free block at the tail: the explicit
    /// tail-discard path in `dealloc` and after every `add_to_free_list` call (which may
    /// coalesce the freed block up to the tail).  This maintains the invariant that no free
    /// block ever sits at the stack tail.
    ///
    /// Recovery management: the caller is the sole manager of `recovery_needed` and must set
    /// it before invoking this function and clear it after.  This avoids a double-set under
    /// the CAS-based atomic helpers.
    ///
    /// With the `atomic` feature the caller holds `self.lock` for the duration,
    /// so the free-list unlinks and tail discards here are serialised against other threads.
    fn cascade_discard_free_tail(&self) -> io::Result<()> {
        let arena_start = Self::OFFSET_SIZE + Self::HEADER_SIZE;
        loop {
            let tail = self.stack.len()?;
            if tail <= arena_start {
                break;
            }
            // Read the footer of the last block to get its size
            let mut footer_buf = [0u8; 8];
            self.stack
                .get_into(tail - Self::BLOCK_FOOTER_SIZE, &mut footer_buf)?;
            let sz = u64::from_le_bytes(footer_buf);
            // Validate: size must be at least minimum, 8-aligned, and fit within the arena
            let Some(hdr) = tail
                .checked_sub(sz + Self::BLOCK_OVERHEAD_SIZE)
                .filter(|&h| h >= arena_start && sz >= Self::MIN_BLOCK_PAYLOAD_SIZE && sz % 8 == 0)
            else {
                break;
            };
            // Cross-check: header size must match footer size and block must be free
            let mut hdr_buf = [0u8; 16];
            self.stack.get_into(hdr, &mut hdr_buf)?;
            let hdr_size = read_buf_le!(hdr_buf, 0 => u64);
            if hdr_buf[8] & 1 == 0 || hdr_size != sz {
                break;
            }
            // New tail is a free block; unlink it and discard it
            self.unlink_from_free_list(hdr + Self::BLOCK_HEADER_SIZE)?;
            self.stack.discard(sz + Self::BLOCK_OVERHEAD_SIZE)?;
        }
        Ok(())
    }

    fn recovery(&self) -> io::Result<()> {
        // Walk the stack and rebuild the free list in memory, then write it back to disk.
        // This is needed when the allocator detects corruption or an unclean shutdown.
        // The free list is reconstructed by scanning through all blocks and treating any block
        // with an invalid size or missing free flag as allocated, while valid free blocks are
        // added to the free list.  This allows recovery from various forms of corruption,
        // including torn writes that partially update a block header or footer.
        let arena_start = Self::OFFSET_SIZE + Self::HEADER_SIZE;
        let stack_len = self.stack.len()?;
        let mut pos = arena_start;
        let mut free_blocks: Vec<u64> = Vec::new();

        while pos < stack_len {
            let remaining = stack_len - pos;

            // If fewer than BLOCK_OVERHEAD_SIZE bytes remain, a partial block was written; truncate.
            if remaining < Self::BLOCK_OVERHEAD_SIZE {
                self.stack.discard(remaining)?;
                break;
            }

            // Read block header: size(8) + flags(4) + reserved(4)
            let mut hdr_buf = [0u8; 16];
            self.stack.get_into(pos, &mut hdr_buf)?;
            let mut size = read_buf_le!(hdr_buf, 0 => u64);
            let is_free = hdr_buf[8] & 1 != 0;

            // Validate: size must be ≥ minimum, 8-aligned, and the full block must fit in the stack.
            let mut block_total = match size.checked_add(Self::BLOCK_OVERHEAD_SIZE).filter(|&t| {
                size >= Self::MIN_BLOCK_PAYLOAD_SIZE && size % 8 == 0 && pos + t <= stack_len
            }) {
                Some(t) => t,
                None => {
                    if size < Self::MIN_BLOCK_PAYLOAD_SIZE
                        || size % 8 != 0
                        || size.checked_add(Self::BLOCK_OVERHEAD_SIZE).is_none()
                    {
                        // The header does not describe a valid block. Two cases:
                        //   * All-zero trailing region → an interrupted tail-grow
                        //     `realloc`, which `extend`s (zero-filling) the payload
                        //     before rewriting the header/footer to cover it. The
                        //     valid block ends at `pos` and the zeros beyond it have
                        //     no header (`size` reads 0). A real block is never
                        //     all-zero (size ≥ MIN_BLOCK_PAYLOAD_SIZE), so roll the
                        //     extension back by truncating to `pos` — restoring the
                        //     pre-grow tail the failed `realloc` handed back.
                        //   * Anything else → genuine mid-arena corruption; fail
                        //     loudly rather than discard the data that follows.
                        if self.stack.get(pos, stack_len)?.iter().all(|&b| b == 0) {
                            self.stack.discard(remaining)?;
                            break;
                        }
                        return Err(io_error!(
                            InvalidData,
                            format!(
                                "recovery: corrupted block header at offset {pos}: \
                                 invalid size {size}; manual repair required"
                            )
                        ));
                    }
                    // Size is valid but the block extends past the stack end: partial tail write.
                    self.stack.discard(stack_len - pos)?;
                    break;
                }
            };

            // Detect a partially-completed split: the header size H may still point past
            // the inner footer to the outer footer of the second sub-block (value F < H).
            // Validate the three-point pattern:
            //   • footer at pos+HEADER+H        says F  (second sub-block's footer)
            //   • footer at pos+HEADER+R        says R  (first sub-block's inner footer)
            //   • header at pos+HEADER+R+FOOTER says F  (second sub-block's header size)
            // where R = H − F − OVERHEAD.  If all match, the header was never shrunk;
            // fix it to R so the scan navigates into the two sub-blocks correctly.
            {
                let mut outer_footer_buf = [0u8; 8];
                // footer_pos = pos + HEADER + H; within bounds because block_total was valid
                self.stack
                    .get_into(pos + Self::BLOCK_HEADER_SIZE + size, &mut outer_footer_buf)?;
                let f = u64::from_le_bytes(outer_footer_buf);
                if f != size
                    && f >= Self::MIN_BLOCK_PAYLOAD_SIZE
                    && f % 8 == 0
                    && let Some(r) = size
                        .checked_sub(f)
                        .and_then(|d| d.checked_sub(Self::BLOCK_OVERHEAD_SIZE))
                        .filter(|&r| r >= Self::MIN_BLOCK_PAYLOAD_SIZE && r % 8 == 0)
                {
                    let inner_footer_pos = pos + Self::BLOCK_HEADER_SIZE + r;
                    let second_hdr_pos = inner_footer_pos + Self::BLOCK_FOOTER_SIZE;
                    if second_hdr_pos + Self::BLOCK_HEADER_SIZE <= stack_len {
                        let mut inner_footer_buf = [0u8; 8];
                        let mut second_size_buf = [0u8; 8];
                        self.stack
                            .get_into(inner_footer_pos, &mut inner_footer_buf)?;
                        self.stack.get_into(second_hdr_pos, &mut second_size_buf)?;
                        if u64::from_le_bytes(inner_footer_buf) == r
                            && u64::from_le_bytes(second_size_buf) == f
                        {
                            // Confirmed partial split: update the header to the correct size.
                            self.stack.set(pos, r.to_le_bytes().as_slice())?;
                            size = r;
                            block_total = r + Self::BLOCK_OVERHEAD_SIZE;
                        }
                    }
                }
            }

            // Normalize the footer to the (authoritative) header size. Every
            // block-resizing operation commits its new size to the header before
            // the matching footer — a coalescing free writes header then footer,
            // a tail grow writes header then footer, a split's header is fixed by
            // the partial-split check above — so on a crash between those two
            // writes the header is correct and the footer is stale. The walk
            // follows headers, so a stale footer slips through undetected here yet
            // corrupts a later neighbour's coalesce (which reads this footer) and
            // eventually desyncs the walk. Rewriting the footer to match makes the
            // block whole. Healthy blocks already agree, so this is a no-op for
            // them.
            let footer_pos = pos + Self::BLOCK_HEADER_SIZE + size;
            let mut footer_buf = [0u8; 8];
            self.stack.get_into(footer_pos, &mut footer_buf)?;
            if u64::from_le_bytes(footer_buf) != size {
                self.stack.set(footer_pos, size.to_le_bytes().as_slice())?;
            }

            if is_free {
                free_blocks.push(pos + Self::BLOCK_HEADER_SIZE);
            }
            pos += block_total;
        }

        // Rebuild the free list: rewrite next_free/prev_free for each free block in encounter order,
        // ignoring all stored pointer values.
        let count = free_blocks.len();
        for i in 0..count {
            let curr = free_blocks[i];
            let next = if i + 1 < count { free_blocks[i + 1] } else { 0 };
            let prev = if i > 0 { free_blocks[i - 1] } else { 0 };
            let mut ptr_buf = [0u8; 16];
            write_buf!(next => ptr_buf, 0);
            write_buf!(prev => ptr_buf, 8);
            self.stack.set(curr, ptr_buf)?;
        }

        // Update free_head to the first free block found, or 0 if none.
        let new_free_head = free_blocks.first().copied().unwrap_or(0);
        self.stack
            .set(Self::FREE_HEAD_OFFSET, new_free_head.to_le_bytes())?;

        // Authoritative reset: recovery may have been triggered with the on-disk flag already
        // clear (e.g. an out-of-range free_head in `new`), so write 0 directly rather than via
        // the CAS clear, which under the `atomic` feature would fail when the flag is not 1.
        self.stack.set(Self::OFFSET_SIZE + 8, [0u8; 4].as_slice())
    }
}

#[cfg(feature = "set")]
impl BStackAllocator for FirstFitBStackAllocator {
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

    #[inline]
    fn alloc(&self, len: u64) -> io::Result<BStackOwnedSlice<'_, Self>> {
        self.alloc_impl(len, true)
    }

    fn dealloc<'a>(
        &'a self,
        slice: BStackOwnedSlice<'a, Self>,
    ) -> Result<(), BStackAllocError<'a, Self>> {
        let slice = ensure_own_handle(self, slice, "FirstFitBStackAllocator::dealloc")?;
        let start = slice.start();
        let len = slice.len();
        // Set to true once the block is being physically reclaimed and can no
        // longer be safely handed back to the caller.
        let mut lost = false;
        let result = (|| -> io::Result<()> {
            if slice.is_empty() && slice.start() == 0 {
                return Ok(());
            }

            // Use the aligned block size for validation: the user-visible len may be smaller than
            // MIN_BLOCK_PAYLOAD_SIZE (e.g. alloc(5) returns a 5-byte slice backed by a 16-byte block).
            let aligned_len = self.align_len(slice.len());
            if self.is_impossible_block_start(slice.start())
                || self.is_impossible_block_end(slice.start() + aligned_len)
                || self.is_impossible_block_size(aligned_len)
            {
                return Err(io_error!(
                    InvalidInput,
                    "invalid slice: start or end offset is impossible"
                ));
            }
            // Double-free detection: the block must not already be marked free.
            let mut flags_buf = [0u8; 4];
            self.stack
                .get_into(slice.start() - Self::BLOCK_HEADER_SIZE + 8, &mut flags_buf)?;
            if flags_buf[0] & 1 != 0 {
                return Err(io_error!(
                    InvalidInput,
                    "double-free: block is already free"
                ));
            }

            // Hold the lock across the tail check and the free-list mutation / tail discard, so
            // both the read of the tail and the write that follows are atomic w.r.t. other threads.
            #[cfg(feature = "atomic")]
            let _guard = self.lock.lock().unwrap();

            // Special case for dealloc of the tail block:
            // if slice.start() + aligned_len == self.len() - Self::BLOCK_FOOTER_SIZE, just discard it.
            let current_tail = self.stack.len()?;
            if slice.start() + aligned_len == current_tail - Self::BLOCK_FOOTER_SIZE {
                // Set recovery_needed before the bare discard (0.2.1 fix) so a crash anywhere
                // in the discard + cascade sequence is detected on reopen — without it, a crash
                // between the discard and cascade's first unlink could leave a free block at the
                // new tail with the flag clear, violating the "tail is always allocated" invariant
                // silently.  This function is the sole manager of the flag for the tail path;
                // cascade_discard_free_tail does not touch it.
                self.set_recovery_needed()?;
                // Past this point the tail block is being physically discarded.
                lost = true;
                self.stack
                    .discard(aligned_len + Self::BLOCK_OVERHEAD_SIZE)?;
                self.cascade_discard_free_tail()?;
                return self.clear_recovery_needed();
            }
            self.set_recovery_needed()?;
            // Past this point the block is being pushed onto the free list.
            lost = true;
            self.add_to_free_list(slice.start())?;
            self.cascade_discard_free_tail()?;
            self.clear_recovery_needed()
        })();
        result.map_err(|source| BStackAllocError {
            source,
            handle: if lost {
                None
            } else {
                // SAFETY: (start, len) still describes the live, unmodified block.
                Some(unsafe { BStackOwnedSlice::from_raw_parts(self, start, len) })
            },
        })
    }

    #[inline]
    fn realloc<'a>(
        &'a self,
        slice: BStackOwnedSlice<'a, Self>,
        new_len: u64,
    ) -> Result<BStackOwnedSlice<'a, Self>, BStackAllocError<'a, Self>> {
        self.realloc_impl(slice, new_len, true)
    }
}

#[cfg(feature = "set")]
impl BStackUninitAllocator for FirstFitBStackAllocator {
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

/// Reclaiming a free block without scrubbing its payload.
///
/// A freed block keeps its previous occupant's bytes: `dealloc` rewrites the
/// header, footer and free-list links and nothing else. `alloc` therefore stages
/// a whole `24 + aligned_len` block image and writes it, so that clearing the
/// block's flags word (or writing the split block's footer) also scrubs the
/// payload in the same `set`.
///
/// [`alloc_uninit`](BStackUninitAllocator::alloc_uninit) drops the image: the
/// reuse path writes only the 8-byte flags word (no split) or the 8-byte footer
/// (split), and the tail path replaces the full-block `push` with one
/// [`BStack::extend_sparse_batched`] carrying just the header and footer size
/// words. The call count is identical — the saving is in bytes written and
/// journalled, and in the staging allocation, both of which scale with the
/// request.
///
/// [`realloc_uninit`](BStackUninitAllocator::realloc_uninit) drops the four
/// `zero` calls that scrub bytes newly exposed inside the caller's own block —
/// each a whole durable sync — and shortens the merge path's single large write
/// down to the metadata it has to place. Its move paths are byte-for-byte the
/// zeroing ones: there the payload scrub rides along with a copy that has to be
/// written anyway, so skipping it would buy nothing and splitting it out would
/// cost an extra sync.
///
/// Crash consistency is unchanged throughout. Every path issues the same
/// [`BStack`] calls in the same order under the same `recovery_needed`
/// bracketing, and the bytes left unscrubbed are always interior to a live
/// block, which the recovery scan never reads — it walks block headers from the
/// arena start and strides by their recorded sizes.
#[cfg(feature = "set")]
impl FirstFitBStackAllocator {
    /// Shared body of [`alloc`](BStackAllocator::alloc) and
    /// [`alloc_uninit`](BStackUninitAllocator::alloc_uninit).
    ///
    /// [`true`] stages a full block image so the block's metadata and a
    /// scrub of its payload land in one `set`; [`false`] drops the image
    /// and writes the metadata words alone. Both branches issue the same
    /// [`BStack`] calls in the same order — a reuse still writes the free-list
    /// links, the flags word and the footer, and a miss still creates the block
    /// at the tail in one call — so `recovery_needed` bracketing and the recovery
    /// scan are unaffected.
    fn alloc_impl(&self, len: u64, init: bool) -> io::Result<BStackOwnedSlice<'_, Self>> {
        if len == 0 {
            return Ok(BStackOwnedSlice::empty(self));
        }

        // Make len aligned to 8 bytes and at least 16
        let aligned_len = self.align_len(len);

        // Heap allocate zero buffer; both branches below need a buffer of this exact
        // size to fuse their metadata write with the payload scrub. `false`
        // scrubs nothing, so it writes the metadata words alone and skips the
        // allocation entirely.
        let mut buf = init.then(|| vec![0u8; (Self::BLOCK_OVERHEAD_SIZE + aligned_len) as usize]);

        // Hold the lock across the free-list search and the mutation/extension that follows,
        // so the read-modify-write of the free list (and any tail push) is atomic w.r.t. other
        // threads. recovery_needed is set only around the actual mutation below.
        #[cfg(feature = "atomic")]
        let _guard = self.lock.lock().unwrap();

        let block_found = self.find_large_enough_block(aligned_len)?;
        if block_found.0 != 0 {
            // Found a big enough block at offset block_found. Remove it from the free list and return it.
            // If the block is much bigger than needed, split it and add the remainder back to the free list.

            // Set recovery needed before modifying the free list and clear it after,
            // so that if a crash happens in the middle, the allocator can detect it and recover the free list in the next run.
            self.set_recovery_needed()?;
            self.unlink_block(
                block_found.0,
                block_found.1,
                aligned_len,
                buf.as_deref_mut(),
            )?;
            self.clear_recovery_needed()?;
            // Split puts the allocated block at the back of the found block;
            // no-split uses the found block in full from the front.
            // Must mirror unlink_block's split threshold exactly.
            let payload = if block_found.1
                >= aligned_len + Self::BLOCK_OVERHEAD_SIZE + Self::MIN_BLOCK_PAYLOAD_SIZE
            {
                block_found.0 + block_found.1 - aligned_len
            } else {
                block_found.0
            };
            Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, payload, len) })
        } else {
            // No free block fits; create the block at the tail in one call. Both
            // forms are a single atomic BStack call, so no recovery_needed marking
            // is required; the lock above already excludes concurrent tail
            // modification.
            let block_len = Self::BLOCK_OVERHEAD_SIZE + aligned_len;
            let ptr = match buf.as_mut() {
                // Push the full block: header + zero payload + footer.
                Some(buf) => {
                    write_buf!(aligned_len => buf, 0);
                    write_buf!(aligned_len => buf, (aligned_len + Self::BLOCK_HEADER_SIZE) as usize);
                    self.stack.push(&*buf)?
                }
                // Only the header and footer size words have to be written: one
                // sparse extend realises the whole block with a single `set_len`
                // and writes just those two, so the payload between them costs no
                // write I/O. It reads back as zero — a permitted "unspecified"
                // value — and needs no staging buffer.
                None => {
                    let size = aligned_len.to_le_bytes();
                    self.stack.extend_sparse_batched(
                        [(0, size), (Self::BLOCK_HEADER_SIZE + aligned_len, size)],
                        block_len,
                    )?
                }
            } + Self::BLOCK_HEADER_SIZE;
            // SAFETY: ptr and len from a fresh tail allocation of `block_len` bytes
            Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, ptr, len) })
        }
    }

    /// Try to satisfy a grow to `aligned_new_len` **in place** by absorbing the
    /// immediately following block when it is free and large enough (splitting
    /// off any remainder back onto the free list). Returns `true` if the block
    /// was grown in place at `start`, `false` if the next block is unavailable
    /// and the caller must relocate instead.
    ///
    /// `block_size` is the current physical payload size, `user_len` the
    /// caller-visible length (for scrubbing stale bytes), and `init` whether
    /// newly exposed bytes must be zeroed. The caller must hold the lock (under
    /// `atomic`); this method sets and clears `recovery_needed` around its own
    /// mutations. On an I/O failure mid-merge the error propagates with
    /// `recovery_needed` left set for the next reopen.
    fn try_grow_into_next_free(
        &self,
        start: u64,
        block_size: u64,
        aligned_new_len: u64,
        user_len: u64,
        init: bool,
    ) -> io::Result<bool> {
        // Special case: next block is free and can be merged in place to accommodate the new size.
        // This avoids copying data.

        let next_block = start + block_size + Self::BLOCK_OVERHEAD_SIZE;
        if next_block <= self.stack.len()? - Self::BLOCK_FOOTER_SIZE - Self::MIN_BLOCK_PAYLOAD_SIZE
        {
            let mut next_hdr_buf = [0u8; 16];
            self.stack
                .get_into(next_block - Self::BLOCK_HEADER_SIZE, &mut next_hdr_buf)?;
            let next_block_size = read_buf_le!(next_hdr_buf, 0 => u64);
            let next_block_is_free = next_hdr_buf[8] & 1 != 0;

            // Validate: next_block_size must be ≥ minimum, 8-aligned, and large enough to hold
            // the new size when merged with the current block, and free
            if next_block_is_free
                && next_block_size >= Self::MIN_BLOCK_PAYLOAD_SIZE
                && next_block_size % 8 == 0
                && block_size + Self::BLOCK_OVERHEAD_SIZE + next_block_size >= aligned_new_len
            {
                // Pre-zero the stale bytes between the user-visible len and the existing
                // block's payload end.  After the merge, those bytes become part of the
                // larger user-visible slice and must be zero.  Done before set_recovery_needed
                // because it is an idempotent single-write that doesn't change user-visible
                // state (the bytes are not part of the input slice's len), so a crash here
                // leaves the file in a fully consistent allocator state with no recovery
                // needed.
                if user_len < block_size && init {
                    self.stack.zero(start + user_len, block_size - user_len)?;
                }

                // Unlink the next block from the free list, then merge it into the current block.
                self.set_recovery_needed()?;
                self.unlink_from_free_list(next_block)?;
                // merged_size includes the overhead bytes absorbed from between the two blocks
                let merged_size = block_size + Self::BLOCK_OVERHEAD_SIZE + next_block_size;

                // Buffer covering [start+block_size, start+merged_size+FOOTER).
                // Used for both the no-split and split paths.
                let mut zero_buff = vec![
                    0u8;
                    (next_block_size + Self::BLOCK_OVERHEAD_SIZE + Self::BLOCK_FOOTER_SIZE)
                        as usize
                ];

                if merged_size
                    >= aligned_new_len + Self::BLOCK_OVERHEAD_SIZE + Self::MIN_BLOCK_PAYLOAD_SIZE
                {
                    // The merged block is much larger than needed — split it.
                    // Pack the allocated-block footer, free-block header (size + is_free flag),
                    // free-list next/prev pointers, and free-block footer into zero_buff so
                    // they all land in one write.
                    let remainder_size = merged_size - aligned_new_len - Self::BLOCK_OVERHEAD_SIZE;
                    let new_free_start = start + aligned_new_len + Self::BLOCK_OVERHEAD_SIZE;
                    let mut head_buf = [0u8; 8];
                    self.stack.get_into(Self::FREE_HEAD_OFFSET, &mut head_buf)?;
                    let old_head = u64::from_le_bytes(head_buf);

                    // All offsets are relative to zero_buff[0] = start + block_size.
                    let alloc_footer_off = (aligned_new_len - block_size) as usize;
                    let free_hdr_off = alloc_footer_off + Self::BLOCK_FOOTER_SIZE as usize;
                    let free_payload_off = alloc_footer_off + Self::BLOCK_OVERHEAD_SIZE as usize;
                    let free_footer_off = (next_block_size + Self::BLOCK_OVERHEAD_SIZE) as usize;

                    write_buf!(aligned_new_len => zero_buff, alloc_footer_off);
                    write_buf!(remainder_size => zero_buff, free_hdr_off);
                    write_buf!(1u32 => zero_buff, free_hdr_off + 8); // is_free = 1
                    write_buf!(old_head => zero_buff, free_payload_off); // next_free = old head
                    // prev_free stays 0
                    write_buf!(remainder_size => zero_buff, free_footer_off);

                    // Set the header to merged_size first so that if we crash after the
                    // big write but before the aligned_new_len update, recovery sees a
                    // header/footer mismatch (merged_size vs. remainder_size) and can
                    // detect and repair the partial split.
                    self.stack
                        .set(start - Self::BLOCK_HEADER_SIZE, merged_size.to_le_bytes())?;
                    // Single write: zeroes the inter-block overhead, writes the allocated
                    // block's new footer, the complete free block, and the free block's footer.
                    // `false` starts the same write at the allocated block's
                    // footer instead, dropping only the leading scrub — the bytes it
                    // would have cleared are all payload of the caller's own resized
                    // block, and every metadata word still lands in this one call.
                    let write_from = if init { 0 } else { alloc_footer_off };
                    self.stack.set(
                        start + block_size + write_from as u64,
                        &zero_buff[write_from..],
                    )?;
                    // Shrink the allocated block's header to the used size.
                    self.stack.set(
                        start - Self::BLOCK_HEADER_SIZE,
                        aligned_new_len.to_le_bytes(),
                    )?;
                    // Link forward: free_head → new free block
                    // Failure cause: orphaned block
                    self.stack
                        .set(Self::FREE_HEAD_OFFSET, new_free_start.to_le_bytes())?;
                    // Link backward: old head's prev_free → new free block
                    // Failure cause: orphaned block with stale forward link from old head (detectable in recovery) but no backward link
                    if old_head != 0 {
                        self.stack.set(old_head + 8, new_free_start.to_le_bytes())?;
                    }
                } else {
                    // No split: write the merged block's header and footer.
                    self.stack
                        .set(start - Self::BLOCK_HEADER_SIZE, merged_size.to_le_bytes())?;
                    let footer_off = (next_block_size + Self::BLOCK_OVERHEAD_SIZE) as usize;
                    if init {
                        write_buf!(merged_size => zero_buff, footer_off);
                        self.stack.set(start + block_size, &zero_buff)?;
                    } else {
                        // Everything before the footer is absorbed overhead and
                        // payload of the caller's own block, so the same single
                        // call need only place the merged footer.
                        self.stack
                            .set(start + merged_size, merged_size.to_le_bytes())?;
                    }
                }
                self.clear_recovery_needed()?;
                // SAFETY: slice resized by merging with adjacent free block
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Shared body of [`realloc`](BStackAllocator::realloc) and
    /// [`realloc_uninit`](BStackUninitAllocator::realloc_uninit).
    ///
    /// [`false`] drops the four `zero` calls that exist purely to scrub
    /// bytes newly exposed inside a block the caller already owns, and shortens
    /// (never splits) the merge path's single big write. The move paths keep
    /// their full-size fused write: separating the copied prefix from the block
    /// metadata would turn one durable sync into two, which costs more than the
    /// bytes it saves.
    fn realloc_impl<'a>(
        &'a self,
        slice: BStackOwnedSlice<'a, Self>,
        new_len: u64,
        init: bool,
    ) -> Result<BStackOwnedSlice<'a, Self>, BStackAllocError<'a, Self>> {
        let slice = ensure_own_handle(self, slice, "FirstFitBStackAllocator::realloc")?;
        if slice.is_empty() && slice.start() == 0 {
            // Original is an empty handle; hand it back on failure.
            return self.alloc(new_len).map_err(|source| {
                BStackAllocError::with_handle(source, BStackOwnedSlice::empty(self))
            });
        }
        if new_len == 0 {
            // dealloc consumes `slice`; on failure it returns the handle inside
            // its own BStackAllocError, which propagates unchanged via `?`.
            self.dealloc(slice)?;
            return Ok(BStackOwnedSlice::empty(self));
        }

        let start = slice.start();
        let old_len = slice.len();
        // The surviving allocation to hand back on failure. Starts as the
        // original block; in the move-to-new-block paths it becomes the new
        // region once that region is committed and populated (which happens
        // before the old block is freed), so a later failure freeing the old
        // block still returns a valid, fully-resized handle.
        let mut recovered = (start, old_len);
        let result = (|| -> io::Result<BStackOwnedSlice<'a, Self>> {
            // Use the aligned block size for validation (same reason as dealloc).
            let aligned_current_len = self.align_len(slice.len());
            if self.is_impossible_block_start(slice.start())
                || self.is_impossible_block_end(slice.start() + aligned_current_len)
                || self.is_impossible_block_size(aligned_current_len)
            {
                return Err(io_error!(
                    InvalidInput,
                    "invalid slice: start or end offset is impossible"
                ));
            }

            let aligned_new_len = self.align_len(new_len);

            // If the new length.next_multiple_of(8) is the same as the old length.next_multiple_of(8)
            // the block stays put.  When growing the user-visible len within the same alignment
            // bucket (e.g. 17 → 20, both align to 24), bytes [slice.len(), new_len) may still
            // hold stale data from a previous larger slice, so zero them in a single atomic write.
            if aligned_new_len == aligned_current_len {
                if new_len > slice.len() && init {
                    // Atomic safety:
                    // We do not need to set recovery_needed here because the write is atomic
                    // and since this is not a free block, it should not be in the free list and thus
                    // never modified by other threads that run this allocator concurrently.
                    self.stack
                        .zero(slice.start() + slice.len(), new_len - slice.len())?;
                }
                // SAFETY: same offset, new_len within the existing allocated block
                return Ok(unsafe {
                    BStackOwnedSlice::from_raw_parts(self, slice.start(), new_len)
                });
            }

            // Special case for realloc of the tail block:
            // The tail block cannot be shrunk below Self::MIN_BLOCK_PAYLOAD_SIZE. This is enforced
            // by the align_len function, so if new_len is smaller than that, aligned_new_len will be the same as
            // aligned_current_len and we will just return the same slice without shrinking.
            // if slice.start() + aligned_current_len == self.len() - Self::BLOCK_FOOTER_SIZE, just extend or discard.
            //
            // Hold the lock for the whole tail check + resize: reading the tail and then
            // extending/discarding it must be atomic w.r.t. other threads' pushes. If this is not
            // the tail block the guard is dropped and the lock-free in-place paths below run.
            {
                #[cfg(feature = "atomic")]
                let _guard = self.lock.lock().unwrap();
                let current_tail = self.stack.len()?;
                if slice.start() + aligned_current_len == current_tail - Self::BLOCK_FOOTER_SIZE {
                    match aligned_new_len.cmp(&aligned_current_len) {
                        // Reconstruct rather than move `slice` out of the closure's borrow.
                        std::cmp::Ordering::Equal => {
                            return Ok(unsafe {
                                BStackOwnedSlice::from_raw_parts(self, slice.start(), slice.len())
                            });
                        } // Included but this should never happen
                        std::cmp::Ordering::Greater => {
                            self.set_recovery_needed()?;
                            // Extend payload by the delta; footer moves forward
                            self.stack.extend(aligned_new_len - aligned_current_len)?;
                            // Zero from slice.len() through the old footer area.  The old footer
                            // (8 bytes at aligned_current_len) is now absorbed into the new payload,
                            // and bytes [slice.len(), aligned_current_len) may hold stale data from
                            // a prior larger slice — both must be cleared in one atomic write.
                            if init {
                                self.stack.zero(
                                    slice.start() + slice.len(),
                                    aligned_current_len + Self::BLOCK_FOOTER_SIZE - slice.len(),
                                )?;
                            }
                            self.stack.set(
                                slice.start() - Self::BLOCK_HEADER_SIZE,
                                aligned_new_len.to_le_bytes(),
                            )?;
                            self.stack.set(
                                slice.start() + aligned_new_len,
                                aligned_new_len.to_le_bytes(),
                            )?;
                            self.clear_recovery_needed()?;
                            // SAFETY: slice extended in place at tail
                            return Ok(unsafe {
                                BStackOwnedSlice::from_raw_parts(self, slice.start(), new_len)
                            });
                        }
                        std::cmp::Ordering::Less => {
                            // Keep the block; don't reclaim the tail in place. A
                            // physical shrink needs a header write plus a discard
                            // (metadata + size change) that cannot be one
                            // crash-atomic call, and the block-walking recovery
                            // cannot parse the torn intermediate. Narrowing only
                            // the user length (an oversized block, as a non-tail
                            // shrink does) needs no writes; the tail is reclaimed
                            // on free.
                            // SAFETY: same block, new_len ≤ old len ≤ block size.
                            return Ok(unsafe {
                                BStackOwnedSlice::from_raw_parts(self, slice.start(), new_len)
                            });
                        }
                    }
                }
            }

            // Special case: same block optimizations
            // Read the block size
            let mut block_size_buf = [0u8; 8];
            self.stack
                .get_into(slice.start() - Self::BLOCK_HEADER_SIZE, &mut block_size_buf)?;
            let block_size = u64::from_le_bytes(block_size_buf);
            if block_size >= aligned_new_len {
                // Atomic safety: Same block, not in free list
                // The block is already big enough.  When growing past the previous user-visible
                // len, zero bytes [slice.len(), new_len) in one atomic write — this covers both
                // the gap [slice.len(), aligned_current_len) (potentially stale) and the
                // newly-exposed range [aligned_current_len, new_len).
                if new_len > slice.len() && init {
                    self.stack
                        .zero(slice.start() + slice.len(), new_len - slice.len())?;
                }
                // SAFETY: new_len fits within existing block size
                return Ok(unsafe {
                    BStackOwnedSlice::from_raw_parts(self, slice.start(), new_len)
                });
            }

            // From here on we either merge with an adjacent free block or allocate a fresh one:
            // both walk/mutate the free list (and the new-block path may push), so hold the lock
            // for the remainder of the function. recovery_needed is set only around each mutation.
            #[cfg(feature = "atomic")]
            let _guard = self.lock.lock().unwrap();

            // Grow in place by absorbing the immediately following free block
            // if it is free and large enough. Runs under the lock held above.
            if self.try_grow_into_next_free(
                slice.start(),
                block_size,
                aligned_new_len,
                slice.len(),
                init,
            )? {
                // SAFETY: block resized in place by merging the next free block.
                return Ok(unsafe {
                    BStackOwnedSlice::from_raw_parts(self, slice.start(), new_len)
                });
            }

            // For non-tail blocks, we need to find a new block for the new size, copy the data, and free the old block.
            let block_found = self.find_large_enough_block(aligned_new_len)?;
            // Both branches build a full block buffer (overhead + payload) of the same size,
            // so allocate it once up front.
            let mut block_buf = vec![0u8; (aligned_new_len + Self::BLOCK_OVERHEAD_SIZE) as usize];
            if block_found.0 != 0 {
                // Found a big enough block at offset block_found. Remove it from the free list and return it.
                // If the block is much bigger than needed, split it and add the remainder back to the free list.

                // Copy only the user-visible bytes from the old block into the new block's
                // buffer; bytes beyond `slice.len()` in the buffer stay at the zero-init from
                // `vec!`, so the new block's payload past `slice.len()` is zero — matching
                // extend/calloc semantics for newly-exposed bytes after realloc.
                let copy_len = slice.len().min(aligned_new_len);
                self.stack.get_into(
                    slice.start(),
                    &mut block_buf[Self::BLOCK_HEADER_SIZE as usize
                        ..(copy_len + Self::BLOCK_HEADER_SIZE) as usize],
                )?;
                self.set_recovery_needed()?;
                self.unlink_block(
                    block_found.0,
                    block_found.1,
                    aligned_new_len,
                    Some(block_buf.as_mut_slice()),
                )?;
                // Must mirror unlink_block's split threshold exactly.  The split puts the
                // allocated block at the back of the found block; without a split, the found
                // block is used in full from the front.
                let new_payload = if block_found.1
                    >= aligned_new_len + Self::BLOCK_OVERHEAD_SIZE + Self::MIN_BLOCK_PAYLOAD_SIZE
                {
                    block_found.0 + block_found.1 - aligned_new_len
                } else {
                    block_found.0
                };
                // The new block is committed and populated; it is now the
                // survivor, so a failure freeing the old block returns the new
                // region (the old block leaks until crash recovery).
                recovered = (new_payload, new_len);
                self.add_to_free_list(slice.start())?;
                self.cascade_discard_free_tail()?;
                self.clear_recovery_needed()?;
                // SAFETY: new_payload from allocated block via unlink_block
                Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, new_payload, new_len) })
            } else {
                // No free block fits; push the full new block in one call, then free the old one.
                // Copy only the user-visible bytes; the rest of `block_buf` stays zeroed.
                let copy_len = (slice.len().min(aligned_new_len)) as usize;
                write_buf!(aligned_new_len => block_buf, 0);
                self.stack.get_into(
                    slice.start(),
                    &mut block_buf[Self::BLOCK_HEADER_SIZE as usize
                        ..Self::BLOCK_HEADER_SIZE as usize + copy_len],
                )?;
                write_buf!(aligned_new_len => block_buf, (aligned_new_len + Self::BLOCK_HEADER_SIZE) as usize);
                self.set_recovery_needed()?;
                let ptr = self.stack.push(&block_buf)? + Self::BLOCK_HEADER_SIZE;
                // The new block is committed and populated; it is now the
                // survivor, so a failure freeing the old block returns the new
                // region (the old block leaks until crash recovery).
                recovered = (ptr, new_len);
                self.add_to_free_list(slice.start())?;
                self.cascade_discard_free_tail()?;
                self.clear_recovery_needed()?;
                // SAFETY: ptr from fresh allocation via self.stack.push
                Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, ptr, new_len) })
            }
        })();
        result.map_err(|source| BStackAllocError {
            source,
            // SAFETY: `recovered` names a live region owned by the caller — the
            // untouched original, or the committed new region on the move paths.
            handle: Some(unsafe {
                BStackOwnedSlice::from_raw_parts(self, recovered.0, recovered.1)
            }),
        })
    }

    /// Minimum front trim, in bytes, that `realloc_inplace` can perform: the
    /// carved-off front must be a valid standalone free block, whose payload is
    /// `pf - BLOCK_OVERHEAD_SIZE` and must reach `MIN_BLOCK_PAYLOAD_SIZE`.
    const MIN_FRONT_TRIM: u64 = Self::BLOCK_OVERHEAD_SIZE + Self::MIN_BLOCK_PAYLOAD_SIZE; // 40

    /// Read and sanity-check the physical block size from the header at
    /// `start - BLOCK_HEADER_SIZE`. Returns the stored payload size, or an error
    /// if it is not a well-formed, in-bounds block size.
    fn read_block_size(&self, start: u64) -> io::Result<u64> {
        let mut buf = [0u8; 8];
        self.stack
            .get_into(start - Self::BLOCK_HEADER_SIZE, &mut buf)?;
        let size = u64::from_le_bytes(buf);
        let tail = self.stack.len()?;
        // size must be aligned, at least the minimum, and the whole block
        // (header + payload + footer) must fit within the stack.
        let end_ok = start
            .checked_add(size)
            .and_then(|e| e.checked_add(Self::BLOCK_FOOTER_SIZE))
            .is_some_and(|e| e <= tail);
        if size < Self::MIN_BLOCK_PAYLOAD_SIZE || !size.is_multiple_of(8) || !end_ok {
            return Err(io_error!(
                InvalidInput,
                "realloc_inplace: block header reports an invalid size"
            ));
        }
        Ok(size)
    }

    /// One `BLOCK_OVERHEAD_SIZE` boundary write: a free block's footer
    /// (`footer_size`) immediately followed by an *allocated* block's header
    /// (`header_size`, flags word left zero). Written at the shared boundary of
    /// the two blocks (`allocated_header_start − BLOCK_FOOTER_SIZE`) by the
    /// front-shrink and front-grow-shrink paths, which both place a free block
    /// directly before the retained allocation.
    #[inline]
    fn boundary_footer_then_alloc_header(
        footer_size: u64,
        header_size: u64,
    ) -> [u8; Self::BLOCK_OVERHEAD_SIZE as usize] {
        let mut buf = [0u8; Self::BLOCK_OVERHEAD_SIZE as usize];
        write_buf!(footer_size => buf, 0);
        write_buf!(header_size => buf, 8);
        // buf[16..24] stays zero: the allocated block's flags + reserved words.
        buf
    }

    /// Grow the back edge of the block at `start` to `new_len` bytes **in
    /// place** (`new_len > old_len`). Supports the three non-moving paths — the
    /// block is already large enough, it is the tail block and can be extended,
    /// or the immediately following block is free and large enough to merge into
    /// (via [`try_grow_into_next_free`](Self::try_grow_into_next_free)) — and
    /// returns [`io::ErrorKind::Unsupported`] otherwise (which would require
    /// relocating the payload). The untouched original handle is recoverable on
    /// failure; the tail-extend path may return `None` once its multi-write
    /// commit has begun.
    fn grow_back_inplace<'a>(
        &'a self,
        start: u64,
        old_len: u64,
        new_len: u64,
    ) -> Result<BStackOwnedSlice<'a, Self>, BStackAllocError<'a, Self>> {
        let mut lost = false;
        let result = (|| -> io::Result<BStackOwnedSlice<'a, Self>> {
            let block_size = self.read_block_size(start)?;
            let aligned_new = self.align_len(new_len);

            // Already big enough: only the visible length grows. Zero the gap
            // [old_len, new_len) — it may hold stale bytes from a prior occupant
            // — in one atomic write. No structural change, no recovery flag.
            if block_size >= aligned_new {
                if new_len > old_len {
                    self.stack.zero(start + old_len, new_len - old_len)?;
                }
                // SAFETY: new_len fits within the existing block.
                return Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, start, new_len) });
            }

            // Physical growth. Only the tail block can grow without moving.
            #[cfg(feature = "atomic")]
            let _guard = self.lock.lock().unwrap();
            let tail = self.stack.len()?;
            if start + block_size == tail - Self::BLOCK_FOOTER_SIZE {
                self.set_recovery_needed()?;
                lost = true;
                // Extend the payload by the delta; the old footer is absorbed.
                self.stack.extend(aligned_new - block_size)?;
                // Zero [old_len, block_size + FOOTER): the old gap plus the old
                // footer bytes now inside the payload. `extend` already zeroed
                // everything past the old footer.
                self.stack.zero(
                    start + old_len,
                    block_size + Self::BLOCK_FOOTER_SIZE - old_len,
                )?;
                self.stack
                    .set(start - Self::BLOCK_HEADER_SIZE, aligned_new.to_le_bytes())?;
                self.stack
                    .set(start + aligned_new, aligned_new.to_le_bytes())?;
                self.clear_recovery_needed()?;
                // SAFETY: block extended in place at the tail.
                return Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, start, new_len) });
            }

            // Not the tail: absorb the immediately following free block if it is
            // free and large enough — the same in-place merge `realloc` performs.
            // The lock is held above; the helper manages `recovery_needed`. On a
            // mid-merge failure it propagates and the original handle is returned,
            // matching `realloc`'s merge-path contract (the block is left mid-merge
            // with `recovery_needed` set for the next reopen).
            if self.try_grow_into_next_free(start, block_size, aligned_new, old_len, true)? {
                // SAFETY: block grown in place by merging the next free block.
                return Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, start, new_len) });
            }
            Err(io_error!(
                Unsupported,
                "realloc_inplace: back grow would relocate a non-tail block"
            ))
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

    /// Shrink the front edge of the block at `start` by `pf` bytes **in place**,
    /// yielding a handle at `start + pf` with visible length `new_len`.
    ///
    /// The carved-off front `[start, start + pf)` becomes a free block (coalesced
    /// into a free left neighbour if one exists). Requires `pf >=
    /// MIN_FRONT_TRIM` and `pf % 8 == 0` — a smaller or misaligned trim cannot
    /// form a valid, aligned free block and returns
    /// [`io::ErrorKind::Unsupported`]. The retained tail `[start + pf, start +
    /// block_size)` never moves.
    ///
    /// The carve reuses the split shape that [`unlink_block`](Self::unlink_block)
    /// produces, so a torn write is repaired by [`recovery`](Self::recovery)'s
    /// partial-split detection. Once the multi-write carve begins the original
    /// handle can no longer be safely returned, so a mid-carve failure yields
    /// `handle: None` and leaves `recovery_needed` set for the next reopen.
    fn shrink_front_inplace<'a>(
        &'a self,
        start: u64,
        old_len: u64,
        pf: u64,
        new_len: u64,
    ) -> Result<BStackOwnedSlice<'a, Self>, BStackAllocError<'a, Self>> {
        let mut lost = false;
        let result = (|| -> io::Result<BStackOwnedSlice<'a, Self>> {
            // A front trim below the minimum, or one that would misalign the
            // retained block, cannot be done in place.
            if pf < Self::MIN_FRONT_TRIM || !pf.is_multiple_of(8) {
                return Err(io_error!(
                    Unsupported,
                    "realloc_inplace: front trim too small or misaligned to carve in place"
                ));
            }
            let block_size = self.read_block_size(start)?;
            // Retained block payload size after the carve. It must stay a valid
            // block and still cover the requested visible length.
            let retained = match block_size.checked_sub(pf) {
                Some(r) if r >= Self::MIN_BLOCK_PAYLOAD_SIZE && r >= new_len => r,
                _ => {
                    return Err(io_error!(
                        Unsupported,
                        "realloc_inplace: front trim leaves too small a retained block"
                    ));
                }
            };
            let front_payload = pf - Self::BLOCK_OVERHEAD_SIZE; // >= MIN_BLOCK_PAYLOAD_SIZE
            let new_start = start + pf;

            #[cfg(feature = "atomic")]
            let _guard = self.lock.lock().unwrap();
            self.set_recovery_needed()?;
            // The carve is committed from here: the block structure changes and
            // the original (start, old_len) can no longer be handed back.
            lost = true;

            // W1: front block footer (front_payload) + retained block header
            // (size `retained`, flags = allocated) in one write at new_start-24.
            let w1 = Self::boundary_footer_then_alloc_header(front_payload, retained);
            self.stack.set(new_start - Self::BLOCK_OVERHEAD_SIZE, w1)?;

            // W2: retained block footer. This is where the original block's
            // footer sat; overwriting it to `retained` is the point past which
            // recovery must reconstruct the split rather than the whole block.
            self.stack.set(start + block_size, retained.to_le_bytes())?;

            // W3: shrink the front header to `front_payload`, keeping its
            // allocated flag (only the 8-byte size word is rewritten). Committed
            // last so recovery's partial-split check can fire on a torn W2->W3.
            self.stack
                .set(start - Self::BLOCK_HEADER_SIZE, front_payload.to_le_bytes())?;

            // Free the now-allocated front block: marks it free, coalesces a free
            // left neighbour, and prepends it to the free list. It can never be
            // the tail (the retained block follows it), so the cascade is a
            // no-op but is issued for consistency with the free-list contract.
            self.add_to_free_list(start)?;
            self.cascade_discard_free_tail()?;
            self.clear_recovery_needed()?;
            // SAFETY: retained block at new_start with capacity `retained` >= new_len.
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

    /// Minimum front growth, in bytes, that `realloc_inplace` can perform: below
    /// this the carve's metadata write would overlap the old boundary tags, so
    /// a torn write could not be walked back to the original two blocks.
    const MIN_FRONT_GROW: u64 = Self::BLOCK_OVERHEAD_SIZE; // 24

    /// Grow the front edge of the block at `start` by `pg` bytes **in place**,
    /// consuming `pg` bytes from a free *left* neighbour, and yielding a handle
    /// at `start - pg` with visible length `new_len`. The retained bytes never
    /// move; the `pg` newly exposed front bytes are zeroed.
    ///
    /// Requires a free left neighbour of payload `lsize` that either keeps a
    /// valid remainder (`pg <= lsize - MIN_BLOCK_PAYLOAD_SIZE`) or is fully
    /// absorbed (`pg == lsize + BLOCK_OVERHEAD_SIZE`), plus `pg >= MIN_FRONT_GROW`
    /// and `pg % 8 == 0`; anything else returns [`io::ErrorKind::Unsupported`].
    ///
    /// The shrink-remainder path writes the neighbour's new footer + our new
    /// header, then our footer, then shrinks the neighbour header **last** — the
    /// same three-write shape as [`shrink_front_inplace`](Self::shrink_front_inplace)
    /// with the free block on the left, so a torn write walks back to the
    /// original pair (before the last write) or forward to the merged result
    /// (after it). The absorb path unlinks the neighbour, then overwrites its
    /// header with our grown header; a crash before that overwrite leaves the
    /// neighbour marked free for recovery to relink. Once mutation begins the
    /// original handle can no longer be returned (`handle: None`).
    fn grow_front_inplace<'a>(
        &'a self,
        start: u64,
        old_len: u64,
        pg: u64,
        new_len: u64,
    ) -> Result<BStackOwnedSlice<'a, Self>, BStackAllocError<'a, Self>> {
        let mut lost = false;
        let result = (|| -> io::Result<BStackOwnedSlice<'a, Self>> {
            if pg < Self::MIN_FRONT_GROW || !pg.is_multiple_of(8) {
                return Err(io_error!(
                    Unsupported,
                    "realloc_inplace: front growth too small or misaligned to carve in place"
                ));
            }
            let arena_start = Self::OFFSET_SIZE + Self::HEADER_SIZE;
            let our_header = start - Self::BLOCK_HEADER_SIZE;
            if our_header <= arena_start {
                // No room for a left neighbour: this is the first arena block.
                return Err(io_error!(
                    Unsupported,
                    "realloc_inplace: front growth has no left neighbour"
                ));
            }

            // Under the lock so the neighbour read and the free-list mutation are
            // atomic w.r.t. concurrent alloc/dealloc (which pop free blocks).
            #[cfg(feature = "atomic")]
            let _guard = self.lock.lock().unwrap();

            let block_size = self.read_block_size(start)?;
            let our_new_size = match block_size.checked_add(pg) {
                Some(s) => s,
                None => {
                    return Err(io_error!(
                        InvalidInput,
                        "realloc_inplace: grown block size overflows"
                    ));
                }
            };

            // Locate and validate the free left neighbour via its footer tag.
            let mut fbuf = [0u8; 8];
            self.stack
                .get_into(our_header - Self::BLOCK_FOOTER_SIZE, &mut fbuf)?;
            let lsize = u64::from_le_bytes(fbuf);
            let unsupported = || {
                Err(io_error!(
                    Unsupported,
                    "realloc_inplace: front growth needs a large-enough free left neighbour"
                ))
            };
            if lsize < Self::MIN_BLOCK_PAYLOAD_SIZE || !lsize.is_multiple_of(8) {
                return unsupported();
            }
            let l_header = match our_header
                .checked_sub(lsize + Self::BLOCK_OVERHEAD_SIZE)
                .filter(|&h| h >= arena_start)
            {
                Some(h) => h,
                None => return unsupported(),
            };
            let mut lhbuf = [0u8; 16];
            self.stack.get_into(l_header, &mut lhbuf)?;
            let l_hdr_size = read_buf_le!(lhbuf, 0 => u64);
            let l_is_free = lhbuf[8] & 1 != 0;
            if !l_is_free || l_hdr_size != lsize {
                return unsupported();
            }

            // Decide the path first. Only a `pg` that fits the neighbour is
            // valid — Shrink keeps a remainder, Absorb consumes it whole — and any
            // larger `pg` is rejected here. Computing `new_start`/`new_header`
            // before this check would subtract an unvalidated `pg` from `start`
            // and underflow for a too-large front grow (a debug-build panic).
            enum Mode {
                Shrink(u64), // neighbour's new payload size
                Absorb,
            }
            let mode = if pg <= lsize.saturating_sub(Self::MIN_BLOCK_PAYLOAD_SIZE) {
                Mode::Shrink(lsize - pg)
            } else if pg == lsize + Self::BLOCK_OVERHEAD_SIZE {
                Mode::Absorb
            } else {
                return unsupported();
            };
            // `pg` now fits the neighbour, so both blocks stay within the arena and
            // neither subtraction underflows. Both paths keep the retained block
            // anchored and end with our header at `new_header`.
            let new_start = start - pg;
            let new_header = new_start - Self::BLOCK_HEADER_SIZE; // start - pg - 16

            self.set_recovery_needed()?;
            lost = true;

            match mode {
                Mode::Shrink(l_new_size) => {
                    // W1: neighbour's new footer + our new header, one write at
                    // new_header - FOOTER. pg >= 24 keeps this clear of the old
                    // boundary tags at start-24 / start-16.
                    let w1 = Self::boundary_footer_then_alloc_header(l_new_size, our_new_size);
                    self.stack.set(new_header - Self::BLOCK_FOOTER_SIZE, w1)?;
                    // W2: our footer at its (unchanged) position, new size.
                    self.stack
                        .set(start + block_size, our_new_size.to_le_bytes())?;
                    // W3: shrink the neighbour header last (size word only; keeps
                    // its free flag and its intact free-list pointers).
                    self.stack.set(l_header, l_new_size.to_le_bytes())?;
                }
                Mode::Absorb => {
                    // Consume the whole neighbour: unlink it, then overwrite its
                    // header with our grown allocated header and rewrite our footer.
                    self.unlink_from_free_list(l_header + Self::BLOCK_HEADER_SIZE)?;
                    let mut hdr = [0u8; Self::BLOCK_HEADER_SIZE as usize];
                    write_buf!(our_new_size => hdr, 0);
                    // hdr[8..16] = flags(0)+reserved(0) => allocated.
                    self.stack.set(new_header, hdr)?;
                    self.stack
                        .set(start + block_size, our_new_size.to_le_bytes())?;
                }
            }

            // Zero the newly exposed front bytes [new_start, start). They held
            // the neighbour's (now-consumed) tail and the old boundary tags.
            self.stack.zero(new_start, pg)?;
            self.clear_recovery_needed()?;
            // SAFETY: block now spans new_start with capacity our_new_size >= new_len.
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

impl BStackInPlaceResizeAllocator for FirstFitBStackAllocator {
    /// Front and back in-place resize for the first-fit arena.
    ///
    /// | `(prepend, append)`                | Behaviour                                              |
    /// |------------------------------------|--------------------------------------------------------|
    /// | `append <= 0`, `prepend == 0`      | narrow the visible length within the block (no I/O)     |
    /// | `append > 0`, `prepend == 0`       | back grow: same-block, else tail-extend, else merge next free block, else `Unsupported` |
    /// | `prepend < 0`, `append <= 0`       | carve the front into a free block, narrow the back      |
    /// | `prepend > 0`, `append == 0`       | grow the front from a free left neighbour               |
    /// | mixed grow/shrink across edges     | `Unsupported`                                           |
    ///
    /// Front shrink requires an 8-aligned trim of at least
    /// `BLOCK_OVERHEAD_SIZE + MIN_BLOCK_PAYLOAD_SIZE` (40) bytes so the carved-off
    /// front is a valid free block; smaller or misaligned trims return
    /// `Unsupported`. The structural paths (front carve, tail extend) bracket
    /// their writes with `recovery_needed` and reproduce the split shape recovery
    /// already repairs; a mid-mutation I/O failure returns `handle: None`.
    fn realloc_inplace<'a>(
        &'a self,
        slice: BStackOwnedSlice<'a, Self>,
        prepend: i64,
        append: i64,
    ) -> Result<BStackOwnedSlice<'a, Self>, BStackAllocError<'a, Self>> {
        // Reject a handle from another allocator instance before any logic runs
        // (see the module's "Foreign handles" section).
        let slice = ensure_own_handle(self, slice, "FirstFitBStackAllocator::realloc_inplace")?;
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

        // Resulting length, validated before any I/O. Guard the cast and both
        // additions so a hostile handle or delta cannot wrap into a bogus length.
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

        // Shrinking to nothing frees the region; delegate to dealloc, which owns
        // the handle-return contract on failure.
        if new_len == 0 {
            return self.dealloc(slice).map(|()| BStackOwnedSlice::empty(self));
        }

        // Validate the handle addresses a real block (mirrors dealloc/realloc).
        let aligned_old = self.align_len(old_len);
        if self.is_impossible_block_start(start)
            || self.is_impossible_block_end(start + aligned_old)
            || self.is_impossible_block_size(aligned_old)
        {
            return Err(BStackAllocError::with_handle(
                io_error!(
                    InvalidInput,
                    "realloc_inplace: slice does not describe a valid block"
                ),
                slice,
            ));
        }

        // `slice` is consumed by the branches below; reconstruct the original
        // handle for the Unsupported paths that never touch the block.
        // SAFETY: (start, old_len) still names the live, unmodified block.
        let unsupported = |msg: &'static str| {
            BStackAllocError::with_handle(io_error!(Unsupported, msg), unsafe {
                BStackOwnedSlice::from_raw_parts(self, start, old_len)
            })
        };

        if prepend == 0 {
            if append <= 0 {
                // Identity or back shrink: the block is untouched, only the
                // visible length changes. new_len <= old_len <= block size.
                // SAFETY: (start, new_len) lies within the existing block.
                return Ok(unsafe { BStackOwnedSlice::from_raw_parts(self, start, new_len) });
            }
            return self.grow_back_inplace(start, old_len, new_len);
        }

        if prepend < 0 {
            if append > 0 {
                return Err(unsupported(
                    "realloc_inplace: simultaneous front shrink and back grow not supported",
                ));
            }
            // prepend < 0, so `-prepend > 0` fits in u64.
            let pf = prepend.unsigned_abs();
            return self.shrink_front_inplace(start, old_len, pf, new_len);
        }

        // prepend > 0: front grow, consuming a free left neighbour.
        if append != 0 {
            return Err(unsupported(
                "realloc_inplace: simultaneous front grow and back resize not supported",
            ));
        }
        // prepend > 0 fits in u64.
        let pg = prepend as u64;
        self.grow_front_inplace(start, old_len, pg, new_len)
    }
}

// Fault-injection failure tests (non-`atomic` white-box; op-agnostic fuzz covers
// the `atomic` build). FirstFit's central crash-consistency invariant is the
// `recovery_needed` flag protocol plus the reopen recovery scan, so the tests
// drive faults across that boundary and confirm the handle contract and that a
// reopen leaves surviving allocations intact.
#[cfg(all(
    test,
    debug_assertions,
    feature = "fault-injection",
    feature = "set",
    not(feature = "atomic")
))]
mod fault_tests {
    use super::FirstFitBStackAllocator;
    use crate::BStack;
    use crate::alloc::{BStackAllocator, BStackInPlaceResizeAllocator};
    use crate::alloc_fuzz::common::{Guard, policies::FailOpAt, temp_path};
    use crate::fault::FaultPolicy;
    use std::io::ErrorKind;
    use std::sync::Arc;

    fn arm(alloc: &FirstFitBStackAllocator, policy: FailOpAt) {
        let policy: Arc<dyn FaultPolicy> = Arc::new(policy);
        alloc.stack().set_fault_policy(Some(policy));
    }
    fn disarm(alloc: &FirstFitBStackAllocator) {
        alloc.stack().set_fault_policy(None);
    }

    // Allocating with no reusable free block pushes a fresh block in one `push`.
    // A fault there surfaces cleanly and leaves the arena unchanged and usable.
    #[test]
    fn alloc_push_fault_surfaces_cleanly() {
        let path = temp_path("ff_alloc");
        let _g = Guard(path.clone());
        let alloc = FirstFitBStackAllocator::new(BStack::open(&path).unwrap()).unwrap();

        arm(&alloc, FailOpAt::new("push", 0, ErrorKind::Other));
        let err = alloc
            .alloc(64)
            .expect_err("alloc must fail when push faults");
        disarm(&alloc);
        assert_eq!(err.kind(), ErrorKind::Other);

        // Arena untouched: a fresh alloc succeeds and round-trips.
        let mut s = alloc.alloc(64).unwrap();
        s.write([7u8; 64]).unwrap();
        assert_eq!(s.read().unwrap(), vec![7u8; 64]);
    }

    // A non-tail free-list `dealloc` sets `recovery_needed` first; faulting that
    // very first `set` fires before any mutation, so the flag is *not* left set,
    // the block stays live, and the handle is returned for a clean retry.
    #[test]
    fn dealloc_nontail_fault_before_mutation_returns_handle() {
        let path = temp_path("ff_retain");
        let _g = Guard(path.clone());
        let alloc = FirstFitBStackAllocator::new(BStack::open(&path).unwrap()).unwrap();

        let _a = alloc.alloc(64).unwrap();
        let mut b = alloc.alloc(64).unwrap();
        b.write([2u8; 64]).unwrap();
        let _c = alloc.alloc(64).unwrap(); // keep b non-tail
        let (b_start, b_len) = (b.start(), b.len());

        arm(&alloc, FailOpAt::new("set", 0, ErrorKind::Other));
        let err = alloc
            .dealloc(b)
            .expect_err("dealloc must fail when the flag write faults");
        disarm(&alloc);

        let handle = err
            .handle
            .expect("fault before mutation must return the surviving handle");
        assert_eq!((handle.start(), handle.len()), (b_start, b_len));
        assert_eq!(handle.read().unwrap(), vec![2u8; 64], "data must be intact");
        // Retry (unarmed) frees it cleanly.
        alloc.dealloc(handle).unwrap();
    }

    // A tail `dealloc` sets `recovery_needed`, then faults its `discard`. The
    // block is reported lost (handle `None`), the flag is left set on disk, and a
    // reopen runs the recovery scan; surviving allocations must be intact and the
    // allocator usable afterwards.
    #[test]
    fn dealloc_tail_discard_fault_recovers_on_reopen() {
        let path = temp_path("ff_tail");
        let _g = Guard(path.clone());

        let (a_start, b_start) = {
            let alloc = FirstFitBStackAllocator::new(BStack::open(&path).unwrap()).unwrap();
            let mut a = alloc.alloc(64).unwrap();
            a.write([1u8; 64]).unwrap();
            let mut b = alloc.alloc(64).unwrap();
            b.write([2u8; 64]).unwrap();
            let mut c = alloc.alloc(64).unwrap();
            c.write([3u8; 64]).unwrap();
            let (a_start, b_start) = (a.start(), b.start());

            arm(&alloc, FailOpAt::new("discard", 0, ErrorKind::Other));
            let err = alloc
                .dealloc(c)
                .expect_err("tail dealloc must fail when discard faults");
            disarm(&alloc);
            assert!(
                err.handle.is_none(),
                "past the lost point the handle must not be returned"
            );
            drop(alloc);
            (a_start, b_start)
        };

        // Reopen runs the recovery scan (recovery_needed was set).
        let alloc = FirstFitBStackAllocator::new(BStack::open(&path).unwrap()).unwrap();
        assert_eq!(
            alloc.stack().get(a_start, a_start + 64).unwrap(),
            vec![1u8; 64]
        );
        assert_eq!(
            alloc.stack().get(b_start, b_start + 64).unwrap(),
            vec![2u8; 64]
        );

        // Allocator is consistent and usable.
        let mut d = alloc.alloc(128).unwrap();
        d.write([4u8; 128]).unwrap();
        assert_eq!(d.read().unwrap(), vec![4u8; 128]);
    }

    // A tail-block-grow `realloc` `extend`s (zero-filling) the payload before
    // rewriting the header/footer to cover it. A fault in that window strands a
    // zero-filled tail region with no block header; the recovery scan used to
    // read it as a size-0 block and reject the whole file. Recovery now rolls
    // the extension back by truncation, so reopen succeeds, the block's bytes
    // survive, and the allocator stays usable. Faulting the post-`extend` `zero`
    // and the header `set` covers both stranded-tail variants.
    fn tail_grow_fault_recovers(fault_op: &'static str) {
        let path = temp_path(&format!("ff_tailgrow_{fault_op}"));
        let _g = Guard(path.clone());

        let (start, old_len) = {
            let alloc = FirstFitBStackAllocator::new(BStack::open(&path).unwrap()).unwrap();
            // A single allocation is the tail block, so the grow takes the
            // in-place tail-extend path. 866 → 941 crosses an 8-byte alignment
            // bucket (872 → 944), so the grow really extends the file.
            let mut a = alloc.alloc(866).unwrap();
            a.write([0xA7u8; 866]).unwrap();
            let (start, old_len) = (a.start(), a.len());

            arm(&alloc, FailOpAt::new(fault_op, 0, ErrorKind::Other));
            let err = alloc
                .realloc(a, 941)
                .expect_err("realloc must report the injected fault");
            disarm(&alloc);
            // The grow faulted before committing: the original block is handed back.
            let handle = err.handle.expect("the original block must survive");
            assert_eq!((handle.start(), handle.len()), (start, old_len));
            (start, old_len)
        };

        // Reopen runs recovery (recovery_needed was left set); it must not error.
        let alloc = FirstFitBStackAllocator::new(BStack::open(&path).unwrap())
            .expect("recovery must roll back the interrupted tail grow, not error");
        assert_eq!(
            alloc.stack().get(start, start + old_len).unwrap(),
            vec![0xA7u8; old_len as usize],
            "the block's bytes survive the rolled-back grow"
        );
        let mut d = alloc.alloc(200).unwrap();
        d.write([0x5Cu8; 200]).unwrap();
        assert_eq!(d.read().unwrap(), vec![0x5Cu8; 200]);
    }

    #[test]
    fn tail_grow_fault_at_zero_recovers() {
        tail_grow_fault_recovers("zero");
    }

    #[test]
    fn tail_grow_fault_at_header_set_recovers() {
        tail_grow_fault_recovers("set");
    }

    /// Walk the arena block-by-block (by header size) and assert every block's
    /// footer equals its header — the invariant recovery must restore. Panics on
    /// the first mismatch or an unparseable header, so a stale footer left by an
    /// interrupted coalesce is caught directly.
    fn assert_arena_footers_match(alloc: &FirstFitBStackAllocator, ctx: &str) {
        const ARENA_START: u64 = 48;
        const HDR: u64 = 16;
        const OVERHEAD: u64 = 24;
        const MIN: u64 = 16;
        let stack = alloc.stack();
        let stack_len = stack.len().unwrap();
        let mut pos = ARENA_START;
        while pos + OVERHEAD <= stack_len {
            let mut hb = [0u8; 8];
            stack.get_into(pos, &mut hb).unwrap();
            let size = u64::from_le_bytes(hb);
            assert!(
                size >= MIN && size % 8 == 0 && pos + size + OVERHEAD <= stack_len,
                "{ctx}: unparseable header {size} at {pos}"
            );
            let mut fb = [0u8; 8];
            stack.get_into(pos + HDR + size, &mut fb).unwrap();
            assert_eq!(
                u64::from_le_bytes(fb),
                size,
                "{ctx}: footer≠header at block {pos}"
            );
            pos += size + OVERHEAD;
        }
    }

    // Freeing a block sandwiched between two free blocks coalesces all three:
    // `add_to_free_list` writes the merged block's header, then its footer, as
    // two separate `set`s. A fault between them leaves header=merged-size but a
    // stale footer. The recovery walk follows headers, so the merged block spans
    // correctly and the flag is cleared — the mismatch slips through. Left there,
    // the stale footer (it still equals the right sub-block's size, so it points
    // back at that sub-block's untouched interior header) later lets a
    // neighbour's left-coalesce walk into the merged block's interior and
    // coalesce onto a ghost header, overlapping two blocks and eventually
    // desyncing the walk. Recovery now normalizes every block's footer to its
    // (authoritative, written-first) header. Sweep the fault across the
    // coalescing free's writes and assert the arena is whole after recovery.
    #[test]
    fn dealloc_three_way_coalesce_footer_fault_recovers() {
        for at in 0..16u64 {
            let path = temp_path(&format!("ff_coalesce_{at}"));
            let _g = Guard(path.clone());

            let g0 = {
                let alloc = FirstFitBStackAllocator::new(BStack::open(&path).unwrap()).unwrap();
                // guard0 | A | B | C | tail, all non-tail middle blocks. Distinct
                // sizes so the stale-footer geometry matches the fuzz's finding.
                let mut g0 = alloc.alloc(64).unwrap();
                g0.write([0x10u8; 64]).unwrap();
                let a = alloc.alloc(56).unwrap();
                let b = alloc.alloc(64).unwrap();
                let c = alloc.alloc(32).unwrap();
                let _tail = alloc.alloc(64).unwrap();
                let g0s = g0.start();

                // Free the two outer blocks so B is sandwiched between free A and C.
                alloc.dealloc(a).unwrap();
                alloc.dealloc(c).unwrap();

                // Free B: a three-way coalesce. Fault its `at`-th `set`.
                arm(&alloc, FailOpAt::new("set", at, ErrorKind::Other));
                let _ = alloc.dealloc(b); // may fault (past-lost → handle None) or succeed
                disarm(&alloc);
                g0s
            };

            // Reopen runs recovery; it must not error on the interrupted coalesce.
            let alloc = FirstFitBStackAllocator::new(BStack::open(&path).unwrap())
                .unwrap_or_else(|e| panic!("at={at}: recovery must not error: {e}"));
            // Recovery must have made every block whole (header == footer); a stale
            // merged-block footer is exactly the bug.
            assert_arena_footers_match(&alloc, &format!("at={at}"));
            // Untouched neighbour survives, and the allocator stays usable.
            assert_eq!(
                alloc.stack().get(g0, g0 + 64).unwrap(),
                vec![0x10u8; 64],
                "at={at}: left guard intact"
            );
            let mut r = alloc.alloc(240).unwrap();
            r.write([0x22u8; 240]).unwrap();
            assert_eq!(
                r.read().unwrap(),
                vec![0x22u8; 240],
                "at={at}: reuse reads back"
            );
            alloc.dealloc(r).unwrap();
            assert_arena_footers_match(&alloc, &format!("at={at} after reuse"));
        }
    }

    // Front shrink writes are: set_recovery_needed (set #0), W1 inner footer +
    // retained header (#1), W2 retained footer (#2), W3 shrink front header (#3).
    // Faulting W1 (before the split commits) must leave the original block whole;
    // faulting W3 (after W1+W2, the partial-split state) must let recovery
    // reconstruct the [free front | retained] pair with the retained bytes intact.
    #[test]
    fn front_shrink_fault_before_commit_recovers() {
        let path = temp_path("ff_fshrink_pre");
        let _g = Guard(path.clone());
        let start = {
            let alloc = FirstFitBStackAllocator::new(BStack::open(&path).unwrap()).unwrap();
            let mut h = alloc.alloc(100).unwrap();
            h.write([0xC3u8; 100]).unwrap();
            let start = h.start();
            arm(&alloc, FailOpAt::new("set", 1, ErrorKind::Other)); // fault W1
            let err = alloc
                .realloc_inplace(h, -40, 0)
                .expect_err("front shrink must report the fault");
            disarm(&alloc);
            assert!(err.handle.is_none(), "past the lost point: handle is None");
            drop(alloc);
            start
        };
        let alloc = FirstFitBStackAllocator::new(BStack::open(&path).unwrap()).unwrap();
        assert_arena_footers_match(&alloc, "front shrink W1 fault");
        // The whole original block survived untouched.
        assert_eq!(
            alloc.stack().get(start, start + 100).unwrap(),
            vec![0xC3u8; 100]
        );
        let mut d = alloc.alloc(64).unwrap();
        d.write([5u8; 64]).unwrap();
        assert_eq!(d.read().unwrap(), vec![5u8; 64]);
    }

    #[test]
    fn front_shrink_fault_after_split_recovers() {
        let path = temp_path("ff_fshrink_split");
        let _g = Guard(path.clone());
        let start = {
            let alloc = FirstFitBStackAllocator::new(BStack::open(&path).unwrap()).unwrap();
            let mut h = alloc.alloc(100).unwrap();
            h.write([0xC3u8; 100]).unwrap();
            let start = h.start();
            arm(&alloc, FailOpAt::new("set", 3, ErrorKind::Other)); // fault W3
            let err = alloc
                .realloc_inplace(h, -40, 0)
                .expect_err("front shrink must report the fault");
            disarm(&alloc);
            assert!(err.handle.is_none());
            drop(alloc);
            start
        };
        let alloc = FirstFitBStackAllocator::new(BStack::open(&path).unwrap()).unwrap();
        assert_arena_footers_match(&alloc, "front shrink W3 fault");
        // Retained bytes [40, 100) live on at the carved offset start+40.
        assert_eq!(
            alloc.stack().get(start + 40, start + 100).unwrap(),
            vec![0xC3u8; 60]
        );
        let mut d = alloc.alloc(64).unwrap();
        d.write([6u8; 64]).unwrap();
        assert_eq!(d.read().unwrap(), vec![6u8; 64]);
    }

    // Front grow (shrink-neighbour) writes: set_recovery_needed (#0), W1 neighbour
    // footer + our header (#1), W2 our footer (#2), W3 shrink neighbour header (#3).
    // Faulting W3 leaves the partial state that recovery rolls back to the original
    // [free neighbour | our block] pair; our bytes must be intact.
    #[test]
    fn front_grow_fault_recovers() {
        let path = temp_path("ff_fgrow");
        let _g = Guard(path.clone());
        let b_start = {
            let alloc = FirstFitBStackAllocator::new(BStack::open(&path).unwrap()).unwrap();
            let a = alloc.alloc(100).unwrap();
            let mut b = alloc.alloc(50).unwrap();
            b.write([0x9Bu8; 50]).unwrap();
            let b_start = b.start();
            alloc.dealloc(a).map_err(|e| e.source).unwrap();
            arm(&alloc, FailOpAt::new("set", 3, ErrorKind::Other)); // fault W3
            let err = alloc
                .realloc_inplace(b, 32, 0)
                .expect_err("front grow must report the fault");
            disarm(&alloc);
            assert!(err.handle.is_none());
            drop(alloc);
            b_start
        };
        let alloc = FirstFitBStackAllocator::new(BStack::open(&path).unwrap()).unwrap();
        assert_arena_footers_match(&alloc, "front grow W3 fault");
        // Our block rolled back in place; its bytes survive at the original offset.
        assert_eq!(
            alloc.stack().get(b_start, b_start + 50).unwrap(),
            vec![0x9Bu8; 50]
        );
        let mut d = alloc.alloc(64).unwrap();
        d.write([8u8; 64]).unwrap();
        assert_eq!(d.read().unwrap(), vec![8u8; 64]);
    }
}

// In-place resize (`BStackInPlaceResizeAllocator`) and owned-slice subslice/join.
#[cfg(all(test, feature = "set"))]
mod inplace_resize_tests {
    use super::FirstFitBStackAllocator;
    use crate::BStack;
    use crate::alloc::{BStackAllocator, BStackInPlaceResizeAllocator};
    use std::io::ErrorKind;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Guard(std::path::PathBuf);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn open_fresh() -> (FirstFitBStackAllocator, Guard) {
        static CTR: AtomicU64 = AtomicU64::new(0);
        let id = CTR.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("bstack_ff_ipr_{pid}_{id}.bin"));
        let alloc = FirstFitBStackAllocator::new(BStack::open(&path).unwrap()).unwrap();
        (alloc, Guard(path))
    }

    fn pattern(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i % 251) as u8).collect()
    }

    #[test]
    fn front_shrink_preserves_retained_bytes_and_frees_front() {
        let (alloc, _g) = open_fresh();
        let mut h = alloc.alloc(100).unwrap();
        h.write(pattern(100)).unwrap();
        let (orig_start, orig_len) = (h.start(), h.len());

        // Trim 40 bytes off the front (pf = 40 = MIN_FRONT_TRIM).
        let r = alloc.realloc_inplace(h, -40, 0).unwrap();
        assert_eq!(r.start(), orig_start + 40);
        assert_eq!(r.len(), orig_len - 40);
        assert_eq!(r.read().unwrap(), pattern(100)[40..100].to_vec());

        // The carved-off front (payload 16) is a free block reusable at its
        // original offset.
        let reused = alloc.alloc(16).unwrap();
        assert_eq!(reused.start(), orig_start);
        alloc.dealloc(reused).map_err(|e| e.source).unwrap();
        alloc.dealloc(r).map_err(|e| e.source).unwrap();
    }

    #[test]
    fn front_and_back_shrink_together() {
        let (alloc, _g) = open_fresh();
        let mut h = alloc.alloc(100).unwrap();
        h.write(pattern(100)).unwrap();
        let orig_start = h.start();

        let r = alloc.realloc_inplace(h, -40, -8).unwrap();
        assert_eq!(r.start(), orig_start + 40);
        assert_eq!(r.len(), 52);
        assert_eq!(r.read().unwrap(), pattern(100)[40..92].to_vec());
    }

    #[test]
    fn empty_handle_is_always_unsupported() {
        let (alloc, _g) = open_fresh();
        // Every (prepend, append) on an empty handle, including the no-op, is
        // Unsupported and returns the handle untouched.
        for (p, a) in [(0, 0), (0, 16), (16, 0), (-16, 16)] {
            let h = alloc.alloc(0).unwrap();
            assert!(h.is_empty());
            let err = alloc.realloc_inplace(h, p, a).unwrap_err();
            assert_eq!(err.source.kind(), ErrorKind::Unsupported);
            assert!(err.handle.is_some());
        }
    }

    #[test]
    fn back_grow_tail_extends_in_place() {
        let (alloc, _g) = open_fresh();
        let mut h = alloc.alloc(50).unwrap();
        h.write(pattern(50)).unwrap();
        let orig_start = h.start();

        let r = alloc.realloc_inplace(h, 0, 40).unwrap();
        assert_eq!(r.start(), orig_start);
        assert_eq!(r.len(), 90);
        let got = r.read().unwrap();
        assert_eq!(&got[..50], &pattern(50)[..]);
        assert_eq!(&got[50..], &vec![0u8; 40][..]);
    }

    #[test]
    fn back_grow_merges_next_free_block() {
        let (alloc, _g) = open_fresh();
        let mut a = alloc.alloc(50).unwrap();
        a.write(pattern(50)).unwrap();
        let a_start = a.start();
        let b = alloc.alloc(50).unwrap();
        let _c = alloc.alloc(10).unwrap(); // keep `b` non-tail so it stays on the free list
        alloc.dealloc(b).map_err(|e| e.source).unwrap(); // `b` is now a's free right neighbour

        // Non-tail, block too small: grows by absorbing the free right neighbour.
        let r = alloc.realloc_inplace(a, 0, 50).unwrap();
        assert_eq!(r.start(), a_start);
        assert_eq!(r.len(), 100);
        let got = r.read().unwrap();
        assert_eq!(&got[..50], &pattern(50)[..]);
        assert_eq!(&got[50..], &vec![0u8; 50][..]);
    }

    #[test]
    fn back_grow_within_block_is_inplace() {
        let (alloc, _g) = open_fresh();
        let mut h = alloc.alloc(5).unwrap(); // 16-byte block backs a 5-byte slice
        h.write(pattern(5)).unwrap();
        let r = alloc.realloc_inplace(h, 0, 3).unwrap();
        assert_eq!(r.len(), 8);
        let got = r.read().unwrap();
        assert_eq!(&got[..5], &pattern(5)[..]);
        assert_eq!(&got[5..], &[0u8, 0, 0]);
    }

    #[test]
    fn back_shrink_narrows_visible_len() {
        let (alloc, _g) = open_fresh();
        let mut h = alloc.alloc(100).unwrap();
        h.write(pattern(100)).unwrap();
        let orig_start = h.start();
        let r = alloc.realloc_inplace(h, 0, -40).unwrap();
        assert_eq!(r.start(), orig_start);
        assert_eq!(r.len(), 60);
        assert_eq!(r.read().unwrap(), pattern(100)[..60].to_vec());
    }

    #[test]
    fn front_grow_is_unsupported_and_returns_handle() {
        let (alloc, _g) = open_fresh();
        let h = alloc.alloc(50).unwrap();
        let err = alloc.realloc_inplace(h, 8, 0).unwrap_err();
        assert_eq!(err.source.kind(), ErrorKind::Unsupported);
        let back = err.handle.expect("front grow must return the handle");
        assert_eq!(back.len(), 50);
    }

    #[test]
    fn small_front_trim_is_unsupported() {
        let (alloc, _g) = open_fresh();
        let h = alloc.alloc(100).unwrap();
        // pf = 8 < MIN_FRONT_TRIM (40): cannot carve a valid free block.
        let err = alloc.realloc_inplace(h, -8, 0).unwrap_err();
        assert_eq!(err.source.kind(), ErrorKind::Unsupported);
        assert!(err.handle.is_some());
    }

    #[test]
    fn negative_result_length_is_invalid_input() {
        let (alloc, _g) = open_fresh();
        let h = alloc.alloc(50).unwrap();
        let err = alloc.realloc_inplace(h, 0, -60).unwrap_err();
        assert_eq!(err.source.kind(), ErrorKind::InvalidInput);
        assert!(err.handle.is_some());
    }

    #[test]
    fn front_shrink_survives_reopen() {
        let path = {
            let (alloc, g) = open_fresh();
            let p = g.0.clone();
            std::mem::forget(g); // keep the file for reopen
            let mut h = alloc.alloc(100).unwrap();
            h.write(pattern(100)).unwrap();
            let r = alloc.realloc_inplace(h, -40, 0).unwrap();
            alloc.dealloc(r).map_err(|e| e.source).unwrap();
            drop(alloc);
            p
        };
        let _g = Guard(path.clone());
        // Reopen: recovery must accept the arena and the allocator stays usable.
        let alloc = FirstFitBStackAllocator::new(BStack::open(&path).unwrap()).unwrap();
        let mut s = alloc.alloc(64).unwrap();
        s.write([9u8; 64]).unwrap();
        assert_eq!(s.read().unwrap(), vec![9u8; 64]);
    }

    #[test]
    fn front_grow_shrinks_free_left_neighbour() {
        let (alloc, _g) = open_fresh();
        let a = alloc.alloc(100).unwrap();
        let mut b = alloc.alloc(50).unwrap();
        b.write(pattern(50)).unwrap();
        let b_start = b.start();
        alloc.dealloc(a).map_err(|e| e.source).unwrap(); // `a` is now b's free left neighbour

        let r = alloc.realloc_inplace(b, 32, 0).unwrap();
        assert_eq!(r.start(), b_start - 32);
        assert_eq!(r.len(), 82);
        let got = r.read().unwrap();
        assert_eq!(&got[..32], &vec![0u8; 32][..]); // newly exposed front, zeroed
        assert_eq!(&got[32..], &pattern(50)[..]); // retained bytes, unmoved
    }

    #[test]
    fn front_grow_absorbs_whole_free_left_neighbour() {
        let (alloc, _g) = open_fresh();
        let a = alloc.alloc(100).unwrap(); // aligned block payload = 104
        let mut b = alloc.alloc(50).unwrap();
        b.write(pattern(50)).unwrap();
        let b_start = b.start();
        alloc.dealloc(a).map_err(|e| e.source).unwrap();

        // pg = lsize + BLOCK_OVERHEAD_SIZE = 104 + 24 = 128 fully absorbs `a`.
        let r = alloc.realloc_inplace(b, 128, 0).unwrap();
        assert_eq!(r.start(), b_start - 128);
        assert_eq!(r.len(), 178);
        let got = r.read().unwrap();
        assert_eq!(&got[..128], &vec![0u8; 128][..]);
        assert_eq!(&got[128..], &pattern(50)[..]);
    }

    #[test]
    fn front_grow_far_larger_than_neighbour_is_unsupported_not_panic() {
        // Regression: `grow_front_inplace` computed `new_start = start - pg` (and
        // `new_start - BLOCK_HEADER_SIZE`) *before* the Shrink/Absorb decision
        // validated that `pg` fits the neighbour. A front grow larger than the
        // block's own offset therefore underflowed — a debug-build panic — instead
        // of returning `Unsupported`. It must now reject cleanly with the handle
        // intact, for any oversized `pg`.
        let (alloc, _g) = open_fresh();
        let a = alloc.alloc(100).unwrap();
        let mut b = alloc.alloc(50).unwrap();
        b.write(pattern(50)).unwrap();
        let b_start = b.start();
        alloc.dealloc(a).map_err(|e| e.source).unwrap(); // `a` is b's free left neighbour

        // `pg` dwarfs both the neighbour (payload 104) and `b_start`, so the old
        // `start - pg` would underflow. Neither Shrink nor Absorb matches, so the
        // correct answer is `Unsupported`.
        let err = alloc.realloc_inplace(b, 4096, 0).unwrap_err();
        assert_eq!(err.source.kind(), ErrorKind::Unsupported);
        let b = err
            .handle
            .expect("oversized front grow must return the handle");
        assert_eq!(b.start(), b_start, "block not moved");
        assert_eq!(b.read().unwrap(), pattern(50), "original bytes intact");
    }

    #[test]
    fn front_grow_without_left_neighbour_is_unsupported() {
        let (alloc, _g) = open_fresh();
        let b = alloc.alloc(50).unwrap(); // first arena block: no left neighbour
        let err = alloc.realloc_inplace(b, 32, 0).unwrap_err();
        assert_eq!(err.source.kind(), ErrorKind::Unsupported);
        assert!(err.handle.is_some());
    }

    #[test]
    fn front_grow_with_allocated_left_neighbour_is_unsupported() {
        let (alloc, _g) = open_fresh();
        let _a = alloc.alloc(100).unwrap(); // allocated, not freed
        let b = alloc.alloc(50).unwrap();
        let err = alloc.realloc_inplace(b, 32, 0).unwrap_err();
        assert_eq!(err.source.kind(), ErrorKind::Unsupported);
        assert!(err.handle.is_some());
    }

    #[test]
    fn small_front_grow_is_unsupported() {
        let (alloc, _g) = open_fresh();
        let a = alloc.alloc(100).unwrap();
        let b = alloc.alloc(50).unwrap();
        alloc.dealloc(a).map_err(|e| e.source).unwrap();
        // pg = 8 < MIN_FRONT_GROW (24).
        let err = alloc.realloc_inplace(b, 8, 0).unwrap_err();
        assert_eq!(err.source.kind(), ErrorKind::Unsupported);
        assert!(err.handle.is_some());
    }

    #[test]
    fn front_grow_survives_reopen() {
        let path = {
            let (alloc, g) = open_fresh();
            let p = g.0.clone();
            std::mem::forget(g);
            let a = alloc.alloc(100).unwrap();
            let mut b = alloc.alloc(50).unwrap();
            b.write(pattern(50)).unwrap();
            alloc.dealloc(a).map_err(|e| e.source).unwrap();
            let r = alloc.realloc_inplace(b, 32, 0).unwrap();
            alloc.dealloc(r).map_err(|e| e.source).unwrap();
            drop(alloc);
            p
        };
        let _g = Guard(path.clone());
        let alloc = FirstFitBStackAllocator::new(BStack::open(&path).unwrap()).unwrap();
        let mut s = alloc.alloc(64).unwrap();
        s.write([7u8; 64]).unwrap();
        assert_eq!(s.read().unwrap(), vec![7u8; 64]);
    }

    #[test]
    fn realloc_inplace_rejects_foreign_handle() {
        let (a1, _g1) = open_fresh();
        let (a2, _g2) = open_fresh();
        let h = a1.alloc(64).unwrap();
        let err = a2.realloc_inplace(h, 0, 8).unwrap_err();
        assert_eq!(err.source.kind(), ErrorKind::InvalidInput);
        let h = err
            .handle
            .expect("a refused handle is returned, not leaked");
        a1.dealloc(h).map_err(|e| e.source).unwrap();
    }
}

// Owned-slice subslice/join built on in-place resize (needs `set` + `atomic`).
#[cfg(all(test, feature = "set", feature = "atomic"))]
mod owned_slice_subslice_join_tests {
    use super::FirstFitBStackAllocator;
    use crate::BStack;
    use crate::alloc::BStackAllocator;
    use std::io::ErrorKind;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Guard(std::path::PathBuf);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn open_fresh() -> (FirstFitBStackAllocator, Guard) {
        static CTR: AtomicU64 = AtomicU64::new(0);
        let id = CTR.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("bstack_ff_join_{pid}_{id}.bin"));
        let alloc = FirstFitBStackAllocator::new(BStack::open(&path).unwrap()).unwrap();
        (alloc, Guard(path))
    }

    fn pattern(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i % 251) as u8).collect()
    }

    #[test]
    fn try_subslice_inplace_returns_window() {
        let (alloc, _g) = open_fresh();
        let mut h = alloc.alloc(100).unwrap();
        h.write(pattern(100)).unwrap();
        let r = h.try_subslice_inplace(40, 90).unwrap();
        assert_eq!(r.len(), 50);
        assert_eq!(r.read().unwrap(), pattern(100)[40..90].to_vec());
    }

    #[test]
    fn try_subslice_inplace_small_front_is_unsupported() {
        let (alloc, _g) = open_fresh();
        let mut h = alloc.alloc(100).unwrap();
        h.write(pattern(100)).unwrap();
        let err = h.try_subslice_inplace(10, 50).unwrap_err();
        assert_eq!(err.source.kind(), ErrorKind::Unsupported);
        assert!(err.handle.is_some());
    }

    #[test]
    fn try_subslice_falls_back_when_inplace_unsupported() {
        let (alloc, _g) = open_fresh();
        let mut h = alloc.alloc(100).unwrap();
        h.write(pattern(100)).unwrap();
        // start = 10 (< MIN_FRONT_TRIM) forces the alloc+copy fallback.
        let r = h.try_subslice(10, 50).unwrap();
        assert_eq!(r.len(), 40);
        assert_eq!(r.read().unwrap(), pattern(100)[10..50].to_vec());
    }

    #[test]
    fn try_join_inplace_grows_self_tail() {
        let (alloc, _g) = open_fresh();
        let mut other = alloc.alloc(30).unwrap();
        other.write(pattern(30)).unwrap();
        let mut selfh = alloc.alloc(20).unwrap(); // tail block
        selfh.write(vec![0xEE; 20]).unwrap();

        let joined = selfh.try_join_inplace(other).map_err(|e| e.source).unwrap();
        assert_eq!(joined.len(), 50);
        let got = joined.read().unwrap();
        assert_eq!(&got[..20], &vec![0xEE; 20][..]);
        assert_eq!(&got[20..], &pattern(30)[..]);
    }

    #[test]
    fn try_join_falls_back_when_no_inplace_direction() {
        let (alloc, _g) = open_fresh();
        let mut other = alloc.alloc(30).unwrap();
        other.write(pattern(30)).unwrap();
        let mut selfh = alloc.alloc(20).unwrap();
        selfh.write(vec![0xEE; 20]).unwrap();
        let _keep_tail = alloc.alloc(10).unwrap(); // make selfh non-tail

        let joined = selfh.try_join(other).map_err(|e| e.source).unwrap();
        assert_eq!(joined.len(), 50);
        let got = joined.read().unwrap();
        assert_eq!(&got[..20], &vec![0xEE; 20][..]);
        assert_eq!(&got[20..], &pattern(30)[..]);
    }

    #[test]
    fn try_join_with_empty_returns_other() {
        let (alloc, _g) = open_fresh();
        let mut a = alloc.alloc(16).unwrap();
        a.write(pattern(16)).unwrap();
        let empty = crate::alloc::BStackOwnedSlice::empty(&alloc);
        let joined = a.try_join_inplace(empty).map_err(|e| e.source).unwrap();
        assert_eq!(joined.read().unwrap(), pattern(16));
    }

    #[test]
    fn try_join_inplace_uses_front_grow_mirror() {
        let (alloc, _g) = open_fresh();
        // `x` becomes a free left neighbour of `other`; `self` is non-tail and
        // its block cannot grow, so the join must extend `other`'s front.
        let x = alloc.alloc(100).unwrap();
        let mut other = alloc.alloc(50).unwrap();
        other.write(pattern(50)).unwrap();
        let mut selfh = alloc.alloc(40).unwrap();
        selfh.write(vec![0xEE; 40]).unwrap();
        let _keep_tail = alloc.alloc(10).unwrap(); // selfh is non-tail
        alloc.dealloc(x).map_err(|e| e.source).unwrap(); // free left neighbour of `other`

        let joined = selfh.try_join_inplace(other).map_err(|e| e.source).unwrap();
        assert_eq!(joined.len(), 90);
        let got = joined.read().unwrap();
        assert_eq!(&got[..40], &vec![0xEE; 40][..]); // self, copied into other's grown front
        assert_eq!(&got[40..], &pattern(50)[..]); // other, never moved
    }

    #[test]
    fn try_join_rejects_other_from_another_allocator() {
        let (a1, _g1) = open_fresh();
        let (a2, _g2) = open_fresh();
        let s = a1.alloc(30).unwrap();
        let o = a2.alloc(20).unwrap(); // foreign to a1
        let err = s
            .try_join(o)
            .expect_err("join must refuse a foreign `other`");
        assert_eq!(err.source.kind(), ErrorKind::InvalidInput);
        // Both inputs are returned, not leaked: `self` in `first`, `other` in
        // `second`.
        let s = err.first.expect("`self` returned in `first`");
        let o = err.second.expect("`other` returned in `second`");
        a1.dealloc(s).map_err(|e| e.source).unwrap();
        a2.dealloc(o).map_err(|e| e.source).unwrap();
    }
}
