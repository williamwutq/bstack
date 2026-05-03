use super::{BStackAllocator, BStackSlice};
use crate::BStack;
use std::io;

/// Full magic for FirstFitBStackAllocator
#[cfg(feature = "set")]
const ALFF_MAGIC: [u8; 8] = *b"ALFF\x00\x01\x01\x00";

/// Compatibility prefix checked on open: `ALFF` + major 0 + minor 1.
/// Any file whose first 6 bytes match is considered compatible.
#[cfg(feature = "set")]
const ALFF_MAGIC_PREFIX: [u8; 6] = *b"ALFF\x00\x01";

/// A persistent first-fit free-list allocator implementing [`BStackAllocator`]
/// on top of a [`BStack`].
///
/// Unlike [`LinearBStackAllocator`], freed regions are tracked on disk in a
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
/// `recovery_needed` flag in the allocator header before mutating the free
/// list and clears it after all writes complete.  On the next
/// [`FirstFitBStackAllocator::new`] call, if `recovery_needed` is set, a
/// single linear scan of the arena rebuilds the free list from the `is_free`
/// flags in block headers — no stored pointer values are trusted.  Any
/// partial block at the tail is also truncated.  Recovery is O(n) in arena
/// size and runs at most once per crash event.
///
/// # Thread safety
///
/// `FirstFitBStackAllocator` is **neither `Send` nor `Sync`**.  Each instance
/// must be confined to one thread.
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
/// let a = alloc.alloc(64)?;
/// let b = alloc.alloc(64)?;
/// a.write(b"hello world")?;
///
/// alloc.dealloc(a)?;           // freed; coalesced if adjacent to another free block
///
/// let c = alloc.alloc(64)?;    // reuses a's slot
/// assert_eq!(c.start(), a.start());
///
/// let stack = alloc.into_stack();
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "set")]
pub struct FirstFitBStackAllocator {
    stack: BStack,
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
            return Ok(Self { stack });
        }
        // Validate header
        let stack_len = stack.len()?;
        if stack_len < Self::OFFSET_SIZE + Self::HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "stack too short to contain allocator header",
            ));
        }
        let header = stack.get(Self::OFFSET_SIZE, Self::OFFSET_SIZE + Self::HEADER_SIZE)?;
        // Check magic prefix for compatibility with 0.1.x files.
        if header[..ALFF_MAGIC_PREFIX.len()] != ALFF_MAGIC_PREFIX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid magic prefix: expected ALFF\\x00\\x01",
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
        let alloc = Self { stack };
        if recovery_needed {
            alloc.recovery()?;
        }
        Ok(alloc)
    }

    #[inline]
    fn set_recovery_needed(&self) -> io::Result<()> {
        self.stack
            .set(Self::OFFSET_SIZE + 8, 1u32.to_le_bytes().as_slice())
    }

    #[inline]
    fn clear_recovery_needed(&self) -> io::Result<()> {
        self.stack.set(Self::OFFSET_SIZE + 8, [0u8; 4].as_slice())
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
        let next = u64::from_le_bytes(ptrs[0..8].try_into().unwrap());
        let prev = u64::from_le_bytes(ptrs[8..16].try_into().unwrap());
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
        //   4. Tail reclamation: if the merged block ends at the stack tail, discard it entirely.
        //   5. Otherwise, prepend the merged block to the free list.

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
            let next_size = u64::from_le_bytes(next_hdr[0..8].try_into().unwrap());
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
                let prev_hdr_size = u64::from_le_bytes(prev_hdr[0..8].try_into().unwrap());
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
        update_buf[0..4].copy_from_slice(&1u32.to_le_bytes()); // is_free = 1
        update_buf[8..16].copy_from_slice(&next_block.to_le_bytes()); // next_free = old head
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
        let mut head = u64::from_le_bytes(
            self.stack
                .get(Self::FREE_HEAD_OFFSET, Self::FREE_HEAD_OFFSET + 8)?
                .try_into()
                .unwrap(),
        );
        while head != 0 {
            let size_flags_and_ptr_buf = &mut [0u8; Self::BLOCK_HEADER_SIZE as usize + 8];
            self.stack
                .get_into(head - Self::BLOCK_HEADER_SIZE, size_flags_and_ptr_buf)?;
            let block_size = u64::from_le_bytes(size_flags_and_ptr_buf[0..8].try_into().unwrap());
            let is_free = size_flags_and_ptr_buf[8] & 1 != 0;
            debug_assert!(
                is_free,
                "corrupted free list: block at offset {head} is not marked free"
            );
            if !is_free {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("corrupted free list: block at offset {head} is not marked free"),
                ));
            } else if self.is_impossible_block_size(block_size) || block_size % 8 != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "corrupted free list: block at offset {head} has invalid size {block_size}"
                    ),
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
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("corrupted free list: next block offset {head} is invalid"),
                ));
            }
        }

        Ok((block_found, found_size))
    }

    fn unlink_block(
        &self,
        found_start: u64,
        found_size: u64,
        requested_size: u64,
        // Need to be Self::BLOCK_OVERHEAD_SIZE + data size
        // Where from Self::BLOCK_HEADER_SIZE to end - Self::BLOCK_FOOTER_SIZE is the content
        content_buffer: &mut [u8],
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

            // Write the footer of the allocated block to content buffer
            content_buffer[(requested_size + Self::BLOCK_HEADER_SIZE) as usize
                ..(requested_size + Self::BLOCK_OVERHEAD_SIZE) as usize]
                .copy_from_slice(&requested_size.to_le_bytes());

            // Update 1
            // Update the footer of the free block and write the header of the allocated block together
            // Flag and reserved bytes are already 0, so the new block is marked as allocated.
            let update_buf = &mut [0u8; Self::BLOCK_OVERHEAD_SIZE as usize];
            update_buf[..8].copy_from_slice(&remaining_size.to_le_bytes());
            update_buf[8..16].copy_from_slice(&requested_size.to_le_bytes());
            self.stack.set(found_start + remaining_size, update_buf)?;

            // Update 2
            // Update the footer of the allocated block and zero out
            // If this step fails and middle is already updated, nothing bad happens since
            // the middle of a free block is just garbage data
            self.stack.set(
                found_start + remaining_size + Self::BLOCK_OVERHEAD_SIZE,
                &content_buffer[Self::BLOCK_HEADER_SIZE as usize..],
            )?;

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
            let next = u64::from_le_bytes(pointers_buf[0..8].try_into().unwrap());
            let prev = u64::from_le_bytes(pointers_buf[8..16].try_into().unwrap());

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
            content_buffer[8..16].copy_from_slice(&[0u8; 8]);
            self.stack.set(
                found_start - Self::BLOCK_HEADER_SIZE + 8,
                &content_buffer[8..Self::BLOCK_HEADER_SIZE as usize + requested_size as usize],
            )?;

            Ok(())
        }
    }

    /// After discarding the tail block, cascade-discard any free blocks that are now the new tail.
    ///
    /// This maintains the invariant that no free block ever sits at the stack tail, which in turn
    /// makes tail reclamation inside `add_to_free_list` impossible (and therefore omitted).
    ///
    /// Sets `recovery_needed` only if at least one cascade discard is required, and clears it
    /// once after all iterations so the cost is one set + one clear regardless of cascade depth.
    fn cascade_discard_free_tail(&self) -> io::Result<()> {
        let arena_start = Self::OFFSET_SIZE + Self::HEADER_SIZE;
        let mut needs_clear = false;
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
            let hdr_size = u64::from_le_bytes(hdr_buf[0..8].try_into().unwrap());
            if hdr_buf[8] & 1 == 0 || hdr_size != sz {
                break;
            }
            // New tail is a free block; unlink it and discard it
            if !needs_clear {
                self.set_recovery_needed()?;
                needs_clear = true;
            }
            self.unlink_from_free_list(hdr + Self::BLOCK_HEADER_SIZE)?;
            self.stack.discard(sz + Self::BLOCK_OVERHEAD_SIZE)?;
        }
        if needs_clear {
            self.clear_recovery_needed()?;
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
            let mut size = u64::from_le_bytes(hdr_buf[0..8].try_into().unwrap());
            let is_free = hdr_buf[8] & 1 != 0;

            // Validate: size must be ≥ minimum, 8-aligned, and the full block must fit in the stack.
            let mut block_total = match size.checked_add(Self::BLOCK_OVERHEAD_SIZE).filter(|&t| {
                size >= Self::MIN_BLOCK_PAYLOAD_SIZE && size % 8 == 0 && pos + t <= stack_len
            }) {
                Some(t) => t,
                None => {
                    // Corrupt or partial block at the tail; truncate everything from here.
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
            ptr_buf[0..8].copy_from_slice(&next.to_le_bytes());
            ptr_buf[8..16].copy_from_slice(&prev.to_le_bytes());
            self.stack.set(curr, ptr_buf)?;
        }

        // Update free_head to the first free block found, or 0 if none.
        let new_free_head = free_blocks.first().copied().unwrap_or(0);
        self.stack
            .set(Self::FREE_HEAD_OFFSET, new_free_head.to_le_bytes())?;

        self.clear_recovery_needed()
    }
}

#[cfg(feature = "set")]
impl BStackAllocator for FirstFitBStackAllocator {
    fn stack(&self) -> &BStack {
        &self.stack
    }

    fn into_stack(self) -> BStack {
        self.stack
    }

    fn alloc(&self, len: u64) -> io::Result<BStackSlice<'_, Self>> {
        // Make len aligned to 8 bytes and at least 16
        let aligned_len = self.align_len(len);

        let block_found = self.find_large_enough_block(aligned_len)?;
        if block_found.0 != 0 {
            // Found a big enough block at offset block_found. Remove it from the free list and return it.
            // If the block is much bigger than needed, split it and add the remainder back to the free list.

            // Heap allocate zero buffer
            let mut zero_buf = vec![0u8; (Self::BLOCK_OVERHEAD_SIZE + aligned_len) as usize];

            // Set recovery needed before modifying the free list and clear it after,
            // so that if a crash happens in the middle, the allocator can detect it and recover the free list in the next run.
            self.set_recovery_needed()?;
            self.unlink_block(
                block_found.0,
                block_found.1,
                aligned_len,
                zero_buf.as_mut_slice(),
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
            Ok(BStackSlice::new(self, payload, len))
        } else {
            // No free block fits; push the full block (header + zero payload + footer) in one call.
            let mut block_buf = vec![0u8; (aligned_len + Self::BLOCK_OVERHEAD_SIZE) as usize];
            block_buf[..8].copy_from_slice(&aligned_len.to_le_bytes());
            block_buf[(aligned_len + Self::BLOCK_HEADER_SIZE) as usize..]
                .copy_from_slice(&aligned_len.to_le_bytes());
            let ptr = self.stack.push(&block_buf)? + Self::BLOCK_HEADER_SIZE;
            Ok(BStackSlice::new(self, ptr, len))
        }
    }

    fn dealloc(&self, slice: BStackSlice<'_, Self>) -> io::Result<()> {
        // Use the aligned block size for validation: the user-visible len may be smaller than
        // MIN_BLOCK_PAYLOAD_SIZE (e.g. alloc(5) returns a 5-byte slice backed by a 16-byte block).
        let aligned_len = self.align_len(slice.len());
        if self.is_impossible_block_start(slice.start())
            || self.is_impossible_block_end(slice.start() + aligned_len)
            || self.is_impossible_block_size(aligned_len)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid slice: start or end offset is impossible",
            ));
        }
        // Special case for dealloc of the tail block:
        // if slice.start() + aligned_len == self.len() - Self::BLOCK_FOOTER_SIZE, just discard it.
        let current_tail = self.stack.len()?;
        if slice.start() + aligned_len == current_tail - Self::BLOCK_FOOTER_SIZE {
            self.stack
                .discard(aligned_len + Self::BLOCK_OVERHEAD_SIZE)?;
            self.cascade_discard_free_tail()?;
            return Ok(());
        }
        self.set_recovery_needed()?;
        self.add_to_free_list(slice.start())?;
        self.clear_recovery_needed()
    }

    fn realloc<'a>(
        &'a self,
        slice: BStackSlice<'a, Self>,
        new_len: u64,
    ) -> io::Result<BStackSlice<'a, Self>> {
        // Use the aligned block size for validation (same reason as dealloc).
        let aligned_current_len = self.align_len(slice.len());
        if self.is_impossible_block_start(slice.start())
            || self.is_impossible_block_end(slice.start() + aligned_current_len)
            || self.is_impossible_block_size(aligned_current_len)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid slice: start or end offset is impossible",
            ));
        }

        let aligned_new_len = self.align_len(new_len);

        // If the new length.next_multiple_of(8) is the same as the old length.next_multiple_of(8)
        // the block stays put.  When growing the user-visible len within the same alignment
        // bucket (e.g. 17 → 20, both align to 24), bytes [slice.len(), new_len) may still
        // hold stale data from a previous larger slice, so zero them in a single atomic write.
        if aligned_new_len == aligned_current_len {
            if new_len > slice.len() {
                self.stack
                    .zero(slice.start() + slice.len(), new_len - slice.len())?;
            }
            return Ok(BStackSlice::new(self, slice.start(), new_len));
        }

        // Special case for realloc of the tail block:
        // The tail block cannot be shrunk below Self::MIN_BLOCK_PAYLOAD_SIZE. This is enforced
        // by the align_len function, so if new_len is smaller than that, aligned_new_len will be the same as
        // aligned_current_len and we will just return the same slice without shrinking.
        // if slice.start() + aligned_current_len == self.len() - Self::BLOCK_FOOTER_SIZE, just extend or discard.
        let current_tail = self.stack.len()?;
        if slice.start() + aligned_current_len == current_tail - Self::BLOCK_FOOTER_SIZE {
            match aligned_new_len.cmp(&aligned_current_len) {
                std::cmp::Ordering::Equal => return Ok(slice), // Included but this should never happen
                std::cmp::Ordering::Greater => {
                    // Extend payload by the delta; footer moves forward
                    self.stack.extend(aligned_new_len - aligned_current_len)?;
                    // Zero from slice.len() through the old footer area.  The old footer
                    // (8 bytes at aligned_current_len) is now absorbed into the new payload,
                    // and bytes [slice.len(), aligned_current_len) may hold stale data from
                    // a prior larger slice — both must be cleared in one atomic write.
                    self.stack.zero(
                        slice.start() + slice.len(),
                        aligned_current_len + Self::BLOCK_FOOTER_SIZE - slice.len(),
                    )?;
                    self.stack.set(
                        slice.start() - Self::BLOCK_HEADER_SIZE,
                        aligned_new_len.to_le_bytes(),
                    )?;
                    self.stack.set(
                        slice.start() + aligned_new_len,
                        aligned_new_len.to_le_bytes(),
                    )?;
                    return Ok(BStackSlice::new(self, slice.start(), new_len));
                }
                std::cmp::Ordering::Less => {
                    // Write new footer before discarding so it lands at the right position
                    self.stack.set(
                        slice.start() + aligned_new_len,
                        aligned_new_len.to_le_bytes(),
                    )?;
                    self.stack.set(
                        slice.start() - Self::BLOCK_HEADER_SIZE,
                        aligned_new_len.to_le_bytes(),
                    )?;
                    self.stack.discard(aligned_current_len - aligned_new_len)?;
                    return Ok(BStackSlice::new(self, slice.start(), new_len));
                }
            }
        }

        // Special case: same block optimizations
        // Read the block size
        let block_size_buf = self.stack.get(
            slice.start() - Self::BLOCK_HEADER_SIZE,
            slice.start() - Self::BLOCK_HEADER_SIZE + 8,
        )?;
        let block_size = u64::from_le_bytes(block_size_buf.try_into().unwrap());
        if block_size >= aligned_new_len {
            // The block is already big enough.  When growing past the previous user-visible
            // len, zero bytes [slice.len(), new_len) in one atomic write — this covers both
            // the gap [slice.len(), aligned_current_len) (potentially stale) and the
            // newly-exposed range [aligned_current_len, new_len).
            if new_len > slice.len() {
                self.stack
                    .zero(slice.start() + slice.len(), new_len - slice.len())?;
            }
            return Ok(BStackSlice::new(self, slice.start(), new_len));
        }

        // Special case: next block is free and can be merged in place to accommodate the new size.
        // This avoids copying data.
        let next_block = slice.start() + block_size + Self::BLOCK_OVERHEAD_SIZE;
        if next_block <= self.stack.len()? - Self::BLOCK_FOOTER_SIZE - Self::MIN_BLOCK_PAYLOAD_SIZE
        {
            let mut next_hdr_buf = [0u8; 16];
            self.stack
                .get_into(next_block - Self::BLOCK_HEADER_SIZE, &mut next_hdr_buf)?;
            let next_block_size = u64::from_le_bytes(next_hdr_buf[0..8].try_into().unwrap());
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
                if slice.len() < block_size {
                    self.stack
                        .zero(slice.start() + slice.len(), block_size - slice.len())?;
                }

                // Unlink the next block from the free list, then merge it into the current block.
                self.set_recovery_needed()?;
                self.unlink_from_free_list(next_block)?;
                // merged_size includes the overhead bytes absorbed from between the two blocks
                let merged_size = block_size + Self::BLOCK_OVERHEAD_SIZE + next_block_size;

                // Buffer covering [slice.start()+block_size, slice.start()+merged_size+FOOTER).
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
                    let new_free_start =
                        slice.start() + aligned_new_len + Self::BLOCK_OVERHEAD_SIZE;
                    let mut head_buf = [0u8; 8];
                    self.stack.get_into(Self::FREE_HEAD_OFFSET, &mut head_buf)?;
                    let old_head = u64::from_le_bytes(head_buf);

                    // All offsets are relative to zero_buff[0] = slice.start() + block_size.
                    let alloc_footer_off = (aligned_new_len - block_size) as usize;
                    let free_hdr_off = alloc_footer_off + Self::BLOCK_FOOTER_SIZE as usize;
                    let free_payload_off = alloc_footer_off + Self::BLOCK_OVERHEAD_SIZE as usize;
                    let free_footer_off = (next_block_size + Self::BLOCK_OVERHEAD_SIZE) as usize;

                    zero_buff[alloc_footer_off..alloc_footer_off + 8]
                        .copy_from_slice(&aligned_new_len.to_le_bytes());
                    zero_buff[free_hdr_off..free_hdr_off + 8]
                        .copy_from_slice(&remainder_size.to_le_bytes());
                    zero_buff[free_hdr_off + 8..free_hdr_off + 12]
                        .copy_from_slice(&1u32.to_le_bytes()); // is_free = 1
                    zero_buff[free_payload_off..free_payload_off + 8]
                        .copy_from_slice(&old_head.to_le_bytes()); // next_free = old head
                    // prev_free stays 0
                    zero_buff[free_footer_off..free_footer_off + 8]
                        .copy_from_slice(&remainder_size.to_le_bytes());

                    // Set the header to merged_size first so that if we crash after the
                    // big write but before the aligned_new_len update, recovery sees a
                    // header/footer mismatch (merged_size vs. remainder_size) and can
                    // detect and repair the partial split.
                    self.stack.set(
                        slice.start() - Self::BLOCK_HEADER_SIZE,
                        merged_size.to_le_bytes(),
                    )?;
                    // Single write: zeroes the inter-block overhead, writes the allocated
                    // block's new footer, the complete free block, and the free block's footer.
                    self.stack.set(slice.start() + block_size, &zero_buff)?;
                    // Shrink the allocated block's header to the used size.
                    self.stack.set(
                        slice.start() - Self::BLOCK_HEADER_SIZE,
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
                    self.stack.set(
                        slice.start() - Self::BLOCK_HEADER_SIZE,
                        merged_size.to_le_bytes(),
                    )?;
                    zero_buff[(next_block_size + Self::BLOCK_OVERHEAD_SIZE) as usize..]
                        .copy_from_slice(&merged_size.to_le_bytes());
                    self.stack.set(slice.start() + block_size, &zero_buff)?;
                }
                self.clear_recovery_needed()?;
                return Ok(BStackSlice::new(self, slice.start(), new_len));
            }
        }

        // For non-tail blocks, we need to find a new block for the new size, copy the data, and free the old block.
        let block_found = self.find_large_enough_block(aligned_new_len)?;
        if block_found.0 != 0 {
            // Found a big enough block at offset block_found. Remove it from the free list and return it.
            // If the block is much bigger than needed, split it and add the remainder back to the free list.

            // Copy only the user-visible bytes from the old block into the new block's
            // buffer; bytes beyond `slice.len()` in the buffer stay at the zero-init from
            // `vec!`, so the new block's payload past `slice.len()` is zero — matching
            // extend/calloc semantics for newly-exposed bytes after realloc.
            let copy_len = slice.len().min(aligned_new_len);
            let mut data_buf = vec![0u8; (Self::BLOCK_OVERHEAD_SIZE + aligned_new_len) as usize];
            self.stack.get_into(
                slice.start(),
                &mut data_buf[Self::BLOCK_HEADER_SIZE as usize
                    ..(copy_len + Self::BLOCK_HEADER_SIZE) as usize],
            )?;
            self.set_recovery_needed()?;
            self.unlink_block(
                block_found.0,
                block_found.1,
                aligned_new_len,
                data_buf.as_mut_slice(),
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
            self.add_to_free_list(slice.start())?;
            self.clear_recovery_needed()?;
            Ok(BStackSlice::new(self, new_payload, new_len))
        } else {
            // No free block fits; push the full new block in one call, then free the old one.
            // Copy only the user-visible bytes; the rest of `block_buf` stays zeroed.
            let copy_len = (slice.len().min(aligned_new_len)) as usize;
            let mut block_buf = vec![0u8; (aligned_new_len + Self::BLOCK_OVERHEAD_SIZE) as usize];
            block_buf[..8].copy_from_slice(&aligned_new_len.to_le_bytes());
            self.stack.get_into(
                slice.start(),
                &mut block_buf
                    [Self::BLOCK_HEADER_SIZE as usize..Self::BLOCK_HEADER_SIZE as usize + copy_len],
            )?;
            block_buf[(aligned_new_len + Self::BLOCK_HEADER_SIZE) as usize..]
                .copy_from_slice(&aligned_new_len.to_le_bytes());
            self.set_recovery_needed()?;
            let ptr = self.stack.push(&block_buf)? + Self::BLOCK_HEADER_SIZE;
            self.add_to_free_list(slice.start())?;
            self.clear_recovery_needed()?;
            Ok(BStackSlice::new(self, ptr, new_len))
        }
    }
}
