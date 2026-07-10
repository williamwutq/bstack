//! Block creation: allocate, stamp the header, and initialize refcounts /
//! control blocks. The teardown side lives in [`crate::handle`]; this is the
//! matching build side.
//!
//! These are the low-level, type-agnostic primitives the `#[bstack_block]`
//! macro's generated constructors (and the tests) build on. Writing the
//! type-specific payload — child refs and POD fields after the header — is the
//! caller's job; these helpers only lay down the header and the injected
//! refcount / control machinery at the fixed offsets from [`crate::layout`].

use std::io;

use bstack::{BStackOwnedSliceAllocator, BStackRange};

use crate::layout::{self, BlockHeader, EightCC};
use crate::teardown::dealloc_range;

/// Allocate a `size`-byte block and stamp its `BlockHeader { size, tag }`.
///
/// Returns the block's range. The bytes after the header are left as the
/// allocator provided them; the caller fills in the payload. On a write failure
/// the freshly allocated block is released so nothing leaks.
pub fn alloc_block<A: BStackOwnedSliceAllocator>(
    allocator: &A,
    tag: EightCC,
    size: u64,
) -> io::Result<BStackRange> {
    let mut slice = allocator.alloc(size)?;
    let header = BlockHeader { size, tag };
    if let Err(e) = slice.write_range(0, bytemuck::bytes_of(&header)) {
        let _ = allocator.dealloc(slice);
        return Err(e);
    }
    Ok(slice.as_range())
}

/// Initialize a plain `#[bstack_block(rc)]` block's inline refcount to 1.
///
/// Call once after [`alloc_block`] and after the payload is written. One is the
/// count the single returned `BStackRc` accounts for.
pub fn init_rc<A: BStackOwnedSliceAllocator>(
    allocator: &A,
    data: BStackRange,
) -> io::Result<()> {
    let off = data.start() + layout::RC_REFCOUNT_OFFSET;
    allocator.stack().set(off, 1u64.to_le_bytes())
}

/// Allocate and wire the control block for an already-allocated
/// `#[bstack_block(rc, weak)]` data block.
///
/// Writes the control header, `strong = 1`, `weak = 1` (the phantom weak held by
/// the strong owners), and the `x` forward pointer to the data block; then
/// writes the data block's `ctrl` back-pointer. Returns the control block's
/// range. `control_size` is `size_of::<T::Control>()`.
///
/// On failure the control block is released; the caller still owns (and must
/// release) the data block.
pub fn alloc_control<A: BStackOwnedSliceAllocator>(
    allocator: &A,
    ctrl_tag: EightCC,
    data: BStackRange,
    control_size: u64,
) -> io::Result<BStackRange> {
    // Build the entire control-block payload in memory and commit it in a single
    // write: header, strong = 1, weak = 1 (phantom), x -> data.
    let mut payload = vec![0u8; control_size as usize];
    let header = BlockHeader { size: control_size, tag: ctrl_tag };
    payload[..layout::HEADER_SIZE as usize].copy_from_slice(bytemuck::bytes_of(&header));
    let put = |payload: &mut [u8], off: u64, val: u64| {
        let o = off as usize;
        payload[o..o + 8].copy_from_slice(&val.to_le_bytes());
    };
    put(&mut payload, layout::CTRL_STRONG_OFFSET, 1);
    put(&mut payload, layout::CTRL_WEAK_OFFSET, 1);
    put(&mut payload, layout::CTRL_DATA_OFFSET, data.start());

    let mut slice = allocator.alloc(control_size)?;
    let ctrl = slice.as_range();
    if let Err(e) = slice.write_range(0, &payload) {
        let _ = allocator.dealloc(slice);
        return Err(e);
    }

    // The data block's `ctrl` back-pointer lives in a different block, so it is
    // one more (unavoidable) write into that region.
    let backptr = data.start() + layout::CTRL_BACKPTR_OFFSET;
    if let Err(e) = allocator.stack().set(backptr, ctrl.start().to_le_bytes()) {
        let _ = unsafe { dealloc_range(allocator, ctrl) };
        return Err(e);
    }
    Ok(ctrl)
}
