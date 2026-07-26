//! Block creation: allocate, stamp the header, and initialize refcounts /
//! control blocks. The teardown side lives in [`crate::handle`]; this is the
//! matching build side.
//!
//! These are the low-level, type-agnostic primitives the `#[bstack_block]`
//! macro's generated constructors (and the tests) build on. Writing the
//! type-specific payload — child refs and POD fields after the header — is the
//! caller's job; these helpers only lay down the header and the injected
//! refcount / control machinery at the fixed offsets from [`crate::layout`].

use core::mem::size_of;
use std::io;

use bstack::{BStackOwnedSliceAllocator, BStackRange};

use crate::block::BStackWeakable;
use crate::handle::WeakRef;
use crate::layout::{self, BlockHeader, EightCC};
use crate::reference::BStackRef;
use crate::shared::{BStackRc, BStackWeak};
use crate::teardown::{BStackDrop, dealloc_range};

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
pub fn init_rc<A: BStackOwnedSliceAllocator>(allocator: &A, data: BStackRange) -> io::Result<()> {
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
    let header = BlockHeader {
        size: control_size,
        tag: ctrl_tag,
    };
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

/// Set a `#[bstack_weak]` field, located at absolute on-disk offset `field_off`,
/// to point at `new_weak` — releasing any weak reference the field previously
/// held.
///
/// The field stores the child's **control-block** offset, not its data offset:
/// the control block outlives the data block (it lives while `weak > 0`), so
/// resolving it at teardown is sound even after the target's data has been
/// freed. `new_weak` is consumed and the weak count it holds becomes the field's;
/// a previous non-null target has its weak count decremented. 0 means "unset".
pub fn set_weak_field<'w, T: BStackWeakable, A: BStackOwnedSliceAllocator>(
    allocator: &A,
    field_off: u64,
    new_weak: BStackWeak<'w, T, A>,
) -> io::Result<()> {
    let stack = allocator.stack();

    // Read the old target before overwriting it.
    let mut buf = [0u8; 8];
    stack.get_into(field_off, &mut buf)?;
    let old = u64::from_le_bytes(buf);

    // Commit the new pointer FIRST, as a single atomic write: the live field
    // transitions directly from the old target to the new one and is never
    // observed pointing at a released control block. `new_weak` is consumed
    // without decrementing — its weak count becomes the field's.
    let ctrl = new_weak.into_raw();
    stack.set(field_off, ctrl.into_range().start().to_le_bytes())?;

    // Only now release the old target — pure reclamation, since the field no
    // longer refers to it. A crash before this leaks at most the old control
    // block (its weak count stays one too high), never a dangling field.
    if old != 0 {
        let old_ctrl = unsafe {
            BStackRef::<T::Control>::from_range(BStackRange::new(
                old,
                size_of::<T::Control>() as u64,
            ))
        };
        WeakRef::<T>(old_ctrl).bstack_drop(allocator)?;
    }
    Ok(())
}

/// Attempt to upgrade a `#[bstack_weak]` field (holding a control-block offset at
/// `field_off`) to a strong handle. Returns `None` if the field is unset (0) or
/// the target's strong count has already reached zero. What a generated weak
/// field accessor calls.
pub fn upgrade_weak_field<'a, T: BStackWeakable, A: BStackOwnedSliceAllocator>(
    allocator: &'a A,
    field_off: u64,
) -> io::Result<Option<BStackRc<'a, T, A>>> {
    let mut buf = [0u8; 8];
    allocator.stack().get_into(field_off, &mut buf)?;
    let off = u64::from_le_bytes(buf);
    if off == 0 {
        return Ok(None);
    }
    let ctrl = unsafe {
        BStackRef::<T::Control>::from_range(BStackRange::new(off, size_of::<T::Control>() as u64))
    };
    // Borrow a weak over the field's control ref just long enough to upgrade;
    // consume it via `into_raw` so the field's own weak count is untouched.
    let weak = unsafe { BStackWeak::from_raw(ctrl, allocator) };
    let result = weak.upgrade();
    let _ = weak.into_raw();
    result
}
