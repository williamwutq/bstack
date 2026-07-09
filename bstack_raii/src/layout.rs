//! On-disk primitives shared by every block: the type tag and the block header.
//!
//! Both are [`bytemuck::Pod`] so they can be embedded directly in a generated
//! `XOnDisk` struct and read back with `bytemuck::from_bytes`.

use bytemuck::{Pod, Zeroable};

/// An 8-byte type tag stored in every [`BlockHeader`].
///
/// Used instead of a 4-byte `FourCC` because `bstack` offsets are 64-bit, so
/// 8-byte alignment is natural. The `#[bstack_block]` macro derives it from the
/// block type's name via [`EightCC::from_name`]; [`crate::BStackCast`] compares
/// it during safe downcasts.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Pod, Zeroable)]
pub struct EightCC(pub [u8; 8]);

impl EightCC {
    /// Wrap a raw 8-byte tag.
    pub const fn new(tag: [u8; 8]) -> Self {
        Self(tag)
    }

    /// Derive a tag from a type name: the first 8 bytes, zero-padded (or
    /// truncated). `const` so the macro can emit it in a `const` context.
    pub const fn from_name(name: &str) -> Self {
        let bytes = name.as_bytes();
        let mut out = [0u8; 8];
        let mut i = 0;
        while i < 8 && i < bytes.len() {
            out[i] = bytes[i];
            i += 1;
        }
        Self(out)
    }
}

/// The header prefixing every on-disk block. 16 bytes.
///
/// `size` is the payload length in bytes; `tag` is the [`EightCC`] discriminant
/// written by the allocator at block creation. Declared `#[repr(C)]` rather than
/// `#[repr(C, packed)]`: a `u64` followed by an 8-byte tag is already densely
/// packed with no padding, and avoiding `packed` keeps field access sound. The
/// *generated* `XOnDisk` structs that embed this and then mix in smaller POD
/// fields are the ones that need `packed`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct BlockHeader {
    pub size: u64,
    pub tag: EightCC,
}

/// Byte length of a [`BlockHeader`] — the offset at which a block's payload
/// begins.
pub const HEADER_SIZE: u64 = core::mem::size_of::<BlockHeader>() as u64;

/// On-disk width of a reference. Per RAII.md, an on-disk `BStackRef<T>` stores
/// only the `u64` offset; the length is recovered at resolve time from the
/// target type's fixed `size_of::<T::OnDisk>()`. (This is why the RAII layer is,
/// for now, a fixed-size-block model.)
pub const REF_SIZE: u64 = 8;

// -- Injected-field offsets ------------------------------------------------
//
// RAII.md injects the refcount / control back-pointer / control counters
// immediately after the header, ahead of any user fields and in a fixed order.
// Their offsets are therefore the same for *every* block, so they live here as
// constants rather than as per-type trait members.

/// `#[bstack_block(rc)]` data block: offset of the inline `refcount: AtomicU64`,
/// injected right after the header.
///
/// ```text
/// struct XOnDisk { header, refcount: AtomicU64, <user fields...> }
/// ```
pub const RC_REFCOUNT_OFFSET: u64 = HEADER_SIZE;

/// `#[bstack_block(rc, weak)]` data block: offset of the `ctrl` back-pointer to
/// the control block, injected right after the header.
///
/// ```text
/// struct XOnDisk { header, ctrl: BStackRef<XOnDiskRef>, <user fields...> }
/// ```
pub const CTRL_BACKPTR_OFFSET: u64 = HEADER_SIZE;

/// `#[bstack_block(rc, weak)]` control block (`XOnDiskRef`): offset of `strong`.
///
/// ```text
/// struct XOnDiskRef { header, strong: AtomicU64, weak: AtomicU64, x: BStackRef<X> }
/// ```
pub const CTRL_STRONG_OFFSET: u64 = HEADER_SIZE;

/// Control block: offset of `weak` (starts at 1 — the phantom weak held
/// collectively by all live strong owners).
pub const CTRL_WEAK_OFFSET: u64 = HEADER_SIZE + 8;

/// Control block: offset of `x`, the forward pointer back to the data block.
/// Read by [`crate::BStackWeak::upgrade`] once it wins the strong CAS.
pub const CTRL_DATA_OFFSET: u64 = HEADER_SIZE + 16;

// Guard the hand-derived offsets against a header size change.
const _: () = assert!(HEADER_SIZE == 16);
