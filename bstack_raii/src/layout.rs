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
