//! [`BStackString`]: a standalone, owned, growable UTF-8 string block.
//!
//! The on-disk analogue of [`std::string::String`]. Where the `#[bstack_block]`
//! macro only lets a `String` live *inside* a struct field, `BStackString` is a
//! first-class owned block you can hold on its own, put in a
//! [`crate::BStackDeque`], or store as a value in a [`crate::BStackHashMap`] /
//! [`crate::BStackBTreeMap`].
//!
//! Like every variable-length container here, it is a fixed handle block
//! ([`StringOnDisk`]) — header + a pointer to a separate bytes block + the byte
//! length — so the handle never moves and the type is a normal
//! [`BStackBlock`] (composable as a field, referenced, cloned). The UTF-8 bytes
//! live in their own block; mutating the contents reallocates only that block and
//! swaps the handle's `{data, len}` in one atomic write.

use core::mem::size_of;
use std::io;

use crate::BStackRaiiAllocator;
use bstack::{BStack, BStackRange};
use bytemuck::{Pod, Zeroable};

use super::util::{alloc_image, read_fields, read_u64};
use crate::block::{BStackBlock, BStackCast};
use crate::clone::{ClonePlan, TryCloneIn};
use crate::layout::{BlockHeader, EightCC, HEADER_SIZE};
use crate::owned::BStackOwned;
use crate::teardown::{AutoDrop, BStackDrop, dealloc_range};

/// The on-disk image of a [`BStackString`]: header, a pointer to the UTF-8 bytes
/// block (`0` = empty), and the byte length. `#[repr(C)]`, `u64` fields only —
/// fixed-size and non-generic, so the handle is a normal block.
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub struct StringOnDisk {
    /// The 16-byte block header (size + type tag).
    pub header: BlockHeader,
    /// Offset of the UTF-8 bytes block, or `0` when the string is empty.
    pub data: u64,
    /// Length of the string in bytes.
    pub len: u64,
}

const DATA_OFF: u64 = HEADER_SIZE; // 16
const LEN_OFF: u64 = HEADER_SIZE + 8; // 24
const STRING_SIZE: u64 = size_of::<StringOnDisk>() as u64;

/// A standalone owned UTF-8 string block.
///
/// A typed handle (a newtype over a [`BStackRange`]); [`new`](Self::new) returns a
/// bare [`BStackOwned<BStackString>`] that frees nothing on scope exit — free it
/// with [`bstack_drop`](BStackDrop::bstack_drop) or wrap it ([`AutoDrop`] /
/// [`crate::BStackCow`]).
pub struct BStackString {
    range: BStackRange,
}

impl BStackString {
    /// Allocate a bytes block holding `bytes` (or return `0` for an empty slice),
    /// releasing it without leaking on write failure.
    fn alloc_bytes<A: BStackRaiiAllocator>(allocator: &A, bytes: &[u8]) -> io::Result<u64> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let mut slice = allocator.alloc(bytes.len() as u64)?;
        if let Err(e) = slice.write_range(0, bytes) {
            let _ = allocator.dealloc(slice);
            return Err(e);
        }
        Ok(slice.as_range().start())
    }

    /// Create a string from `s`.
    pub fn new<A: BStackRaiiAllocator>(allocator: &A, s: &str) -> io::Result<BStackOwned<Self>> {
        let len = s.len() as u64;
        let data = Self::alloc_bytes(allocator, s.as_bytes())?;
        let od = StringOnDisk {
            header: BlockHeader {
                size: STRING_SIZE,
                tag: Self::eightcc(),
            },
            data,
            len,
        };
        match alloc_image(allocator, bytemuck::bytes_of(&od)) {
            // SAFETY: a freshly allocated block owned by no other handle.
            Ok(range) => Ok(unsafe { BStackOwned::from_raw(Self::from_range(range)) }),
            Err(e) => {
                if data != 0 {
                    // SAFETY: the bytes block was just allocated, referenced by nobody.
                    let _ = unsafe { dealloc_range(allocator, BStackRange::new(data, len)) };
                }
                Err(e)
            }
        }
    }

    /// Length in bytes.
    pub fn len(&self, stack: &BStack) -> io::Result<u64> {
        read_u64(stack, self.range.start() + LEN_OFF)
    }

    /// Whether the string is empty.
    pub fn is_empty(&self, stack: &BStack) -> io::Result<bool> {
        Ok(self.len(stack)? == 0)
    }

    /// Read the raw UTF-8 bytes.
    pub fn read_bytes(&self, stack: &BStack) -> io::Result<Vec<u8>> {
        let [data, len] = read_fields::<2>(stack, self.range.start() + DATA_OFF)?;
        let len = len as usize;
        let mut buf = vec![0u8; len];
        if len != 0 {
            stack.get_into(data, &mut buf)?;
        }
        Ok(buf)
    }

    /// Read the contents as a `String` (validating UTF-8).
    pub fn to_string(&self, stack: &BStack) -> io::Result<String> {
        String::from_utf8(self.read_bytes(stack)?)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Replace the contents with `s`, atomically swapping in the new bytes block
    /// and freeing the old one.
    ///
    /// The new bytes are written to a fresh block first, then the handle's
    /// `{data, len}` pair is updated in one atomic write (a crash before it leaves
    /// the old string intact; after it, the new). The old bytes block is then
    /// freed (leak-only on a crash in between).
    pub fn set<A: BStackRaiiAllocator>(&self, allocator: &A, s: &str) -> io::Result<()> {
        let handle = self.range.start();
        let stack = allocator.stack();
        let newlen = s.len() as u64;
        let newdata = Self::alloc_bytes(allocator, s.as_bytes())?;

        let [old_data, old_len] = read_fields::<2>(stack, handle + DATA_OFF)?;

        // `data` and `len` are contiguous — swap both in one 16-byte write.
        let mut buf = [0u8; 16];
        buf[0..8].copy_from_slice(&newdata.to_le_bytes());
        buf[8..16].copy_from_slice(&newlen.to_le_bytes());
        if let Err(e) = stack.set(handle + DATA_OFF, buf) {
            if newdata != 0 {
                // SAFETY: never linked into the handle; reclaim it.
                let _ = unsafe { dealloc_range(allocator, BStackRange::new(newdata, newlen)) };
            }
            return Err(e);
        }

        if old_data != 0 {
            // SAFETY: the handle no longer points at the old bytes block.
            let _ = unsafe { dealloc_range(allocator, BStackRange::new(old_data, old_len)) };
        }
        Ok(())
    }

    /// Append `s` to the string.
    pub fn push_str<A: BStackRaiiAllocator>(&self, allocator: &A, s: &str) -> io::Result<()> {
        let mut cur = self.to_string(allocator.stack())?;
        cur.push_str(s);
        self.set(allocator, &cur)
    }

    /// Append a single character.
    pub fn push<A: BStackRaiiAllocator>(&self, allocator: &A, ch: char) -> io::Result<()> {
        let mut buf = [0u8; 4];
        self.push_str(allocator, ch.encode_utf8(&mut buf))
    }

    /// Truncate to `new_len` **bytes**, which must be a UTF-8 char boundary and
    /// not exceed the current length; longer values leave the string unchanged.
    pub fn truncate<A: BStackRaiiAllocator>(
        &self,
        allocator: &A,
        new_len: usize,
    ) -> io::Result<()> {
        let mut cur = self.to_string(allocator.stack())?;
        if new_len >= cur.len() {
            return Ok(());
        }
        if !cur.is_char_boundary(new_len) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "truncate: byte index is not a UTF-8 char boundary",
            ));
        }
        cur.truncate(new_len);
        self.set(allocator, &cur)
    }

    /// Empty the string (frees its bytes block).
    pub fn clear<A: BStackRaiiAllocator>(&self, allocator: &A) -> io::Result<()> {
        self.set(allocator, "")
    }

    /// The number of Unicode scalar values (`char`s), not bytes.
    pub fn char_count(&self, stack: &BStack) -> io::Result<usize> {
        Ok(self.to_string(stack)?.chars().count())
    }

    /// Whether the contents equal `s`, byte-for-byte (no UTF-8 validation).
    pub fn eq_str(&self, stack: &BStack, s: &str) -> io::Result<bool> {
        Ok(self.read_bytes(stack)? == s.as_bytes())
    }

    /// Whether the contents begin with `prefix`.
    pub fn starts_with(&self, stack: &BStack, prefix: &str) -> io::Result<bool> {
        Ok(self.read_bytes(stack)?.starts_with(prefix.as_bytes()))
    }

    /// Whether the contents end with `suffix`.
    pub fn ends_with(&self, stack: &BStack, suffix: &str) -> io::Result<bool> {
        Ok(self.read_bytes(stack)?.ends_with(suffix.as_bytes()))
    }

    /// Whether the contents contain `needle`.
    pub fn contains(&self, stack: &BStack, needle: &str) -> io::Result<bool> {
        Ok(self.to_string(stack)?.contains(needle))
    }

    /// Attach an allocator to make an auto-freeing [`AutoDrop`] guard.
    pub fn auto<A: BStackRaiiAllocator>(self, allocator: &A) -> AutoDrop<'_, Self, A> {
        // SAFETY: sole ownership was asserted when the string was created.
        unsafe { AutoDrop::from_raw(self, allocator) }
    }
}

impl BStackCast for BStackString {
    /// A fixed `"Str"` tag (non-generic type).
    fn eightcc() -> EightCC {
        EightCC::new([b'S', b't', b'r', 0x80, 0x81, 0x82, 0x83, 0x84])
    }
}

// Self-contained (no separate control block): may be `#[embed]`ded.
impl crate::block::BStackEmbeddable for BStackString {}

impl BStackBlock for BStackString {
    type OnDisk = StringOnDisk;

    fn from_range(range: BStackRange) -> Self {
        BStackString { range }
    }

    fn range(&self) -> BStackRange {
        self.range
    }

    /// Free the bytes block, **without** freeing the handle block itself.
    fn __bstack_drop_children<A: BStackRaiiAllocator>(
        range: BStackRange,
        allocator: &A,
    ) -> io::Result<()> {
        let [data, len] = read_fields::<2>(allocator.stack(), range.start() + DATA_OFF)?;
        if data != 0 {
            // SAFETY: the string solely owns its bytes block.
            unsafe { dealloc_range(allocator, BStackRange::new(data, len))? };
        }
        Ok(())
    }

    /// Deep-clone: copy the bytes into a fresh block and stage the handle, in the
    /// parent plan's single atomic commit.
    fn __bstack_clone_children_inplace<A: BStackRaiiAllocator>(
        &self,
        allocator: &A,
        plan: &mut ClonePlan,
    ) -> io::Result<Self::OnDisk> {
        let handle = self.range.start();
        let [data, len] = read_fields::<2>(allocator.stack(), handle + DATA_OFF)?;

        let new_data = if len != 0 {
            let mut bytes = vec![0u8; len as usize];
            allocator.stack().get_into(data, &mut bytes)?;
            let dst = plan.alloc_raw(allocator, len)?;
            plan.write(dst.start(), bytes);
            dst.start()
        } else {
            0
        };

        let od = StringOnDisk {
            header: BlockHeader {
                size: STRING_SIZE,
                tag: Self::eightcc(),
            },
            data: new_data,
            len,
        };
        Ok(od)
    }
}

impl BStackDrop for BStackString {
    fn bstack_drop<A: BStackRaiiAllocator>(self, allocator: &A) -> io::Result<()> {
        Self::__bstack_drop_children(self.range, allocator)?;
        // SAFETY: sole ownership of the handle block was asserted at construction.
        unsafe { dealloc_range(allocator, self.range) }
    }
}

impl TryCloneIn for BStackString {
    fn try_clone_in<A: BStackRaiiAllocator>(&self, allocator: &A) -> io::Result<BStackOwned<Self>> {
        let mut plan = ClonePlan::new();
        let dst = match self.__bstack_clone_into(allocator, &mut plan) {
            Ok(range) => range,
            Err(e) => {
                plan.rollback(allocator);
                return Err(e);
            }
        };
        plan.commit(allocator)?;
        // SAFETY: `dst` is a fresh block owned by nobody else.
        Ok(unsafe { BStackOwned::from_raw(Self::from_range(dst)) })
    }
}
