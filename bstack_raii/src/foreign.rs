//! [`Foreign<T>`]: a cross-file pointer — "a slice with a file identity attached".
//!
//! An in-file reference stores just a `u64` offset (length recovered from
//! `size_of::<T::OnDisk>()`). A `Foreign<T>` widens that with the target file's
//! identity: on disk it is a [`ForeignPtr`] `{ file_id: u64, offset: u64 }` (16
//! bytes, `Pod`) — length is still recovered from the type, so it is *not* stored.
//! Dereferencing resolves `file_id` through the process-wide
//! [registry](crate::registry) to the file's live allocator, then reads/writes at
//! `offset` in that file.
//!
//! As a `#[bstack_block]` field, a `Foreign<T>` carries the same ownership
//! annotations as an in-file field — `#[bstack_owned/strong/weak/ref]` (or none) —
//! but applied to the target `T` **in its own file**: an owning foreign pointer
//! frees / decrements / releases the target *on the other side* at teardown, and a
//! deep clone copies it across files. Those cross-file **teardown** and **deep
//! clone** dispatches are still deferred; today the field is byte-copied on clone
//! (an alias) and freed by nobody on teardown, regardless of annotation. The
//! annotation is recorded so the eventual dispatch is per-kind. Construction,
//! nullability (`Option<Foreign<T>>`, `offset == 0` niche), and resolution are
//! implemented here.

use core::marker::PhantomData;

use bstack::{BStack, BStackAllocator, BStackRange};
use bytemuck::{Pod, Zeroable};

use crate::block::BStackBlock;
use crate::registry::{self, FileId, FileRegistry};

/// The on-disk form of a [`Foreign`] pointer: a file identity plus an offset in
/// that file. 16 bytes, `Pod`. The target's length is **not** stored — it is
/// recovered from `size_of::<T::OnDisk>()`, exactly like an in-file `#[bstack_ref]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct ForeignPtr {
    /// The target file's [`FileId`] as a `u64` (`0` = [`FileId::SELF`]).
    file_id: u64,
    /// The target's offset within that file.
    offset: u64,
}

impl ForeignPtr {
    /// A wide pointer from a raw `(file_id, offset)`.
    pub const fn new(file_id: u64, offset: u64) -> Self {
        Self { file_id, offset }
    }
    /// The raw file-id word.
    pub const fn file_id(self) -> u64 {
        self.file_id
    }
    /// The target offset.
    pub const fn offset(self) -> u64 {
        self.offset
    }
}

/// A typed cross-file pointer to a `T`: a [`FileId`] + an offset. Resolved through
/// the process-wide [registry](crate::registry) (or a specific [`FileRegistry`]).
///
/// `Copy` regardless of `T` (it holds only a [`ForeignPtr`]). See the [module
/// docs](self) for what is deferred (teardown / deep clone).
pub struct Foreign<T> {
    ptr: ForeignPtr,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Clone for Foreign<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for Foreign<T> {}

impl<T> Foreign<T> {
    /// A foreign pointer to `offset` within the file identified by `file`.
    pub const fn new(file: FileId, offset: u64) -> Self {
        Self {
            ptr: ForeignPtr::new(file.as_u64(), offset),
            _marker: PhantomData,
        }
    }

    /// Reconstruct from the stored on-disk [`ForeignPtr`].
    pub const fn from_ptr(ptr: ForeignPtr) -> Self {
        Self {
            ptr,
            _marker: PhantomData,
        }
    }

    /// The on-disk wide pointer, for storing into a field.
    pub const fn ptr(self) -> ForeignPtr {
        self.ptr
    }

    /// The target's offset within its file.
    pub const fn offset(self) -> u64 {
        self.ptr.offset
    }

    /// Whether this points into the *current* file ([`FileId::SELF`]).
    pub const fn is_self(self) -> bool {
        self.ptr.file_id == 0
    }

    /// The file this points into. A raw file-id that does not fit the [`FileId`]
    /// space (corruption / a wider build) maps to [`FileId::SELF`]; use
    /// [`with`](Self::with)/[`with_in`](Self::with_in), which reject a bad id
    /// outright, when that distinction matters.
    pub fn file_id(self) -> FileId {
        FileId::from_u64(self.ptr.file_id).unwrap_or(FileId::SELF)
    }
}

impl<T: BStackBlock> Foreign<T> {
    /// A foreign pointer to the block `target` currently occupies, in file `file`.
    pub fn at(file: FileId, target: &T) -> Self {
        Self::new(file, target.range().start())
    }

    /// The target's range in its file (`offset` + `size_of::<T::OnDisk>()`).
    pub fn range(self) -> BStackRange {
        BStackRange::new(
            self.ptr.offset,
            core::mem::size_of::<T::OnDisk>() as u64,
        )
    }

    /// Resolve the pointer and run `f` with a `T` handle at the target plus the
    /// [`BStack`] of the file it lives in.
    ///
    /// There is exactly one registry — the process-wide one ([`crate::registry`]) —
    /// so resolution never takes a registry argument: a `Foreign` (e.g. one moved
    /// out via `bstack_move!`) is always resolvable on its own. [`SELF`](FileId::SELF)
    /// resolves against `local` directly (no registry, no lock); a foreign id
    /// resolves via the global registry, yielding `None` if it is uninitialized, or
    /// the target file is unknown / not currently attached / the id is malformed.
    pub fn with<A, R>(self, local: &A, f: impl FnOnce(T, &BStack) -> R) -> Option<R>
    where
        A: BStackAllocator,
    {
        let t = T::from_range(self.range());
        if self.ptr.file_id == 0 {
            Some(f(t, local.stack()))
        } else {
            let id = FileId::from_u64(self.ptr.file_id)?;
            registry::with_host(id, |host| f(t, host.stack()))
        }
    }

    /// Like [`with`](Self::with) but against an explicit `registry` — crate-internal,
    /// for tests (the global is a one-shot `OnceLock`, awkward to exercise in unit
    /// tests). Production code uses [`with`](Self::with) against the sole registry.
    pub(crate) fn with_in<A, R>(
        self,
        registry: &FileRegistry,
        local: &A,
        f: impl FnOnce(T, &BStack) -> R,
    ) -> Option<R>
    where
        A: BStackAllocator,
    {
        let t = T::from_range(self.range());
        if self.ptr.file_id == 0 {
            Some(f(t, local.stack()))
        } else {
            let id = FileId::from_u64(self.ptr.file_id)?;
            registry.with_host(id, |host| f(t, host.stack()))
        }
    }
}
