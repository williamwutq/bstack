//! [`Foreign<T>`]: a cross-file pointer — "a slice with a file identity attached".
//!
//! An in-file reference stores just a `u64` offset (length recovered from
//! `size_of::<T::OnDisk>()`). A `Foreign<T>` widens that with the target file's
//! identity: on disk it is an inert wire record `{ file_id: u64, address: u64 }` (16
//! bytes, `Pod`) — length is still recovered from the type, so it is *not* stored.
//! Dereferencing resolves `file_id` through the process-wide
//! [registry](crate::registry) to the file's live allocator, then reads/writes at
//! `offset` in that file.
//!
//! As a `#[bstack_block]` field, a `Foreign<T>` carries the same ownership
//! annotations as an in-file field — `#[bstack_owned/strong/weak/ref]` (or none) —
//! but applied to the target `T` **in its own file**: an owning foreign pointer
//! frees / decrements / releases the target *on the other side* at teardown, and a
//! deep clone copies (owned) or re-references (strong/weak) it across files. Those
//! cross-file **teardown** ([`foreign_drop_owned`]/`_strong`/`_weak`) and **deep
//! clone** ([`foreign_clone_owned`]/`_strong`/`_weak`) dispatches run the ordinary
//! generic machinery against a [`ForeignHostAllocator`](crate::registry::ForeignHostAllocator)
//! over the target's live host, selected per-annotation by the generated field code;
//! `#[bstack_ref]` aliases (byte-copied, owns nothing). Construction, nullability
//! (`Option<Foreign<T>>`, `offset == 0` niche), and resolution are here.
//!
//! **Cross-file atomicity is best-effort**, and inherently so: two independent
//! bstack files have no shared commit, so a deep clone / teardown that spans the home
//! file and a foreign file cannot be one atomic unit. Each *file's own* commit is
//! atomic (the foreign side runs its own crash-safe `try_clone_in` / teardown through
//! the adapter), and the ordering always errs toward **over-provisioning** — an
//! orphaned fresh block or an over-count, which leaks — never toward an under-count
//! (a premature free / double-free). A target file not attached at the time makes an
//! owning clone *error* rather than silently alias.

use core::marker::PhantomData;
use core::num::NonZeroU64;
use std::io;

use bstack::{BStack, BStackAllocator, BStackRange, BStackSlice};
use bytemuck::{Pod, Zeroable};

use crate::BStackRaiiAllocator;
use crate::block::{BStackBlock, BStackShared, BStackWeakable};
use crate::clone::TryCloneIn;
use crate::handle::{OwnedRef, WeakRef};
use crate::layout;
use crate::refcount;
use crate::reference::BStackRef;
#[cfg(test)]
use crate::registry::FileRegistry;
use crate::registry::{self, FileId};
use crate::teardown::BStackDrop;

/// The on-disk **wire** form of a [`Foreign`] pointer: a file identity plus an address
/// in that file. 16 bytes, `Pod`. `file_id == 0` is [`SELF`](FileId::SELF) (a pointer
/// into the current file); the target's length is **not** stored — it is recovered from
/// `size_of::<T::OnDisk>()`, exactly like an in-file `#[bstack_ref]`.
///
/// This is the inert wire form only: it carries no type and no resolution API, and is
/// not part of the public prelude (it is `#[doc(hidden)]`). The in-memory reference is
/// [`Foreign<T>`], which carries the target type and — for a `SELF` pointer — a borrow
/// of the file it was read from. The macro converts between the two at the load/store
/// boundary; user code should never name this type.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct ForeignRepr {
    /// The target file's [`FileId`] as a `u64` (`0` = [`FileId::SELF`]).
    file_id: u64,
    /// The target's address within that file.
    offset: u64,
}

impl ForeignRepr {
    /// A wire pointer from a raw `(file_id, offset)`.
    pub const fn new(file_id: u64, offset: u64) -> Self {
        Self { file_id, offset }
    }
    /// The raw file-id word.
    pub const fn file_id(self) -> u64 {
        self.file_id
    }
    /// The target address.
    pub const fn offset(self) -> u64 {
        self.offset
    }
}

/// An **explicit** cross-file pointer: a non-`SELF` file id and an address. The
/// `NonZeroU64` file id is the enum niche that keeps [`FilePtr`] at 16 bytes and makes
/// "explicit" unrepresentable as `SELF`. It carries no borrow — an explicit pointer
/// resolves through the registry, so it is valid independent of any file handle (its
/// deref is fallible).
#[derive(Clone, Copy)]
pub(crate) struct ExternalPtr {
    file_id: NonZeroU64,
    address: u64,
}

/// A **`SELF`** pointer: an address in the current file, branded with a borrow `'a` of
/// that file. The brand is **covariant** in `'a` (an explicit `'static` pointer narrows
/// freely), but a `SELF` pointer can never *widen* its `'a`, so Rust will not let it
/// outlive — or be stored beyond — the file it was read from. Length is recovered from
/// the target type: this is the analogue of a lifetime-branded [`BStackRef`].
#[derive(Clone, Copy)]
pub(crate) struct SelfPtr<'a> {
    address: u64,
    _brand: PhantomData<fn() -> &'a ()>,
}

/// The in-memory discriminated form of a cross-file pointer: either [`ExternalPtr`]
/// (explicit, registry-resolved, borrow-free) or [`SelfPtr`] (`SELF`, borrow-bound).
/// 16 bytes — the `NonZeroU64` niche in `Ext` encodes the discriminant for free
/// (`file_id == 0` ⇒ `SelfRef`).
#[derive(Clone, Copy)]
pub(crate) enum FilePtr<'a> {
    Ext(ExternalPtr),
    SelfRef(SelfPtr<'a>),
}

/// A typed cross-file pointer to a `T`. Either **explicit** (resolved through the
/// process-wide [registry](crate::registry), borrow-free, deref fallible) or
/// [`SELF`](FileId::SELF) (an address in the file it was read from, bound to that file's
/// borrow `'a`).
///
/// `Copy`. An explicit pointer can be [`detach`](Self::detach)ed to a `'static`,
/// borrow-free `Foreign`; a `SELF` pointer cannot (it is only valid within the scope of
/// the file it was read from). `T: 'static` because a persisted block target holds no
/// in-memory borrow. See the [module docs](self) for teardown / deep-clone semantics.
pub struct Foreign<'a, T: 'static> {
    inner: FilePtr<'a>,
    _marker: PhantomData<fn() -> T>,
}

impl<'a, T: 'static> Clone for Foreign<'a, T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<'a, T: 'static> Copy for Foreign<'a, T> {}

impl<'a, T: 'static> Foreign<'a, T> {
    /// Reconstruct from the stored on-disk [`ForeignRepr`]. A `SELF` pointer becomes a
    /// borrow-bound [`SelfPtr`]; an explicit one an [`ExternalPtr`] (which ignores the
    /// borrow — [`detach`](Self::detach) it to escape).
    ///
    /// # Safety
    /// Two obligations:
    /// * `repr` must be a pointer previously stored into this file — it names a valid
    ///   `T` (explicit ⇒ in its own file; `SELF` ⇒ in the file it was read from).
    /// * the caller must bind the returned `Foreign<'a, T>`'s lifetime `'a` to that
    ///   file's borrow (a generated field accessor does this by tying `'a` to the
    ///   `&'a BStack` / `&'a A` it read through), so a `SELF` pointer cannot escape it.
    pub unsafe fn from_repr(repr: ForeignRepr) -> Self {
        let inner = match NonZeroU64::new(repr.file_id) {
            Some(file_id) => FilePtr::Ext(ExternalPtr {
                file_id,
                address: repr.offset,
            }),
            None => FilePtr::SelfRef(SelfPtr {
                address: repr.offset,
                _brand: PhantomData,
            }),
        };
        Self {
            inner,
            _marker: PhantomData,
        }
    }

    /// The on-disk wire pointer, for storing into a field.
    pub fn repr(self) -> ForeignRepr {
        match self.inner {
            FilePtr::Ext(e) => ForeignRepr::new(e.file_id.get(), e.address),
            FilePtr::SelfRef(s) => ForeignRepr::new(0, s.address),
        }
    }

    /// The target's address within its file.
    pub fn offset(self) -> u64 {
        match self.inner {
            FilePtr::Ext(e) => e.address,
            FilePtr::SelfRef(s) => s.address,
        }
    }

    /// Whether this points into the *current* file ([`FileId::SELF`]).
    pub fn is_self(self) -> bool {
        matches!(self.inner, FilePtr::SelfRef(_))
    }

    /// The file this points into: [`SELF`](FileId::SELF) for a `SELF` pointer, else the
    /// explicit target file.
    pub fn file_id(self) -> FileId {
        match self.inner {
            FilePtr::Ext(e) => FileId::from_u64(e.file_id.get()).unwrap_or(FileId::SELF),
            FilePtr::SelfRef(_) => FileId::SELF,
        }
    }

    /// Promote an **explicit** pointer to a `'static`, borrow-free [`Foreign`] — the
    /// registry-resolved form that can be stored anywhere and outlives any file handle
    /// (its deref stays fallible). `None` for a `SELF` pointer, which is only valid
    /// within the scope of the file it was read from.
    pub fn detach(self) -> Option<Foreign<'static, T>> {
        match self.inner {
            FilePtr::Ext(e) => Some(Foreign {
                inner: FilePtr::Ext(e),
                _marker: PhantomData,
            }),
            FilePtr::SelfRef(_) => None,
        }
    }
}

impl<T: 'static> Foreign<'static, T> {
    /// A raw explicit foreign pointer to `offset` within `file`. The sound way to obtain
    /// a `Foreign` is by reading a `#[bstack_block]` field (borrow-bound) or via
    /// [`from_local`](Self::from_local) / [`at`](Self::at); this raw form is for tests
    /// and low-level code.
    ///
    /// # Safety
    /// `offset` names a valid `T` block in `file`. If `file` is [`SELF`](FileId::SELF)
    /// the result is a `'static` `SELF` pointer whose deref against the wrong file reads
    /// the wrong data — the caller must resolve it only against its home file. (A sound
    /// `SELF` pointer is borrow-bound; read it from its field instead.)
    pub const unsafe fn new(file: FileId, offset: u64) -> Self {
        let inner = match NonZeroU64::new(file.as_u64()) {
            Some(file_id) => FilePtr::Ext(ExternalPtr {
                file_id,
                address: offset,
            }),
            None => FilePtr::SelfRef(SelfPtr {
                address: offset,
                _brand: PhantomData,
            }),
        };
        Self {
            inner,
            _marker: PhantomData,
        }
    }
}

impl<'a, T: BStackBlock + 'static> Foreign<'a, T> {
    /// The target's range in its file (`address` + `size_of::<T::OnDisk>()`).
    pub fn range(self) -> BStackRange {
        BStackRange::new(self.offset(), core::mem::size_of::<T::OnDisk>() as u64)
    }

    /// Resolve the pointer and run `f` with a `T` handle at the target plus the
    /// [`BStack`] of the file it lives in.
    ///
    /// The two failure modes are kept apart rather than conflated into one
    /// `Option`: `Ok(None)` is the [null niche](self) (`address == 0`, a genuinely
    /// absent pointer — not an error, same as reading a null `#[bstack_ref]`
    /// field); `Err` is an I/O-shaped [`io::ErrorKind::NotFound`] meaning the
    /// pointer is non-null but its target file can't currently be reached (a
    /// malformed / out-of-range file id, or a file that is unknown / not
    /// currently attached to the [registry](crate::registry)).
    ///
    /// There is exactly one registry — the process-wide one — so an explicit pointer
    /// (e.g. one moved out via `bstack_move!`) is always resolvable on its own; `local`
    /// is used only to resolve a [`SELF`](FileId::SELF) pointer (no registry, no lock).
    pub fn with<A, R>(self, local: &A, f: impl FnOnce(T, &BStack) -> R) -> io::Result<Option<R>>
    where
        A: BStackAllocator,
    {
        if self.offset() == 0 {
            return Ok(None);
        }
        let t = T::from_range(self.range());
        match self.inner {
            FilePtr::SelfRef(_) => Ok(Some(f(t, local.stack()))),
            FilePtr::Ext(e) => {
                let id = FileId::from_u64(e.file_id.get()).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "Foreign: file id out of range")
                })?;
                registry::with_host(id, |host| f(t, host.stack()))
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::NotFound,
                            "Foreign: target file not attached",
                        )
                    })
                    .map(Some)
            }
        }
    }

    /// **foreign → normal** (`bstack_cast!(foreign as BStackRef<T>)`): the offset-only
    /// [`BStackRef`] to the target, valid **in the target's own file**. `Some` iff
    /// that file is [`SELF`](FileId::SELF) or currently attached (so the ref is
    /// resolvable); `None` otherwise. Does no I/O — pair the ref with the target
    /// file's stack (e.g. via [`with`](Self::with)) to read it.
    pub fn as_local_ref(self) -> Option<BStackRef<T>> {
        let resolvable = match self.inner {
            FilePtr::SelfRef(_) => true,
            FilePtr::Ext(e) => FileId::from_u64(e.file_id.get())
                .and_then(|id| registry::get().map(|r| r.is_live(id)))
                .unwrap_or(false),
        };
        // SAFETY: `range()` is this pointer's target region; the returned ref is a
        // plain offset handle (no aliasing/liveness claim beyond the caller's).
        resolvable.then(|| unsafe { BStackRef::from_range(self.range()) })
    }

    /// Like [`with`](Self::with) but against an explicit `registry` — crate-internal,
    /// for tests (the global is a one-shot `OnceLock`, awkward to exercise in unit
    /// tests). Production code uses [`with`](Self::with) against the sole registry.
    #[cfg(test)]
    pub(crate) fn with_in<A, R>(
        self,
        registry: &FileRegistry,
        local: &A,
        f: impl FnOnce(T, &BStack) -> R,
    ) -> io::Result<Option<R>>
    where
        A: BStackAllocator,
    {
        if self.offset() == 0 {
            return Ok(None);
        }
        let t = T::from_range(self.range());
        match self.inner {
            FilePtr::SelfRef(_) => Ok(Some(f(t, local.stack()))),
            FilePtr::Ext(e) => {
                let id = FileId::from_u64(e.file_id.get()).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "Foreign: file id out of range")
                })?;
                registry
                    .with_host(id, |host| f(t, host.stack()))
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::NotFound,
                            "Foreign: target file not attached",
                        )
                    })
                    .map(Some)
            }
        }
    }
}

impl<T: BStackBlock + 'static> Foreign<'static, T> {
    /// A foreign pointer to the block `target` currently occupies, in file `file`.
    pub fn at(file: FileId, target: &T) -> Self {
        // SAFETY: `target` is a live `T` handle, so its start names a valid `T`.
        unsafe { Self::new(file, target.range().start()) }
    }

    /// **normal → foreign** (`bstack_cast!(slice as Foreign<T>)`): name the block a
    /// `BStackSlice` points at as a `Foreign`, resolving the slice's file to its
    /// [`FileId`] via the registry's reverse map. `None` if that file is not
    /// currently attached (so it has no id to name). Does no I/O.
    pub fn from_local(slice: &BStackSlice<'_>) -> Option<Self> {
        let id = registry::id_of_host(slice.stack())?;
        // SAFETY: `slice` names a live block in a registered file (`id`).
        Some(unsafe { Self::new(id, slice.start()) })
    }
}

// ---------------------------------------------------------------------------
// Cross-file teardown helpers.
//
// These run a `Foreign<T>` field's per-kind teardown in *whichever file the target
// lives in*, selected entirely by the `alloc` handed in: the local allocator for a
// `SELF` target, or a
// [`ForeignHostAllocator`](crate::registry::ForeignHostAllocator) (an allocator view
// of the foreign host) for a cross-file one. Because the whole teardown machinery
// (`OwnedRef` / `BStackShared::drop_strong_ref` / `WeakRef`, and the recursive
// `__bstack_drop_children` they call) is generic over `A: BStackRaiiAllocator`, the
// same code frees the target in its own file with no duplication — reads/writes go
// through `alloc.stack()`, and frees are tagged with `alloc.wal_file_id()` so the
// home WAL reclaims them in the right file. The generated `Foreign` field teardown
// picks the helper by the field's annotation. `#[bstack_ref]` has no helper (a
// foreign ref owns nothing).
// ---------------------------------------------------------------------------

/// Tear down an `#[bstack_owned] Foreign<T>` target: free the block at `offset`
/// (and, recursively, its own children) in the file `alloc` addresses.
///
/// # Safety
/// `offset` names a live `T` block, in the file `alloc` addresses, exclusively owned
/// by this foreign pointer (freed exactly once).
pub unsafe fn foreign_drop_owned<T: BStackBlock, A: BStackRaiiAllocator>(
    alloc: &A,
    offset: u64,
) -> io::Result<()> {
    let range = BStackRange::new(offset, core::mem::size_of::<T::OnDisk>() as u64);
    // SAFETY: `range` is the caller-asserted live block of `T`.
    let child = unsafe { BStackRef::<T>::from_range(range) };
    OwnedRef(child).bstack_drop(alloc)
}

/// Tear down an `#[bstack_strong] Foreign<T>` target: decrement the strong count at
/// `offset` (the target's *data* block) and, at zero, free it in the file `alloc`
/// addresses. `T` must be a shared block.
///
/// # Safety
/// `offset` names a live shared `T` data block in the file `alloc` addresses, holding
/// one strong reference on behalf of this foreign pointer.
pub unsafe fn foreign_drop_strong<T: BStackShared, A: BStackRaiiAllocator>(
    alloc: &A,
    offset: u64,
) -> io::Result<()> {
    let range = BStackRange::new(offset, core::mem::size_of::<T::OnDisk>() as u64);
    // SAFETY: `range` is the caller-asserted live data block of a shared `T`.
    let data = unsafe { BStackRef::<T>::from_range(range) };
    <T as BStackShared>::drop_strong_ref(data, alloc)
}

/// Tear down a `#[bstack_weak] Foreign<T>` target: decrement the weak count in the
/// *control* block at `ctrl_offset` and, at zero, free the control block in the file
/// `alloc` addresses. The data block is never touched. `T` must be weakable, and the
/// foreign pointer stores the **control** offset (as an in-file weak field does).
///
/// # Safety
/// `ctrl_offset` names a live `T::Control` block in the file `alloc` addresses,
/// holding one weak reference on behalf of this foreign pointer.
pub unsafe fn foreign_drop_weak<T: BStackWeakable, A: BStackRaiiAllocator>(
    alloc: &A,
    ctrl_offset: u64,
) -> io::Result<()> {
    let range = BStackRange::new(ctrl_offset, core::mem::size_of::<T::Control>() as u64);
    // SAFETY: `range` is the caller-asserted live control block of a weakable `T`.
    let ctrl = unsafe { BStackRef::<T::Control>::from_range(range) };
    WeakRef::<T>(ctrl).bstack_drop(alloc)
}

// ---------------------------------------------------------------------------
// Cross-file deep-clone helpers (the mirror of the teardown helpers above).
//
// A `Foreign<T>` field's clone acts on the target *in the target's own file* per its
// annotation: `#[bstack_owned]` deep-copies the target (a fresh block on that file,
// the pointer repointed); `#[bstack_strong]` / `#[bstack_weak]` share the same target
// and bump its count; `#[bstack_ref]` aliases (byte-copied — no helper). A `SELF`
// pointer instead folds into the *home* clone plan (atomic with the home commit); the
// generated code picks that path. These helpers cover the cross-file case.
//
// **Atomicity across files is best-effort (option-1):** the foreign side is touched
// eagerly (its own commit for `owned`, an atomic increment for `strong`/`weak`) BEFORE
// the home clone commits, and a home failure afterwards does not undo it. That always
// errs toward **over-provisioning** — an orphaned fresh block, or an over-count — which
// leaks, never toward an under-count (which would be a premature free / double-free).
// A target file that is not currently attached makes the clone **error** (aliasing an
// owning pointer would create a second owner ⇒ later double-free); the generated code
// enforces that before calling these.
// ---------------------------------------------------------------------------

/// Deep-clone an `#[bstack_owned] Foreign<T>` target at `offset` in the file `alloc`
/// addresses; returns the new copy's offset. Self-contained atomic commit on that
/// file; eager (a later home-clone failure leaks the new block).
///
/// # Safety
/// `offset` names a live `T` block, in the file `alloc` addresses, owned by this
/// foreign pointer.
pub unsafe fn foreign_clone_owned<T: TryCloneIn + BStackBlock, A: BStackRaiiAllocator>(
    alloc: &A,
    offset: u64,
) -> io::Result<u64> {
    let range = BStackRange::new(offset, core::mem::size_of::<T::OnDisk>() as u64);
    let src = T::from_range(range);
    let new = src.try_clone_in(alloc)?;
    Ok(new.handle().range().start())
}

/// Bump the strong count of an `#[bstack_strong] Foreign<T>` target at `offset` (its
/// data block) in the file `alloc` addresses — the strong reference the clone
/// acquires. Eager atomic increment.
///
/// # Safety
/// `offset` names a live shared `T` data block in the file `alloc` addresses.
pub unsafe fn foreign_clone_strong<T: BStackShared, A: BStackRaiiAllocator>(
    alloc: &A,
    offset: u64,
) -> io::Result<()> {
    let range = BStackRange::new(offset, core::mem::size_of::<T::OnDisk>() as u64);
    // SAFETY: `range` is the caller-asserted live data block of a shared `T`.
    let data = unsafe { BStackRef::<T>::from_range(range) };
    let (data_ref, ctrl) = <T as BStackShared>::strong_parts(data, alloc)?;
    let off = match ctrl {
        None => data_ref.into_range().start() + layout::RC_REFCOUNT_OFFSET,
        Some(c) => c.start() + layout::CTRL_STRONG_OFFSET,
    };
    refcount::fetch_add(alloc.stack(), off, 1)?;
    Ok(())
}

/// Bump the weak count of a `#[bstack_weak] Foreign<T>` target's control block at
/// `ctrl_offset` in the file `alloc` addresses — the weak reference the clone
/// acquires. Eager atomic increment. (`T` documents the intended weakable target; the
/// increment needs only the offset.)
///
/// # Safety
/// `ctrl_offset` names a live `T::Control` block in the file `alloc` addresses.
pub unsafe fn foreign_clone_weak<T: BStackWeakable, A: BStackRaiiAllocator>(
    alloc: &A,
    ctrl_offset: u64,
) -> io::Result<()> {
    refcount::fetch_add(alloc.stack(), ctrl_offset + layout::CTRL_WEAK_OFFSET, 1)?;
    Ok(())
}
