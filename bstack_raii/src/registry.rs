//! Process-wide registry of bstack files — the stateful foundation for
//! `Foreign<T>` (cross-file pointers), built before `Foreign<T>` itself.
//!
//! A `Foreign<T>` is "a slice with a file identity attached": a [`FileId`] plus a
//! [`BStackRange`]. Paths are variable-length and awkward to embed on disk, so the
//! registry maps each file's **persistent path** to a small, **stable** numeric
//! [`FileId`] that a `Foreign` can store as a plain integer. Resolving that id
//! back to the file's live allocator (to read/write/allocate in it) happens here.
//!
//! ## Two layers
//!
//! * **Persistent** — a dedicated bstack file (its own [`FirstFitBStackAllocator`])
//!   holding the **append-only** path table. Ids are just indices into it, so a
//!   `Foreign` written to disk with `FileId(5)` means the same path on every future
//!   run. Paths are *never removed* (that would renumber ids and dangle stored
//!   `Foreign`s); only appended.
//! * **In-memory** — mirrors the path↔id maps and adds `id -> live host`: the
//!   *open* allocator for a file, type-erased behind [`ForeignHost`]. Guarded by a
//!   [`parking_lot::RwLock`].
//!
//! ## Why an `RwLock` (and why `parking_lot`)
//!
//! Resolving a foreign file to run an op on it is **hot** and concurrent (many
//! readers); *detaching* a live file is **cold** and must not race an in-flight op
//! (an exclusive writer). That is exactly a read-write lock — and since the read
//! side sits on the bstack io hot path, [`parking_lot::RwLock`] (cheaper, no
//! poisoning) is preferred over `std`. [`FileRegistry::with_host`] holds the read
//! lock for the whole duration of the caller's closure, so a concurrent
//! [`detach`](FileRegistry::detach) blocks until the op finishes — the "stop
//! token" that keeps a live file from vanishing mid-operation.
//!
//! ## Optional and zero-cost when unused
//!
//! The registry is a lazily-created global: a program that never registers a file
//! never instantiates it, so ordinary single-file ops pay nothing. It is brought
//! up explicitly with [`init`].

use core::fmt;
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use bstack::{BStack, BStackAllocator, BStackOwnedSlice, BStackOwnedSliceAllocator, BStackRange};
use parking_lot::RwLock;

use crate::BStackRaiiAllocator;

/// A small, stable identity for a registered bstack file: an index into the
/// registry's append-only path table.
///
/// Backed by a `u32` (a sane program opens far fewer than `u16::MAX` files, so
/// this is generous headroom), but a `Foreign` pointer stores it **widened to a
/// `u64`** — for alignment next to a [`BStackRange`], and to leave room for future
/// RTTI. [`as_u64`](Self::as_u64) / [`from_u64`](Self::from_u64) bridge the two.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FileId(u32);

impl FileId {
    /// The raw `u32` index.
    pub const fn get(self) -> u32 {
        self.0
    }

    /// The id widened to the `u64` a `Foreign` pointer stores on disk.
    pub const fn as_u64(self) -> u64 {
        self.0 as u64
    }

    /// Reconstruct a `FileId` from its on-disk `u64` form, rejecting values that
    /// do not fit the `u32` id space (corruption / a foreign id from a wider build).
    pub const fn from_u64(v: u64) -> Option<Self> {
        if v <= u32::MAX as u64 {
            Some(FileId(v as u32))
        } else {
            None
        }
    }
}

/// A thread-shareable [`BStackRaiiAllocator`] — the bound a file's live host must
/// satisfy to be stored in (and resolved from) the registry across threads.
///
/// Purely a convenience alias (`BStackRaiiAllocator + Send + Sync`, blanket-impl'd)
/// so call sites don't repeat the `+ Send + Sync` every time. It is **not** what
/// the registry stores: `BStackRaiiAllocator` is not object-safe (`BStackAllocator:
/// Sized`, plus the GAT `Allocated<'a>` and `alloc -> Self::Allocated<'_>`), so
/// there is no `dyn SyncBStackRaiiAllocator`. [`ForeignHost`] is its object-safe
/// projection, and what actually goes behind the `Arc<dyn …>`.
pub trait SyncBStackRaiiAllocator: BStackRaiiAllocator + Send + Sync {}
impl<A: BStackRaiiAllocator + Send + Sync> SyncBStackRaiiAllocator for A {}

/// Error returned by [`ForeignHost::realloc`] and [`ForeignHost::dealloc`] when the
/// operation fails — the object-safe, range-based analogue of bstack's
/// `BStackAllocError`.
///
/// A failed resize or free almost always leaves a valid allocation behind — the
/// original region untouched, or the new region fully committed. This type carries
/// that surviving region's range back to the caller so it can retry, fall back, or
/// explicitly [`dealloc`](ForeignHost::dealloc) it rather than leak it. Because a
/// bare [`BStackRange`] carries no ownership or `Drop`, *not* returning it here
/// would silently lose the region.
///
/// Implements [`std::error::Error`] (delegating [`Display`](fmt::Display) to
/// [`source`](Self::source)), so `?` works in functions that return it.
pub struct ForeignAllocError {
    /// The underlying I/O error that caused the operation to fail.
    pub source: io::Error,
    /// The recovered region's range, if it survived the failure.
    ///
    /// * `Some` — the allocation is intact and owned by the caller again (the
    ///   overwhelmingly common case: an untouched original or a fully committed new
    ///   region).
    /// * `None` — the region was consumed or lost during the failed operation (a
    ///   multi-step path whose later step failed, or a crash mid-op); any bytes are
    ///   recoverable only through the file's crash-recovery / WAL. Treat `None` as
    ///   "not recoverable here," not as impossible.
    pub handle: Option<BStackRange>,
}

impl ForeignAllocError {
    /// Construct an error that hands the still-valid range back to the caller.
    #[inline]
    pub fn with_handle(source: io::Error, handle: BStackRange) -> Self {
        Self {
            source,
            handle: Some(handle),
        }
    }

    /// Construct an error whose region was consumed or lost and cannot be returned.
    #[inline]
    pub fn lost(source: io::Error) -> Self {
        Self {
            source,
            handle: None,
        }
    }
}

impl fmt::Debug for ForeignAllocError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignAllocError")
            .field("source", &self.source)
            .field("handle", &self.handle)
            .finish()
    }
}

impl fmt::Display for ForeignAllocError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.source, f)
    }
}

impl std::error::Error for ForeignAllocError {}

/// An **object-safe, range-based** view of a live file's allocator — the
/// type-erased handle a `Foreign<T>` uses to reach *into another file*.
///
/// This mirrors bstack's `BStackAllocator` surface (`stack` / `alloc` / `realloc` /
/// `dealloc`, plus `len` / `is_empty`), but is deliberately object-safe so the
/// registry can store `Arc<dyn ForeignHost>` for files backed by different
/// allocator types: it drops the GAT `Allocated<'a>` handle and the associated
/// `Error` in favour of a plain [`BStackRange`] and [`io::Error`] — the very things
/// that make `BStackRaiiAllocator` itself non-object-safe (see
/// [`SyncBStackRaiiAllocator`]). Blanket-implemented for every
/// [`SyncBStackRaiiAllocator`], forwarding to the real allocator.
///
/// Because a [`BStackRange`] carries no ownership (unlike a `BStackOwnedSlice`),
/// [`realloc`](Self::realloc) and [`dealloc`](Self::dealloc) are `unsafe`: the
/// caller asserts the range is a live allocation in this file that no other handle
/// will also resize or free. On the failure path they return a [`ForeignAllocError`]
/// carrying the surviving range, so a failed op never silently leaks. Raw reads and
/// writes go through [`stack`](Self::stack) (`get_into` / `set`).
///
/// # Crash consistency
///
/// Every method forwards to a single underlying allocator/stack call, so it
/// inherits that call's crash-consistency class (see the concrete allocator's docs).
pub trait ForeignHost: Send + Sync {
    /// A shared reference to this file's underlying [`BStack`], for raw reads and
    /// writes (`get_into` / `set`) at a resolved offset.
    fn stack(&self) -> &BStack;

    /// Allocate `len` zero-initialised bytes, returning the region's range. The
    /// region is durably synced before returning; `len = 0` is valid.
    fn alloc(&self, len: u64) -> io::Result<BStackRange>;

    /// Resize the region at `handle` to `new_len` bytes, returning the (possibly
    /// moved) new range.
    ///
    /// # Safety
    /// `handle` must be a live allocation in this file, solely owned by the caller.
    ///
    /// # Errors
    /// Returns a [`ForeignAllocError`] on failure (including when the allocator does
    /// not support reallocation). A failed resize leaves the original region intact,
    /// so implementations return it in [`ForeignAllocError::handle`] (`Some`)
    /// whenever it survives, reserving `None` for a genuinely lost region.
    unsafe fn realloc(
        &self,
        handle: BStackRange,
        new_len: u64,
    ) -> Result<BStackRange, ForeignAllocError>;

    /// Release the region at `handle`.
    ///
    /// # Safety
    /// `handle` must be a live allocation in this file, solely owned by the caller
    /// and freed exactly once.
    ///
    /// # Errors
    /// Returns a [`ForeignAllocError`] on failure. A failed free normally leaves the
    /// region still allocated, so implementations return it in
    /// [`ForeignAllocError::handle`] (`Some`) whenever it survives, reserving `None`
    /// for a genuinely lost region (where handing it back would risk a double-free).
    unsafe fn dealloc(&self, handle: BStackRange) -> Result<(), ForeignAllocError>;

    /// This file's WAL anchor slot, if it participates in crash reclamation
    /// ([`BStackRaiiAllocator::wal_anchor`]).
    fn wal_anchor(&self) -> Option<u64>;
}

impl<A: SyncBStackRaiiAllocator> ForeignHost for A {
    fn stack(&self) -> &BStack {
        <A as BStackAllocator>::stack(self)
    }

    fn alloc(&self, len: u64) -> io::Result<BStackRange> {
        Ok(<A as BStackAllocator>::alloc(self, len)?.as_range())
    }

    unsafe fn realloc(
        &self,
        handle: BStackRange,
        new_len: u64,
    ) -> Result<BStackRange, ForeignAllocError> {
        // SAFETY: caller's contract — a live, solely-owned allocation in this file.
        let slice: BStackOwnedSlice<'_, A> =
            unsafe { BStackOwnedSlice::from_raw_range(self, handle) };
        match <A as BStackAllocator>::realloc(self, slice, new_len) {
            Ok(s) => Ok(s.as_range()),
            Err(e) => Err(ForeignAllocError {
                source: e.source,
                handle: e.handle.map(|h| h.as_range()),
            }),
        }
    }

    unsafe fn dealloc(&self, handle: BStackRange) -> Result<(), ForeignAllocError> {
        // SAFETY: caller's contract — a live, solely-owned allocation in this file.
        let slice: BStackOwnedSlice<'_, A> =
            unsafe { BStackOwnedSlice::from_raw_range(self, handle) };
        match <A as BStackAllocator>::dealloc(self, slice) {
            Ok(()) => Ok(()),
            Err(e) => Err(ForeignAllocError {
                source: e.source,
                handle: e.handle.map(|h| h.as_range()),
            }),
        }
    }

    fn wal_anchor(&self) -> Option<u64> {
        <A as BStackRaiiAllocator>::wal_anchor(self)
    }
}

/// Persistent backing: an append-only log on the registry's own bstack file.
///
/// No allocator needed — the path table is append-only, and a `BStack` *is* a
/// durable stack, so we just `push` one record per path and read them back from the
/// bottom. Each record is `[len: u64 | path bytes]`; the record's index (order of
/// pushing) is its `FileId`. Each `push` is crash-atomic (bstack contract), so a
/// crash leaves whole records only — a partial trailing record is impossible.
struct RegistryStore {
    stack: BStack,
}

impl RegistryStore {
    /// Open (or create) the registry file and load its path table into memory.
    fn open(path: &Path) -> io::Result<(Self, Vec<PathBuf>)> {
        let stack = BStack::open(path)?;
        let paths = Self::load(&stack)?;
        Ok((RegistryStore { stack }, paths))
    }

    /// Load the append-only path table from the registry's bstack file into memory.
    fn load(stack: &BStack) -> io::Result<Vec<PathBuf>> {
        let total = stack.len()? as usize;
        if total == 0 {
            return Ok(Vec::new());
        }
        let mut buf = vec![0u8; total];
        stack.get_into(0, &mut buf)?;
        let mut paths = Vec::new();
        let mut cur = 0usize;
        while cur + 8 <= buf.len() {
            let len = u64::from_le_bytes(buf[cur..cur + 8].try_into().unwrap()) as usize;
            cur += 8;
            if cur + len > buf.len() {
                // Truncated trailing record — shouldn't happen (push is atomic), but
                // stop rather than misparse.
                break;
            }
            paths.push(bytes_to_path(&buf[cur..cur + len]));
            cur += len;
        }
        Ok(paths)
    }

    /// Append one `[len | path]` record to the log (one atomic `push`).
    fn append(&self, path: &Path) -> io::Result<()> {
        let bytes = path_to_bytes(path);
        let mut rec = Vec::with_capacity(8 + bytes.len());
        rec.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        rec.extend_from_slice(&bytes);
        self.stack.push(rec)?;
        Ok(())
    }
}

/// The registry itself.
///
/// See [`FileRegistry`] for the public interface.
struct RegistryInner<'h> {
    /// `id -> path` (append-only; index is the `FileId`).
    paths: Vec<PathBuf>,
    /// `path -> id`, for idempotent registration.
    by_path: HashMap<PathBuf, FileId>,
    /// `id -> live host` (the open allocator), or `None` when the file is not
    /// currently attached. In-memory only.
    live: Vec<Option<Arc<dyn ForeignHost + 'h>>>,
    store: RegistryStore,
}

/// The file registry (see the [module docs](self)).
///
/// The in-memory mirror of the persistent path table, plus the live host for each
/// file. Guarded by a [`parking_lot::RwLock`] for concurrent reads and exclusive writes.
///
/// The `'h` lifetime is how long an attached host must live: a host need only
/// outlive *its attachment*, not the whole program, so a scoped `FileRegistry<'a>`
/// can hold hosts borrowing local data. The process-wide singleton behind [`init`]
/// is a `FileRegistry<'static>` (a `static` can hold nothing shorter), which is why
/// the free-function [`attach`] requires `'static` while [`FileRegistry::attach`]
/// does not.
pub struct FileRegistry<'h> {
    inner: RwLock<RegistryInner<'h>>,
}

impl<'h> FileRegistry<'h> {
    /// Open (or create) a registry backed by the file at `path`. The process-wide
    /// [`init`] wraps this; a standalone instance is mainly useful for tests
    /// (the global is a one-shot singleton).
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        let (store, paths) = RegistryStore::open(path)?;
        let by_path = paths
            .iter()
            .enumerate()
            .map(|(i, p)| (p.clone(), FileId(i as u32)))
            .collect();
        let live = (0..paths.len()).map(|_| None).collect();
        Ok(FileRegistry {
            inner: RwLock::new(RegistryInner {
                paths,
                by_path,
                live,
                store,
            }),
        })
    }

    /// Assign (or look up) the stable [`FileId`] for `path`, persisting a new path
    /// to the append-only table. Idempotent: an already-registered path returns its
    /// existing id without touching disk.
    pub fn register_path(&self, path: &Path) -> io::Result<FileId> {
        let mut g = self.inner.write();
        if let Some(&id) = g.by_path.get(path) {
            return Ok(id);
        }
        let id = FileId(g.paths.len() as u32);
        // Persist first (append the record); only mutate memory once the disk write
        // succeeds, so a failed append leaves us consistent.
        g.store.append(path)?;
        g.paths.push(path.to_path_buf());
        g.by_path.insert(path.to_path_buf(), id);
        g.live.push(None);
        Ok(id)
    }

    /// Register `path` (if needed) and mark it **live**, storing `host` as the open
    /// allocator resolved by [`with_host`](Self::with_host). Returns the file's id.
    ///
    /// `host` need only live as long as `'h` (until this registry — or the host's
    /// [`detach`](Self::detach) — drops it), not `'static`.
    pub fn attach(&self, path: &Path, host: Arc<dyn ForeignHost + 'h>) -> io::Result<FileId> {
        let id = self.register_path(path)?;
        let mut g = self.inner.write();
        g.live[id.0 as usize] = Some(host);
        Ok(id)
    }

    /// Drop the live host for `id` (the file's *path* stays registered forever).
    /// Takes the write lock, so it waits for any in-flight [`with_host`] op to
    /// finish and cannot run *during* one.
    pub fn detach(&self, id: FileId) {
        let mut g = self.inner.write();
        if let Some(slot) = g.live.get_mut(id.0 as usize) {
            *slot = None;
        }
    }

    /// Run `f` against `id`'s live host under a (recursive) read lock, so the file
    /// cannot be [`detach`](Self::detach)ed while `f` runs. Returns `None` if `id`
    /// is unknown or not currently live.
    ///
    /// Uses `read_recursive`, so a foreign op whose `f` itself resolves *another*
    /// foreign file (nesting `with_host`) never deadlocks behind a queued writer:
    /// readers are admitted even while a `detach` waits. The trade-off is that a
    /// `detach` can be starved by a continuous stream of readers — acceptable, since
    /// detaching is cold and a perpetually-in-use file cannot be safely detached
    /// anyway.
    pub fn with_host<R>(&self, id: FileId, f: impl FnOnce(&dyn ForeignHost) -> R) -> Option<R> {
        let g = self.inner.read_recursive();
        let host = g.live.get(id.0 as usize)?.as_ref()?;
        Some(f(&**host))
    }

    /// The path registered for `id`, if any.
    pub fn path_of(&self, id: FileId) -> Option<PathBuf> {
        self.inner.read().paths.get(id.0 as usize).cloned()
    }

    /// The id registered for `path`, if any.
    pub fn id_of(&self, path: &Path) -> Option<FileId> {
        self.inner.read().by_path.get(path).copied()
    }

    /// Whether `id` currently has a live host attached.
    pub fn is_live(&self, id: FileId) -> bool {
        self.inner
            .read()
            .live
            .get(id.0 as usize)
            .is_some_and(Option::is_some)
    }
}

/// The lazily-instantiated process-wide singleton + free-function front door.
/// A `static` holds nothing shorter than `'static`, so the global registry's hosts
/// are `'static` (see [`FileRegistry`] for the scoped, shorter-lived alternative).
static REGISTRY: OnceLock<FileRegistry<'static>> = OnceLock::new();

/// Bring up the process-wide registry, backed by the bstack file at
/// `registry_path` (created if absent, its path table loaded if present). Call
/// once, before any [`attach`]/[`register_path`]. Errors if already initialized.
pub fn init(registry_path: impl AsRef<Path>) -> io::Result<()> {
    let reg = FileRegistry::open(registry_path.as_ref())?;
    REGISTRY.set(reg).map_err(|_| {
        io::Error::new(
            io::ErrorKind::AlreadyExists,
            "file registry already initialized",
        )
    })
}

/// The initialized registry, or `None` if [`init`] has not run. `Foreign`
/// resolution uses this so an unregistered/opt-out program pays nothing.
pub fn get() -> Option<&'static FileRegistry<'static>> {
    REGISTRY.get()
}

fn require() -> io::Result<&'static FileRegistry<'static>> {
    REGISTRY.get().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "file registry not initialized, init with `bstack_raii::registry::init` first",
        )
    })
}

/// [`FileRegistry::register_path`] on the process-wide registry.
pub fn register_path(path: impl AsRef<Path>) -> io::Result<FileId> {
    require()?.register_path(path.as_ref())
}

/// [`FileRegistry::attach`] on the process-wide registry, taking an owned
/// allocator (any [`SyncBStackRaiiAllocator`]) as the file's live host.
///
/// The `'static` bound is inherent to the *global* registry — a `static` cannot
/// hold a shorter-lived host. It is not a constraint of the machinery: bstack's own
/// allocators own their file and are `'static` anyway, and a host that borrows must
/// go through a scoped [`FileRegistry`] instance (whose `attach` accepts any `'h`).
pub fn attach<A>(path: impl AsRef<Path>, allocator: A) -> io::Result<FileId>
where
    A: SyncBStackRaiiAllocator + 'static,
{
    require()?.attach(path.as_ref(), Arc::new(allocator))
}

/// [`FileRegistry::detach`] on the process-wide registry (no-op if uninitialized).
pub fn detach(id: FileId) {
    if let Some(reg) = REGISTRY.get() {
        reg.detach(id);
    }
}

/// [`FileRegistry::with_host`] on the process-wide registry (`None` if
/// uninitialized or `id` is not live).
pub fn with_host<R>(id: FileId, f: impl FnOnce(&dyn ForeignHost) -> R) -> Option<R> {
    REGISTRY.get()?.with_host(id, f)
}

/// The path registered for `id`, if any.
pub fn path_of(id: FileId) -> Option<PathBuf> {
    REGISTRY.get()?.path_of(id)
}

/// The id registered for `path`, if any.
pub fn id_of(path: impl AsRef<Path>) -> Option<FileId> {
    REGISTRY.get()?.id_of(path.as_ref())
}

// Path <-> bytes (exact round-trip on unix; lossy elsewhere).

#[cfg(unix)]
fn path_to_bytes(p: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    p.as_os_str().as_bytes().to_vec()
}

#[cfg(unix)]
fn bytes_to_path(b: &[u8]) -> PathBuf {
    use std::os::unix::ffi::OsStrExt;
    std::ffi::OsStr::from_bytes(b).to_owned().into()
}

#[cfg(not(unix))]
fn path_to_bytes(p: &Path) -> Vec<u8> {
    p.to_string_lossy().into_owned().into_bytes()
}

#[cfg(not(unix))]
fn bytes_to_path(b: &[u8]) -> PathBuf {
    PathBuf::from(String::from_utf8_lossy(b).into_owned())
}
