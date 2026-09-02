//! Lifecycle-hook slice abstraction for transparent I/O interception.
//!
//! Requires feature `guarded` (which implies `alloc`).
//!
//! # Overview
//!
//! [`BStackGuardedSlice`] is the core trait.  Implement [`raw_block`] to bind the trait
//! to a [`BStackSlice`], then override any of the four hook methods to intercept
//! I/O.  All read/write/cursor methods are derived automatically from the hooks.
//!
//! The allocator type `A` is a **generic parameter** of the trait rather than an
//! associated type, so a single implementing struct can satisfy
//! `BStackGuardedSlice<'a, A>` for any allocator `A` without coupling the trait
//! to a single concrete allocator.
//!
//! # Borrow semantics
//!
//! A guard sits at the same semantic position as [`BStackSlice`]: a **borrowed
//! I/O view** that owns no region, carries no allocator handle, and frees nothing
//! on `Drop`. It binds only to a [`BStackSlice`], so it inherits the crate's
//! on-disk borrow soundness by composition rather than defining rules of its own.
//! A guard cannot hold a still-freeable region — [`BStackOwnedSlice`](crate::BStackOwnedSlice)
//! exposes only `&self`-scoped views with no safe path to a `'a`-lifetime slice,
//! and `dealloc`/`realloc` consume the owning handle by value — so a guard built
//! in safe code can neither free a region nor observe one freed out from under it.
//! Only `unsafe` (the [`raw_block`] you implement, or
//! [`BStackSlice::from_raw_parts`]) can break that, exactly as for a bare
//! [`BStackSlice`].
//!
//! [`as_slice`]: BStackGuardedSlice::as_slice
//! [`raw_block`]: BStackGuardedSlice::raw_block

use super::{BStackAllocator, BStackRange, BStackSlice};
use std::{borrow::Cow, io, ops::Range};

/// A [`BStackSlice`] abstraction with lifecycle hooks for transparent I/O
/// interception.
///
/// `A` is the allocator type, given as a **generic parameter** so that a single
/// implementing struct can satisfy `BStackGuardedSlice<'a, A>` for any allocator
/// without being locked to one concrete choice.
///
/// # Required methods
///
/// Implement [`len`](BStackGuardedSlice::len) and the `unsafe`
/// [`raw_block`](BStackGuardedSlice::raw_block), which binds the trait to an
/// underlying [`BStackSlice`]; every other method has a working default.
/// [`as_slice`](BStackGuardedSlice::as_slice) defaults to returning
/// [`Unsupported`](std::io::ErrorKind::Unsupported) — override it to expose an
/// apparent view when a meaningful one exists.
///
/// # Hooks
///
/// Override any combination of four hooks to intercept I/O:
///
/// | Hook         | Role                                                        |
/// |--------------|-------------------------------------------------------------|
/// | [`decode`]   | Transform raw bytes read from disk into the apparent bytes.  |
/// | [`encode`]   | Transform apparent bytes into the raw bytes written to disk. |
/// | [`on_read`]  | Observe or deny a read. Return `Err` to deny.               |
/// | [`on_write`] | Observe a completed write (audit, metadata).                |
///
/// [`decode`]/[`encode`] default to identity (`Cow::Borrowed`, no allocation);
/// [`on_read`]/[`on_write`] default to no-ops. Both `offset`s are **relative to
/// the start of the slice** (`0` is the first byte of this view).
///
/// [`decode`]/[`encode`] are whole-block transforms: `read`/`write` and the
/// derived range methods route the entire apparent block through them, which is
/// why a transforming guard (encryption, compression) can implement just
/// [`len`](BStackGuardedSlice::len), [`raw_block`], [`decode`], and [`encode`].
///
/// ## Deprecated hooks
///
/// The former hooks are **deprecated since 0.4.4** and removed in 0.5.0:
/// `post_read` → [`decode`], `pre_write` → [`encode`], `pre_read` → [`on_read`],
/// `post_write` → [`on_write`]. Note `pre_read`'s offset was *absolute* whereas
/// [`on_read`]'s is *relative*. The new hooks bridge to the old ones by default,
/// so an implementor overriding only the old hooks keeps working unchanged until
/// 0.5.0.
///
/// # Borrow semantics
///
/// A guard sits at the same semantic position as [`BStackSlice`]: a borrowed I/O
/// view that owns no region and frees nothing on `Drop`. Because it binds only to
/// a [`BStackSlice`] it cannot hold a still-freeable region, and so inherits the
/// crate's on-disk borrow soundness unchanged. See the module-level
/// documentation for the full argument.
///
/// # Lifetime
///
/// `'a` is the allocator lifetime, matching [`BStackSlice<'a>`]. All implementors
/// must satisfy `Self: 'a` and `A: 'a`. [`as_slice`](BStackGuardedSlice::as_slice)
/// and [`raw_block`](BStackGuardedSlice::raw_block) hand back `BStackSlice<'a>` at
/// that full lifetime — the borrowed-view convention shared with
/// [`BStackSlice::subslice`], not the `&self`-scoped narrowing used by ownership
/// handles.
///
/// [`decode`]: BStackGuardedSlice::decode
/// [`encode`]: BStackGuardedSlice::encode
/// [`on_read`]: BStackGuardedSlice::on_read
/// [`on_write`]: BStackGuardedSlice::on_write
/// [`raw_block`]: BStackGuardedSlice::raw_block
pub trait BStackGuardedSlice<'a, A: BStackAllocator + 'a>
where
    Self: 'a,
{
    /// The apparent slice for this guard view.
    ///
    /// All I/O methods appears to operate within this slice, and all hooks
    /// receive offsets relative to this slice. The implementation should only
    /// return `Some` if the usage of this view is somehow safe and reflect
    /// the actual underlying data. For example, exposing a cyphertext for
    /// encrypted data does not help the caller to understand the actual plaintext
    /// data, so for a guard that performs decryption, this method should return
    /// `None` since there is no clear mapping between the apparent slice and the
    /// underlying data.
    ///
    /// The returned slice should have length equal to called `len()`.
    ///
    /// ## Mutability and safety
    ///
    /// The returned slice is read-only. Mutating the returned slice may cause
    /// data corruption or violate allocator invariants, unless the implementation
    /// explicitly allows it and documents the safety contract. If the implementation
    /// allows mutation, it must ensure that all hooks are properly fired on
    /// subsequent reads and writes, and that any necessary synchronization is
    /// performed to prevent data races or undefined behavior.
    #[inline]
    fn as_slice(&self) -> Result<BStackSlice<'a>, io::Error> {
        Err(io_error!(
            Unsupported,
            "operation not supported on this guarded slice"
        ))
    }

    /// The length of the data in this guarded view.
    ///
    /// This should return the length of the apparent slice returned by `as_slice`,
    /// not the underlying raw block length.
    fn len(&self) -> u64;

    /// Returns `true` if this guarded view contains no data.
    ///
    /// This is a convenience method that defaults to `self.len() == 0`.
    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The raw I/O block for this guarded view.
    ///
    /// A **required** method with no default: return the [`BStackSlice`] that
    /// hooks and I/O actually operate on. It coincides with the apparent
    /// [`as_slice`](BStackGuardedSlice::as_slice) view unless hooks operate on a
    /// coarser granularity than the slice — for example, a block cipher that must
    /// process aligned 16-byte blocks.
    ///
    /// Can be used by custom subview implementations to issue reads against the full block
    /// rather than the narrowed sub-range.
    ///
    /// ## Safety
    ///
    /// In general, calling this method is only safe when pre and post read/write
    /// hooks are not active or are called during the call, otherwise the caller
    /// may risk data corruption or undefined behavior. Prefer overriding `as_slice`
    /// when possible.
    unsafe fn raw_block(&self) -> BStackSlice<'a>;

    /// Transform raw bytes read from the underlying store into the apparent bytes.
    ///
    /// The inverse of [`encode`](BStackGuardedSlice::encode). Called by
    /// [`read`](BStackGuardedSlice::read) with the whole raw block; return
    /// `Cow::Borrowed` to pass through without allocation, or `Cow::Owned` for
    /// decryption, decompression, or other transformations. The transformed
    /// length should equal [`len`](BStackGuardedSlice::len).
    ///
    /// Defaults to the deprecated [`post_read`](BStackGuardedSlice::post_read)
    /// (identity), for back-compat during the deprecation window.
    #[inline]
    fn decode<'d>(&self, data: &'d [u8]) -> io::Result<Cow<'d, [u8]>> {
        #[allow(deprecated)]
        self.post_read(data)
    }

    /// Transform apparent bytes into the raw bytes written to the underlying store.
    ///
    /// The inverse of [`decode`](BStackGuardedSlice::decode). Called by
    /// [`write`](BStackGuardedSlice::write) with the whole apparent block; return
    /// `Cow::Borrowed` to pass through, or `Cow::Owned` for encryption,
    /// compression, or other transformations.
    ///
    /// Defaults to the deprecated [`pre_write`](BStackGuardedSlice::pre_write)
    /// (identity), for back-compat during the deprecation window.
    #[inline]
    fn encode<'d>(&self, data: &'d [u8]) -> io::Result<Cow<'d, [u8]>> {
        #[allow(deprecated)]
        self.pre_write(data)
    }

    /// Observe or deny a read of `len` raw bytes at `offset`.
    ///
    /// `offset` is **relative to the start of the slice**; `len` is the number of
    /// raw bytes about to be read (before [`decode`](BStackGuardedSlice::decode)).
    /// Return `Err` to deny.
    ///
    /// Defaults to bridging the deprecated
    /// [`pre_read`](BStackGuardedSlice::pre_read), whose offset was *absolute*: the
    /// bridge adds the slice start, so an implementor overriding only `pre_read`
    /// still receives the absolute offset it expects.
    #[inline]
    fn on_read(&self, offset: u64, len: u64) -> io::Result<()> {
        let start = unsafe { self.raw_block() }.start();
        #[allow(deprecated)]
        self.pre_read(start.saturating_add(offset), len)
    }

    /// Observe a completed write of `len` logical bytes at `offset`.
    ///
    /// `offset` is **relative to the start of the slice**; `len` is the number of
    /// apparent bytes written (before [`encode`](BStackGuardedSlice::encode)). The
    /// write has already succeeded; use for auditing or metadata.
    ///
    /// Defaults to the deprecated [`post_write`](BStackGuardedSlice::post_write),
    /// whose offset was already relative.
    #[inline]
    fn on_write(&self, offset: u64, len: u64) -> io::Result<()> {
        #[allow(deprecated)]
        self.post_write(offset, len)
    }

    /// Deprecated: renamed to [`on_read`](BStackGuardedSlice::on_read); its offset
    /// is now **relative** to the slice, not absolute.
    #[deprecated(since = "0.4.4", note = "renamed to `on_read`; offset is now relative to the slice")]
    #[inline]
    fn pre_read(&self, _offset: u64, _len: u64) -> io::Result<()> {
        Ok(())
    }

    /// Deprecated: renamed to [`decode`](BStackGuardedSlice::decode).
    #[deprecated(since = "0.4.4", note = "renamed to `decode`")]
    #[inline]
    fn post_read<'d>(&self, data: &'d [u8]) -> io::Result<Cow<'d, [u8]>> {
        Ok(Cow::Borrowed(data))
    }

    /// Deprecated: renamed to [`encode`](BStackGuardedSlice::encode).
    #[deprecated(since = "0.4.4", note = "renamed to `encode`")]
    #[inline]
    fn pre_write<'d>(&self, data: &'d [u8]) -> io::Result<Cow<'d, [u8]>> {
        Ok(Cow::Borrowed(data))
    }

    /// Deprecated: renamed to [`on_write`](BStackGuardedSlice::on_write).
    #[deprecated(since = "0.4.4", note = "renamed to `on_write`")]
    #[inline]
    fn post_write(&self, _offset: u64, _len: u64) -> io::Result<()> {
        Ok(())
    }

    /// Absolute start offset of the **raw block** (`raw_block().start()`) within
    /// the [`BStack`](crate::BStack) payload.
    ///
    /// This is a physical-storage coordinate. The *apparent* view is always
    /// addressed as `[0, len())`; `start` locates where the raw (stored) bytes
    /// actually live. For a pass-through guard (identity `encode`/`decode`) the two
    /// coincide, but for a transforming
    /// guard (encryption, compression) the raw span `end - start` is the encoded
    /// size and differs from the apparent [`len`](BStackGuardedSlice::len). Parity
    /// with [`BStackSlice::start`].
    #[inline]
    fn start(&self) -> u64 {
        unsafe { self.raw_block() }.start()
    }

    /// Absolute exclusive end offset of the **raw block**
    /// (`start() + raw_block().len()`).
    ///
    /// A physical-storage coordinate; `end - start` is the encoded byte count,
    /// which may exceed or fall short of the apparent
    /// [`len`](BStackGuardedSlice::len) for a transforming guard. Parity with
    /// [`BStackSlice::end`].
    #[inline]
    fn end(&self) -> u64 {
        unsafe { self.raw_block() }.end()
    }

    /// Half-open **raw block** byte range `start()..end()` within the
    /// [`BStack`](crate::BStack) payload.
    ///
    /// The physical span the encoded bytes occupy, not the apparent `[0, len())`
    /// view. Parity with [`BStackSlice::range`].
    #[inline]
    fn range(&self) -> Range<u64> {
        unsafe { self.raw_block() }.range()
    }

    /// The **raw block** location as a [`BStackRange`] — the physical
    /// `(offset, len)` pair describing where the encoded bytes are stored.
    ///
    /// Useful for recording or comparing storage locations; it is *not* the
    /// apparent view, and its `len` is the encoded size rather than the decoded
    /// [`len`](BStackGuardedSlice::len). Parity with [`BStackSlice::as_range`].
    #[inline]
    fn as_range(&self) -> BStackRange {
        unsafe { self.raw_block() }.as_range()
    }

    /// The [`BStack`](crate::BStack) backing this guard, borrowed for the full
    /// allocator lifetime `'a`.
    ///
    /// Every guard I/O method ultimately reads from and writes to this store;
    /// `stack` hands out the same reference for callers that need to issue their
    /// own operations against it (for example, cross-region atomics that take a
    /// `&BStack`). Parity with [`BStackSlice::stack`].
    #[inline]
    fn stack(&self) -> &'a crate::BStack {
        unsafe { self.raw_block() }.stack()
    }

    /// Read the entire apparent slice — the decoded bytes — into a newly allocated
    /// `Vec<u8>`.
    ///
    /// Fires [`on_read`](BStackGuardedSlice::on_read)`(0, raw_len)`, reads the raw
    /// block, then passes it through [`decode`](BStackGuardedSlice::decode); the
    /// decoded length may differ from the raw length. Allocates up to twice the raw
    /// length — once for the raw read, and again for the decoded output whenever
    /// `decode` returns `Cow::Owned`. A pass-through guard (identity `decode`)
    /// returns `Cow::Borrowed` and so allocates only the raw read.
    fn read(&self) -> io::Result<Vec<u8>> {
        let slice = unsafe { self.raw_block() };
        self.on_read(0, slice.len())?;
        let raw = slice.read()?;
        Ok(self.decode(&raw)?.into_owned())
    }

    /// Read the entire apparent slice (the decoded bytes) into `buf`.
    ///
    /// `buf.len()` must equal the decoded length, otherwise returns `InvalidInput`.
    ///
    /// **Unlike [`BStackSlice::read_into`], this does not avoid allocation.** It
    /// reads the raw block into a temporary `Vec` and runs
    /// [`decode`](BStackGuardedSlice::decode) (which may allocate again) before
    /// copying the result into `buf` — the caller's buffer is not read into
    /// directly. Use it for the exact-length delivery and length check, not to save
    /// an allocation; when you want the bytes owned anyway, prefer
    /// [`read`](BStackGuardedSlice::read).
    fn read_into(&self, buf: &mut [u8]) -> io::Result<()> {
        let slice = unsafe { self.raw_block() };
        self.on_read(0, slice.len())?;
        let raw = slice.read()?;
        let decoded = self.decode(&raw)?;
        if decoded.len() != buf.len() {
            return Err(io_error!(
                InvalidInput,
                "buffer length does not match decoded length"
            ));
        }
        buf.copy_from_slice(decoded.as_ref());
        Ok(())
    }

    /// Read the apparent sub-range `[start, end)` into a newly allocated `Vec<u8>`.
    ///
    /// There is no partial decode: this reads and decodes the **whole** block (via
    /// [`read`](BStackGuardedSlice::read)) and then copies out the requested
    /// sub-range. Returns `InvalidInput` if `start > end` or `end` exceeds the
    /// apparent [`len`](BStackGuardedSlice::len). Parity with
    /// [`BStackSlice::read_range`], but note the whole-block read and the extra
    /// allocation.
    fn read_range(&self, start: u64, end: u64) -> io::Result<Vec<u8>> {
        let whole = self.read()?;
        if start > end || end as usize > whole.len() {
            return Err(io_error!(
                InvalidInput,
                "range out of bounds of apparent slice"
            ));
        }
        Ok(whole[start as usize..end as usize].to_vec())
    }

    /// Read the apparent sub-range `[start, start + buf.len())` into `buf`.
    ///
    /// Like [`read_range`](BStackGuardedSlice::read_range), this decodes the
    /// **whole** block into a temporary `Vec` first and then copies the sub-range
    /// into `buf` — so, unlike [`BStackSlice::read_range_into`], it allocates and
    /// reads more than the requested range. Returns `InvalidInput` if the range
    /// exceeds the apparent [`len`](BStackGuardedSlice::len).
    fn read_range_into(&self, start: u64, buf: &mut [u8]) -> io::Result<()> {
        let whole = self.read()?;
        let s = start as usize;
        let e = s
            .checked_add(buf.len())
            .filter(|&e| e <= whole.len())
            .ok_or_else(|| io_error!(InvalidInput, "range out of bounds of apparent slice"))?;
        buf.copy_from_slice(&whole[s..e]);
        Ok(())
    }

    /// Read the byte at apparent index `index`, or `None` if out of range.
    ///
    /// Like every scan/search convenience here
    /// ([`contains`](BStackGuardedSlice::contains),
    /// [`find`](BStackGuardedSlice::find),
    /// [`position`](BStackGuardedSlice::position), and the rest), this reads and
    /// decodes the **whole** apparent block on each call. To run several queries,
    /// call [`read`](BStackGuardedSlice::read) once and scan the returned `Vec`
    /// yourself. Parity with [`BStackSlice::get`].
    fn get(&self, index: u64) -> io::Result<Option<u8>> {
        Ok(self.read()?.get(index as usize).copied())
    }

    /// Whether the apparent bytes contain `needle`. Parity with
    /// [`BStackSlice::contains`].
    fn contains(&self, needle: u8) -> io::Result<bool> {
        Ok(self.read()?.contains(&needle))
    }

    /// Whether the apparent bytes start with `prefix`. Parity with
    /// [`BStackSlice::starts_with`].
    fn starts_with(&self, prefix: &[u8]) -> io::Result<bool> {
        Ok(self.read()?.starts_with(prefix))
    }

    /// Whether the apparent bytes end with `suffix`. Parity with
    /// [`BStackSlice::ends_with`].
    fn ends_with(&self, suffix: &[u8]) -> io::Result<bool> {
        Ok(self.read()?.ends_with(suffix))
    }

    /// Apparent index of the first byte equal to `needle`. Parity with
    /// [`BStackSlice::find`].
    fn find(&self, needle: u8) -> io::Result<Option<u64>> {
        Ok(self.read()?.iter().position(|&b| b == needle).map(|i| i as u64))
    }

    /// Apparent index of the last byte equal to `needle`. Parity with
    /// [`BStackSlice::rfind`].
    fn rfind(&self, needle: u8) -> io::Result<Option<u64>> {
        Ok(self.read()?.iter().rposition(|&b| b == needle).map(|i| i as u64))
    }

    /// Apparent index of the first byte satisfying `predicate`. Parity with
    /// [`BStackSlice::position`].
    fn position(&self, predicate: impl Fn(u8) -> bool) -> io::Result<Option<u64>> {
        Ok(self.read()?.iter().position(|&b| predicate(b)).map(|i| i as u64))
    }

    /// Apparent index of the last byte satisfying `predicate`. Parity with
    /// [`BStackSlice::rposition`].
    fn rposition(&self, predicate: impl Fn(u8) -> bool) -> io::Result<Option<u64>> {
        Ok(self.read()?.iter().rposition(|&b| predicate(b)).map(|i| i as u64))
    }

    /// Overwrite the whole apparent slice with `data`.
    ///
    /// Passes `data` through [`encode`](BStackGuardedSlice::encode), writes the
    /// result to the raw block, then fires
    /// [`on_write`](BStackGuardedSlice::on_write)`(0, data.len())`. `data` is the
    /// full apparent block — for a length-preserving guard `data.len()` should
    /// equal [`len`](BStackGuardedSlice::len).
    ///
    /// Requires feature `set`.
    #[cfg(feature = "set")]
    fn write(&self, data: impl AsRef<[u8]>) -> io::Result<()> {
        let mut slice = unsafe { self.raw_block() };
        let data = data.as_ref();
        let cooked = self.encode(data)?;
        slice.write(cooked.as_ref())?;
        self.on_write(0, data.len() as u64)
    }

    /// Overwrite the whole apparent slice with `src`; `src.len()` must equal
    /// [`len`](BStackGuardedSlice::len). Parity with
    /// [`BStackSlice::copy_from_slice`] (returns `InvalidInput` on mismatch rather
    /// than panicking). Requires feature `set`.
    #[cfg(feature = "set")]
    fn copy_from_slice(&self, src: &[u8]) -> io::Result<()> {
        if src.len() as u64 != self.len() {
            return Err(io_error!(InvalidInput, "copy_from_slice: length mismatch"));
        }
        self.write(src)
    }

    /// Overwrite the apparent sub-range at `start` with `data`, atomic
    /// read-modify-write.
    ///
    /// One crash-atomic [`BStack::process_gen`](crate::BStack::process_gen): decodes
    /// the whole block, splices `data` in at `start`, re-encodes, and writes — the
    /// only correct way to patch a transformed (e.g. AEAD) block, all under one write
    /// lock. The internal read does **not** fire
    /// [`on_read`](BStackGuardedSlice::on_read); only
    /// [`on_write`](BStackGuardedSlice::on_write)`(start, data.len())` fires. Parity
    /// with [`BStackSlice::write_range`]. Requires features `set` and `atomic`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    fn write_range(&self, start: u64, data: impl AsRef<[u8]>) -> io::Result<()> {
        let data = data.as_ref();
        atomic_process(self, |whole| {
            let s = start as usize;
            let e = s
                .checked_add(data.len())
                .filter(|&e| e <= whole.len())
                .ok_or_else(|| io_error!(InvalidInput, "range out of bounds of apparent slice"))?;
            whole[s..e].copy_from_slice(data);
            Ok(())
        })?;
        self.on_write(start, data.len() as u64)
    }

    /// Zero the whole apparent slice — writes `encode(&[0; len])`.
    ///
    /// Zeroes the *apparent* (decoded) content, not the raw storage: for a
    /// transforming guard the bytes on disk become the encoding of zeros (e.g. the
    /// ciphertext of a zero block), not literal `0x00`. To scrub the raw bytes,
    /// operate on the underlying [`BStackSlice`] directly. Requires feature `set`.
    #[cfg(feature = "set")]
    fn zero(&self) -> io::Result<()> {
        self.write(vec![0u8; self.len() as usize])
    }

    /// Zero the apparent sub-range `[start, start + n)`, atomic read-modify-write.
    /// Parity with [`BStackSlice::zero_range`]. Requires features `set` and `atomic`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    fn zero_range(&self, start: u64, n: u64) -> io::Result<()> {
        self.write_range(start, vec![0u8; n as usize])
    }

    /// Fill the whole apparent slice with `value` — writes `encode(&[value; len])`.
    ///
    /// Sets the *apparent* content; like [`zero`](BStackGuardedSlice::zero), the raw
    /// bytes on disk are the *encoding* of the fill, not `value` repeated. Parity
    /// with [`BStackSlice::fill`]. Requires feature `set`.
    #[cfg(feature = "set")]
    fn fill(&self, value: u8) -> io::Result<()> {
        self.write(vec![value; self.len() as usize])
    }

    /// Fill the whole apparent slice by calling `f` once per apparent byte, then
    /// writing `encode` of the result.
    ///
    /// `f` generates the *apparent* (decoded) bytes; the raw storage is their
    /// encoding. Parity with [`BStackSlice::fill_with`]. Requires feature `set`.
    #[cfg(feature = "set")]
    fn fill_with(&self, mut f: impl FnMut() -> u8) -> io::Result<()> {
        let buf: Vec<u8> = (0..self.len()).map(|_| f()).collect();
        self.write(buf)
    }

    /// Transform the whole apparent block in place, atomic read-modify-write.
    ///
    /// One crash-atomic [`BStack::process_gen`](crate::BStack::process_gen): decodes
    /// the block, hands it to `f` as a length-preserving mutable slice, re-encodes,
    /// and writes — all under one write lock. The internal read does **not** fire
    /// [`on_read`](BStackGuardedSlice::on_read); only
    /// [`on_write`](BStackGuardedSlice::on_write)`(0, len)` fires, after the write.
    /// The general form of `write_range`/`zero`/`fill`. Requires features `set` and
    /// `atomic`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    fn process(&self, f: impl FnOnce(&mut [u8])) -> io::Result<()> {
        let len = self.len();
        atomic_process(self, |whole| {
            f(whole.as_mut_slice());
            Ok(())
        })?;
        self.on_write(0, len)
    }

    /// Copy the apparent sub-range `src` to `dest` within the slice, atomic
    /// read-modify-write. Parity with [`BStackSlice::copy_within`]. Requires
    /// features `set` and `atomic`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    fn copy_within(&self, src: Range<u64>, dest: u64) -> io::Result<()> {
        let mut n = 0u64;
        atomic_process(self, |whole| {
            let (ss, se, d) = (src.start as usize, src.end as usize, dest as usize);
            let count = se
                .checked_sub(ss)
                .filter(|&c| se <= whole.len() && d.checked_add(c).is_some_and(|e| e <= whole.len()))
                .ok_or_else(|| io_error!(InvalidInput, "copy_within range out of bounds"))?;
            whole.copy_within(ss..se, d);
            n = count as u64;
            Ok(())
        })?;
        self.on_write(dest, n)
    }
}

/// Atomic decode → mutate → encode → write for the length-preserving update methods.
///
/// One crash-atomic [`BStack::process_gen`](crate::BStack::process_gen) on the raw
/// block: under a single held write lock it reads the raw bytes, decodes them, hands
/// the apparent block to `f`, re-encodes, and writes back. `encode` must preserve
/// the raw block length. Any error (`decode`, `f`, `encode`, or a length change)
/// aborts the sequence with **no** write.
#[cfg(all(feature = "set", feature = "atomic"))]
fn atomic_process<'g, A, G>(g: &G, f: impl FnOnce(&mut Vec<u8>) -> io::Result<()>) -> io::Result<()>
where
    A: BStackAllocator + 'g,
    G: BStackGuardedSlice<'g, A> + ?Sized,
{
    use crate::{BStackGenOp, bstack_unsafe_reborrow, bstack_unsafe_reborrow_mut};
    let raw = unsafe { g.raw_block() };
    let stack = raw.stack();
    let raw_start = raw.start();
    let raw_len = raw.len() as usize;
    let mut raw_buf = vec![0u8; raw_len];
    let mut cooked: Vec<u8> = Vec::new();
    let mut f = Some(f);
    let mut step = 0u32;
    stack.process_gen(|| {
        let op = match step {
            // SAFETY: `raw_buf` is declared before this call and never moved; the
            // Read op is consumed before the closure runs again, so no other access
            // to `raw_buf` overlaps it.
            0 => Some(BStackGenOp::Read {
                offset: raw_start,
                buf: bstack_unsafe_reborrow_mut!(&mut raw_buf[..]),
            }),
            1 => {
                let built = (|| -> io::Result<()> {
                    let mut whole = g.decode(&raw_buf)?.into_owned();
                    (f.take().expect("atomic_process f called once"))(&mut whole)?;
                    let out = g.encode(&whole)?;
                    if out.len() != raw_buf.len() {
                        return Err(io_error!(
                            InvalidInput,
                            "guard `encode` returned {} bytes for a {}-byte block; operations \
                             that re-encode the whole block after a partial change require \
                             `encode` to preserve the block length. This typically indicates a \
                             programming error in the guard's `encode` implementation.",
                            out.len(),
                            raw_buf.len()
                        ));
                    }
                    cooked = out.into_owned();
                    Ok(())
                })();
                match built {
                    // Empty block (also empty encode) — nothing to write.
                    Ok(()) if cooked.is_empty() => None,
                    // SAFETY: `cooked` is declared before this call, is not mutated
                    // after this step, and the Write ends the sequence.
                    Ok(()) => Some(BStackGenOp::Write {
                        offset: raw_start,
                        data: bstack_unsafe_reborrow!(&cooked[..]),
                    }),
                    Err(e) => Some(BStackGenOp::Abort { source: Some(e) }),
                }
            }
            _ => None,
        };
        step += 1;
        op
    })
}

/// Marker trait for [`BStackGuardedSlice`] implementations that guarantee
/// atomicity and crash safety.
///
/// # Safety
///
/// Implementors must uphold **both** of the following invariants:
///
/// 1. **Atomicity** — for each `read` or `write` call, the pre-hook, I/O, and
///    post-hook execute as an uninterruptible unit.  No other operation on the
///    same underlying slice can observe an intermediate state.
///
/// 2. **Crash safety** — if the process crashes after a `write` returns `Ok`,
///    the slice contains either the fully written new value or the previous
///    value.  Partially-written states must be impossible or automatically
///    recoverable on the next open.
///
/// Note: holding the bstack write lock alone does **not** satisfy invariant 2
/// unless the implementation can also ensure crash safety or recoverability at
/// the slice level.  For example, an implementation that writes to a temporary
/// location and atomically renames on success and an implementation that only
/// issue one write per slice satisfies the contract, but an implementation that
/// performs multiple writes that may result in partial updates does not.
///
/// Requires feature `atomic`.
#[cfg(feature = "atomic")]
pub unsafe trait BStackAtomicGuardedSlice<'a, A: BStackAllocator + 'a>:
    BStackGuardedSlice<'a, A>
where
    Self: 'a,
{
    /// Swap the raw stored bytes of this guard with `other` — a single
    /// crash-atomic [`BStackSlice::swap`] on the raw block.
    ///
    /// Operates on **raw** bytes and bypasses [`encode`](BStackGuardedSlice::encode)/
    /// [`decode`](BStackGuardedSlice::decode); for a pass-through guard the raw and
    /// apparent bytes coincide.
    /// Requires features `set` and `atomic`.
    #[cfg(feature = "set")]
    fn swap(&self, other: &mut BStackSlice<'_>) -> io::Result<()> {
        let mut slice = unsafe { self.raw_block() };
        slice.swap(other)
    }

    /// Compare-and-swap on the raw stored bytes: if `guard`'s bytes equal
    /// `expected`, overwrite the raw block with `new_bytes` and return the prior
    /// raw bytes. One crash-atomic [`BStackSlice::cas_on`].
    ///
    /// Operates on **raw** bytes and bypasses `encode`/`decode`; `expected` and
    /// `new_bytes` are in raw (encoded) form. Requires features `set` and `atomic`.
    #[cfg(feature = "set")]
    fn cas_on(
        &self,
        guard: &BStackSlice<'_>,
        expected: impl AsRef<[u8]>,
        new_bytes: impl AsRef<[u8]>,
    ) -> io::Result<Option<Vec<u8>>> {
        let mut slice = unsafe { self.raw_block() };
        slice.cas_on(guard, expected, new_bytes)
    }

    /// Like [`cas_on`](Self::cas_on) but swaps when the comparison **fails**
    /// ([`BStackSlice::cas_on_ne`]). Raw bytes. Requires `set` and `atomic`.
    #[cfg(feature = "set")]
    fn cas_on_ne(
        &self,
        guard: &BStackSlice<'_>,
        expected: impl AsRef<[u8]>,
        new_bytes: impl AsRef<[u8]>,
    ) -> io::Result<Option<Vec<u8>>> {
        let mut slice = unsafe { self.raw_block() };
        slice.cas_on_ne(guard, expected, new_bytes)
    }

    /// Masked compare-and-swap on the raw stored bytes
    /// ([`BStackSlice::cas_on_masked`]): the condition is
    /// `(guard[i] & mask[i]) == (expected[i] & mask[i])`. Raw bytes. Requires
    /// `set` and `atomic`.
    #[cfg(feature = "set")]
    fn cas_on_masked(
        &self,
        guard: &BStackSlice<'_>,
        mask: impl AsRef<[u8]>,
        expected: impl AsRef<[u8]>,
        new_bytes: impl AsRef<[u8]>,
    ) -> io::Result<Option<Vec<u8>>> {
        let mut slice = unsafe { self.raw_block() };
        slice.cas_on_masked(guard, mask, expected, new_bytes)
    }
}

/// Extension trait for [`BStackGuardedSlice`] implementations that can produce
/// a narrowed sub-view while preserving the full hook scope of the parent.
///
/// [`raw_block`]: BStackGuardedSlice::raw_block
pub trait BStackGuardedSliceSubview<'a, A: BStackAllocator + 'a>:
    BStackGuardedSlice<'a, A>
where
    Self: 'a,
{
    /// Narrow this view to the sub-range `[start, end)` within the slice.
    ///
    /// `start` and `end` are relative to [`as_slice`](BStackGuardedSlice::as_slice),
    /// or equivalently in the range `[0, len())`. The returned view preserves
    /// the parent's hook scope — calls to `on_read`, `decode`, etc. on the
    /// subview delegate to the parent with appropriately translated offsets.
    ///
    /// # Panics
    ///
    /// Panics if the specified range is out of bounds of the apparent slice.
    fn subview(&self, start: u64, end: u64) -> impl BStackGuardedSliceSubview<'a, A> + '_;

    /// Narrow this view to the sub-range specified by `range`.
    ///
    /// `start` and `end` are relative to [`as_slice`](BStackGuardedSlice::as_slice),
    /// or equivalently in the range `[0, len())`. The returned view preserves
    /// the parent's hook scope — calls to `on_read`, `decode`, etc. on the
    /// subview delegate to the parent with appropriately translated offsets.
    ///
    /// # Panics
    ///
    /// Panics if the specified range is out of bounds of the apparent slice.
    #[inline]
    fn subview_range(
        &self,
        range: std::ops::Range<u64>,
    ) -> impl BStackGuardedSliceSubview<'a, A> + '_ {
        self.subview(range.start, range.end)
    }

    /// Sub-view of the first `min(n, len)` bytes. Parity with [`BStackSlice::head`].
    #[inline]
    fn head(&self, n: u64) -> impl BStackGuardedSliceSubview<'a, A> + '_ {
        self.subview(0, n.min(self.len()))
    }

    /// Sub-view of the last `min(n, len)` bytes. Parity with [`BStackSlice::tail`].
    #[inline]
    fn tail(&self, n: u64) -> impl BStackGuardedSliceSubview<'a, A> + '_ {
        let len = self.len();
        let n = n.min(len);
        self.subview(len - n, len)
    }
}

/// Marker trait for [`BStackGuardedSliceSubview`] implementations that also
/// satisfy [`BStackAtomicGuardedSlice`]'s atomicity and crash-safety contract.
///
/// # Safety
///
/// See [`BStackAtomicGuardedSlice`] for the full safety contract.
///
/// Requires feature `atomic`.
#[cfg(feature = "atomic")]
pub unsafe trait BStackAtomicGuardedSliceSubview<'a, A: BStackAllocator + 'a>:
    BStackAtomicGuardedSlice<'a, A> + BStackGuardedSliceSubview<'a, A>
where
    Self: 'a,
{
}
