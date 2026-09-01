//! Range access control on [`BStack`] (the `expensive-slice-access-control`
//! feature): the capability tokens, the authority resolution, and the checked
//! policy entry points.
//!
//! The lock-free policy machinery — the mode enum, the point table, and its
//! lookup / range-check / set operations — lives in [`acl_core`](crate::acl_core).
//! This module bolts it onto a live stack: the two one-shot tokens, the check
//! that every guarded I/O path consults, and [`protect`](BStack::protect) /
//! [`protect_as`](BStack::protect_as) that arm ranges.
//!
//! The [`acl_check!`] macro lives at the module root (always compiled) so the
//! guarded I/O paths in `lib.rs` can invoke it unconditionally; it folds to
//! nothing without the feature. Everything else is in the gated `inner` module.

/// Enforce the access-control policy over `[$a, $b)` for one axis on `$stack`,
/// then `?` on denial — expanding to nothing without the
/// `expensive-slice-access-control` feature. The op is a bare
/// [`AccessOp`](crate::AccessOp) variant (`Read`/`Write`/`Truncate`); authorities
/// default to [`NONE`](crate::BStackAccessAuthorities::NONE), overridable by a
/// trailing expression. Takes the stack explicitly (like [`fault_point!`], since
/// `self` cannot cross the macro's hygiene boundary).
#[allow(unused_macros)]
macro_rules! acl_check {
    ($stack:expr, $a:expr, $b:expr, $op:ident $(,)?) => {
        acl_check!($stack, $a, $b, $op, $crate::BStackAccessAuthorities::NONE)
    };
    ($stack:expr, $a:expr, $b:expr, $op:ident, $held:expr $(,)?) => {{
        #[cfg(feature = "expensive-slice-access-control")]
        $stack.acl_check($a, $b, $crate::AccessOp::$op, $held)?;
    }};
}
pub(crate) use acl_check;

#[cfg(feature = "expensive-slice-access-control")]
mod inner {
    use crate::fault::fault_point;
    use crate::io_core::{HEADER_SIZE, commit_shrink, repeat_fill, set_in_place};
    use crate::{
        AccessOp, BStack, BStackAccess, BStackAccessAuthorities, check_offset_unlocked, checked_end,
    };
    use std::io::{self, Seek, SeekFrom};
    use std::sync::atomic::Ordering;

    #[cfg(unix)]
    use crate::io_core::pread_exact_raw;
    #[cfg(windows)]
    use crate::io_core::pread_exact_raw_handle;
    #[cfg(any(unix, windows))]
    use crate::io_core::{pread_exact, pread_exact_into};
    #[cfg(not(any(unix, windows)))]
    use std::io::Read;

    #[cfg(feature = "atomic")]
    use crate::io_core::{
        durable_sync, is_atomic_write, journaled_copy, journaled_exchange, journaled_move, read_at,
        write_at,
    };

    /// A one-shot capability token authorizing guard-level access to a stack's
    /// protected ranges.
    ///
    /// Minted at most once per handle via [`BStack::take_protection`]; neither
    /// `Clone` nor `Copy`, so the authority cannot be duplicated. Present it to a
    /// `*_as` entry point to act on a [`Prot`](BStackAccess::Prot)/
    /// [`RwProt`](BStackAccess::RwProt) range or to re-arm a range this token
    /// governs. Incomparable with [`BStackAllocAuthority`]: neither reaches the
    /// other's private ranges.
    pub struct BStackProtection<'a> {
        stack: &'a BStack,
    }

    /// A one-shot capability token authorizing allocator-level access to a stack's
    /// [`Alloc`](BStackAccess::Alloc) ranges.
    ///
    /// Minted at most once per handle via [`BStack::take_alloc_authority`]; neither
    /// `Clone` nor `Copy`. Incomparable with [`BStackProtection`].
    pub struct BStackAllocAuthority<'a> {
        stack: &'a BStack,
    }

    /// A presented access token, resolved to the authorities it carries for the
    /// stack it was minted from. Implemented for references to the two token types
    /// and for `()` (no token).
    pub trait BStackAuthority {
        /// The authorities this token grants when acting on `stack`. A token minted
        /// from a *different* stack grants nothing.
        fn authorities_for(&self, stack: &BStack) -> BStackAccessAuthorities;
    }

    impl BStackAuthority for () {
        #[inline]
        fn authorities_for(&self, _stack: &BStack) -> BStackAccessAuthorities {
            BStackAccessAuthorities::NONE
        }
    }

    // Resolved authorities carry themselves — the form a slice stores after a
    // token grant, so its I/O can present the authority it was given.
    impl BStackAuthority for BStackAccessAuthorities {
        #[inline]
        fn authorities_for(&self, _stack: &BStack) -> BStackAccessAuthorities {
            *self
        }
    }

    impl BStackAuthority for &BStackProtection<'_> {
        #[inline]
        fn authorities_for(&self, stack: &BStack) -> BStackAccessAuthorities {
            // `BStack: Eq` is pointer identity, so this rejects a token minted
            // from any other stack.
            if self.stack == stack {
                BStackAccessAuthorities::GUARD
            } else {
                BStackAccessAuthorities::NONE
            }
        }
    }

    impl BStackAuthority for &BStackAllocAuthority<'_> {
        #[inline]
        fn authorities_for(&self, stack: &BStack) -> BStackAccessAuthorities {
            if self.stack == stack {
                BStackAccessAuthorities::ALLOC
            } else {
                BStackAccessAuthorities::NONE
            }
        }
    }

    impl BStack {
        /// Mint the guard capability [token](BStackProtection) for this handle, or
        /// `None` if it has already been taken. One-shot minting is what makes the
        /// token mean anything.
        pub fn take_protection(&self) -> Option<BStackProtection<'_>> {
            if self.protection_taken.swap(true, Ordering::AcqRel) {
                None
            } else {
                Some(BStackProtection { stack: self })
            }
        }

        /// Mint the allocator capability [token](BStackAllocAuthority) for this
        /// handle, or `None` if it has already been taken.
        pub fn take_alloc_authority(&self) -> Option<BStackAllocAuthority<'_>> {
            if self.alloc_authority_taken.swap(true, Ordering::AcqRel) {
                None
            } else {
                Some(BStackAllocAuthority { stack: self })
            }
        }

        /// Check that `[a, b)` permits `op` under the authorities `held`, returning
        /// [`PermissionDenied`](io::ErrorKind::PermissionDenied) otherwise. A relaxed
        /// load short-circuits the whole thing on a stack that has never been
        /// protected. The `acl` lock is separate from the stack lock, so this works
        /// on the lock-free read fast path too.
        pub(crate) fn acl_check(
            &self,
            a: u64,
            b: u64,
            op: AccessOp,
            held: BStackAccessAuthorities,
        ) -> io::Result<()> {
            if !self.acl_active.load(Ordering::Acquire) {
                return Ok(());
            }
            let table = self.acl.read().unwrap();
            if table.check(a, b, op, held) {
                Ok(())
            } else {
                Err(io_error!(
                    PermissionDenied,
                    format!("{op:?} on [{a}, {b}) denied by access control")
                ))
            }
        }

        /// Arm `[offset, offset + len)` with `mode`, tokenless.
        ///
        /// Crate-internal: all public protection goes through
        /// [`BStackOwnedSlice::protect`](crate::BStackOwnedSlice::protect), which
        /// bounds the range to a genuine allocation rather than an arbitrary span.
        ///
        /// A tokenless caller may only tighten a range that is currently
        /// [`All`](BStackAccess::All) everywhere; arming a range already governed by
        /// a token is [`PermissionDenied`](io::ErrorKind::PermissionDenied). Present a
        /// token via `protect_as` to re-mode a range you own. Nothing is persisted,
        /// so reopening clears the policy.
        ///
        /// # Errors
        ///
        /// [`InvalidInput`](io::ErrorKind::InvalidInput) if `offset + len` overflows;
        /// [`PermissionDenied`](io::ErrorKind::PermissionDenied) if the range is not
        /// entirely unprotected.
        pub(crate) fn protect(&self, offset: u64, len: u64, mode: BStackAccess) -> io::Result<()> {
            self.protect_as((), offset, len, mode)
        }

        /// Arm `[offset, offset + len)` with `mode`, presenting `auth`. Crate-internal;
        /// see [`protect`](Self::protect).
        ///
        /// The token sibling of `protect`: a caller may re-mode any range whose
        /// current mode its token can already write (which, by the incomparability
        /// of the two tokens, keeps a guard out of [`Alloc`](BStackAccess::Alloc)
        /// ranges and an allocator out of [`Prot`](BStackAccess::Prot) ranges). A
        /// tokenless `auth` of `()` falls back to the tighten-`All`-only rule.
        ///
        /// # Errors
        ///
        /// [`InvalidInput`](io::ErrorKind::InvalidInput) if `offset + len` overflows;
        /// [`PermissionDenied`](io::ErrorKind::PermissionDenied) if the current policy
        /// does not admit the token over the whole range.
        pub(crate) fn protect_as(
            &self,
            auth: impl BStackAuthority,
            offset: u64,
            len: u64,
            mode: BStackAccess,
        ) -> io::Result<()> {
            if len == 0 {
                return Ok(());
            }
            let end = checked_end(offset, len, "protect: offset + len overflows u64")?;
            let held = auth.authorities_for(self);
            // Stack lock first, then the acl lock, so in-flight writers drain before
            // the new policy is published (the ordering of `lock_up_to`).
            let _guard = self.write_lock()?;
            let mut table = self.acl.write().unwrap();
            let admitted = if held == BStackAccessAuthorities::NONE {
                table.all_over(offset, end)
            } else {
                table.check(offset, end, AccessOp::Write, held)
            };
            if !admitted {
                return Err(io_error!(
                    PermissionDenied,
                    format!("protect: [{offset}, {end}) not admitted by current policy")
                ));
            }
            table.set(offset, end, mode);
            // Publish before releasing the acl lock so a later relaxed load that sees
            // any of these points also sees `acl_active`.
            self.acl_active.store(true, Ordering::Release);
            Ok(())
        }

        /// The mode currently governing logical `offset` (for inspection/testing).
        #[must_use]
        pub fn access_at(&self, offset: u64) -> BStackAccess {
            if !self.acl_active.load(Ordering::Acquire) {
                return BStackAccess::All;
            }
            self.acl.read().unwrap().mode_at(offset)
        }

        /// [`set`](BStack::set) presenting an access token: writing a
        /// [`Prot`](BStackAccess::Prot)/[`Alloc`](BStackAccess::Alloc) range requires
        /// the matching capability, checked against the range's mode before any I/O.
        ///
        /// The body mirrors [`set`](BStack::set) with the token check spliced in after
        /// the locked-prefix check.
        ///
        /// # Errors
        ///
        /// [`PermissionDenied`](io::ErrorKind::PermissionDenied) if the range's access
        /// mode denies the write under `auth`, plus every error [`set`](BStack::set)
        /// itself can return.
        pub fn set_as(
            &self,
            auth: impl BStackAuthority,
            offset: u64,
            data: impl AsRef<[u8]>,
        ) -> io::Result<()> {
            let data = data.as_ref();
            if data.is_empty() {
                return Ok(());
            }
            let held = auth.authorities_for(self);
            let end = checked_end(offset, data.len() as u64, "set: offset + len overflows u64")?;
            let mut guard = self.write_lock()?;
            let (file, _, replay) = &mut *guard;
            let locked = self.locked.load(Ordering::Acquire);
            check_offset_unlocked("set", offset, end, locked)?;
            acl_check!(self, offset, end, Write, held);
            let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
            if end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!("set: write end ({end}) exceeds payload size ({data_size})")
                ));
            }
            fault_point!(self, "set");
            Self::mark_replay(replay, set_in_place(file, data_size, offset, data))
        }

        /// [`discard`](BStack::discard) presenting an access token: truncating a range
        /// whose mode restricts the truncate axis requires the matching capability,
        /// checked over the discarded tail `[new_len, old_len)` before any I/O.
        ///
        /// # Errors
        ///
        /// [`PermissionDenied`](io::ErrorKind::PermissionDenied) if the tail's access
        /// mode denies truncation under `auth`, plus every error
        /// [`discard`](BStack::discard) itself can return.
        pub fn discard_as(&self, auth: impl BStackAuthority, n: u64) -> io::Result<()> {
            if n == 0 {
                return Ok(());
            }
            let held = auth.authorities_for(self);
            let mut guard = self.write_lock()?;
            let (file, clen, replay) = &mut *guard;
            let raw_size = file.seek(SeekFrom::End(0))?;
            let data_size = raw_size - HEADER_SIZE;
            if n > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!("discard({n}) exceeds payload size ({data_size})")
                ));
            }
            let new_data_len = data_size - n;
            let locked = self.locked.load(Ordering::Acquire);
            if new_data_len < locked {
                return Err(io_error!(
                    InvalidInput,
                    format!("discard({n}) would shrink payload below locked length ({locked})")
                ));
            }
            acl_check!(self, new_data_len, data_size, Truncate, held);
            fault_point!(self, "discard");
            Self::mark_replay(replay, commit_shrink(file, clen, new_data_len))?;
            Ok(())
        }

        /// [`get`](BStack::get) presenting an access token: reading a
        /// [`Prot`](BStackAccess::Prot)/[`Alloc`](BStackAccess::Alloc) range (or any
        /// range that denies tokenless reads) requires the matching capability,
        /// checked over `[start, end)` before any I/O — ahead of the locked-region
        /// fast path, so a [`Locked`](BStackAccess::Locked) range is still denied.
        ///
        /// # Errors
        ///
        /// [`PermissionDenied`](io::ErrorKind::PermissionDenied) if the range's access
        /// mode denies the read under `auth`, plus every error [`get`](BStack::get)
        /// itself can return.
        pub fn get_as(
            &self,
            auth: impl BStackAuthority,
            start: u64,
            end: u64,
        ) -> io::Result<Vec<u8>> {
            acl_check!(self, start, end, Read, auth.authorities_for(self));
            if end < start {
                return Err(io_error!(
                    InvalidInput,
                    format!("get: end ({end}) < start ({start})")
                ));
            }
            // Fast-path: if the range lies entirely within the locked region, serve
            // from the in-memory cache (if enabled) or a lock-free pread.
            #[cfg(any(unix, windows))]
            {
                let locked = self.locked.load(Ordering::Acquire);
                if end <= locked {
                    if self.cache_enabled {
                        let len = (end - start) as usize;
                        let mut buf = vec![0u8; len];
                        let cache = self.cache.lock().unwrap();
                        buf.copy_from_slice(&cache[start as usize..end as usize]);
                        return Ok(buf);
                    }
                    #[cfg(unix)]
                    {
                        let mut buf = vec![0u8; (end - start) as usize];
                        pread_exact_raw(self.fd, HEADER_SIZE + start, &mut buf)?;
                        return Ok(buf);
                    }
                    #[cfg(windows)]
                    {
                        let mut buf = vec![0u8; (end - start) as usize];
                        pread_exact_raw_handle(self.handle, HEADER_SIZE + start, &mut buf)?;
                        return Ok(buf);
                    }
                }
            }
            #[cfg(any(unix, windows))]
            {
                let guard = self.read_lock()?;
                let file = &guard.0;
                let data_size = file.metadata()?.len().saturating_sub(HEADER_SIZE);
                if end > data_size {
                    return Err(io_error!(
                        InvalidInput,
                        format!("get: end ({end}) exceeds payload size ({data_size})")
                    ));
                }
                fault_point!(self, "get");
                pread_exact(file, HEADER_SIZE + start, (end - start) as usize)
            }
            #[cfg(not(any(unix, windows)))]
            {
                let locked = self.locked.load(Ordering::Acquire);
                if end <= locked && self.cache_enabled {
                    let cache = self.cache.lock().unwrap();
                    return Ok(cache[start as usize..end as usize].to_vec());
                }
                let mut guard = self.write_lock_read()?;
                let file = &mut guard.0;
                let raw_size = file.seek(SeekFrom::End(0))?;
                let data_size = raw_size.saturating_sub(HEADER_SIZE);
                if end > data_size {
                    return Err(io_error!(
                        InvalidInput,
                        format!("get: end ({end}) exceeds payload size ({data_size})")
                    ));
                }
                fault_point!(self, "get");
                file.seek(SeekFrom::Start(HEADER_SIZE + start))?;
                let mut buf = vec![0u8; (end - start) as usize];
                file.read_exact(&mut buf)?;
                Ok(buf)
            }
        }

        /// [`get_into`](BStack::get_into) presenting an access token, mirroring the
        /// tokenless body but checking with `auth`.
        pub fn get_into_as(
            &self,
            auth: impl BStackAuthority,
            start: u64,
            buf: &mut [u8],
        ) -> io::Result<()> {
            if buf.is_empty() {
                return Ok(());
            }
            let len = buf.len() as u64;
            let end = start
                .checked_add(len)
                .ok_or_else(|| io_error!(InvalidInput, "get_into: start + len overflows u64"))?;
            acl_check!(self, start, end, Read, auth.authorities_for(self));
            #[cfg(any(unix, windows))]
            {
                let locked = self.locked.load(Ordering::Acquire);
                if end <= locked {
                    if self.cache_enabled {
                        let cache = self.cache.lock().unwrap();
                        buf.copy_from_slice(&cache[start as usize..end as usize]);
                        return Ok(());
                    }
                    #[cfg(unix)]
                    return pread_exact_raw(self.fd, HEADER_SIZE + start, buf);
                    #[cfg(windows)]
                    return pread_exact_raw_handle(self.handle, HEADER_SIZE + start, buf);
                }
            }
            #[cfg(any(unix, windows))]
            {
                let guard = self.read_lock()?;
                let file = &guard.0;
                let data_size = file.metadata()?.len().saturating_sub(HEADER_SIZE);
                if end > data_size {
                    return Err(io_error!(
                        InvalidInput,
                        format!("get_into: end ({end}) exceeds payload size ({data_size})")
                    ));
                }
                fault_point!(self, "get_into");
                pread_exact_into(file, HEADER_SIZE + start, buf)
            }
            #[cfg(not(any(unix, windows)))]
            {
                let locked = self.locked.load(Ordering::Acquire);
                if end <= locked && self.cache_enabled {
                    let cache = self.cache.lock().unwrap();
                    buf.copy_from_slice(&cache[start as usize..end as usize]);
                    return Ok(());
                }
                let mut guard = self.write_lock_read()?;
                let file = &mut guard.0;
                let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
                if end > data_size {
                    return Err(io_error!(
                        InvalidInput,
                        format!("get_into: end ({end}) exceeds payload size ({data_size})")
                    ));
                }
                fault_point!(self, "get_into");
                file.seek(SeekFrom::Start(HEADER_SIZE + start))?;
                file.read_exact(buf)
            }
        }

        /// [`zero`](BStack::zero) presenting an access token.
        pub fn zero_as(&self, auth: impl BStackAuthority, offset: u64, n: u64) -> io::Result<()> {
            if n == 0 {
                return Ok(());
            }
            let held = auth.authorities_for(self);
            let end = checked_end(offset, n, "zero: offset + n overflows u64")?;
            let mut guard = self.write_lock()?;
            let (file, _, replay) = &mut *guard;
            let locked = self.locked.load(Ordering::Acquire);
            check_offset_unlocked("zero", offset, end, locked)?;
            acl_check!(self, offset, end, Write, held);
            let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
            if end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!("zero: write end ({end}) exceeds payload size ({data_size})")
                ));
            }
            fault_point!(self, "zero");
            Self::mark_replay(replay, repeat_fill(file, data_size, offset, &[0u8], n))
        }

        /// [`repeat`](BStack::repeat) presenting an access token.
        pub fn repeat_as(
            &self,
            auth: impl BStackAuthority,
            offset: u64,
            pattern: impl AsRef<[u8]>,
            count: u64,
        ) -> io::Result<()> {
            let pattern = pattern.as_ref();
            if pattern.is_empty() || count == 0 {
                return Ok(());
            }
            let held = auth.authorities_for(self);
            let total = (pattern.len() as u64).checked_mul(count).ok_or_else(|| {
                io_error!(InvalidInput, "repeat: count * pattern.len() overflows u64")
            })?;
            let end = checked_end(
                offset,
                total,
                "repeat: offset + count*pattern.len() overflows u64",
            )?;
            let mut guard = self.write_lock()?;
            let (file, _, replay) = &mut *guard;
            let locked = self.locked.load(Ordering::Acquire);
            check_offset_unlocked("repeat", offset, end, locked)?;
            acl_check!(self, offset, end, Write, held);
            let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
            if end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!("repeat: write end ({end}) exceeds payload size ({data_size})")
                ));
            }
            fault_point!(self, "repeat");
            Self::mark_replay(replay, repeat_fill(file, data_size, offset, pattern, count))
        }

        /// [`cas`](BStack::cas) presenting an access token.
        #[cfg(feature = "atomic")]
        pub fn cas_as(
            &self,
            auth: impl BStackAuthority,
            offset: u64,
            old: impl AsRef<[u8]>,
            new: impl AsRef<[u8]>,
        ) -> io::Result<bool> {
            let old = old.as_ref();
            let new = new.as_ref();
            if old.len() != new.len() {
                return Ok(false);
            }
            if old.is_empty() {
                return Ok(true);
            }
            let held = auth.authorities_for(self);
            let end = checked_end(offset, old.len() as u64, "cas: offset + len overflows u64")?;
            let mut guard = self.write_lock()?;
            let (file, _, replay) = &mut *guard;
            let locked = self.locked.load(Ordering::Acquire);
            check_offset_unlocked("cas", offset, end, locked)?;
            let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
            if end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!("cas: range [{offset}, {end}) exceeds payload size ({data_size})")
                ));
            }
            acl_check!(self, offset, end, Write, held);
            fault_point!(self, "cas");
            let mut current = vec![0u8; old.len()];
            read_at(file, offset, &mut current)?;
            if current != old {
                return Ok(false);
            }
            Self::mark_replay(replay, set_in_place(file, data_size, offset, new))?;
            Ok(true)
        }

        /// [`cross_exchange`](BStack::cross_exchange) presenting an access token.
        #[cfg(feature = "atomic")]
        pub fn cross_exchange_as(
            &self,
            auth: impl BStackAuthority,
            a: u64,
            b: u64,
            n: u64,
        ) -> io::Result<()> {
            let held = auth.authorities_for(self);
            let a_end = checked_end(a, n, "cross_exchange: a + n overflows u64")?;
            let b_end = checked_end(b, n, "cross_exchange: b + n overflows u64")?;
            if n > 0 {
                let (lo, hi) = if a < b { (a, b) } else { (b, a) };
                if lo + n > hi {
                    return Err(io_error!(
                        InvalidInput,
                        format!(
                            "cross_exchange: regions [{a}, {a_end}) and [{b}, {b_end}) overlap"
                        )
                    ));
                }
            }
            let mut guard = self.write_lock()?;
            let (file, _, replay) = &mut *guard;
            let locked = self.locked.load(Ordering::Acquire);
            if a < locked {
                return Err(io_error!(
                    InvalidInput,
                    format!(
                        "cross_exchange: region [{a}, {a_end}) overlaps locked region [0, {locked})"
                    )
                ));
            }
            if b < locked {
                return Err(io_error!(
                    InvalidInput,
                    format!(
                        "cross_exchange: region [{b}, {b_end}) overlaps locked region [0, {locked})"
                    )
                ));
            }
            let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
            if a_end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!(
                        "cross_exchange: region [{a}, {a_end}) exceeds payload size ({data_size})"
                    )
                ));
            }
            if b_end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!(
                        "cross_exchange: region [{b}, {b_end}) exceeds payload size ({data_size})"
                    )
                ));
            }
            if n == 0 {
                return Ok(());
            }
            acl_check!(self, a, a_end, Write, held);
            acl_check!(self, b, b_end, Write, held);
            fault_point!(self, "cross_exchange");
            Self::mark_replay(replay, journaled_exchange(file, data_size, a, b, n))
        }

        /// [`copy`](BStack::copy) presenting an access token.
        #[cfg(feature = "atomic")]
        pub fn copy_as(
            &self,
            auth: impl BStackAuthority,
            from: u64,
            to: u64,
            n: u64,
        ) -> io::Result<()> {
            let held = auth.authorities_for(self);
            let from_end = checked_end(from, n, "copy: from + n overflows u64")?;
            let to_end = checked_end(to, n, "copy: to + n overflows u64")?;
            let mut guard = self.write_lock()?;
            let (file, _, replay) = &mut *guard;
            let locked = self.locked.load(Ordering::Acquire);
            if to < locked {
                return Err(io_error!(
                    InvalidInput,
                    format!(
                        "copy: destination [{to}, {to_end}) overlaps locked region [0, {locked})"
                    )
                ));
            }
            let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
            if from_end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!("copy: source [{from}, {from_end}) exceeds payload size ({data_size})")
                ));
            }
            if to_end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!(
                        "copy: destination [{to}, {to_end}) exceeds payload size ({data_size})"
                    )
                ));
            }
            if n == 0 {
                return Ok(());
            }
            if from == to {
                return Ok(());
            }
            acl_check!(self, from, from_end, Read, held);
            acl_check!(self, to, to_end, Write, held);
            fault_point!(self, "copy");
            if is_atomic_write(to, n) {
                let mut buf = vec![0u8; n as usize];
                read_at(file, from, &mut buf)?;
                Self::mark_replay(replay, write_at(file, to, &buf))?;
                Self::mark_replay(replay, durable_sync(file))
            } else if from < to_end && to < from_end {
                Self::mark_replay(replay, journaled_move(file, data_size, from, to, n))
            } else {
                Self::mark_replay(replay, journaled_copy(file, data_size, from, to, n))
            }
        }

        /// [`process`](BStack::process) presenting an access token.
        #[cfg(feature = "atomic")]
        pub fn process_as<F>(
            &self,
            auth: impl BStackAuthority,
            start: u64,
            end: u64,
            f: F,
        ) -> io::Result<()>
        where
            F: FnOnce(&mut [u8]),
        {
            if end < start {
                return Err(io_error!(
                    InvalidInput,
                    format!("process: end ({end}) < start ({start})")
                ));
            }
            let held = auth.authorities_for(self);
            let n = end - start;
            let mut guard = self.write_lock()?;
            let (file, _, replay) = &mut *guard;
            let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
            if end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!("process: end ({end}) exceeds payload size ({data_size})")
                ));
            }
            let locked = self.locked.load(Ordering::Acquire);
            if start < locked {
                return Err(io_error!(
                    InvalidInput,
                    format!("process: range [{start}, {end}) overlaps locked region [0, {locked})")
                ));
            }
            acl_check!(self, start, end, Write, held);
            fault_point!(self, "process");
            let mut buf = vec![0u8; n as usize];
            if n > 0 {
                read_at(file, start, &mut buf)?;
            }
            f(&mut buf);
            if n > 0 {
                Self::mark_replay(replay, set_in_place(file, data_size, start, &buf))?;
            }
            Ok(())
        }

        /// [`eq_crds`](BStack::eq_crds) presenting an access token.
        #[cfg(feature = "atomic")]
        pub fn eq_crds_as(
            &self,
            auth: impl BStackAuthority,
            a_offset: u64,
            a_expected: impl AsRef<[u8]>,
            b_offset: u64,
            b_buf: impl AsRef<[u8]>,
        ) -> io::Result<Option<Vec<u8>>> {
            let a_expected = a_expected.as_ref();
            let b_buf = b_buf.as_ref();
            let held = auth.authorities_for(self);
            let a_end = checked_end(
                a_offset,
                a_expected.len() as u64,
                "eq_crds: a_offset + a_len overflows u64",
            )?;
            let b_end = checked_end(
                b_offset,
                b_buf.len() as u64,
                "eq_crds: b_offset + b_len overflows u64",
            )?;
            let mut guard = self.write_lock()?;
            let (file, _, replay) = &mut *guard;
            let locked = self.locked.load(Ordering::Acquire);
            if !b_buf.is_empty() && b_offset < locked {
                return Err(io_error!(
                    InvalidInput,
                    format!(
                        "eq_crds: B range [{b_offset}, {b_end}) overlaps locked region [0, {locked})"
                    )
                ));
            }
            let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
            if !a_expected.is_empty() && a_end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!(
                        "eq_crds: A range [{a_offset}, {a_end}) exceeds payload size ({data_size})"
                    )
                ));
            }
            if !b_buf.is_empty() && b_end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!(
                        "eq_crds: B range [{b_offset}, {b_end}) exceeds payload size ({data_size})"
                    )
                ));
            }
            acl_check!(self, a_offset, a_end, Read, held);
            acl_check!(self, b_offset, b_end, Write, held);
            fault_point!(self, "eq_crds");
            let mut a_current = vec![0u8; a_expected.len()];
            if !a_expected.is_empty() {
                read_at(file, a_offset, &mut a_current)?;
            }
            if a_current != a_expected {
                return Ok(None);
            }
            if b_buf.is_empty() {
                return Ok(Some(Vec::new()));
            }
            let mut old_b = vec![0u8; b_buf.len()];
            read_at(file, b_offset, &mut old_b)?;
            Self::mark_replay(replay, set_in_place(file, data_size, b_offset, b_buf))?;
            Ok(Some(old_b))
        }

        /// [`ne_crds`](BStack::ne_crds) presenting an access token.
        #[cfg(feature = "atomic")]
        pub fn ne_crds_as(
            &self,
            auth: impl BStackAuthority,
            a_offset: u64,
            a_expected: impl AsRef<[u8]>,
            b_offset: u64,
            b_buf: impl AsRef<[u8]>,
        ) -> io::Result<Option<Vec<u8>>> {
            let a_expected = a_expected.as_ref();
            let b_buf = b_buf.as_ref();
            let held = auth.authorities_for(self);
            let a_end = checked_end(
                a_offset,
                a_expected.len() as u64,
                "ne_crds: a_offset + a_len overflows u64",
            )?;
            let b_end = checked_end(
                b_offset,
                b_buf.len() as u64,
                "ne_crds: b_offset + b_len overflows u64",
            )?;
            let mut guard = self.write_lock()?;
            let (file, _, replay) = &mut *guard;
            let locked = self.locked.load(Ordering::Acquire);
            if !b_buf.is_empty() && b_offset < locked {
                return Err(io_error!(
                    InvalidInput,
                    format!(
                        "ne_crds: B range [{b_offset}, {b_end}) overlaps locked region [0, {locked})"
                    )
                ));
            }
            let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
            if !a_expected.is_empty() && a_end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!(
                        "ne_crds: A range [{a_offset}, {a_end}) exceeds payload size ({data_size})"
                    )
                ));
            }
            if !b_buf.is_empty() && b_end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!(
                        "ne_crds: B range [{b_offset}, {b_end}) exceeds payload size ({data_size})"
                    )
                ));
            }
            acl_check!(self, a_offset, a_end, Read, held);
            acl_check!(self, b_offset, b_end, Write, held);
            fault_point!(self, "ne_crds");
            let mut a_current = vec![0u8; a_expected.len()];
            if !a_expected.is_empty() {
                read_at(file, a_offset, &mut a_current)?;
            }
            if a_current == a_expected {
                return Ok(None);
            }
            if b_buf.is_empty() {
                return Ok(Some(Vec::new()));
            }
            let mut old_b = vec![0u8; b_buf.len()];
            read_at(file, b_offset, &mut old_b)?;
            Self::mark_replay(replay, set_in_place(file, data_size, b_offset, b_buf))?;
            Ok(Some(old_b))
        }

        /// [`masked_eq_crds`](BStack::masked_eq_crds) presenting an access token.
        #[cfg(feature = "atomic")]
        pub fn masked_eq_crds_as(
            &self,
            auth: impl BStackAuthority,
            a_offset: u64,
            mask: impl AsRef<[u8]>,
            a_expected: impl AsRef<[u8]>,
            b_offset: u64,
            b_buf: impl AsRef<[u8]>,
        ) -> io::Result<Option<Vec<u8>>> {
            let mask = mask.as_ref();
            let a_expected = a_expected.as_ref();
            let b_buf = b_buf.as_ref();
            if mask.len() != a_expected.len() {
                return Err(io_error!(
                    InvalidInput,
                    "masked_eq_crds: mask length ({}) != a_expected length ({})",
                    mask.len(),
                    a_expected.len()
                ));
            }
            let held = auth.authorities_for(self);
            let a_end = checked_end(
                a_offset,
                a_expected.len() as u64,
                "masked_eq_crds: a_offset + a_len overflows u64",
            )?;
            let b_end = checked_end(
                b_offset,
                b_buf.len() as u64,
                "masked_eq_crds: b_offset + b_len overflows u64",
            )?;
            let mut guard = self.write_lock()?;
            let (file, _, replay) = &mut *guard;
            let locked = self.locked.load(Ordering::Acquire);
            if !b_buf.is_empty() && b_offset < locked {
                return Err(io_error!(
                    InvalidInput,
                    format!(
                        "masked_eq_crds: B range [{b_offset}, {b_end}) overlaps locked region [0, {locked})"
                    )
                ));
            }
            let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
            if !a_expected.is_empty() && a_end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!(
                        "masked_eq_crds: A range [{a_offset}, {a_end}) exceeds payload size ({data_size})"
                    )
                ));
            }
            if !b_buf.is_empty() && b_end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!(
                        "masked_eq_crds: B range [{b_offset}, {b_end}) exceeds payload size ({data_size})"
                    )
                ));
            }
            acl_check!(self, a_offset, a_end, Read, held);
            acl_check!(self, b_offset, b_end, Write, held);
            fault_point!(self, "masked_eq_crds");
            let mut a_current = vec![0u8; a_expected.len()];
            if !a_expected.is_empty() {
                read_at(file, a_offset, &mut a_current)?;
            }
            let masked_match = a_current
                .iter()
                .zip(mask.iter())
                .zip(a_expected.iter())
                .all(|((&a, &m), &e)| (a & m) == (e & m));
            if !masked_match {
                return Ok(None);
            }
            if b_buf.is_empty() {
                return Ok(Some(Vec::new()));
            }
            let mut old_b = vec![0u8; b_buf.len()];
            read_at(file, b_offset, &mut old_b)?;
            Self::mark_replay(replay, set_in_place(file, data_size, b_offset, b_buf))?;
            Ok(Some(old_b))
        }
    }
}

#[cfg(feature = "expensive-slice-access-control")]
pub use inner::*;
