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
    use crate::io_core::{HEADER_SIZE, commit_shrink, set_in_place};
    use crate::{
        AccessOp, BStack, BStackAccess, BStackAccessAuthorities, check_offset_unlocked, checked_end,
    };
    use std::io::{self, Seek, SeekFrom};
    use std::sync::atomic::Ordering;

    #[cfg(any(unix, windows))]
    use crate::io_core::pread_exact;
    #[cfg(unix)]
    use crate::io_core::pread_exact_raw;
    #[cfg(windows)]
    use crate::io_core::pread_exact_raw_handle;
    #[cfg(not(any(unix, windows)))]
    use std::io::Read;

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
        /// A tokenless caller may only tighten a range that is currently
        /// [`All`](BStackAccess::All) everywhere; arming a range already governed by
        /// a token is [`PermissionDenied`](io::ErrorKind::PermissionDenied). Present a
        /// token via [`protect_as`](BStack::protect_as) to re-mode a range you own.
        /// Nothing is persisted, so reopening clears the policy.
        ///
        /// # Errors
        ///
        /// [`InvalidInput`](io::ErrorKind::InvalidInput) if `offset + len` overflows;
        /// [`PermissionDenied`](io::ErrorKind::PermissionDenied) if the range is not
        /// entirely unprotected.
        pub fn protect(&self, offset: u64, len: u64, mode: BStackAccess) -> io::Result<()> {
            self.protect_as((), offset, len, mode)
        }

        /// Arm `[offset, offset + len)` with `mode`, presenting `auth`.
        ///
        /// The token sibling of [`protect`](BStack::protect): a caller may re-mode any
        /// range whose current mode its token can already write (which, by the
        /// incomparability of the two tokens, keeps a guard out of
        /// [`Alloc`](BStackAccess::Alloc) ranges and an allocator out of
        /// [`Prot`](BStackAccess::Prot) ranges). A tokenless `auth` of `()` falls back
        /// to the tighten-`All`-only rule.
        ///
        /// # Errors
        ///
        /// [`InvalidInput`](io::ErrorKind::InvalidInput) if `offset + len` overflows;
        /// [`PermissionDenied`](io::ErrorKind::PermissionDenied) if the current policy
        /// does not admit the token over the whole range.
        pub fn protect_as(
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
    }
}

#[cfg(feature = "expensive-slice-access-control")]
pub use inner::*;
