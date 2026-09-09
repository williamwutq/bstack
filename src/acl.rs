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

/// Define a non-generic allocator-metadata I/O helper on `BStack`. With the
/// `expensive-slice-access-control` feature it presents synthetic
/// [`ALLOC`](crate::BStackAccessAuthorities::ALLOC) authority to the
/// token-carrying `_as` op (the header stays permanently `Alloc`-marked, so the
/// allocator's own write is admitted); without the feature it is the plain op.
/// One definition serves both builds, so there is no separate shim. The
/// generator/`set_batched` helpers are generic and written out by hand.
#[cfg(all(feature = "alloc", feature = "set"))]
macro_rules! meta_dispatch {
    (
        $(#[$attr:meta])*
        [$op:ident / $op_as:ident]
        fn $name:ident ( $($arg:ident : $ty:ty),* $(,)? ) -> $ret:ty
    ) => {
        $(#[$attr])*
        #[inline]
        pub(crate) fn $name(&self, $($arg: $ty),*) -> $ret {
            #[cfg(feature = "expensive-slice-access-control")]
            {
                self.$op_as(crate::BStackAccessAuthorities::ALLOC, $($arg),*)
            }
            #[cfg(not(feature = "expensive-slice-access-control"))]
            {
                self.$op($($arg),*)
            }
        }
    };
}

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
        durable_sync, is_atomic_write, journaled_copy, journaled_exchange, journaled_move, write_at,
    };
    // Frontier/tail commit helpers shared by the append/shrink/read `_as` siblings;
    // these mirror non-atomic `BStack` methods, so they cannot be `atomic`-gated.
    #[cfg(feature = "atomic")]
    use crate::BStackGenOp;
    #[cfg(feature = "atomic")]
    use crate::io_core::commit_tail_replace;
    use crate::io_core::{commit_grow, commit_sparse_extend, read_at};
    use crate::validate_sparse_blocks;
    use std::io::Write;
    // Helpers for the duplicated `inplace_gen_as` engine.
    #[cfg(feature = "atomic")]
    use crate::fault::fault_probe;
    #[cfg(feature = "atomic")]
    use crate::io_core::{
        OverlayData, inplace_overlay_insert, inplace_overlay_read, inplace_validate_read,
        inplace_validate_repeat, inplace_validate_write, journaled_multi_overlay,
        journaled_multi_set,
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
        #[inline]
        pub fn take_protection(&self) -> Option<BStackProtection<'_>> {
            if self.protection_taken.swap(true, Ordering::AcqRel) {
                None
            } else {
                Some(BStackProtection { stack: self })
            }
        }

        /// Mint the allocator capability [token](BStackAllocAuthority) for this
        /// handle, or `None` if it has already been taken.
        #[inline]
        pub fn take_alloc_authority(&self) -> Option<BStackAllocAuthority<'_>> {
            if self.alloc_authority_taken.swap(true, Ordering::AcqRel) {
                None
            } else {
                Some(BStackAllocAuthority { stack: self })
            }
        }

        /// Check that `[a, b)` permits `op` under the authorities `held`, returning
        /// [`PermissionDenied`](io::ErrorKind::PermissionDenied) otherwise. An
        /// unprotected stack has an empty table, so the check is a cheap miss. The
        /// `acl` lock is separate from the stack lock, so this works
        /// on the lock-free read fast path too.
        pub(crate) fn acl_check(
            &self,
            a: u64,
            b: u64,
            op: AccessOp,
            held: BStackAccessAuthorities,
        ) -> io::Result<()> {
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

        /// [`process_gen`](BStack::process_gen) run under an access token: every
        /// per-op check is evaluated as if `auth` were presented, so an allocator
        /// can drive its own [`Alloc`](BStackAccess)-marked metadata (the free-list
        /// head, say) through a journalled sequence without ever lifting the mark —
        /// leaving no window in which a concurrent op could see it unprotected.
        ///
        /// Present [`ALLOC`](BStackAccessAuthorities::ALLOC) or an alloc token. The
        /// body duplicates `process_gen` (kept in `lib.rs` as the tokenless engine)
        /// rather than threading authority through the crash-atomic core; the two
        /// must stay in sync.
        #[cfg(feature = "atomic")]
        pub fn process_gen_as<'a, A, F>(&self, auth: A, mut f: F) -> io::Result<()>
        where
            A: super::BStackAuthority,
            F: FnMut() -> Option<BStackGenOp<'a>>,
        {
            let held = auth.authorities_for(self);
            let mut guard = self.write_lock()?;
            let (file, clen, replay) = &mut *guard;
            let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
            let locked = self.locked.load(Ordering::Acquire);
            fault_point!(self, "process_gen");
            loop {
                match f() {
                    Some(BStackGenOp::Read { offset, buf }) => {
                        let end = checked_end(
                            offset,
                            buf.len() as u64,
                            "process_gen: read offset + buf.len() overflows u64",
                        )?;
                        if end > data_size {
                            return Err(io_error!(
                                InvalidInput,
                                format!(
                                    "process_gen: read range [{offset}, {end}) exceeds payload size ({data_size})"
                                )
                            ));
                        }
                        // Per-step read fault: stands in for this `Read`'s I/O, and
                        // like a genuine read failure here it ends the whole call —
                        // `process_gen` has no channel to report a step failure on.
                        // Consulted for every `Read` op, including ones the fast
                        // paths below serve without touching the disk, so the
                        // schedule does not shift with cache state.
                        fault_point!(self, "process_gen:read");
                        acl_check!(self, offset, end, Read, held);
                        // Fast path: locked bytes are immutable, so they can be
                        // served from the cache or via a lock-free pread instead
                        // of going through the held file handle — mirroring how
                        // `get_into` treats reads of the locked region.
                        #[cfg(any(unix, windows))]
                        {
                            if end <= locked {
                                if self.cache_enabled {
                                    let cache = self.cache.lock().unwrap();
                                    buf.copy_from_slice(&cache[offset as usize..end as usize]);
                                } else {
                                    #[cfg(unix)]
                                    pread_exact_raw(self.fd, HEADER_SIZE + offset, buf)?;
                                    #[cfg(windows)]
                                    pread_exact_raw_handle(self.handle, HEADER_SIZE + offset, buf)?;
                                }
                            } else {
                                pread_exact_into(file, HEADER_SIZE + offset, buf)?;
                            }
                        }
                        #[cfg(not(any(unix, windows)))]
                        {
                            if end <= locked && self.cache_enabled {
                                let cache = self.cache.lock().unwrap();
                                buf.copy_from_slice(&cache[offset as usize..end as usize]);
                            } else {
                                read_at(file, offset, buf)?;
                            }
                        }
                    }
                    Some(BStackGenOp::Write { offset, data }) => {
                        let end = checked_end(
                            offset,
                            data.len() as u64,
                            "process_gen: write offset + data.len() overflows u64",
                        )?;
                        if offset < locked {
                            return Err(io_error!(
                                InvalidInput,
                                format!(
                                    "process_gen: write range [{offset}, {end}) overlaps locked region [0, {locked})"
                                )
                            ));
                        }
                        if end > data_size {
                            return Err(io_error!(
                                InvalidInput,
                                format!(
                                    "process_gen: write range [{offset}, {end}) exceeds payload size ({data_size})"
                                )
                            ));
                        }
                        acl_check!(self, offset, end, Write, held);
                        if !data.is_empty() {
                            Self::mark_replay(replay, set_in_place(file, data_size, offset, data))?;
                        }
                        return Ok(());
                    }
                    Some(BStackGenOp::Repeat {
                        offset,
                        pattern,
                        count,
                    }) => {
                        // Empty pattern or zero count is a no-op, matching `zero(_, 0)`.
                        if pattern.is_empty() || count == 0 {
                            return Ok(());
                        }
                        let total = (pattern.len() as u64).checked_mul(count).ok_or_else(|| {
                            io_error!(InvalidInput, "process_gen: repeat length overflows u64")
                        })?;
                        let end = checked_end(
                            offset,
                            total,
                            "process_gen: repeat offset + length overflows u64",
                        )?;
                        if offset < locked {
                            return Err(io_error!(
                                InvalidInput,
                                format!(
                                    "process_gen: repeat range [{offset}, {end}) overlaps locked region [0, {locked})"
                                )
                            ));
                        }
                        if end > data_size {
                            return Err(io_error!(
                                InvalidInput,
                                format!(
                                    "process_gen: repeat range [{offset}, {end}) exceeds payload size ({data_size})"
                                )
                            ));
                        }
                        Self::mark_replay(
                            replay,
                            repeat_fill(file, data_size, offset, pattern, count),
                        )?;
                        return Ok(());
                    }
                    Some(BStackGenOp::Swap {
                        a_offset,
                        b_offset,
                        len,
                    }) => {
                        let a_end = checked_end(
                            a_offset,
                            len,
                            "process_gen: a_offset + len overflows u64",
                        )?;
                        let b_end = checked_end(
                            b_offset,
                            len,
                            "process_gen: b_offset + len overflows u64",
                        )?;
                        if len > 0 {
                            let (lo, hi) = if a_offset < b_offset {
                                (a_offset, b_offset)
                            } else {
                                (b_offset, a_offset)
                            };
                            if lo + len > hi {
                                return Err(io_error!(
                                    InvalidInput,
                                    format!(
                                        "process_gen: swap regions [{a_offset}, {a_end}) and [{b_offset}, {b_end}) overlap"
                                    )
                                ));
                            }
                        }
                        if a_offset < locked {
                            return Err(io_error!(
                                InvalidInput,
                                format!(
                                    "process_gen: swap region [{a_offset}, {a_end}) overlaps locked region [0, {locked})"
                                )
                            ));
                        }
                        if b_offset < locked {
                            return Err(io_error!(
                                InvalidInput,
                                format!(
                                    "process_gen: swap region [{b_offset}, {b_end}) overlaps locked region [0, {locked})"
                                )
                            ));
                        }
                        if a_end > data_size {
                            return Err(io_error!(
                                InvalidInput,
                                format!(
                                    "process_gen: swap region [{a_offset}, {a_end}) exceeds payload size ({data_size})"
                                )
                            ));
                        }
                        if b_end > data_size {
                            return Err(io_error!(
                                InvalidInput,
                                format!(
                                    "process_gen: swap region [{b_offset}, {b_end}) exceeds payload size ({data_size})"
                                )
                            ));
                        }
                        acl_check!(self, a_offset, a_end, Write, held);
                        acl_check!(self, b_offset, b_end, Write, held);
                        if len > 0 {
                            Self::mark_replay(
                                replay,
                                journaled_exchange(file, data_size, a_offset, b_offset, len),
                            )?;
                        }
                        return Ok(());
                    }
                    Some(BStackGenOp::Push { data }) => {
                        if !data.is_empty() {
                            let file_end = file.seek(SeekFrom::End(0))?;
                            let logical_offset = file_end - HEADER_SIZE;
                            acl_check!(
                                self,
                                logical_offset,
                                logical_offset + data.len() as u64,
                                Write,
                                held
                            );
                            if let Err(e) = file.write_all(data) {
                                // A failed rollback leaves a stale tail past the committed length:
                                // defer it to the next write's replay.
                                if file.set_len(file_end).is_err() {
                                    *replay = true;
                                }
                                return Err(e);
                            }
                            let new_len = logical_offset + data.len() as u64;
                            Self::mark_replay(
                                replay,
                                commit_grow(file, clen, new_len, logical_offset, file_end),
                            )?;
                        }
                        return Ok(());
                    }
                    Some(BStackGenOp::Pop { buf }) => {
                        let n = buf.len() as u64;
                        if n > data_size {
                            return Err(io_error!(
                                InvalidInput,
                                format!("process_gen: pop({n}) exceeds payload size ({data_size})")
                            ));
                        }
                        let new_data_len = data_size - n;
                        if new_data_len < locked {
                            return Err(io_error!(
                                InvalidInput,
                                format!(
                                    "process_gen: pop({n}) would shrink payload below locked length ({locked})"
                                )
                            ));
                        }
                        acl_check!(self, new_data_len, data_size, Truncate, held);
                        if n > 0 {
                            read_at(file, new_data_len, buf)?;
                            Self::mark_replay(replay, commit_shrink(file, clen, new_data_len))?;
                        }
                        return Ok(());
                    }
                    Some(BStackGenOp::Discard { len }) => {
                        if len > data_size {
                            return Err(io_error!(
                                InvalidInput,
                                format!(
                                    "process_gen: discard({len}) exceeds payload size ({data_size})"
                                )
                            ));
                        }
                        let new_data_len = data_size - len;
                        if new_data_len < locked {
                            return Err(io_error!(
                                InvalidInput,
                                format!(
                                    "process_gen: discard({len}) would shrink payload below locked length ({locked})"
                                )
                            ));
                        }
                        acl_check!(self, new_data_len, data_size, Truncate, held);
                        if len > 0 {
                            Self::mark_replay(replay, commit_shrink(file, clen, new_data_len))?;
                        }
                        return Ok(());
                    }
                    Some(BStackGenOp::Atrunc { n, data }) => {
                        if n > data_size {
                            return Err(io_error!(
                                InvalidInput,
                                format!(
                                    "process_gen: atrunc n ({n}) exceeds payload size ({data_size})"
                                )
                            ));
                        }
                        let new_tail_start = data_size - n;
                        if new_tail_start < locked {
                            return Err(io_error!(
                                InvalidInput,
                                format!(
                                    "process_gen: atrunc would modify locked region [0, {locked})"
                                )
                            ));
                        }
                        acl_check!(self, new_tail_start, data_size, Truncate, held);
                        if n != 0 || !data.is_empty() {
                            let file_end = HEADER_SIZE + data_size;
                            Self::mark_replay(
                                replay,
                                commit_tail_replace(file, clen, new_tail_start, n, data, file_end),
                            )?;
                        }
                        return Ok(());
                    }
                    Some(BStackGenOp::Splice { old, new }) => {
                        let n = old.len() as u64;
                        if n > data_size {
                            return Err(io_error!(
                                InvalidInput,
                                format!(
                                    "process_gen: splice n ({n}) exceeds payload size ({data_size})"
                                )
                            ));
                        }
                        let new_tail_start = data_size - n;
                        if new_tail_start < locked {
                            return Err(io_error!(
                                InvalidInput,
                                format!(
                                    "process_gen: splice would modify locked region [0, {locked})"
                                )
                            ));
                        }
                        acl_check!(self, new_tail_start, data_size, Truncate, held);
                        if n != 0 || !new.is_empty() {
                            // Read the removed bytes before any mutation.
                            read_at(file, new_tail_start, old)?;
                            let file_end = HEADER_SIZE + data_size;
                            Self::mark_replay(
                                replay,
                                commit_tail_replace(file, clen, new_tail_start, n, new, file_end),
                            )?;
                        }
                        return Ok(());
                    }
                    Some(BStackGenOp::Sparse { writes, length }) => {
                        // Copy the borrowed blocks into a local list so they can be
                        // filtered and sorted for validation (the source slice is `&'a`).
                        let mut blocks: Vec<(u64, &[u8])> = writes
                            .iter()
                            .map(|(off, d)| (*off, *d))
                            .filter(|(_, d)| !d.is_empty())
                            .collect();
                        validate_sparse_blocks(&mut blocks, length, "process_gen: sparse")?;
                        if length != 0 {
                            let file_end = HEADER_SIZE + data_size;
                            let new_len = checked_end(
                                data_size,
                                length,
                                "process_gen: sparse data_size + length overflows u64",
                            )?;
                            acl_check!(self, data_size, new_len, Write, held);
                            Self::mark_replay(
                                replay,
                                commit_sparse_extend(
                                    file, clen, data_size, file_end, new_len, &blocks,
                                ),
                            )?;
                        }
                        return Ok(());
                    }
                    Some(BStackGenOp::Len { out }) => {
                        *out = data_size;
                    }
                    Some(BStackGenOp::Abort { source }) => {
                        // Nothing has been mutated: every mutating op ends the
                        // sequence, so reaching here means only reads have run.
                        return source.map_or(Ok(()), Err);
                    }
                    None => return Ok(()),
                }
            }
        }

        /// [`inplace_gen`](BStack::inplace_gen) run under an access token — the
        /// generator counterpart to [`process_gen_as`](Self::process_gen_as), for
        /// the overlay-resolving in-place engine. Duplicates `inplace_gen` (kept in
        /// `lib.rs`) rather than threading authority through it; the two must stay
        /// in sync.
        #[cfg(feature = "atomic")]
        pub fn inplace_gen_as<'a, A, F>(&self, auth: A, mut f: F) -> io::Result<()>
        where
            A: super::BStackAuthority,
            F: FnMut(io::Result<()>) -> Option<BStackGenOp<'a>>,
        {
            let held = auth.authorities_for(self);
            let mut guard = self.write_lock()?;
            let (file, _, replay) = &mut *guard;
            let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
            let locked = self.locked.load(Ordering::Acquire);
            fault_point!(self, "inplace_gen");
            // Sorted, pairwise-non-overlapping set of pending in-place edits, each
            // borrowing the caller's `Write` data (or `Repeat` pattern) for the
            // lifetime of the call.
            let mut overlay: Vec<(u64, OverlayData<'a>)> = Vec::new();
            let mut feedback: io::Result<()> = Ok(());
            loop {
                match f(feedback) {
                    Some(BStackGenOp::Read { offset, buf }) => {
                        // Validate first so a bad range still beats an injected
                        // fault, then let the policy stand in for the read itself.
                        // Unlike `process_gen`, a failed `Read` here does not end
                        // the call: it is reported to the generator through its
                        // `feedback` argument, so an injected fault must take the
                        // same route as a genuine one.
                        feedback = match inplace_validate_read(offset, buf.len() as u64, data_size)
                        {
                            Err(e) => Err(e),
                            Ok(()) => {
                                // A denied read is reported through `feedback`, the same
                                // route a genuine read failure takes. Validation passed,
                                // so `offset + len` cannot overflow.
                                #[cfg(feature = "expensive-slice-access-control")]
                                let gate = self.acl_check(
                                    offset,
                                    offset + buf.len() as u64,
                                    AccessOp::Read,
                                    held,
                                );
                                #[cfg(not(feature = "expensive-slice-access-control"))]
                                let gate: io::Result<()> = Ok(());
                                gate.and_then(|()| {
                                    fault_probe!(self, "inplace_gen:read").map_or_else(
                                        || {
                                            inplace_overlay_read(
                                                file, data_size, offset, buf, &overlay,
                                            )
                                        },
                                        Err,
                                    )
                                })
                            }
                        };
                    }
                    Some(BStackGenOp::Write { offset, data }) => {
                        feedback = inplace_validate_write(offset, data, data_size, locked);
                        // A denial is routed to the generator like a validation error,
                        // not returned from the call. `is_ok` implies the range already
                        // passed `inplace_validate_write`'s `checked_end`, so `offset +
                        // len` cannot overflow here.
                        #[cfg(feature = "expensive-slice-access-control")]
                        if feedback.is_ok() {
                            let end = offset + data.len() as u64;
                            feedback = self.acl_check(offset, end, AccessOp::Write, held);
                        }
                        if feedback.is_ok() && !data.is_empty() {
                            inplace_overlay_insert(
                                &mut overlay,
                                offset,
                                OverlayData::Literal(data),
                            );
                        }
                    }
                    Some(BStackGenOp::Repeat {
                        offset,
                        pattern,
                        count,
                    }) => {
                        feedback =
                            inplace_validate_repeat(offset, pattern, count, data_size, locked);
                        if feedback.is_ok() && !pattern.is_empty() && count > 0 {
                            // Non-overflowing after validation.
                            let len = pattern.len() as u64 * count;
                            inplace_overlay_insert(
                                &mut overlay,
                                offset,
                                OverlayData::Repeat {
                                    pattern,
                                    phase: 0,
                                    len,
                                },
                            );
                        }
                    }
                    Some(BStackGenOp::Len { out }) => {
                        *out = data_size;
                        feedback = Ok(());
                    }
                    Some(BStackGenOp::Abort { source }) => {
                        // Drop the overlay without committing: the pending writes
                        // only ever existed in memory, so the file is untouched.
                        return source.map_or(Ok(()), Err);
                    }
                    Some(BStackGenOp::Swap { .. }) => {
                        feedback = Err(io_error!(
                            InvalidInput,
                            "inplace_gen: Swap is not permitted (Read/Write/Len only)"
                        ));
                    }
                    Some(BStackGenOp::Push { .. }) => {
                        feedback = Err(io_error!(
                            InvalidInput,
                            "inplace_gen: Push is not permitted (in-place writes only)"
                        ));
                    }
                    Some(BStackGenOp::Pop { .. }) => {
                        feedback = Err(io_error!(
                            InvalidInput,
                            "inplace_gen: Pop is not permitted (in-place writes only)"
                        ));
                    }
                    Some(BStackGenOp::Discard { .. }) => {
                        feedback = Err(io_error!(
                            InvalidInput,
                            "inplace_gen: Discard is not permitted (in-place writes only)"
                        ));
                    }
                    Some(BStackGenOp::Atrunc { .. }) => {
                        feedback = Err(io_error!(
                            InvalidInput,
                            "inplace_gen: Atrunc is not permitted (in-place writes only)"
                        ));
                    }
                    Some(BStackGenOp::Splice { .. }) => {
                        feedback = Err(io_error!(
                            InvalidInput,
                            "inplace_gen: Splice is not permitted (in-place writes only)"
                        ));
                    }
                    Some(BStackGenOp::Sparse { .. }) => {
                        feedback = Err(io_error!(
                            InvalidInput,
                            "inplace_gen: Sparse is not permitted (in-place writes only)"
                        ));
                    }
                    None => break,
                }
            }
            // Commit the accumulated edits. Zero → nothing to do; a lone literal takes
            // the ordinary single-write path and a lone repeat the compact repeat-fill
            // journal; several edits go through the multi-write journal (which streams
            // any repeat block rather than materialising it).
            match overlay.len() {
                0 => Ok(()),
                1 => match overlay[0] {
                    (offset, OverlayData::Literal(data)) => {
                        Self::mark_replay(replay, set_in_place(file, data_size, offset, data))
                    }
                    // A lone repeat is never sliced (slicing needs an overlapping edit,
                    // which would leave it non-lone), so `phase == 0` and `len` is a
                    // whole number of periods — exactly what `repeat_fill` expects.
                    (
                        offset,
                        OverlayData::Repeat {
                            pattern,
                            phase: 0,
                            len,
                        },
                    ) if len % pattern.len() as u64 == 0 => Self::mark_replay(
                        replay,
                        repeat_fill(file, data_size, offset, pattern, len / pattern.len() as u64),
                    ),
                    _ => Self::mark_replay(
                        replay,
                        journaled_multi_overlay(file, data_size, &overlay),
                    ),
                },
                _ => Self::mark_replay(replay, journaled_multi_overlay(file, data_size, &overlay)),
            }
        }

        /// Arm `[offset, offset + len)` with `mode`, presenting `auth`. Crate-internal.
        ///
        /// All public protection goes through
        /// [`BStackOwnedSlice::protect`](crate::BStackOwnedSlice::protect), which
        /// bounds the range to a genuine allocation rather than an arbitrary span.
        /// Nothing is persisted, so reopening clears the policy.
        ///
        /// A caller may re-mode any range whose
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
            Ok(())
        }

        /// Burn the allocator-authority mint, so no external caller can obtain
        /// [`Alloc`](BStackAccess::Alloc) authority over an allocator's arena.
        ///
        /// Every allocator constructor calls this: an allocator owns its stack
        /// exclusively, and its metadata marks and reclaims are made through the
        /// crate-internal [`acl_mark_alloc`](Self::acl_mark_alloc) /
        /// [`acl_reclaim`](Self::acl_reclaim), never through the public token. Left
        /// mintable, `allocator.stack().take_alloc_authority()` would hand a caller
        /// the very capability that makes metadata inviolable. Idempotent — the
        /// mint is one-shot.
        pub(crate) fn acl_claim_alloc(&self) {
            let _ = self.take_alloc_authority();
        }

        /// Mark `[offset, offset + len)` as allocator-owned
        /// [`Alloc`](BStackAccess::Alloc) metadata.
        ///
        /// Presents synthetic [`ALLOC`](BStackAccessAuthorities::ALLOC) authority:
        /// the crate-internal caller *is* the allocator, so it needs no minted
        /// token to arm its own metadata. Re-marking a range already `Alloc` (a
        /// reopened arena, a reused block) is admitted.
        ///
        // The metadata-marking hook. Marking a region `Alloc` also requires the
        // allocator to route its own reads/writes of that region through the
        // `_as(ALLOC)` siblings, so per-allocator adoption is staged separately
        // from the (universal) mint-burn and dealloc-reclaim wiring.
        #[allow(dead_code)]
        pub(crate) fn acl_mark_alloc(&self, offset: u64, len: u64) -> io::Result<()> {
            self.protect_as(
                BStackAccessAuthorities::ALLOC,
                offset,
                len,
                BStackAccess::Alloc,
            )
        }

        /// Whether `[offset, offset + len)` may be reclaimed on `dealloc` — i.e.
        /// carries no caller-set policy (nothing but
        /// [`All`](BStackAccess::All)/[`Alloc`](BStackAccess::Alloc)).
        ///
        /// The read-only half of [`acl_reclaim`](Self::acl_reclaim), split out so a
        /// bulk free can validate every handle *before* clearing any — keeping the
        /// batch atomic. Returns [`PermissionDenied`](io::ErrorKind::PermissionDenied)
        /// otherwise.
        pub(crate) fn acl_reclaimable(&self, offset: u64, len: u64) -> io::Result<()> {
            if len == 0 {
                return Ok(());
            }
            let end = checked_end(offset, len, "acl_reclaimable: offset + len overflows u64")?;
            if self.acl.read().unwrap().reclaimable_by_alloc(offset, end) {
                Ok(())
            } else {
                Err(io_error!(
                    PermissionDenied,
                    format!(
                        "dealloc: [{offset}, {end}) carries access-control policy; \
                         unprotect it before freeing"
                    )
                ))
            }
        }

        /// Reclaim `[offset, offset + len)` on `dealloc`: refuse (see
        /// [`acl_reclaimable`](Self::acl_reclaimable)) if it carries caller-set
        /// policy, so a stated policy is never silently dropped nor left to poison
        /// the block's next owner; otherwise reset the range to
        /// [`All`](BStackAccess::All), clearing the allocator's own
        /// [`Alloc`](BStackAccess::Alloc) metadata marks over it.
        pub(crate) fn acl_reclaim(&self, offset: u64, len: u64) -> io::Result<()> {
            if len == 0 {
                return Ok(());
            }
            let end = checked_end(offset, len, "acl_reclaim: offset + len overflows u64")?;
            // Stack lock first, then the acl lock, as in `protect_as`.
            let _guard = self.write_lock()?;
            let mut table = self.acl.write().unwrap();
            if !table.reclaimable_by_alloc(offset, end) {
                return Err(io_error!(
                    PermissionDenied,
                    format!(
                        "dealloc: [{offset}, {end}) carries access-control policy; \
                         unprotect it before freeing"
                    )
                ));
            }
            table.set(offset, end, BStackAccess::All);
            Ok(())
        }

        /// The mode currently governing logical `offset` (for inspection/testing).
        #[inline]
        #[must_use]
        pub fn access_at(&self, offset: u64) -> BStackAccess {
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

        /// [`swap`](BStack::swap) presenting an access token.
        #[cfg(feature = "atomic")]
        pub fn swap_as(
            &self,
            auth: impl BStackAuthority,
            offset: u64,
            buf: impl AsRef<[u8]>,
        ) -> io::Result<Vec<u8>> {
            let buf = buf.as_ref();
            if buf.is_empty() {
                return Ok(Vec::new());
            }
            let held = auth.authorities_for(self);
            let end = checked_end(offset, buf.len() as u64, "swap: offset + len overflows u64")?;
            let mut guard = self.write_lock()?;
            let (file, _, replay) = &mut *guard;
            let locked = self.locked.load(Ordering::Acquire);
            check_offset_unlocked("swap", offset, end, locked)?;
            let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
            if end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!("swap: range [{offset}, {end}) exceeds payload size ({data_size})")
                ));
            }
            acl_check!(self, offset, end, Write, held);
            fault_point!(self, "swap");
            let mut old = vec![0u8; buf.len()];
            read_at(file, offset, &mut old)?;
            Self::mark_replay(replay, set_in_place(file, data_size, offset, buf))?;
            Ok(old)
        }

        /// [`swap_into`](BStack::swap_into) presenting an access token.
        #[cfg(feature = "atomic")]
        pub fn swap_into_as(
            &self,
            auth: impl BStackAuthority,
            offset: u64,
            buf: &mut [u8],
        ) -> io::Result<()> {
            if buf.is_empty() {
                return Ok(());
            }
            let held = auth.authorities_for(self);
            let end = checked_end(
                offset,
                buf.len() as u64,
                "swap_into: offset + len overflows u64",
            )?;
            let mut guard = self.write_lock()?;
            let (file, _, replay) = &mut *guard;
            let locked = self.locked.load(Ordering::Acquire);
            check_offset_unlocked("swap_into", offset, end, locked)?;
            let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
            if end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!(
                        "swap_into: range [{offset}, {end}) exceeds payload size ({data_size})"
                    )
                ));
            }
            acl_check!(self, offset, end, Write, held);
            fault_point!(self, "swap_into");
            let mut tmp = vec![0u8; buf.len()];
            read_at(file, offset, &mut tmp)?;
            Self::mark_replay(replay, set_in_place(file, data_size, offset, buf))?;
            buf.copy_from_slice(&tmp);
            Ok(())
        }

        /// [`splice`](BStack::splice) presenting an access token.
        #[cfg(feature = "atomic")]
        pub fn splice_as(
            &self,
            auth: impl BStackAuthority,
            n: u64,
            buf: impl AsRef<[u8]>,
        ) -> io::Result<Vec<u8>> {
            let buf = buf.as_ref();
            let buf_len = buf.len() as u64;
            if n == 0 && buf_len == 0 {
                return Ok(Vec::new());
            }
            let held = auth.authorities_for(self);
            let mut guard = self.write_lock()?;
            let (file, clen, replay) = &mut *guard;
            let file_end = file.seek(SeekFrom::End(0))?;
            let data_size = file_end - HEADER_SIZE;
            if n > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!("splice: n ({n}) exceeds payload size ({data_size})")
                ));
            }
            let locked = self.locked.load(Ordering::Acquire);
            let new_tail_start = data_size - n;
            if new_tail_start < locked {
                return Err(io_error!(
                    InvalidInput,
                    format!("splice: operation would modify locked region [0, {locked})")
                ));
            }
            acl_check!(self, new_tail_start, data_size, Truncate, held);
            fault_point!(self, "splice");
            let mut removed = vec![0u8; n as usize];
            read_at(file, new_tail_start, &mut removed)?;
            Self::mark_replay(
                replay,
                commit_tail_replace(file, clen, new_tail_start, n, buf, file_end),
            )?;
            Ok(removed)
        }

        /// [`splice_into`](BStack::splice_into) presenting an access token.
        #[cfg(feature = "atomic")]
        pub fn splice_into_as(
            &self,
            auth: impl BStackAuthority,
            old: &mut [u8],
            new: impl AsRef<[u8]>,
        ) -> io::Result<()> {
            let new = new.as_ref();
            let n = old.len() as u64;
            let new_len = new.len() as u64;
            if n == 0 && new_len == 0 {
                return Ok(());
            }
            let held = auth.authorities_for(self);
            let mut guard = self.write_lock()?;
            let (file, clen, replay) = &mut *guard;
            let file_end = file.seek(SeekFrom::End(0))?;
            let data_size = file_end - HEADER_SIZE;
            if n > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!("splice_into: n ({n}) exceeds payload size ({data_size})")
                ));
            }
            let locked = self.locked.load(Ordering::Acquire);
            let new_tail_start = data_size - n;
            if new_tail_start < locked {
                return Err(io_error!(
                    InvalidInput,
                    format!("splice_into: operation would modify locked region [0, {locked})")
                ));
            }
            acl_check!(self, new_tail_start, data_size, Truncate, held);
            fault_point!(self, "splice_into");
            read_at(file, new_tail_start, old)?;
            Self::mark_replay(
                replay,
                commit_tail_replace(file, clen, new_tail_start, n, new, file_end),
            )
        }

        /// [`replace`](BStack::replace) presenting an access token.
        #[cfg(feature = "atomic")]
        pub fn replace_as<F>(&self, auth: impl BStackAuthority, n: u64, f: F) -> io::Result<()>
        where
            F: FnOnce(&[u8]) -> Vec<u8>,
        {
            let held = auth.authorities_for(self);
            let mut guard = self.write_lock()?;
            let (file, clen, replay) = &mut *guard;
            let file_end = file.seek(SeekFrom::End(0))?;
            let data_size = file_end - HEADER_SIZE;
            if n > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!("replace: n ({n}) exceeds payload size ({data_size})")
                ));
            }
            let locked = self.locked.load(Ordering::Acquire);
            let new_tail_start = data_size - n;
            if new_tail_start < locked {
                return Err(io_error!(
                    InvalidInput,
                    format!("replace: operation would modify locked region [0, {locked})")
                ));
            }
            acl_check!(self, new_tail_start, data_size, Truncate, held);
            fault_point!(self, "replace");
            let mut old_tail = vec![0u8; n as usize];
            read_at(file, new_tail_start, &mut old_tail)?;
            let new_tail = f(&old_tail);
            Self::mark_replay(
                replay,
                commit_tail_replace(file, clen, new_tail_start, n, &new_tail, file_end),
            )
        }

        /// [`set_batched`](BStack::set_batched) presenting an access token.
        #[cfg(feature = "atomic")]
        pub fn set_batched_as<I, D>(&self, auth: impl BStackAuthority, writes: I) -> io::Result<()>
        where
            I: IntoIterator<Item = (u64, D)>,
            D: AsRef<[u8]>,
        {
            let owned: Vec<(u64, D)> = writes.into_iter().collect();
            let mut blocks: Vec<(u64, &[u8])> = owned
                .iter()
                .map(|(off, d)| (*off, d.as_ref()))
                .filter(|(_, d)| !d.is_empty())
                .collect();
            if blocks.is_empty() {
                return Ok(());
            }
            let held = auth.authorities_for(self);
            let mut guard = self.write_lock()?;
            let (file, _, replay) = &mut *guard;
            let locked = self.locked.load(Ordering::Acquire);
            let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
            for (off, data) in &blocks {
                let end = checked_end(
                    *off,
                    data.len() as u64,
                    "set_batched: offset + len overflows u64",
                )?;
                if *off < locked {
                    return Err(io_error!(
                        InvalidInput,
                        format!(
                            "set_batched: write range [{off}, {end}) overlaps locked region [0, {locked})"
                        )
                    ));
                }
                if end > data_size {
                    return Err(io_error!(
                        InvalidInput,
                        format!(
                            "set_batched: write range [{off}, {end}) exceeds payload size ({data_size})"
                        )
                    ));
                }
                acl_check!(self, *off, end, Write, held);
            }
            fault_point!(self, "set_batched");
            if blocks.len() == 1 {
                let (off, data) = blocks[0];
                return Self::mark_replay(replay, set_in_place(file, data_size, off, data));
            }
            blocks.sort_by_key(|(off, _)| *off);
            for pair in blocks.windows(2) {
                let (a_off, a_data) = pair[0];
                let (b_off, _) = pair[1];
                let a_end = a_off + a_data.len() as u64;
                if a_end > b_off {
                    return Err(io_error!(
                        InvalidInput,
                        format!(
                            "set_batched: write range [{a_off}, {a_end}) overlaps [{b_off}, ...)"
                        )
                    ));
                }
            }
            Self::mark_replay(replay, journaled_multi_set(file, data_size, &blocks))
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

        /// [`push`](BStack::push) presenting an access token.
        pub fn push_as(
            &self,
            auth: impl BStackAuthority,
            data: impl AsRef<[u8]>,
        ) -> io::Result<u64> {
            let data = data.as_ref();
            let held = auth.authorities_for(self);
            let mut guard = self.write_lock()?;
            let (file, clen, replay) = &mut *guard;
            let file_end = file.seek(SeekFrom::End(0))?;
            let logical_offset = file_end - HEADER_SIZE;
            if data.is_empty() {
                return Ok(logical_offset);
            }
            acl_check!(
                self,
                logical_offset,
                logical_offset + data.len() as u64,
                Write,
                held
            );
            fault_point!(self, "push");
            if let Err(e) = file.write_all(data) {
                if file.set_len(file_end).is_err() {
                    *replay = true;
                }
                return Err(e);
            }
            let new_len = logical_offset + data.len() as u64;
            Self::mark_replay(
                replay,
                commit_grow(file, clen, new_len, logical_offset, file_end),
            )?;
            Ok(logical_offset)
        }

        /// [`extend`](BStack::extend) presenting an access token.
        pub fn extend_as(&self, auth: impl BStackAuthority, n: u64) -> io::Result<u64> {
            let held = auth.authorities_for(self);
            let mut guard = self.write_lock()?;
            let (file, clen, replay) = &mut *guard;
            let file_end = file.seek(SeekFrom::End(0))?;
            let logical_offset = file_end - HEADER_SIZE;
            if n == 0 {
                return Ok(logical_offset);
            }
            acl_check!(self, logical_offset, logical_offset + n, Write, held);
            fault_point!(self, "extend");
            let new_file_end = file_end + n;
            Self::mark_replay(replay, file.set_len(new_file_end))?;
            let new_len = logical_offset + n;
            Self::mark_replay(
                replay,
                commit_grow(file, clen, new_len, logical_offset, file_end),
            )?;
            Ok(logical_offset)
        }

        /// [`resize`](BStack::resize) presenting an access token.
        pub fn resize_as(&self, auth: impl BStackAuthority, target: u64) -> io::Result<u64> {
            let held = auth.authorities_for(self);
            let mut guard = self.write_lock()?;
            let (file, clen, replay) = &mut *guard;
            let file_end = file.seek(SeekFrom::End(0))?;
            let data_size = file_end - HEADER_SIZE;
            if target == data_size {
                return Ok(data_size);
            }
            if target < data_size {
                let locked = self.locked.load(Ordering::Acquire);
                if target < locked {
                    return Err(io_error!(
                        InvalidInput,
                        format!(
                            "resize({target}) would shrink payload below locked length ({locked})"
                        )
                    ));
                }
                acl_check!(self, target, data_size, Truncate, held);
                fault_point!(self, "resize");
                Self::mark_replay(replay, commit_shrink(file, clen, target))?;
                return Ok(data_size);
            }
            acl_check!(self, data_size, target, Write, held);
            fault_point!(self, "resize");
            Self::mark_replay(replay, file.set_len(HEADER_SIZE + target))?;
            Self::mark_replay(replay, commit_grow(file, clen, target, data_size, file_end))?;
            Ok(data_size)
        }

        /// [`ensure`](BStack::ensure) presenting an access token.
        pub fn ensure_as(&self, auth: impl BStackAuthority, target: u64) -> io::Result<u64> {
            let held = auth.authorities_for(self);
            let mut guard = self.write_lock()?;
            let (file, clen, replay) = &mut *guard;
            let file_end = file.seek(SeekFrom::End(0))?;
            let data_size = file_end - HEADER_SIZE;
            if target <= data_size {
                return Ok(data_size);
            }
            acl_check!(self, data_size, target, Write, held);
            fault_point!(self, "ensure");
            Self::mark_replay(replay, file.set_len(HEADER_SIZE + target))?;
            Self::mark_replay(replay, commit_grow(file, clen, target, data_size, file_end))?;
            Ok(data_size)
        }

        /// [`extend_sparse`](BStack::extend_sparse) presenting an access token.
        pub fn extend_sparse_as(
            &self,
            auth: impl BStackAuthority,
            buf: impl AsRef<[u8]>,
            length: u64,
        ) -> io::Result<u64> {
            let buf = buf.as_ref();
            if buf.len() as u64 > length {
                return Err(io_error!(
                    InvalidInput,
                    "extend_sparse: buffer length ({}) exceeds extension length ({length})",
                    buf.len()
                ));
            }
            let held = auth.authorities_for(self);
            let mut guard = self.write_lock()?;
            let (file, clen, replay) = &mut *guard;
            let file_end = file.seek(SeekFrom::End(0))?;
            let logical_offset = file_end - HEADER_SIZE;
            if length == 0 {
                return Ok(logical_offset);
            }
            let new_len = logical_offset.checked_add(length).ok_or_else(|| {
                io_error!(
                    InvalidInput,
                    "extend_sparse: payload size + length overflows u64"
                )
            })?;
            acl_check!(self, logical_offset, new_len, Write, held);
            fault_point!(self, "extend_sparse");
            let one = [(0u64, buf)];
            let blocks: &[(u64, &[u8])] = if buf.is_empty() { &[] } else { &one };
            Self::mark_replay(
                replay,
                commit_sparse_extend(file, clen, logical_offset, file_end, new_len, blocks),
            )?;
            Ok(logical_offset)
        }

        /// [`extend_sparse_batched`](BStack::extend_sparse_batched) presenting an access token.
        pub fn extend_sparse_batched_as<I, D>(
            &self,
            auth: impl BStackAuthority,
            writes: I,
            length: u64,
        ) -> io::Result<u64>
        where
            I: IntoIterator<Item = (u64, D)>,
            D: AsRef<[u8]>,
        {
            let owned: Vec<(u64, D)> = writes.into_iter().collect();
            let mut blocks: Vec<(u64, &[u8])> = owned
                .iter()
                .map(|(off, d)| (*off, d.as_ref()))
                .filter(|(_, d)| !d.is_empty())
                .collect();
            validate_sparse_blocks(&mut blocks, length, "extend_sparse_batched")?;
            let held = auth.authorities_for(self);
            let mut guard = self.write_lock()?;
            let (file, clen, replay) = &mut *guard;
            let file_end = file.seek(SeekFrom::End(0))?;
            let logical_offset = file_end - HEADER_SIZE;
            if length == 0 {
                return Ok(logical_offset);
            }
            let new_len = logical_offset.checked_add(length).ok_or_else(|| {
                io_error!(
                    InvalidInput,
                    "extend_sparse_batched: payload size + length overflows u64"
                )
            })?;
            acl_check!(self, logical_offset, new_len, Write, held);
            fault_point!(self, "extend_sparse_batched");
            Self::mark_replay(
                replay,
                commit_sparse_extend(file, clen, logical_offset, file_end, new_len, &blocks),
            )?;
            Ok(logical_offset)
        }

        /// [`pop`](BStack::pop) presenting an access token.
        pub fn pop_as(&self, auth: impl BStackAuthority, n: u64) -> io::Result<Vec<u8>> {
            let held = auth.authorities_for(self);
            let mut guard = self.write_lock()?;
            let (file, clen, replay) = &mut *guard;
            let raw_size = file.seek(SeekFrom::End(0))?;
            let data_size = raw_size - HEADER_SIZE;
            if n > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!("pop({n}) exceeds payload size ({data_size})")
                ));
            }
            let new_data_len = data_size - n;
            let locked = self.locked.load(Ordering::Acquire);
            if new_data_len < locked {
                return Err(io_error!(
                    InvalidInput,
                    format!("pop({n}) would shrink payload below locked length ({locked})")
                ));
            }
            acl_check!(self, new_data_len, data_size, Truncate, held);
            let mut buf = vec![0u8; n as usize];
            fault_point!(self, "pop");
            read_at(file, new_data_len, &mut buf)?;
            Self::mark_replay(replay, commit_shrink(file, clen, new_data_len))?;
            Ok(buf)
        }

        /// [`pop_into`](BStack::pop_into) presenting an access token.
        pub fn pop_into_as(&self, auth: impl BStackAuthority, buf: &mut [u8]) -> io::Result<()> {
            if buf.is_empty() {
                return Ok(());
            }
            let n = buf.len() as u64;
            let held = auth.authorities_for(self);
            let mut guard = self.write_lock()?;
            let (file, clen, replay) = &mut *guard;
            let raw_size = file.seek(SeekFrom::End(0))?;
            let data_size = raw_size - HEADER_SIZE;
            if n > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!("pop_into({n}) exceeds payload size ({data_size})")
                ));
            }
            let new_data_len = data_size - n;
            let locked = self.locked.load(Ordering::Acquire);
            if new_data_len < locked {
                return Err(io_error!(
                    InvalidInput,
                    format!("pop_into({n}) would shrink payload below locked length ({locked})")
                ));
            }
            acl_check!(self, new_data_len, data_size, Truncate, held);
            fault_point!(self, "pop_into");
            read_at(file, new_data_len, buf)?;
            Self::mark_replay(replay, commit_shrink(file, clen, new_data_len))?;
            Ok(())
        }

        /// [`peek`](BStack::peek) presenting an access token.
        pub fn peek_as(&self, auth: impl BStackAuthority, offset: u64) -> io::Result<Vec<u8>> {
            let held = auth.authorities_for(self);
            #[cfg(any(unix, windows))]
            {
                let guard = self.read_lock()?;
                let file = &guard.0;
                let data_size = file.metadata()?.len().saturating_sub(HEADER_SIZE);
                if offset > data_size {
                    return Err(io_error!(
                        InvalidInput,
                        format!("peek offset ({offset}) exceeds payload size ({data_size})")
                    ));
                }
                acl_check!(self, offset, data_size, Read, held);
                fault_point!(self, "peek");
                pread_exact(file, HEADER_SIZE + offset, (data_size - offset) as usize)
            }
            #[cfg(not(any(unix, windows)))]
            {
                let mut guard = self.write_lock_read()?;
                let file = &mut guard.0;
                let raw_size = file.seek(SeekFrom::End(0))?;
                let data_size = raw_size.saturating_sub(HEADER_SIZE);
                if offset > data_size {
                    return Err(io_error!(
                        InvalidInput,
                        format!("peek offset ({offset}) exceeds payload size ({data_size})")
                    ));
                }
                acl_check!(self, offset, data_size, Read, held);
                fault_point!(self, "peek");
                file.seek(SeekFrom::Start(HEADER_SIZE + offset))?;
                let mut buf = vec![0u8; (data_size - offset) as usize];
                file.read_exact(&mut buf)?;
                Ok(buf)
            }
        }

        /// [`peek_into`](BStack::peek_into) presenting an access token.
        pub fn peek_into_as(
            &self,
            auth: impl BStackAuthority,
            offset: u64,
            buf: &mut [u8],
        ) -> io::Result<()> {
            if buf.is_empty() {
                return Ok(());
            }
            let len = buf.len() as u64;
            let end = offset
                .checked_add(len)
                .ok_or_else(|| io_error!(InvalidInput, "peek_into: offset + len overflows u64"))?;
            let held = auth.authorities_for(self);
            acl_check!(self, offset, end, Read, held);
            #[cfg(any(unix, windows))]
            {
                let guard = self.read_lock()?;
                let file = &guard.0;
                let data_size = file.metadata()?.len().saturating_sub(HEADER_SIZE);
                if end > data_size {
                    return Err(io_error!(
                        InvalidInput,
                        format!(
                            "peek_into: range [{offset}, {end}) exceeds payload size ({data_size})"
                        )
                    ));
                }
                fault_point!(self, "peek_into");
                pread_exact_into(file, HEADER_SIZE + offset, buf)
            }
            #[cfg(not(any(unix, windows)))]
            {
                let mut guard = self.write_lock_read()?;
                let file = &mut guard.0;
                let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
                if end > data_size {
                    return Err(io_error!(
                        InvalidInput,
                        format!(
                            "peek_into: range [{offset}, {end}) exceeds payload size ({data_size})"
                        )
                    ));
                }
                fault_point!(self, "peek_into");
                file.seek(SeekFrom::Start(HEADER_SIZE + offset))?;
                file.read_exact(buf)
            }
        }

        /// [`atrunc`](BStack::atrunc) presenting an access token.
        #[cfg(feature = "atomic")]
        pub fn atrunc_as(
            &self,
            auth: impl BStackAuthority,
            n: u64,
            buf: impl AsRef<[u8]>,
        ) -> io::Result<()> {
            let buf = buf.as_ref();
            let buf_len = buf.len() as u64;
            if n == 0 && buf_len == 0 {
                return Ok(());
            }
            let held = auth.authorities_for(self);
            let mut guard = self.write_lock()?;
            let (file, clen, replay) = &mut *guard;
            let file_end = file.seek(SeekFrom::End(0))?;
            let data_size = file_end - HEADER_SIZE;
            if n > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!("atrunc: n ({n}) exceeds payload size ({data_size})")
                ));
            }
            let locked = self.locked.load(Ordering::Acquire);
            let new_tail_start = data_size - n;
            if new_tail_start < locked {
                return Err(io_error!(
                    InvalidInput,
                    format!("atrunc: operation would modify locked region [0, {locked})")
                ));
            }
            acl_check!(self, new_tail_start, data_size, Truncate, held);
            fault_point!(self, "atrunc");
            Self::mark_replay(
                replay,
                commit_tail_replace(file, clen, new_tail_start, n, buf, file_end),
            )
        }

        /// [`try_extend`](BStack::try_extend) presenting an access token.
        #[cfg(feature = "atomic")]
        pub fn try_extend_as(
            &self,
            auth: impl BStackAuthority,
            s: u64,
            buf: impl AsRef<[u8]>,
        ) -> io::Result<bool> {
            let buf = buf.as_ref();
            let held = auth.authorities_for(self);
            let mut guard = self.write_lock()?;
            let (file, clen, replay) = &mut *guard;
            let file_end = file.seek(SeekFrom::End(0))?;
            let data_size = file_end - HEADER_SIZE;
            if data_size != s {
                return Ok(false);
            }
            if buf.is_empty() {
                return Ok(true);
            }
            acl_check!(self, data_size, data_size + buf.len() as u64, Write, held);
            fault_point!(self, "try_extend");
            if let Err(e) = file.write_all(buf) {
                if file.set_len(file_end).is_err() {
                    *replay = true;
                }
                return Err(e);
            }
            let new_len = data_size + buf.len() as u64;
            Self::mark_replay(
                replay,
                commit_grow(file, clen, new_len, data_size, file_end),
            )?;
            Ok(true)
        }

        /// [`try_extend_zeros`](BStack::try_extend_zeros) presenting an access token.
        #[cfg(feature = "atomic")]
        pub fn try_extend_zeros_as(
            &self,
            auth: impl BStackAuthority,
            s: u64,
            n: u64,
        ) -> io::Result<bool> {
            let held = auth.authorities_for(self);
            let mut guard = self.write_lock()?;
            let (file, clen, replay) = &mut *guard;
            let file_end = file.seek(SeekFrom::End(0))?;
            let data_size = file_end - HEADER_SIZE;
            if data_size != s {
                return Ok(false);
            }
            if n == 0 {
                return Ok(true);
            }
            let new_len = checked_end(
                data_size,
                n,
                "try_extend_zeros: data_size + n overflows u64",
            )?;
            acl_check!(self, data_size, new_len, Write, held);
            fault_point!(self, "try_extend_zeros");
            Self::mark_replay(replay, file.set_len(HEADER_SIZE + new_len))?;
            Self::mark_replay(
                replay,
                commit_grow(file, clen, new_len, data_size, file_end),
            )?;
            Ok(true)
        }

        /// [`try_extend_sparse`](BStack::try_extend_sparse) presenting an access token.
        #[cfg(feature = "atomic")]
        pub fn try_extend_sparse_as(
            &self,
            auth: impl BStackAuthority,
            s: u64,
            buf: impl AsRef<[u8]>,
            length: u64,
        ) -> io::Result<bool> {
            let buf = buf.as_ref();
            if buf.len() as u64 > length {
                return Err(io_error!(
                    InvalidInput,
                    "try_extend_sparse: buffer length ({}) exceeds extension length ({length})",
                    buf.len()
                ));
            }
            let held = auth.authorities_for(self);
            let mut guard = self.write_lock()?;
            let (file, clen, replay) = &mut *guard;
            let file_end = file.seek(SeekFrom::End(0))?;
            let data_size = file_end - HEADER_SIZE;
            if data_size != s {
                return Ok(false);
            }
            if length == 0 {
                return Ok(true);
            }
            let new_len = checked_end(
                data_size,
                length,
                "try_extend_sparse: data_size + length overflows u64",
            )?;
            acl_check!(self, data_size, new_len, Write, held);
            fault_point!(self, "try_extend_sparse");
            let one = [(0u64, buf)];
            let blocks: &[(u64, &[u8])] = if buf.is_empty() { &[] } else { &one };
            Self::mark_replay(
                replay,
                commit_sparse_extend(file, clen, data_size, file_end, new_len, blocks),
            )?;
            Ok(true)
        }

        /// [`try_extend_sparse_batched`](BStack::try_extend_sparse_batched) presenting a token.
        #[cfg(feature = "atomic")]
        pub fn try_extend_sparse_batched_as<I, D>(
            &self,
            auth: impl BStackAuthority,
            s: u64,
            writes: I,
            length: u64,
        ) -> io::Result<bool>
        where
            I: IntoIterator<Item = (u64, D)>,
            D: AsRef<[u8]>,
        {
            let owned: Vec<(u64, D)> = writes.into_iter().collect();
            let mut blocks: Vec<(u64, &[u8])> = owned
                .iter()
                .map(|(off, d)| (*off, d.as_ref()))
                .filter(|(_, d)| !d.is_empty())
                .collect();
            validate_sparse_blocks(&mut blocks, length, "try_extend_sparse_batched")?;
            let held = auth.authorities_for(self);
            let mut guard = self.write_lock()?;
            let (file, clen, replay) = &mut *guard;
            let file_end = file.seek(SeekFrom::End(0))?;
            let data_size = file_end - HEADER_SIZE;
            if data_size != s {
                return Ok(false);
            }
            if length == 0 {
                return Ok(true);
            }
            let new_len = checked_end(
                data_size,
                length,
                "try_extend_sparse_batched: data_size + length overflows u64",
            )?;
            acl_check!(self, data_size, new_len, Write, held);
            fault_point!(self, "try_extend_sparse_batched");
            Self::mark_replay(
                replay,
                commit_sparse_extend(file, clen, data_size, file_end, new_len, &blocks),
            )?;
            Ok(true)
        }

        /// [`try_discard`](BStack::try_discard) presenting an access token.
        #[cfg(feature = "atomic")]
        pub fn try_discard_as(
            &self,
            auth: impl BStackAuthority,
            s: u64,
            n: u64,
        ) -> io::Result<bool> {
            if n == 0 {
                let guard = self.read_lock()?;
                let file = &guard.0;
                let data_size = file.metadata()?.len().saturating_sub(HEADER_SIZE);
                return Ok(data_size == s);
            }
            let held = auth.authorities_for(self);
            let mut guard = self.write_lock()?;
            let (file, clen, replay) = &mut *guard;
            let raw_size = file.seek(SeekFrom::End(0))?;
            let data_size = raw_size - HEADER_SIZE;
            if data_size != s {
                return Ok(false);
            }
            if n > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!("try_discard: n ({n}) exceeds payload size ({data_size})")
                ));
            }
            let new_data_len = data_size - n;
            let locked = self.locked.load(Ordering::Acquire);
            if new_data_len < locked {
                return Err(io_error!(
                    InvalidInput,
                    format!("try_discard: would shrink payload below locked length ({locked})")
                ));
            }
            acl_check!(self, new_data_len, data_size, Truncate, held);
            fault_point!(self, "try_discard");
            Self::mark_replay(replay, commit_shrink(file, clen, new_data_len))?;
            Ok(true)
        }

        /// [`get_batched`](BStack::get_batched) presenting an access token.
        #[cfg(feature = "atomic")]
        pub fn get_batched_as<I>(
            &self,
            auth: impl BStackAuthority,
            ranges: I,
        ) -> io::Result<Vec<Vec<u8>>>
        where
            I: IntoIterator<Item = std::ops::Range<u64>>,
        {
            let held = auth.authorities_for(self);
            let ranges: Vec<std::ops::Range<u64>> = ranges.into_iter().collect();
            if ranges.is_empty() {
                return Ok(Vec::new());
            }
            for r in &ranges {
                if r.end < r.start {
                    return Err(io_error!(
                        InvalidInput,
                        "get_batched: end ({}) < start ({})",
                        r.end,
                        r.start
                    ));
                }
                acl_check!(self, r.start, r.end, Read, held);
            }
            #[cfg(any(unix, windows))]
            {
                let guard = self.read_lock()?;
                let file = &guard.0;
                let data_size = file.metadata()?.len().saturating_sub(HEADER_SIZE);
                fault_point!(self, "get_batched");
                let mut results = Vec::with_capacity(ranges.len());
                for r in &ranges {
                    if r.end > data_size {
                        return Err(io_error!(
                            InvalidInput,
                            "get_batched: end ({}) exceeds payload size ({data_size})",
                            r.end
                        ));
                    }
                    results.push(pread_exact(
                        file,
                        HEADER_SIZE + r.start,
                        (r.end - r.start) as usize,
                    )?);
                }
                Ok(results)
            }
            #[cfg(not(any(unix, windows)))]
            {
                let mut guard = self.write_lock_read()?;
                let file = &mut guard.0;
                let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
                fault_point!(self, "get_batched");
                let mut results = Vec::with_capacity(ranges.len());
                for r in &ranges {
                    if r.end > data_size {
                        return Err(io_error!(
                            InvalidInput,
                            "get_batched: end ({}) exceeds payload size ({data_size})",
                            r.end
                        ));
                    }
                    file.seek(SeekFrom::Start(HEADER_SIZE + r.start))?;
                    let mut buf = vec![0u8; (r.end - r.start) as usize];
                    file.read_exact(&mut buf)?;
                    results.push(buf);
                }
                Ok(results)
            }
        }

        /// [`get_batched_into`](BStack::get_batched_into) presenting an access token.
        #[cfg(feature = "atomic")]
        pub fn get_batched_into_as<'a, I>(
            &self,
            auth: impl BStackAuthority,
            bufs: I,
        ) -> io::Result<()>
        where
            I: IntoIterator<Item = (u64, &'a mut [u8])>,
        {
            let held = auth.authorities_for(self);
            let bufs: Vec<(u64, &'a mut [u8])> = bufs.into_iter().collect();
            if bufs.is_empty() {
                return Ok(());
            }
            #[cfg(any(unix, windows))]
            {
                let guard = self.read_lock()?;
                let file = &guard.0;
                let data_size = file.metadata()?.len().saturating_sub(HEADER_SIZE);
                fault_point!(self, "get_batched_into");
                for (ptr, buf) in bufs {
                    let end = ptr.checked_add(buf.len() as u64).ok_or_else(|| {
                        io_error!(
                            InvalidInput,
                            "get_batched_into: offset + buf.len() overflows u64"
                        )
                    })?;
                    if end > data_size {
                        return Err(io_error!(
                            InvalidInput,
                            format!(
                                "get_batched_into: end ({end}) exceeds payload size ({data_size})",
                            )
                        ));
                    }
                    acl_check!(self, ptr, end, Read, held);
                    pread_exact_into(file, HEADER_SIZE + ptr, buf)?;
                }
                Ok(())
            }
            #[cfg(not(any(unix, windows)))]
            {
                let mut guard = self.write_lock_read()?;
                let file = &mut guard.0;
                let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
                fault_point!(self, "get_batched_into");
                for (ptr, buf) in bufs {
                    let end = ptr.checked_add(buf.len() as u64).ok_or_else(|| {
                        io_error!(
                            InvalidInput,
                            "get_batched_into: offset + buf.len() overflows u64"
                        )
                    })?;
                    if end > data_size {
                        return Err(io_error!(
                            InvalidInput,
                            format!(
                                "get_batched_into: end ({end}) exceeds payload size ({data_size})",
                            )
                        ));
                    }
                    acl_check!(self, ptr, end, Read, held);
                    file.seek(SeekFrom::Start(HEADER_SIZE + ptr))?;
                    file.read_exact(buf)?;
                }
                Ok(())
            }
        }

        /// [`get_batched_gen`](BStack::get_batched_gen) presenting an access token.
        #[cfg(feature = "atomic")]
        pub fn get_batched_gen_as<'a, F>(
            &self,
            auth: impl BStackAuthority,
            mut f: F,
        ) -> io::Result<()>
        where
            F: FnMut() -> Option<(u64, &'a mut [u8])>,
        {
            let held = auth.authorities_for(self);
            #[cfg(any(unix, windows))]
            {
                let guard = self.read_lock()?;
                let file = &guard.0;
                let data_size = file.metadata()?.len().saturating_sub(HEADER_SIZE);
                fault_point!(self, "get_batched_gen");
                while let Some((offset, buf)) = f() {
                    let end = offset.checked_add(buf.len() as u64).ok_or_else(|| {
                        io_error!(
                            InvalidInput,
                            "get_batched_gen: offset + buf.len() overflows u64"
                        )
                    })?;
                    if end > data_size {
                        return Err(io_error!(
                            InvalidInput,
                            format!(
                                "get_batched_gen: end ({end}) exceeds payload size ({data_size})"
                            )
                        ));
                    }
                    acl_check!(self, offset, end, Read, held);
                    fault_point!(self, "get_batched_gen:read");
                    pread_exact_into(file, HEADER_SIZE + offset, buf)?;
                }
                Ok(())
            }
            #[cfg(not(any(unix, windows)))]
            {
                let mut guard = self.write_lock_read()?;
                let file = &mut guard.0;
                let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
                fault_point!(self, "get_batched_gen");
                while let Some((offset, buf)) = f() {
                    let end = offset.checked_add(buf.len() as u64).ok_or_else(|| {
                        io_error!(
                            InvalidInput,
                            "get_batched_gen: offset + buf.len() overflows u64"
                        )
                    })?;
                    if end > data_size {
                        return Err(io_error!(
                            InvalidInput,
                            format!(
                                "get_batched_gen: end ({end}) exceeds payload size ({data_size})"
                            )
                        ));
                    }
                    acl_check!(self, offset, end, Read, held);
                    fault_point!(self, "get_batched_gen:read");
                    file.seek(SeekFrom::Start(HEADER_SIZE + offset))?;
                    file.read_exact(buf)?;
                }
                Ok(())
            }
        }
    }
}

#[cfg(feature = "expensive-slice-access-control")]
pub use inner::*;

// Without the feature the allocator wiring still compiles: these fold to nothing
// and inline away, keeping the build byte-identical to one that never mentioned
// access control. Only the allocators (the `alloc` feature) call them.
#[cfg(all(feature = "alloc", not(feature = "expensive-slice-access-control")))]
impl crate::BStack {
    #[inline]
    pub(crate) fn acl_claim_alloc(&self) {}

    #[inline]
    #[allow(dead_code)] // metadata-marking hook; see the gated sibling
    pub(crate) fn acl_mark_alloc(&self, _offset: u64, _len: u64) -> std::io::Result<()> {
        Ok(())
    }

    #[inline]
    pub(crate) fn acl_reclaimable(&self, _offset: u64, _len: u64) -> std::io::Result<()> {
        Ok(())
    }

    #[inline]
    pub(crate) fn acl_reclaim(&self, _offset: u64, _len: u64) -> std::io::Result<()> {
        Ok(())
    }
}

// Allocator-metadata I/O, generated by `meta_dispatch!` (defined at the module
// root). One definition per method serves every feature configuration.
#[cfg(all(feature = "alloc", feature = "set"))]
impl crate::BStack {
    meta_dispatch!(
        #[allow(dead_code)]
        [set / set_as]
        fn meta_set(offset: u64, data: impl AsRef<[u8]>) -> std::io::Result<()>
    );

    /// Read a little-endian `u64` of allocator metadata at `offset`. See the
    /// `meta_dispatch!`-generated siblings; the read is followed by a decode, so
    /// this one is written out rather than generated.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn meta_read_u64(&self, offset: u64) -> std::io::Result<u64> {
        let mut buf = [0u8; 8];
        #[cfg(feature = "expensive-slice-access-control")]
        self.get_into_as(crate::BStackAccessAuthorities::ALLOC, offset, &mut buf)?;
        #[cfg(not(feature = "expensive-slice-access-control"))]
        self.get_into(offset, &mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }
}

#[cfg(all(feature = "alloc", feature = "set", feature = "atomic"))]
impl crate::BStack {
    meta_dispatch!(
        [cross_exchange / cross_exchange_as]
        fn meta_cross_exchange(a: u64, b: u64, n: u64) -> std::io::Result<()>
    );
    meta_dispatch!(
        #[allow(dead_code)]
        [cas / cas_as]
        fn meta_cas(offset: u64, old: impl AsRef<[u8]>, new: impl AsRef<[u8]>) -> std::io::Result<bool>
    );

    /// `process_gen` run as the allocator. Generic, so written out rather than
    /// generated; see the `meta_dispatch!` siblings.
    #[inline]
    pub(crate) fn meta_process_gen<'a, F>(&self, f: F) -> std::io::Result<()>
    where
        F: FnMut() -> Option<crate::BStackGenOp<'a>>,
    {
        #[cfg(feature = "expensive-slice-access-control")]
        {
            self.process_gen_as(crate::BStackAccessAuthorities::ALLOC, f)
        }
        #[cfg(not(feature = "expensive-slice-access-control"))]
        {
            self.process_gen(f)
        }
    }

    /// `inplace_gen` run as the allocator. See [`meta_process_gen`](Self::meta_process_gen).
    #[inline]
    pub(crate) fn meta_inplace_gen<'a, F>(&self, f: F) -> std::io::Result<()>
    where
        F: FnMut(std::io::Result<()>) -> Option<crate::BStackGenOp<'a>>,
    {
        #[cfg(feature = "expensive-slice-access-control")]
        {
            self.inplace_gen_as(crate::BStackAccessAuthorities::ALLOC, f)
        }
        #[cfg(not(feature = "expensive-slice-access-control"))]
        {
            self.inplace_gen(f)
        }
    }

    /// `set_batched` of allocator metadata. See [`meta_process_gen`](Self::meta_process_gen).
    #[inline]
    pub(crate) fn meta_set_batched<I, D>(&self, writes: I) -> std::io::Result<()>
    where
        I: IntoIterator<Item = (u64, D)>,
        D: AsRef<[u8]>,
    {
        #[cfg(feature = "expensive-slice-access-control")]
        {
            self.set_batched_as(crate::BStackAccessAuthorities::ALLOC, writes)
        }
        #[cfg(not(feature = "expensive-slice-access-control"))]
        {
            self.set_batched(writes)
        }
    }
}

#[cfg(all(test, feature = "expensive-slice-access-control"))]
mod acl_tests {
    use crate::*;
    use std::io;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn mk() -> (BStack, PathBuf) {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("bstack_acl_{pid}_{id}.bin"));
        let _ = std::fs::remove_file(&path);
        (BStack::open(&path).unwrap(), path)
    }

    struct Guard(PathBuf);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn seed(s: &BStack, n: usize) {
        s.push(vec![0xAAu8; n]).unwrap();
    }

    #[test]
    fn unprotected_stack_is_transparent() {
        let (s, p) = mk();
        let _g = Guard(p);
        seed(&s, 64);
        assert_eq!(s.access_at(10), BStackAccess::All);
        s.set(0, [1, 2, 3]).unwrap();
        assert_eq!(s.get(0, 3).unwrap(), [1, 2, 3]);
        s.discard(8).unwrap();
        assert_eq!(s.len().unwrap(), 56);
    }

    #[test]
    fn prot_range_needs_guard_on_every_axis() {
        let (s, p) = mk();
        let _g = Guard(p);
        seed(&s, 64);
        let prot = s.take_protection().unwrap();
        s.protect_as(&prot, 16, 16, BStackAccess::Prot).unwrap();
        assert_eq!(s.access_at(20), BStackAccess::Prot);

        // Tokenless is denied on read, write, and (via the tail) truncate.
        assert_eq!(
            s.set(16, [0; 4]).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(
            s.get(16, 20).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(
            s.discard(56).unwrap_err().kind(), // would cut into [16,32)
            io::ErrorKind::PermissionDenied
        );

        // The guard token satisfies all three.
        s.set_as(&prot, 16, [7u8; 4]).unwrap();
        assert_eq!(s.get_as(&prot, 16, 20).unwrap(), [7, 7, 7, 7]);
        s.discard_as(&prot, 56).unwrap();
        assert_eq!(s.len().unwrap(), 8);
    }

    #[test]
    fn alloc_and_guard_are_incomparable() {
        let (s, p) = mk();
        let _g = Guard(p);
        seed(&s, 64);
        let alloc = s.take_alloc_authority().unwrap();
        let prot = s.take_protection().unwrap();
        s.protect_as(&alloc, 0, 16, BStackAccess::Alloc).unwrap();

        // The guard cannot touch an Alloc range, nor re-mode it.
        assert_eq!(
            s.get_as(&prot, 0, 4).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(
            s.protect_as(&prot, 0, 16, BStackAccess::Prot)
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
        // The allocator can.
        s.set_as(&alloc, 0, [1u8; 4]).unwrap();
        assert_eq!(s.get_as(&alloc, 0, 4).unwrap(), [1, 1, 1, 1]);
    }

    #[test]
    fn readonly_denies_writes_only() {
        let (s, p) = mk();
        let _g = Guard(p);
        seed(&s, 32);
        s.protect_as((), 0, 16, BStackAccess::ReadOnly).unwrap();
        assert_eq!(s.get(0, 4).unwrap().len(), 4); // reads pass
        assert_eq!(
            s.set(0, [0u8; 4]).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(
            s.zero(0, 4).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
    }

    #[test]
    fn locked_denies_reads() {
        let (s, p) = mk();
        let _g = Guard(p);
        seed(&s, 32);
        s.protect_as((), 0, 16, BStackAccess::Locked).unwrap();
        assert_eq!(
            s.get(0, 4).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(
            s.peek(0).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
    }

    #[test]
    fn tokenless_protect_only_tightens_all() {
        let (s, p) = mk();
        let _g = Guard(p);
        seed(&s, 32);
        s.protect_as((), 0, 16, BStackAccess::ReadOnly).unwrap();
        // Re-arming a now-non-All range without a token is denied.
        assert_eq!(
            s.protect_as((), 0, 16, BStackAccess::All)
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
    }

    #[test]
    fn tokens_are_one_shot() {
        let (s, p) = mk();
        let _g = Guard(p);
        assert!(s.take_protection().is_some());
        assert!(s.take_protection().is_none());
        assert!(s.take_alloc_authority().is_some());
        assert!(s.take_alloc_authority().is_none());
    }

    #[test]
    fn foreign_token_grants_nothing() {
        let (s1, p1) = mk();
        let _g1 = Guard(p1);
        let (s2, p2) = mk();
        let _g2 = Guard(p2);
        seed(&s1, 32);
        let foreign = s2.take_protection().unwrap();
        s1.protect_as((), 0, 16, BStackAccess::Prot).unwrap();
        // A token minted from s2 is treated as tokenless on s1.
        assert_eq!(
            s1.get_as(&foreign, 0, 4).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
    }

    #[test]
    fn append_checks_its_target_region() {
        let (s, p) = mk();
        let _g = Guard(p);
        seed(&s, 16);
        // Arm the region a push would land in, before its bytes exist.
        s.protect_as((), 16, 16, BStackAccess::Locked).unwrap();
        assert_eq!(
            s.push(vec![0u8; 8]).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(
            s.extend(8).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(s.len().unwrap(), 16); // nothing appended
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn atomic_ops_respect_protection() {
        let (s, p) = mk();
        let _g = Guard(p);
        seed(&s, 64);
        let prot = s.take_protection().unwrap();
        s.protect_as(&prot, 16, 16, BStackAccess::Prot).unwrap();
        let denied = |k: io::ErrorKind| assert_eq!(k, io::ErrorKind::PermissionDenied);

        // In-place mutators over the protected range are denied tokenless.
        denied(s.swap(16, [0u8; 4]).unwrap_err().kind());
        denied(s.cas(16, [0xAAu8; 4], [0u8; 4]).unwrap_err().kind());
        denied(s.process(16, 20, |b| b.fill(0)).unwrap_err().kind());
        // copy: source read denied.
        denied(s.copy(16, 40, 4).unwrap_err().kind());
        // copy: destination write denied.
        denied(s.copy(40, 16, 4).unwrap_err().kind());
        // cross_exchange touching the range denied.
        denied(s.cross_exchange(16, 40, 4).unwrap_err().kind());
        // A tail replace that reaches into the range is denied.
        denied(s.atrunc(56, []).unwrap_err().kind()); // truncates [8, 64) ⊇ [16,32)
        // Batched read of the range denied.
        denied(s.get_batched(std::iter::once(16..20)).unwrap_err().kind());

        // Outside the protected range, the same ops succeed.
        s.swap(40, [1u8; 4]).unwrap();
        assert_eq!(s.get(40, 44).unwrap(), [1, 1, 1, 1]);
    }

    #[test]
    fn authorized_slice_reaches_prot_region() {
        let (s, p) = mk();
        let _g = Guard(p);
        let alloc = crate::LinearBStackAllocator::new(s);
        let mut slice = alloc.alloc(32).unwrap();
        let prot = alloc.stack().take_protection().unwrap();
        slice.protect_as(&prot, BStackAccess::Prot).unwrap();
        // Without authority, the slice's own I/O is denied.
        assert_eq!(
            slice.read().unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        // Grant the slice the guard authority; its I/O now reaches the region.
        slice.authorize(&prot);
        slice.write([7u8; 4]).unwrap();
        let bytes = slice.read().unwrap();
        assert_eq!(bytes.len(), 32);
        assert_eq!(&bytes[..4], &[7, 7, 7, 7]);
    }

    #[test]
    fn owned_slice_protect_forwards_to_stack() {
        let (s, p) = mk();
        let _g = Guard(p);
        let alloc = crate::LinearBStackAllocator::new(s);
        let mut slice = alloc.alloc(32).unwrap();
        // Arm the allocation as ReadOnly through the owned handle.
        slice.protect(BStackAccess::ReadOnly).unwrap();
        assert_eq!(
            alloc.stack().access_at(slice.start()),
            BStackAccess::ReadOnly
        );
        // Reads through the slice pass; writes are denied by the forwarded policy.
        assert!(slice.read().is_ok());
        assert_eq!(
            slice.write([0u8; 4]).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
    }

    #[test]
    fn owned_slice_protect_as_with_guard() {
        let (s, p) = mk();
        let _g = Guard(p);
        let alloc = crate::LinearBStackAllocator::new(s);
        let slice = alloc.alloc(32).unwrap();
        let prot = alloc.stack().take_protection().unwrap();
        // Arm as Prot; only the guard token may then read or write it.
        slice.protect_as(&prot, BStackAccess::Prot).unwrap();
        assert_eq!(
            slice.read().unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        // The stack's token-carrying entry points still reach it.
        let stack = alloc.stack();
        stack.set_as(&prot, slice.start(), [9u8; 4]).unwrap();
        assert_eq!(
            stack
                .get_as(&prot, slice.start(), slice.start() + 4)
                .unwrap(),
            [9, 9, 9, 9]
        );
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn set_batched_checks_every_block() {
        let (s, p) = mk();
        let _g = Guard(p);
        seed(&s, 64);
        s.protect_as((), 32, 8, BStackAccess::ReadOnly).unwrap();
        // One block lands in the ReadOnly region → the whole batch is refused.
        assert_eq!(
            s.set_batched([(0u64, vec![1u8; 4]), (32u64, vec![2u8; 4])])
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
        // The permitted block was not applied (checks run before the journal).
        assert_eq!(s.get(0, 4).unwrap(), [0xAA, 0xAA, 0xAA, 0xAA]);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn authorized_slice_atomic_ops_reach_prot() {
        let (s, p) = mk();
        let _g = Guard(p);
        let alloc = crate::LinearBStackAllocator::new(s);
        let mut slice = alloc.alloc(32).unwrap();
        let prot = alloc.stack().take_protection().unwrap();
        slice.protect_as(&prot, BStackAccess::Prot).unwrap();
        // Tokenless slice process (a write) is denied.
        assert_eq!(
            slice
                .as_slice_mut()
                .process(|b| b.fill(9))
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
        // With authority the atomic slice ops reach the Prot region.
        slice.authorize(&prot);
        slice.as_slice_mut().process(|b| b.fill(1)).unwrap();
        assert_eq!(&slice.read().unwrap()[..4], &[1, 1, 1, 1]);
    }

    #[test]
    fn authorized_stack_compound_ops_reach_prot() {
        let (s, p) = mk();
        let _g = Guard(p);
        seed(&s, 64);
        let prot = s.take_protection().unwrap();
        // Interior range for the in-place writes; tail range for the replacements.
        s.protect_as(&prot, 16, 16, BStackAccess::Prot).unwrap();
        s.protect_as(&prot, 48, 16, BStackAccess::Prot).unwrap();
        let denied = |k: io::ErrorKind| assert_eq!(k, io::ErrorKind::PermissionDenied);

        // swap over [16,20): tokenless denied, `swap_as` reaches it.
        denied(s.swap(16, [0u8; 4]).unwrap_err().kind());
        s.swap_as(&prot, 16, [1u8; 4]).unwrap();
        assert_eq!(s.get_as(&prot, 16, 20).unwrap(), [1, 1, 1, 1]);

        // swap_into over [16,20): reads back the bytes just written.
        let mut buf = [2u8; 4];
        denied(s.swap_into(16, &mut [0u8; 4]).unwrap_err().kind());
        s.swap_into_as(&prot, 16, &mut buf).unwrap();
        assert_eq!(buf, [1, 1, 1, 1]);
        assert_eq!(s.get_as(&prot, 16, 20).unwrap(), [2, 2, 2, 2]);

        // set_batched touching [16,20): tokenless denied, `set_batched_as` succeeds.
        denied(
            s.set_batched(std::iter::once((16u64, [3u8; 4])))
                .unwrap_err()
                .kind(),
        );
        s.set_batched_as(&prot, std::iter::once((16u64, [3u8; 4])))
            .unwrap();
        assert_eq!(s.get_as(&prot, 16, 20).unwrap(), [3, 3, 3, 3]);

        // Tail replacements reaching [48,64) via a 20-byte tail (touches [44,64)).
        denied(s.splice(20, []).unwrap_err().kind());
        let removed = s.splice_as(&prot, 20, [7u8; 20]).unwrap();
        assert_eq!(removed.len(), 20);
        assert_eq!(s.get_as(&prot, 44, 64).unwrap(), [7u8; 20]);

        let mut old = [0u8; 20];
        denied(s.splice_into(&mut [0u8; 20], []).unwrap_err().kind());
        s.splice_into_as(&prot, &mut old, [8u8; 20]).unwrap();
        assert_eq!(old, [7u8; 20]);
        assert_eq!(s.get_as(&prot, 44, 64).unwrap(), [8u8; 20]);

        denied(s.replace(20, |b| b.to_vec()).unwrap_err().kind());
        s.replace_as(&prot, 20, |b| b.iter().map(|x| x + 1).collect())
            .unwrap();
        assert_eq!(s.get_as(&prot, 44, 64).unwrap(), [9u8; 20]);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn authorized_stack_append_ops_reach_prot() {
        // Appends land in a Prot window armed just past the tail: tokenless denied,
        // `_as` reaches it. Chained so each op fills the next slice of the window.
        let (s, p) = mk();
        let _g = Guard(p);
        seed(&s, 48);
        let prot = s.take_protection().unwrap();
        s.protect_as(&prot, 48, 32, BStackAccess::Prot).unwrap();
        let denied = |k: io::ErrorKind| assert_eq!(k, io::ErrorKind::PermissionDenied);

        denied(s.push([1u8; 4]).unwrap_err().kind());
        assert_eq!(s.push_as(&prot, [1u8; 4]).unwrap(), 48);

        denied(s.extend(4).unwrap_err().kind());
        assert_eq!(s.extend_as(&prot, 4).unwrap(), 52);

        denied(s.extend_sparse([2u8; 2], 4).unwrap_err().kind());
        assert_eq!(s.extend_sparse_as(&prot, [2u8; 2], 4).unwrap(), 56);

        denied(
            s.extend_sparse_batched(std::iter::once((0u64, [3u8; 2])), 4)
                .unwrap_err()
                .kind(),
        );
        assert_eq!(
            s.extend_sparse_batched_as(&prot, std::iter::once((0u64, [3u8; 2])), 4)
                .unwrap(),
            60
        );

        denied(s.try_extend(64, [4u8; 4]).unwrap_err().kind());
        assert!(s.try_extend_as(&prot, 64, [4u8; 4]).unwrap());

        denied(s.try_extend_zeros(68, 4).unwrap_err().kind());
        assert!(s.try_extend_zeros_as(&prot, 68, 4).unwrap());

        denied(s.try_extend_sparse(72, [5u8; 2], 4).unwrap_err().kind());
        assert!(s.try_extend_sparse_as(&prot, 72, [5u8; 2], 4).unwrap());

        denied(
            s.try_extend_sparse_batched(76, std::iter::once((0u64, [6u8; 2])), 4)
                .unwrap_err()
                .kind(),
        );
        assert!(
            s.try_extend_sparse_batched_as(&prot, 76, std::iter::once((0u64, [6u8; 2])), 4)
                .unwrap()
        );
        assert_eq!(s.len().unwrap(), 80);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn authorized_stack_removal_ops_reach_prot() {
        // Tail removals reaching a Prot tail are denied tokenless and succeed with
        // the token. Fresh stack per op keeps the protected region well-defined.
        let denied = |k: io::ErrorKind| assert_eq!(k, io::ErrorKind::PermissionDenied);
        {
            let (s, p) = mk();
            let _g = Guard(p);
            seed(&s, 64);
            let prot = s.take_protection().unwrap();
            s.protect_as(&prot, 48, 16, BStackAccess::Prot).unwrap();
            denied(s.pop(20).unwrap_err().kind());
            assert_eq!(s.pop_as(&prot, 20).unwrap().len(), 20);
            assert_eq!(s.len().unwrap(), 44);
        }
        {
            let (s, p) = mk();
            let _g = Guard(p);
            seed(&s, 64);
            let prot = s.take_protection().unwrap();
            s.protect_as(&prot, 48, 16, BStackAccess::Prot).unwrap();
            let mut buf = [0u8; 20];
            denied(s.pop_into(&mut [0u8; 20]).unwrap_err().kind());
            s.pop_into_as(&prot, &mut buf).unwrap();
            assert_eq!(s.len().unwrap(), 44);
        }
        {
            // atrunc actually rewrites the tail, so its check matters most here.
            let (s, p) = mk();
            let _g = Guard(p);
            seed(&s, 64);
            let prot = s.take_protection().unwrap();
            s.protect_as(&prot, 48, 16, BStackAccess::Prot).unwrap();
            denied(s.atrunc(20, [1u8; 4]).unwrap_err().kind());
            s.atrunc_as(&prot, 20, [1u8; 4]).unwrap();
            assert_eq!(s.len().unwrap(), 48);
        }
        {
            let (s, p) = mk();
            let _g = Guard(p);
            seed(&s, 64);
            let prot = s.take_protection().unwrap();
            s.protect_as(&prot, 48, 16, BStackAccess::Prot).unwrap();
            denied(s.try_discard(64, 20).unwrap_err().kind());
            assert!(s.try_discard_as(&prot, 64, 20).unwrap());
            assert_eq!(s.len().unwrap(), 44);
        }
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn authorized_stack_read_ops_reach_prot() {
        let (s, p) = mk();
        let _g = Guard(p);
        seed(&s, 64);
        let prot = s.take_protection().unwrap();
        s.protect_as(&prot, 16, 16, BStackAccess::Prot).unwrap();
        s.set_as(&prot, 16, [5u8; 16]).unwrap();
        let denied = |k: io::ErrorKind| assert_eq!(k, io::ErrorKind::PermissionDenied);

        // peek reads from offset to end; starting inside the Prot region is denied.
        denied(s.peek(16).unwrap_err().kind());
        assert_eq!(&s.peek_as(&prot, 16).unwrap()[..4], &[5, 5, 5, 5]);

        let mut b = [0u8; 4];
        denied(s.peek_into(16, &mut [0u8; 4]).unwrap_err().kind());
        s.peek_into_as(&prot, 16, &mut b).unwrap();
        assert_eq!(b, [5, 5, 5, 5]);

        denied(s.get_batched(std::iter::once(16..20)).unwrap_err().kind());
        assert_eq!(
            s.get_batched_as(&prot, std::iter::once(16..20)).unwrap()[0],
            vec![5, 5, 5, 5]
        );

        let mut b2 = [0u8; 4];
        {
            let mut dbuf = [0u8; 4];
            denied(
                s.get_batched_into(std::iter::once((16u64, &mut dbuf[..])))
                    .unwrap_err()
                    .kind(),
            );
        }
        s.get_batched_into_as(&prot, std::iter::once((16u64, &mut b2[..])))
            .unwrap();
        assert_eq!(b2, [5, 5, 5, 5]);

        // Lending closure: yield a raw-pointer slice, as the other gen tests do.
        let mut gbuf = [0u8; 4];
        let ptr = gbuf.as_mut_ptr();
        let mut called = false;
        denied(
            s.get_batched_gen(|| {
                if called {
                    None
                } else {
                    called = true;
                    Some((16u64, unsafe { std::slice::from_raw_parts_mut(ptr, 4) }))
                }
            })
            .unwrap_err()
            .kind(),
        );
        let mut called2 = false;
        s.get_batched_gen_as(&prot, || {
            if called2 {
                None
            } else {
                called2 = true;
                Some((16u64, unsafe { std::slice::from_raw_parts_mut(ptr, 4) }))
            }
        })
        .unwrap();
        assert_eq!(gbuf, [5, 5, 5, 5]);
    }

    #[test]
    fn authorized_stack_resize_ops_reach_prot() {
        let denied = |k: io::ErrorKind| assert_eq!(k, io::ErrorKind::PermissionDenied);
        // resize shrink reaching a Prot tail.
        {
            let (s, p) = mk();
            let _g = Guard(p);
            seed(&s, 64);
            let prot = s.take_protection().unwrap();
            s.protect_as(&prot, 48, 16, BStackAccess::Prot).unwrap();
            denied(s.resize(44).unwrap_err().kind());
            assert_eq!(s.resize_as(&prot, 44).unwrap(), 64);
            assert_eq!(s.len().unwrap(), 44);
        }
        // resize grow into a Prot region armed past the tail.
        {
            let (s, p) = mk();
            let _g = Guard(p);
            seed(&s, 48);
            let prot = s.take_protection().unwrap();
            s.protect_as(&prot, 48, 16, BStackAccess::Prot).unwrap();
            denied(s.resize(60).unwrap_err().kind());
            assert_eq!(s.resize_as(&prot, 60).unwrap(), 48);
            assert_eq!(s.len().unwrap(), 60);
        }
        // ensure grow into a Prot region.
        {
            let (s, p) = mk();
            let _g = Guard(p);
            seed(&s, 48);
            let prot = s.take_protection().unwrap();
            s.protect_as(&prot, 48, 16, BStackAccess::Prot).unwrap();
            denied(s.ensure(60).unwrap_err().kind());
            assert_eq!(s.ensure_as(&prot, 60).unwrap(), 48);
            assert_eq!(s.len().unwrap(), 60);
        }
    }

    #[test]
    fn merge_requires_matching_authority() {
        let (s, p) = mk();
        let _g = Guard(p);
        let alloc = crate::LinearBStackAllocator::new(s);
        let owned = alloc.alloc(32).unwrap();
        let full = owned.as_slice();
        let mut left = full.subslice(0, 16);
        let right = full.subslice(16, 32);
        // Same (NONE) authority: the adjacent subslices merge.
        assert!(left.merge_adjacent(&right).is_some());
        // Grant `left` an authority; now the authorities differ and merge refuses.
        let prot = alloc.stack().take_protection().unwrap();
        left.authorize(&prot);
        assert!(left.merge(&right).is_none());
        assert!(left.merge_adjacent(&right).is_none());
    }

    #[test]
    fn allocator_constructor_burns_alloc_authority() {
        // Every allocator claims the alloc-authority mint on construction, so no
        // external caller can obtain `Alloc` authority over its arena.
        let (s, p) = mk();
        let _g = Guard(p);
        let alloc = crate::LinearBStackAllocator::new(s);
        assert!(alloc.stack().take_alloc_authority().is_none());
        // The guard mint is independent and still available.
        assert!(alloc.stack().take_protection().is_some());

        let (s2, p2) = mk();
        let _g2 = Guard(p2);
        let ff = crate::FirstFitBStackAllocator::new(s2).unwrap();
        assert!(ff.stack().take_alloc_authority().is_none());
    }

    #[test]
    fn dealloc_refuses_protected_region_then_frees_once_cleared() {
        let (s, p) = mk();
        let _g = Guard(p);
        let alloc = crate::LinearBStackAllocator::new(s);
        let slice = alloc.alloc(32).unwrap();
        let start = slice.start();
        let len = slice.len();
        let prot = alloc.stack().take_protection().unwrap();
        slice.protect_as(&prot, BStackAccess::Prot).unwrap();

        // Freeing a region that carries caller policy is refused, and the handle
        // comes back intact rather than being consumed.
        let err = alloc.dealloc(slice).unwrap_err();
        assert_eq!(err.source.kind(), io::ErrorKind::PermissionDenied);
        let slice = err.into_handle().expect("region survives a refused free");

        // The guard holder clears the policy, and the free now succeeds.
        alloc
            .stack()
            .protect_as(&prot, start, len, BStackAccess::All)
            .unwrap();
        alloc.dealloc(slice).unwrap();
        assert_eq!(alloc.stack().len().unwrap(), 0);
    }

    #[test]
    fn dealloc_bulk_refuses_batch_with_any_protected_handle() {
        use crate::BStackBulkAllocator;
        let (s, p) = mk();
        let _g = Guard(p);
        let alloc = crate::LinearBStackAllocator::new(s);
        let a = alloc.alloc(16).unwrap();
        let b = alloc.alloc(16).unwrap();
        let prot = alloc.stack().take_protection().unwrap();
        // Arm just one of the two allocations.
        b.protect_as(&prot, BStackAccess::ReadOnly).unwrap();

        let err = alloc.dealloc_bulk([a, b]).unwrap_err();
        assert_eq!(err.source.kind(), io::ErrorKind::PermissionDenied);
        // The whole batch is refused up front, so both handles are returned.
        assert_eq!(err.into_handles().len(), 2);
    }
}
