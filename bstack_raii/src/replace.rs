//! [`ReplaceError<V>`]: the error a generated `replace_<field>` mutator returns.

use std::error::Error;
use std::fmt;
use std::io;

/// The error a [`#[bstack_mut]`](crate::bstack_block) `replace_<field>` mutator
/// returns when the swap fails partway through.
///
/// `replace_<field>` **consumes** the value you hand it. A bare `io::Result`
/// would then *lose* that value on an I/O failure — its on-disk block would be
/// neither linked into the field nor returned, an unreachable orphan. So a failed
/// `replace_` returns this instead, handing the still-valid value back in
/// [`value`](Self::value) — the same region-hand-back contract as bstack's
/// `BStackAllocError` and [`ForeignAllocError`](crate::registry::ForeignAllocError).
///
/// The *old* value is never at risk: the swap is a single crash-atomic `set`, so
/// on failure the field still holds it (it is simply not moved out).
///
/// Implements [`std::error::Error`] (delegating [`Display`](fmt::Display) to
/// [`source`](Self::source)).
pub struct ReplaceError<V> {
    /// The underlying I/O error that caused the swap to fail.
    pub source: io::Error,
    /// The value that was to be installed, handed back if it survived.
    ///
    /// * `Some` — recovered: the value is intact and yours again. Re-attach it
    ///   (retry `replace_`) or free it — dropping it as-is may leak, since a bare
    ///   handle is unrooted (see the crate's *moved-out-is-unrooted* rule).
    /// * `None` — unrecoverable here: a post-commit reconstruction of the *old*
    ///   value failed after the new one was already installed, so it is the old
    ///   block that is now reachable only through crash-recovery / the WAL. The
    ///   new value is safely in the field. Treat `None` as "not recoverable
    ///   here," not as impossible.
    pub value: Option<V>,
}

impl<V> ReplaceError<V> {
    /// An error that hands the still-valid `value` back to the caller.
    #[inline]
    pub fn recovered(source: io::Error, value: V) -> Self {
        Self {
            source,
            value: Some(value),
        }
    }

    /// An error whose value could not be recovered here (see [`value`](Self::value)).
    #[inline]
    pub fn lost(source: io::Error) -> Self {
        Self {
            source,
            value: None,
        }
    }

    /// Discard the recovered value (if any) and take just the underlying
    /// `io::Error`. Explicit, because dropping a recovered value may leak.
    #[inline]
    pub fn into_source(self) -> io::Error {
        self.source
    }
}

// Manual, so `V` need not be `Debug` (the handed-back handles generally aren't).
impl<V> fmt::Debug for ReplaceError<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplaceError")
            .field("source", &self.source)
            .field("value", &self.value.as_ref().map(|_| "..."))
            .finish()
    }
}

impl<V> fmt::Display for ReplaceError<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.source, f)
    }
}

impl<V> Error for ReplaceError<V> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
