//! [`TryClone`]: a fallible clone for handles whose duplication touches disk.
//!
//! [`crate::BStackRc`] and [`crate::BStackWeak`] cannot implement `Clone`,
//! because duplicating them must atomically bump an on-disk refcount, which can
//! fail with an [`io::Error`]. `Clone::clone` has no way to report that, so this
//! layer exposes an explicit fallible clone instead.

use std::io;

/// Duplicate `self`, performing any fallible I/O the duplication requires.
pub trait TryClone: Sized {
    fn try_clone(&self) -> io::Result<Self>;
}
