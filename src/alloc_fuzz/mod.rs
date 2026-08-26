//! Cross-allocator fuzz suites.
//!
//! Drivers over one shared machinery ([`common`]), in a grid — the initialised,
//! uninitialised, and in-place-resize allocation APIs, each run on the happy path
//! and (under `fault-injection`) under injected torn-write faults:
//!
//! |               | happy path   | fault-injection      |
//! |---------------|--------------|----------------------|
//! | `alloc`       | [`init`]     | [`init_fault`]       |
//! | `_uninit`     | [`uninit`]   | [`uninit_fault`]     |
//! | `_inplace`    | [`inplace`]  | [`inplace_fault`]    |
//!
//! The fault suites are compiled only in debug builds with the `fault-injection`
//! feature; the happy-path suites always run. The in-place suites cover only the
//! three allocators that implement
//! [`BStackInPlaceResizeAllocator`](crate::alloc::BStackInPlaceResizeAllocator)
//! (first-fit, ghost-tree, segregated). See [`common`] for the temp-file
//! management, payload model, operation generator, and per-allocator constructors
//! every driver shares.

pub(crate) mod common;

mod init;
mod inplace;
mod uninit;

#[cfg(all(debug_assertions, feature = "fault-injection"))]
mod init_fault;
#[cfg(all(debug_assertions, feature = "fault-injection"))]
mod inplace_fault;
#[cfg(all(debug_assertions, feature = "fault-injection"))]
mod uninit_fault;
