//! Cross-allocator fuzz suites.
//!
//! Four drivers over one shared machinery ([`common`]), in a 2×2 grid — the
//! initialised vs. uninitialised allocation API, each run on the happy path and
//! (under `fault-injection`) under injected torn-write faults:
//!
//! |            | happy path      | fault-injection      |
//! |------------|-----------------|----------------------|
//! | `alloc`    | [`init`]        | [`init_fault`]       |
//! | `_uninit`  | [`uninit`]      | [`uninit_fault`]     |
//!
//! The fault suites are compiled only in debug builds with the `fault-injection`
//! feature; the happy-path suites always run. See [`common`] for the temp-file
//! management, payload model, operation generator, and per-allocator constructors
//! every driver shares.

pub(crate) mod common;

mod init;
mod uninit;

#[cfg(all(debug_assertions, feature = "fault-injection"))]
mod init_fault;
#[cfg(all(debug_assertions, feature = "fault-injection"))]
mod uninit_fault;
