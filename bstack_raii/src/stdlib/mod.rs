//! `bstack_raii`'s standard library: small, ergonomic handle types built
//! **entirely** on the crate's ownership primitives ([`crate::BStackOwned`],
//! [`crate::BStackRef`], [`crate::BStackRc`], the [`crate::TryCloneIn`] /
//! [`crate::BStackDrop`] contracts) and the `#[bstack_block]` macro.
//!
//! Nothing here reaches below those primitives — the stdlib is a *consumer* of
//! the same public surface downstream crates use, so each type doubles as a
//! worked example of composing the ownership model. It is deliberately kept
//! separate from the low-level modules: the runtime and macro define *what a
//! block is*; the stdlib defines *convenient ways to hold one*.
//!
//! ## Contents
//!
//! | Type                | Rust analogue        | What it holds                          |
//! |---------------------|----------------------|----------------------------------------|
//! | [`BStackCow<T>`]    | [`std::borrow::Cow`] | either a borrowed [`crate::BStackRef`] or an owned [`crate::BStackOwned`] block, deep-copying on first write. |
//! | [`BStackBox<T>`]    | [`std::boxed::Box`]  | a single owned [`Pod`](bytemuck::Pod) value in its own block — the macro-free way to own a bare scalar/POD struct. |

mod boxed;
mod cow;

pub use boxed::{BStackBox, BoxOnDisk};
pub use cow::BStackCow;
