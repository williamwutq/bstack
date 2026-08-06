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
//! | [`BStackLinkedList<T>`] | [`std::collections::LinkedList`] | an owned doubly-linked list of block values (non-intrusive, single-ref nodes). Prefer [`BStackDeque`] / [`crate::BStackBlockVec`] unless you need O(1) end/splice ops. |
//! | [`BStackDeque<T>`]  | [`std::collections::VecDeque`] | an owned double-ended queue: a contiguous ring of value refs (no per-element pointer chasing), O(1) amortized push/pop at both ends. |
//! | [`BStackHashMap<K, V>`] | [`std::collections::HashMap`] | an owned open-addressing map from a [`Pod`](bytemuck::Pod) key to a block value — keyed lookup without a linear scan. |
//! | [`BStackBTreeMap<K, V>`] | [`std::collections::BTreeMap`] | an owned **ordered** map: a copy-on-write B-tree (wide contiguous nodes, few seeks per lookup) with sorted iteration. Keys are `Pod + Ord`. |
//! | [`BStackString`]    | [`std::string::String`] | a standalone owned, growable UTF-8 string block — the first-class way to own text (a deque element, a map value). |

mod boxed;
mod cow;
mod deque;
mod hash;
mod list;
mod map;
mod string;
mod tree;
mod util;

pub use boxed::{BStackBox, BoxOnDisk};
pub use cow::BStackCow;
pub use deque::{BStackDeque, DequeOnDisk};
pub use list::{BStackLinkedList, ListOnDisk, NodeOnDisk};
pub use map::{BStackHashMap, MapOnDisk};
pub use string::{BStackString, StringOnDisk};
pub use tree::{BStackBTreeMap, TreeOnDisk};
