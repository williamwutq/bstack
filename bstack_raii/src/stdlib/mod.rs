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
//! | [`BStackCountingBloomFilter<K>`] | (Bloom filter) | a probabilistic set: no false negatives, supports removal; a cheap fast-reject front for exact lookups. |
//! | [`BStackHashSet<K>`] | [`std::collections::HashSet`] | an owned open-addressing set of `Pod` keys, with an embedded Bloom-filter fast-reject front. |
//! | [`BStackBTreeSet<K>`] | [`std::collections::BTreeSet`] | an owned **ordered** set (copy-on-write B-tree, sorted iteration), with an embedded Bloom-filter front. Keys are `Pod + Ord`. |
//! | [`BStackBinaryHeap<K, V>`] | [`std::collections::BinaryHeap`] | an owned priority queue (array-backed binary **min**-heap, pointer-free): `pop` returns the smallest-key entry. Keys are `Pod + Ord`. |

mod bloom;
mod boxed;
mod btreeset;
mod cow;
mod deque;
mod hash;
mod hashset;
mod heap;
mod list;
mod map;
mod string;
mod tree;
mod util;

pub use bloom::{BStackCountingBloomFilter, BloomOnDisk};
pub use boxed::{BStackBox, BoxOnDisk};
pub use btreeset::{BStackBTreeSet, BTreeSetIter, TreeSetOnDisk};
pub use cow::BStackCow;
pub use deque::{BStackDeque, DequeIter, DequeOnDisk};
pub use hashset::{BStackHashSet, HashSetIter, HashSetOnDisk};
pub use heap::{BStackBinaryHeap, HeapOnDisk};
pub use list::{BStackLinkedList, ListIter, ListOnDisk, NodeOnDisk};
pub use map::{BStackHashMap, HashMapIter, MapOnDisk};
pub use string::{BStackString, StringOnDisk};
pub use tree::{BStackBTreeMap, BTreeMapIter, TreeOnDisk};
