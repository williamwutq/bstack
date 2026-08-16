//! The shared "sink" fixtures — the cube's axes declared **once**.
//!
//! * Element library: [`Leaf`] (plain block, the universal owned/ref/embed target)
//!   and [`Shared`] (an `rc, weak` block, the universal strong/weak target).
//! * Self-contained kitchen-sinks (build from an allocator alone, own everything they
//!   reference): [`BlockSink`], [`MutSink`], [`EmbedSink`], [`ForeignSelfSink`],
//!   [`RcSink`], [`RcWeakSink`], [`EnumSink`], [`GenSink`].
//! * [`RefWeakSink`] holds `#[bstack_ref]` / `#[bstack_weak]` cells that point at
//!   *external* targets whose lifetime the test manages, so it has no all-in-one
//!   builder — its tests wire targets inline.
#![allow(dead_code)]

use std::io;

use bstack_raii::registry::FileId;
use bstack_raii::{
    BStackBlock, BStackOwned, BStackRaiiAllocator, BStackRc, Foreign, bstack_block, bstack_enum,
};

// ---------------------------------------------------------------------------
// Element library
// ---------------------------------------------------------------------------

/// Plain block — the universal owned / ref / embed target.
#[bstack_block]
pub struct Leaf {
    pub v: u32,
}

/// A reference-counted, weak-observable block — the universal strong / weak target.
#[bstack_block(rc, weak)]
pub struct Shared {
    pub v: u32,
}

// ---------------------------------------------------------------------------
// BlockSink — one field per self-owning (kind × shape) cell (plain block)
// ---------------------------------------------------------------------------

/// A self-contained kitchen-sink: one field per self-owning (kind × shape) cell.
/// Everything it references it also owns, so [`block_sink`] builds it from an
/// allocator alone and its teardown reclaims the whole tree.
#[bstack_block]
pub struct BlockSink {
    pub pod: u64,
    pub tuple_pod: (u32, u64),
    pub arr_pod: [u32; 3],
    pub string: String,
    #[bstack_owned]
    pub owned: Leaf,
    #[bstack_strong]
    pub strong: Shared,
    #[embed]
    pub emb: Leaf,
    #[bstack_owned]
    pub vec_owned: Vec<Leaf>,
    pub vec_pod: Vec<u32>,
    #[bstack_owned]
    pub arr_owned: [Leaf; 2],
    #[bstack_owned]
    pub nested: [[Leaf; 2]; 2],
    // Per-kind vectors / arrays and containers-of-containers (all self-owning).
    #[bstack_strong]
    pub strong_vec: Vec<Shared>,
    #[bstack_strong]
    pub strong_arr: [Shared; 2],
    #[bstack_owned]
    pub vec_of_arr: Vec<[Leaf; 2]>,
    #[bstack_owned]
    pub arr_of_vec: [Vec<Leaf>; 2],
}

/// Build a fully-populated [`BlockSink`]; field values are distinct so a read
/// pinpoints its cell.
pub fn block_sink<A: BStackRaiiAllocator>(a: &A) -> io::Result<BStackOwned<BlockSink>> {
    BlockSink::new(
        a,
        7,
        (1, 2),
        [3, 4, 5],
        "hi",
        Leaf::new(a, 10)?,
        Shared::new(a, 20)?,
        Leaf::new(a, 30)?,
        vec![Leaf::new(a, 40)?, Leaf::new(a, 41)?],
        &[100, 101, 102],
        [Leaf::new(a, 50)?, Leaf::new(a, 51)?],
        [
            [Leaf::new(a, 60)?, Leaf::new(a, 61)?],
            [Leaf::new(a, 62)?, Leaf::new(a, 63)?],
        ],
        vec![Shared::new(a, 70)?, Shared::new(a, 71)?],
        [Shared::new(a, 80)?, Shared::new(a, 81)?],
        vec![[Leaf::new(a, 90)?, Leaf::new(a, 91)?]],
        [
            vec![Leaf::new(a, 92)?],
            vec![Leaf::new(a, 93)?, Leaf::new(a, 94)?],
        ],
    )
}

// ---------------------------------------------------------------------------
// MutSink — the `#[bstack_mut]` self-owning cells
// ---------------------------------------------------------------------------

/// One `#[bstack_mut]` field per self-owning mutable cell: POD scalar (`set_`), POD
/// tuple (`set_`), owned/strong scalar (`replace_`), and an owned array
/// (`replace_<f>_at` + whole `replace_<f>`).
#[bstack_block]
pub struct MutSink {
    #[bstack_mut]
    pub pod: u64,
    #[bstack_mut]
    pub tuple: (u32, u64),
    #[bstack_mut]
    #[bstack_owned]
    pub owned: Leaf,
    #[bstack_mut]
    #[bstack_strong]
    pub strong: Shared,
    #[bstack_mut]
    #[bstack_owned]
    pub arr: [Leaf; 3],
}

pub fn mut_sink<A: BStackRaiiAllocator>(a: &A) -> io::Result<BStackOwned<MutSink>> {
    MutSink::new(
        a,
        1,
        (2, 3),
        Leaf::new(a, 10)?,
        Shared::new(a, 20)?,
        [Leaf::new(a, 30)?, Leaf::new(a, 31)?, Leaf::new(a, 32)?],
    )
}

// ---------------------------------------------------------------------------
// EmbedSink — embedding a whole kitchen-sink block (the "embed of everything")
// ---------------------------------------------------------------------------

#[bstack_block]
pub struct EmbedSink {
    pub tag: u32,
    #[embed]
    pub child: BlockSink,
}

pub fn embed_sink<A: BStackRaiiAllocator>(a: &A) -> io::Result<BStackOwned<EmbedSink>> {
    EmbedSink::new(a, 9, block_sink(a)?)
}

// ---------------------------------------------------------------------------
// Foreign (SELF) — a self-contained owned/opt foreign sink
// ---------------------------------------------------------------------------

/// Owned foreign cells pointing at SELF targets (in the same file), relinquished so
/// the sink solely owns them — teardown frees them via `SELF => home`.
#[bstack_block]
pub struct ForeignSelfSink {
    #[bstack_owned]
    pub of: Foreign<Leaf>,
    #[bstack_owned]
    pub opt: Option<Foreign<Leaf>>,
}

pub fn foreign_self_sink<A: BStackRaiiAllocator>(
    a: &A,
) -> io::Result<BStackOwned<ForeignSelfSink>> {
    // Create a SELF `Leaf` and relinquish ownership (its block persists), yielding a
    // bare offset to wrap as a `Foreign` the field then owns.
    let self_leaf = |v: u32| -> io::Result<u64> {
        let l = Leaf::new(a, v)?;
        let off = l.handle().range().start();
        let _ = l.into_inner();
        Ok(off)
    };
    let of = unsafe { Foreign::<Leaf>::new(FileId::SELF, self_leaf(10)?) };
    let opt = Some(unsafe { Foreign::<Leaf>::new(FileId::SELF, self_leaf(20)?) });
    ForeignSelfSink::new(a, of, opt)
}

// ---------------------------------------------------------------------------
// Shared containers — the block is itself `rc` / `rc, weak`
// ---------------------------------------------------------------------------

#[bstack_block(rc)]
pub struct RcSink {
    pub pod: u32,
    #[bstack_owned]
    pub child: Leaf,
}

pub fn rc_sink<A: BStackRaiiAllocator>(a: &A) -> io::Result<BStackRc<'_, RcSink, A>> {
    RcSink::new(a, 7, Leaf::new(a, 10)?)
}

#[bstack_block(rc, weak)]
pub struct RcWeakSink {
    pub pod: u32,
    #[bstack_owned]
    pub child: Leaf,
}

pub fn rcweak_sink<A: BStackRaiiAllocator>(a: &A) -> io::Result<BStackRc<'_, RcWeakSink, A>> {
    RcWeakSink::new(a, 7, Leaf::new(a, 10)?)
}

// ---------------------------------------------------------------------------
// EnumSink — one variant per self-owning enum cell
// ---------------------------------------------------------------------------

#[bstack_enum]
pub enum EnumSink {
    Unit,
    Num(u32),
    Pair(u32, u64),
    #[bstack_owned]
    Owned(Leaf),
    #[bstack_strong]
    Strong(Shared),
    #[bstack_owned]
    Items(Vec<Leaf>),
    #[bstack_owned]
    Arr([Leaf; 2]),
    #[embed]
    Emb(Leaf),
}

/// The `Owned` variant — a convenient owning enum value for teardown / clone / move.
pub fn enum_owned<A: BStackRaiiAllocator>(a: &A) -> io::Result<BStackOwned<EnumSink>> {
    EnumSink::new(a, EnumSinkData::Owned(Leaf::new(a, 42)?))
}

/// A **mutable** enum (whole-value `replace`). Separate from [`EnumSink`] because the
/// enum-`#[bstack_mut]` guard rejects an `#[embed]` variant, which `EnumSink` has.
#[bstack_enum]
#[bstack_mut]
pub enum MutEnum {
    Empty,
    Num(u32),
    #[bstack_owned]
    Owned(Leaf),
}

/// A shared (`rc`) enum — the enum is itself reference-counted.
#[bstack_enum(rc)]
pub enum RcEnum {
    Empty,
    Num(u32),
    #[bstack_owned]
    Owned(Leaf),
}

// ---------------------------------------------------------------------------
// GenSink<T> — a generic holder (instantiated per test)
// ---------------------------------------------------------------------------

#[bstack_block]
pub struct GenSink<T> {
    pub tag: u32,
    #[bstack_owned]
    pub owned: T,
    #[bstack_owned]
    pub vec: Vec<T>,
}

/// A const-generic array field (`[T; N]`, single const dim).
#[bstack_block]
pub struct ConstArrSink<const N: usize> {
    #[bstack_owned]
    pub xs: [Leaf; N],
}

// ---------------------------------------------------------------------------
// RefWeakSink — ref / weak cells (external targets; wired inline by tests)
// ---------------------------------------------------------------------------

#[bstack_block]
pub struct RefWeakSink {
    #[bstack_ref]
    pub refd: Leaf,
    #[bstack_ref]
    pub opt_ref: Option<Leaf>,
    #[bstack_ref]
    pub arr_ref: [Leaf; 2],
    #[bstack_ref]
    pub ref_vec: Vec<Leaf>,
    #[bstack_weak]
    pub weak: Shared,
    #[bstack_weak]
    pub weak_arr: [Shared; 2],
}

/// An enum with `#[bstack_ref]` / `#[bstack_weak]` variants (external targets).
#[bstack_enum]
pub enum RefEnum {
    Empty,
    #[bstack_ref]
    Ref(Leaf),
    #[bstack_weak]
    Weak(Shared),
}
