//! Core primitives for range access control (the `expensive-slice-access-control`
//! feature).
//!
//! This module is the lock-free heart of the range access-control policy layer:
//! the [`BStackAccess`] mode enum, the sorted change-point table
//! ([`PointTable`]) with its lookup / range-check / set operations, and the
//! authority model that decides whether a caller may act on a range.
//!
//! It carries **no** locking, no `BStack` wiring, and no capability-token
//! lifetimes — those live in the `BStack` integration. Here an authority is just
//! the set of tokens a caller *presents* ([`BStackAccessAuthorities`]); the table answers
//! whether that presentation satisfies a mode.
//!
//! # Model
//!
//! Every offset in the payload has a mode. The table stores only the offsets
//! where the mode *changes* (`(offset, mode)`, sorted, coalesced); an absent
//! leading point implies [`All`](BStackAccess::All) from 0, so an unprotected
//! stack is the empty table. Each mode fixes, per axis (read / write / truncate),
//! which authorities satisfy it. The two tokens — a guard and an allocator — are
//! **incomparable**: neither implies the other, so [`Prot`](BStackAccess::Prot)
//! and [`Alloc`](BStackAccess::Alloc) are each private to their own holder.

/// The three axes a mode governs independently.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessOp {
    /// Reading bytes in the range.
    Read,
    /// Writing bytes in the range (same-length, in place).
    Write,
    /// Discarding the range by truncation (checked over `[new_len, old_len)`).
    Truncate,
}

/// Which authorities satisfy one cell of the mode table.
///
/// `Any` needs no token; `None` is satisfiable by no one. The two tokens are
/// incomparable, so `Guard` and `Allocator` are distinct, and
/// `GuardOrAllocator` is the only cell either token satisfies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BStackAccessRequirement {
    /// Anyone, with or without a token.
    Any,
    /// The guard token only.
    Guard,
    /// The allocator token only.
    Allocator,
    /// Either token (used by [`Rw`](BStackAccess::Rw) truncate).
    GuardOrAllocator,
    /// No authority satisfies it.
    None,
}

impl BStackAccessRequirement {
    /// Whether a caller presenting `held` satisfies this requirement.
    #[inline]
    #[must_use]
    pub const fn satisfied_by(self, held: BStackAccessAuthorities) -> bool {
        match self {
            Self::Any => true,
            Self::Guard => held.guard,
            Self::Allocator => held.alloc,
            Self::GuardOrAllocator => held.guard || held.alloc,
            Self::None => false,
        }
    }
}

/// The tokens a caller presents when acting on a range. Pure input to the
/// checks; the one-shot minting that makes tokens meaningful lives in the
/// `BStack` integration, not here.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BStackAccessAuthorities {
    /// Holds the guard capability (the protection token).
    pub guard: bool,
    /// Holds the allocator capability (the alloc-authority token).
    pub alloc: bool,
}

impl BStackAccessAuthorities {
    /// No tokens — an ordinary caller.
    pub const NONE: Self = Self {
        guard: false,
        alloc: false,
    };
    /// The guard token only.
    pub const GUARD: Self = Self {
        guard: true,
        alloc: false,
    };
    /// The allocator token only.
    pub const ALLOC: Self = Self {
        guard: false,
        alloc: true,
    };
}

/// Access mode for a range, one per distinct region of the point table.
///
/// [`requirement`](BStackAccess::requirement) encodes each mode's per-axis
/// authority. [`All`](BStackAccess::All) is the default everywhere and what an
/// unprotected stack reports.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum BStackAccess {
    /// Read / write / truncate all open to anyone. The default.
    #[default]
    All,
    /// Read / write open to anyone; truncate needs a guard or the allocator.
    Rw,
    /// Read / write open to anyone; truncate denied to everyone.
    RwStrict,
    /// Read / write / truncate all need the guard token.
    Prot,
    /// Read / write need the guard token; truncate denied to everyone.
    RwProt,
    /// Read / write / truncate all need the allocator token.
    Alloc,
    /// Read open to anyone; write and truncate denied.
    ReadOnly,
    /// Read / write / truncate all denied.
    Locked,
}

impl BStackAccess {
    /// The authority that satisfies `op` under this mode.
    #[inline]
    #[must_use]
    pub const fn requirement(self, op: AccessOp) -> BStackAccessRequirement {
        use AccessOp::{Read, Truncate, Write};
        use BStackAccessRequirement::{Allocator, Any, Guard, GuardOrAllocator, None};
        match (self, op) {
            (Self::All, _) => Any,

            (Self::Rw, Read | Write) => Any,
            (Self::Rw, Truncate) => GuardOrAllocator,

            (Self::RwStrict, Read | Write) => Any,
            (Self::RwStrict, Truncate) => None,

            (Self::Prot, _) => Guard,

            (Self::RwProt, Read | Write) => Guard,
            (Self::RwProt, Truncate) => None,

            (Self::Alloc, _) => Allocator,

            (Self::ReadOnly, Read) => Any,
            (Self::ReadOnly, Write | Truncate) => None,

            (Self::Locked, _) => None,
        }
    }

    /// Whether a caller presenting `held` may perform `op` under this mode.
    #[inline]
    #[must_use]
    pub const fn permits(self, op: AccessOp, held: BStackAccessAuthorities) -> bool {
        self.requirement(op).satisfied_by(held)
    }
}

/// A sorted, coalesced table of change points.
///
/// `(off, mode)` means `mode` applies from `off` until the next point; an absent
/// leading point implies [`All`](BStackAccess::All) from 0. Adjacent equal modes
/// are coalesced, so the table is proportional to the number of distinct regions
/// and one protected header is two entries. The vec is read far more often than
/// written, so a read is a `partition_point` over a contiguous array rather than
/// a tree walk.
// Scaffolding for the not-yet-wired BStack integration; unused until then.
#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
pub(crate) struct PointTable {
    /// Change points, strictly increasing in offset, no adjacent equal modes,
    /// no leading `All`.
    points: Vec<(u64, BStackAccess)>,
}

#[allow(dead_code)]
impl PointTable {
    /// An empty table: [`All`](BStackAccess::All) everywhere.
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self { points: Vec::new() }
    }

    /// Whether the table is [`All`](BStackAccess::All) everywhere. The
    /// `AtomicBool` short-circuit in the `BStack` integration mirrors this.
    #[inline]
    #[must_use]
    pub fn is_unprotected(&self) -> bool {
        self.points.is_empty()
    }

    /// The stored change points, for inspection and testing.
    #[inline]
    #[must_use]
    pub fn points(&self) -> &[(u64, BStackAccess)] {
        &self.points
    }

    /// The mode in effect at `off`.
    ///
    /// `partition_point(|p| p.0 <= off) - 1`, with `All` when no point starts at
    /// or before `off`. Points beyond `len` are retained, so a mode set before
    /// its bytes arrive still reports here.
    #[inline]
    #[must_use]
    pub fn mode_at(&self, off: u64) -> BStackAccess {
        let i = self.points.partition_point(|p| p.0 <= off);
        if i == 0 {
            BStackAccess::All
        } else {
            self.points[i - 1].1
        }
    }

    /// Whether every offset in `[a, b)` is currently [`All`](BStackAccess::All).
    ///
    /// The tokenless authorization rule for arming a range: a caller with no
    /// token may only tighten a range that is entirely unprotected. An empty
    /// range is trivially uniform.
    #[must_use]
    pub fn all_over(&self, a: u64, b: u64) -> bool {
        if a >= b {
            return true;
        }
        if self.mode_at(a) != BStackAccess::All {
            return false;
        }
        // Any change point inside `[a, b)` breaks uniformity.
        let i = self.points.partition_point(|p| p.0 <= a);
        i >= self.points.len() || self.points[i].0 >= b
    }

    /// Whether every offset in `[a, b)` permits `op` under `held`.
    ///
    /// One `partition_point` locates the mode at `a`; a forward scan folds in
    /// each point strictly inside `(a, b)`, stopping at the first denial. The
    /// common case spans one region and the scan never runs. An empty range
    /// (`a >= b`) is trivially permitted.
    #[must_use]
    pub fn check(&self, a: u64, b: u64, op: AccessOp, held: BStackAccessAuthorities) -> bool {
        if a >= b {
            return true;
        }
        // Mode governing the start of the range.
        let start = self.points.partition_point(|p| p.0 <= a);
        let mode = if start == 0 {
            BStackAccess::All
        } else {
            self.points[start - 1].1
        };
        if !mode.permits(op, held) {
            return false;
        }
        // Every point strictly inside (a, b) opens a new region to check.
        let mut j = start;
        while j < self.points.len() && self.points[j].0 < b {
            if !self.points[j].1.permits(op, held) {
                return false;
            }
            j += 1;
        }
        true
    }

    /// Set `[a, b)` to `mode`, leaving everything at or after `b` unchanged.
    ///
    /// Splices in the up-to-two boundaries the range needs — `(a, mode)` and a
    /// `(b, resume)` restoring the mode that governed `b` — dropping each when it
    /// would duplicate the region it opens against, and dropping the following
    /// point when the tail now resumes its mode. Two binary searches locate the
    /// affected span; the rest is proportional to the points in `[a, b]` plus the
    /// array shift, with no full-table pass. An empty range is a no-op. This is
    /// the raw table mutation: it does **not** consult authorities — the `BStack`
    /// integration gates who may call it.
    pub fn set(&mut self, a: u64, b: u64, mode: BStackAccess) {
        if a >= b {
            return;
        }
        // Mode governing b now; restored as the tail resumes after the set.
        let hi = self.points.partition_point(|p| p.0 <= b);
        let resume = if hi == 0 {
            BStackAccess::All
        } else {
            self.points[hi - 1].1
        };
        // First point at or after a, and the mode of the region entering a.
        let lo = self.points.partition_point(|p| p.0 < a);
        let before = if lo == 0 {
            BStackAccess::All
        } else {
            self.points[lo - 1].1
        };

        // Boundaries for [a, b): each omitted when it repeats the mode before it.
        // The point after the range (index `hi`) never needs dropping: it already
        // differed from `resume` in the coalesced table, and `resume` is the mode
        // the tail now resumes with, so the join stays distinct.
        let mut mid = [(0u64, BStackAccess::All); 2];
        let mut n = 0;
        if mode != before {
            mid[n] = (a, mode);
            n += 1;
        }
        if resume != mode {
            mid[n] = (b, resume);
            n += 1;
        }

        self.points.splice(lo..hi, mid[..n].iter().copied());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use AccessOp::{Read, Truncate, Write};
    use BStackAccess::{All, Alloc, Locked, Prot, ReadOnly, Rw, RwProt, RwStrict};

    const NONE: BStackAccessAuthorities = BStackAccessAuthorities::NONE;
    const GUARD: BStackAccessAuthorities = BStackAccessAuthorities::GUARD;
    const ALLOC: BStackAccessAuthorities = BStackAccessAuthorities::ALLOC;

    #[test]
    fn requirement_matrix_is_correct() {
        // All: everything to anyone.
        for op in [Read, Write, Truncate] {
            assert!(All.permits(op, NONE));
        }
        // Rw: rw to anyone, truncate to either token but not the tokenless.
        assert!(Rw.permits(Write, NONE));
        assert!(!Rw.permits(Truncate, NONE));
        assert!(Rw.permits(Truncate, GUARD));
        assert!(Rw.permits(Truncate, ALLOC));
        // RwStrict: rw to anyone, truncate to no one.
        assert!(RwStrict.permits(Write, NONE));
        assert!(!RwStrict.permits(Truncate, ALLOC));
        // Prot vs Alloc are incomparable on every axis.
        for op in [Read, Write, Truncate] {
            assert!(Prot.permits(op, GUARD));
            assert!(!Prot.permits(op, ALLOC));
            assert!(Alloc.permits(op, ALLOC));
            assert!(!Alloc.permits(op, GUARD));
        }
        // RwProt: rw to the guard, truncate to no one.
        assert!(RwProt.permits(Write, GUARD));
        assert!(!RwProt.permits(Truncate, GUARD));
        // ReadOnly / Locked.
        assert!(ReadOnly.permits(Read, NONE));
        assert!(!ReadOnly.permits(Write, GUARD));
        assert!(!Locked.permits(Read, GUARD));
    }

    #[test]
    fn empty_table_is_all() {
        let t = PointTable::new();
        assert!(t.is_unprotected());
        assert_eq!(t.mode_at(0), All);
        assert_eq!(t.mode_at(1_000_000), All);
        assert!(t.check(0, 100, Write, NONE));
    }

    #[test]
    fn mode_at_boundaries() {
        let mut t = PointTable::new();
        t.set(16, 32, Alloc);
        // [0,16) All, [16,32) Alloc, [32,..) All.
        assert_eq!(t.mode_at(15), All);
        assert_eq!(t.mode_at(16), Alloc);
        assert_eq!(t.mode_at(31), Alloc);
        assert_eq!(t.mode_at(32), All);
        // One protected header is exactly two entries.
        assert_eq!(t.points(), &[(16, Alloc), (32, All)]);
    }

    #[test]
    fn range_check_folds_most_restrictive() {
        let mut t = PointTable::new();
        t.set(16, 32, Alloc);
        // A read fully inside the Alloc region needs the alloc token.
        assert!(t.check(20, 24, Read, ALLOC));
        assert!(!t.check(20, 24, Read, GUARD));
        // A range spanning All -> Alloc -> All is denied without the token,
        // because the Alloc slice in the middle dominates.
        assert!(!t.check(0, 64, Read, NONE));
        assert!(t.check(0, 64, Read, ALLOC));
        // A range entirely outside the protected slice is open.
        assert!(t.check(0, 16, Write, NONE));
        assert!(t.check(32, 64, Write, NONE));
    }

    #[test]
    fn set_coalesces_adjacent_equal_modes() {
        let mut t = PointTable::new();
        t.set(10, 20, Prot);
        t.set(20, 30, Prot);
        // Two adjacent Prot regions collapse to one.
        assert_eq!(t.points(), &[(10, Prot), (30, All)]);
    }

    #[test]
    fn set_over_all_leaves_no_leading_point() {
        let mut t = PointTable::new();
        t.set(0, 50, All);
        assert!(t.is_unprotected());
    }

    #[test]
    fn set_overwrites_inner_points() {
        let mut t = PointTable::new();
        t.set(10, 20, Prot);
        t.set(30, 40, Alloc);
        // Overwrite a span that swallows the first region and part of the gap.
        t.set(5, 35, Rw);
        // [0,5) All, [5,35) Rw, [35,40) Alloc, [40,..) All.
        assert_eq!(t.points(), &[(5, Rw), (35, Alloc), (40, All)]);
        assert_eq!(t.mode_at(4), All);
        assert_eq!(t.mode_at(5), Rw);
        assert_eq!(t.mode_at(35), Alloc);
        assert_eq!(t.mode_at(40), All);
    }

    #[test]
    fn set_restores_resume_mode_at_b() {
        let mut t = PointTable::new();
        t.set(10, 40, Alloc);
        // Carve a Prot window inside the Alloc region; Alloc must resume after.
        t.set(20, 30, Prot);
        assert_eq!(t.mode_at(19), Alloc);
        assert_eq!(t.mode_at(20), Prot);
        assert_eq!(t.mode_at(30), Alloc);
        assert_eq!(t.mode_at(40), All);
    }

    #[test]
    fn empty_range_is_noop_and_permitted() {
        let mut t = PointTable::new();
        t.set(10, 20, Locked);
        assert!(t.check(15, 15, Read, NONE));
        t.set(15, 15, All);
        assert_eq!(t.mode_at(15), Locked);
    }

    #[test]
    fn all_over_detects_uniform_all() {
        let mut t = PointTable::new();
        assert!(t.all_over(0, 100));
        assert!(t.all_over(50, 50)); // empty
        t.set(20, 40, Prot);
        assert!(t.all_over(0, 20)); // touches boundary but stays All
        assert!(!t.all_over(0, 21)); // reaches into Prot
        assert!(!t.all_over(25, 30)); // inside Prot
        assert!(t.all_over(40, 100)); // All resumes after
        assert!(!t.all_over(30, 60)); // spans Prot -> All
    }

    #[test]
    fn points_beyond_len_are_retained() {
        let mut t = PointTable::new();
        // Arm a region before its bytes exist.
        t.set(1000, 2000, Prot);
        assert_eq!(t.mode_at(1500), Prot);
        assert_eq!(t.points(), &[(1000, Prot), (2000, All)]);
    }

    // Naive reference: materialize per-offset modes and re-derive the change
    // points. Deliberately O(span) and obviously correct.
    fn reference_set(cells: &mut [BStackAccess], a: u64, b: u64, mode: BStackAccess) {
        for c in &mut cells[a as usize..b as usize] {
            *c = mode;
        }
    }
    fn cells_to_points(cells: &[BStackAccess]) -> Vec<(u64, BStackAccess)> {
        let mut out = Vec::new();
        let mut prev = All;
        for (i, &m) in cells.iter().enumerate() {
            if m != prev {
                out.push((i as u64, m));
                prev = m;
            }
        }
        out
    }

    #[test]
    fn set_matches_naive_reference_randomized() {
        use rand::RngExt;
        use rand::SeedableRng;
        let modes = [All, Rw, RwStrict, Prot, RwProt, Alloc, ReadOnly, Locked];
        let mut rng = rand::rngs::StdRng::seed_from_u64(0xB57A_C0DE);
        const SPAN: u64 = 32;
        for _ in 0..2000 {
            let mut t = PointTable::new();
            // One extra sentinel cell (always All) captures the closing boundary
            // a set with `b == SPAN` records at offset SPAN.
            let mut cells = [All; SPAN as usize + 1];
            for _ in 0..8 {
                let x = rng.random_range(0..=SPAN);
                let y = rng.random_range(0..=SPAN);
                let (a, b) = (x.min(y), x.max(y));
                let m = modes[rng.random_range(0..modes.len())];
                t.set(a, b, m);
                reference_set(&mut cells, a, b, m);
                // Points always match the coalesced reference derivation.
                assert_eq!(t.points(), cells_to_points(&cells).as_slice());
                // Invariant: strictly increasing offsets, no adjacent equal modes,
                // no leading All.
                let pts = t.points();
                for w in pts.windows(2) {
                    assert!(w[0].0 < w[1].0);
                    assert_ne!(w[0].1, w[1].1);
                }
                if let Some(first) = pts.first() {
                    assert_ne!(first.1, All);
                }
            }
        }
    }
}
