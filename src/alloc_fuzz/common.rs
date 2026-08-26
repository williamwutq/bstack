// Shared test-support: not every item is exercised under every feature combo
// (e.g. `Operation::Reopen` and `verify_prefix` are only used once the
// fault-injection suite is compiled in). Keep the module warning-clean.
#![allow(dead_code)]

//! Shared support for the allocator fuzz suites.
//!
//! [`init`](super::init), `init_fault`,
//! [`uninit`](super::uninit), and `uninit_fault` (and, in future, an
//! alloc bench) drive the same cross-allocator machinery: temp-file management,
//! deterministic byte-pattern payloads, an adversarial "looks like allocator
//! internals" payload source, a weighted random-operation generator, the
//! per-allocator constructor closures, and (under `fault-injection`) the shared
//! [`RandomFaults`](policies::RandomFaults) policy. Centralising them here keeps
//! the suites from drifting apart.

use crate::BStack;
use crate::alloc::{BStackOwnedSlice, BStackOwnedSliceAllocator};
use rand::{RngExt, SeedableRng, rngs::StdRng};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

/// Seed a reproducible RNG for a happy-path fuzz driver and print the seed, so a
/// failing run — whose captured stderr `cargo test` shows on panic — can be
/// replayed with `BSTACK_FUZZ_SEED=<n>`. Without that env var a fresh random
/// master is drawn each run.
///
/// The label is the running test's thread name (its full path, e.g.
/// `alloc_fuzz::inplace::first_fit::resize`), and an FNV-1a hash of it salts the
/// stream so distinct drivers/allocators do not share an op stream under one
/// master seed. The seeded fault drivers print the same `[label salt=…] SEED=…`
/// line by hand; this is the happy-path equivalent.
pub(crate) fn seeded_rng() -> StdRng {
    let label = std::thread::current().name().unwrap_or("fuzz").to_string();
    let master = std::env::var("BSTACK_FUZZ_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| rand::rng().random_range(0..=u64::MAX));
    let mut salt = 0xcbf2_9ce4_8422_2325u64; // FNV-1a offset basis
    for &b in label.as_bytes() {
        salt ^= b as u64;
        salt = salt.wrapping_mul(0x0000_0100_0000_01b3); // FNV prime
    }
    eprintln!("[{label} salt={salt:#018x}] BSTACK_FUZZ_SEED={master}");
    StdRng::seed_from_u64(master ^ salt)
}

/// RAII guard that removes a temp backing file when dropped.
pub(crate) struct Guard(pub std::path::PathBuf);
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// A process-unique temp path for a `.bstack` backing file.
pub(crate) fn temp_path(prefix: &str) -> std::path::PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("bstack_fuzz_{prefix}_{pid}_{id}.bin"))
}

/// Fill `buf` with a cheap, regenerable pattern derived from `id` and `bias`.
///
/// `bias` is a per-run salt so that two suites (or two parallel test binaries)
/// reusing the same `id` produce disjoint patterns — a stray cross-context read
/// then shows up as a mismatch instead of a false pass.
pub(crate) fn fill(buf: &mut [u8], id: u64, bias: u64) {
    let seed = id ^ bias;
    for (i, b) in buf.iter_mut().enumerate() {
        *b = ((seed >> ((i % 8) * 8)) & 0xFF) as u8;
    }
}

/// Assert every byte of `buf` matches the [`fill`] pattern for `id`/`bias`.
pub(crate) fn check(buf: &[u8], id: u64, bias: u64, ctx: &str) {
    let seed = id ^ bias;
    for (i, &b) in buf.iter().enumerate() {
        let expected = ((seed >> ((i % 8) * 8)) & 0xFF) as u8;
        assert_eq!(
            b, expected,
            "{ctx}: corruption at [{i}]: got {b:#04x}, expected {expected:#04x} (id={id}, bias={bias})"
        );
    }
}

/// Assert every byte of `buf` is zero (used to check realloc zero-extension).
pub(crate) fn check_is_zero(buf: &[u8], ctx: &str) {
    for (i, &b) in buf.iter().enumerate() {
        assert_eq!(b, 0, "{ctx}: expected zero at [{i}], got {b:#04x}");
    }
}

/// The two kinds of data a fuzz allocation is filled with.
///
/// * [`Payload::Seeded`] — a period-8 byte pattern keyed by an id. Cheap: only
///   the id is stored, and the bytes are regenerated on demand via [`fill`].
/// * [`Payload::Raw`] — an adversarial snapshot of bytes copied out of the
///   BStack itself (see [`adversarial_bytes`]); the exact bytes are retained so
///   they can be verified on read-back. For a populated arena these bytes look
///   like real allocator internals (headers, free-list pointers, AVL nodes,
///   overhead tags), so writing them into a live allocation checks that
///   adversarial user data resembling metadata cannot trick an allocator.
pub(crate) enum Payload {
    Seeded(u64),
    Raw(Vec<u8>),
}

impl Payload {
    /// The intended byte length of this payload.
    pub(crate) fn len(&self, slice_len: u64) -> u64 {
        match self {
            Payload::Seeded(_) => slice_len,
            Payload::Raw(bytes) => bytes.len() as u64,
        }
    }

    /// Write this payload into the beginning of `slice`.
    pub(crate) fn write<A: BStackOwnedSliceAllocator>(
        &self,
        slice: &mut BStackOwnedSlice<'_, A>,
        bias: u64,
    ) -> io::Result<()> {
        match self {
            Payload::Seeded(id) => {
                let mut buf = vec![0u8; slice.len() as usize];
                fill(&mut buf, *id, bias);
                slice.write(&buf)
            }
            Payload::Raw(bytes) => slice.write(bytes),
        }
    }

    /// Verify the whole allocation matches this payload, panicking on mismatch.
    pub(crate) fn verify<A: BStackOwnedSliceAllocator>(
        &self,
        slice: &BStackOwnedSlice<'_, A>,
        bias: u64,
        ctx: &str,
    ) {
        let got = slice.read().unwrap();
        match self {
            Payload::Seeded(id) => check(&got, *id, bias, ctx),
            Payload::Raw(bytes) => {
                assert_eq!(got.len(), bytes.len(), "{ctx}: raw payload length mismatch");
                assert!(got == *bytes, "{ctx}: raw payload corruption");
            }
        }
    }

    /// Verify only the first `n` bytes match (the prefix preserved across a
    /// realloc that keeps `min(old_len, new_len)` bytes).
    pub(crate) fn verify_prefix<A: BStackOwnedSliceAllocator>(
        &self,
        slice: &BStackOwnedSlice<'_, A>,
        n: u64,
        bias: u64,
        ctx: &str,
    ) {
        let got = slice.read_range(0, n).unwrap();
        let n = n as usize;
        match self {
            Payload::Seeded(id) => check(&got, *id, bias, ctx),
            Payload::Raw(bytes) => {
                assert!(
                    bytes.len() >= n,
                    "{ctx}: stored raw payload shorter than prefix"
                );
                assert!(got == bytes[..n], "{ctx}: raw payload prefix corruption");
            }
        }
    }
}

/// Read `len` bytes from a random, 8-byte-**aligned**, in-bounds region of
/// `stack` to use as an allocation's payload.
///
/// The source offset is chosen so `src + len` never exceeds the payload size,
/// so this can never trigger a spurious out-of-bounds I/O error. For a
/// populated arena the returned bytes are real allocator internals, which is
/// exactly the adversarial input we want to feed back into an allocation.
///
/// Returns `None` when the stack is too small to source `len` bytes; the caller
/// should fall back to a [`Payload::Seeded`].
pub(crate) fn adversarial_bytes<R: rand::RngExt>(
    stack: &BStack,
    len: u64,
    rng: &mut R,
) -> Option<Vec<u8>> {
    if len == 0 {
        return Some(Vec::new());
    }
    let total = stack.len().ok()?;
    if total < len {
        return None;
    }
    // Largest valid start, floored to an 8-byte boundary.
    let aligned_max = (total - len) & !7u64;
    let src = if aligned_max == 0 {
        0
    } else {
        rng.random_range(0..=aligned_max / 8) * 8
    };
    stack.get(src, src + len).ok()
}

/// A single fuzz operation. `Reopen` is only emitted by suites that opt in via
/// `allow_reopen` (the fault suite).
pub(crate) enum Operation {
    Alloc(u64),
    Realloc(u64),
    Dealloc,
    Check,
    Reopen,
}

/// Weighted random operation generator shared by the fuzz suites.
///
/// When `have_live` is false only [`Operation::Alloc`] is produced (nothing to
/// realloc/dealloc/check yet). `allow_reopen` enables the [`Operation::Reopen`]
/// slice of the distribution.
pub(crate) fn gen_op<R: rand::RngExt>(
    rng: &mut R,
    cfg: &FuzzConfig,
    have_live: bool,
    allow_reopen: bool,
) -> Operation {
    if !have_live {
        return Operation::Alloc(rng.random_range(0..=cfg.max_alloc));
    }
    let roll: u32 = rng.random_range(0..100);
    match roll {
        0..=44 => Operation::Alloc(rng.random_range(0..=cfg.max_alloc)),
        45..=64 => Operation::Realloc(rng.random_range(0..=cfg.max_alloc)),
        65..=79 => Operation::Dealloc,
        80..=94 => Operation::Check,
        _ if allow_reopen => Operation::Reopen,
        _ => Operation::Check,
    }
}

/// Decide whether this allocation should carry an adversarial [`Payload::Raw`]
/// (rolling against `cfg.adversarial_pct`), building it from `stack` if so;
/// otherwise a [`Payload::Seeded`] keyed by `id`.
pub(crate) fn make_payload<R: rand::RngExt>(
    stack: &BStack,
    len: u64,
    id: u64,
    cfg: &FuzzConfig,
    rng: &mut R,
) -> Payload {
    if rng.random_range(0..100) < cfg.adversarial_pct
        && let Some(bytes) = adversarial_bytes(stack, len, rng)
    {
        return Payload::Raw(bytes);
    }
    Payload::Seeded(id)
}

// The in-place suites need a payload keyed on the *absolute physical offset*,
// not the logical index the `Payload` above uses. `realloc_inplace` can move the
// front edge, so a byte's logical index shifts while its physical offset — and
// the retained value living there — does not. Keying the expected byte on the
// stack offset makes the trait's core promise ("the retained bytes occupy the
// same physical offsets") checkable by a single read of the overlap.

/// Expected byte at absolute stack offset `p` for a live allocation tagged `id`
/// in a run salted by `bias`. Deterministic and cheap to regenerate.
pub(crate) fn pat(p: u64, id: u64, bias: u64) -> u8 {
    let mut z =
        p.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (id ^ bias).wrapping_mul(0xD1B5_4A32_D192_ED03);
    z ^= z >> 33;
    z = z.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
    z ^= z >> 33;
    (z & 0xFF) as u8
}

/// Fill `slice` with the [`pat`] pattern keyed to its current physical offsets.
pub(crate) fn write_pattern<A: BStackOwnedSliceAllocator>(
    slice: &mut BStackOwnedSlice<'_, A>,
    id: u64,
    bias: u64,
) -> io::Result<()> {
    let start = slice.start();
    let mut buf = vec![0u8; slice.len() as usize];
    for (i, b) in buf.iter_mut().enumerate() {
        *b = pat(start + i as u64, id, bias);
    }
    slice.write(&buf)
}

/// Verify every byte of `slice` matches the [`pat`] pattern for its physical
/// offsets, panicking on mismatch.
pub(crate) fn verify_pattern<A: BStackOwnedSliceAllocator>(
    slice: &BStackOwnedSlice<'_, A>,
    id: u64,
    bias: u64,
    ctx: &str,
) {
    verify_pattern_range(
        slice,
        slice.start(),
        slice.start() + slice.len(),
        id,
        bias,
        ctx,
    )
}

/// Verify the absolute physical range `[lo, hi)` of `slice` matches the [`pat`]
/// pattern. `lo`/`hi` must lie within the slice's physical extent; an empty
/// range is a no-op. Used to check the retained overlap after an in-place
/// resize.
pub(crate) fn verify_pattern_range<A: BStackOwnedSliceAllocator>(
    slice: &BStackOwnedSlice<'_, A>,
    lo: u64,
    hi: u64,
    id: u64,
    bias: u64,
    ctx: &str,
) {
    if hi <= lo {
        return;
    }
    let start = slice.start();
    let got = slice.read_range(lo - start, hi - start).unwrap();
    for (k, &b) in got.iter().enumerate() {
        let p = lo + k as u64;
        let e = pat(p, id, bias);
        assert_eq!(
            b, e,
            "{ctx}: corruption at phys {p}: got {b:#04x}, expected {e:#04x} (id={id}, bias={bias})"
        );
    }
}

/// Generate a random `(prepend, append)` delta pair for a live allocation of
/// `len` bytes. Biases toward pure back-edge and pure front-edge moves (the
/// cases allocators are most likely to support) while still emitting mixed
/// moves, front deltas snapped to 8-byte alignment (front carves are
/// alignment-constrained), and the occasional identity no-op. Many pairs are
/// `Unsupported` for a given allocator; the driver treats that as a valid
/// outcome, so the generator need not know each allocator's capabilities.
pub(crate) fn gen_inplace_deltas<R: rand::RngExt>(
    rng: &mut R,
    len: u64,
    cfg: &FuzzConfig,
) -> (i64, i64) {
    let l = len as i64;
    let max = cfg.max_alloc as i64;
    // Back edge: shrink to (just past) empty, or grow up to max_alloc.
    let back = |rng: &mut R| rng.random_range(-(l + 1)..=max);
    // Front edge: 8-aligned magnitude so front carves can meet alignment rules.
    let front = |rng: &mut R| {
        let cap = (len.max(64) / 8 + 8) as i64;
        let mag = rng.random_range(0..=cap) * 8;
        if rng.random_bool(0.5) { -mag } else { mag }
    };
    match rng.random_range(0..100) {
        0..=49 => (0, back(rng)),           // append-only
        50..=79 => (front(rng), 0),         // prepend-only
        80..=94 => (front(rng), back(rng)), // both edges
        _ => (0, 0),                        // identity no-op
    }
}

/// Volume/shape knobs, read once from the environment with sane defaults so CI
/// can crank a longer fuzz run without recompiling. No file snapshots are
/// taken: a failing run panics with enough context (id/bias/offset) to
/// reproduce from the printed seed.
pub(crate) struct FuzzConfig {
    /// Total operations for the single-session drivers.
    pub ops: usize,
    /// Number of reopen sessions for the reopen driver.
    pub sessions: usize,
    /// Operations per reopen session.
    pub ops_per_session: usize,
    /// Maximum allocation length requested.
    pub max_alloc: u64,
    /// Fault suite: operations between reopen+recover integrity checks.
    pub reopen_every: usize,
    /// Percent chance [0,100) that an allocation uses an adversarial payload.
    pub adversarial_pct: u32,
}

impl FuzzConfig {
    pub(crate) fn from_env() -> Self {
        fn env<T: std::str::FromStr>(key: &str, default: T) -> T {
            std::env::var(key)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        }
        Self {
            ops: env("BSTACK_FUZZ_OPS", 10_000),
            sessions: env("BSTACK_FUZZ_SESSIONS", 20),
            ops_per_session: env("BSTACK_FUZZ_OPS_PER", 100),
            max_alloc: env("BSTACK_FUZZ_MAX_ALLOC", 1024),
            reopen_every: env("BSTACK_FUZZ_REOPEN_EVERY", 200),
            adversarial_pct: env("BSTACK_FUZZ_ADVERSARIAL_PCT", 25),
        }
    }
}

/// Build a `Fn(BStack) -> io::Result<A>` constructor for cross-allocator drivers.
///
/// * `make_allocator!(SlabBStackAllocator, 16)` — for slab-style allocators
///   that take a size on fresh construction: fresh arena → `new(bs, size)`,
///   existing arena → `open(bs)`.
/// * `make_allocator!(FirstFitBStackAllocator)` — for allocators whose `new` is
///   the sole constructor and reattaches an existing arena itself.
macro_rules! make_allocator {
    ($ty:ty, $size:expr) => {
        |bs: $crate::BStack| {
            if bs.is_empty().unwrap() {
                <$ty>::new(bs, $size)
            } else {
                <$ty>::open(bs)
            }
        }
    };
    ($ty:ty) => {
        <$ty>::new
    };
}
pub(crate) use make_allocator;

// Fault-injection policies
// Only compiled when the fault-injection machinery exists. Shared by the
// per-allocator failure unit tests (`FailOpAt`) and the fault fuzz suite.

#[cfg(all(debug_assertions, feature = "fault-injection"))]
pub(crate) mod policies {
    use crate::fault::FaultPolicy;
    use std::io;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Fail the `at`-th (0-based) consultation whose operation name equals `op`,
    /// exactly once, with an [`io::Error`] of the given kind. Every other
    /// operation — including same-named ones before/after `at`, and differently
    /// named ones — proceeds normally.
    ///
    /// Counting is per matching name (not the global `seq`), so a target like
    /// `FailOpAt::new("set", 1, ..)` reliably hits the second `set` an operation
    /// performs regardless of how many unrelated `len`/`get` calls interleave.
    pub(crate) struct FailOpAt {
        op: &'static str,
        at: u64,
        kind: io::ErrorKind,
        seen: AtomicU64,
    }

    impl FailOpAt {
        pub(crate) fn new(op: &'static str, at: u64, kind: io::ErrorKind) -> Self {
            Self {
                op,
                at,
                kind,
                seen: AtomicU64::new(0),
            }
        }
    }

    impl FaultPolicy for FailOpAt {
        fn next_fault(&self, op: &'static str, _seq: u64) -> Option<io::Error> {
            if op != self.op {
                return None;
            }
            let n = self.seen.fetch_add(1, Ordering::SeqCst);
            (n == self.at).then(|| io::Error::new(self.kind, format!("injected fault at {op}#{n}")))
        }
    }

    /// Fails a pseudo-random subset of consultations at rate `per_mille`
    /// (faults per thousand), deterministically from `seed` and an internal
    /// counter. The counter is the policy's own — not the stack's `seq` — so it
    /// survives the disarm/re-arm the fault fuzz drivers perform around every
    /// operation, keeping the schedule reproducible across arming boundaries.
    ///
    /// Shared by every fault fuzz suite (`alloc_fault_tests`,
    /// `alloc_uninit_fuzz_tests`) so their fault schedules stay identical.
    pub(crate) struct RandomFaults {
        seed: u64,
        per_mille: u64,
        counter: AtomicU64,
    }

    impl RandomFaults {
        pub(crate) fn new(seed: u64, per_mille: u64) -> Self {
            Self {
                seed,
                per_mille,
                counter: AtomicU64::new(0),
            }
        }
    }

    impl FaultPolicy for RandomFaults {
        fn next_fault(&self, op: &'static str, _seq: u64) -> Option<io::Error> {
            let n = self.counter.fetch_add(1, Ordering::Relaxed);
            // splitmix64 hash of (seed, n) → uniform u64.
            let mut z = self
                .seed
                .wrapping_add(n.wrapping_mul(0x9E37_79B9_7F4A_7C15));
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            (z % 1000 < self.per_mille)
                .then(|| io::Error::other(format!("injected fault at {op} (n={n})")))
        }
    }

    /// Fault rate in faults-per-thousand, overridable via `BSTACK_FAULT_PER_MILLE`.
    pub(crate) fn per_mille() -> u64 {
        std::env::var("BSTACK_FAULT_PER_MILLE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30)
    }
}
