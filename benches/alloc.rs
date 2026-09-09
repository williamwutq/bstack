//! Cross-allocator throughput benchmark for the `bstack` disk allocators.
//!
//! This mirrors the machinery of the allocator fuzz / fault-injection suites
//! (`src/alloc_fuzz_tests.rs`, `src/alloc_fault_tests.rs`): a per-run temp
//! backing file guarded by RAII, a `make_allocator!` constructor macro, a
//! weighted random operation generator, and a size generator. The debug/log
//! plumbing of the original driver is dropped — a benchmark only needs numbers.
//!
//! Three ideas carried over from the reference bench earn their keep:
//!
//! * **Pre-generated decisions** — every operation and every length is drawn
//!   from a seeded RNG *before* the timed region, so the measurement captures
//!   allocator I/O rather than RNG cost, and every sample replays the identical
//!   workload.
//! * **Multi-threaded drivers** — each allocator is exercised at several thread
//!   counts over one shared allocator, surfacing the cost of the internal
//!   serialization (`Mutex`) the `atomic` feature adds. Reported time is the
//!   slowest thread's wall clock, i.e. the true parallel latency of the batch.
//! * **Named, swappable workloads** — the operation mix and the size
//!   distribution are named presets ([`OpMix`], [`SizeDist`]) selectable from
//!   the environment, so a run can be re-shaped without editing or recompiling.
//!
//! The concurrent path requires the allocators to be `Sync`, which they are
//! only under the `atomic` feature — hence this bench's `required-features`.
//!
//! # Configuration
//!
//! All knobs are read once from the environment (see [`BenchConfig::from_env`]):
//!
//! | Variable                  | Meaning                                            | Default        |
//! |---------------------------|----------------------------------------------------|----------------|
//! | `BSTACK_BENCH_OP`         | op mix: preset name, or a `alloc,realloc,dealloc` weight triple | `mixed` |
//! | `BSTACK_BENCH_SIZE`       | size distribution (see [`SizeDist`])               | `uniform`      |
//! | `BSTACK_BENCH_MAX`        | maximum allocation length drawn                    | `1024`         |
//! | `BSTACK_BENCH_THREADS`    | comma-separated thread counts                      | `1,2,4,16`     |
//! | `BSTACK_BENCH_PRE_ALLOC`  | live allocations pre-populated per benchmark       | `256`          |
//! | `BSTACK_BENCH_SEED`       | seed for the decision stream                       | `48`           |
//!
//! Op-mix presets: `mixed`, `alloc-only`, `alloc-heavy`, `realloc-heavy`,
//! `churn`. Size presets: `uniform`, `fixed`, `gamma[:k:theta_frac]`,
//! `bimodal[:small:p_large]`.
//!
//! Uses the 0.4.0 slice API: [`BStackOwnedSliceAllocator`] /
//! [`BStackOwnedSlice`], with `alloc` / `realloc` / `dealloc` returning owned
//! handles and a [`BStackAllocError`] carrying the surviving region on failure.

#[cfg(feature = "debug-no-sync")]
compile_error!(
    "benches/alloc.rs requires the `debug-no-sync` feature to be disabled; benchmarking without the durable fsync is pointless"
);

use bstack::{
    BStack, BStackOwnedSlice, BStackOwnedSliceAllocator, CheckedSlabBStackAllocator,
    FirstFitBStackAllocator, GhostTreeBstackAllocator, SegregatedBStackAllocator,
    SlabBStackAllocator,
};
use criterion::{Criterion, criterion_group, criterion_main};
use rand::{RngExt, SeedableRng, rngs::StdRng};
use std::hint::black_box;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Barrier, OnceLock};
use std::time::{Duration, Instant};

// --- defaults (overridable via the environment) -----------------------------

const DEFAULT_PRE_ALLOC: usize = 256;
const DEFAULT_THREADS: &[usize] = &[1, 2, 4, 16];
const DEFAULT_SEED: u64 = 48;
const DEFAULT_MAX_SIZE: u64 = 1024;

// --- temp-file plumbing (mirrors alloc_test_common / locked_region) ---------

/// RAII guard that removes a temp backing file when dropped.
struct Guard(std::path::PathBuf);
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// A process-unique temp path for a `.bstack` backing file.
fn temp_path(prefix: &str) -> std::path::PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let safe: String = prefix
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    std::env::temp_dir().join(format!("bstack_bench_{safe}_{pid}_{id}.bin"))
}

/// Build a `Fn(BStack) -> io::Result<A>` constructor, matching the fuzz suites'
/// `make_allocator!`: slab-style allocators take a block size on a fresh arena
/// and reattach an existing one via `open`; single-constructor allocators use
/// their `new` directly.
macro_rules! make_allocator {
    ($ty:ty, $size:expr) => {
        |bs: BStack| {
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

// --- operation mix ----------------------------------------------------------

/// One benchmark operation. Unlike the fuzz `Operation`, this carries no payload
/// or integrity metadata — the bench measures allocator I/O, not correctness.
#[derive(Clone, Copy)]
enum Operation {
    Alloc,
    Realloc,
    Dealloc,
}

/// A configurable weighting of the three operations. Weights are relative (they
/// need not sum to 100); an all-zero mix degenerates to pure alloc.
#[derive(Clone, Copy)]
struct OpMix {
    alloc: u32,
    realloc: u32,
    dealloc: u32,
}

impl OpMix {
    /// Growth-biased, keeps the live set populated. Mirrors the fuzz generator.
    const MIXED: Self = Self {
        alloc: 45,
        realloc: 20,
        dealloc: 35,
    };
    /// Pure allocation churn — no reuse, arena only ever grows. Isolates the
    /// bump/extend fast path.
    const ALLOC_ONLY: Self = Self {
        alloc: 100,
        realloc: 0,
        dealloc: 0,
    };
    /// Allocation-dominated with a little recycling.
    const ALLOC_HEAVY: Self = Self {
        alloc: 70,
        realloc: 10,
        dealloc: 20,
    };
    /// Resize-dominated — stresses the copy/grow/shrink paths.
    const REALLOC_HEAVY: Self = Self {
        alloc: 25,
        realloc: 55,
        dealloc: 20,
    };
    /// Steady state: allocations and frees balanced, so the live set and arena
    /// size hover — exercises free-list reuse rather than pure growth.
    const CHURN: Self = Self {
        alloc: 40,
        realloc: 20,
        dealloc: 40,
    };

    /// Draw one operation according to the weights.
    fn sample(&self, rng: &mut StdRng) -> Operation {
        let total = self.alloc + self.realloc + self.dealloc;
        if total == 0 {
            return Operation::Alloc;
        }
        let roll = rng.random_range(0..total);
        if roll < self.alloc {
            Operation::Alloc
        } else if roll < self.alloc + self.realloc {
            Operation::Realloc
        } else {
            Operation::Dealloc
        }
    }

    /// Parse a preset name or an `alloc,realloc,dealloc` weight triple, returning
    /// the mix and a compact label for the benchmark id.
    fn parse(s: &str) -> (Self, String) {
        match s.trim().to_ascii_lowercase().as_str() {
            "mixed" => (Self::MIXED, "mixed".into()),
            "alloc-only" | "alloc_only" | "alloconly" => (Self::ALLOC_ONLY, "alloc-only".into()),
            "alloc-heavy" | "alloc_heavy" => (Self::ALLOC_HEAVY, "alloc-heavy".into()),
            "realloc-heavy" | "realloc_heavy" => (Self::REALLOC_HEAVY, "realloc-heavy".into()),
            "churn" => (Self::CHURN, "churn".into()),
            custom => {
                let parts: Vec<u32> = custom
                    .split(',')
                    .filter_map(|p| p.trim().parse().ok())
                    .collect();
                if let [a, r, d] = parts[..] {
                    (
                        Self {
                            alloc: a,
                            realloc: r,
                            dealloc: d,
                        },
                        format!("op{a}-{r}-{d}"),
                    )
                } else {
                    eprintln!("bench: unrecognised BSTACK_BENCH_OP={custom:?}, using `mixed`");
                    (Self::MIXED, "mixed".into())
                }
            }
        }
    }
}

/// ## Size distribution
/// How allocation lengths are drawn, given a per-allocator upper bound `max`.
///
/// Each variant models a different real workload shape. All are clamped to
/// `0..=max` so slab allocators (which reject over-block requests) stay valid.
#[derive(Clone, Copy)]
enum SizeDist {
    /// Every allocation is exactly `max` bytes. Models homogeneous fixed-size
    /// records — the natural slab workload.
    Fixed,
    /// Uniform over `0..=max`. The neutral baseline; no shape assumptions.
    Uniform,
    /// Gamma(`k`, `theta`) clamped to `0..=max`, with `theta = max * theta_frac`.
    /// A smooth right-skewed curve: a mass of small allocations with a light
    /// tail of large ones — the classic object-heap size profile. With `k > 1`
    /// the density is zero at zero, rises to a mode at `(k-1)*theta`, then
    /// decays. Defaults (`k=2`, `theta_frac=0.25`) put the mode near `max/4`.
    Gamma { k: f64, theta_frac: f64 },
    /// Bimodal: a `small`-byte allocation with probability `1 - p_large`,
    /// otherwise a full `max`-byte one. Models mixed metadata-plus-blob traffic.
    Bimodal { small: u64, p_large: f64 },
}

impl SizeDist {
    /// Draw one length in `0..=max`.
    fn sample(&self, rng: &mut StdRng, max: u64) -> u64 {
        match *self {
            SizeDist::Fixed => max,
            SizeDist::Uniform => rng.random_range(0..=max),
            SizeDist::Gamma { k, theta_frac } => {
                let theta = (max as f64) * theta_frac;
                gamma_sample_u64(rng, max, k, theta)
            }
            SizeDist::Bimodal { small, p_large } => {
                if rng.random_bool(p_large) {
                    max
                } else {
                    small.min(max)
                }
            }
        }
    }

    /// Parse `uniform` | `fixed` | `gamma[:k:theta_frac]` | `bimodal[:small:p]`.
    fn parse(s: &str) -> (Self, String) {
        let s = s.trim().to_ascii_lowercase();
        let mut it = s.split(':');
        let kind = it.next().unwrap_or("uniform");
        let a = it.next();
        let b = it.next();
        match kind {
            "fixed" => (SizeDist::Fixed, "fixed".into()),
            "uniform" => (SizeDist::Uniform, "uniform".into()),
            "gamma" => {
                let k = a.and_then(|v| v.parse().ok()).unwrap_or(2.0);
                let theta_frac = b.and_then(|v| v.parse().ok()).unwrap_or(0.25);
                (
                    SizeDist::Gamma { k, theta_frac },
                    format!("gamma-k{k}-t{theta_frac}"),
                )
            }
            "bimodal" => {
                let small = a.and_then(|v| v.parse().ok()).unwrap_or(16);
                let p_large = b.and_then(|v| v.parse().ok()).unwrap_or(0.1);
                (
                    SizeDist::Bimodal { small, p_large },
                    format!("bimodal-s{small}-p{p_large}"),
                )
            }
            other => {
                eprintln!("bench: unrecognised BSTACK_BENCH_SIZE={other:?}, using `uniform`");
                (SizeDist::Uniform, "uniform".into())
            }
        }
    }
}

/// Sample from the standard Gamma(`k`) distribution (scale 1) via the
/// Marsaglia-Tsang method. Returns a non-negative `f64`.
fn sample_gamma_standard(rng: &mut StdRng, k: f64) -> f64 {
    assert!(k > 0.0, "shape k must be positive");

    if k < 1.0 {
        // Boost trick: Gamma(k) == Gamma(k+1) * U^(1/k).
        let u = rng.random::<f64>();
        return sample_gamma_standard(rng, k + 1.0) * u.powf(1.0 / k);
    }

    // Marsaglia-Tsang (valid for k >= 1).
    let d = k - 1.0 / 3.0;
    let c = 1.0 / (9.0 * d).sqrt();

    loop {
        // Standard normal via Box-Muller. Draw u1 in (0, 1] so ln() is finite.
        let u1 = 1.0 - rng.random::<f64>();
        let u2 = rng.random::<f64>();
        let x = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();

        let v_raw = 1.0 + c * x;
        if v_raw <= 0.0 {
            continue;
        }
        let v = v_raw * v_raw * v_raw; // v = (1 + c*x)^3

        let u = 1.0 - rng.random::<f64>();

        // Squeeze check (fast path), then the exact log check (rarely reached).
        if u < 1.0 - 0.0331 * (x * x) * (x * x) {
            return d * v;
        }
        if u.ln() < 0.5 * x * x + d * (1.0 - v + v.ln()) {
            return d * v;
        }
    }
}

/// Draw a `u64` in `0..=max` from Gamma(`k`, `theta`) by rejection.
///
/// The gamma distribution is unbounded above, so out-of-range draws are
/// re-sampled; with a mode chosen near the middle of the range this is rare. A
/// degenerate `theta <= 0` (i.e. `max == 0`) collapses to `0`.
fn gamma_sample_u64(rng: &mut StdRng, max: u64, k: f64, theta: f64) -> u64 {
    if max == 0 || theta <= 0.0 {
        return 0;
    }
    loop {
        let sample = sample_gamma_standard(rng, k) * theta;
        let scaled = sample.round();
        if (0.0..=max as f64).contains(&scaled) {
            return scaled as u64;
        }
    }
}

/// The whole run's configuration, resolved once from the environment.
struct BenchConfig {
    op_mix: OpMix,
    op_label: String,
    size_dist: SizeDist,
    size_label: String,
    /// Maximum allocation length drawn. Slab allocators impose no upper bound
    /// (large requests just span multiple blocks), so this is a pure workload
    /// knob, not a per-allocator constraint.
    max: u64,
    threads: Vec<usize>,
    pre_alloc: usize,
    seed: u64,
}

impl BenchConfig {
    fn from_env() -> Self {
        fn var(key: &str) -> Option<String> {
            std::env::var(key).ok().filter(|v| !v.trim().is_empty())
        }
        let (op_mix, op_label) =
            OpMix::parse(&var("BSTACK_BENCH_OP").unwrap_or_else(|| "mixed".into()));
        let (size_dist, size_label) =
            SizeDist::parse(&var("BSTACK_BENCH_SIZE").unwrap_or_else(|| "uniform".into()));
        let threads = var("BSTACK_BENCH_THREADS")
            .map(|s| {
                s.split(',')
                    .filter_map(|p| p.trim().parse().ok())
                    .collect::<Vec<usize>>()
            })
            .filter(|v: &Vec<usize>| !v.is_empty())
            .unwrap_or_else(|| DEFAULT_THREADS.to_vec());
        let cfg = Self {
            op_mix,
            op_label,
            size_dist,
            size_label,
            max: var("BSTACK_BENCH_MAX")
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_MAX_SIZE),
            threads,
            pre_alloc: var("BSTACK_BENCH_PRE_ALLOC")
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_PRE_ALLOC),
            seed: var("BSTACK_BENCH_SEED")
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_SEED),
        };
        eprintln!(
            "bstack alloc bench: op={} size={} max={} threads={:?} pre_alloc={} seed={}",
            cfg.op_label, cfg.size_label, cfg.max, cfg.threads, cfg.pre_alloc, cfg.seed,
        );
        cfg
    }
}

fn config() -> &'static BenchConfig {
    static CFG: OnceLock<BenchConfig> = OnceLock::new();
    CFG.get_or_init(BenchConfig::from_env)
}

/// Benchmark one allocator at one thread count.
///
/// Decisions (ops + sizes) are drawn from a seeded RNG outside the timed region
/// and split evenly across threads; the last thread absorbs any remainder. Each
/// thread pre-populates its own live set (untimed), all threads rendezvous on a
/// barrier, and only the operation replay is timed. The reported per-sample
/// duration is the slowest thread's elapsed time — the batch's parallel latency.
fn bench_allocator<A, M, OG, SG>(
    c: &mut Criterion,
    thread_count: usize,
    group_name: &str,
    bench_name: &str,
    make: M,
    op_gen: OG,
    size_gen: SG,
) where
    A: BStackOwnedSliceAllocator + Sync,
    M: Fn(BStack) -> io::Result<A>,
    OG: Fn(&mut StdRng) -> Operation,
    SG: Fn(&mut StdRng) -> u64,
{
    assert!(thread_count >= 1, "thread_count must be at least 1");
    let cfg = config();
    let mut group = c.benchmark_group(group_name);
    group.sampling_mode(criterion::SamplingMode::Flat);
    group.bench_function(bench_name, |b| {
        b.iter_custom(|iters| {
            // --- setup (not timed) ---
            let path = temp_path(bench_name);
            let _guard = Guard(path.clone());
            let alloc = make(BStack::open(&path).unwrap()).unwrap();

            // Pre-generate all decisions from one seeded stream.
            let mut rng = StdRng::seed_from_u64(cfg.seed);
            let all_ops: Vec<Operation> = (0..iters).map(|_| op_gen(&mut rng)).collect();
            let all_pre_sizes: Vec<u64> = (0..cfg.pre_alloc).map(|_| size_gen(&mut rng)).collect();
            let all_sizes: Vec<u64> = (0..iters as usize).map(|_| size_gen(&mut rng)).collect();

            let iters_per_thread = iters as usize / thread_count;
            let pre_per_thread = cfg.pre_alloc / thread_count;

            // Per-thread (pre_sizes, ops, sizes); the last thread takes the tail.
            let thread_data: Vec<(&[u64], &[Operation], &[u64])> = (0..thread_count)
                .map(|tid| {
                    let last = tid == thread_count - 1;
                    let pre_start = tid * pre_per_thread;
                    let pre_end = if last {
                        cfg.pre_alloc
                    } else {
                        pre_start + pre_per_thread
                    };
                    let op_start = tid * iters_per_thread;
                    let op_end = if last {
                        iters as usize
                    } else {
                        op_start + iters_per_thread
                    };
                    (
                        &all_pre_sizes[pre_start..pre_end],
                        &all_ops[op_start..op_end],
                        &all_sizes[op_start..op_end],
                    )
                })
                .collect();

            // Barrier: no thread starts timing until every thread has finished
            // pre-allocation, so setup cost never leaks into the measurement.
            let barrier = Barrier::new(thread_count);

            std::thread::scope(|s| {
                let handles: Vec<_> = thread_data
                    .into_iter()
                    .enumerate()
                    .map(|(tid, (pre_sizes, ops, sizes))| {
                        let alloc = &alloc;
                        let barrier = &barrier;
                        s.spawn(move || {
                            let mut live: Vec<BStackOwnedSlice<'_, A>> =
                                Vec::with_capacity(pre_sizes.len());
                            // Distinct per-thread seed so index choices don't correlate.
                            let mut rng = StdRng::seed_from_u64(cfg.seed + tid as u64);

                            // Pre-populate (not timed). Failures just shrink the pool.
                            for &len in pre_sizes {
                                if let Ok(sl) = alloc.alloc(len) {
                                    live.push(sl);
                                }
                            }

                            barrier.wait();

                            // --- timed region ---
                            let start = Instant::now();
                            for (&len, &op) in sizes.iter().zip(ops.iter()) {
                                match op {
                                    Operation::Alloc => {
                                        if let Ok(sl) = alloc.alloc(len) {
                                            live.push(sl);
                                        }
                                    }
                                    Operation::Realloc => {
                                        if live.is_empty() {
                                            if let Ok(sl) = alloc.alloc(len) {
                                                live.push(sl);
                                            }
                                        } else {
                                            let i = rng.random_range(0..live.len());
                                            let sl = live.swap_remove(i);
                                            match alloc.realloc(sl, len) {
                                                Ok(sl) => live.push(sl),
                                                // Re-track the survivor so the pool
                                                // stays representative across errors.
                                                Err(e) => {
                                                    if let Some(h) = e.handle {
                                                        live.push(h);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Operation::Dealloc => {
                                        if live.is_empty() {
                                            if let Ok(sl) = alloc.alloc(len) {
                                                live.push(sl);
                                            }
                                        } else {
                                            let i = rng.random_range(0..live.len());
                                            let sl = live.swap_remove(i);
                                            if let Err(e) = alloc.dealloc(sl) {
                                                if let Some(h) = e.handle {
                                                    live.push(h);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            let elapsed = start.elapsed();
                            black_box(&live);
                            elapsed
                        })
                    })
                    .collect();

                // Parallel latency of the batch is the slowest thread.
                handles
                    .into_iter()
                    .map(|h| h.join().unwrap())
                    .max()
                    .unwrap_or(Duration::ZERO)
            })
            // `alloc`, `live`, then `_guard` drop here (not timed).
        });
    });
    group.finish();
}

/// Register `make` at every configured thread count under `group`, drawing sizes
/// from the active [`SizeDist`] (up to `cfg.max`) and ops from the active
/// [`OpMix`]. The benchmark id encodes the workload so distinct configs don't
/// clobber each other's baselines.
macro_rules! register {
    ($c:expr, $group:expr, $make:expr) => {{
        let cfg = config();
        let max = cfg.max;
        for &t in &cfg.threads {
            let mix = cfg.op_mix;
            let dist = cfg.size_dist;
            let name = format!("{}/{}/{t}t", cfg.op_label, cfg.size_label);
            bench_allocator(
                $c,
                t,
                $group,
                &name,
                $make,
                move |rng| mix.sample(rng),
                move |rng| dist.sample(rng, max),
            );
        }
    }};
}

fn bench_first_fit(c: &mut Criterion) {
    register!(
        c,
        "alloc/first_fit",
        make_allocator!(FirstFitBStackAllocator)
    );
}

fn bench_ghost_tree(c: &mut Criterion) {
    register!(
        c,
        "alloc/ghost_tree",
        make_allocator!(GhostTreeBstackAllocator)
    );
}

fn bench_segregated(c: &mut Criterion) {
    register!(
        c,
        "alloc/segregated",
        make_allocator!(SegregatedBStackAllocator)
    );
}

fn bench_slab_16(c: &mut Criterion) {
    register!(c, "alloc/slab_16", make_allocator!(SlabBStackAllocator, 16));
}

fn bench_slab_24(c: &mut Criterion) {
    register!(c, "alloc/slab_24", make_allocator!(SlabBStackAllocator, 24));
}

fn bench_slab_32(c: &mut Criterion) {
    register!(c, "alloc/slab_32", make_allocator!(SlabBStackAllocator, 32));
}

fn bench_slab_64(c: &mut Criterion) {
    register!(c, "alloc/slab_64", make_allocator!(SlabBStackAllocator, 64));
}

fn bench_slab_128(c: &mut Criterion) {
    register!(
        c,
        "alloc/slab_128",
        make_allocator!(SlabBStackAllocator, 128)
    );
}

fn bench_checked_slab_16(c: &mut Criterion) {
    register!(
        c,
        "alloc/checked_slab_16",
        make_allocator!(CheckedSlabBStackAllocator, 16)
    );
}

fn bench_checked_slab_24(c: &mut Criterion) {
    register!(
        c,
        "alloc/checked_slab_24",
        make_allocator!(CheckedSlabBStackAllocator, 24)
    );
}

fn bench_checked_slab_32(c: &mut Criterion) {
    register!(
        c,
        "alloc/checked_slab_32",
        make_allocator!(CheckedSlabBStackAllocator, 32)
    );
}

fn bench_checked_slab_64(c: &mut Criterion) {
    register!(
        c,
        "alloc/checked_slab_64",
        make_allocator!(CheckedSlabBStackAllocator, 64)
    );
}

fn bench_checked_slab_128(c: &mut Criterion) {
    register!(
        c,
        "alloc/checked_slab_128",
        make_allocator!(CheckedSlabBStackAllocator, 128)
    );
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(60));
    targets =
        bench_first_fit,
        bench_ghost_tree,
        bench_segregated,
        bench_slab_16,
        bench_slab_24,
        bench_slab_32,
        bench_slab_64,
        bench_slab_128,
        bench_checked_slab_16,
        bench_checked_slab_24,
        bench_checked_slab_32,
        bench_checked_slab_64,
        bench_checked_slab_128
}
criterion_main!(benches);
