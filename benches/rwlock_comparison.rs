//! Benchmarks comparing `std::sync::RwLock`, `parking_lot::RwLock`, and
//! `usync::RwLock` as backends for [`BStack`].
//!
//! # How to run
//!
//! Run this benchmark three times — once per lock implementation — to compare
//! all three in the same Criterion output directory:
//!
//! ```sh
//! cargo bench --bench rwlock_comparison                        # std (baseline)
//! cargo bench --bench rwlock_comparison --features parking_lot # parking_lot
//! cargo bench --bench rwlock_comparison --features usync       # usync
//! ```
//!
//! The benchmark IDs embed the active lock name (e.g.
//! `push_single_threaded/parking_lot/512_ops`) so Criterion can overlay all
//! three runs in the same HTML report when they share a baseline directory.
//!
//! # What is benchmarked
//!
//! **Part A — raw lock microbenchmarks** (all three types in one binary):
//! - Uncontested write acquisition / release
//! - Uncontested read acquisition / release
//! - Contended write acquisition under N concurrent writer threads (2, 4, 8)
//!
//! **Part B — `BStack` operation benchmarks** (compiled-in lock only):
//! - `push`           — single-threaded
//! - `pop` / `discard` — single-threaded
//! - `peek` / `get`   — single-threaded read
//! - `extend`         — single-threaded zero-fill append
//! - `len`            — single-threaded metadata read
//! - Mixed push/pop/peek — single-threaded interleaved read+write
//! - Contended `push` — multi-threaded (2, 4, 8 threads)
//! - Contended mixed  — multi-threaded (2, 4 threads)

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::fs;
use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

// ─────────────────────────────────────────────────────────────────────────────
// Lock label embedded in every benchmark ID so Criterion can overlay runs.
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "parking_lot")]
const LOCK: &str = "parking_lot";
#[cfg(all(feature = "usync", not(feature = "parking_lot")))]
const LOCK: &str = "usync";
#[cfg(not(any(feature = "parking_lot", feature = "usync")))]
const LOCK: &str = "std";

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn temp_path(tag: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "bstack_bench_{}_{}_{}.bin",
        LOCK,
        tag,
        std::process::id()
    ));
    p
}

fn remove(p: &std::path::Path) {
    let _ = fs::remove_file(p);
}

// ─────────────────────────────────────────────────────────────────────────────
// Part A: raw lock microbenchmarks — all three types in one binary
// ─────────────────────────────────────────────────────────────────────────────

fn lock_micro_uncontested(c: &mut Criterion) {
    let mut group = c.benchmark_group("lock_micro_uncontested");

    // std — write
    {
        let lock = std::sync::RwLock::new(0u64);
        group.bench_function("std/write", |b| {
            b.iter(|| {
                let mut g = lock.write().unwrap();
                *g = g.wrapping_add(1);
            });
        });
    }
    // parking_lot — write
    {
        let lock = parking_lot::RwLock::new(0u64);
        group.bench_function("parking_lot/write", |b| {
            b.iter(|| {
                let mut g = lock.write();
                *g = g.wrapping_add(1);
            });
        });
    }
    // usync — write
    {
        let lock = usync::RwLock::new(0u64);
        group.bench_function("usync/write", |b| {
            b.iter(|| {
                let mut g = lock.write();
                *g = g.wrapping_add(1);
            });
        });
    }

    // std — read
    {
        let lock = std::sync::RwLock::new(42u64);
        group.bench_function("std/read", |b| {
            b.iter(|| {
                let g = lock.read().unwrap();
                black_box(*g);
            });
        });
    }
    // parking_lot — read
    {
        let lock = parking_lot::RwLock::new(42u64);
        group.bench_function("parking_lot/read", |b| {
            b.iter(|| {
                let g = lock.read();
                black_box(*g);
            });
        });
    }
    // usync — read
    {
        let lock = usync::RwLock::new(42u64);
        group.bench_function("usync/read", |b| {
            b.iter(|| {
                let g = lock.read();
                black_box(*g);
            });
        });
    }

    group.finish();
}

fn lock_micro_contended(c: &mut Criterion) {
    const OPS_PER_THREAD: usize = 200;

    let mut group = c.benchmark_group("lock_micro_contended_write");
    group.measurement_time(Duration::from_secs(10));

    for &n_threads in &[2usize, 4, 8] {
        group.throughput(Throughput::Elements((OPS_PER_THREAD * n_threads) as u64));

        // std
        group.bench_with_input(BenchmarkId::new("std", n_threads), &n_threads, |b, &n| {
            let lock = Arc::new(std::sync::RwLock::new(0u64));
            b.iter(|| {
                let barrier = Arc::new(Barrier::new(n + 1));
                let mut handles = Vec::with_capacity(n);
                for _ in 0..n {
                    let l = Arc::clone(&lock);
                    let bar = Arc::clone(&barrier);
                    handles.push(thread::spawn(move || {
                        bar.wait();
                        for _ in 0..OPS_PER_THREAD {
                            let mut g = l.write().unwrap();
                            *g = g.wrapping_add(1);
                        }
                    }));
                }
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
            });
        });

        // parking_lot
        group.bench_with_input(
            BenchmarkId::new("parking_lot", n_threads),
            &n_threads,
            |b, &n| {
                let lock = Arc::new(parking_lot::RwLock::new(0u64));
                b.iter(|| {
                    let barrier = Arc::new(Barrier::new(n + 1));
                    let mut handles = Vec::with_capacity(n);
                    for _ in 0..n {
                        let l = Arc::clone(&lock);
                        let bar = Arc::clone(&barrier);
                        handles.push(thread::spawn(move || {
                            bar.wait();
                            for _ in 0..OPS_PER_THREAD {
                                let mut g = l.write();
                                *g = g.wrapping_add(1);
                            }
                        }));
                    }
                    barrier.wait();
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );

        // usync
        group.bench_with_input(BenchmarkId::new("usync", n_threads), &n_threads, |b, &n| {
            let lock = Arc::new(usync::RwLock::new(0u64));
            b.iter(|| {
                let barrier = Arc::new(Barrier::new(n + 1));
                let mut handles = Vec::with_capacity(n);
                for _ in 0..n {
                    let l = Arc::clone(&lock);
                    let bar = Arc::clone(&barrier);
                    handles.push(thread::spawn(move || {
                        bar.wait();
                        for _ in 0..OPS_PER_THREAD {
                            let mut g = l.write();
                            *g = g.wrapping_add(1);
                        }
                    }));
                }
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
            });
        });
    }

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Part B: BStack operation benchmarks (compiled-in lock, identified by LOCK)
// ─────────────────────────────────────────────────────────────────────────────

const PAYLOAD_SMALL: &[u8] = &[0x42u8; 32];
const PAYLOAD_LARGE: &[u8] = &[0xABu8; 512];

// B1: push — single-threaded
fn bstack_push_st(c: &mut Criterion) {
    use bstack::BStack;

    const OPS: u64 = 512;
    let mut group = c.benchmark_group("push_single_threaded");
    group.throughput(Throughput::Elements(OPS));
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new(LOCK, format!("{OPS}_ops")), |b| {
        let path = temp_path("push_st");
        remove(&path);
        let stack = BStack::open(&path).unwrap();
        b.iter(|| {
            for _ in 0..OPS {
                stack.push(PAYLOAD_SMALL).unwrap();
            }
            stack.discard(OPS * PAYLOAD_SMALL.len() as u64).unwrap();
        });
        remove(&path);
    });

    group.finish();
}

// B2: pop — single-threaded
fn bstack_pop_st(c: &mut Criterion) {
    use bstack::BStack;

    const OPS: u64 = 512;
    let item = PAYLOAD_SMALL.len() as u64;
    let mut group = c.benchmark_group("pop_single_threaded");
    group.throughput(Throughput::Elements(OPS));
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new(LOCK, format!("{OPS}_ops")), |b| {
        let path = temp_path("pop_st");
        remove(&path);
        let stack = BStack::open(&path).unwrap();
        // Pre-fill once outside the timed loop.
        for _ in 0..OPS {
            stack.push(PAYLOAD_SMALL).unwrap();
        }
        b.iter(|| {
            let mut buf = [0u8; 32];
            for _ in 0..OPS {
                stack.pop_into(&mut buf).unwrap();
            }
            // Refill for next iteration.
            for _ in 0..OPS {
                stack.push(PAYLOAD_SMALL).unwrap();
            }
        });
        remove(&path);
    });

    group.finish();
}

// B3: discard — single-threaded bulk removal
fn bstack_discard_st(c: &mut Criterion) {
    use bstack::BStack;

    const FILL: u64 = 16 * 1024; // 16 KiB
    let mut group = c.benchmark_group("discard_single_threaded");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new(LOCK, "16KiB"), |b| {
        let path = temp_path("discard_st");
        remove(&path);
        let stack = BStack::open(&path).unwrap();
        b.iter(|| {
            stack.extend(FILL).unwrap();
            stack.discard(FILL).unwrap();
        });
        remove(&path);
    });

    group.finish();
}

// B4: extend — single-threaded zero-fill append
fn bstack_extend_st(c: &mut Criterion) {
    use bstack::BStack;

    const OPS: u64 = 256;
    const CHUNK: u64 = 64;
    let mut group = c.benchmark_group("extend_single_threaded");
    group.throughput(Throughput::Bytes(OPS * CHUNK));
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new(LOCK, format!("{OPS}_x_{CHUNK}B")), |b| {
        let path = temp_path("extend_st");
        remove(&path);
        let stack = BStack::open(&path).unwrap();
        b.iter(|| {
            for _ in 0..OPS {
                stack.extend(CHUNK).unwrap();
            }
            stack.discard(OPS * CHUNK).unwrap();
        });
        remove(&path);
    });

    group.finish();
}

// B5: peek / get — single-threaded reads
fn bstack_peek_st(c: &mut Criterion) {
    use bstack::BStack;

    const OPS: u64 = 512;
    const ITEM: usize = PAYLOAD_SMALL.len();

    let mut group = c.benchmark_group("peek_single_threaded");
    group.throughput(Throughput::Elements(OPS));
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new(LOCK, format!("{OPS}_ops")), |b| {
        let path = temp_path("peek_st");
        remove(&path);
        let stack = BStack::open(&path).unwrap();
        // Fill with data to peek at.
        for _ in 0..OPS {
            stack.push(PAYLOAD_SMALL).unwrap();
        }
        let top = stack.len().unwrap() - ITEM as u64;
        b.iter(|| {
            let mut buf = [0u8; ITEM];
            for _ in 0..OPS {
                stack.peek_into(top, &mut buf).unwrap();
                black_box(buf);
            }
        });
        remove(&path);
    });

    group.finish();
}

// B6: get (range read) — single-threaded
fn bstack_get_st(c: &mut Criterion) {
    use bstack::BStack;

    const RANGE: u64 = 512;
    let mut group = c.benchmark_group("get_single_threaded");
    group.throughput(Throughput::Bytes(RANGE));
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new(LOCK, format!("{RANGE}B_range")), |b| {
        let path = temp_path("get_st");
        remove(&path);
        let stack = BStack::open(&path).unwrap();
        stack.extend(RANGE).unwrap();
        b.iter(|| {
            let mut buf = [0u8; RANGE as usize];
            stack.get_into(0, &mut buf).unwrap();
            black_box(buf);
        });
        remove(&path);
    });

    group.finish();
}

// B7: len — single-threaded metadata read
fn bstack_len_st(c: &mut Criterion) {
    use bstack::BStack;

    const OPS: u64 = 1024;
    let mut group = c.benchmark_group("len_single_threaded");
    group.throughput(Throughput::Elements(OPS));
    group.measurement_time(Duration::from_secs(5));

    group.bench_function(BenchmarkId::new(LOCK, format!("{OPS}_ops")), |b| {
        let path = temp_path("len_st");
        remove(&path);
        let stack = BStack::open(&path).unwrap();
        stack.extend(256).unwrap();
        b.iter(|| {
            for _ in 0..OPS {
                black_box(stack.len().unwrap());
            }
        });
        remove(&path);
    });

    group.finish();
}

// B8: large-payload push — single-threaded
fn bstack_push_large_st(c: &mut Criterion) {
    use bstack::BStack;

    const OPS: u64 = 128;
    let mut group = c.benchmark_group("push_large_payload_single_threaded");
    group.throughput(Throughput::Bytes(OPS * PAYLOAD_LARGE.len() as u64));
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(
        BenchmarkId::new(LOCK, format!("{OPS}_x_{}B", PAYLOAD_LARGE.len())),
        |b| {
            let path = temp_path("push_large_st");
            remove(&path);
            let stack = BStack::open(&path).unwrap();
            b.iter(|| {
                for _ in 0..OPS {
                    stack.push(PAYLOAD_LARGE).unwrap();
                }
                stack.discard(OPS * PAYLOAD_LARGE.len() as u64).unwrap();
            });
            remove(&path);
        },
    );

    group.finish();
}

// B9: mixed push/pop/peek — single-threaded
fn bstack_mixed_st(c: &mut Criterion) {
    use bstack::BStack;

    // Pattern per iteration: 2 pushes, 1 pop, 1 peek, 1 len (×64 rounds)
    const ROUNDS: u64 = 64;
    const OPS_PER_ROUND: u64 = 5;
    let total = ROUNDS * OPS_PER_ROUND;

    let mut group = c.benchmark_group("mixed_single_threaded");
    group.throughput(Throughput::Elements(total));
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new(LOCK, format!("{ROUNDS}_rounds")), |b| {
        let path = temp_path("mixed_st");
        remove(&path);
        let stack = BStack::open(&path).unwrap();
        // Pre-fill so pops don't underflow.
        for _ in 0..ROUNDS {
            stack.push(PAYLOAD_SMALL).unwrap();
        }
        b.iter(|| {
            let mut buf = [0u8; 32];
            for _ in 0..ROUNDS {
                stack.push(PAYLOAD_SMALL).unwrap();
                stack.push(PAYLOAD_SMALL).unwrap();
                stack.pop_into(&mut buf).unwrap();
                let top = stack.len().unwrap() - buf.len() as u64;
                stack.peek_into(top, &mut buf).unwrap();
                black_box(stack.len().unwrap());
            }
        });
        // Clean up.
        let remaining = stack.len().unwrap();
        stack.discard(remaining).unwrap();
        remove(&path);
    });

    group.finish();
}

// B10: push — multi-threaded contended write
fn bstack_push_mt(c: &mut Criterion) {
    use bstack::BStack;

    const OPS_PER_THREAD: usize = 64;

    let mut group = c.benchmark_group("push_multi_threaded");
    group.measurement_time(Duration::from_secs(15));

    for &n_threads in &[2usize, 4, 8] {
        let total = (OPS_PER_THREAD * n_threads) as u64;
        group.throughput(Throughput::Elements(total));

        group.bench_with_input(BenchmarkId::new(LOCK, n_threads), &n_threads, |b, &n| {
            let path = temp_path(&format!("push_mt_{n}"));
            remove(&path);
            let stack = Arc::new(BStack::open(&path).unwrap());
            b.iter(|| {
                let barrier = Arc::new(Barrier::new(n + 1));
                let mut handles = Vec::with_capacity(n);
                for _ in 0..n {
                    let s = Arc::clone(&stack);
                    let bar = Arc::clone(&barrier);
                    handles.push(thread::spawn(move || {
                        bar.wait();
                        for _ in 0..OPS_PER_THREAD {
                            s.push(PAYLOAD_SMALL).unwrap();
                        }
                    }));
                }
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                stack.discard(total * PAYLOAD_SMALL.len() as u64).unwrap();
            });
            remove(&path);
        });
    }

    group.finish();
}

// B11: mixed push/pop/peek — multi-threaded contended
fn bstack_mixed_mt(c: &mut Criterion) {
    use bstack::BStack;

    const OPS_PER_THREAD: usize = 32;

    let mut group = c.benchmark_group("mixed_multi_threaded");
    group.measurement_time(Duration::from_secs(15));

    for &n_threads in &[2usize, 4] {
        group.throughput(Throughput::Elements((OPS_PER_THREAD * n_threads) as u64));

        group.bench_with_input(BenchmarkId::new(LOCK, n_threads), &n_threads, |b, &n| {
            let path = temp_path(&format!("mixed_mt_{n}"));
            remove(&path);
            let stack = Arc::new(BStack::open(&path).unwrap());
            // Pre-fill so pop threads don't underflow immediately.
            for _ in 0..n * OPS_PER_THREAD {
                stack.push(PAYLOAD_SMALL).unwrap();
            }
            b.iter(|| {
                let barrier = Arc::new(Barrier::new(n + 1));
                let mut handles = Vec::with_capacity(n);
                for thread_idx in 0..n {
                    let s = Arc::clone(&stack);
                    let bar = Arc::clone(&barrier);
                    handles.push(thread::spawn(move || {
                        bar.wait();
                        let mut buf = [0u8; 32];
                        for i in 0..OPS_PER_THREAD {
                            // Even threads push; odd threads pop.
                            if (thread_idx + i) % 2 == 0 {
                                let _ = s.push(PAYLOAD_SMALL);
                            } else {
                                let _ = s.pop_into(&mut buf);
                            }
                        }
                    }));
                }
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
            });
            // Drain remainder.
            let remaining = stack.len().unwrap();
            if remaining > 0 {
                stack.discard(remaining).unwrap();
            }
            remove(&path);
        });
    }

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Criterion groups
// ─────────────────────────────────────────────────────────────────────────────

criterion_group!(lock_micro, lock_micro_uncontested, lock_micro_contended,);
criterion_group!(
    bstack_single_threaded,
    bstack_push_st,
    bstack_pop_st,
    bstack_discard_st,
    bstack_extend_st,
    bstack_peek_st,
    bstack_get_st,
    bstack_len_st,
    bstack_push_large_st,
    bstack_mixed_st,
);
criterion_group!(bstack_multi_threaded, bstack_push_mt, bstack_mixed_mt,);

criterion_main!(lock_micro, bstack_single_threaded, bstack_multi_threaded);
