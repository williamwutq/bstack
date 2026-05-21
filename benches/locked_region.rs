use bstack::BStack;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};

const LOCK_SIZE: u64 = 1024 * 1024; // 1 MiB locked region
const READ_SIZES: &[usize] = &[8, 64, 512, 4096];

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_path(prefix: &str) -> std::path::PathBuf {
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("bstack_bench_{prefix}_{pid}_{id}.bin"))
}

fn setup_stack(cached: bool) -> (Arc<BStack>, std::path::PathBuf) {
    let path = tmp_path(if cached { "cached" } else { "pread" });
    let s = if cached {
        BStack::open_cached(&path).unwrap()
    } else {
        BStack::open(&path).unwrap()
    };
    let chunk = vec![0xABu8; 4096];
    let mut written = 0u64;
    while written < LOCK_SIZE {
        let n = chunk.len().min((LOCK_SIZE - written) as usize);
        s.push(&chunk[..n]).unwrap();
        written += n as u64;
    }
    s.lock_up_to(LOCK_SIZE).unwrap();
    (Arc::new(s), path)
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("locked_region/get");

    for &read_size in READ_SIZES {
        let (s, path) = setup_stack(false);
        group.bench_with_input(BenchmarkId::new("pread", read_size), &read_size, |b, &n| {
            b.iter(|| black_box(s.get(0, n as u64).unwrap()))
        });
        drop(s);
        let _ = std::fs::remove_file(&path);

        let (s, path) = setup_stack(true);
        group.bench_with_input(BenchmarkId::new("cache", read_size), &read_size, |b, &n| {
            b.iter(|| black_box(s.get(0, n as u64).unwrap()))
        });
        drop(s);
        let _ = std::fs::remove_file(&path);
    }

    group.finish();
}

fn bench_get_into(c: &mut Criterion) {
    let mut group = c.benchmark_group("locked_region/get_into");

    for &read_size in READ_SIZES {
        let mut buf = vec![0u8; read_size];

        let (s, path) = setup_stack(false);
        group.bench_with_input(BenchmarkId::new("pread", read_size), &read_size, |b, &n| {
            b.iter(|| {
                s.get_into(0, &mut buf[..n]).unwrap();
                black_box(&buf);
            })
        });
        drop(s);
        let _ = std::fs::remove_file(&path);

        let (s, path) = setup_stack(true);
        group.bench_with_input(BenchmarkId::new("cache", read_size), &read_size, |b, &n| {
            b.iter(|| {
                s.get_into(0, &mut buf[..n]).unwrap();
                black_box(&buf);
            })
        });
        drop(s);
        let _ = std::fs::remove_file(&path);
    }

    group.finish();
}

/// Concurrent benchmark: `num_threads` threads all reading from the same stack
/// simultaneously.  Uses `iter_custom` so each measured sample distributes
/// `iters` operations evenly across threads and reports wall time / iter,
/// exposing Mutex contention on the cache path.
fn bench_get_concurrent(c: &mut Criterion, num_threads: usize) {
    let mut group = c.benchmark_group(format!("locked_region/get_concurrent_{num_threads}t"));

    for &read_size in READ_SIZES {
        let (s, path) = setup_stack(false);
        group.bench_with_input(BenchmarkId::new("pread", read_size), &read_size, |b, &n| {
            b.iter_custom(|iters| {
                let per_thread = (iters / num_threads as u64).max(1);
                let barrier = Arc::new(Barrier::new(num_threads + 1));
                let handles: Vec<_> = (0..num_threads)
                    .map(|_| {
                        let stack = Arc::clone(&s);
                        let bar = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            bar.wait();
                            for _ in 0..per_thread {
                                black_box(stack.get(0, n as u64).unwrap());
                            }
                        })
                    })
                    .collect();
                barrier.wait();
                let start = std::time::Instant::now();
                for h in handles {
                    h.join().unwrap();
                }
                // Report elapsed as if we did `iters` total operations so
                // Criterion's time/iter reflects per-op wall latency under
                // contention.
                start.elapsed()
            });
        });
        drop(s);
        let _ = std::fs::remove_file(&path);

        let (s, path) = setup_stack(true);
        group.bench_with_input(BenchmarkId::new("cache", read_size), &read_size, |b, &n| {
            b.iter_custom(|iters| {
                let per_thread = (iters / num_threads as u64).max(1);
                let barrier = Arc::new(Barrier::new(num_threads + 1));
                let handles: Vec<_> = (0..num_threads)
                    .map(|_| {
                        let stack = Arc::clone(&s);
                        let bar = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            bar.wait();
                            for _ in 0..per_thread {
                                black_box(stack.get(0, n as u64).unwrap());
                            }
                        })
                    })
                    .collect();
                barrier.wait();
                let start = std::time::Instant::now();
                for h in handles {
                    h.join().unwrap();
                }
                start.elapsed()
            });
        });
        drop(s);
        let _ = std::fs::remove_file(&path);
    }

    group.finish();
}

fn bench_get_concurrent_32(c: &mut Criterion) {
    bench_get_concurrent(c, 32);
}

fn bench_get_concurrent_64(c: &mut Criterion) {
    bench_get_concurrent(c, 64);
}

criterion_group!(
    benches,
    bench_get,
    bench_get_into,
    bench_get_concurrent_32,
    bench_get_concurrent_64
);
criterion_main!(benches);
