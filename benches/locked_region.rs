use bstack::BStack;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use std::sync::atomic::{AtomicU64, Ordering};

const LOCK_SIZE: u64 = 1024 * 1024; // 1 MiB locked region
const READ_SIZES: &[usize] = &[8, 64, 512, 4096];

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_path(prefix: &str) -> std::path::PathBuf {
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("bstack_bench_{prefix}_{pid}_{id}.bin"))
}

fn setup_stack(cached: bool) -> (BStack, std::path::PathBuf) {
    let path = tmp_path(if cached { "cached" } else { "pread" });
    let s = if cached {
        BStack::open_cached(&path).unwrap()
    } else {
        BStack::open(&path).unwrap()
    };
    // Write LOCK_SIZE bytes of data and lock the region.
    let chunk = vec![0xABu8; 4096];
    let mut written = 0u64;
    while written < LOCK_SIZE {
        let n = chunk.len().min((LOCK_SIZE - written) as usize);
        s.push(&chunk[..n]).unwrap();
        written += n as u64;
    }
    s.lock_up_to(LOCK_SIZE).unwrap();
    (s, path)
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("locked_region/get");

    for &read_size in READ_SIZES {
        // pread baseline
        let (s, path) = setup_stack(false);
        group.bench_with_input(BenchmarkId::new("pread", read_size), &read_size, |b, &n| {
            b.iter(|| black_box(s.get(0, n as u64).unwrap()))
        });
        drop(s);
        let _ = std::fs::remove_file(&path);

        // cache
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

        // pread baseline
        let (s, path) = setup_stack(false);
        group.bench_with_input(BenchmarkId::new("pread", read_size), &read_size, |b, &n| {
            b.iter(|| {
                s.get_into(0, &mut buf[..n]).unwrap();
                black_box(&buf);
            })
        });
        drop(s);
        let _ = std::fs::remove_file(&path);

        // cache
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

criterion_group!(benches, bench_get, bench_get_into);
criterion_main!(benches);
