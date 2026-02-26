use std::sync::Barrier;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use lamlock::Lock;

use lamlock_bench::schedule::Schedule;
use lamlock_bench::workloads::Workload;
use lamlock_bench::workloads::btree::BTreeWorkload;
use lamlock_bench::workloads::combining::CombiningWorkload;
use lamlock_bench::workloads::database::DatabaseWorkload;
use lamlock_bench::workloads::kdtree::KdTreeWorkload;
use lamlock_bench::workloads::lru::LruWorkload;
use lamlock_bench::workloads::nbody::NbodyWorkload;
use lamlock_bench::workloads::slab::SlabWorkload;
use lamlock_bench::workloads::wal::WalWorkload;

const OPS_PER_THREAD: usize = 5_000;

fn run_bench<W: Workload, S: Schedule<W::State>>(
    b: &mut criterion::Bencher,
    threads: usize,
    ops: usize,
    w: &W,
) {
    b.iter_custom(|iters| {
        let mut total = Duration::ZERO;
        for _ in 0..iters {
            let lock = S::new(w.init_state());
            let barrier = Barrier::new(threads + 1);
            let start = std::thread::scope(|scope| {
                for tid in 0..threads {
                    let barrier = &barrier;
                    let lock = &lock;
                    scope.spawn(move || {
                        barrier.wait();
                        w.run_thread(lock, tid, threads, ops);
                    });
                }
                barrier.wait();
                Instant::now()
            });
            total += start.elapsed();
        }
        total
    });
}

fn bench_workload<W: Workload>(c: &mut Criterion, workload: &W) {
    let mut group = c.benchmark_group(workload.name());
    group.sample_size(10);
    for &threads in &[1, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("lamlock", threads),
            &threads,
            |b, &t| run_bench::<W, Lock<W::State>>(b, t, OPS_PER_THREAD, workload),
        );
        group.bench_with_input(
            BenchmarkId::new("std-mutex", threads),
            &threads,
            |b, &t| {
                run_bench::<W, std::sync::Mutex<W::State>>(b, t, OPS_PER_THREAD, workload)
            },
        );
    }
    group.finish();
}

fn bench_combining(c: &mut Criterion) {
    bench_workload(c, &CombiningWorkload);
}

fn bench_nbody(c: &mut Criterion) {
    bench_workload(c, &NbodyWorkload);
}

fn bench_kdtree(c: &mut Criterion) {
    bench_workload(c, &KdTreeWorkload);
}

fn bench_database(c: &mut Criterion) {
    bench_workload(c, &DatabaseWorkload);
}

fn bench_btree(c: &mut Criterion) {
    bench_workload(c, &BTreeWorkload);
}

fn bench_slab(c: &mut Criterion) {
    bench_workload(c, &SlabWorkload);
}

fn bench_wal(c: &mut Criterion) {
    bench_workload(c, &WalWorkload);
}

fn bench_lru(c: &mut Criterion) {
    bench_workload(c, &LruWorkload);
}

criterion_group!(
    benches,
    bench_combining,
    bench_nbody,
    bench_kdtree,
    bench_database,
    bench_btree,
    bench_slab,
    bench_wal,
    bench_lru
);
criterion_main!(benches);
