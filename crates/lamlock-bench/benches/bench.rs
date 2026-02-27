use std::sync::Barrier;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use lamlock::Lock;

use lamlock_bench::schedule::Schedule;
use lamlock_bench::workloads::btree::BTreeWorkload;
use lamlock_bench::workloads::hashtable::HashTableWorkload;
use lamlock_bench::workloads::pqueue::PQueueWorkload;
use lamlock_bench::workloads::ringbuf::RingBufWorkload;
use lamlock_bench::workloads::slab::SlabWorkload;
use lamlock_bench::workloads::stack::StackWorkload;
use lamlock_bench::workloads::Workload;

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
    group.measurement_time(std::time::Duration::from_secs(10));
    for &threads in &[64, 128, 256, 512] {
        // lamlock (futex + panic-safe)
        group.bench_with_input(BenchmarkId::new("lamlock", threads), &threads, |b, &t| {
            run_bench::<W, Lock<W::State, true, true>>(b, t, OPS_PER_THREAD, workload)
        });
        // lamlock-no-panic (futex, no panic handling)
        group.bench_with_input(
            BenchmarkId::new("lamlock-no-panic", threads),
            &threads,
            |b, &t| run_bench::<W, Lock<W::State, true, false>>(b, t, OPS_PER_THREAD, workload),
        );
        // lamlock-spin (spin-only, panic-safe)
        group.bench_with_input(
            BenchmarkId::new("lamlock-spin", threads),
            &threads,
            |b, &t| run_bench::<W, Lock<W::State, false, true>>(b, t, OPS_PER_THREAD, workload),
        );
        // lamlock-spin-no-panic (spin-only, no panic handling)
        group.bench_with_input(
            BenchmarkId::new("lamlock-spin-no-panic", threads),
            &threads,
            |b, &t| run_bench::<W, Lock<W::State, false, false>>(b, t, OPS_PER_THREAD, workload),
        );
        // std-mutex baseline
        group.bench_with_input(BenchmarkId::new("std-mutex", threads), &threads, |b, &t| {
            run_bench::<W, std::sync::Mutex<W::State>>(b, t, OPS_PER_THREAD, workload)
        });
    }
    group.finish();
}

fn bench_slab(c: &mut Criterion) {
    bench_workload(c, &SlabWorkload);
}

fn bench_pqueue(c: &mut Criterion) {
    bench_workload(c, &PQueueWorkload);
}

fn bench_ringbuf(c: &mut Criterion) {
    bench_workload(c, &RingBufWorkload);
}

fn bench_stack(c: &mut Criterion) {
    bench_workload(c, &StackWorkload);
}

fn bench_hashtable(c: &mut Criterion) {
    bench_workload(c, &HashTableWorkload);
}

fn bench_btree(c: &mut Criterion) {
    bench_workload(c, &BTreeWorkload);
}

criterion_group!(
    benches,
    bench_slab,
    bench_pqueue,
    bench_ringbuf,
    bench_stack,
    bench_hashtable,
    bench_btree,
);
criterion_main!(benches);
