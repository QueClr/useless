pub mod combining;
pub mod database;
pub mod kdtree;
pub mod nbody;

use crate::harness::{self, BenchConfig, ThreadRecorder};
use crate::schedule::Schedule;
use crate::stats::BenchmarkResult;

#[allow(dead_code)]
pub trait Workload: Sync + 'static {
    type State: Send;
    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
    fn init_state(&self) -> Self::State;
    fn run_thread<S: Schedule<Self::State>>(
        &self,
        lock: &S,
        thread_id: usize,
        ops: usize,
        recorder: &mut ThreadRecorder,
    );
}

pub fn all_workload_names() -> Vec<&'static str> {
    vec!["combining", "nbody", "kdtree", "database"]
}

fn bench_workload_with_lock<W, S>(
    workload: &W,
    thread_count: usize,
    ops_per_thread: usize,
    iterations: usize,
    warmup: usize,
) -> BenchmarkResult
where
    W: Workload,
    S: Schedule<W::State>,
{
    let config = BenchConfig {
        thread_count,
        ops_per_thread,
    };

    let mut wall_times = Vec::new();
    let mut throughputs = Vec::new();
    let mut latencies = Vec::new();

    for i in 0..(warmup + iterations) {
        let lock = S::new(workload.init_state());
        let result = harness::run_iteration(&config, &|tid, barrier, recorder| {
            barrier.wait();
            workload.run_thread(&lock, tid, ops_per_thread, recorder);
        });
        if i >= warmup {
            wall_times.push(result.wall_time_ms);
            throughputs.push(result.throughput_ops_sec);
            latencies.push(result.latency);
        }
    }

    BenchmarkResult::from_iterations(
        workload.name(),
        S::name(),
        thread_count,
        ops_per_thread,
        &wall_times,
        &throughputs,
        &latencies,
    )
}

fn bench_workload<W: Workload>(
    workload: &W,
    thread_counts: &[usize],
    ops_per_thread: usize,
    iterations: usize,
    warmup: usize,
) -> Vec<BenchmarkResult> {
    let mut results = Vec::new();
    for &tc in thread_counts {
        println!(
            "  [{}] threads={} lock=lamlock ...",
            workload.name(),
            tc
        );
        results.push(bench_workload_with_lock::<W, lamlock::Lock<W::State>>(
            workload,
            tc,
            ops_per_thread,
            iterations,
            warmup,
        ));
        println!(
            "  [{}] threads={} lock=std-mutex ...",
            workload.name(),
            tc
        );
        results.push(bench_workload_with_lock::<W, std::sync::Mutex<W::State>>(
            workload,
            tc,
            ops_per_thread,
            iterations,
            warmup,
        ));
    }
    results
}

pub fn run_all(
    workload_names: &[String],
    thread_counts: &[usize],
    ops_per_thread: usize,
    iterations: usize,
    warmup: usize,
) -> Vec<BenchmarkResult> {
    let mut results = Vec::new();
    for name in workload_names {
        println!("Running workload: {}", name);
        let r = match name.as_str() {
            "combining" => bench_workload(
                &combining::CombiningWorkload,
                thread_counts,
                ops_per_thread,
                iterations,
                warmup,
            ),
            "nbody" => bench_workload(
                &nbody::NbodyWorkload,
                thread_counts,
                ops_per_thread,
                iterations,
                warmup,
            ),
            "kdtree" => bench_workload(
                &kdtree::KdTreeWorkload,
                thread_counts,
                ops_per_thread,
                iterations,
                warmup,
            ),
            "database" => bench_workload(
                &database::DatabaseWorkload,
                thread_counts,
                ops_per_thread,
                iterations,
                warmup,
            ),
            other => {
                eprintln!("Unknown workload: {}", other);
                continue;
            }
        };
        results.extend(r);
    }
    results
}
