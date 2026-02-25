use std::sync::Barrier;
use std::time::Instant;

use crate::stats::LatencyStats;

pub struct BenchConfig {
    pub thread_count: usize,
    pub ops_per_thread: usize,
}

pub struct ThreadRecorder {
    timestamps: Vec<Instant>,
}

impl ThreadRecorder {
    pub fn new(capacity: usize) -> Self {
        Self {
            timestamps: Vec::with_capacity(capacity + 1),
        }
    }

    pub fn record(&mut self) {
        self.timestamps.push(Instant::now());
    }

    pub fn latencies_ns(&self) -> Vec<f64> {
        self.timestamps
            .windows(2)
            .map(|w| w[1].duration_since(w[0]).as_nanos() as f64)
            .collect()
    }
}

pub struct IterationResult {
    pub wall_time_ms: f64,
    pub throughput_ops_sec: f64,
    pub latency: LatencyStats,
}

/// Run a single iteration: spawns `thread_count` threads, each calling `thread_fn`.
/// `thread_fn(thread_id, barrier, recorder)` — must call `barrier.wait()` before starting work.
pub fn run_iteration(
    config: &BenchConfig,
    thread_fn: &(dyn Fn(usize, &Barrier, &mut ThreadRecorder) + Sync),
) -> IterationResult {
    let barrier = Barrier::new(config.thread_count + 1);
    let total_ops = config.thread_count * config.ops_per_thread;

    std::thread::scope(|scope| {
        let mut handles = Vec::new();

        for tid in 0..config.thread_count {
            let barrier_ref = &barrier;
            let handle = scope.spawn(move || {
                let mut recorder = ThreadRecorder::new(config.ops_per_thread);
                thread_fn(tid, barrier_ref, &mut recorder);
                recorder
            });
            handles.push(handle);
        }

        // Synchronize start
        barrier.wait();
        let wall_start = Instant::now();

        // Wait for all threads and collect recorders
        let mut all_latencies: Vec<f64> = Vec::new();
        for h in handles {
            let recorder = h.join().unwrap();
            all_latencies.extend(recorder.latencies_ns());
        }

        let wall_elapsed = wall_start.elapsed();
        let wall_time_ms = wall_elapsed.as_secs_f64() * 1000.0;
        let throughput_ops_sec = total_ops as f64 / wall_elapsed.as_secs_f64();
        let latency = LatencyStats::from_samples(&mut all_latencies);

        IterationResult {
            wall_time_ms,
            throughput_ops_sec,
            latency,
        }
    })
}
