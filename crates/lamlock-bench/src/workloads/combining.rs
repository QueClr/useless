use rand::Rng;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

use crate::schedule::Schedule;
use crate::workloads::Workload;

/// Batch size: how many samples each thread submits per lock.schedule() call.
const BATCH_SIZE: usize = 1000;

/// Number of buckets in the frequency table — makes the data structure
/// span many cache lines so flat-combining's cache-locality advantage materializes.
const NUM_BUCKETS: usize = 4096;

/// Running statistics tracker with a large frequency table.
/// The frequency table spans 4096 × 8 bytes = 32KB, which is roughly L1 cache size.
/// Every sample touches multiple fields spread across the struct, making
/// cache-line bouncing expensive under std::sync::Mutex but free under flat-combining.
pub struct RunningStats {
    pub count: u64,
    pub sum: f64,
    pub sum_sq: f64,
    pub min: f64,
    pub max: f64,
    pub histogram: [u64; NUM_BUCKETS],
}

impl RunningStats {
    pub fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            sum_sq: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            histogram: [0; NUM_BUCKETS],
        }
    }

    fn add_sample(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        self.sum_sq += value * value;
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
        // Scatter updates across many cache lines: hash the value to a bucket
        // and also update neighboring buckets to simulate realistic analytics
        let base = ((value * 64.0) as usize) % NUM_BUCKETS;
        self.histogram[base] += 1;
        // Touch a few nearby buckets to increase cache footprint per op
        self.histogram[(base + 1) % NUM_BUCKETS] += 1;
        self.histogram[(base + 37) % NUM_BUCKETS] += 1;
        self.histogram[(base + 997) % NUM_BUCKETS] += 1;
    }
}

pub struct CombiningWorkload;

impl Workload for CombiningWorkload {
    type State = RunningStats;

    fn name(&self) -> &'static str {
        "combining"
    }

    fn description(&self) -> &'static str {
        "Stats aggregator with large histogram — batched, cache-sensitive"
    }

    fn init_state(&self) -> Self::State {
        RunningStats::new()
    }

    fn run_thread<S: Schedule<Self::State>>(
        &self,
        lock: &S,
        thread_id: usize,
        _thread_count: usize,
        ops: usize,
    ) {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(thread_id as u64 * 12345 + 67890);
        let samples: Vec<f64> = (0..ops).map(|_| rng.random::<f64>() * 64.0).collect();

        for batch in samples.chunks(BATCH_SIZE) {
            lock.schedule(|state| {
                for &sample in batch {
                    state.add_sample(sample);
                }
            });
        }
    }
}
