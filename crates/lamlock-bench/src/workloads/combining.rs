use rand::Rng;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

use crate::harness::ThreadRecorder;
use crate::schedule::Schedule;
use crate::workloads::Workload;

pub struct RunningStats {
    pub count: u64,
    pub sum: f64,
    pub sum_sq: f64,
    pub min: f64,
    pub max: f64,
    pub histogram: [u64; 64],
}

impl RunningStats {
    pub fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            sum_sq: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            histogram: [0; 64],
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
        let bucket = ((value.clamp(0.0, 63.0)) as usize).min(63);
        self.histogram[bucket] += 1;
    }
}

pub struct CombiningWorkload;

impl Workload for CombiningWorkload {
    type State = RunningStats;

    fn name(&self) -> &'static str {
        "combining"
    }

    fn description(&self) -> &'static str {
        "Concurrent stats aggregator — small uniform writes, ideal for flat-combining"
    }

    fn init_state(&self) -> Self::State {
        RunningStats::new()
    }

    fn run_thread<S: Schedule<Self::State>>(
        &self,
        lock: &S,
        thread_id: usize,
        ops: usize,
        recorder: &mut ThreadRecorder,
    ) {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(thread_id as u64 * 12345 + 67890);
        let samples: Vec<f64> = (0..ops).map(|_| rng.random::<f64>() * 64.0).collect();

        recorder.record();
        for &sample in &samples {
            lock.schedule(|state| {
                state.add_sample(sample);
            });
            recorder.record();
        }
    }
}
