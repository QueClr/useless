use rand::Rng;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

use crate::harness::ThreadRecorder;
use crate::schedule::Schedule;
use crate::workloads::Workload;

const NUM_BODIES: usize = 128;
const G: f64 = 6.674e-11;
const DT: f64 = 0.01;
const SOFTENING: f64 = 1e-9;

#[derive(Clone)]
pub struct Body {
    pub pos: [f64; 3],
    pub vel: [f64; 3],
    pub mass: f64,
}

pub struct NbodyWorkload;

impl Workload for NbodyWorkload {
    type State = Vec<Body>;

    fn name(&self) -> &'static str {
        "nbody"
    }

    fn description(&self) -> &'static str {
        "N-body gravitational simulation — compute forces outside lock, apply updates inside"
    }

    fn init_state(&self) -> Self::State {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(0xDEADBEEF);
        (0..NUM_BODIES)
            .map(|_| Body {
                pos: [
                    rng.random::<f64>() * 100.0,
                    rng.random::<f64>() * 100.0,
                    rng.random::<f64>() * 100.0,
                ],
                vel: [0.0; 3],
                mass: rng.random::<f64>() * 1e10 + 1e8,
            })
            .collect()
    }

    fn run_thread<S: Schedule<Self::State>>(
        &self,
        lock: &S,
        thread_id: usize,
        ops: usize,
        recorder: &mut ThreadRecorder,
    ) {
        // Each thread is responsible for a subset of bodies
        let start = (thread_id * NUM_BODIES) / num_threads_hint(thread_id);
        let end = ((thread_id + 1) * NUM_BODIES) / num_threads_hint(thread_id);
        let my_range = start..end.min(NUM_BODIES);

        recorder.record();
        for _ in 0..ops {
            // Read current positions (snapshot under lock)
            let snapshot: Vec<Body> = lock.schedule(|bodies| bodies.clone());

            // Compute force deltas outside the lock
            let mut accels = vec![[0.0f64; 3]; my_range.len()];
            for (local_i, i) in my_range.clone().enumerate() {
                for j in 0..NUM_BODIES {
                    if i == j {
                        continue;
                    }
                    let dx = snapshot[j].pos[0] - snapshot[i].pos[0];
                    let dy = snapshot[j].pos[1] - snapshot[i].pos[1];
                    let dz = snapshot[j].pos[2] - snapshot[i].pos[2];
                    let dist_sq = dx * dx + dy * dy + dz * dz + SOFTENING;
                    let inv_dist = 1.0 / dist_sq.sqrt();
                    let inv_dist3 = inv_dist * inv_dist * inv_dist;
                    let force = G * snapshot[j].mass * inv_dist3;
                    accels[local_i][0] += force * dx;
                    accels[local_i][1] += force * dy;
                    accels[local_i][2] += force * dz;
                }
            }

            // Apply updates under lock
            lock.schedule(|bodies| {
                for (local_i, i) in my_range.clone().enumerate() {
                    for d in 0..3 {
                        bodies[i].vel[d] += accels[local_i][d] * DT;
                        bodies[i].pos[d] += bodies[i].vel[d] * DT;
                    }
                }
            });
            recorder.record();
        }
    }
}

/// Estimate total thread count from thread_id. Since we don't pass thread_count
/// into the workload, we use a heuristic: available parallelism.
fn num_threads_hint(_thread_id: usize) -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
