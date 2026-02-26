use rand::Rng;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

use crate::schedule::Schedule;
use crate::workloads::Workload;

const NUM_BODIES: usize = 128;
const G: f64 = 6.674e-11;
const DT: f64 = 0.01;
const SOFTENING: f64 = 1e-9;

/// Batch size: how many full simulation steps each thread submits per lock.schedule() call.
/// Each step computes all pairwise forces and applies velocity/position updates.
/// This is a heavy critical section that keeps the body array cache-hot.
const BATCH_SIZE: usize = 8;

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
        "N-body gravitational simulation — batched full-step updates inside lock"
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
        thread_count: usize,
        ops: usize,
    ) {
        // Each thread is responsible for a subset of bodies
        let start = (thread_id * NUM_BODIES) / thread_count;
        let end = ((thread_id + 1) * NUM_BODIES) / thread_count;
        let my_range = start..end.min(NUM_BODIES);

        // Submit work in BATCHES — each batch does multiple simulation steps
        // with force computation + position updates all inside the lock,
        // keeping the body array cache-hot for the combiner.
        for _ in (0..ops).step_by(BATCH_SIZE) {
            let steps = BATCH_SIZE.min(ops);
            let range = my_range.clone();
            lock.schedule(move |bodies: &mut Vec<Body>| {
                let mut accels = vec![[0.0f64; 3]; range.len()];
                for _ in 0..steps {
                    // Compute forces for our range
                    for accel in accels.iter_mut() {
                        *accel = [0.0; 3];
                    }
                    for (local_i, i) in range.clone().enumerate() {
                        for j in 0..NUM_BODIES {
                            if i == j {
                                continue;
                            }
                            let dx = bodies[j].pos[0] - bodies[i].pos[0];
                            let dy = bodies[j].pos[1] - bodies[i].pos[1];
                            let dz = bodies[j].pos[2] - bodies[i].pos[2];
                            let dist_sq = dx * dx + dy * dy + dz * dz + SOFTENING;
                            let inv_dist = 1.0 / dist_sq.sqrt();
                            let inv_dist3 = inv_dist * inv_dist * inv_dist;
                            let force = G * bodies[j].mass * inv_dist3;
                            accels[local_i][0] += force * dx;
                            accels[local_i][1] += force * dy;
                            accels[local_i][2] += force * dz;
                        }
                    }
                    // Apply updates
                    for (local_i, i) in range.clone().enumerate() {
                        for d in 0..3 {
                            bodies[i].vel[d] += accels[local_i][d] * DT;
                            bodies[i].pos[d] += bodies[i].vel[d] * DT;
                        }
                    }
                }
            });
        }
    }
}
