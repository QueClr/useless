use std::collections::HashMap;

use rand::Rng;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

use crate::schedule::Schedule;
use crate::workloads::Workload;

const INITIAL_ENTRIES: usize = 10_000;
const VALUE_SIZE: usize = 64;
const ZIPF_EXPONENT: f64 = 1.2;

/// Batch size: how many DB ops each thread submits per lock.schedule() call.
const BATCH_SIZE: usize = 1000;

/// Zipfian distribution: generates keys biased toward hot keys.
fn zipfian_key(rng: &mut Xoshiro256PlusPlus, n: u64) -> u64 {
    let u: f64 = rng.random::<f64>();
    let rank = ((u * (n as f64).powf(1.0 - ZIPF_EXPONENT) + (1.0 - u)).powf(1.0 / (1.0 - ZIPF_EXPONENT))) as u64;
    rank.min(n - 1)
}

/// Fixed-size value to avoid heap allocation inside the lock.
type Value = [u8; VALUE_SIZE];

#[derive(Clone, Copy)]
enum DbOp {
    Read(u64),
    Write(u64, Value),
    Delete(u64),
    Scan(u64, usize),
}

pub struct DatabaseWorkload;

impl Workload for DatabaseWorkload {
    type State = HashMap<u64, Value>;

    fn name(&self) -> &'static str {
        "database"
    }

    fn description(&self) -> &'static str {
        "Key-value store — batched ops with fixed-size values, no allocations inside lock"
    }

    fn init_state(&self) -> Self::State {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(0xFEEDFACE);
        let mut map = HashMap::with_capacity(INITIAL_ENTRIES);
        for i in 0..INITIAL_ENTRIES as u64 {
            let mut value = [0u8; VALUE_SIZE];
            for byte in &mut value {
                *byte = rng.random::<u8>();
            }
            map.insert(i, value);
        }
        map
    }

    fn run_thread<S: Schedule<Self::State>>(
        &self,
        lock: &S,
        thread_id: usize,
        _thread_count: usize,
        ops: usize,
    ) {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(thread_id as u64 * 99999 + 42);
        let n = INITIAL_ENTRIES as u64;

        // Pre-generate operations with fixed-size values (no heap allocation)
        let operations: Vec<DbOp> = (0..ops)
            .map(|_| {
                let roll: f64 = rng.random();
                if roll < 0.80 {
                    DbOp::Read(zipfian_key(&mut rng, n))
                } else if roll < 0.90 {
                    let key = zipfian_key(&mut rng, n);
                    let mut value = [0u8; VALUE_SIZE];
                    for byte in &mut value {
                        *byte = rng.random::<u8>();
                    }
                    DbOp::Write(key, value)
                } else if roll < 0.95 {
                    DbOp::Delete(zipfian_key(&mut rng, n))
                } else {
                    let start = zipfian_key(&mut rng, n);
                    let count = rng.random_range(1..=10);
                    DbOp::Scan(start, count)
                }
            })
            .collect();

        for batch in operations.chunks(BATCH_SIZE) {
            lock.schedule(|db| {
                for op in batch {
                    match op {
                        DbOp::Read(key) => {
                            let _ = db.get(key);
                        }
                        DbOp::Write(key, value) => {
                            db.insert(*key, *value);
                        }
                        DbOp::Delete(key) => {
                            db.remove(key);
                        }
                        DbOp::Scan(start, count) => {
                            for k in *start..*start + *count as u64 {
                                let _ = db.get(&k);
                            }
                        }
                    }
                }
            });
        }
    }
}
