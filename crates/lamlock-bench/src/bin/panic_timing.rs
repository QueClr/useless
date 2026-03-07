//! Measures lock cleanup time after a panic inside a critical section.
//!
//! For each (variant, thread_count) configuration:
//!   1. Spawn N threads running concurrent B-tree operations.
//!   2. When any thread reaches 80% of its own batches, it panics
//!      inside `lock.schedule()`, recording the instant just before.
//!   3. The lock's bomb propagates poisoning; all other threads observe
//!      `LockPoisoned` and unwind.
//!   4. Cleanup time = time from panic instant to `thread::scope` exit.
//!
//! Since every thread panics at 80%, no thread can finish normally —
//! all remaining threads still have ≥20% of work left when the first
//! panic fires.
//!
//! Compares futex (`Lock<_, true, true>`) vs spin (`Lock<_, false, true>`).
//!
//! Run: `cargo run --bin panic_timing --release -p lamlock-bench`

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Barrier;
use std::time::{Duration, Instant};

use lamlock::Lock;
use lamlock_bench::schedule::Schedule;
use lamlock_bench::workloads::btree::{BTree, BTreeWorkload};
use lamlock_bench::workloads::Workload;
use rand::Rng;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

const OPS_PER_THREAD: usize = 5_000;
const BATCH_SIZE: usize = 1000;
const KEY_RANGE: u64 = 10_000;
const PANIC_PERCENT: usize = 80;
const ITERATIONS: usize = 50;
const THREADS: &[usize] = &[2, 4, 8, 16, 32, 64, 128, 256, 512];

/// Batch index at which a thread panics.
/// With 5 batches per thread (5000 ops / 1000 batch size) and 80%,
/// this is batch index 4 (0-indexed), i.e. the 5th and last batch.
const PANIC_AT_BATCH: usize = (OPS_PER_THREAD / BATCH_SIZE) * PANIC_PERCENT / 100;

// Duplicated from btree.rs (private there).
#[derive(Clone, Copy)]
enum BTreeOp {
    Insert(u64, u64),
    Search(u64),
    Delete(u64),
}

fn generate_ops(rng: &mut Xoshiro256PlusPlus, count: usize) -> Vec<BTreeOp> {
    (0..count)
        .map(|_| {
            let r: f64 = rng.random();
            if r < 0.45 {
                BTreeOp::Insert(rng.random_range(0..KEY_RANGE), rng.random::<u64>())
            } else if r < 0.90 {
                BTreeOp::Delete(rng.random_range(0..KEY_RANGE))
            } else {
                BTreeOp::Search(rng.random_range(0..KEY_RANGE))
            }
        })
        .collect()
}

fn execute_batch(tree: &mut BTree, batch: &[BTreeOp]) -> u64 {
    let mut checksum = 0u64;
    for op in batch {
        match *op {
            BTreeOp::Insert(k, v) => {
                if tree.insert(k, v) {
                    checksum = checksum.wrapping_add(1);
                }
            }
            BTreeOp::Search(k) => {
                if let Some(v) = tree.search(k) {
                    checksum = checksum.wrapping_add(v);
                }
            }
            BTreeOp::Delete(k) => {
                if tree.delete(k) {
                    checksum = checksum.wrapping_add(1);
                }
            }
        }
    }
    checksum
}

struct PanicTimingResult {
    thread_count: usize,
    variant: &'static str,
    samples: Vec<Duration>,
}

impl PanicTimingResult {
    fn mean(&self) -> Duration {
        let total: Duration = self.samples.iter().sum();
        total / self.samples.len() as u32
    }
}

fn run_panic_timing<S: Schedule<BTree>>(
    threads: usize,
    variant_name: &'static str,
) -> PanicTimingResult {
    let mut samples = Vec::with_capacity(ITERATIONS);

    for iter in 0..ITERATIONS {
        let tree = BTreeWorkload.init_state();
        let lock = S::new(tree);
        let barrier = Barrier::new(threads + 1);
        let epoch = Instant::now();
        // First thread to panic CAS-es this from 0 to its timestamp.
        let panic_nanos = AtomicU64::new(0);

        let _ = catch_unwind(AssertUnwindSafe(|| {
            std::thread::scope(|scope| {
                for tid in 0..threads {
                    let lock = &lock;
                    let barrier = &barrier;
                    let epoch = &epoch;
                    let panic_nanos = &panic_nanos;

                    scope.spawn(move || {
                        let mut rng = Xoshiro256PlusPlus::seed_from_u64(
                            tid as u64 * 66666 + 13579 + iter as u64 * 99991,
                        );
                        barrier.wait();

                        let ops = generate_ops(&mut rng, OPS_PER_THREAD);

                        for (batch_idx, batch) in ops.chunks(BATCH_SIZE).enumerate() {
                            if batch_idx >= PANIC_AT_BATCH {
                                // Reached 80% — panic inside the lock.
                                // Only the first thread to CAS records the timestamp.
                                lock.schedule(|tree| {
                                    execute_batch(tree, batch);
                                    let now = epoch.elapsed().as_nanos() as u64;
                                    let _ = panic_nanos.compare_exchange(
                                        0,
                                        now,
                                        Ordering::Release,
                                        Ordering::Relaxed,
                                    );
                                    panic!("intentional panic for cleanup timing");
                                });
                                unreachable!();
                            }

                            // Normal batch — will panic via unwrap if lock is poisoned.
                            lock.schedule(|tree| {
                                execute_batch(tree, batch);
                            });
                        }
                    });
                }
                barrier.wait();
            });
        }));

        let total_nanos = epoch.elapsed().as_nanos() as u64;
        let panic_at = panic_nanos.load(Ordering::Acquire);
        if panic_at > 0 && total_nanos > panic_at {
            samples.push(Duration::from_nanos(total_nanos - panic_at));
        }
    }

    PanicTimingResult {
        thread_count: threads,
        variant: variant_name,
        samples,
    }
}

fn fmt_time(d: Duration) -> String {
    let ns = d.as_nanos() as f64;
    if ns >= 1e9 {
        format!("{:.2}s", ns / 1e9)
    } else if ns >= 1e6 {
        format!("{:.2}ms", ns / 1e6)
    } else if ns >= 1e3 {
        format!("{:.2}\u{b5}s", ns / 1e3)
    } else {
        format!("{:.0}ns", ns)
    }
}

fn print_table(results: &[PanicTimingResult]) {
    use std::collections::BTreeMap;

    // Group by thread_count -> variant -> median.
    let mut by_threads: BTreeMap<usize, BTreeMap<&str, Duration>> = BTreeMap::new();
    for r in results {
        if !r.samples.is_empty() {
            by_threads
                .entry(r.thread_count)
                .or_default()
                .insert(r.variant, r.mean());
        }
    }

    let variants = ["lamlock", "lamlock-spin"];
    let w = 14; // column width

    let mut hdr = format!("{:>3}", "thr");
    for v in &variants {
        hdr += &format!(" {:>w$}", v);
    }
    let sep = "\u{2500}".repeat(hdr.len());

    println!();
    println!("  Panic Cleanup (btree, mean of {ITERATIONS} runs)");
    println!("  {sep}");
    println!("  {hdr}");
    println!("  {sep}");
    for (&threads, times) in &by_threads {
        let mut line = format!("{threads:>3}");
        for v in &variants {
            if let Some(&d) = times.get(v) {
                line += &format!(" {:>w$}", fmt_time(d));
            } else {
                line += &format!(" {:>w$}", "\u{2014}");
            }
        }
        println!("  {line}");
    }
    println!("  {sep}");
}

fn write_json(results: &[PanicTimingResult]) {
    use std::collections::BTreeMap;
    use std::fmt::Write;

    // Group by variant -> thread_count.
    let mut grouped: BTreeMap<&str, BTreeMap<usize, &PanicTimingResult>> = BTreeMap::new();
    for r in results {
        grouped.entry(r.variant).or_default().insert(r.thread_count, r);
    }

    let mut json = String::from("{\n");
    let variants: Vec<_> = grouped.keys().copied().collect();
    for (vi, variant) in variants.iter().enumerate() {
        let _ = write!(json, "  \"{variant}\": {{\n");
        let thread_map = &grouped[variant];
        let thread_counts: Vec<_> = thread_map.keys().copied().collect();
        for (ti, &tc) in thread_counts.iter().enumerate() {
            let r = thread_map[&tc];
            let mean_ns = r.mean().as_nanos();
            let _ = write!(json, "    \"{tc}\": {mean_ns}");
            if ti + 1 < thread_counts.len() {
                json.push_str(",\n");
            } else {
                json.push('\n');
            }
        }
        let _ = write!(json, "  }}");
        if vi + 1 < variants.len() {
            json.push_str(",\n");
        } else {
            json.push('\n');
        }
    }
    json.push_str("}\n");

    // Write into target/ (two levels up from CARGO_MANIFEST_DIR for workspace crates).
    let target_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("can't find workspace root")
        .join("target");
    let path = target_dir.join("panic_timing_results.json");
    std::fs::write(&path, &json).expect("failed to write panic_timing_results.json");
    println!("\n  wrote {}", path.display());
}

fn main() {
    // Suppress panic messages — all panics in this binary are intentional.
    std::panic::set_hook(Box::new(|_| {}));

    let mut results = Vec::new();

    for &threads in THREADS {
        eprint!("  threads={threads:<4}");

        eprint!(" lamlock...");
        results.push(run_panic_timing::<Lock<BTree, true, true>>(
            threads, "lamlock",
        ));

        eprint!(" lamlock-spin...");
        results.push(run_panic_timing::<Lock<BTree, false, true>>(
            threads,
            "lamlock-spin",
        ));

        eprintln!(" done");
    }

    print_table(&results);
    write_json(&results);
}
