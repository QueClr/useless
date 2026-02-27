# lamlock-bench

Criterion benchmarks for [lamlock](../lamlock/) — comparing it against `std::sync::Mutex` across several high-contention workloads.

## Workloads

| Workload  | Description |
|-----------|-------------|
| `stack`   | Concurrent stack with push/pop operations |
| `pqueue`  | Min-heap priority queue with insert/extract |
| `ringbuf` | Fixed-capacity ring buffer with enqueue/dequeue |
| `slab`    | Slab allocator with alloc/free cycles |

Each workload runs **5 000 ops/thread** at 64, 128, 256, and 512 threads.

## Variants

Five lock implementations are benchmarked:

| Variant | Futex | Panic Handling |
|---------|-------|----------------|
| `lamlock` | ✅ | ✅ |
| `lamlock-no-panic` | ✅ | ❌ |
| `lamlock-spin` | ❌ (spin-only) | ✅ |
| `lamlock-spin-no-panic` | ❌ (spin-only) | ❌ |
| `std-mutex` | — | — |

This isolates the performance impact of **futex wait/notify** (vs pure spinning) and **panic-safety** (bomb logic) on lamlock.

## Running

```bash
cargo bench            # run all benchmarks (Criterion)
python3 compare.py     # print a side-by-side comparison table
```

`compare.py` reads the Criterion JSON output from `target/criterion/` and prints mean times with **Δ%** columns (negative = faster than std-mutex).
