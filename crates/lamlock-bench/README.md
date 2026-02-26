# lamlock-bench

Criterion benchmarks for [lamlock](../lamlock/) — comparing it against `std::sync::Mutex` across several high-contention workloads.

## Workloads

| Workload  | Description |
|-----------|-------------|
| `stack`   | Concurrent stack with push/pop operations |
| `pqueue`  | Min-heap priority queue with insert/extract |
| `ringbuf` | Fixed-capacity ring buffer with enqueue/dequeue |
| `slab`    | Slab allocator with alloc/free cycles |

Each workload runs **5 000 ops/thread** at 64, 128, 256, and 512 threads under both `lamlock` and `std::sync::Mutex`.

## Running

```bash
cargo bench            # run all benchmarks (Criterion)
python3 compare.py     # print a side-by-side comparison table
```

`compare.py` reads the Criterion JSON output from `target/criterion/` and prints mean times with a **Δ%** column (negative = lamlock is faster).

## Example results — AMD Ryzen 9 5950X

```
---------------------------------------------------------
Workload       Threads      lamlock    std-mutex       Δ%
---------------------------------------------------------
stack               64     3.217 ms     3.551 ms    -9.4%
stack              128     6.436 ms     7.211 ms   -10.7%
stack              256    13.241 ms    14.502 ms    -8.7%
stack              512    27.291 ms    29.292 ms    -6.8%
pqueue              64     2.571 ms     2.973 ms   -13.5%
pqueue             128     5.121 ms     5.977 ms   -14.3%
pqueue             256    10.982 ms    12.248 ms   -10.3%
pqueue             512    23.051 ms    24.980 ms    -7.7%
ringbuf             64     3.094 ms     3.446 ms   -10.2%
ringbuf            128     6.565 ms     7.078 ms    -7.3%
ringbuf            256    14.230 ms    15.313 ms    -7.1%
ringbuf            512    29.517 ms    31.802 ms    -7.2%
slab                64    14.263 ms     7.368 ms   +93.6%
slab               128    47.590 ms    24.732 ms   +92.4%
slab               256    58.210 ms    85.872 ms   -32.2%
slab               512    64.252 ms    88.301 ms   -27.2%
---------------------------------------------------------
```
