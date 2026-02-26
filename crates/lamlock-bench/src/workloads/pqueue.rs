use std::hint::black_box;

use rand::Rng;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

use crate::schedule::Schedule;
use crate::workloads::Workload;

const CAPACITY: usize = 1024;

/// Batch size: how many push/pop-min ops each thread submits per lock.schedule() call.
const BATCH_SIZE: usize = 1000;

/// Pre-fill to ~50% capacity.
const INITIAL_FILL: usize = CAPACITY / 2;

const PAYLOAD_SIZE: usize = 24;

/// Timer entry: deadline + inline context payload.
/// At 80 bytes per entry, each sift swap moves data across multiple cache lines,
/// making cache locality (flat-combining's advantage) more impactful.
#[derive(Clone, Copy)]
struct Entry {
    deadline: u64,
    payload: [u8; PAYLOAD_SIZE],
}

/// Fixed-capacity min-heap (priority queue). No heap allocation during push/pop.
/// Mimics a timer heap where the smallest deadline fires first.
pub struct MinHeap {
    data: Vec<Entry>,
}

impl MinHeap {
    fn new() -> Self {
        Self {
            data: Vec::with_capacity(CAPACITY),
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn push(&mut self, entry: Entry) -> bool {
        if self.data.len() >= CAPACITY {
            return false;
        }
        self.data.push(entry);
        self.sift_up(self.data.len() - 1);
        true
    }

    fn pop_min(&mut self) -> Option<Entry> {
        if self.data.is_empty() {
            return None;
        }
        let min = self.data[0];
        let last = self.data.len() - 1;
        self.data.swap(0, last);
        self.data.pop();
        if !self.data.is_empty() {
            self.sift_down(0);
        }
        Some(min)
    }

    fn sift_up(&mut self, mut idx: usize) {
        while idx > 0 {
            let parent = (idx - 1) / 2;
            if self.data[idx].deadline < self.data[parent].deadline {
                self.data.swap(idx, parent);
                idx = parent;
            } else {
                break;
            }
        }
    }

    fn sift_down(&mut self, mut idx: usize) {
        let len = self.data.len();
        loop {
            let left = 2 * idx + 1;
            let right = 2 * idx + 2;
            let mut smallest = idx;

            if left < len && self.data[left].deadline < self.data[smallest].deadline {
                smallest = left;
            }
            if right < len && self.data[right].deadline < self.data[smallest].deadline {
                smallest = right;
            }
            if smallest != idx {
                self.data.swap(idx, smallest);
                idx = smallest;
            } else {
                break;
            }
        }
    }
}

#[derive(Clone, Copy)]
enum PQueueOp {
    Push(Entry),
    PopMin,
}

pub struct PQueueWorkload;

impl Workload for PQueueWorkload {
    type State = MinHeap;

    fn name(&self) -> &'static str {
        "pqueue"
    }

    fn description(&self) -> &'static str {
        "Min-heap priority queue (timer heap) — batched push/pop-min, fat entries"
    }

    fn init_state(&self) -> Self::State {
        let mut heap = MinHeap::new();
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(0x71BE_4EAF);
        for _ in 0..INITIAL_FILL {
            let deadline = rng.random_range(0..1_000_000u64);
            let mut payload = [0u8; PAYLOAD_SIZE];
            for byte in &mut payload {
                *byte = rng.random::<u8>();
            }
            heap.push(Entry { deadline, payload });
        }
        heap
    }

    fn run_thread<S: Schedule<Self::State>>(
        &self,
        lock: &S,
        thread_id: usize,
        _thread_count: usize,
        ops: usize,
    ) {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(thread_id as u64 * 55555 + 12321);

        // 35% push, 65% pop-min — pop-heavy since sift_down is more expensive
        let operations: Vec<PQueueOp> = (0..ops)
            .map(|_| {
                let r: f64 = rng.random();
                if r < 0.35 {
                    let deadline = rng.random_range(0..1_000_000u64);
                    let mut payload = [0u8; PAYLOAD_SIZE];
                    for byte in &mut payload {
                        *byte = rng.random::<u8>();
                    }
                    PQueueOp::Push(Entry { deadline, payload })
                } else {
                    PQueueOp::PopMin
                }
            })
            .collect();

        for batch in operations.chunks(BATCH_SIZE) {
            let result = lock.schedule(|heap| {
                let mut last = 0u64;
                for op in batch {
                    match *op {
                        PQueueOp::Push(entry) => {
                            heap.push(entry);
                        }
                        PQueueOp::PopMin => {
                            if let Some(e) = heap.pop_min() {
                                last = e.deadline;
                            }
                        }
                    }
                }
                last
            });
            black_box(result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(deadline: u64) -> Entry {
        Entry {
            deadline,
            payload: [0u8; PAYLOAD_SIZE],
        }
    }

    #[test]
    fn test_heap_push_pop() {
        let mut heap = MinHeap::new();
        heap.push(entry(30));
        heap.push(entry(10));
        heap.push(entry(20));

        assert_eq!(heap.pop_min().unwrap().deadline, 10);
        assert_eq!(heap.pop_min().unwrap().deadline, 20);
        assert_eq!(heap.pop_min().unwrap().deadline, 30);
        assert!(heap.pop_min().is_none());
    }

    #[test]
    fn test_heap_full() {
        let mut heap = MinHeap::new();
        for i in 0..CAPACITY as u64 {
            assert!(heap.push(entry(i)));
        }
        assert!(!heap.push(entry(999)));
        assert_eq!(heap.len(), CAPACITY);
    }

    #[test]
    fn test_heap_ordering() {
        let mut heap = MinHeap::new();
        for i in (0..100u64).rev() {
            heap.push(entry(i));
        }
        for i in 0..100u64 {
            assert_eq!(heap.pop_min().unwrap().deadline, i);
        }
    }
}
