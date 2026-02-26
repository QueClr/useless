use std::hint::black_box;

use rand::Rng;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

use crate::schedule::Schedule;
use crate::workloads::Workload;

/// Stack capacity: 512 entries × 32B = 16KB, fits in L1.
const CAPACITY: usize = 512;

/// Batch size per schedule() call.
const BATCH_SIZE: usize = 1000;

/// How many entries peek_top reads.
const PEEK_WINDOW: usize = 16;

/// Pre-fill to ~50% capacity.
const INITIAL_FILL: usize = CAPACITY / 2;

/// Stack entry — 32 bytes, half a cache line.
/// Large enough that push/pop moves real data (not trivially short),
/// small enough to keep the working set in L1.
#[derive(Clone, Copy)]
#[repr(C)]
struct StackEntry {
    id: u64,           // 8
    priority: u32,     // 4
    flags: u32,        // 4
    payload: [u8; 16], // 16
}
// Static assert: size should be 32.
const _: () = assert!(core::mem::size_of::<StackEntry>() == 32);

impl StackEntry {
    fn checksum(&self) -> u64 {
        self.id
            .wrapping_add(self.priority as u64)
            .wrapping_add(self.flags as u64)
            .wrapping_add(self.payload[0] as u64)
            .wrapping_add(self.payload[8] as u64)
            .wrapping_add(self.payload[15] as u64)
    }
}

/// Fixed-capacity LIFO stack backed by a Vec.
/// Push/pop only touch the top (1–2 cache lines).
/// peek_top_n scans a contiguous window near the top.
pub struct Stack {
    data: Vec<StackEntry>,
}

impl Stack {
    fn new() -> Self {
        Self {
            data: Vec::with_capacity(CAPACITY),
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn push(&mut self, entry: StackEntry) -> bool {
        if self.data.len() >= CAPACITY {
            return false;
        }
        self.data.push(entry);
        true
    }

    fn pop(&mut self) -> Option<StackEntry> {
        self.data.pop()
    }

    /// Scan the top `n` entries and compute a checksum.
    /// Touches contiguous memory near the end of the vec — prefetcher-friendly.
    fn peek_top_n(&self, n: usize) -> u64 {
        let len = self.data.len();
        let n = n.min(len);
        if n == 0 {
            return 0;
        }
        let mut checksum: u64 = 0;
        for i in 0..n {
            checksum = checksum.wrapping_add(self.data[len - 1 - i].checksum());
        }
        checksum
    }
}

#[derive(Clone, Copy)]
enum StackOp {
    Push(StackEntry),
    Pop,
    PeekTop(usize),
}

pub struct StackWorkload;

impl Workload for StackWorkload {
    type State = Stack;

    fn name(&self) -> &'static str {
        "stack"
    }

    fn description(&self) -> &'static str {
        "LIFO stack — batched push/pop/peek, contiguous top-of-stack access"
    }

    fn init_state(&self) -> Self::State {
        let mut stack = Stack::new();
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(0xCAFE_BABE);
        for i in 0..INITIAL_FILL {
            let mut payload = [0u8; 16];
            for byte in &mut payload {
                *byte = rng.random::<u8>();
            }
            stack.push(StackEntry {
                id: i as u64,
                priority: rng.random_range(0..1000u32),
                flags: rng.random::<u32>(),
                payload,
            });
        }
        stack
    }

    fn run_thread<S: Schedule<Self::State>>(
        &self,
        lock: &S,
        thread_id: usize,
        _thread_count: usize,
        ops: usize,
    ) {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(thread_id as u64 * 77777 + 31337);

        // 40% push, 40% pop, 20% peek_top
        let operations: Vec<StackOp> = (0..ops)
            .map(|_| {
                let r: f64 = rng.random();
                if r < 0.40 {
                    let mut payload = [0u8; 16];
                    for byte in &mut payload {
                        *byte = rng.random::<u8>();
                    }
                    StackOp::Push(StackEntry {
                        id: rng.random::<u64>(),
                        priority: rng.random_range(0..1000u32),
                        flags: rng.random::<u32>(),
                        payload,
                    })
                } else if r < 0.80 {
                    StackOp::Pop
                } else {
                    let n = rng.random_range(8..PEEK_WINDOW);
                    StackOp::PeekTop(n)
                }
            })
            .collect();

        for batch in operations.chunks(BATCH_SIZE) {
            let result = lock.schedule(|stack| {
                let mut checksum = 0u64;
                for op in batch {
                    match *op {
                        StackOp::Push(entry) => {
                            stack.push(entry);
                        }
                        StackOp::Pop => {
                            if let Some(e) = stack.pop() {
                                checksum = checksum.wrapping_add(e.checksum());
                            }
                        }
                        StackOp::PeekTop(n) => {
                            checksum = checksum.wrapping_add(stack.peek_top_n(n));
                        }
                    }
                }
                checksum
            });
            black_box(result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: u64) -> StackEntry {
        StackEntry {
            id,
            priority: 0,
            flags: 0,
            payload: [0u8; 16],
        }
    }

    #[test]
    fn test_push_pop() {
        let mut stack = Stack::new();
        stack.push(entry(1));
        stack.push(entry(2));
        stack.push(entry(3));

        assert_eq!(stack.pop().unwrap().id, 3);
        assert_eq!(stack.pop().unwrap().id, 2);
        assert_eq!(stack.pop().unwrap().id, 1);
        assert!(stack.pop().is_none());
    }

    #[test]
    fn test_full() {
        let mut stack = Stack::new();
        for i in 0..CAPACITY as u64 {
            assert!(stack.push(entry(i)));
        }
        assert!(!stack.push(entry(999)));
        assert_eq!(stack.len(), CAPACITY);
    }

    #[test]
    fn test_peek_top() {
        let mut stack = Stack::new();
        for i in 0..10u64 {
            stack.push(entry(i));
        }
        let checksum = stack.peek_top_n(3);
        // Should read entries 9, 8, 7 → checksums = 9 + 8 + 7 = 24
        assert_eq!(checksum, 24);
        // peek doesn't remove anything
        assert_eq!(stack.len(), 10);
    }

    #[test]
    fn test_peek_empty() {
        let stack = Stack::new();
        assert_eq!(stack.peek_top_n(5), 0);
    }
}
