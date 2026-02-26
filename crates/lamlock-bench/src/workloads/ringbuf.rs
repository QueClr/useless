use std::hint::black_box;

use rand::Rng;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

use crate::schedule::Schedule;
use crate::workloads::Workload;

/// Ring buffer capacity. 256 entries × 64B = 16KB.
const CAPACITY: usize = 256;
const CAPACITY_MASK: usize = CAPACITY - 1; // power-of-two wrap
/// Batch size per schedule() call.
const BATCH_SIZE: usize = 1000;
/// How many entries a scan operation reads.
const SCAN_WINDOW: usize = 24;
/// How many entries a drain pops.
const DRAIN_COUNT: usize = 16;

/// Log entry — 64 bytes, exactly one cache line.
#[derive(Clone, Copy)]
#[repr(C)]
struct LogEntry {
    timestamp: u64,    // 8
    level: u8,         // 1
    _pad: [u8; 3],     // 3 (alignment)
    tag: u32,          // 4
    message: [u8; 48], // 48
}
// Static assert: size should be 64.
const _: () = assert!(core::mem::size_of::<LogEntry>() == 64);

impl LogEntry {
    const EMPTY: Self = Self {
        timestamp: 0,
        level: 0,
        _pad: [0; 3],
        tag: 0,
        message: [0u8; 48],
    };
}

pub struct RingBuffer {
    entries: Vec<LogEntry>,
    /// Write cursor (monotonically increasing, wrap via mask).
    head: usize,
    /// Number of valid entries currently stored.
    count: usize,
}

impl RingBuffer {
    fn new() -> Self {
        Self {
            entries: vec![LogEntry::EMPTY; CAPACITY],
            head: 0,
            count: 0,
        }
    }

    /// Produce: write a log entry at the head position, advance cursor.
    fn produce(&mut self, entry: LogEntry) {
        let idx = self.head & CAPACITY_MASK;
        self.entries[idx] = entry;
        self.head += 1;
        if self.count < CAPACITY {
            self.count += 1;
        }
    }

    /// Scan: read the last `n` entries and compute a checksum.
    /// This is the expensive operation — it touches many contiguous cache lines.
    fn scan(&self, n: usize) -> u64 {
        let n = n.min(self.count);
        if n == 0 {
            return 0;
        }
        let mut checksum: u64 = 0;
        // Walk backwards from head.
        for i in 0..n {
            let idx = (self.head.wrapping_sub(1 + i)) & CAPACITY_MASK;
            let e = &self.entries[idx];
            // Mix multiple fields to prevent the optimizer from eliding reads.
            checksum = checksum
                .wrapping_add(e.timestamp)
                .wrapping_add(e.tag as u64)
                .wrapping_add(e.level as u64);
            // Touch several bytes of the message payload.
            checksum = checksum.wrapping_add(e.message[0] as u64);
            checksum = checksum.wrapping_add(e.message[24] as u64);
            checksum = checksum.wrapping_add(e.message[47] as u64);
        }
        checksum
    }

    /// Drain: pop `n` oldest entries from the tail, return how many were drained.
    fn drain(&mut self, n: usize) -> usize {
        let drained = n.min(self.count);
        self.count -= drained;
        drained
    }
}

#[derive(Clone, Copy)]
enum RingOp {
    Produce(LogEntry),
    Scan(usize),
    Drain(usize),
}

pub struct RingBufWorkload;

impl Workload for RingBufWorkload {
    type State = RingBuffer;

    fn name(&self) -> &'static str {
        "ringbuf"
    }

    fn description(&self) -> &'static str {
        "Ring buffer log — produce/scan/drain, contiguous cache-line sweeps"
    }

    fn init_state(&self) -> Self::State {
        let mut buf = RingBuffer::new();
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(0xDEAD_C0DE);
        // Pre-fill to ~75% capacity.
        for i in 0..(CAPACITY * 3 / 4) {
            let mut msg = [0u8; 48];
            for byte in &mut msg {
                *byte = rng.random::<u8>();
            }
            buf.produce(LogEntry {
                timestamp: i as u64,
                level: rng.random_range(0..5u8),
                _pad: [0; 3],
                tag: rng.random_range(0..100u32),
                message: msg,
            });
        }
        buf
    }

    fn run_thread<S: Schedule<Self::State>>(
        &self,
        lock: &S,
        thread_id: usize,
        _thread_count: usize,
        ops: usize,
    ) {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(thread_id as u64 * 33333 + 54321);

        let operations: Vec<RingOp> = (0..ops)
            .map(|_| {
                let r: f64 = rng.random();
                if r < 0.50 {
                    let mut msg = [0u8; 48];
                    for byte in &mut msg {
                        *byte = rng.random::<u8>();
                    }
                    RingOp::Produce(LogEntry {
                        timestamp: rng.random::<u64>(),
                        level: rng.random_range(0..5u8),
                        _pad: [0; 3],
                        tag: rng.random_range(0..100u32),
                        message: msg,
                    })
                } else if r < 0.80 {
                    let n = rng.random_range(8..SCAN_WINDOW);
                    RingOp::Scan(n)
                } else {
                    let n = rng.random_range(4..DRAIN_COUNT);
                    RingOp::Drain(n)
                }
            })
            .collect();

        for batch in operations.chunks(BATCH_SIZE) {
            let result = lock.schedule(|buf| {
                let mut checksum = 0u64;
                for op in batch {
                    match *op {
                        RingOp::Produce(entry) => {
                            buf.produce(entry);
                        }
                        RingOp::Scan(n) => {
                            checksum = checksum.wrapping_add(buf.scan(n));
                        }
                        RingOp::Drain(n) => {
                            checksum = checksum.wrapping_add(buf.drain(n) as u64);
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

    fn make_entry(ts: u64, tag: u32) -> LogEntry {
        LogEntry {
            timestamp: ts,
            level: 1,
            _pad: [0; 3],
            tag,
            message: [0u8; 48],
        }
    }

    #[test]
    fn test_produce_and_scan() {
        let mut buf = RingBuffer::new();
        buf.produce(make_entry(100, 1));
        buf.produce(make_entry(200, 2));
        buf.produce(make_entry(300, 3));

        let checksum = buf.scan(2);
        // Should read entries with ts=300 and ts=200.
        // checksum = 300 + 3 + 1 + 200 + 2 + 1 = 507 (plus message bytes which are 0).
        assert!(checksum > 0);
        assert_eq!(buf.count, 3);
    }

    #[test]
    fn test_drain() {
        let mut buf = RingBuffer::new();
        for i in 0..10 {
            buf.produce(make_entry(i, 0));
        }
        assert_eq!(buf.count, 10);
        assert_eq!(buf.drain(4), 4);
        assert_eq!(buf.count, 6);
    }

    #[test]
    fn test_wrap_around() {
        let mut buf = RingBuffer::new();
        // Write more than capacity to force wrap-around.
        for i in 0..(CAPACITY + 50) as u64 {
            buf.produce(make_entry(i, 0));
        }
        assert_eq!(buf.count, CAPACITY);
        // The most recent entry should have timestamp CAPACITY + 49.
        let latest_idx = (buf.head.wrapping_sub(1)) & CAPACITY_MASK;
        assert_eq!(buf.entries[latest_idx].timestamp, (CAPACITY + 49) as u64);
    }

    #[test]
    fn test_scan_empty() {
        let buf = RingBuffer::new();
        assert_eq!(buf.scan(10), 0);
    }
}
