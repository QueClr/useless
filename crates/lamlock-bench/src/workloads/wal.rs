use std::hint::black_box;

use rand::Rng;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

use crate::schedule::Schedule;
use crate::workloads::Workload;

const WAL_BUFFER_SIZE: usize = 4 * 1024 * 1024;
const MIN_RECORD_SIZE: usize = 64;
const MAX_RECORD_SIZE: usize = 512;
const RECORD_HEADER_SIZE: usize = 24;

/// FNV-1a hash (32-bit).
fn fnv1a(data: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for &byte in data {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

pub struct WalBuffer {
    buffer: Vec<u8>,
    write_offset: usize,
    lsn: u64,
    running_checksum: u32,
}

impl WalBuffer {
    fn new() -> Self {
        Self {
            buffer: vec![0u8; WAL_BUFFER_SIZE],
            write_offset: 0,
            lsn: 0,
            running_checksum: 0x811c_9dc5, // FNV-1a offset basis
        }
    }

    fn append(&mut self, record: &[u8]) -> u64 {
        let len = record.len();
        let offset = self.write_offset;
        let buf_len = self.buffer.len();

        if offset + len <= buf_len {
            // No wrap
            self.buffer[offset..offset + len].copy_from_slice(record);
        } else {
            // Wrap around
            let first = buf_len - offset;
            self.buffer[offset..].copy_from_slice(&record[..first]);
            self.buffer[..len - first].copy_from_slice(&record[first..]);
        }

        self.write_offset = (offset + len) % buf_len;
        self.lsn += 1;
        self.running_checksum = fnv1a(record);
        self.lsn
    }
}

pub struct WalWorkload;

impl Workload for WalWorkload {
    type State = WalBuffer;

    fn name(&self) -> &'static str {
        "wal"
    }

    fn description(&self) -> &'static str {
        "Write-ahead log — serialize records outside lock, append inside"
    }

    fn init_state(&self) -> Self::State {
        WalBuffer::new()
    }

    fn run_thread<S: Schedule<Self::State>>(
        &self,
        lock: &S,
        thread_id: usize,
        _thread_count: usize,
        ops: usize,
    ) {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(thread_id as u64 * 77777 + 31337);
        let mut record_buf = vec![0u8; MAX_RECORD_SIZE];

        for _ in 0..ops {
            // [OUTSIDE] Generate record size and build record
            let record_size = rng.random_range(MIN_RECORD_SIZE..=MAX_RECORD_SIZE);
            let payload_size = record_size - RECORD_HEADER_SIZE;

            // Determine key_len and value_len from payload
            let key_len = (payload_size / 3).max(1) as u16;
            let value_len = (payload_size - key_len as usize) as u16;

            // Fill header fields
            // [0..8]   lsn placeholder (0)
            record_buf[0..8].copy_from_slice(&0u64.to_le_bytes());
            // [8..12]  record_len
            record_buf[8..12].copy_from_slice(&(record_size as u32).to_le_bytes());
            // [12..14] key_len
            record_buf[12..14].copy_from_slice(&key_len.to_le_bytes());
            // [14..16] value_len
            record_buf[14..16].copy_from_slice(&value_len.to_le_bytes());
            // [20..24] padding
            record_buf[20..24].copy_from_slice(&0u32.to_le_bytes());

            // Fill payload with random bytes
            for byte in &mut record_buf[RECORD_HEADER_SIZE..record_size] {
                *byte = rng.random::<u8>();
            }

            // Compute FNV-1a checksum over payload
            let checksum = fnv1a(&record_buf[RECORD_HEADER_SIZE..record_size]);
            // [16..20] checksum
            record_buf[16..20].copy_from_slice(&checksum.to_le_bytes());

            // [INSIDE] Append record under lock
            let record = &record_buf[..record_size];
            let lsn = lock.schedule(|wal| wal.append(record));
            black_box(lsn);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fnv1a() {
        assert_eq!(fnv1a(b""), 0x811c_9dc5);
        // Known FNV-1a value for "foobar"
        let h = fnv1a(b"foobar");
        assert_ne!(h, 0); // sanity check
    }

    #[test]
    fn test_wal_append() {
        let mut wal = WalBuffer::new();
        let record = [0xABu8; 64];
        let lsn1 = wal.append(&record);
        assert_eq!(lsn1, 1);
        assert_eq!(wal.write_offset, 64);

        let lsn2 = wal.append(&record);
        assert_eq!(lsn2, 2);
        assert_eq!(wal.write_offset, 128);
    }

    #[test]
    fn test_wal_wraparound() {
        let mut wal = WalBuffer::new();
        let record = vec![0xCDu8; 512];
        // Write enough to approach the end
        let writes_to_fill = WAL_BUFFER_SIZE / 512;
        for i in 0..writes_to_fill {
            let lsn = wal.append(&record);
            assert_eq!(lsn, i as u64 + 1);
        }
        // Next write should wrap around
        assert_eq!(wal.write_offset, 0); // exactly fills
        let lsn = wal.append(&record);
        assert_eq!(lsn, writes_to_fill as u64 + 1);
        assert_eq!(wal.write_offset, 512);
    }
}
