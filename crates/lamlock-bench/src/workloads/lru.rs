use std::collections::HashMap;
use std::hint::black_box;

use rand::Rng;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

use crate::schedule::Schedule;
use crate::workloads::Workload;

const LRU_CAPACITY: usize = 4096;
const VALUE_SIZE: usize = 256;
const KEY_SPACE: u64 = 16384;
const ZIPF_EXPONENT: f64 = 1.2;
const SENTINEL: usize = usize::MAX;

/// Batch size: how many cache ops each thread submits per lock.schedule() call.
const BATCH_SIZE: usize = 1000;

/// FNV-1a hash (32-bit).
fn fnv1a(data: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for &byte in data {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

/// Compute a 256-byte value from a key via iterated FNV-1a.
fn compute_value(key: u64) -> [u8; VALUE_SIZE] {
    let mut result = [0u8; VALUE_SIZE];
    let key_bytes = key.to_le_bytes();
    let mut hash = fnv1a(&key_bytes);
    for chunk in result.chunks_exact_mut(4) {
        chunk.copy_from_slice(&hash.to_le_bytes());
        // Feed hash back to get next 4 bytes
        hash = fnv1a(&hash.to_le_bytes());
    }
    result
}

/// Zipfian distribution: generates keys biased toward hot keys.
fn zipfian_key(rng: &mut Xoshiro256PlusPlus, n: u64) -> u64 {
    let u: f64 = rng.random::<f64>();
    let rank =
        (u * (n as f64).powf(1.0 - ZIPF_EXPONENT) + (1.0 - u)).powf(1.0 / (1.0 - ZIPF_EXPONENT));
    (rank as u64).min(n - 1)
}

struct LruEntry {
    key: u64,
    value: [u8; VALUE_SIZE],
    prev: usize,
    next: usize,
}

pub struct LruCache {
    entries: Vec<LruEntry>,
    map: HashMap<u64, usize>,
    head: usize, // MRU
    tail: usize, // LRU
    free_head: usize,
    len: usize,
}

impl LruCache {
    fn new() -> Self {
        // Pre-allocate arena with free list
        let entries: Vec<LruEntry> = (0..LRU_CAPACITY)
            .map(|i| LruEntry {
                key: 0,
                value: [0u8; VALUE_SIZE],
                prev: SENTINEL,
                next: if i + 1 < LRU_CAPACITY {
                    i + 1
                } else {
                    SENTINEL
                },
            })
            .collect();

        // Free list uses `next` pointers
        Self {
            entries,
            map: HashMap::with_capacity(LRU_CAPACITY),
            head: SENTINEL,
            tail: SENTINEL,
            free_head: 0,
            len: 0,
        }
    }

    fn detach(&mut self, idx: usize) {
        let prev = self.entries[idx].prev;
        let next = self.entries[idx].next;

        if prev != SENTINEL {
            self.entries[prev].next = next;
        } else {
            self.head = next;
        }

        if next != SENTINEL {
            self.entries[next].prev = prev;
        } else {
            self.tail = prev;
        }

        self.entries[idx].prev = SENTINEL;
        self.entries[idx].next = SENTINEL;
    }

    fn attach_front(&mut self, idx: usize) {
        self.entries[idx].prev = SENTINEL;
        self.entries[idx].next = self.head;

        if self.head != SENTINEL {
            self.entries[self.head].prev = idx;
        }
        self.head = idx;

        if self.tail == SENTINEL {
            self.tail = idx;
        }
    }

    fn alloc_slot(&mut self) -> usize {
        let idx = self.free_head;
        debug_assert!(idx != SENTINEL);
        self.free_head = self.entries[idx].next;
        self.entries[idx].next = SENTINEL;
        self.entries[idx].prev = SENTINEL;
        idx
    }

    fn free_slot(&mut self, idx: usize) {
        self.entries[idx].next = self.free_head;
        self.entries[idx].prev = SENTINEL;
        self.free_head = idx;
    }

    /// Look up a key. If found, move to front (MRU). Returns whether it was a hit.
    fn get(&mut self, key: u64) -> bool {
        if let Some(&idx) = self.map.get(&key) {
            self.detach(idx);
            self.attach_front(idx);
            true
        } else {
            false
        }
    }

    /// Insert a key-value pair. Evicts LRU if at capacity. Returns whether eviction happened.
    fn put(&mut self, key: u64, value: &[u8; VALUE_SIZE]) -> bool {
        // If already present, update and move to front
        if let Some(&idx) = self.map.get(&key) {
            self.entries[idx].value = *value;
            self.detach(idx);
            self.attach_front(idx);
            return false;
        }

        let evicted = if self.len >= LRU_CAPACITY {
            // Evict LRU (tail)
            let victim = self.tail;
            debug_assert!(victim != SENTINEL);
            let victim_key = self.entries[victim].key;
            self.detach(victim);
            self.map.remove(&victim_key);
            self.free_slot(victim);
            self.len -= 1;
            true
        } else {
            false
        };

        let idx = self.alloc_slot();
        self.entries[idx].key = key;
        self.entries[idx].value = *value;
        self.attach_front(idx);
        self.map.insert(key, idx);
        self.len += 1;
        evicted
    }

    /// Combined get-or-insert: look up key, if miss insert with provided value.
    /// Returns true on hit, false on miss (value was inserted).
    fn get_or_insert(&mut self, key: u64, value: &[u8; VALUE_SIZE]) -> bool {
        if self.get(key) {
            true
        } else {
            self.put(key, value);
            false
        }
    }
}

pub struct LruWorkload;

impl Workload for LruWorkload {
    type State = LruCache;

    fn name(&self) -> &'static str {
        "lru"
    }

    fn description(&self) -> &'static str {
        "LRU cache — batched get-or-insert with compute-on-miss"
    }

    fn init_state(&self) -> Self::State {
        let mut cache = LruCache::new();
        // Pre-populate to capacity with keys 0..LRU_CAPACITY
        for key in 0..LRU_CAPACITY as u64 {
            let value = compute_value(key);
            cache.put(key, &value);
        }
        cache
    }

    fn run_thread<S: Schedule<Self::State>>(
        &self,
        lock: &S,
        thread_id: usize,
        _thread_count: usize,
        ops: usize,
    ) {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(thread_id as u64 * 88888 + 24680);

        // [OUTSIDE] Pre-generate all keys and compute their values upfront.
        // In a real system the value computation is the expensive part we cache to avoid,
        // but for the benchmark we pre-compute so the entire batch can be submitted to the lock.
        let batch_data: Vec<(u64, [u8; VALUE_SIZE])> = (0..ops)
            .map(|_| {
                let key = zipfian_key(&mut rng, KEY_SPACE);
                let value = compute_value(key);
                (key, value)
            })
            .collect();

        // [INSIDE] Submit work in BATCHES — the combiner processes many LRU operations
        // (linked-list pointer manipulation + HashMap lookups) while the cache stays hot.
        for batch in batch_data.chunks(BATCH_SIZE) {
            let hits = lock.schedule(|cache| {
                let mut hit_count = 0u64;
                for &(key, ref value) in batch {
                    if cache.get_or_insert(key, value) {
                        hit_count += 1;
                    }
                }
                hit_count
            });
            black_box(hits);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_basic() {
        let mut cache = LruCache::new();
        let val = [0xAAu8; VALUE_SIZE];

        assert!(!cache.get(1));
        cache.put(1, &val);
        assert!(cache.get(1));
        assert!(!cache.get(2));
    }

    #[test]
    fn test_lru_eviction() {
        let mut cache = LruCache::new();
        let val = [0xBBu8; VALUE_SIZE];

        // Fill to capacity
        for i in 0..LRU_CAPACITY as u64 {
            let evicted = cache.put(i, &val);
            assert!(!evicted);
        }
        assert_eq!(cache.len, LRU_CAPACITY);

        // Next insert should evict the oldest (key 0)
        let evicted = cache.put(LRU_CAPACITY as u64, &val);
        assert!(evicted);
        assert!(!cache.get(0)); // key 0 was evicted
        assert!(cache.get(LRU_CAPACITY as u64)); // new key present
    }

    #[test]
    fn test_lru_access_prevents_eviction() {
        let mut cache = LruCache::new();
        let val = [0xCCu8; VALUE_SIZE];

        // Fill to capacity
        for i in 0..LRU_CAPACITY as u64 {
            cache.put(i, &val);
        }

        // Access key 0, making it MRU
        assert!(cache.get(0));

        // Insert LRU_CAPACITY more items — key 0 should survive for a while
        // since it was moved to front
        cache.put(LRU_CAPACITY as u64, &val);
        // Key 1 should be evicted (it was the LRU after key 0 was accessed)
        assert!(!cache.get(1));
        // Key 0 should still be present (it was moved to MRU)
        assert!(cache.get(0));
    }

    #[test]
    fn test_compute_value_deterministic() {
        let v1 = compute_value(42);
        let v2 = compute_value(42);
        assert_eq!(v1, v2);

        let v3 = compute_value(43);
        assert_ne!(v1, v3);
    }

    #[test]
    fn test_lru_get_or_insert() {
        let mut cache = LruCache::new();
        let val = [0xDDu8; VALUE_SIZE];

        // Miss — should insert
        assert!(!cache.get_or_insert(42, &val));
        // Hit — should find it
        assert!(cache.get_or_insert(42, &val));
    }
}
