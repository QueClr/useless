use std::hint::black_box;

use rand::Rng;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

use crate::schedule::Schedule;
use crate::workloads::Workload;

/// Table capacity — must be a power of two.
/// 512 slots × 40B = 20KB, fits L1 data cache.
const CAPACITY: usize = 512;
const CAPACITY_MASK: usize = CAPACITY - 1;

/// Batch size per schedule() call.
const BATCH_SIZE: usize = 1000;

/// Pre-fill to ~50% capacity (load factor 0.5).
const INITIAL_FILL: usize = CAPACITY / 2;

/// Value payload — 24 bytes, keeps each slot at 40B total (key 8 + occupied 1 + pad 7 + value 24).
const VALUE_SIZE: usize = 24;

/// Bounded key range so removes/gets actually hit existing keys.
const KEY_RANGE: u64 = 2048;

#[derive(Clone, Copy)]
#[repr(C)]
struct Slot {
    key: u64,                // 8
    occupied: bool,          // 1
    _pad: [u8; 7],           // 7
    value: [u8; VALUE_SIZE], // 24
}
// Static assert: 40 bytes per slot.
const _: () = assert!(core::mem::size_of::<Slot>() == 40);

impl Slot {
    const EMPTY: Self = Self {
        key: 0,
        occupied: false,
        _pad: [0; 7],
        value: [0u8; VALUE_SIZE],
    };
}

pub struct HashTable {
    slots: Vec<Slot>,
    count: usize,
}

impl HashTable {
    fn new() -> Self {
        Self {
            slots: vec![Slot::EMPTY; CAPACITY],
            count: 0,
        }
    }

    /// Fibonacci hashing — fast, no modulo, good dispersion.
    #[inline]
    fn hash(key: u64) -> usize {
        // 2^64 / φ ≈ 11400714819323198485
        ((key.wrapping_mul(11400714819323198485)) >> (64 - 9)) as usize
    }

    /// Insert or overwrite. Returns true if a new key was inserted.
    fn insert(&mut self, key: u64, value: [u8; VALUE_SIZE]) -> bool {
        if self.count >= CAPACITY {
            return false;
        }
        let mut idx = Self::hash(key) & CAPACITY_MASK;
        loop {
            let slot = &mut self.slots[idx];
            if !slot.occupied {
                slot.key = key;
                slot.occupied = true;
                slot.value = value;
                self.count += 1;
                return true;
            }
            if slot.key == key {
                slot.value = value; // overwrite
                return false;
            }
            idx = (idx + 1) & CAPACITY_MASK;
        }
    }

    /// Lookup. Returns a checksum of the value if found.
    fn get(&self, key: u64) -> Option<u64> {
        let mut idx = Self::hash(key) & CAPACITY_MASK;
        for _ in 0..CAPACITY {
            let slot = &self.slots[idx];
            if !slot.occupied {
                return None;
            }
            if slot.key == key {
                // Touch the value bytes to force cache reads.
                let cs = (slot.value[0] as u64)
                    .wrapping_add(slot.value[8] as u64)
                    .wrapping_add(slot.value[16] as u64)
                    .wrapping_add(slot.value[23] as u64);
                return Some(cs);
            }
            idx = (idx + 1) & CAPACITY_MASK;
        }
        None // table full, key not present
    }

    /// Remove with backward-shift deletion (keeps probe chains intact,
    /// no tombstones needed). Returns true if the key was found and removed.
    fn remove(&mut self, key: u64) -> bool {
        let mut idx = Self::hash(key) & CAPACITY_MASK;
        // Find the slot.
        let mut found = false;
        for _ in 0..CAPACITY {
            let slot = &self.slots[idx];
            if !slot.occupied {
                return false;
            }
            if slot.key == key {
                found = true;
                break;
            }
            idx = (idx + 1) & CAPACITY_MASK;
        }
        if !found {
            return false; // table full, key not present
        }
        // Backward-shift: pull later entries back to fill the gap.
        self.slots[idx].occupied = false;
        self.count -= 1;
        let mut empty = idx;
        loop {
            idx = (idx + 1) & CAPACITY_MASK;
            let slot = &self.slots[idx];
            if !slot.occupied {
                break;
            }
            let ideal = Self::hash(slot.key) & CAPACITY_MASK;
            // Should this entry be shifted back? Yes if its ideal position
            // is at or before the empty slot (accounting for wrap-around).
            if (idx >= ideal && (empty < ideal || empty >= idx))
                || (idx < ideal && empty >= idx && empty < ideal)
            {
                // Not displaced, or displaced in the wrong direction — skip.
                continue;
            }
            self.slots[empty] = self.slots[idx];
            self.slots[idx].occupied = false;
            empty = idx;
        }
        true
    }
}

#[derive(Clone, Copy)]
enum HashOp {
    Insert(u64, [u8; VALUE_SIZE]),
    Get(u64),
    Remove(u64),
}

pub struct HashTableWorkload;

impl Workload for HashTableWorkload {
    type State = HashTable;

    fn name(&self) -> &'static str {
        "hashtable"
    }

    fn description(&self) -> &'static str {
        "Open-addressing hashtable — insert/remove heavy, linear probing, backward-shift delete"
    }

    fn init_state(&self) -> Self::State {
        let mut table = HashTable::new();
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(0xBA5E_1A61);
        for _ in 0..INITIAL_FILL {
            let key = rng.random_range(0..KEY_RANGE);
            let mut value = [0u8; VALUE_SIZE];
            for byte in &mut value {
                *byte = rng.random::<u8>();
            }
            table.insert(key, value);
        }
        table
    }

    fn run_thread<S: Schedule<Self::State>>(
        &self,
        lock: &S,
        thread_id: usize,
        _thread_count: usize,
        ops: usize,
    ) {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(thread_id as u64 * 88888 + 42424);

        // 45% insert, 45% remove, 10% get
        let operations: Vec<HashOp> = (0..ops)
            .map(|_| {
                let r: f64 = rng.random();
                if r < 0.45 {
                    let key = rng.random_range(0..KEY_RANGE);
                    let mut value = [0u8; VALUE_SIZE];
                    for byte in &mut value {
                        *byte = rng.random::<u8>();
                    }
                    HashOp::Insert(key, value)
                } else if r < 0.90 {
                    let key = rng.random_range(0..KEY_RANGE);
                    HashOp::Remove(key)
                } else {
                    let key = rng.random_range(0..KEY_RANGE);
                    HashOp::Get(key)
                }
            })
            .collect();

        for batch in operations.chunks(BATCH_SIZE) {
            let result = lock.schedule(|table| {
                let mut checksum = 0u64;
                for op in batch {
                    match *op {
                        HashOp::Insert(key, value) => {
                            if table.insert(key, value) {
                                checksum = checksum.wrapping_add(1);
                            }
                        }
                        HashOp::Get(key) => {
                            if let Some(cs) = table.get(key) {
                                checksum = checksum.wrapping_add(cs);
                            }
                        }
                        HashOp::Remove(key) => {
                            if table.remove(key) {
                                checksum = checksum.wrapping_add(1);
                            }
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

    fn val(v: u8) -> [u8; VALUE_SIZE] {
        let mut a = [0u8; VALUE_SIZE];
        a[0] = v;
        a
    }

    #[test]
    fn test_insert_get() {
        let mut t = HashTable::new();
        assert!(t.insert(10, val(1)));
        assert!(t.insert(20, val(2)));
        assert_eq!(t.count, 2);
        assert!(t.get(10).is_some());
        assert!(t.get(20).is_some());
        assert!(t.get(30).is_none());
    }

    #[test]
    fn test_overwrite() {
        let mut t = HashTable::new();
        assert!(t.insert(10, val(1)));
        assert!(!t.insert(10, val(2))); // overwrite returns false
        assert_eq!(t.count, 1);
    }

    #[test]
    fn test_remove() {
        let mut t = HashTable::new();
        t.insert(10, val(1));
        t.insert(20, val(2));
        assert!(t.remove(10));
        assert_eq!(t.count, 1);
        assert!(t.get(10).is_none());
        assert!(t.get(20).is_some());
        assert!(!t.remove(10)); // already removed
    }

    #[test]
    fn test_remove_with_chain() {
        // Force a probe chain by inserting keys that hash to the same slot.
        let mut t = HashTable::new();
        // Insert several keys — some will collide and form chains.
        let keys: Vec<u64> = (0..64).collect();
        for &k in &keys {
            t.insert(k, val(k as u8));
        }
        assert_eq!(t.count, 64);
        // Remove every other key, then verify the remaining ones are still reachable.
        for &k in keys.iter().step_by(2) {
            assert!(t.remove(k));
        }
        assert_eq!(t.count, 32);
        for &k in keys.iter().skip(1).step_by(2) {
            assert!(t.get(k).is_some(), "key {} should still be present", k);
        }
    }

    #[test]
    fn test_full() {
        let mut t = HashTable::new();
        for i in 0..CAPACITY as u64 {
            assert!(t.insert(i, val(0)));
        }
        assert!(!t.insert(9999, val(0)));
        assert_eq!(t.count, CAPACITY);
    }
}
