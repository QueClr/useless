use std::hint::black_box;

use rand::Rng;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

use crate::schedule::Schedule;
use crate::workloads::Workload;

const NUM_SLABS: usize = 64;
const SLOTS_PER_SLAB: usize = 512; // 8 × u64 bitmap words
const BITMAP_WORDS: usize = SLOTS_PER_SLAB / 64;
const MAX_HELD_PER_THREAD: usize = 256;
const SLOT_SIZE: usize = 64;

struct Slab {
    bitmap: [u64; BITMAP_WORDS], // 1 = occupied, 0 = free
    free_count: u16,
}

impl Slab {
    fn new() -> Self {
        Self {
            bitmap: [0; BITMAP_WORDS],
            free_count: SLOTS_PER_SLAB as u16,
        }
    }
}

pub struct SlabAllocator {
    slabs: Vec<Slab>,
    free_count: usize,
}

impl SlabAllocator {
    fn new() -> Self {
        Self {
            slabs: (0..NUM_SLABS).map(|_| Slab::new()).collect(),
            free_count: NUM_SLABS * SLOTS_PER_SLAB,
        }
    }

    fn alloc(&mut self) -> Option<usize> {
        if self.free_count == 0 {
            return None;
        }
        for (slab_idx, slab) in self.slabs.iter_mut().enumerate() {
            if slab.free_count == 0 {
                continue;
            }
            for (word_idx, word) in slab.bitmap.iter_mut().enumerate() {
                if *word == u64::MAX {
                    continue;
                }
                let bit_idx = (!*word).trailing_zeros() as usize;
                *word |= 1u64 << bit_idx;
                slab.free_count -= 1;
                self.free_count -= 1;
                return Some(slab_idx * SLOTS_PER_SLAB + word_idx * 64 + bit_idx);
            }
        }
        None
    }

    fn free(&mut self, slot: usize) {
        let slab_idx = slot / SLOTS_PER_SLAB;
        let within_slab = slot % SLOTS_PER_SLAB;
        let word_idx = within_slab / 64;
        let bit_idx = within_slab % 64;
        let slab = &mut self.slabs[slab_idx];
        slab.bitmap[word_idx] &= !(1u64 << bit_idx);
        slab.free_count += 1;
        self.free_count += 1;
    }
}

pub struct SlabWorkload;

impl Workload for SlabWorkload {
    type State = SlabAllocator;

    fn name(&self) -> &'static str {
        "slab"
    }

    fn description(&self) -> &'static str {
        "Slab allocator bitmap — scan/modify bitmaps inside lock, write scratch buffer outside"
    }

    fn init_state(&self) -> Self::State {
        // Pre-allocate ~50% of slots for realistic fragmentation
        let mut alloc = SlabAllocator::new();
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(0x514B_A10C);
        // Set random bits in each slab to get ~50% occupancy
        for slab in &mut alloc.slabs {
            for word in &mut slab.bitmap {
                *word = rng.random::<u64>(); // ~50% bits set
            }
            slab.free_count = slab.bitmap.iter().map(|w| w.count_zeros() as u16).sum();
        }
        alloc.free_count = alloc.slabs.iter().map(|s| s.free_count as usize).sum();
        alloc
    }

    fn run_thread<S: Schedule<Self::State>>(
        &self,
        lock: &S,
        thread_id: usize,
        _thread_count: usize,
        ops: usize,
    ) {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(thread_id as u64 * 11111 + 99999);
        let mut held: Vec<usize> = Vec::with_capacity(MAX_HELD_PER_THREAD);
        let scratch_size = MAX_HELD_PER_THREAD * SLOT_SIZE;
        let mut scratch = vec![0u8; scratch_size];
        let mut accumulator: u64 = 0;

        for _ in 0..ops {
            if held.len() < MAX_HELD_PER_THREAD && rng.random::<f64>() < 0.6 {
                // Allocate
                let slot = lock.schedule(|alloc| alloc.alloc());
                if let Some(slot) = black_box(slot) {
                    // Outside lock: write to scratch buffer at slot-derived offset
                    let offset = (slot * SLOT_SIZE) % (scratch_size - SLOT_SIZE);
                    let end = offset + SLOT_SIZE;
                    let pattern = (slot & 0xFF) as u8;
                    for byte in &mut scratch[offset..end] {
                        *byte ^= pattern;
                    }
                    held.push(slot);
                }
            } else if !held.is_empty() {
                // Free
                let idx = rng.random_range(0..held.len());
                let slot = held.swap_remove(idx);

                // Outside lock: read from scratch buffer, accumulate
                let offset = (slot % scratch.len()).saturating_sub(SLOT_SIZE).max(0);
                let end = (offset + SLOT_SIZE).min(scratch.len());
                for &byte in &scratch[offset..end] {
                    accumulator = accumulator.wrapping_add(byte as u64);
                }

                lock.schedule(|alloc| alloc.free(slot));
            }
        }
        black_box(accumulator);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slab_alloc_free() {
        let mut alloc = SlabAllocator::new();
        let total = NUM_SLABS * SLOTS_PER_SLAB;
        assert_eq!(alloc.free_count, total);

        let slot = alloc.alloc().unwrap();
        assert_eq!(alloc.free_count, total - 1);

        alloc.free(slot);
        assert_eq!(alloc.free_count, total);
    }

    #[test]
    fn test_slab_alloc_unique() {
        let mut alloc = SlabAllocator::new();
        let mut slots = Vec::new();
        for _ in 0..1024 {
            slots.push(alloc.alloc().unwrap());
        }
        // All slots should be unique
        slots.sort();
        slots.dedup();
        assert_eq!(slots.len(), 1024);
    }

    #[test]
    fn test_slab_full() {
        let mut alloc = SlabAllocator::new();
        let total = NUM_SLABS * SLOTS_PER_SLAB;
        for _ in 0..total {
            assert!(alloc.alloc().is_some());
        }
        assert!(alloc.alloc().is_none());
        assert_eq!(alloc.free_count, 0);
    }
}
