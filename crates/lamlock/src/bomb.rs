use core::{mem::ManuallyDrop, ptr::NonNull, sync::atomic::Ordering};

use crate::{node::Node, rawlock::RawLock};

pub struct LightWeightBomb<'a, const PANIC_SAFE: bool = true> {
    raw: &'a RawLock,
}

impl<'a, const PANIC_SAFE: bool> LightWeightBomb<'a, PANIC_SAFE> {
    pub fn new(raw: &'a RawLock) -> Self {
        Self { raw }
    }

    pub fn get_raw(&self) -> &'a RawLock {
        self.raw
    }

    pub fn diffuse(self) {
        core::mem::forget(self);
    }
}

impl<'a, const PANIC_SAFE: bool> Drop for LightWeightBomb<'a, PANIC_SAFE> {
    #[cold]
    fn drop(&mut self) {
        if PANIC_SAFE {
            self.raw.poison();
        }
    }
}

pub struct HeavyWeightBomb<'a, const PANIC_SAFE: bool = true, const USE_FUTEX: bool = true> {
    ignitor: ManuallyDrop<LightWeightBomb<'a, PANIC_SAFE>>,
    atom: NonNull<Node>,
}

impl<'a, const PANIC_SAFE: bool, const USE_FUTEX: bool> Drop
    for HeavyWeightBomb<'a, PANIC_SAFE, USE_FUTEX>
{
    #[cold]
    fn drop(&mut self) {
        if !PANIC_SAFE {
            return;
        }
        unsafe {
            ManuallyDrop::drop(&mut self.ignitor);
        }
        loop {
            let next = unsafe { self.atom.as_ref().load_next(Ordering::Acquire) };
            if let Some(next) = next {
                Node::wake_as_poisoned::<USE_FUTEX>(self.atom);
                self.atom = next;
                continue;
            }
            if self.ignitor.get_raw().try_close(self.atom) {
                Node::wake_as_poisoned::<USE_FUTEX>(self.atom);
                break;
            }
            while unsafe { self.atom.as_ref().load_next(Ordering::Relaxed).is_none() } {
                core::hint::spin_loop();
            }
        }
    }
}

impl<'a, const PANIC_SAFE: bool, const USE_FUTEX: bool> HeavyWeightBomb<'a, PANIC_SAFE, USE_FUTEX> {
    pub fn new(lock: &'a RawLock, atom: NonNull<Node>) -> Self {
        Self {
            ignitor: ManuallyDrop::new(LightWeightBomb::new(lock)),
            atom,
        }
    }
    pub fn diffuse(self) {
        core::mem::forget(self);
    }
    pub fn reset(&mut self, new_atom: NonNull<Node>) {
        self.atom = new_atom;
    }
}

#[cfg(test)]
mod tests {
    extern crate std;
    use super::*;
    use crate::node::{self, Node};
    use crate::rawlock::RawLock;
    use core::ptr::NonNull;

    #[test]
    fn test_light_weight_bomb_diffuse() {
        let raw = RawLock::new();
        let bomb = LightWeightBomb::<true>::new(&raw);
        bomb.diffuse();
        assert!(!raw.is_poisoned(core::sync::atomic::Ordering::Acquire));
    }

    #[test]
    fn test_light_weight_bomb_poison() {
        let raw = RawLock::new();
        std::thread::scope(|s| {
            let raw = &raw;
            s.spawn(move || {
                LightWeightBomb::<true>::new(&raw);
            });
            while !raw.is_poisoned(core::sync::atomic::Ordering::Acquire) {
                core::hint::spin_loop();
            }
        });
    }

    #[test]
    fn test_light_weight_bomb_no_panic_no_poison() {
        let raw = RawLock::new();
        std::thread::scope(|s| {
            let raw = &raw;
            s.spawn(move || {
                LightWeightBomb::<false>::new(&raw);
            });
        });
        assert!(!raw.is_poisoned(core::sync::atomic::Ordering::Acquire));
    }

    #[test]
    fn test_heavy_weight_bomb() {
        const NUM_THREADS: usize = 10;
        let barrier = std::sync::Barrier::new(NUM_THREADS);
        let raw = RawLock::new();
        std::thread::scope(|s| {
            let raw = &raw;
            let barrier = &barrier;
            for _ in 0..NUM_THREADS {
                s.spawn({
                    let raw = raw;
                    move || {
                        let node = Node::new(|_| {});
                        let this = NonNull::from(&node);
                        if let Some(prev) = raw.swap_tail(this) {
                            unsafe {
                                prev.as_ref().store_next(this);
                            }
                            barrier.wait();
                            assert!(node.wait::<true>() == node::POISONED);
                        } else {
                            let _bomb = HeavyWeightBomb::<true, true>::new(raw, this);
                            barrier.wait();
                        }
                    }
                });
            }
        });
    }
}
