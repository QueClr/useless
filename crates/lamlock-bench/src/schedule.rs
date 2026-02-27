use std::sync::Mutex;

use lamlock::Lock;

pub trait Schedule<T>: Sync + Send {
    fn name() -> &'static str
    where
        Self: Sized;
    fn new(value: T) -> Self
    where
        Self: Sized;
    fn schedule<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R + Send,
        R: Send;
}

impl<T: Send, const USE_FUTEX: bool, const PANIC_SAFE: bool> Schedule<T>
    for Lock<T, USE_FUTEX, PANIC_SAFE>
{
    fn name() -> &'static str {
        match (USE_FUTEX, PANIC_SAFE) {
            (true, true) => "lamlock",
            (true, false) => "lamlock-no-panic",
            (false, true) => "lamlock-spin",
            (false, false) => "lamlock-spin-no-panic",
        }
    }
    fn new(value: T) -> Self {
        Lock::new(value)
    }
    fn schedule<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R + Send,
        R: Send,
    {
        self.run(f).unwrap()
    }
}

impl<T: Send> Schedule<T> for Mutex<T> {
    fn name() -> &'static str {
        "std-mutex"
    }
    fn new(value: T) -> Self {
        Mutex::new(value)
    }
    fn schedule<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R + Send,
        R: Send,
    {
        f(&mut self.lock().unwrap())
    }
}
