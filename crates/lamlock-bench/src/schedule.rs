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

impl<T: Send> Schedule<T> for Lock<T> {
    fn name() -> &'static str {
        "lamlock"
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
