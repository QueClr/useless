use core::{
    ptr::NonNull,
    sync::atomic::{AtomicU32, Ordering},
};

#[repr(transparent)]
pub struct Futex(AtomicU32);

impl core::ops::Deref for Futex {
    type Target = AtomicU32;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Futex {
    #[inline(always)]
    pub const fn new(value: u32) -> Self {
        Self(AtomicU32::new(value))
    }

    #[inline(always)]
    pub fn wait<const USE_FUTEX: bool>(this: NonNull<Self>, value: u32) {
        if USE_FUTEX {
            Self::wait_futex(this, value);
        } else {
            Self::wait_spinning(this, value);
        }
    }

    #[inline(always)]
    fn wait_futex(this: NonNull<Self>, value: u32) {
        #[cfg(not(miri))]
        while unsafe { this.as_ref().load(Ordering::Acquire) == value } {
            while let Err(rustix::io::Errno::INTR) = rustix::thread::futex::wait(
                unsafe { &this.as_ref().0 },
                rustix::thread::futex::Flags::PRIVATE,
                value,
                None,
            ) {
                core::hint::spin_loop();
            }
        }

        #[cfg(miri)]
        while unsafe { this.as_ref().load(Ordering::Acquire) == value } {
            core::hint::spin_loop();
        }
    }

    #[inline(always)]
    fn wait_spinning(this: NonNull<Self>, value: u32) {
        while unsafe { this.as_ref().load(Ordering::Acquire) == value } {
            core::hint::spin_loop();
        }
    }

    #[inline(always)]
    pub fn notify<const USE_FUTEX: bool>(
        this: NonNull<Self>,
        new_val: u32,
        #[allow(unused)] old_val: u32,
    ) {
        if USE_FUTEX {
            Self::notify_futex(this, new_val, old_val);
        } else {
            Self::notify_spinning(this, new_val);
        }
    }

    #[inline(always)]
    fn notify_futex(this: NonNull<Self>, new_val: u32, #[allow(unused)] old_val: u32) {
        #[cfg(not(miri))]
        if unsafe { this.as_ref().swap(new_val, Ordering::AcqRel) == old_val } {
            let _ = rustix::thread::futex::wake(
                unsafe { &this.as_ref().0 },
                rustix::thread::futex::Flags::PRIVATE,
                1,
            );
        }

        #[cfg(miri)]
        unsafe {
            this.as_ref().store(new_val, Ordering::Release);
        }
    }

    #[inline(always)]
    fn notify_spinning(this: NonNull<Self>, new_val: u32) {
        unsafe {
            this.as_ref().store(new_val, Ordering::Release);
        }
    }
}
