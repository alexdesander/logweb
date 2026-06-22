use std::{
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    thread::{self, Thread, ThreadId},
    time::{Duration, Instant},
};

const EMPTY: u8 = 0;
const PARKED: u8 = 1;
const NOTIFIED: u8 = 2;

pub struct Waiter {
    inner: Arc<Inner>,
}

#[derive(Clone)]
pub struct Waker {
    inner: Arc<Inner>,
}

struct Inner {
    state: AtomicU8,
    sleeper: Thread,
    sleeper_id: ThreadId,
}

impl Waiter {
    /// Must be created on the thread that will call `wait`.
    pub fn new() -> Self {
        let sleeper = thread::current();

        Self {
            inner: Arc::new(Inner {
                state: AtomicU8::new(EMPTY),
                sleeper_id: sleeper.id(),
                sleeper,
            }),
        }
    }

    pub fn waker(&self) -> Waker {
        Waker {
            inner: self.inner.clone(),
        }
    }

    pub fn pair() -> (Self, Waker) {
        let waiter = Self::new();
        let waker = waiter.waker();
        (waiter, waker)
    }

    pub fn wait(&self) {
        assert_eq!(
            thread::current().id(),
            self.inner.sleeper_id,
            "Waiter may only be used by the thread that created it"
        );

        loop {
            match self.inner.state.load(Ordering::Acquire) {
                NOTIFIED => {
                    if self
                        .inner
                        .state
                        .compare_exchange(NOTIFIED, EMPTY, Ordering::Acquire, Ordering::Acquire)
                        .is_ok()
                    {
                        return;
                    }
                }

                EMPTY => {
                    let _ = self.inner.state.compare_exchange(
                        EMPTY,
                        PARKED,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                }

                PARKED => {
                    thread::park();
                }

                _ => unreachable!(),
            }
        }
    }

    /// Returns `true` if woken, `false` if the timeout elapsed.
    pub fn _wait_timeout(&self, dur: Duration) -> bool {
        assert_eq!(
            thread::current().id(),
            self.inner.sleeper_id,
            "Waiter may only be used by the thread that created it"
        );

        let start = Instant::now();

        loop {
            match self.inner.state.load(Ordering::Acquire) {
                NOTIFIED => {
                    if self
                        .inner
                        .state
                        .compare_exchange(NOTIFIED, EMPTY, Ordering::Acquire, Ordering::Acquire)
                        .is_ok()
                    {
                        return true;
                    }
                }

                EMPTY => {
                    let _ = self.inner.state.compare_exchange(
                        EMPTY,
                        PARKED,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                }

                PARKED => {
                    let elapsed = start.elapsed();

                    if elapsed >= dur {
                        match self.inner.state.compare_exchange(
                            PARKED,
                            EMPTY,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(_) => return false,
                            Err(NOTIFIED) => continue,
                            Err(_) => continue,
                        }
                    }

                    thread::park_timeout(dur - elapsed);
                }

                _ => unreachable!(),
            }
        }
    }
}

impl Waker {
    pub fn wake(&self) {
        let previous = self.inner.state.swap(NOTIFIED, Ordering::Release);

        if previous == PARKED {
            self.inner.sleeper.unpark();
        }
    }
}
