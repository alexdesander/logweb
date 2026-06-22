use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use crate::{store::LogStore, waker::Waker};

pub mod systemd;

pub trait LogCollector: Send {
    fn collect_next(&mut self, store: &LogStore, shutdown: &AtomicBool) -> io::Result<()>;
}

pub struct CollectionThread {
    shutdown: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl CollectionThread {
    pub fn run<C: LogCollector + 'static>(
        mut collector: C,
        waker: Waker,
        log_store: LogStore,
    ) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = Arc::clone(&shutdown);

        let thread = thread::spawn(move || {
            let store = log_store;
            while !thread_shutdown.load(Ordering::Acquire) {
                if let Err(err) = collector.collect_next(&store, &thread_shutdown) {
                    eprintln!("collector failed: {err}");
                    thread::sleep(Duration::from_millis(250));
                }

                waker.wake();
            }
        });

        Self {
            shutdown,
            thread: Some(thread),
        }
    }
}

impl Drop for CollectionThread {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);

        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
