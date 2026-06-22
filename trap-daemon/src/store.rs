use std::{
    io::{self, Write},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use common::{serialize_log, serialized_log_len};
use crossbeam::utils::CachePadded;
use parking_lot::Mutex;

const DROPPED_LOG_PRODUCER: &[u8] = b"trap-daemon";

#[derive(Clone)]
pub struct LogStore {
    inner: Arc<LogStoreInner>,
}

impl LogStore {
    pub fn new(max_pending_bytes: usize) -> Self {
        Self {
            inner: Arc::new(LogStoreInner::new(max_pending_bytes)),
        }
    }

    pub fn store(&self, timestamp: u64, producer: &[u8], message: &[u8]) -> bool {
        self.inner.store(timestamp, producer, message)
    }

    pub fn read_into<W: Write>(&self, dst: W) -> io::Result<usize> {
        self.inner.read_into(dst)
    }

    pub fn has_pending(&self) -> bool {
        self.inner.has_pending()
    }
}

struct LogStoreInner {
    write_store: CachePadded<Mutex<Vec<u8>>>,
    read_store: Mutex<Vec<u8>>,
    pending_bytes: AtomicUsize,
    max_pending_bytes: usize,
    dropped_records: AtomicU64,
}

impl LogStoreInner {
    pub fn new(max_pending_bytes: usize) -> Self {
        Self {
            write_store: CachePadded::new(Mutex::new(Vec::with_capacity(8 * 8192))),
            read_store: Mutex::new(Vec::with_capacity(8 * 8192)),
            pending_bytes: AtomicUsize::new(0),
            max_pending_bytes,
            dropped_records: AtomicU64::new(0),
        }
    }

    fn store(&self, timestamp: u64, producer: &[u8], message: &[u8]) -> bool {
        let len = serialized_log_len(producer, message);

        if !self.try_reserve(len) {
            self.dropped_records.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        let mut store = self.write_store.lock();
        let written = serialize_log(&mut *store, timestamp, producer, message)
            .expect("serializing into a Vec cannot fail");
        debug_assert_eq!(written, len);

        true
    }

    fn read_into<W: Write>(&self, mut dst: W) -> io::Result<usize> {
        let mut written = self.write_read_store(&mut dst)?;

        {
            let mut write_store = self.write_store.lock();
            self.append_dropped_report(&mut write_store);

            if write_store.is_empty() {
                return Ok(written);
            }

            let mut read_store = self.read_store.lock();
            debug_assert!(read_store.is_empty());
            std::mem::swap(&mut *write_store, &mut *read_store);
        }

        written += self.write_read_store(&mut dst)?;

        Ok(written)
    }

    fn write_read_store<W: Write>(&self, dst: &mut W) -> io::Result<usize> {
        let mut read_store = self.read_store.lock();

        if read_store.is_empty() {
            return Ok(0);
        }

        dst.write_all(&read_store)?;

        let len = read_store.len();
        read_store.clear();
        self.release(len);

        Ok(len)
    }

    fn append_dropped_report(&self, write_store: &mut Vec<u8>) {
        let dropped = self.dropped_records.swap(0, Ordering::AcqRel);

        if dropped == 0 {
            return;
        }

        let message = format!(
            "dropped {dropped} log records because pending buffer exceeded {} bytes",
            self.max_pending_bytes
        );
        let len = serialized_log_len(DROPPED_LOG_PRODUCER, message.as_bytes());

        if !self.try_reserve_report(len) {
            self.dropped_records.fetch_add(dropped, Ordering::Relaxed);
            return;
        }

        let written = serialize_log(
            write_store,
            now_realtime_usec(),
            DROPPED_LOG_PRODUCER,
            message.as_bytes(),
        )
        .expect("serializing into a Vec cannot fail");
        debug_assert_eq!(written, len);
    }

    fn has_pending(&self) -> bool {
        self.pending_bytes.load(Ordering::Acquire) > 0
            || self.dropped_records.load(Ordering::Acquire) > 0
    }

    fn try_reserve(&self, len: usize) -> bool {
        if len > self.max_pending_bytes {
            return false;
        }

        let mut current = self.pending_bytes.load(Ordering::Acquire);

        loop {
            let Some(next) = current.checked_add(len) else {
                return false;
            };

            if next > self.max_pending_bytes {
                return false;
            }

            match self.pending_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    fn try_reserve_report(&self, len: usize) -> bool {
        if self.try_reserve(len) {
            return true;
        }

        self.pending_bytes
            .compare_exchange(0, len, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn release(&self, len: usize) {
        self.pending_bytes.fetch_sub(len, Ordering::AcqRel);
    }
}

fn now_realtime_usec() -> u64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    duration
        .as_secs()
        .saturating_mul(1_000_000)
        .saturating_add(u64::from(duration.subsec_micros()))
}
