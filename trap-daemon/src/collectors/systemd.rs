use std::{
    ffi::c_char,
    io, ptr, slice,
    sync::atomic::{AtomicBool, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use libc::size_t;
use libsystemd_sys::journal::{
    sd_journal, sd_journal_close, sd_journal_get_data, sd_journal_get_realtime_usec,
    sd_journal_next, sd_journal_open, sd_journal_seek_realtime_usec, sd_journal_wait,
};

use crate::{collectors::LogCollector, store::LogStore};

const JOURNAL_WAIT_TIMEOUT_USEC: u64 = 500_000;

const SOURCE_PLACEHOLDER: &[u8] = b"[PLACEHOLDER]";
const MESSAGE_PLACEHOLDER: &[u8] = b"[PLACEHOLDER]";

const SOURCE_FIELDS: &[&[u8]] = &[
    b"_SYSTEMD_UNIT\0",
    b"_SYSTEMD_USER_UNIT\0",
    b"SYSLOG_IDENTIFIER\0",
    b"_COMM\0",
    b"_EXE\0",
    b"_PID\0",
];

pub struct SystemdCollector {
    journal: *mut sd_journal,

    // Reused every entry. This avoids allocating a new Vec/String per log.
    source_buf: Vec<u8>,
}

unsafe impl Send for SystemdCollector {}

impl SystemdCollector {
    pub fn new() -> io::Result<Self> {
        let start_usec = now_realtime_usec();
        let mut journal: *mut sd_journal = ptr::null_mut();

        unsafe {
            sd_result(sd_journal_open(&mut journal, 0))?;

            if let Err(err) = sd_result(sd_journal_seek_realtime_usec(journal, start_usec)) {
                sd_journal_close(journal);
                return Err(err);
            }
        }

        Ok(Self {
            journal,
            source_buf: Vec::with_capacity(128),
        })
    }

    fn read_timestamp_usec(&self) -> io::Result<u64> {
        let mut timestamp_usec = 0_u64;

        unsafe {
            sd_result(sd_journal_get_realtime_usec(
                self.journal,
                &mut timestamp_usec,
            ))?;
        }

        Ok(timestamp_usec)
    }

    fn read_source_into_buffer(&mut self) -> io::Result<&[u8]> {
        unsafe {
            self.source_buf.clear();

            for &field in SOURCE_FIELDS {
                let Some(source) = read_field(self.journal, field)? else {
                    continue;
                };

                if source.is_empty() {
                    continue;
                }

                self.source_buf.extend_from_slice(source);
                return Ok(&self.source_buf);
            }

            self.source_buf.extend_from_slice(SOURCE_PLACEHOLDER);
            Ok(&self.source_buf)
        }
    }
}

impl Drop for SystemdCollector {
    fn drop(&mut self) {
        unsafe {
            if !self.journal.is_null() {
                sd_journal_close(self.journal);
            }
        }
    }
}

impl LogCollector for SystemdCollector {
    fn collect_next(&mut self, log_store: &LogStore, shutdown: &AtomicBool) -> io::Result<()> {
        unsafe {
            while !shutdown.load(Ordering::Acquire) {
                let n = sd_result(sd_journal_next(self.journal))?;

                if n == 0 {
                    sd_result(sd_journal_wait(self.journal, JOURNAL_WAIT_TIMEOUT_USEC))?;
                    continue;
                }

                let timestamp = self.read_timestamp_usec()?;

                // Copy source into reusable buffer because the next get_data call
                // may invalidate the borrowed source pointer.
                let journal = self.journal;
                let source = self.read_source_into_buffer()?;

                // MESSAGE is borrowed directly and serialized immediately before
                // another sd_journal_get_data / sd_journal_next call can invalidate it.
                let message = read_field(journal, b"MESSAGE\0")?.unwrap_or(MESSAGE_PLACEHOLDER);

                log_store.store(timestamp, source, message);

                return Ok(());
            }
        }

        Ok(())
    }
}

/// Returns the value part of `FIELD=value`.
///
/// `field` must be NUL-terminated, e.g. `b"MESSAGE\0"`.
///
/// The returned slice is journal-owned and very short-lived.
unsafe fn read_field<'a>(
    journal: *mut sd_journal,
    field: &'static [u8],
) -> io::Result<Option<&'a [u8]>> {
    unsafe {
        debug_assert_eq!(field.last(), Some(&0));

        let mut data: *mut u8 = ptr::null_mut();
        let mut len: size_t = 0;

        let r = sd_journal_get_data(
            journal,
            field.as_ptr() as *const c_char,
            &mut data,
            &mut len,
        );

        if r == -libc::ENOENT {
            return Ok(None);
        }

        sd_result(r)?;

        // `field` includes the trailing NUL, while returned data is `FIELD=value`.
        // So `field.len()` skips `FIELD=`.
        let value_offset = field.len();

        if data.is_null() || len <= value_offset {
            return Ok(None);
        }

        let bytes = slice::from_raw_parts(data as *const u8, len);

        Ok(Some(&bytes[value_offset..]))
    }
}

fn now_realtime_usec() -> u64 {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

    duration
        .as_secs()
        .saturating_mul(1_000_000)
        .saturating_add(u64::from(duration.subsec_micros()))
}

fn sd_result(r: i32) -> io::Result<i32> {
    if r < 0 {
        return Err(io::Error::from_raw_os_error(-r));
    }

    Ok(r)
}
