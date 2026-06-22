use clap::Parser;
use color_eyre::eyre::Result;
use common::{Log, LogLevel, LogwebSender, Message, MessageContent, unix_timestamp};
use std::{
    io::{self, BufRead},
    net::TcpStream,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{Receiver, RecvTimeoutError, TrySendError},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::classification::LogClassifier;

mod classification;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The spider endpoint to connect to.
    ///
    /// Examples:
    /// - 127.0.0.1:9000
    /// - [::1]:9000
    /// - logs.example.com:9000
    spider_endpoint: String,

    /// Producer name to attach to outgoing log messages.
    producer: String,

    /// Maximum approximate size of a log batch before sending.
    #[arg(long, default_value_t = 64 * 1024)]
    max_batch_bytes: usize,

    /// Maximum age of a log batch before sending, in seconds.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..))]
    max_batch_age_secs: u64,

    /// Seconds to wait between reconnect attempts.
    #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..))]
    retry_secs: u64,

    /// Maximum number of logs to queue before skipping incoming logs.
    #[arg(long, default_value_t = 10_000, value_parser = clap::value_parser!(usize))]
    max_queue_logs: usize,

    /// Text file containing level=regex mappings for log classification.
    #[arg(long)]
    level_regex_file: Option<PathBuf>,
}

struct Config {
    producer: String,
    max_batch_bytes: usize,
    max_batch_age: Duration,
    retry_delay: Duration,
    classifier: LogClassifier,
}

struct QueuedLog {
    log: Log,
    approx_bytes: usize,
}

fn main() -> Result<()> {
    color_eyre::install()?;

    let cli = Cli::parse();
    let spider_endpoint = cli.spider_endpoint;
    let max_queue_logs = cli.max_queue_logs;

    let classifier = match &cli.level_regex_file {
        Some(path) => LogClassifier::from_file(path)?,
        None => LogClassifier::new(),
    };

    let config = Config {
        producer: cli.producer,
        max_batch_bytes: cli.max_batch_bytes,
        max_batch_age: Duration::from_secs(cli.max_batch_age_secs),
        retry_delay: Duration::from_secs(cli.retry_secs),
        classifier,
    };

    let connected = Arc::new(AtomicBool::new(false));
    let skipped_logs = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = std::sync::mpsc::sync_channel(max_queue_logs);

    let sending_connected = Arc::clone(&connected);
    let sending_skipped_logs = Arc::clone(&skipped_logs);
    let sending_thread = std::thread::spawn(move || {
        sending_thread(
            spider_endpoint,
            config,
            rx,
            sending_connected,
            sending_skipped_logs,
        );
    });

    for line in io::stdin().lock().lines() {
        let content = line?;

        let queued_log = QueuedLog {
            approx_bytes: approx_log_size(&content),
            log: Log {
                occurrence: unix_timestamp(),
                // LogLevel is classified later in the sending_thread,
                // to take work off the main thread.
                level: LogLevel::Unknown,
                content,
            },
        };

        if !connected.load(Ordering::Relaxed) {
            skipped_logs.fetch_add(1, Ordering::Relaxed);
            continue;
        }

        match tx.try_send(queued_log) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                skipped_logs.fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Disconnected(_)) => break,
        }
    }

    drop(tx);
    let _ = sending_thread.join();

    Ok(())
}

fn sending_thread(
    spider_endpoint: String,
    config: Config,
    rx: Receiver<QueuedLog>,
    connected: Arc<AtomicBool>,
    skipped_logs: Arc<AtomicUsize>,
) {
    loop {
        connected.store(false, Ordering::Relaxed);

        let stream = match TcpStream::connect(spider_endpoint.as_str()) {
            Ok(stream) => stream,
            Err(_) => {
                if !drop_logs_until_retry(&rx, config.retry_delay, &skipped_logs) {
                    return;
                }
                continue;
            }
        };

        let mut sender = LogwebSender::new(config.producer.clone(), stream);
        connected.store(true, Ordering::Relaxed);

        if send_truncated_if_needed(&mut sender, &config.producer, &skipped_logs)
            .and_then(|_| send_connected(&mut sender, &rx, &config, &skipped_logs))
            .is_ok()
        {
            connected.store(false, Ordering::Relaxed);
            let _ = sender.finish();
            return;
        }

        connected.store(false, Ordering::Relaxed);
        drop(sender);

        if !drop_logs_until_retry(&rx, config.retry_delay, &skipped_logs) {
            return;
        }
    }
}

fn send_connected(
    sender: &mut LogwebSender,
    rx: &Receiver<QueuedLog>,
    config: &Config,
    skipped_logs: &AtomicUsize,
) -> io::Result<()> {
    let mut batch = Vec::new();
    let mut batch_bytes = 0;
    let mut batch_started = Instant::now();

    loop {
        if batch.is_empty() {
            send_truncated_if_needed(sender, &config.producer, skipped_logs)?;

            let Ok(mut queued_log) = rx.recv() else {
                break;
            };

            batch_started = Instant::now();
            batch_bytes = queued_log.approx_bytes;
            queued_log.log.level = config.classifier.classify(&queued_log.log.content);
            batch.push(queued_log.log);
        }

        let elapsed = batch_started.elapsed();

        if batch_bytes >= config.max_batch_bytes || elapsed >= config.max_batch_age {
            flush(sender, &config.producer, &mut batch)?;
            batch_bytes = 0;
            continue;
        }

        match rx.recv_timeout(config.max_batch_age.saturating_sub(elapsed)) {
            Ok(mut queued_log) => {
                batch_bytes += queued_log.approx_bytes;
                queued_log.log.level = config.classifier.classify(&queued_log.log.content);
                batch.push(queued_log.log);
            }
            Err(RecvTimeoutError::Timeout) => {
                flush(sender, &config.producer, &mut batch)?;
                batch_bytes = 0;
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }

    if !batch.is_empty() {
        flush(sender, &config.producer, &mut batch)?;
    }

    send_truncated_if_needed(sender, &config.producer, skipped_logs)?;

    Ok(())
}

fn drop_logs_until_retry(
    rx: &Receiver<QueuedLog>,
    retry_delay: Duration,
    skipped_logs: &AtomicUsize,
) -> bool {
    let retry_at = Instant::now() + retry_delay;

    loop {
        let remaining = retry_at.saturating_duration_since(Instant::now());

        if remaining.is_zero() {
            return true;
        }

        match rx.recv_timeout(remaining) {
            Ok(_) => {
                skipped_logs.fetch_add(1, Ordering::Relaxed);
            }
            Err(RecvTimeoutError::Timeout) => return true,
            Err(RecvTimeoutError::Disconnected) => return false,
        }
    }
}

fn send_truncated_if_needed(
    sender: &mut LogwebSender,
    producer: &str,
    skipped_logs: &AtomicUsize,
) -> io::Result<()> {
    let skipped = skipped_logs.swap(0, Ordering::Relaxed);

    if skipped == 0 {
        return Ok(());
    }

    let result = sender
        .send(&Message {
            producer: producer.to_string(),
            content: MessageContent::Truncated,
        })
        .and_then(|_| sender.flush());

    if result.is_err() {
        skipped_logs.fetch_add(skipped, Ordering::Relaxed);
    }

    result
}

fn flush(sender: &mut LogwebSender, producer: &str, batch: &mut Vec<Log>) -> io::Result<()> {
    sender.send(&Message {
        producer: producer.to_string(),
        content: MessageContent::Logs(std::mem::take(batch)),
    })?;

    sender.flush()
}

fn approx_log_size(content: &str) -> usize {
    content.len() + std::mem::size_of::<u64>() + std::mem::size_of::<LogLevel>() + 16
}
