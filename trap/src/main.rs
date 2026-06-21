use clap::Parser;
use color_eyre::eyre::Result;
use common::{Log, LogLevel, LogwebSender, Message, MessageContent};
use std::{
    io::{self, BufRead},
    net::{SocketAddr, TcpStream},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, RecvTimeoutError},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::classification::LogClassifier;

mod classification;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The socket address of the spider to connect to.
    spider_addr: SocketAddr,

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
    let (tx, rx) = std::sync::mpsc::channel();

    let sending_connected = Arc::clone(&connected);
    let sending_thread = std::thread::spawn(move || {
        sending_thread(cli.spider_addr, config, rx, sending_connected);
    });

    for line in io::stdin().lock().lines() {
        let content = line?;

        let queued_log = QueuedLog {
            approx_bytes: approx_log_size(&content),
            log: Log {
                occurance: unix_timestamp(),
                // LogLevel is classified later in the sending_thread,
                // to take work off the main thread.
                level: LogLevel::Unknown,
                content,
            },
        };

        if connected.load(Ordering::Relaxed) && tx.send(queued_log).is_err() {
            break;
        }
    }

    drop(tx);
    let _ = sending_thread.join();

    Ok(())
}

fn sending_thread(
    spider_addr: SocketAddr,
    config: Config,
    rx: Receiver<QueuedLog>,
    connected: Arc<AtomicBool>,
) {
    loop {
        connected.store(false, Ordering::Relaxed);

        let stream = match TcpStream::connect(spider_addr) {
            Ok(stream) => stream,
            Err(_) => {
                if !drop_logs_until_retry(&rx, config.retry_delay) {
                    return;
                }
                continue;
            }
        };

        let mut sender = LogwebSender::new(stream);
        connected.store(true, Ordering::Relaxed);

        if send_connected(&mut sender, &rx, &config).is_ok() {
            connected.store(false, Ordering::Relaxed);
            let _ = sender.finish();
            return;
        }

        connected.store(false, Ordering::Relaxed);
        drop(sender);

        if !drop_logs_until_retry(&rx, config.retry_delay) {
            return;
        }
    }
}

fn send_connected(
    sender: &mut LogwebSender,
    rx: &Receiver<QueuedLog>,
    config: &Config,
) -> io::Result<()> {
    let mut batch = Vec::new();
    let mut batch_bytes = 0;
    let mut batch_started = Instant::now();

    loop {
        if batch.is_empty() {
            let Ok(mut queued_log) = rx.recv() else { break };

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

    Ok(())
}

fn drop_logs_until_retry(rx: &Receiver<QueuedLog>, retry_delay: Duration) -> bool {
    let retry_at = Instant::now() + retry_delay;

    loop {
        let remaining = retry_at.saturating_duration_since(Instant::now());

        if remaining.is_zero() {
            return true;
        }

        match rx.recv_timeout(remaining) {
            Ok(_) => {}
            Err(RecvTimeoutError::Timeout) => return true,
            Err(RecvTimeoutError::Disconnected) => return false,
        }
    }
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

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is before unix epoch")
        .as_secs()
}
