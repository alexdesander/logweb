use std::{
    io::{self, Write},
    net::TcpStream,
    time::{Duration, Instant},
};

use clap::Parser;
use zstd::stream::write::Encoder;

use crate::{
    collectors::{CollectionThread, systemd::SystemdCollector},
    store::LogStore,
};

mod collectors;
mod store;
mod waker;

const DEFAULT_SEND_INTERVAL_MS: u64 = 500;
const DEFAULT_MAX_PENDING_BYTES: usize = 64 * 1024 * 1024;
const RECONNECT_INITIAL_DELAY: Duration = Duration::from_millis(250);
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);
const ZSTD_LEVEL: i32 = 2;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The spider endpoint to connect to.
    #[arg(value_name = "SPIDER_ENDPOINT")]
    spider_endpoint: String,

    /// Minimum milliseconds between send attempts.
    #[arg(long, default_value_t = DEFAULT_SEND_INTERVAL_MS)]
    send_interval_ms: u64,

    /// Maximum pending encoded log bytes before new records are dropped.
    #[arg(long, default_value_t = DEFAULT_MAX_PENDING_BYTES)]
    max_pending_bytes: usize,
}

fn main() -> io::Result<()> {
    let cli = Cli::parse();

    if cli.send_interval_ms == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--send-interval-ms must be greater than zero",
        ));
    }

    if cli.max_pending_bytes == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--max-pending-bytes must be greater than zero",
        ));
    }

    let send_interval = Duration::from_millis(cli.send_interval_ms);
    let log_store = LogStore::new(cli.max_pending_bytes);
    let (waiter, waker) = waker::Waiter::pair();

    let collector = SystemdCollector::new()?;
    let _systemd_collector = CollectionThread::run(collector, waker.clone(), log_store.clone());

    let mut writer = connect_spider(&cli.spider_endpoint);

    let mut last_send = Instant::now();

    loop {
        if !log_store.has_pending() {
            waiter.wait();
        }

        let elapsed = last_send.elapsed();
        if elapsed < send_interval {
            std::thread::sleep(send_interval - elapsed);
        }

        last_send = Instant::now();

        if let Err(err) = log_store
            .read_into(&mut writer)
            .and_then(|_| writer.flush())
        {
            eprintln!(
                "tcp connection to {} broke: {err}; reconnecting",
                cli.spider_endpoint
            );
            writer = connect_spider(&cli.spider_endpoint);
            last_send = Instant::now() - send_interval;
        }
    }
}

fn connect_spider(spider_endpoint: &str) -> Encoder<'static, TcpStream> {
    let mut delay = RECONNECT_INITIAL_DELAY;

    loop {
        match try_connect_spider(spider_endpoint) {
            Ok(writer) => {
                eprintln!("connected to {spider_endpoint}");
                return writer;
            }
            Err(err) => {
                eprintln!("failed to connect to {spider_endpoint}: {err}; retrying in {delay:?}");
                std::thread::sleep(delay);
                delay = (delay * 2).min(RECONNECT_MAX_DELAY);
            }
        }
    }
}

fn try_connect_spider(spider_endpoint: &str) -> io::Result<Encoder<'static, TcpStream>> {
    let spider = TcpStream::connect(spider_endpoint)?;
    spider.set_nodelay(true)?;

    Encoder::new(spider, ZSTD_LEVEL)
}
