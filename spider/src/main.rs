use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

use async_compression::tokio::bufread::ZstdDecoder;
use clap::Parser;
use color_eyre::{
    Report,
    eyre::{Context, Result, eyre},
};
use common::{OwnedLog, deserialize_log_async};
use tokio::{
    io::BufReader,
    net::{TcpListener, TcpStream},
    sync::mpsc,
};

use crate::database::StoredLog;

const LOG_CHANNEL_CAPACITY: usize = 8_192;
const LOG_BATCH_MAX_RECORDS: usize = 256;

mod database;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The port that traps have to connect with.
    listening_port: u16,
    /// Where the SQLite database is stored.
    database_path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    let cli = Cli::parse();
    let database = database::connect(&cli.database_path)?;

    let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), cli.listening_port);

    let listener = TcpListener::bind(listen_addr)
        .await
        .wrap_err_with(|| format!("failed to bind listener on {listen_addr}"))?;

    let (log_tx, log_rx) = mpsc::channel(LOG_CHANNEL_CAPACITY);

    tokio::task::spawn_blocking(move || {
        if let Err(err) = database_writer_task(database, log_rx) {
            eprintln!("database writer task failed: {err:?}");
        }
    });

    println!("Listening on {listen_addr}");

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                let log_tx = log_tx.clone();

                tokio::spawn(async move {
                    handle_trap_connection(stream, addr, log_tx).await;
                });
            }
            Err(err) => {
                eprintln!("Failed to accept incoming connection: {err}");

                // Avoid spinning if the listener repeatedly errors.
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

async fn handle_trap_connection(
    stream: TcpStream,
    addr: SocketAddr,
    log_tx: mpsc::Sender<StoredLog>,
) {
    match process_trap(stream, addr, log_tx).await {
        Ok(()) => {
            println!("[{addr}] connection closed");
        }
        Err(err) => {
            print_connection_error(addr, &err);
        }
    }
}

async fn process_trap(
    stream: TcpStream,
    addr: SocketAddr,
    log_tx: mpsc::Sender<StoredLog>,
) -> Result<()> {
    stream
        .set_nodelay(true)
        .wrap_err("failed to set TCP_NODELAY")?;

    let reader = BufReader::new(stream);

    // This must live for the whole connection because the sender writes one
    // continuous zstd stream.
    let mut decoder = ZstdDecoder::new(reader);

    loop {
        match deserialize_log_async(&mut decoder).await {
            Ok(log) => {
                process_log(addr, log, &log_tx).await?;
            }
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(());
            }
            Err(err) => {
                return Err(err).wrap_err("failed to deserialize log record");
            }
        }
    }
}

async fn process_log(
    sender: SocketAddr,
    log: OwnedLog,
    log_tx: &mpsc::Sender<StoredLog>,
) -> Result<()> {
    log_tx
        .send(StoredLog {
            timestamp_utc_usec: log.timestamp,
            sender,
            producer: log.producer,
            message: log.message,
        })
        .await
        .map_err(|_| eyre!("database writer task has stopped"))?;

    Ok(())
}

fn database_writer_task(
    mut database: rusqlite::Connection,
    mut log_rx: mpsc::Receiver<StoredLog>,
) -> Result<()> {
    let mut batch = Vec::with_capacity(LOG_BATCH_MAX_RECORDS);

    while let Some(log) = log_rx.blocking_recv() {
        batch.push(log);

        while batch.len() < LOG_BATCH_MAX_RECORDS {
            match log_rx.try_recv() {
                Ok(log) => batch.push(log),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            };
        }

        let dropped = database::insert_logs_best_effort(&mut database, &batch);

        if dropped > 0 {
            eprintln!("dropped {dropped} log records during database write");
        }

        batch.clear();
    }

    Ok(())
}

fn print_connection_error(addr: SocketAddr, err: &Report) {
    eprintln!();
    eprintln!("[{addr}] connection failed");

    for (idx, cause) in err.chain().enumerate() {
        if idx == 0 {
            eprintln!("  error: {cause}");
        } else {
            eprintln!("  caused by: {cause}");
        }
    }

    eprintln!();
}
