use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

use clap::Parser;
use color_eyre::{
    Report,
    eyre::{Context, Result, bail},
};
use common::{LogwebReceiver, Message, MessageContent};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::Sender,
};

use crate::database::{DatabaseConnection, log_append_thread};

mod database;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The port that traps have to connect with.
    listening_port: u16,
    /// The filepath where the logs should be stored.
    database_path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    let cli = Cli::parse();

    let mut database = DatabaseConnection::new(cli.database_path)?;
    database.create_tables()?;

    let (log_append_tx, log_append_rx) = tokio::sync::mpsc::channel(8192);

    let _log_append_thread = std::thread::spawn(move || {
        if let Err(err) = log_append_thread(database, log_append_rx) {
            eprintln!("log append thread failed: {err}");
        }
    });

    let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), cli.listening_port);

    let listener = TcpListener::bind(listen_addr)
        .await
        .wrap_err_with(|| format!("failed to bind listener on {listen_addr}"))?;

    println!("Listening on {listen_addr}");

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                let log_append_tx = log_append_tx.clone();

                tokio::spawn(async move {
                    handle_trap_connection(stream, addr, log_append_tx).await;
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
    log_append_tx: Sender<Message>,
) {
    match process_trap(stream, addr, log_append_tx).await {
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
    log_append_tx: Sender<Message>,
) -> Result<()> {
    let mut stream = LogwebReceiver::new(stream);

    let first_msg = stream
        .recv()
        .await
        .wrap_err("failed to receive initial trap message")?;

    match &first_msg.content {
        MessageContent::TrapInit { .. } => {
            println!("[{addr}] trap initialized");

            log_append_tx
                .send(first_msg)
                .await
                .wrap_err("failed to queue initial trap message for database write")?;
        }
        other => {
            bail!(
                "expected TrapInit as first message, received {}",
                message_kind(other)
            );
        }
    }

    loop {
        let msg = stream
            .recv()
            .await
            .wrap_err("failed to receive trap message")?;

        match &msg.content {
            MessageContent::TrapInit { .. } => {
                bail!("received TrapInit more than once");
            }
            MessageContent::TrapDown { .. } => {
                println!("[{addr}] trap down");

                log_append_tx
                    .send(msg)
                    .await
                    .wrap_err("failed to queue TrapDown message for database write")?;

                break;
            }
            MessageContent::Logs(logs) => {
                println!("[{addr}] received logs: {}", logs.len());

                log_append_tx
                    .send(msg)
                    .await
                    .wrap_err("failed to queue logs for database write")?;
            }
            MessageContent::Truncated => {
                println!("[{addr}] logs truncated");

                log_append_tx
                    .send(msg)
                    .await
                    .wrap_err("failed to queue Truncated message for database write")?;
            }
        }
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

fn message_kind(content: &MessageContent) -> &'static str {
    match content {
        MessageContent::TrapInit { .. } => "TrapInit",
        MessageContent::TrapDown { .. } => "TrapDown",
        MessageContent::Logs(_) => "Logs",
        MessageContent::Truncated => "Truncated",
    }
}
