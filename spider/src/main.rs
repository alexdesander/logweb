use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use clap::Parser;
use color_eyre::{
    Report,
    eyre::{Context, Result, bail},
};
use common::{LogwebReceiver, MessageContent};
use tokio::net::{TcpListener, TcpStream};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The port that traps have to connect with.
    listening_port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    let cli = Cli::parse();

    let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), cli.listening_port);

    let listener = TcpListener::bind(listen_addr)
        .await
        .wrap_err_with(|| format!("failed to bind listener on {listen_addr}"))?;

    println!("Listening on {listen_addr}");

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                tokio::spawn(async move {
                    handle_trap_connection(stream, addr).await;
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

async fn handle_trap_connection(stream: TcpStream, addr: SocketAddr) {
    match process_trap(stream, addr).await {
        Ok(()) => {
            println!("[{addr}] connection closed");
        }
        Err(err) => {
            print_connection_error(addr, &err);
        }
    }
}

async fn process_trap(stream: TcpStream, addr: SocketAddr) -> Result<()> {
    let mut stream = LogwebReceiver::new(stream);

    let first_msg = stream
        .recv()
        .await
        .wrap_err("failed to receive initial trap message")?;

    match first_msg.content {
        MessageContent::TrapInit { .. } => {
            println!("[{addr}] trap initialized");
        }
        other => {
            bail!(
                "expected TrapInit as first message, received {}",
                message_kind(&other)
            );
        }
    }

    loop {
        let msg = stream
            .recv()
            .await
            .wrap_err("failed to receive trap message")?;

        match msg.content {
            MessageContent::TrapInit { .. } => {
                bail!("received TrapInit more than once");
            }
            MessageContent::TrapDown { .. } => {
                println!("[{addr}] trap down");
                break;
            }
            MessageContent::Logs(logs) => {
                println!("[{addr}] received logs: {}", logs.len());
            }
            MessageContent::Truncated => {
                println!("[{addr}] logs truncated");
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
