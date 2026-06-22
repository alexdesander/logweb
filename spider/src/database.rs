use std::{net::SocketAddr, path::Path};

use color_eyre::{Result, eyre::Context};
use rusqlite::{Connection, params};
use rustc_hash::FxHashMap;

#[derive(Debug, Clone)]
pub struct StoredLog {
    pub timestamp_utc_usec: u64,
    pub sender: SocketAddr,
    pub producer: String,
    pub message: String,
}

fn create_tables(con: &Connection) -> Result<()> {
    con.execute_batch(
        r#"
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS producers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS senders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            addr TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp_utc_usec INTEGER NOT NULL,
            sender_id INTEGER NOT NULL,
            producer_id INTEGER NOT NULL,
            message TEXT NOT NULL,

            FOREIGN KEY (sender_id)
                REFERENCES senders(id)
                ON DELETE RESTRICT,

            FOREIGN KEY (producer_id)
                REFERENCES producers(id)
                ON DELETE RESTRICT
        );

        CREATE INDEX IF NOT EXISTS idx_messages_sender_id
            ON messages(sender_id);

        CREATE INDEX IF NOT EXISTS idx_messages_producer_id
            ON messages(producer_id);
        "#,
    )?;

    Ok(())
}

pub fn connect(path: impl AsRef<Path>) -> Result<Connection> {
    let con = Connection::open(path).wrap_err("failed to open SQLite database")?;
    create_tables(&con).wrap_err("failed to initialize SQLite database")?;
    Ok(con)
}

pub fn insert_logs(con: &mut Connection, logs: &[StoredLog]) -> Result<()> {
    let tx = con.transaction()?;

    {
        let mut sender_cache: FxHashMap<SocketAddr, i64> = FxHashMap::default();
        let mut producer_cache: FxHashMap<&str, i64> = FxHashMap::default();

        let mut insert_sender = tx.prepare("INSERT OR IGNORE INTO senders (addr) VALUES (?1)")?;

        let mut select_sender = tx.prepare("SELECT id FROM senders WHERE addr = ?1")?;

        let mut insert_producer =
            tx.prepare("INSERT OR IGNORE INTO producers (name) VALUES (?1)")?;

        let mut select_producer = tx.prepare("SELECT id FROM producers WHERE name = ?1")?;

        let mut insert_message = tx.prepare(
            r#"
            INSERT INTO messages (
                timestamp_utc_usec,
                sender_id,
                producer_id,
                message
            )
            VALUES (?1, ?2, ?3, ?4)
            "#,
        )?;

        for log in logs {
            let sender_id = match sender_cache.get(&log.sender) {
                Some(id) => *id,
                None => {
                    let sender = log.sender.to_string();

                    insert_sender.execute(params![&sender])?;

                    let id: i64 = select_sender.query_row(params![&sender], |row| row.get(0))?;

                    sender_cache.insert(log.sender, id);
                    id
                }
            };

            let producer = log.producer.as_str();

            let producer_id = match producer_cache.get(producer) {
                Some(id) => *id,
                None => {
                    insert_producer.execute(params![producer])?;

                    let id: i64 = select_producer.query_row(params![producer], |row| row.get(0))?;

                    producer_cache.insert(producer, id);
                    id
                }
            };

            let timestamp_utc_usec: i64 = log
                .timestamp_utc_usec
                .try_into()
                .wrap_err("timestamp_utc_usec does not fit into SQLite INTEGER")?;

            insert_message.execute(params![
                timestamp_utc_usec,
                sender_id,
                producer_id,
                &log.message,
            ])?;
        }
    }

    tx.commit()?;
    Ok(())
}

pub fn insert_logs_best_effort(con: &mut Connection, logs: &[StoredLog]) -> usize {
    if let Err(err) = insert_logs(con, logs) {
        eprintln!("database batch insert failed; retrying records individually: {err:?}");

        let mut dropped = 0;

        for log in logs {
            if let Err(err) = insert_logs(con, std::slice::from_ref(log)) {
                dropped += 1;
                eprintln!(
                    "dropping log record after database insert failure: sender={} producer={:?} timestamp_utc_usec={} error={err:?}",
                    log.sender, log.producer, log.timestamp_utc_usec
                );
            }
        }

        dropped
    } else {
        0
    }
}
