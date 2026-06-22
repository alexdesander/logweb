use std::path::Path;

use color_eyre::eyre::Result;
use common::{Log, Message, MessageContent};
use rusqlite::{Connection, params};
use tokio::sync::mpsc::Receiver;

const META_LEVEL: i64 = 7;

pub struct DatabaseConnection {
    connection: Connection,
}

impl DatabaseConnection {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self {
            connection: Connection::open(path)?,
        })
    }

    pub fn create_tables(&mut self) -> Result<()> {
        self.connection.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;
            "#,
        )?;

        let tx = self.connection.transaction()?;

        tx.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS Level (
                id   INTEGER PRIMARY KEY,
                text TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS Producer (
                id   INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS Log (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                producer   INTEGER NOT NULL,
                occurrence INTEGER NOT NULL,
                level      INTEGER NOT NULL,
                content    TEXT NOT NULL,

                FOREIGN KEY (producer) REFERENCES Producer(id),
                FOREIGN KEY (level) REFERENCES Level(id)
            );
            "#,
        )?;

        tx.execute(
            r#"
            INSERT OR IGNORE INTO Level (id, text)
            VALUES
                (0, 'UNKNOWN'),
                (1, 'TRACE'),
                (2, 'DEBUG'),
                (3, 'INFO'),
                (4, 'WARN'),
                (5, 'ERROR'),
                (6, 'FATAL'),
                (7, 'META')
            "#,
            [],
        )?;

        tx.commit()?;

        Ok(())
    }

    fn get_or_create_producer_id(tx: &rusqlite::Transaction<'_>, producer: &str) -> Result<i64> {
        tx.execute(
            r#"
            INSERT OR IGNORE INTO Producer (name)
            VALUES (?1)
            "#,
            params![producer],
        )?;

        let producer_id: i64 = tx.query_row(
            r#"
            SELECT id
            FROM Producer
            WHERE name = ?1
            "#,
            params![producer],
            |row| row.get(0),
        )?;

        Ok(producer_id)
    }

    pub fn write_logs(&mut self, producer: &str, logs: &[Log]) -> Result<()> {
        let tx = self.connection.transaction()?;
        let producer_id = Self::get_or_create_producer_id(&tx, producer)?;

        for log in logs {
            tx.execute(
                r#"
                INSERT INTO Log (
                    producer,
                    occurrence,
                    level,
                    content
                )
                VALUES (?1, ?2, ?3, ?4)
                "#,
                params![
                    producer_id,
                    log.occurrence as i64,
                    log.level as i64,
                    &log.content,
                ],
            )?;
        }

        tx.commit()?;

        Ok(())
    }

    pub fn write_meta_log(&mut self, producer: &str, occurrence: u64, content: &str) -> Result<()> {
        let tx = self.connection.transaction()?;
        let producer_id = Self::get_or_create_producer_id(&tx, producer)?;

        tx.execute(
            r#"
            INSERT INTO Log (
                producer,
                occurrence,
                level,
                content
            )
            VALUES (?1, ?2, ?3, ?4)
            "#,
            params![producer_id, occurrence as i64, META_LEVEL, content,],
        )?;

        tx.commit()?;

        Ok(())
    }
}

/// This is run in its own dedicated thread and only appends
/// logs to the database.
pub fn log_append_thread(mut db: DatabaseConnection, mut rx: Receiver<Message>) -> Result<()> {
    while let Some(msg) = rx.blocking_recv() {
        match msg.content {
            MessageContent::Logs(logs) => {
                db.write_logs(&msg.producer, &logs)?;
            }
            MessageContent::TrapInit { occurrence } => {
                db.write_meta_log(&msg.producer, occurrence, "trap initialized")?;
            }
            MessageContent::TrapDown { occurrence } => {
                db.write_meta_log(&msg.producer, occurrence, "trap down")?;
            }
            MessageContent::Truncated => {
                db.write_meta_log(&msg.producer, 0, "logs truncated")?;
            }
        }
    }

    Ok(())
}
