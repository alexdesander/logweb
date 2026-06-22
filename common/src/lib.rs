use std::io::{self, Read, Write};

use tokio::io::{AsyncRead, AsyncReadExt};

pub const MAX_PRODUCER_SIZE: usize = 255;
pub const MAX_MESSAGE_SIZE: usize = 65535;
pub const LOG_HEADER_SIZE: usize = 8 + 1 + 2;

pub fn serialized_log_len(producer: &[u8], message: &[u8]) -> usize {
    LOG_HEADER_SIZE + producer.len().min(MAX_PRODUCER_SIZE) + message.len().min(MAX_MESSAGE_SIZE)
}

pub fn serialize_log<W: Write>(
    mut dst: W,
    timestamp: u64,
    producer: &[u8],
    message: &[u8],
) -> io::Result<usize> {
    let producer_len: u8 = producer.len().min(MAX_PRODUCER_SIZE) as u8;
    let producer = &producer[..producer_len as usize];

    let message_len: u16 = message.len().min(MAX_MESSAGE_SIZE) as u16;
    let message = &message[..message_len as usize];

    dst.write_all(&timestamp.to_le_bytes())?;
    dst.write_all(&[producer_len])?;
    dst.write_all(producer)?;
    dst.write_all(&message_len.to_le_bytes())?;
    dst.write_all(message)?;

    Ok(serialized_log_len(producer, message))
}

pub struct OwnedLog {
    pub timestamp: u64,
    pub producer: String,
    pub message: String,
}

pub fn deserialize_log<R: Read>(reader: &mut R) -> io::Result<OwnedLog> {
    let mut timestamp_buf = [0u8; 8];
    reader.read_exact(&mut timestamp_buf)?;
    let timestamp = u64::from_le_bytes(timestamp_buf);

    let mut producer_len_buf = [0u8; 1];
    reader.read_exact(&mut producer_len_buf)?;
    let producer_len = producer_len_buf[0] as usize;

    let mut producer = vec![0u8; producer_len];
    reader.read_exact(&mut producer)?;

    let mut message_len_buf = [0u8; 2];
    reader.read_exact(&mut message_len_buf)?;
    let message_len = u16::from_le_bytes(message_len_buf) as usize;

    let mut message = vec![0u8; message_len];
    reader.read_exact(&mut message)?;

    let producer = String::from_utf8_lossy(&producer).into_owned();
    let message = String::from_utf8_lossy(&message).into_owned();

    Ok(OwnedLog {
        timestamp,
        producer,
        message,
    })
}

pub async fn deserialize_log_async<R>(reader: &mut R) -> io::Result<OwnedLog>
where
    R: AsyncRead + Unpin,
{
    let mut timestamp_buf = [0u8; 8];
    reader.read_exact(&mut timestamp_buf).await?;
    let timestamp = u64::from_le_bytes(timestamp_buf);

    let mut producer_len_buf = [0u8; 1];
    reader.read_exact(&mut producer_len_buf).await?;
    let producer_len = producer_len_buf[0] as usize;

    let mut producer = vec![0u8; producer_len];
    reader.read_exact(&mut producer).await?;

    let mut message_len_buf = [0u8; 2];
    reader.read_exact(&mut message_len_buf).await?;
    let message_len = u16::from_le_bytes(message_len_buf) as usize;

    let mut message = vec![0u8; message_len];
    reader.read_exact(&mut message).await?;

    let producer = String::from_utf8_lossy(&producer).into_owned();
    let message = String::from_utf8_lossy(&message).into_owned();

    Ok(OwnedLog {
        timestamp,
        producer,
        message,
    })
}
