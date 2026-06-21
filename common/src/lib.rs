use std::{io, net::TcpStream as StdTcpStream};

use async_compression::{futures::write::Lz4Encoder as SyncLz4Encoder, tokio::bufread::Lz4Decoder};
use bitcode::{Decode, Encode};
use futures::{
    executor::block_on,
    io::{AllowStdIo, AsyncWriteExt},
};
use tokio::{
    io::{AsyncReadExt, BufReader},
    net::TcpStream as TokioTcpStream,
};

#[derive(Clone, Copy, Encode, Decode)]
pub enum LogLevel {
    Unknown,
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
}

#[derive(Clone, Encode, Decode)]
pub struct Log {
    /// unix timestamp
    pub occurance: u64,
    pub level: LogLevel,
    pub content: String,
}

#[derive(Clone, Encode, Decode)]
pub enum MessageContent {
    TrapInit { occurance: u64 },
    TrapDown { occurance: u64 },
    Logs(Vec<Log>),
    Truncated,
}

#[derive(Clone, Encode, Decode)]
pub struct Message {
    pub producer: String,
    pub content: MessageContent,
}

pub struct LogwebSender {
    stream: SyncLz4Encoder<AllowStdIo<StdTcpStream>>,
    buffer: bitcode::Buffer,
    finished: bool,
}

impl LogwebSender {
    pub fn new(stream: StdTcpStream) -> Self {
        let mut s = Self {
            stream: SyncLz4Encoder::new(AllowStdIo::new(stream)),
            buffer: bitcode::Buffer::new(),
            finished: false,
        };
        let _ = s.send(&Message {
            producer: "".into(),
            content: MessageContent::TrapInit { occurance: 0 },
        });
        let _ = s.flush();
        s
    }

    pub fn send(&mut self, msg: &Message) -> io::Result<()> {
        if self.finished {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "sender has already been finished",
            ));
        }

        let encoded = self.buffer.encode(msg);

        let len = u32::try_from(encoded.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "encoded message is larger than u32::MAX bytes",
            )
        })?;

        block_on(async {
            self.stream.write_all(&len.to_be_bytes()).await?;
            self.stream.write_all(encoded).await?;
            Ok::<_, io::Error>(())
        })
    }

    pub fn flush(&mut self) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }

        block_on(self.stream.flush())
    }

    /// Finishes the LZ4 frame.
    ///
    /// Call this when no more messages will be sent so compression-finalization
    /// errors can be observed by the caller.
    pub fn finish(&mut self) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }

        block_on(self.stream.close())?;
        self.finished = true;

        Ok(())
    }
}

impl Drop for LogwebSender {
    fn drop(&mut self) {
        let _ = self.send(&Message {
            producer: "".into(),
            content: MessageContent::TrapDown { occurance: 0 },
        });
        let _ = self.flush();
        let _ = self.finish();
    }
}

pub struct LogwebReceiver {
    stream: Lz4Decoder<BufReader<TokioTcpStream>>,
    buffer: bitcode::Buffer,
    buffer2: Vec<u8>,
}

impl LogwebReceiver {
    pub fn new(stream: TokioTcpStream) -> Self {
        Self {
            stream: Lz4Decoder::new(BufReader::new(stream)),
            buffer: bitcode::Buffer::new(),
            buffer2: Vec::new(),
        }
    }

    pub async fn recv(&mut self) -> io::Result<Message> {
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf).await?;

        let len = u32::from_be_bytes(len_buf) as usize;

        if self.buffer2.len() < len {
            self.buffer2.resize(len, 0);
        }

        self.stream.read_exact(&mut self.buffer2[..len]).await?;

        self.buffer
            .decode(&self.buffer2[..len])
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))
    }
}
