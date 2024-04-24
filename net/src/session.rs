use std::{
    fmt::Debug,
    io::{BufReader, Read, Write},
};

use log::debug;

use crate::stream::TcpStream;

static SESSION_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub struct SessionReader {
    stream: BufReader<TcpStream>,
    last_seen: u64,
    keep_alive: u64,
    id: u64,
}

impl SessionReader {
    pub fn update_last_seen(&mut self) {
        self.last_seen = now();
    }

    pub fn is_alive(&self) -> bool {
        let now = now();
        now - self.last_seen < self.keep_alive
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}

impl Read for SessionReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.stream.read(buf)
    }
}

impl Debug for SessionReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionReader")
            .field("last_seen", &self.last_seen)
            .field("keep_alive", &self.keep_alive)
            .field("id", &self.id)
            .finish()
    }
}

pub struct SessionWriter {
    stream: TcpStream,
    id: u64,
}

impl SessionWriter {
    pub fn id(&self) -> u64 {
        self.id
    }
}

impl Clone for SessionWriter {
    fn clone(&self) -> Self {
        Self {
            stream: self.stream.clone(),
            id: self.id,
        }
    }
}

impl Write for SessionWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.stream.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.stream.flush()
    }
}

impl Debug for SessionWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionWriter")
            .field("id", &self.id)
            .finish()
    }
}

pub struct Session {
    stream: TcpStream,
    last_seen: u64,
    keep_alive: u64,
    id: u64,
}

/// `Clone` for `Session` is implemented by cloning the underlying `TcpStream`.
/// See https://doc.rust-lang.org/stable/std/net/struct.TcpStream.html#method.try_clone
impl Clone for Session {
    fn clone(&self) -> Self {
        Self {
            stream: self.stream.clone(),
            last_seen: self.last_seen,
            keep_alive: self.keep_alive,
            id: self.id,
        }
    }
}

impl Session {
    pub fn new(stream: TcpStream, keep_alive: u64) -> Self {
        Self {
            stream,
            last_seen: now(),
            keep_alive,
            id: SESSION_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        }
    }

    pub fn shutdown(self) -> std::io::Result<()> {
        debug!("shutting down session {:?}", self);
        self.stream.shutdown(std::net::Shutdown::Both)
    }

    pub fn addr(&self) -> String {
        self.stream.addr()
    }

    pub fn peer_addr(&self) -> String {
        self.stream.peer_addr()
    }

    pub fn split(self) -> (SessionReader, SessionWriter) {
        let (reader, writer) = (self.stream.clone(), self.stream.clone());
        let reader = BufReader::new(reader);
        let reader = SessionReader {
            stream: reader,
            last_seen: self.last_seen,
            keep_alive: self.keep_alive,
            id: self.id,
        };
        let writer = SessionWriter {
            stream: writer,
            id: self.id,
        };
        (reader, writer)
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}

impl Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("last_seen", &self.last_seen)
            .field("keep_alive", &self.keep_alive)
            .field("id", &self.id)
            .finish()
    }
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time")
        .as_secs()
}
