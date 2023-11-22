use std::io::{BufReader, Read, Write};

use log::debug;

use crate::stream::TcpStream;

#[derive(Debug)]
pub struct SessionReader {
    stream: BufReader<TcpStream>,
    last_seen: u64,
    keep_alive: u64,
}

impl SessionReader {
    pub fn update_last_seen(&mut self) {
        self.last_seen = now();
    }

    pub fn is_alive(&self) -> bool {
        let now = now();
        now - self.last_seen < self.keep_alive
    }
}

impl Read for SessionReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.stream.read(buf)
    }
}

#[derive(Debug)]
pub struct SessionWriter {
    stream: TcpStream,
}

impl Clone for SessionWriter {
    fn clone(&self) -> Self {
        Self {
            stream: self.stream.clone(),
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

#[derive(Debug)]
pub struct Session {
    stream: TcpStream,
    last_seen: u64,
    keep_alive: u64,
}

/// `Clone` for `Session` is implemented by cloning the underlying `TcpStream`.
/// See https://doc.rust-lang.org/stable/std/net/struct.TcpStream.html#method.try_clone
impl Clone for Session {
    fn clone(&self) -> Self {
        Self {
            stream: self.stream.clone(),
            last_seen: self.last_seen,
            keep_alive: self.keep_alive,
        }
    }
}

impl Session {
    pub fn new(stream: TcpStream, keep_alive: u64) -> Self {
        Self {
            stream,
            last_seen: now(),
            keep_alive,
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
        };
        let writer = SessionWriter { stream: writer };
        (reader, writer)
    }
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time")
        .as_secs()
}
