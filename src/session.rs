use std::{
    io::{Read, Write},
    net::TcpStream,
};

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
            stream: self.stream.try_clone().expect("clone"),
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

    pub fn is_alive(&self) -> bool {
        let now = now();
        now - self.last_seen < self.keep_alive
    }

    pub fn update_last_seen(&mut self) {
        self.last_seen = now();
    }
}

impl Write for Session {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.stream.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.stream.flush()
    }
}

impl Read for Session {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.stream.read(buf)
    }
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time")
        .as_secs()
}
