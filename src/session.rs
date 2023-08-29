use std::{
    io::{Read, Write},
    net::TcpStream,
};

pub struct Session {
    stream: TcpStream,
    connect_time: u64,
    last_seen: u64,
    keep_alive: u64,
}

impl Session {
    pub fn new(stream: TcpStream, keep_alive: u64) -> Self {
        let connect_time = now();
        Self {
            stream,
            connect_time,
            last_seen: connect_time,
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
