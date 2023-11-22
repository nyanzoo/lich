
use std::{
    cell::{RefCell, RefMut},
    cmp,
    collections::HashMap,
    io::{Read, Write},
    net::ToSocketAddrs,
    rc::Rc,
    sync::Arc,
    time::Duration,
};

use necronomicon::{Encode, Packet};
use parking_lot::Mutex;

lazy_static::lazy_static! {
    static ref CONNECTIONS: Connections = Connections::new();
    static ref ACCEPTS: Accepts = Accepts::default();
}

pub fn preload_accepts(addrs: &[&str]) {
    for addr in addrs {
        ACCEPTS.push(addr.to_string());
    }
}

/// By default, all addresses are accepted.
/// This function allows you to gaurantee that an address will be rejected with the given error.
pub fn gaurantee_address_rejection<S>(addr: S, err: std::io::ErrorKind)
where
    S: ToString,
{
    CONNECTIONS.insert(addr.to_string(), Connection::Reject(err))
}

/// Retrieves the connection for the given address.
/// If the address has not been accepted, it will return `None`.
pub fn get_connection<S>(addr: S) -> Option<TcpStream>
where
    S: ToString,
{
    CONNECTIONS
        .get(addr.to_string())
        .map(|connection| match connection {
            Connection::Accept(stream) => stream.clone(),
            Connection::Reject(err) => panic!("address rejected with error: {:?}", err),
        })
}

#[derive(Default)]
pub struct Accepts {
    inner: Arc<Mutex<Vec<String>>>,
}

impl Accepts {
    pub fn new() -> Self {
        Self {
            inner: Arc::default(),
        }
    }

    pub fn push(&self, addr: String) {
        let mut inner = self.inner.lock();
        inner.push(addr);
    }

    pub fn pop(&self) -> Option<String> {
        let mut inner = self.inner.lock();
        inner.pop()
    }
}

#[derive(Default)]
pub struct Connections {
    inner: Arc<Mutex<InnerConnections>>,
}

impl Connections {
    pub fn new() -> Self {
        Self {
            inner: Arc::default(),
        }
    }

    pub fn get(&self, addr: String) -> Option<Connection> {
        let inner = self.inner.lock();
        inner.connections.get(&addr).cloned()
    }

    pub fn insert(&self, addr: String, connection: Connection) {
        let mut inner = self.inner.lock();
        inner.connections.insert(addr, connection);
    }

    pub fn remove(&self, addr: String) {
        let mut inner = self.inner.lock();
        inner.connections.remove(&addr);
    }
}

#[derive(Clone)]
pub enum Connection {
    Accept(TcpStream),
    Reject(std::io::ErrorKind),
}

#[derive(Default)]
struct InnerConnections {
    connections: HashMap<String, Connection>,
}

#[derive(Clone, Debug)]
pub struct TcpStream {
    inner: Arc<Mutex<TcpStreamInner>>,
}

#[derive(Debug, Default)]
struct TcpStreamInner {
    addr: String,

    read: Vec<u8>,
    expected_read: Vec<u8>,

    write: Vec<u8>,
    expected_write: Vec<u8>,
}

impl TcpStreamInner {
    fn new(addr: String) -> Self {
        Self {
            addr,
            read: Vec::new(),
            expected_read: Vec::new(),
            write: Vec::new(),
            expected_write: Vec::new(),
        }
    }

    fn verify_reads(&self, expected: &[u8]) {
        assert_eq!(expected, self.expected_read);
    }

    fn fill_read(&mut self, reads: impl Encode<Vec<u8>>) {
        reads.encode(&mut self.read).expect("encode");
    }

    fn verify_writes(&mut self, expected: &[Packet]) {
        let mut expected_v = Vec::new();
        for packet in expected {
            packet.encode(&mut expected_v).expect("encode");
        }
        assert_eq!(
            expected_v,
            self.expected_write.drain(..).collect::<Vec<_>>()
        );
    }

    fn fill_write(&mut self, writes: &[u8]) {
        self.write.extend_from_slice(writes);
    }
}

impl Drop for TcpStreamInner {
    fn drop(&mut self) {
        CONNECTIONS.remove(self.addr.clone());
    }
}

impl TcpStream {
    pub fn connect<A>(addr: A) -> std::io::Result<Self>
    where
        A: ToSocketAddrs,
    {
        let addr = addr.to_socket_addrs()?.next().expect("next").to_string();
        if let Some(stream) = CONNECTIONS.get(addr.clone()) {
            match stream {
                Connection::Accept(stream) => return Ok(stream.clone()),
                Connection::Reject(err) => return Err(std::io::Error::from(err)),
            }
        }

        let stream = Self {
            inner: Arc::new(Mutex::new(TcpStreamInner::new(addr.clone()))),
        };

        CONNECTIONS.insert(addr, Connection::Accept(stream.clone()));
        Ok(stream)
    }

    pub fn retryable_connect<A, P>(addr: A, mut policy: P) -> std::io::Result<Self>
    where
        A: ToSocketAddrs + Clone + std::fmt::Debug,
        P: super::RetryPolicy,
    {
        loop {
            match Self::connect(addr.clone()) {
                Ok(stream) => return Ok(stream),
                Err(err) => {
                    if policy.retry() {
                        log::warn!("retrying connection to {:?}: {err:?}", addr);
                    } else {
                        return Err(err);
                    }
                }
            }
        }
    }

    pub fn addr(&self) -> String {
        self.inner.lock().addr.clone()
    }

    pub fn peer_addr(&self) -> String {
        unimplemented!("peer_addr")
    }

    pub fn set_nonblocking(&self, _nonblocking: bool) -> std::io::Result<()> {
        Ok(())
    }

    pub fn verify_reads(&self, expected: &[u8]) {
        self.inner.lock().verify_reads(expected);
    }

    pub fn fill_read(&self, reads: impl Encode<Vec<u8>>) {
        self.inner.lock().fill_read(reads);
    }

    pub fn verify_writes(&self, expected: &[Packet]) {
        self.inner.lock().verify_writes(expected);
    }

    pub fn fill_write(&self, writes: &[u8]) {
        self.inner.lock().fill_write(writes);
    }

    pub fn shutdown(&self, _: std::net::Shutdown) -> std::io::Result<()> {
        Ok(())
    }
}

impl Read for TcpStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = loop {
            let mut inner = self.inner.lock();
            let read = &mut inner.read;
            let len = cmp::min(buf.len(), read.len());
            buf[..len].copy_from_slice(&read[..len]);
            read.drain(..len);
            if len > 0 {
                break len;
            };
            std::thread::sleep(Duration::from_millis(100));
        };
        Ok(len)
    }
}

impl Write for TcpStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut inner = self.inner.lock();
        let write = &mut inner.expected_write;
        write.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct SocketAddr(String);

pub struct TcpListener(Rc<RefCell<Vec<TcpStream>>>);

impl TcpListener {
    pub fn bind(_: impl ToSocketAddrs) -> std::io::Result<Self> {
        Ok(Self(Rc::default()))
    }

    pub fn accept(&self) -> std::io::Result<(TcpStream, SocketAddr)> {
        let addr = ACCEPTS.pop().expect("no more test connections");
        let stream = TcpStream::connect(addr.clone())?;
        Ok((stream.clone(), SocketAddr(addr.clone())))
    }

    pub fn incoming(&self) -> Incoming {
        let inner = self.0.borrow_mut();
        Incoming(inner)
    }
}

pub struct Incoming<'a>(RefMut<'a, Vec<TcpStream>>);

impl Iterator for Incoming<'_> {
    type Item = std::io::Result<TcpStream>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.drain(..1).next().map(Ok)
    }
}
