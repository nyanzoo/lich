#[cfg(test)]
pub mod mock {
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
}

#[cfg(not(test))]
pub mod real {
    use std::{
        io::{Read, Write},
        net::ToSocketAddrs,
    };

    #[derive(Debug)]
    pub struct TcpStream {
        inner: std::net::TcpStream,
    }

    impl Clone for TcpStream {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.try_clone().expect("try_clone"),
            }
        }
    }

    impl TcpStream {
        pub fn connect<A>(addr: A) -> std::io::Result<Self>
        where
            A: ToSocketAddrs + std::fmt::Debug,
        {
            let inner = std::net::TcpStream::connect(addr)?;
            Ok(Self { inner })
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
            self.inner.local_addr().expect("local_addr").to_string()
        }

        pub fn peer_addr(&self) -> String {
            self.inner.peer_addr().expect("peer_addr").to_string()
        }

        pub fn set_nonblocking(&self, nonblocking: bool) -> std::io::Result<()> {
            self.inner.set_nonblocking(nonblocking)
        }

        pub fn shutdown(&self, how: std::net::Shutdown) -> std::io::Result<()> {
            self.inner.shutdown(how)
        }
    }

    impl Read for TcpStream {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.inner.read(buf)
        }
    }

    impl Write for TcpStream {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.inner.write(buf)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.inner.flush()
        }
    }

    #[derive(Clone, Debug)]
    pub struct SocketAddr(std::net::SocketAddr);

    pub struct TcpListener {
        inner: std::net::TcpListener,
    }

    impl TcpListener {
        pub fn bind(addr: impl ToSocketAddrs) -> std::io::Result<Self> {
            let inner = std::net::TcpListener::bind(addr)?;
            Ok(Self { inner })
        }

        pub fn accept(&self) -> std::io::Result<(TcpStream, SocketAddr)> {
            let (inner, addr) = self.inner.accept()?;
            Ok((TcpStream { inner }, SocketAddr(addr)))
        }

        pub fn incoming(&self) -> Incoming {
            Incoming(self.inner.incoming())
        }
    }

    pub struct Incoming<'a>(std::net::Incoming<'a>);

    impl Iterator for Incoming<'_> {
        type Item = std::io::Result<TcpStream>;

        fn next(&mut self) -> Option<Self::Item> {
            self.0
                .next()
                .map(|inner| inner.map(|inner| TcpStream { inner }))
        }
    }
}

pub trait RetryPolicy {
    fn retry(&mut self) -> bool;
}

pub struct RetryConsistent {
    count: Option<usize>,
    duration: std::time::Duration,
}

impl RetryConsistent {
    pub fn new(duration: std::time::Duration, count: Option<usize>) -> Self {
        Self { count, duration }
    }
}

impl RetryPolicy for RetryConsistent {
    fn retry(&mut self) -> bool {
        std::thread::sleep(self.duration);
        if let Some(count) = self.count.as_mut() {
            if *count == 0 {
                return false;
            }
            *count -= 1;
        }
        return true;
    }
}

pub struct RetryExponential {
    count: Option<usize>,
    duration: std::time::Duration,
    multiplier: f64,
}

impl RetryExponential {
    pub fn new(duration: std::time::Duration, multiplier: f64, count: Option<usize>) -> Self {
        Self {
            count,
            duration,
            multiplier,
        }
    }
}

impl RetryPolicy for RetryExponential {
    fn retry(&mut self) -> bool {
        std::thread::sleep(self.duration);
        if let Some(count) = self.count.as_mut() {
            if *count == 0 {
                return false;
            }
            *count -= 1;
        }
        self.duration =
            std::time::Duration::from_secs_f64(self.duration.as_secs_f64() * self.multiplier);
        return true;
    }
}

#[cfg(test)]
pub use mock::*;

#[cfg(not(test))]
pub use real::*;
