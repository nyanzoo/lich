use std::{
    fmt::Debug,
    io::{Read, Result, Write},
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
    pub fn connect<A>(addr: A) -> Result<Self>
    where
        A: ToSocketAddrs + Debug,
    {
        let inner = std::net::TcpStream::connect(addr)?;
        Ok(Self { inner })
    }

    pub fn retryable_connect<A, P>(addr: A, mut policy: P) -> Result<Self>
    where
        A: ToSocketAddrs + Clone + Debug,
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

    pub fn set_nonblocking(&self, nonblocking: bool) -> Result<()> {
        self.inner.set_nonblocking(nonblocking)
    }

    pub fn shutdown(&self, how: std::net::Shutdown) -> Result<()> {
        self.inner.shutdown(how)
    }
}

impl Read for TcpStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.inner.read(buf)
    }
}

impl Write for TcpStream {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }
}

#[derive(Clone, Debug)]
pub struct SocketAddr(std::net::SocketAddr);

pub struct TcpListener {
    inner: std::net::TcpListener,
}

impl TcpListener {
    pub fn bind(addr: impl ToSocketAddrs) -> Result<Self> {
        let inner = std::net::TcpListener::bind(addr)?;
        Ok(Self { inner })
    }

    pub fn accept(&self) -> Result<(TcpStream, SocketAddr)> {
        let (inner, addr) = self.inner.accept()?;
        Ok((TcpStream { inner }, SocketAddr(addr)))
    }

    pub fn incoming(&self) -> Incoming {
        Incoming(self.inner.incoming())
    }
}

pub struct Incoming<'a>(std::net::Incoming<'a>);

impl Iterator for Incoming<'_> {
    type Item = Result<TcpStream>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|inner| inner.map(|inner| TcpStream { inner }))
    }
}
