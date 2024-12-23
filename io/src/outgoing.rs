use std::{
    fmt::{Debug, Formatter},
    io::BufReader,
    io::Write,
    net::ToSocketAddrs,
};

use crossbeam::channel::{Receiver, Sender, TryRecvError};
use log::{debug, error, info, trace};
use requests::ClientResponse;

use necronomicon::{full_decode, Encode, Packet, Pool, PoolImpl, SharedImpl};
use net::stream::TcpStream;

use crate::{error::Error, BufferOwner};

// Needs to be created by receiving an update from operator, which means `state.rs`
// needs to be able to create `Outgoing` and start it and kill it.
pub struct Outgoing {
    _read: std::thread::JoinHandle<()>,
    _write: std::thread::JoinHandle<()>,

    stream: TcpStream,
    pub addr: String,
}

impl Debug for Outgoing {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Outgoing")
            .field("addr", &self.addr)
            .finish()
    }
}

impl Drop for Outgoing {
    fn drop(&mut self) {
        info!("dropping outgoing");
        if let Err(err) = self.stream.shutdown(std::net::Shutdown::Both) {
            error!("failed to shutdown outgoing: {err}");
        }
    }
}

impl Outgoing {
    pub fn new(
        addr: impl ToSocketAddrs,
        requests_rx: Receiver<Packet<SharedImpl>>,
        ack_tx: Sender<ClientResponse<SharedImpl>>,
        pool: PoolImpl,
    ) -> Result<Self, Error> {
        const MAX_RETRY: usize = 5;
        const MIN_SLEEP_MS: u64 = 100;
        const MAX_SLEEP_MS: u64 = 2000;

        let mut sleep = MIN_SLEEP_MS;
        let mut retry = 0;

        let addr = loop {
            match addr.to_socket_addrs() {
                Ok(mut addr) => break addr.next().expect("no addr found").to_string(),
                Err(err) => {
                    sleep = std::cmp::min(sleep * 2, MAX_SLEEP_MS);
                    if sleep == MAX_SLEEP_MS {
                        retry += 1;
                    }
                    if retry == MAX_RETRY {
                        return Err(Error::Connection);
                    }
                    debug!("failed to connect to outgoing: {err}, retries left {retry}");
                    std::thread::sleep(std::time::Duration::from_millis(sleep));
                }
            }
        };

        let stream = loop {
            match TcpStream::connect(addr.clone()) {
                Ok(stream) => {
                    break stream;
                }
                Err(err) => {
                    sleep = std::cmp::min(sleep * 2, MAX_SLEEP_MS);
                    if sleep == MAX_SLEEP_MS {
                        retry += 1;
                    }
                    if retry == MAX_RETRY {
                        return Err(Error::Connection);
                    }
                    debug!("failed to connect to outgoing: {err}, retries left {retry}");
                    std::thread::sleep(std::time::Duration::from_millis(sleep));
                }
            }
        };
        info!("starting outgoing to {addr}");
        let (mut read, mut write) = (BufReader::new(stream.clone()), stream.clone());

        let (kill_tx, kill_rx) = crossbeam::channel::unbounded();
        let write_handle = std::thread::Builder::new()
            .name(format!("outgoing-write {addr}"))
            .spawn({
                let addr = addr.clone();
                move || {
                    let mut has_written = false;
                    loop {
                        match requests_rx.try_recv() {
                            Ok(packet) => {
                                trace!("sending packet {:?} to {addr}", packet);

                                if let Err(err) = packet.encode(&mut write) {
                                    trace!("failed to encode packet {packet:?} due to {err}, flushing write");
                                    break;
                                }
                                has_written = true;
                            }
                            Err(TryRecvError::Empty) => {
                                if has_written {
                                    has_written = false;
                                    trace!("flushing write");
                                    if let Err(err) = write.flush() {
                                        trace!("failed to flush write due to {err}");
                                        break;
                                    }
                                }
                                std::thread::sleep(std::time::Duration::from_millis(10));
                            }
                            Err(TryRecvError::Disconnected) => {
                                trace!("requests_rx.recv: disconnected");
                                break;
                            }
                        }
                    }

                    trace!("flushing write");
                    if let Err(err) = write.flush() {
                        trace!("failed to flush write due to {err}");
                    }
                    kill_tx.send(()).expect("kill_tx.send");
                }
            })
            .expect("write thread spawn failed");

        let read_handle = std::thread::Builder::new()
            .name(format!("outgoing-read {addr}"))
            .spawn({
                let addr = addr.clone();
                move || loop {
                    match kill_rx.try_recv() {
                        Ok(_) => {
                            trace!("kill_rx.try_recv: received kill signal");
                            break;
                        }
                        Err(TryRecvError::Empty) => {}
                        Err(TryRecvError::Disconnected) => {
                            trace!("kill_rx.try_recv: closed");
                            break;
                        }
                    }

                    let mut previous_decoded_header = None;
                    'pool: loop {
                        let mut buffer = pool.acquire("outgoing decode", BufferOwner::FullDecode);
                        'decode: loop {
                            match full_decode(
                                &mut read,
                                &mut buffer,
                                previous_decoded_header.take(),
                            ) {
                                Ok(packet) => {
                                    trace!("received packet {:?} on {addr}", packet);
                                    let response = ClientResponse::from(packet);
                                    if let Err(err) = ack_tx.send(response) {
                                        trace!("failed to send ack due to {err}");
                                        break 'pool;
                                    }
                                }
                                Err(necronomicon::Error::BufferTooSmallForPacketDecode {
                                    header,
                                    ..
                                }) => {
                                    let _ = previous_decoded_header.insert(header);
                                    break 'decode;
                                }
                                Err(err) => {
                                    trace!("reader err: {err}");
                                    break 'pool;
                                }
                            }
                        }
                    }
                }
            })
            .expect("read thread spawn failed");

        Ok(Self {
            _read: read_handle,
            _write: write_handle,

            stream,
            addr,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use matches::assert_matches;

    use necronomicon::{binary_data, kv_store_codec::Put, Packet, PoolImpl, StorePacket};
    use net::stream::{gaurantee_address_rejection, get_connection};
    use requests::ClientResponse;

    use crate::error::Error;

    use super::Outgoing;

    #[test]
    fn errs_on_fail_to_connect() {
        const ADDRESS: &str = "127.0.0.1:9999";

        let (_, requests_rx) = crossbeam::channel::unbounded();
        let (ack_tx, _) = crossbeam::channel::unbounded();
        gaurantee_address_rejection(ADDRESS, ErrorKind::ConnectionRefused);
        let pool = PoolImpl::new(1, 1024);
        let outgoing = Outgoing::new(ADDRESS, requests_rx, ack_tx, pool);
        assert_matches!(outgoing, Err(Error::Connection));
    }

    #[test]
    fn reads_and_writes() {
        const ADDRESS: &str = "127.0.0.1:9998";

        let (requests_tx, requests_rx) = crossbeam::channel::unbounded();
        let (ack_tx, ack_rx) = crossbeam::channel::unbounded();

        let pool = PoolImpl::new(1, 1024);
        let outgoing = Outgoing::new(ADDRESS, requests_rx, ack_tx, pool);
        assert_matches!(outgoing, Ok(_));

        let put = Put::new(1, 0, binary_data(b"key"), binary_data(b"value"));

        requests_tx
            .send(Packet::Store(StorePacket::Put(put.clone())))
            .expect("send ping");

        // give time for the outgoing to receive the put.
        std::thread::sleep(std::time::Duration::from_secs(1));

        let stream = get_connection(ADDRESS).expect("stream");

        stream.verify_writes(&[Packet::Store(StorePacket::Put(put.clone()))]);
        let put_ack = put.ack();
        stream.fill_read(put_ack.clone());

        let ClientResponse::Put(ack) = ack_rx.recv().expect("ack") else {
            panic!("expected ping ack");
        };

        assert_eq!(ack, put_ack);
    }
}
