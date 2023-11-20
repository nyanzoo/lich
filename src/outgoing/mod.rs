use std::{
    fmt::{Debug, Formatter},
    io::BufReader,
    io::Write,
    net::ToSocketAddrs,
};

use crossbeam::channel::{Receiver, Sender};
use log::{info, trace};

use necronomicon::{full_decode, Encode, Packet};

use crate::{
    common::{reqres::ClientResponse, stream::TcpStream},
    error::Error,
};

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
        self.stream
            .shutdown(std::net::Shutdown::Both)
            .expect("shutdown");
    }
}

impl Outgoing {
    pub(super) fn new(
        addr: impl ToSocketAddrs,
        requests_rx: Receiver<Packet>,
        ack_tx: Sender<ClientResponse>,
    ) -> Result<Self, Error> {
        let addr = addr
            .to_socket_addrs()?
            .next()
            .expect("no addr found")
            .to_string();

        let mut retry = 50;
        let stream = loop {
            match TcpStream::connect(addr.clone()) {
                Ok(stream) => {
                    break stream;
                }
                Err(err) => {
                    retry -= 1;
                    if retry == 0 {
                        return Err(Error::Connection);
                    }
                    info!("failed to connect to outgoing: {err}, retries left {retry}");
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
        };
        info!("starting outgoing to {addr}");
        let (mut read, mut write) = (BufReader::new(stream.clone()), stream.clone());

        let read_handle = std::thread::spawn({
            let addr = addr.clone();
            move || loop {
                let packet = requests_rx.recv().expect("must be able to receive packet");

                trace!("sending packet {:?} to {addr}", packet);
                packet.encode(&mut write).expect("failed to write packet");

                write.flush().expect("failed to flush write");
            }
        });

        let write_handle = std::thread::spawn({
            let addr = addr.clone();
            move || loop {
                let packet = full_decode(&mut read).expect("failed to decode packet");

                trace!("received packet {:?} on {addr}", packet);
                let response = ClientResponse::from(packet);
                ack_tx.send(response).expect("failed to send ack");
            }
        });

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
    use necronomicon::{
        kv_store_codec::{Key, Put},
        Packet,
    };

    use crate::{
        common::{
            reqres::ClientResponse,
            stream::{get_connection, mock::gaurantee_address_rejection},
        },
        error::Error,
    };

    use super::Outgoing;

    #[test]
    fn errs_on_fail_to_connect() {
        const ADDRESS: &str = "127.0.0.1:9999";

        let (_, requests_rx) = crossbeam::channel::unbounded();
        let (ack_tx, _) = crossbeam::channel::unbounded();
        gaurantee_address_rejection(ADDRESS, ErrorKind::ConnectionRefused);
        let outgoing = Outgoing::new(ADDRESS, requests_rx, ack_tx);
        assert_matches!(outgoing, Err(Error::Connection));
    }

    #[test]
    fn reads_and_writes() {
        const ADDRESS: &str = "127.0.0.1:9998";

        let (requests_tx, requests_rx) = crossbeam::channel::unbounded();
        let (ack_tx, ack_rx) = crossbeam::channel::unbounded();

        let outgoing = Outgoing::new(ADDRESS, requests_rx, ack_tx);
        assert_matches!(outgoing, Ok(_));

        let put = Put::new(
            (1, 0),
            Key::try_from("key").expect("key"),
            b"value".to_vec(),
        );

        requests_tx
            .send(Packet::Put(put.clone()))
            .expect("send ping");

        // give time for the outgoing to receive the put.
        std::thread::sleep(std::time::Duration::from_millis(100));

        let stream = get_connection(ADDRESS).expect("stream");

        stream.verify_writes(&[Packet::Put(put.clone())]);
        let put_ack = put.ack();
        stream.fill_read(put_ack.clone());

        let ClientResponse::Put(ack) = ack_rx.recv().expect("ack") else {
            panic!("expected ping ack");
        };

        assert_eq!(ack, put_ack);
    }
}
