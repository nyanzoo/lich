use std::{io::Write, net::ToSocketAddrs};

use crossbeam::channel::{Receiver, Sender};
use log::info;

use necronomicon::{full_decode, Encode, Packet};

use crate::{reqres::ClientResponse, stream::TcpStream};

// Needs to be created by receiving an update from operator, which means `state.rs`
// needs to be able to create `Outgoing` and start it and kill it.
pub(super) struct Outgoing {
    read: std::thread::JoinHandle<()>,
    write: std::thread::JoinHandle<()>,

    stream: TcpStream,
    pub addr: String,
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
    ) -> Self {
        let addr = addr
            .to_socket_addrs()
            .expect("failed to resolve addr")
            .next()
            .expect("no addr found")
            .to_string();

        let mut retry = 5;
        let stream = loop {
            match TcpStream::connect(addr.clone()) {
                Ok(stream) => {
                    break stream;
                }
                Err(err) => {
                    retry -= 1;
                    if retry == 0 {
                        panic!("failed to connect to outgoing: {err}");
                    }
                    info!("failed to connect to outgoing: {err}, retries left {retry}");
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
        };
        info!("starting outgoing to {addr}");
        let (mut read, mut write) = (stream.clone(), stream.clone());

        let read_handle = std::thread::spawn(move || loop {
            let packet = requests_rx.recv().expect("must be able to receive packet");

            packet.encode(&mut write).expect("failed to write packet");

            write.flush().expect("failed to flush write");
        });

        let write_handle = std::thread::spawn(move || loop {
            let packet = full_decode(&mut read).expect("failed to decode packet");

            let response = ClientResponse::from(packet);
            ack_tx.send(response).expect("failed to send ack");
        });

        Self {
            read: read_handle,
            write: write_handle,

            stream,
            addr,
        }
    }
}
