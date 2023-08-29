use std::{io::Write, net::{TcpStream, ToSocketAddrs}};

use crossbeam::channel::{Receiver, Sender};
use log::info;

use necronomicon::{full_decode, Encode, Packet};

use crate::reqres::Response;

// Needs to be created by receiving an update from operator, which means `state.rs`
// needs to be able to create `Outgoing` and start it and kill it.
pub(super) struct Outgoing {
    requests_rx: Receiver<Packet>,
    ack_tx: Sender<Response>,

    stream: TcpStream,
}

impl Outgoing {
    pub(super) fn new(
        addr: impl ToSocketAddrs,
        requests_rx: Receiver<Packet>,
        ack_tx: Sender<Response>,
    ) -> Self {
        let stream = TcpStream::connect(addr).expect("failed to connect to outgoing");

        Self {
            requests_rx,
            ack_tx,

            stream,
        }
    }

    pub(super) fn run(self) {
        info!("starting outgoing");
        let (read, write) = (self.stream.try_clone(), self.stream.try_clone());
        let mut read = read.expect("failed to split stream");
        let mut write = write.expect("failed to split stream");

        std::thread::spawn(move || loop {
            let packet = self
                .requests_rx
                .recv()
                .expect("must be able to receive packet");

            packet.encode(&mut write).expect("failed to write packet");

            write.flush().expect("failed to flush write");
        });

        std::thread::spawn(move || loop {
            let packet = full_decode(&mut read).expect("failed to decode packet");

            let response = Response::from(packet);
            self.ack_tx.send(response).expect("failed to send ack");
        });
    }
}
