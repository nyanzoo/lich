use std::{
    io::Write,
    net::{IpAddr, Ipv6Addr, SocketAddr, TcpListener},
    sync::mpsc::{self, TryRecvError},
    thread::JoinHandle,
};

use log::{error, info, trace};

use necronomicon::{Ack, Decode, Header};
use phylactery::{buffer::InMemBuffer, ring_buffer::Pusher};

use crate::acks::Acks;

/// # Description
/// This is for accepting incoming requests.
pub(super) struct Incoming {
    listener: TcpListener,
    pusher: Pusher<InMemBuffer>,
    ack_rx: mpsc::Receiver<Box<dyn Ack>>,
    acks: Acks,
}

impl Incoming {
    pub(super) fn new(
        port: u16,
        pusher: Pusher<InMemBuffer>,
        ack_rx: mpsc::Receiver<Box<dyn Ack>>,
    ) -> Self {
        let listener = TcpListener::bind(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), port))
            .expect("listener");
        Self {
            listener,
            pusher,
            ack_rx,
            acks: Acks::new(),
        }
    }

    pub(super) fn run(self) -> JoinHandle<()> {
        std::thread::spawn(move || {
            loop {
                match self.listener.accept() {
                    Ok((mut stream, addr)) => {
                        info!("accepted connection from {}", addr);

                        let mut header = Header::decode(&mut stream).expect("decode");

                        match header.

                        let mut buf = vec![0; header.length as usize];

                        // TODO: fill buf
                        // TODO: need to update interface of these to take a `Reader` to allow direct reads into the buffer
                        // without having to copy the data.
                        match self.pusher.push(&buf) {
                            Ok(bytes) => {
                                trace!("pushed {} bytes", bytes);
                                // Try to get all the acks we can.
                                loop {
                                    match self.ack_rx.try_recv() {
                                        Ok(ack) => {
                                            ack.encode(&mut stream).expect("encode");
                                            stream.flush().expect("flush");
                                        }
                                        Err(TryRecvError::Disconnected) => {
                                            panic!("ack_rx disconnected")
                                        }
                                        Err(TryRecvError::Empty) => break,
                                    }
                                }
                            }
                            Err(_) => {
                                let ack = Ack::failure();
                                let ack = ack.encode();
                                _ = stream.write(&ack).expect("ack err");
                            }
                        }
                    }
                    Err(err) => error!("listener.accept: {}", err),
                }
            }
        })
    }
}
