use std::{
    io::Write,
    net::{IpAddr, Ipv6Addr, SocketAddr, TcpListener, TcpStream},
    sync::mpsc::{self, TryRecvError},
    thread::JoinHandle,
};

use log::{error, info, trace};

use necronomicon::{partial_decode, Ack, Encode, Header};
use phylactery::{buffer::InMemBuffer, ring_buffer::Pusher};

use crate::acks::Acks;

/// # Description
/// This is for accepting incoming requests.
pub(super) struct Incoming {
    listener: TcpListener,
    pusher: Pusher<InMemBuffer>,
    ack_rx: mpsc::Receiver<Box<dyn Encode<TcpStream>>>,
    acks: Acks,
}

impl Incoming {
    pub(super) fn new(
        port: u16,
        pusher: Pusher<InMemBuffer>,
        ack_rx: mpsc::Receiver<Box<dyn Encode<TcpStream>>>,
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

    // TODO: we need to have a request type that maps to clients, that way we can send the acks back to the correct client without having to send response back to `Incoming`.
    pub(super) fn run(self) -> JoinHandle<()> {
        std::thread::spawn(move || {
            for stream in self.listener.incoming() {
                match stream {
                    Ok(stream) => {
                        let mut header = Header::decode(&mut stream).expect("decode");

                        let packet = partial_decode(header, &mut stream).expect("partial_decode");

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
                                if let Some(nack) = packet.nack(necronomicon::SERVER_BUSY) {
                                    nack.encode(&mut stream).expect("encode");
                                    stream.flush().expect("flush");
                                }
                            }
                        }
                    }
                    Err(err) => error!("listener.accept: {}", err),
                }
            }
        })
    }
}
