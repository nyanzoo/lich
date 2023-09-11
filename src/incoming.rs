use std::{
    io::Write,
    net::{IpAddr, Ipv6Addr, SocketAddr, TcpListener},
};

use crossbeam::channel::Sender;
use log::{debug, error, info, trace, warn};

use necronomicon::{full_decode, Encode};
use rayon::{ThreadPool, ThreadPoolBuilder};

use crate::{
    reqres::{ClientRequest, ProcessRequest},
    session::Session,
};

/// # Description
/// This is for accepting incoming requests.
pub(super) struct Incoming {
    listener: TcpListener,
    // NOTE: later maybe replace with dequeue? It might be slower but it will allow for recovery of even unprocessed msgs.
    requests_tx: Sender<ProcessRequest>,
    pool: ThreadPool,
}

impl Incoming {
    pub(super) fn new(port: u16, requests_tx: Sender<ProcessRequest>) -> Self {
        info!("starting listening for incoming requests on port {}", port);
        let listener = TcpListener::bind(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), port))
            .expect("listener");
        let pool = ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .expect("thread pool");
        Self {
            listener,
            requests_tx,
            pool,
        }
    }

    // TODO: we need to have a request type that maps to clients, that way we can send the acks back to the correct client without having to send response back to `Incoming`.
    pub(super) fn run(self) {
        info!("starting listening for incoming requests");
        let Self {
            listener,
            requests_tx,
            pool,
        } = self;

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    stream.set_nonblocking(true).expect("set_nonblocking");
                    let requests_tx = requests_tx.clone();
                    let session = Session::new(stream, 5);
                    info!("new session {:?}", session);
                    // I think the acks need to be oneshots that are per
                    pool.install(|| {
                        debug!("spawned thread for incoming session {:?}", session);
                        handle_incoming(session, requests_tx);
                    });
                }
                Err(err) => error!("listener.accept: {}", err),
            }
        }
    }
}

fn handle_incoming(mut session: Session, requests_tx: Sender<ProcessRequest>) {
    let (ack_tx, ack_rx) = std::sync::mpsc::channel();
    loop {
        match full_decode(&mut session) {
            Ok(packet) => {
                session.update_last_seen();
                trace!("got {:?} packet", packet);

                let request = ClientRequest::from(packet.clone());
                let request = ProcessRequest::new(request, ack_tx.clone());

                // TODO: fill buf
                // TODO: need to update interface of these to take a `Reader` to allow direct reads into the buffer
                // without having to copy the data.
                match requests_tx.send(request) {
                    Ok(_) => {
                        trace!("pushed {:?} packet", packet);
                        // Try to get all the acks we can.
                        loop {
                            match ack_rx.try_recv() {
                                Ok(ack) => {
                                    trace!("got ack {:?}", ack);
                                    // TODO: need to see if we need to break here on some cases?
                                    if let Err(_) = ack.encode(&mut session) {
                                        session.flush().expect("flush");
                                    }
                                }
                                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                                    panic!("ack_rx disconnected")
                                }
                                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                            }
                        }

                        trace!("flushing session");
                        session.flush().expect("flush");
                    }
                    Err(err) => {
                        warn!("requests_tx.send: {}", err);
                        if let Some(nack) = packet.nack(necronomicon::SERVER_BUSY) {
                            nack.encode(&mut session).expect("encode");
                            session.flush().expect("flush");
                        }
                        break;
                    }
                }
            }
            Err(necronomicon::Error::Decode(err)) => {
                debug!("decode: {}", err);
                if session.is_alive() {
                    continue;
                } else {
                    break;
                }
            }
            Err(necronomicon::Error::Io(err)) | Err(necronomicon::Error::IncompleteHeader(err)) => {
                // Try to get all the acks we can.
                loop {
                    match ack_rx.try_recv() {
                        Ok(ack) => {
                            trace!("got ack {:?}", ack);
                            // TODO: need to see if we need to break here on some cases?
                            if let Err(_) = ack.encode(&mut session) {
                                session.flush().expect("flush");
                            }
                        }
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                            panic!("ack_rx disconnected")
                        }
                        Err(std::sync::mpsc::TryRecvError::Empty) => break,
                    }
                }

                session.flush().expect("flush");
            }
            Err(err) => {
                error!("err: {}", err);
                break;
            }
        }
    }
}
