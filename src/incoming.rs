use std::{
    io::Write,
    net::{IpAddr, Ipv6Addr, SocketAddr, TcpListener},
    sync::mpsc::{self, TryRecvError},
};

use log::{debug, error, info, trace, warn};

use necronomicon::{full_decode, Encode};
use rayon::{ThreadPool, ThreadPoolBuilder};

use crate::{
    reqres::{ProcessRequest, Request},
    session::Session,
};

/// # Description
/// This is for accepting incoming requests.
pub(super) struct Incoming {
    listener: TcpListener,
    // NOTE: later maybe replace with dequeue? It might be slower but it will allow for recovery of even unprocessed msgs.
    requests_tx: mpsc::Sender<ProcessRequest>,
    pool: ThreadPool,
}

impl Incoming {
    pub(super) fn new(port: u16, requests_tx: mpsc::Sender<ProcessRequest>) -> Self {
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
                    let requests_tx = requests_tx.clone();
                    let session = Session::new(stream, 5);
                    // I think the acks need to be oneshots that are per
                    pool.spawn(|| {
                        handle_incoming(session, requests_tx);
                    });
                }
                Err(err) => error!("listener.accept: {}", err),
            }
        }
    }
}

fn handle_incoming(mut session: Session, requests_tx: mpsc::Sender<ProcessRequest>) {
    let (ack_tx, ack_rx) = mpsc::channel();
    loop {
        match full_decode(&mut session) {
            Ok(packet) => {
                session.update_last_seen();
                trace!("got {:?} packet", packet);

                let request = Request::from(packet.clone());
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
                                Err(TryRecvError::Disconnected) => {
                                    panic!("ack_rx disconnected")
                                }
                                Err(TryRecvError::Empty) => break,
                            }
                        }
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
            Err(err) => {
                warn!("err: {}", err);
                break;
            }
        }
    }
}
