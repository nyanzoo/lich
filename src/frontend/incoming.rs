use std::{
    io::Write,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

use crossbeam::channel::{bounded, Receiver, Sender};
use log::{debug, error, info, trace, warn};

use necronomicon::{full_decode, Encode};
use rayon::{ThreadPool, ThreadPoolBuilder};

use crate::common::{
    reqres::{ClientRequest, ClientResponse, ProcessRequest},
    session::Session,
    stream::TcpListener,
};

/// # Description
/// This is for accepting incoming requests.
pub struct Incoming {
    listener: TcpListener,
    // NOTE: later maybe replace with dequeue? It might be slower but it will allow for recovery of even unprocessed msgs.
    requests_tx: Sender<ProcessRequest>,
    pool: ThreadPool,
}

impl Incoming {
    pub fn new(port: u16, requests_tx: Sender<ProcessRequest>) -> Self {
        info!("starting listening for incoming requests on port {}", port);
        let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port))
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
    pub fn run(self) {
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
                    let (ack_tx, ack_rx) = bounded(0);
                    info!("new session {:?}", session);
                    let (read, write) = (session.clone(), session.clone());
                    // I think the acks need to be oneshots that are per
                    pool.spawn(|| {
                        debug!("spawned thread for requests on session {:?}", read);
                        handle_requests(read, requests_tx, ack_tx);
                    });
                    pool.spawn(|| {
                        debug!("spawned thread for acks for session {:?}", write);
                        handle_acks(write, ack_rx);
                    });
                }
                Err(err) => error!("listener.accept: {}", err),
            }
        }
    }
}

fn handle_acks(mut session: Session, ack_rx: Receiver<ClientResponse>) {
    loop {
        if let Ok(ack) = ack_rx.recv() {
            trace!("got ack {:?}", ack);
            // TODO: need to see if we need to break here on some cases?
            if let Err(err) = ack.encode(&mut session) {
                trace!("failed to encode ack {ack:?} due to {err}, flushing session");
                if let Err(err) = session.flush() {
                    debug!("session.flush: {err}");
                    break;
                }
                // encode again because first failed.
                if let Err(err) = ack.encode(&mut session) {
                    warn!("failed to encode ack {ack:?} due to {err}, flushing session");
                    break;
                }
            }
        } else {
            debug!("ack_rx.recv: closed");
            break;
        }
    }

    trace!("flushing session");
    if let Err(err) = session.flush() {
        warn!("session.flush: {err}");
    }
}

fn handle_requests(
    mut session: Session,
    requests_tx: Sender<ProcessRequest>,
    ack_tx: Sender<ClientResponse>,
) {
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
            Err(err) => {
                warn!("warn: {}", err);
                session.flush().expect("flush");
            }
        }
    }
}