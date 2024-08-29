use std::{
    collections::BTreeMap,
    io::Write,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use crossbeam::channel::{bounded, Receiver, Sender, TryRecvError};
use log::{debug, error, info, trace, warn};
use rayon::{ThreadPool, ThreadPoolBuilder};

use necronomicon::{Encode, PoolImpl, SharedImpl};
use net::{
    session::{Session, SessionReader, SessionWriter},
    stream::TcpListener,
};
use requests::{ClientRequest, ClientResponse, ProcessRequest};

use crate::decode_packet_on_reader_and;

/// # Description
/// This is for accepting incoming requests.
pub struct Incoming {
    listener: TcpListener,
    // NOTE: later maybe replace with deque? It might be slower but it will allow for recovery of even unprocessed msgs.
    requests_tx: Sender<ProcessRequest<SharedImpl>>,
    pool: ThreadPool,
}

impl Incoming {
    pub fn new(port: u16, requests_tx: Sender<ProcessRequest<SharedImpl>>) -> Self {
        info!("starting listening for incoming requests on port {}", port);
        let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port))
            .expect("listener");
        let pool = ThreadPoolBuilder::new()
            .num_threads(4)
            .thread_name(|x| format!("incoming-{x}"))
            .build()
            .expect("thread pool");
        Self {
            listener,
            requests_tx,
            pool,
        }
    }

    // TODO: we need to have a request type that maps to clients, that way we can send the acks back to the correct client without having to send response back to `Incoming`.
    pub fn run(self, buffer_pool: PoolImpl) {
        info!("starting listening for incoming requests");
        let Self {
            listener,
            requests_tx,
            pool,
        } = self;

        let mut session_map = BTreeMap::new();
        // If same stream  reconnects we need to kill the old threads.
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let session = Session::new(stream, 5);
                    if let Some(old_session) =
                        session_map.insert(session.peer_addr(), session.clone())
                    {
                        trace!("killing old session {:?}", old_session);
                        _ = old_session.shutdown();
                    }

                    let requests_tx = requests_tx.clone();
                    let (ack_tx, ack_rx) = bounded::<ClientResponse<SharedImpl>>(1024);
                    info!("new session {:?}", session);
                    let (read, write) = session.split();
                    // let ack_writer = write.clone();
                    let buffer_pool = buffer_pool.clone();

                    pool.spawn(|| {
                        let id = read.id();
                        debug!("spawned thread for requests on session {id}");
                        handle_requests(read, requests_tx, ack_tx, buffer_pool);
                        debug!("thread for requests on session {id} exited");
                    });
                    pool.spawn(|| {
                        let id = write.id();
                        debug!("spawned thread for acks for session {id}");
                        handle_acks(write, ack_rx);
                        debug!("thread for acks for session {id} exited");
                    });
                }
                Err(err) => error!("listener.accept: {}", err),
            }
        }
    }
}

fn handle_acks(mut session: SessionWriter, ack_rx: Receiver<ClientResponse<SharedImpl>>) {
    let mut wrote = false;
    loop {
        match ack_rx.try_recv() {
            Ok(ack) => {
                trace!("got ack {:?}", ack);
                // TODO: need to see if we need to break here on some cases?
                if let Err(err) = ack.encode(&mut session) {
                    trace!("failed to encode ack {ack:?} due to {err}, flushing session");
                    break;
                }

                wrote = true;
            }
            Err(TryRecvError::Empty) => {
                if wrote {
                    if let Err(err) = session.flush() {
                        debug!("session.flush: {err}");
                        break;
                    }
                } else {
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
            Err(TryRecvError::Disconnected) => {
                debug!("ack_rx.recv: disconnected");
                break;
            }
        }
    }

    trace!("flushing session");
    if let Err(err) = session.flush() {
        warn!("session.flush: {err}");
    }
}

fn handle_requests(
    mut reader: SessionReader,
    requests_tx: Sender<ProcessRequest<SharedImpl>>,
    ack_tx: Sender<ClientResponse<SharedImpl>>,
    pool: PoolImpl,
) {
    decode_packet_on_reader_and(&mut reader, &pool, move |packet| {
        let request = ClientRequest::from(packet.clone());
        let request = ProcessRequest::new(request, ack_tx.clone());

        // TODO: fill buf
        // TODO: need to update interface of these to take a `Reader` to allow direct reads into the buffer
        // without having to copy the data.
        match requests_tx.send(request) {
            Ok(_) => {
                trace!("pushed {:?} packet", packet);
                true
            }
            Err(err) => {
                warn!("requests_tx.send: {}", err);
                if let Some(nack) = packet.nack(necronomicon::SERVER_BUSY, None) {
                    ack_tx.send(nack.into()).expect("ack_tx.send");
                }
                false
            }
        }
    });
}
