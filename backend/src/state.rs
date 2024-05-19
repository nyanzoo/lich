use std::{
    collections::HashMap,
    io::{BufReader, Write},
    net::Shutdown,
    thread::JoinHandle,
    time::Duration,
};

use crossbeam::channel::{bounded, Receiver, Select, Sender};
use log::{debug, error, info, trace, warn};
use uuid::Uuid;

use config::EndpointConfig;
use io::outgoing::Outgoing;
use necronomicon::{
    full_decode,
    system_codec::{Join, Position, Role},
    Ack, ByteStr, Encode, Packet, Pool, PoolImpl, SharedImpl,
};
use net::stream::{RetryConsistent, TcpStream};
use requests::{ClientRequest, ClientResponse, PendingRequest, ProcessRequest, System};

use crate::{store::Store, BufferOwner};

const CHANNEL_CAPACITY: usize = 1024;

pub trait State {
    fn next(self: Box<Self>) -> Box<dyn State>;
}

pub struct Init {
    endpoint_config: EndpointConfig,
    store: Store,
    requests_rx: Receiver<ProcessRequest<SharedImpl>>,
    outgoing_pool: PoolImpl,
}

impl Init {
    pub fn init(
        endpoint_config: EndpointConfig,
        store: Store,
        requests_rx: Receiver<ProcessRequest<SharedImpl>>,
        outgoing_pool: PoolImpl,
    ) -> Box<dyn State> {
        Box::new(Self {
            endpoint_config,
            store,
            requests_rx,
            outgoing_pool,
        }) as _
    }
}

impl State for Init {
    fn next(self: Box<Self>) -> Box<dyn State> {
        let Self {
            endpoint_config,
            store,
            requests_rx,
            outgoing_pool,
        } = *self;

        debug!("initialized and moving to waiting for operator");

        let version = store.version();

        let operator = OperatorConnection::connect(
            endpoint_config.operator_addr.clone(),
            endpoint_config.port,
            version,
        );

        Box::new(WaitingForOperator {
            endpoint_config,
            store,
            operator,
            requests_rx,
            outgoing_pool,
        })
    }
}

pub struct WaitingForOperator {
    endpoint_config: EndpointConfig,

    store: Store,

    operator: OperatorConnection,

    requests_rx: Receiver<ProcessRequest<SharedImpl>>,

    outgoing_pool: PoolImpl,
}

impl State for WaitingForOperator {
    fn next(self: Box<Self>) -> Box<dyn State> {
        let Self {
            endpoint_config,
            store,
            operator,
            requests_rx,
            outgoing_pool,
        } = *self;

        // Get the `Report` from operator.
        let operator_rx = operator.rx();
        let Ok(packet) = operator_rx.recv() else {
            return Box::new(Init {
                store,
                requests_rx,
                outgoing_pool,
                endpoint_config,
            });
        };
        let System::Report(report) = packet else {
            panic!("expected report but got {:?}", packet);
        };

        debug!("got report: {:?}", report);
        let position = report.position().clone();

        let (outgoing_tx, outgoing_rx) = bounded(CHANNEL_CAPACITY);

        let outgoing_addr = match position.clone() {
            Position::Frontend { .. } => {
                panic!("got frontend position for a backend!")
            }
            Position::Head { next } => Some(next),
            Position::Middle { next } => Some(next),
            Position::Tail { candidate } => candidate,
            Position::Candidate => None,
            Position::Observer { .. } => {
                panic!("got observer position for a backend!")
            }
        };

        let (outgoing, ack_rx) = if let Some(outgoing_addr) = outgoing_addr {
            let (ack_tx, ack_rx) = bounded(CHANNEL_CAPACITY);
            trace!("creating outgoing to {:?}", outgoing_addr);
            match Outgoing::new(
                outgoing_addr.as_str().expect("valid addr"),
                outgoing_rx,
                ack_tx,
                outgoing_pool.clone(),
            ) {
                Ok(outgoing) => (Some(outgoing), Some(ack_rx)),
                Err(err) => {
                    warn!("failed to create outgoing to {:?}: {}", outgoing_addr, err);
                    return Box::new(WaitingForOperator {
                        endpoint_config,
                        store,
                        operator,
                        requests_rx,
                        outgoing_pool,
                    });
                }
            }
        } else {
            (None, None)
        };

        debug!("moving to ready");
        Box::new(Ready {
            endpoint_config,
            position,

            store,
            not_ready: Default::default(),
            pending: Default::default(),

            operator,

            ack_rx,
            requests_rx,
            outgoing_tx,

            outgoing,
            outgoing_pool,
        })
    }
}

pub struct Ready {
    endpoint_config: EndpointConfig,

    position: Position<SharedImpl>,

    store: Store,
    not_ready: Vec<ProcessRequest<SharedImpl>>,
    pending: HashMap<necronomicon::Uuid, PendingRequest<SharedImpl>>,

    operator: OperatorConnection,

    ack_rx: Option<Receiver<ClientResponse<SharedImpl>>>,
    requests_rx: Receiver<ProcessRequest<SharedImpl>>,
    outgoing_tx: Sender<Packet<SharedImpl>>,

    outgoing: Option<Outgoing>,

    outgoing_pool: PoolImpl,
}

impl State for Ready {
    fn next(self: Box<Self>) -> Box<dyn State> {
        let Self {
            endpoint_config,
            position,
            mut store,
            mut not_ready,
            mut pending,
            operator,
            ack_rx,
            requests_rx,
            outgoing_tx,
            outgoing,
            outgoing_pool,
        } = *self;

        trace!("processing not ready requests");
        for prequest in not_ready.drain(..) {
            if let Position::Tail { .. } = position {
                let mut buffer = outgoing_pool.acquire(BufferOwner::StoreCommit);

                let (request, pending) = prequest.into_parts();
                let response = store.commit_patch(request, &mut buffer);
                let response = ClientResponse::from(response);

                pending.complete(response);
            } else {
                let id = prequest.request.id();
                store.add_to_pending(prequest.request.clone());
                outgoing_tx
                    .send(prequest.request.clone().into())
                    .expect("send");

                let (_, pending_request) = prequest.into_parts();
                pending.insert(id, pending_request);
            }
        }

        trace!("setup select for operator, ack, or client request");

        let mut sel = Select::new();
        let operator_tx = operator.tx();
        let operator_rx = operator.rx();
        let operator_id = sel.recv(&operator_rx);
        let ack_rx_clone = ack_rx.clone();
        let ack_rx_id = if let Some(ack_rx) = ack_rx.as_ref() {
            sel.recv(ack_rx)
        } else {
            usize::MAX
        };
        let request_rx_id = sel.recv(&requests_rx);

        trace!("waiting for client request or ack");
        let oper = sel.select();
        match oper.index() {
            i if i == operator_id => {
                trace!("got operator message");
                let Ok(system) = oper.recv(&operator_rx) else {
                    return Box::new(Init {
                        store,
                        requests_rx,
                        outgoing_pool,
                        endpoint_config,
                    });
                };
                trace!("got system packet: {:?}", system);
                match system {
                    System::Ping(ping) => {
                        operator_tx
                            .send(System::from(Packet::PingAck(ping.ack())))
                            .expect("send");
                    }
                    System::Report(report) => {
                        let new_position = report.position().clone();
                        operator_tx
                            .send(System::ReportAck(report.ack()))
                            .expect("operator ack");

                        return Box::new(Update {
                            endpoint_config,
                            last_position: position,
                            new_position,
                            store,
                            pending,
                            operator,
                            ack_rx,
                            requests_rx,
                            outgoing_tx,
                            outgoing,
                            outgoing_pool,
                        });
                    }
                    _ => {
                        warn!("expected valid system message but got {:?}", system);
                    }
                }
            }
            i if i == ack_rx_id => {
                trace!("got ack");
                // only get acks if we are not tail
                if let Some(ack_rx) = ack_rx.as_ref() {
                    let Ok(response) = oper.recv(ack_rx) else {
                        return Box::new(WaitingForOperator {
                            endpoint_config,
                            store,
                            operator,
                            requests_rx,
                            outgoing_pool,
                        });
                    };

                    trace!("got ack: {:?}", response);

                    let id = response.header().uuid;
                    let mut buffer = outgoing_pool.acquire(BufferOwner::StoreCommit);
                    // TODO: maybe take from config?
                    for packet in store.commit_pending(id, &mut buffer) {
                        let id = packet.header().uuid;
                        if let Some(request) = pending.remove(&id) {
                            request.complete(ClientResponse::from(packet));
                        } else {
                            panic!("got ack for unknown request");
                        }
                    }
                }
            }
            i if i == request_rx_id => {
                trace!("got client request");
                let Ok(prequest) = oper.recv(&requests_rx) else {
                    return Box::new(WaitingForOperator {
                        endpoint_config,
                        store,
                        operator,
                        requests_rx,
                        outgoing_pool,
                    });
                };
                trace!("got request: {:?}", prequest);

                match position {
                    Position::Candidate => {
                        let (request, pending) = prequest.into_parts();

                        debug!("got candidate request: {:?}", request);

                        let ClientRequest::Transfer(transfer) = request else {
                            panic!("expected transfer request but got {:?}", request);
                        };

                        store.reconstruct(&transfer);
                        pending.complete(ClientResponse::Transfer(transfer.ack()));
                    }
                    Position::Tail { .. } => {
                        let mut buffer = outgoing_pool.acquire(BufferOwner::StoreCommit);

                        let (request, pending) = prequest.into_parts();
                        trace!("committing patch {request:?}, since we are tail");
                        let response = store.commit_patch(request, &mut buffer);
                        let response = ClientResponse::from(response);

                        pending.complete(response);
                    }
                    _ => {
                        let id = prequest.request.id();
                        store.add_to_pending(prequest.request.clone());

                        let (request, pending_request) = prequest.into_parts();
                        trace!("sending request {request:?} to next node");
                        if outgoing_tx.send(request.into()).is_err() {
                            warn!("failed to send request to next node");
                            return Box::new(WaitingForOperator {
                                endpoint_config,
                                store,
                                operator,
                                requests_rx,
                                outgoing_pool,
                            });
                        }

                        pending.insert(id, pending_request);
                    }
                }
            }
            _ => {
                panic!("unknown index");
            }
        }

        Box::new(Ready {
            endpoint_config,
            position,
            store,
            pending,
            not_ready,
            operator,
            ack_rx: ack_rx_clone,
            requests_rx: requests_rx.clone(),
            outgoing_tx,
            outgoing,
            outgoing_pool,
        })
    }
}

pub struct Update {
    endpoint_config: EndpointConfig,

    last_position: Position<SharedImpl>,
    new_position: Position<SharedImpl>,

    store: Store,
    pending: HashMap<necronomicon::Uuid, PendingRequest<SharedImpl>>,

    operator: OperatorConnection,

    ack_rx: Option<Receiver<ClientResponse<SharedImpl>>>,
    requests_rx: Receiver<ProcessRequest<SharedImpl>>,
    outgoing_tx: Sender<Packet<SharedImpl>>,

    outgoing: Option<Outgoing>,
    outgoing_pool: PoolImpl,
}

impl State for Update {
    fn next(self: Box<Self>) -> Box<dyn State> {
        let Self {
            endpoint_config,
            last_position,
            new_position,

            store,
            pending,

            operator,

            ack_rx,
            requests_rx,
            outgoing_tx,
            outgoing,
            outgoing_pool,
        } = *self;

        if last_position == new_position {
            Box::new(Ready {
                endpoint_config,
                position: new_position,
                store,
                not_ready: Default::default(),
                pending,
                operator,
                ack_rx,
                requests_rx,
                outgoing_tx,
                outgoing,
                outgoing_pool,
            })
        } else {
            drop(outgoing);
            info!("updating from {:?} to {:?}", last_position, new_position);

            let (outgoing_tx, outgoing_rx) = bounded(CHANNEL_CAPACITY);
            let outgoing_addr = match new_position.clone() {
                Position::Frontend { .. } => {
                    panic!("got frontend position for a backend!")
                }
                Position::Head { next } => Some(next),
                Position::Middle { next } => Some(next),
                Position::Tail { candidate } => candidate,
                Position::Candidate => None,
                Position::Observer { .. } => {
                    panic!("got observer position for a backend!")
                }
            };

            let (outgoing, ack_rx) = if let Some(outgoing_addr) = outgoing_addr {
                let (ack_tx, ack_rx) = bounded(CHANNEL_CAPACITY);
                trace!("creating outgoing to {:?}", outgoing_addr);

                match Outgoing::new(
                    outgoing_addr.as_str().expect("valid addr"),
                    outgoing_rx,
                    ack_tx,
                    outgoing_pool.clone(),
                ) {
                    Ok(outgoing) => (Some(outgoing), Some(ack_rx)),
                    Err(err) => {
                        warn!("failed to create outgoing to {:?}: {}", outgoing_addr, err);
                        return Box::new(WaitingForOperator {
                            endpoint_config,
                            store,
                            operator,
                            requests_rx,
                            outgoing_pool,
                        });
                    }
                }
            } else {
                (None, None)
            };

            if matches!(new_position, Position::Candidate) {
                trace!("moving to transfer");
                Box::new(Transfer {
                    endpoint_config,
                    store,
                    operator,
                    ack_rx,
                    requests_rx,
                    outgoing_tx,
                    outgoing,
                    outgoing_pool,
                })
            } else {
                trace!("moving to ready");
                // TODO: handle role change here!
                // Outgoing loop should at least be paused?
                // If there are changes we might need to close the outgoing loop and start a new one.
                Box::new(Ready {
                    endpoint_config,
                    position: new_position,
                    store,
                    not_ready: Default::default(),
                    pending,
                    operator,
                    ack_rx,
                    requests_rx,
                    outgoing_tx,
                    outgoing,
                    outgoing_pool,
                })
            }
        }
    }
}

pub struct Transfer {
    endpoint_config: EndpointConfig,

    store: Store,

    operator: OperatorConnection,

    ack_rx: Option<Receiver<ClientResponse<SharedImpl>>>,
    requests_rx: Receiver<ProcessRequest<SharedImpl>>,
    outgoing_tx: Sender<Packet<SharedImpl>>,

    outgoing: Option<Outgoing>,
    outgoing_pool: PoolImpl,
}

impl State for Transfer {
    fn next(self: Box<Self>) -> Box<dyn State> {
        let Self {
            endpoint_config,
            store,
            operator,
            ack_rx,
            requests_rx,
            outgoing_tx,
            outgoing,
            outgoing_pool,
        } = *self;

        // TODO: handle transfer change here!
        if let Some(outgoing) = outgoing {
            info!("transfering store to {:?}", outgoing.addr);
            // TODO: if we don't want to nuke the store we will need to send versions!
            for transfer in store.deconstruct_iter(outgoing_pool.clone()) {
                outgoing_tx.send(Packet::Transfer(transfer)).expect("send");
            }

            let mut owned = outgoing_pool.acquire(BufferOwner::Position);

            Box::new(Ready {
                endpoint_config,
                position: Position::Middle {
                    next: ByteStr::from_owned(&outgoing.addr, &mut owned).expect("valid addr"),
                },
                store,
                not_ready: Default::default(),
                pending: Default::default(),
                operator,
                ack_rx,
                requests_rx,
                outgoing_tx,
                outgoing: Some(outgoing),
                outgoing_pool,
            })
        } else {
            Box::new(Ready {
                endpoint_config,
                position: Position::Tail { candidate: None },
                store,
                not_ready: Default::default(),
                pending: Default::default(),
                operator,
                ack_rx,
                requests_rx,
                outgoing_tx,
                outgoing,
                outgoing_pool,
            })
        }
    }
}

pub struct OperatorConnection {
    _read: JoinHandle<()>,
    _write: JoinHandle<()>,
    kill_tx: Sender<()>,
    operator_tx: Sender<System<SharedImpl>>,
    operator_rx: Receiver<System<SharedImpl>>,
}

impl Drop for OperatorConnection {
    fn drop(&mut self) {
        self.kill_tx.send(()).expect("kill operator");
    }
}

impl OperatorConnection {
    fn connect(addr: String, our_port: u16, version: u128) -> Self {
        let (operator_tx, state_rx) = bounded(CHANNEL_CAPACITY);
        let (state_tx, operator_rx) = bounded(CHANNEL_CAPACITY);
        let (kill_tx, kill_rx) = bounded(CHANNEL_CAPACITY);

        trace!("connecting to operator at {:?}", addr);
        let mut operator = TcpStream::retryable_connect(
            addr,
            RetryConsistent::new(Duration::from_millis(1000), None),
        )
        .expect("connect");
        trace!("connected to operator {:?}", operator);
        let mut operator_read = BufReader::new(operator.clone());
        let mut operator_write = operator.clone();

        let pool = PoolImpl::new(1024, 1024);
        let read_pool = pool.clone();
        let read = std::thread::spawn(move || {
            let mut buffer = read_pool.acquire(BufferOwner::OperatorFullDecode);
            let packet = full_decode(&mut operator_read, &mut buffer, None).expect("decode");

            let System::JoinAck(ack) = System::from(packet.clone()) else {
                panic!("expected join ack but got {:?}", packet);
            };

            debug!("got join ack: {:?}", ack);

            // Get the `Report` from operator.
            let report = loop {
                let mut owned = read_pool.acquire(BufferOwner::OperatorFullDecode);
                match full_decode(&mut operator_read, &mut owned, None) {
                    Ok(packet) => {
                        let operator_msg = System::from(packet);

                        if let System::Report(report) = operator_msg.clone() {
                            report.clone().ack().encode(&mut operator).expect("encode");
                            operator.flush().expect("flush");
                            break operator_msg;
                        } else {
                            warn!("expected report but got {:?}", operator_msg);
                        }
                    }
                    Err(err) => {
                        warn!("err: {}", err);
                        return;
                    }
                }
            };

            debug!("got report: {:?}", report);
            state_tx.send(report).expect("send report");

            loop {
                let mut owned = read_pool.acquire(BufferOwner::OperatorFullDecode);
                match full_decode(&mut operator_read, &mut owned, None) {
                    Ok(packet) => {
                        let operator_msg = System::from(packet);

                        state_tx.send(operator_msg).expect("send");
                    }
                    Err(err) => {
                        warn!("err: {}", err);
                        break;
                    }
                }
            }
        });

        let write = std::thread::spawn(move || {
            let fqdn = std::process::Command::new("hostname")
                .arg("-f")
                .output()
                .expect("hostname fqdn")
                .stdout;
            let fqdn = String::from_utf8_lossy(&fqdn).trim().to_string();

            debug!("got fqdn: {}", fqdn);

            let mut owned = pool.acquire(BufferOwner::Join);

            // TODO:
            // We will likely pick to use the same port for each BE node.
            // But we need a way to identify each node.
            // We can use a uuid for this.
            let join = Join::new(
                1,
                Uuid::new_v4().as_u128(),
                Role::Backend(
                    ByteStr::from_owned(format!("{}:{}", fqdn, our_port), &mut owned)
                        .expect("valid addr"),
                ),
                version,
                false,
            );

            debug!("sending join to operator: {:?}", join);
            join.encode(&mut operator_write).expect("encode");
            operator_write.flush().expect("flush");

            loop {
                let mut sel = Select::new();
                let state_rx_id = sel.recv(&state_rx);
                let kill_rx_id = sel.recv(&kill_rx);

                let oper = sel.select();
                match oper.index() {
                    i if i == state_rx_id => match oper.recv::<System<_>>(&state_rx) {
                        Ok(system) => {
                            trace!("got system packet: {:?}", system);
                            system.encode(&mut operator_write).expect("encode");
                            operator_write.flush().expect("flush");
                        }
                        Err(err) => {
                            error!("state_rx err: {}", err);
                        }
                    },
                    i if i == kill_rx_id => {
                        debug!("write got kill signal");
                        let _ = operator_write.shutdown(Shutdown::Both);
                        break;
                    }
                    _ => {
                        panic!("unknown index");
                    }
                }
            }
        });

        Self {
            _read: read,
            _write: write,
            kill_tx,
            operator_tx,
            operator_rx,
        }
    }

    fn tx(&self) -> Sender<System<SharedImpl>> {
        self.operator_tx.clone()
    }

    fn rx(&self) -> Receiver<System<SharedImpl>> {
        self.operator_rx.clone()
    }
}
