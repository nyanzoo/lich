use std::{collections::HashMap, io::Write, net::Shutdown, thread::JoinHandle};

use crossbeam::channel::{bounded, Receiver, Select, Sender};

use log::{debug, info, trace, warn};
use necronomicon::{
    full_decode,
    system_codec::{Join, Position, Role},
    Ack, Encode, Header, Kind, Packet, SUCCESS,
};
use uuid::Uuid;

use crate::{
    config::EndpointConfig,
    outgoing::Outgoing,
    reqres::{ClientResponse, PendingRequest, ProcessRequest, System},
    store::Store,
    stream::TcpStream,
};

const CHANNEL_CAPACITY: usize = 1024;

pub(super) enum State {
    Init {
        store: Store<String>,

        endpoint_config: EndpointConfig,

        requests_rx: Receiver<ProcessRequest>,
    },
    WaitingForOperator {
        store: Store<String>,

        operator: OperatorConnection,

        requests_rx: Receiver<ProcessRequest>,
    },
    Ready {
        position: Position,

        store: Store<String>,
        not_ready: Vec<ProcessRequest>,
        pending: HashMap<u128, PendingRequest>,

        operator: OperatorConnection,

        ack_rx: Option<Receiver<ClientResponse>>,
        requests_rx: Receiver<ProcessRequest>,
        outgoing_tx: Sender<Packet>,

        outgoing: Option<Outgoing>,
    },
    Update {
        last_position: Position,
        new_position: Position,

        store: Store<String>,
        pending: HashMap<u128, PendingRequest>,

        operator: OperatorConnection,

        ack_rx: Option<Receiver<ClientResponse>>,
        requests_rx: Receiver<ProcessRequest>,
        outgoing_tx: Sender<Packet>,

        outgoing: Option<Outgoing>,
    },
    Transfer {
        store: Store<String>,

        operator: OperatorConnection,

        ack_rx: Option<Receiver<ClientResponse>>,
        requests_rx: Receiver<ProcessRequest>,
        outgoing_tx: Sender<Packet>,

        outgoing: Option<Outgoing>,
    },
}

impl State {
    pub(super) fn new(
        endpoint_config: EndpointConfig,
        store: Store<String>,
        requests_rx: Receiver<ProcessRequest>,
    ) -> Self {
        info!("starting state machine");
        Self::Init {
            endpoint_config,
            store,
            requests_rx,
        }
    }

    pub(super) fn next(self) -> Self {
        match self {
            Self::Init {
                endpoint_config,
                store,
                requests_rx,
            } => {
                debug!("initialized and moving to waiting for operator");

                let operator = OperatorConnection::connect(
                    endpoint_config.operator_addr,
                    endpoint_config.port,
                );

                Self::WaitingForOperator {
                    store,
                    operator,
                    requests_rx,
                }
            }

            Self::WaitingForOperator {
                store,
                operator,
                requests_rx,
            } => {
                // Get the `Report` from operator.
                let operator_rx = operator.rx();
                let packet = operator_rx.recv().expect("recv");
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
                };

                let (outgoing, ack_rx) = if let Some(outgoing_addr) = outgoing_addr {
                    let (ack_tx, ack_rx) = bounded(CHANNEL_CAPACITY);
                    trace!("creating outgoing to {}", outgoing_addr);
                    (
                        Some(Outgoing::new(outgoing_addr, outgoing_rx, ack_tx)),
                        Some(ack_rx),
                    )
                } else {
                    (None, None)
                };

                debug!("moving to ready");
                Self::Ready {
                    position,

                    store,
                    not_ready: Default::default(),
                    pending: Default::default(),

                    operator,

                    ack_rx,
                    requests_rx,
                    outgoing_tx,

                    outgoing,
                }
            }

            Self::Ready {
                position,

                mut store,
                mut not_ready,
                mut pending,

                operator,

                // make sure for select operation, after getting new outgoing, that
                // we are using updated ack_rx
                ack_rx,
                requests_rx,
                outgoing_tx,

                outgoing,
            } => {
                for prequest in not_ready.drain(..) {
                    if let Position::Tail { .. } = position {
                        let mut buf = vec![0; 1024 * 1024]; // TODO: handle buffer smarter later!

                        let (request, pending) = prequest.into_parts();
                        let response = store.commit_patch(request, &mut buf);
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
                        let packet = oper.recv(&operator_rx).expect("recv");
                        trace!("got system packet: {:?}", packet);
                        let system = System::from(packet);
                        match system {
                            System::Ping(ping) => {
                                operator_tx
                                    .send(System::from(Packet::PingAck(ping.ack())))
                                    .expect("send");
                            }
                            System::Report(report) => {
                                let new_position = report.position().clone();
                                return Self::Update {
                                    last_position: position,
                                    new_position,
                                    store,
                                    pending,
                                    operator,
                                    ack_rx,
                                    requests_rx,
                                    outgoing_tx,
                                    outgoing,
                                };
                            }
                            System::Transfer(transfer) => {
                                let (ack_tx, ack_rx) = bounded(CHANNEL_CAPACITY);

                                let (outgoing_tx, outgoing_rx) = bounded(CHANNEL_CAPACITY);

                                let outgoing =
                                    Some(Outgoing::new(transfer.candidate(), outgoing_rx, ack_tx));

                                return Self::Transfer {
                                    store,
                                    operator,
                                    ack_rx: Some(ack_rx),
                                    requests_rx,
                                    outgoing_tx,
                                    outgoing,
                                };
                            }
                            _ => {
                                warn!("expected valid system message but got {:?}", system);
                            }
                        }
                    }
                    i if i == ack_rx_id => {
                        // only get acks if we are not tail
                        if let Some(ack_rx) = ack_rx.as_ref() {
                            let ack = oper.recv(ack_rx).expect("recv");
                            trace!("got ack: {:?}", ack);
                            let response = ClientResponse::from(ack);

                            let id = response.header().uuid();
                            // TODO: maybe take from config?
                            let mut buf = vec![0; 1024 * 1024];
                            for packet in store.commit_pending(id, &mut buf) {
                                let id = packet.header().uuid();
                                if let Some(request) = pending.remove(&id) {
                                    request.complete(ClientResponse::from(packet));
                                } else {
                                    panic!("got ack for unknown request");
                                }
                            }
                        }
                    }
                    i if i == request_rx_id => {
                        let prequest = oper.recv(&requests_rx).expect("recv");
                        trace!("got request: {:?}", prequest);

                        if let Position::Tail { .. } = position {
                            let mut buf = vec![0; 1024]; // TODO: handle buffer smarter later!

                            let (request, pending) = prequest.into_parts();
                            trace!("committing patch {request:?}, since we are tail");
                            let response = store.commit_patch(request, &mut buf);
                            let response = ClientResponse::from(response);

                            pending.complete(response);
                        } else {
                            let id = prequest.request.id();
                            store.add_to_pending(prequest.request.clone());

                            let (request, pending_request) = prequest.into_parts();
                            trace!("sending request {request:?} to next node");
                            outgoing_tx.send(request.into()).expect("send");
                            pending.insert(id, pending_request);
                        }
                    }
                    _ => {
                        panic!("unknown index");
                    }
                }

                Self::Ready {
                    position,
                    store,
                    pending,
                    not_ready,
                    operator,
                    ack_rx: ack_rx_clone,
                    requests_rx: requests_rx.clone(),
                    outgoing_tx,
                    outgoing,
                }
            }

            Self::Update {
                last_position,
                new_position,
                store,
                pending,
                operator,
                ack_rx,
                requests_rx,
                outgoing_tx,
                outgoing,
            } => {
                if last_position == new_position {
                    Self::Ready {
                        position: new_position,
                        store,
                        not_ready: Default::default(),
                        pending,
                        operator,
                        ack_rx,
                        requests_rx,
                        outgoing_tx,
                        outgoing,
                    }
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
                    };

                    let (outgoing, ack_rx) = if let Some(outgoing_addr) = outgoing_addr {
                        let (ack_tx, ack_rx) = bounded(CHANNEL_CAPACITY);
                        trace!("creating outgoing to {}", outgoing_addr);
                        (
                            Some(Outgoing::new(outgoing_addr, outgoing_rx, ack_tx)),
                            Some(ack_rx),
                        )
                    } else {
                        (None, None)
                    };

                    // TODO: handle role change here!
                    // Outgoing loop should at least be paused?
                    // If there are changes we might need to close the outgoing loop and start a new one.
                    Self::Ready {
                        position: new_position,
                        store,
                        not_ready: Default::default(),
                        pending,
                        operator,
                        ack_rx,
                        requests_rx,
                        outgoing_tx,
                        outgoing,
                    }
                }
            }

            Self::Transfer {
                store,
                operator,
                ack_rx,
                requests_rx,
                outgoing_tx,
                outgoing,
            } => {
                todo!()
            }
        }
    }
}

pub struct OperatorConnection {
    read: JoinHandle<()>,
    write: JoinHandle<()>,
    kill_tx: Sender<()>,
    operator_tx: Sender<System>,
    operator_rx: Receiver<System>,
}

impl Drop for OperatorConnection {
    fn drop(&mut self) {
        self.kill_tx.send(()).expect("kill operator");
    }
}

impl OperatorConnection {
    fn connect<S>(addr: S, our_port: u16) -> Self
    where
        S: ToString,
    {
        let (operator_tx, state_rx) = bounded(CHANNEL_CAPACITY);
        let (state_tx, operator_rx) = bounded(CHANNEL_CAPACITY);
        let (kill_tx, kill_rx) = bounded(CHANNEL_CAPACITY);

        let mut operator = TcpStream::connect(addr).expect("connect");
        trace!("connected to operator {:?}", operator);
        let mut operator_read = operator.clone();
        let mut operator_write = operator.clone();

        let read = std::thread::spawn(move || {
            let packet = full_decode(&mut operator_read).expect("decode");

            let System::JoinAck(ack) = System::from(packet.clone()) else {
                panic!("expected join ack but got {:?}", packet);
            };

            debug!("got join ack: {:?}", ack);

            // Get the `Report` from operator.
            let report = loop {
                match full_decode(&mut operator_read) {
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
                        panic!("err: {}", err);
                    }
                }
            };

            debug!("got report: {:?}", report);
            state_tx.send(report).expect("send report");

            loop {
                match full_decode(&mut operator_read) {
                    Ok(packet) => {
                        let operator_msg = System::from(packet);

                        state_tx.send(operator_msg).expect("send");
                    }
                    Err(err) => {
                        panic!("err: {}", err);
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

            // TODO:
            // We will likely pick to use the same port for each BE node.
            // But we need a way to identify each node.
            // We can use a uuid for this.
            let join = Join::new(
                Header::new(Kind::Join, 1, Uuid::new_v4().as_u128()),
                Role::Backend(format!("{}:{}", "localhost", our_port)),
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
                    i if i == state_rx_id => {
                        let system: System = oper.recv(&state_rx).expect("recv");
                        trace!("got system packet: {:?}", system);
                        system.encode(&mut operator_write).expect("encode");
                        operator_write.flush().expect("flush");
                    }
                    i if i == kill_rx_id => {
                        debug!("write got kill signal");
                        operator_write.shutdown(Shutdown::Both).expect("shutdown");
                        break;
                    }
                    _ => {
                        panic!("unknown index");
                    }
                }
            }
        });

        Self {
            read,
            write,
            kill_tx,
            operator_tx,
            operator_rx,
        }
    }

    fn tx(&self) -> Sender<System> {
        self.operator_tx.clone()
    }

    fn rx(&self) -> Receiver<System> {
        self.operator_rx.clone()
    }
}
