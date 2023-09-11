use std::{collections::HashMap, io::Write, net::TcpStream};

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

        operator: TcpStream,

        requests_rx: Receiver<ProcessRequest>,
    },
    Ready {
        position: Position,

        store: Store<String>,
        not_ready: Vec<ProcessRequest>,
        pending: HashMap<u128, PendingRequest>,

        operator: TcpStream,

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

        operator: TcpStream,

        ack_rx: Option<Receiver<ClientResponse>>,
        requests_rx: Receiver<ProcessRequest>,
        outgoing_tx: Sender<Packet>,

        outgoing: Option<Outgoing>,
    },
    Transfer {
        store: Store<String>,

        operator: TcpStream,

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
                trace!("initialized and moving to waiting for operator");
                let mut operator =
                    TcpStream::connect(endpoint_config.operator_addr).expect("connect");

                operator.set_nonblocking(true).expect("set_nonblocking");

                let join = Join::new(
                    Header::new(Kind::Join, 1, Uuid::new_v4().as_u128()),
                    Role::Backend(endpoint_config.addr),
                );

                join.encode(&mut operator).expect("encode");
                operator.flush().expect("flush");

                Self::WaitingForOperator {
                    store,
                    operator,
                    requests_rx,
                }
            }

            Self::WaitingForOperator {
                store,
                mut operator,
                requests_rx,
            } => {
                // Wait for the `JoinAck` from operator.
                loop {
                    match full_decode(&mut operator) {
                        Ok(packet) => {
                            let operator_msg = System::from(packet);

                            if let System::JoinAck(ack) = operator_msg {
                                // If okay then we can get the report from operator.
                                // Otherwise we should panic and exit.
                                if ack.response_code() == SUCCESS {
                                    break;
                                } else {
                                    panic!(
                                        "failed to join operator, got response code {}",
                                        ack.response_code()
                                    );
                                }
                            } else {
                                trace!("expected join ack but got {:?}", operator_msg);
                            }
                        }
                        Err(necronomicon::Error::Decode(err)) => {
                            debug!("decode: {}", err);
                            continue;
                        }
                        Err(necronomicon::Error::Io(err))
                        | Err(necronomicon::Error::IncompleteHeader(err)) => match err.kind() {
                            std::io::ErrorKind::WouldBlock => {
                                trace!("would block, try again later");
                                continue;
                            }
                            _ => {
                                panic!("tcpstream io err: {}", err);
                            }
                        },
                        Err(err) => {
                            panic!("err: {:?}", err);
                        }
                    }
                }

                // Get the `Report` from operator.
                let report = loop {
                    match full_decode(&mut operator) {
                        Ok(packet) => {
                            let operator_msg = System::from(packet);

                            if let System::Report(report) = operator_msg {
                                report.clone().ack().encode(&mut operator).expect("encode");
                                operator.flush().expect("flush");
                                break report;
                            } else {
                                trace!("expected report but got {:?}", operator_msg);
                            }
                        }
                        Err(necronomicon::Error::Decode(err)) => {
                            debug!("decode: {}", err);
                            continue;
                        }
                        Err(err) => {
                            panic!("err: {}", err);
                        }
                    }
                };

                trace!("got report: {:?}", report);
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

                trace!("moving to ready");
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

                mut operator,

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

                match full_decode(&mut operator) {
                    Ok(packet) => {
                        trace!("got system packet: {:?}", packet);
                        let system = System::from(packet);
                        match system {
                            System::Ping(ping) => {
                                ping.ack().encode(&mut operator).expect("encode");
                                operator.flush().expect("flush");
                            }
                            System::Report(report) => {
                                let new_position = report.position().clone();
                                return Self::Update {
                                    last_position: position,
                                    new_position: new_position,
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
                    Err(necronomicon::Error::Decode(err)) => {
                        debug!("decode: {}", err);
                    }
                    Err(necronomicon::Error::Io(err))
                    | Err(necronomicon::Error::IncompleteHeader(err)) => match err.kind() {
                        std::io::ErrorKind::WouldBlock => {
                            let mut sel = Select::new();
                            let ack_rx_id = if let Some(ack_rx) = ack_rx.as_ref() {
                                sel.recv(ack_rx)
                            } else {
                                usize::MAX
                            };
                            let request_rx_id = sel.recv(&requests_rx);

                            trace!("waiting for client request or ack");
                            let oper = sel.select();
                            match oper.index() {
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
                        }
                        _ => {
                            panic!("tcpstream io err: {}", err);
                        }
                    },
                    Err(err) => {
                        panic!("decode err: {}", err);
                    }
                }

                Self::Ready {
                    position,
                    store,
                    pending,
                    not_ready,
                    operator,
                    ack_rx,
                    requests_rx,
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
                // TODO: handle role change here!
                // Outgoing loop should at least be paused?
                // If there are changes we might need to close the outgoing loop and start a new one.
                todo!()
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
