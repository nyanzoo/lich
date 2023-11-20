use std::{
    collections::HashMap,
    io::{Write, BufReader},
    net::{Shutdown, ToSocketAddrs},
    thread::JoinHandle,
    time::Duration,
};

use crossbeam::channel::{bounded, Receiver, Select, Sender};

use log::{debug, info, trace, warn};
use necronomicon::{
    full_decode,
    system_codec::{Join, Position, Role},
    Ack, Encode, Packet,
};
use uuid::Uuid;

use crate::{
    common::{
        reqres::{ClientResponse, PendingRequest, ProcessRequest, System},
        stream::{RetryConsistent, TcpStream},
    },
    config::EndpointConfig,
    outgoing::Outgoing,
};

const CHANNEL_CAPACITY: usize = 1024;

pub trait State {
    fn next(self: Box<Self>) -> Box<dyn State>;
}

pub struct Init {
    endpoint_config: EndpointConfig,

    requests_rx: Receiver<ProcessRequest>,
}

impl Init {
    pub fn init(
        endpoint_config: EndpointConfig,
        requests_rx: Receiver<ProcessRequest>,
    ) -> Box<dyn State> {
        info!("starting state machine");
        Box::new(Init {
            endpoint_config,
            requests_rx,
        }) as _
    }
}

impl State for Init {
    fn next(self: Box<Self>) -> Box<dyn State> {
        debug!("initialized and moving to waiting for operator");

        let Init {
            endpoint_config,
            requests_rx,
        } = *self;

        let operator =
            OperatorConnection::connect(endpoint_config.operator_addr, endpoint_config.port);

        Box::new(WaitingForOperator {
            operator,
            requests_rx,
        })
    }
}

pub struct WaitingForOperator {
    operator: OperatorConnection,

    requests_rx: Receiver<ProcessRequest>,
}

impl State for WaitingForOperator {
    fn next(self: Box<Self>) -> Box<dyn State> {
        let WaitingForOperator {
            operator,
            requests_rx,
        } = *self;

        // Get the `Report` from operator.
        let operator_rx = operator.rx();
        let packet = operator_rx.recv().expect("recv");
        let System::Report(report) = packet else {
            panic!("expected report but got {:?}", packet);
        };

        debug!("got report: {:?}", report);
        let position = report.position().clone();

        let (head_outgoing_tx, head_outgoing_rx) = bounded(CHANNEL_CAPACITY);
        let (tail_outgoing_tx, tail_outgoing_rx) = bounded(CHANNEL_CAPACITY);

        let (head_addr, tail_addr) = match position.clone() {
            Position::Frontend { head, tail } => (head, tail),
            _ => {
                panic!("got {position:?} position for a frontend!")
            }
        };

        let (head, head_ack_rx) = if let Some(outgoing_addr) = head_addr {
            let (ack_tx, ack_rx) = bounded(CHANNEL_CAPACITY);
            trace!("creating outgoing to {}", outgoing_addr);
            match Outgoing::new(outgoing_addr.clone(), head_outgoing_rx, ack_tx) {
                Ok(outgoing) => (Some(outgoing), Some(ack_rx)),
                Err(err) => {
                    warn!("failed to create outgoing to {}: {}", outgoing_addr, err);
                    return Box::new(WaitingForOperator {
                        operator,
                        requests_rx,
                    });
                }
            }
        } else {
            (None, None)
        };

        let (tail, tail_ack_rx) = if let Some(outgoing_addr) = tail_addr {
            let (ack_tx, ack_rx) = bounded(CHANNEL_CAPACITY);
            trace!("creating outgoing to {}", outgoing_addr);

            match Outgoing::new(outgoing_addr.clone(), tail_outgoing_rx, ack_tx) {
                Ok(outgoing) => (Some(outgoing), Some(ack_rx)),
                Err(err) => {
                    warn!("failed to create outgoing to {}: {}", outgoing_addr, err);
                    return Box::new(WaitingForOperator {
                        operator,
                        requests_rx,
                    });
                }
            }
        } else {
            (None, None)
        };

        debug!("moving to ready");
        Box::new(Ready {
            position,

            pending: Default::default(),

            operator,

            head_ack_rx,
            tail_ack_rx,

            requests_rx,

            head_outgoing_tx,
            tail_outgoing_tx,

            head,
            tail,
        })
    }
}

pub struct Ready {
    position: Position,

    pending: HashMap<u128, PendingRequest>,

    operator: OperatorConnection,

    head_ack_rx: Option<Receiver<ClientResponse>>,
    tail_ack_rx: Option<Receiver<ClientResponse>>,

    requests_rx: Receiver<ProcessRequest>,
    head_outgoing_tx: Sender<Packet>,
    tail_outgoing_tx: Sender<Packet>,

    head: Option<Outgoing>,
    tail: Option<Outgoing>,
}

impl State for Ready {
    fn next(self: Box<Self>) -> Box<dyn State> {
        let Ready {
            position,

            mut pending,

            operator,

            head_ack_rx,
            tail_ack_rx,

            requests_rx,

            head_outgoing_tx,
            tail_outgoing_tx,

            head,
            tail,
        } = *self;

        let mut sel = Select::new();
        let operator_tx = operator.tx();
        let operator_rx = operator.rx();
        let operator_id = sel.recv(&operator_rx);

        let head_ack_rx_id = if let Some(head_ack_rx) = head_ack_rx.as_ref() {
            sel.recv(head_ack_rx)
        } else {
            usize::MAX
        };

        let tail_ack_rx_id = if let Some(tail_ack_rx) = tail_ack_rx.as_ref() {
            sel.recv(tail_ack_rx)
        } else {
            usize::MAX
        };
        {
            let request_rx_id = sel.recv(&requests_rx);

            trace!("waiting for client request or ack");
            let oper = sel.select();
            match oper.index() {
                i if i == operator_id => {
                    let system = oper.recv(&operator_rx).expect("recv");
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
                                last_position: position,
                                new_position,
                                pending,
                                operator,
                                head_ack_rx,
                                tail_ack_rx,
                                requests_rx,
                                head_outgoing_tx,
                                tail_outgoing_tx,
                                head,
                                tail,
                            });
                        }
                        _ => {
                            warn!("expected valid system message but got {:?}", system);
                        }
                    }
                }
                i if i == head_ack_rx_id => {
                    if let Some(ack_rx) = head_ack_rx.as_ref() {
                        let response = oper.recv(ack_rx).expect("recv");
                        trace!("got ack: {:?}", response);

                        let id = response.header().uuid();
                        if let Some(request) = pending.remove(&id) {
                            request.complete(response);
                        } else {
                            panic!("got ack for unknown request");
                        }
                    }
                }
                i if i == tail_ack_rx_id => {
                    if let Some(ack_rx) = tail_ack_rx.as_ref() {
                        let response = oper.recv(ack_rx).expect("recv");
                        trace!("got ack: {:?}", response);

                        let id = response.header().uuid();
                        if let Some(request) = pending.remove(&id) {
                            request.complete(response);
                        } else {
                            panic!("got ack for unknown request");
                        }
                    }
                }
                i if i == request_rx_id => {
                    let prequest = oper.recv(&requests_rx).expect("recv");
                    trace!("got request: {:?}", prequest);

                    match position {
                        Position::Frontend { .. } => {
                            let id = prequest.request.id();

                            let (request, pending_request) = prequest.into_parts();

                            if request.is_for_head() {
                                trace!("sending request {request:?} to next node");
                                if head_outgoing_tx.send(request.into()).is_err() {
                                    warn!("failed to send request to next node");
                                    return Box::new(WaitingForOperator {
                                        operator,
                                        requests_rx,
                                    });
                                }

                                pending.insert(id, pending_request);
                            } else if request.is_for_tail() {
                                trace!("sending request {request:?} to next node");
                                if tail_outgoing_tx.send(request.into()).is_err() {
                                    warn!("failed to send request to next node");
                                    return Box::new(WaitingForOperator {
                                        operator,
                                        requests_rx,
                                    });
                                }

                                pending.insert(id, pending_request);
                            } else {
                                panic!("got request {request:?} for unknown node");
                            }
                        }
                        _ => {
                            panic!(
                                "got a request with position {position:?} but was not a frontend!"
                            );
                        }
                    }
                }
                _ => {
                    panic!("unknown index");
                }
            }
        }
        Box::new(Ready {
            position,
            pending,
            operator,
            head_ack_rx,
            tail_ack_rx,
            requests_rx,
            head_outgoing_tx,
            tail_outgoing_tx,
            head,
            tail,
        })
    }
}

pub struct Update {
    last_position: Position,
    new_position: Position,

    pending: HashMap<u128, PendingRequest>,

    operator: OperatorConnection,

    head_ack_rx: Option<Receiver<ClientResponse>>,
    tail_ack_rx: Option<Receiver<ClientResponse>>,

    requests_rx: Receiver<ProcessRequest>,
    head_outgoing_tx: Sender<Packet>,
    tail_outgoing_tx: Sender<Packet>,

    head: Option<Outgoing>,
    tail: Option<Outgoing>,
}

impl State for Update {
    fn next(self: Box<Self>) -> Box<dyn State> {
        let Update {
            last_position,
            new_position,
            pending,
            operator,
            head_ack_rx,
            tail_ack_rx,
            requests_rx,
            head_outgoing_tx,
            tail_outgoing_tx,
            head,
            tail,
        } = *self;

        if last_position == new_position {
            Box::new(Ready {
                position: new_position,
                pending,
                operator,
                head_ack_rx,
                tail_ack_rx,
                requests_rx,
                head_outgoing_tx,
                tail_outgoing_tx,
                head,
                tail,
            })
        } else {
            drop(head);
            drop(tail);

            info!("updating from {:?} to {:?}", last_position, new_position);

            let (head_outgoing_tx, head_outgoing_rx) = bounded(CHANNEL_CAPACITY);
            let (tail_outgoing_tx, tail_outgoing_rx) = bounded(CHANNEL_CAPACITY);

            let (head_addr, tail_addr) = match new_position.clone() {
                Position::Frontend { head, tail } => (head, tail),
                _ => {
                    panic!("got {new_position:?} position for a frontend!")
                }
            };

            let (head, head_ack_rx) = if let Some(outgoing_addr) = head_addr {
                let (ack_tx, ack_rx) = bounded(CHANNEL_CAPACITY);
                trace!("creating outgoing to {}", outgoing_addr);

                match Outgoing::new(outgoing_addr.clone(), head_outgoing_rx, ack_tx) {
                    Ok(outgoing) => (Some(outgoing), Some(ack_rx)),
                    Err(err) => {
                        warn!("failed to create outgoing to {}: {}", outgoing_addr, err);
                        return Box::new(WaitingForOperator {
                            operator,
                            requests_rx,
                        });
                    }
                }
            } else {
                (None, None)
            };

            let (tail, tail_ack_rx) = if let Some(outgoing_addr) = tail_addr {
                let (ack_tx, ack_rx) = bounded(CHANNEL_CAPACITY);
                trace!("creating outgoing to {}", outgoing_addr);

                match Outgoing::new(outgoing_addr.clone(), tail_outgoing_rx, ack_tx) {
                    Ok(outgoing) => (Some(outgoing), Some(ack_rx)),
                    Err(err) => {
                        warn!("failed to create outgoing to {}: {}", outgoing_addr, err);
                        return Box::new(WaitingForOperator {
                            operator,
                            requests_rx,
                        });
                    }
                }
            } else {
                (None, None)
            };

            Box::new(Ready {
                position: new_position,
                pending,
                operator,
                head_ack_rx,
                tail_ack_rx,
                requests_rx,
                head_outgoing_tx,
                tail_outgoing_tx,
                head,
                tail,
            })
        }
    }
}

pub struct OperatorConnection {
    _read: JoinHandle<()>,
    _write: JoinHandle<()>,
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
    fn connect<A>(addr: A, our_port: u16) -> Self
    where
        A: ToSocketAddrs + Clone + std::fmt::Debug,
    {
        let (operator_tx, state_rx) = bounded(CHANNEL_CAPACITY);
        let (state_tx, operator_rx) = bounded(CHANNEL_CAPACITY);
        let (kill_tx, kill_rx) = bounded(CHANNEL_CAPACITY);

        trace!("connecting to operator at {:?}", addr);
        let mut operator = TcpStream::retryable_connect(
            addr,
            RetryConsistent::new(Duration::from_millis(500), None),
        )
        .expect("connect");
        trace!("connected to operator {:?}", operator);
        let mut operator_read = BufReader::new(operator.clone());
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
                (1, Uuid::new_v4().as_u128()),
                Role::Frontend(format!("{}:{}", fqdn, our_port)),
                0,
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
            _read: read,
            _write: write,
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
