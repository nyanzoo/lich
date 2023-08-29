use crossbeam::channel::{bounded, Receiver, Select, Sender};

use necronomicon::{
    system_codec::{Chain, Position},
    Packet,
};

use crate::{
    outgoing::Outgoing,
    reqres::{ClientRequest, OperatorRequest, ProcessRequest, Request, Response},
    store::Store,
};

const CHANNEL_CAPACITY: usize = 1024;

pub(super) enum State {
    Init {
        store: Store<String>,

        requests_rx: Receiver<ProcessRequest>,
    },
    WaitingForOperator {
        store: Store<String>,

        requests_rx: Receiver<ProcessRequest>,
    },
    Ready {
        position: Position,

        store: Store<String>,
        pending: Vec<ProcessRequest>,

        ack_rx: Receiver<Response>,
        requests_rx: Receiver<ProcessRequest>,
        outgoing_tx: Sender<Packet>,

        outgoing: Outgoing,
    },
    Update {
        last_position: Position,
        new_position: Position,

        store: Store<String>,

        ack_rx: Receiver<Response>,
        requests_rx: Receiver<ProcessRequest>,
        outgoing_tx: Sender<Packet>,

        outgoing: Outgoing,
    },
    Transfer {
        store: Store<String>,

        ack_rx: Receiver<Response>,
        requests_rx: Receiver<ProcessRequest>,
        outgoing_tx: Sender<Packet>,

        outgoing: Outgoing,
    },
}

impl State {
    pub(super) fn new(store: Store<String>, requests_rx: Receiver<ProcessRequest>) -> Self {
        Self::Init { store, requests_rx }
    }

    pub(super) fn next(self) -> Self {
        match self {
            Self::Init { store, requests_rx } => {
                // TODO: any additional setup?
                Self::WaitingForOperator { store, requests_rx }
            }

            Self::WaitingForOperator { store, requests_rx } => {
                let mut pending = vec![];

                let chain = loop {
                    let request = requests_rx.recv().expect("recv");

                    if let Request::Operator(OperatorRequest::Chain(chain)) = request.request {
                        break chain;
                    } else if let Request::Client(_) = request.request {
                        pending.push(request);
                    } else {
                        panic!("invalid request type {:?}", request);
                    }
                };

                let position = chain.position().clone();

                let (outgoing_tx, outgoing_rx) = bounded(CHANNEL_CAPACITY);

                let outgoing_addr = match position.clone() {
                    Position::Head { next } => next,
                    Position::Middle { next } => next,
                    Position::Tail { frontend } => frontend,
                    Position::Candidate { candidate } => candidate,
                };

                let (ack_tx, ack_rx) = bounded(CHANNEL_CAPACITY);

                let outgoing = Outgoing::new(outgoing_addr, outgoing_rx, ack_tx);

                outgoing_tx
                    .send(Packet::ChainAck(chain.ack()))
                    .expect("send ack");

                Self::Ready {
                    position,

                    store,
                    pending,

                    ack_rx,
                    requests_rx,
                    outgoing_tx,

                    outgoing,
                }
            }

            Self::Ready {
                position,

                mut store,
                mut pending,

                ack_rx,
                requests_rx,
                outgoing_tx,

                outgoing,
            } => {
                for prequest in pending.drain(..) {
                    match prequest.request.clone() {
                        Request::Operator(request) => match request {
                            OperatorRequest::Chain(chain) => {
                                let new_position = chain.position().clone();
                                return Self::Update {
                                    last_position: position,
                                    new_position,
                                    store,
                                    ack_rx,
                                    requests_rx,
                                    outgoing_tx,
                                    outgoing,
                                };
                            }
                            OperatorRequest::Join(join) => todo!(),
                            OperatorRequest::Transfer(transfer) => {
                                let candidate = transfer.candidate();
                                let (outgoing_tx, outgoing_rx) = bounded(CHANNEL_CAPACITY);
                                let (ack_tx, ack_rx) = bounded(CHANNEL_CAPACITY);
                                let outgoing = Outgoing::new(candidate, outgoing_rx, ack_tx);
                                return Self::Transfer {
                                    store,
                                    ack_rx,
                                    requests_rx,
                                    outgoing_tx,
                                    outgoing,
                                };
                            }
                        },
                        Request::Client(request) => {
                            if let Position::Tail { .. } = position {
                                let mut buf = vec![0; 1024]; // TODO: handle buffer smarter later!

                                let response = store.commit_patch(request, &mut buf);
                                let response = Response::from(response);

                                prequest.complete(response);
                            } else {
                                store.add_to_pending(request.clone());
                                outgoing_tx.send(request.into()).expect("send");
                            }
                        }
                    }
                }

                Self::Ready {
                    position,
                    store,
                    pending,
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

                ack_rx,
                requests_rx,
                outgoing_tx,

                mut outgoing,
            } => {
                // TODO: handle role change here!
                // Outgoing loop should at least be paused?
                // If there are changes we might need to close the outgoing loop and start a new one.
                todo!()
            }

            Self::Transfer {
                store,
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
