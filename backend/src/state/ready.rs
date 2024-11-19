use std::collections::BTreeMap;

use crossbeam::channel::{Receiver, Select, Sender};
use log::{debug, trace, warn};

use config::EndpointConfig;
use io::outgoing::Outgoing;
use necronomicon::{system_codec::Position, Ack, Packet, PoolImpl, SharedImpl, SystemPacket};
use phylactery::store::{Request, Response};
use requests::{ClientResponse, ProcessRequest, System};

use crate::{
    operator_connection::OperatorConnection,
    state::{init::Init, update::Update, waiting_for_operator::WaitingForOperator, TIMEOUT},
};

use super::State;

pub struct Ready {
    pub(crate) endpoint_config: EndpointConfig,

    pub(crate) position: Position<SharedImpl>,

    pub(crate) store_tx: Sender<Request>,
    pub(crate) store_rx: Receiver<Response>,
    pub(crate) not_ready: Vec<ProcessRequest<SharedImpl>>,
    pub(crate) pending: BTreeMap<necronomicon::Uuid, ProcessRequest<SharedImpl>>,
    pub(crate) executing: BTreeMap<necronomicon::Uuid, ProcessRequest<SharedImpl>>,

    pub(crate) operator: OperatorConnection,

    pub(crate) ack_rx: Option<Receiver<ClientResponse<SharedImpl>>>,
    pub(crate) requests_rx: Receiver<ProcessRequest<SharedImpl>>,
    pub(crate) outgoing_tx: Sender<Packet<SharedImpl>>,

    pub(crate) outgoing: Option<Outgoing>,

    pub(crate) outgoing_pool: PoolImpl,
}

impl State for Ready {
    fn next(self: Box<Self>) -> Box<dyn State> {
        let Self {
            endpoint_config,
            position,
            store_tx,
            store_rx,
            mut not_ready,
            mut pending,
            mut executing,
            operator,
            ack_rx,
            requests_rx,
            outgoing_tx,
            outgoing,
            outgoing_pool,
        } = *self;

        debug!("ready, processing not ready requests {}", not_ready.len());
        for prequest in not_ready.drain(..) {
            if let Position::Tail { .. } = position {
                let request = prequest.request.clone();
                executing.insert(prequest.request.id(), prequest);
                store_tx.send(request.into()).expect("send to store");
            } else {
                let id = prequest.request.id();
                outgoing_tx
                    .send(prequest.request.clone().into())
                    .expect("send");

                pending.insert(id, prequest);
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
        let store_rx_clone = store_rx.clone();
        let store_rx_id = sel.recv(&store_rx_clone);

        trace!("waiting for client request or ack");
        let oper = sel.select_timeout(TIMEOUT);

        if oper.is_err() {
            trace!("timed out waiting for requests, going back to ready");
            return Box::new(Ready {
                endpoint_config,
                position,
                store_tx,
                store_rx,
                pending,
                executing,
                not_ready,
                operator,
                ack_rx: ack_rx_clone,
                requests_rx: requests_rx.clone(),
                outgoing_tx,
                outgoing,
                outgoing_pool,
            });
        }

        let oper = oper.expect("operation is okay");
        match oper.index() {
            i if i == operator_id => {
                trace!("got operator message");
                let Ok(system) = oper.recv(&operator_rx) else {
                    return Box::new(Init {
                        store_tx,
                        store_rx,
                        requests_rx,
                        outgoing_pool,
                        endpoint_config,
                    });
                };
                trace!("got system packet: {:?}", system);
                match system {
                    System::Ping(ping) => {
                        operator_tx
                            .send(System::from(SystemPacket::PingAck(ping.ack())))
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
                            store_tx,
                            store_rx,
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
                            store_tx,
                            store_rx,
                            operator,
                            requests_rx,
                            outgoing_pool,
                        });
                    };

                    trace!("got ack: {:?}", response);

                    let id = response.header().uuid;
                    if let Some(prequest) = pending.remove(&id) {
                        let request = prequest.request.clone();
                        executing.insert(id, prequest);
                        store_tx.send(request.into()).expect("send to store");
                    } else {
                        panic!("got ack for unknown request");
                    }
                }
            }
            i if i == request_rx_id => {
                trace!("got client request");
                let Ok(prequest) = oper.recv(&requests_rx) else {
                    return Box::new(WaitingForOperator {
                        endpoint_config,
                        store_tx,
                        store_rx,
                        operator,
                        requests_rx,
                        outgoing_pool,
                    });
                };
                trace!("got request: {:?}", prequest);

                match position {
                    Position::Candidate => {
                        unimplemented!("candidate receiving store is not implemented yet");
                        // let (request, pending) = prequest.into_parts();

                        // debug!("got candidate request: {:?}", request);

                        // let ClientRequest::Transfer(transfer) = request else {
                        //     panic!("expected transfer request but got {:?}", request);
                        // };

                        // store.reconstruct(&transfer);
                        // pending.complete(ClientResponse::Transfer(transfer.ack()));
                    }
                    Position::Tail { .. } => {
                        let request = prequest.request.clone();
                        trace!("committing patch {request:?}, since we are tail");
                        executing.insert(request.id(), prequest);
                        store_tx.send(request.into()).expect("send to store");
                    }
                    _ => {
                        let request = prequest.request.clone();
                        let id = request.id();
                        pending.insert(id, prequest);

                        trace!("sending request {request:?} to next node");
                        if outgoing_tx.send(request.into()).is_err() {
                            warn!("failed to send request to next node");
                            return Box::new(WaitingForOperator {
                                endpoint_config,
                                store_tx,
                                store_rx,
                                operator,
                                requests_rx,
                                outgoing_pool,
                            });
                        }
                    }
                }
            }
            i if i == store_rx_id => {
                trace!("get store response");
                let response = oper.recv(&store_rx_clone).expect("store should not crash");
                let id = response.header().uuid;
                let Some(request) = executing.remove(&id) else {
                    panic!("request '{:?}' should exist", id);
                };
                trace!("got store response '{:?}'", response);
                let (_client_request, pending_request) = request.into_parts();
                match response {
                    Response::Create(response) => {
                        pending_request.complete(ClientResponse::CreateQueue(response));
                    }
                    Response::Remove(response) => {
                        pending_request.complete(ClientResponse::DeleteQueue(response));
                    }
                    Response::Push(response) => {
                        pending_request.complete(ClientResponse::Enqueue(response));
                    }
                    Response::Pop(response) => {
                        pending_request.complete(ClientResponse::Dequeue(response));
                    }
                    Response::Peek(response) => {
                        pending_request.complete(ClientResponse::Peek(response));
                    }
                    Response::Delete(response) => {
                        pending_request.complete(ClientResponse::Delete(response));
                    }
                    Response::Get(response) => {
                        pending_request.complete(ClientResponse::Get(response));
                    }
                    Response::Put(response) => {
                        pending_request.complete(ClientResponse::Put(response));
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
            store_tx,
            store_rx,
            pending,
            executing,
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
