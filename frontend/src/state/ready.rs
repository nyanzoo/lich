use std::collections::HashMap;

use crossbeam::channel::{Receiver, Select, Sender};
use log::{trace, warn};

use config::EndpointConfig;
use io::outgoing::Outgoing;
use necronomicon::{system_codec::Position, Ack, Packet, PoolImpl, SharedImpl, SystemPacket};
use requests::{ClientResponse, PendingRequest, ProcessRequest, System};

use crate::{
    state::{init::Init, update::Update, waiting_for_operator::WaitingForOperator},
    OperatorConnection,
};

use super::State;

pub struct Ready {
    pub(crate) endpoint_config: EndpointConfig,
    pub(crate) position: Position<SharedImpl>,

    pub(crate) pending: HashMap<necronomicon::Uuid, PendingRequest<SharedImpl>>,

    pub(crate) operator: OperatorConnection,

    pub(crate) head_ack_rx: Option<Receiver<ClientResponse<SharedImpl>>>,
    pub(crate) tail_ack_rx: Option<Receiver<ClientResponse<SharedImpl>>>,

    pub(crate) requests_rx: Receiver<ProcessRequest<SharedImpl>>,
    pub(crate) head_outgoing_tx: Sender<Packet<SharedImpl>>,
    pub(crate) tail_outgoing_tx: Sender<Packet<SharedImpl>>,

    pub(crate) head: Option<Outgoing>,
    pub(crate) tail: Option<Outgoing>,

    pub(crate) outgoing_pool: PoolImpl,
}

impl State for Ready {
    fn next(self: Box<Self>) -> Box<dyn State> {
        let Ready {
            endpoint_config,
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

            outgoing_pool,
        } = *self;

        let mut sel = Select::new();
        let operator_tx = operator.tx();
        let operator_rx = operator.rx();
        let operator_id = sel.recv(&operator_rx);

        trace!("get rx ids");
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
            trace!("waiting for client request or ack");
            let request_rx_id = sel.recv(&requests_rx);
            let oper = sel.select();
            match oper.index() {
                i if i == operator_id => {
                    trace!("operator message");
                    let Ok(system) = oper.recv(&operator_rx) else {
                        return Box::new(Init {
                            endpoint_config,
                            requests_rx,
                            outgoing_pool,
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

                            trace!("moving to update");
                            return Box::new(Update {
                                endpoint_config,
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
                                outgoing_pool,
                            });
                        }
                        _ => {
                            warn!("expected valid system message but got {:?}", system);
                        }
                    }
                }
                i if i == head_ack_rx_id => {
                    trace!("head ack");
                    if let Some(ack_rx) = head_ack_rx.as_ref() {
                        let response = oper.recv(ack_rx).expect("recv");
                        trace!("got ack: {:?}", response);

                        let id = response.header().uuid;
                        if let Some(request) = pending.remove(&id) {
                            request.complete(response);
                        } else {
                            panic!("got ack for unknown request");
                        }
                    }
                }
                i if i == tail_ack_rx_id => {
                    trace!("tail ack");
                    if let Some(ack_rx) = tail_ack_rx.as_ref() {
                        let response = oper.recv(ack_rx).expect("recv");
                        trace!("got ack: {:?}", response);

                        let id = response.header().uuid;
                        if let Some(request) = pending.remove(&id) {
                            request.complete(response);
                        } else {
                            panic!("got ack for unknown request");
                        }
                    }
                }
                i if i == request_rx_id => {
                    trace!("got request");
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
                                        endpoint_config,
                                        operator,
                                        requests_rx,
                                        outgoing_pool,
                                    });
                                }

                                pending.insert(id, pending_request);
                            } else if request.is_for_tail() {
                                trace!("sending request {request:?} to next node");
                                if tail_outgoing_tx.send(request.into()).is_err() {
                                    warn!("failed to send request to next node");
                                    return Box::new(WaitingForOperator {
                                        endpoint_config,
                                        operator,
                                        requests_rx,
                                        outgoing_pool,
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
            endpoint_config,
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
            outgoing_pool,
        })
    }
}
