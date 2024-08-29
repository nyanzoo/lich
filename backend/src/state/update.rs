use std::collections::BTreeMap;

use crossbeam::channel::{bounded, Receiver, Sender};
use log::{debug, info, trace, warn};

use config::EndpointConfig;
use io::outgoing::Outgoing;
use necronomicon::{system_codec::Position, Packet, PoolImpl, SharedImpl};
use phylactery::store::{Request, Response};
use requests::{ClientResponse, ProcessRequest};

use crate::{
    operator_connection::OperatorConnection,
    state::{transfer::Transfer, waiting_for_operator::WaitingForOperator, CHANNEL_CAPACITY},
};

use super::{ready::Ready, State};

pub struct Update {
    pub(crate) endpoint_config: EndpointConfig,

    pub(crate) last_position: Position<SharedImpl>,
    pub(crate) new_position: Position<SharedImpl>,

    pub(crate) store_tx: Sender<Request>,
    pub(crate) store_rx: Receiver<Response>,
    pub(crate) pending: BTreeMap<necronomicon::Uuid, ProcessRequest<SharedImpl>>,

    pub(crate) operator: OperatorConnection,

    pub(crate) ack_rx: Option<Receiver<ClientResponse<SharedImpl>>>,
    pub(crate) requests_rx: Receiver<ProcessRequest<SharedImpl>>,
    pub(crate) outgoing_tx: Sender<Packet<SharedImpl>>,

    pub(crate) outgoing: Option<Outgoing>,
    pub(crate) outgoing_pool: PoolImpl,
}

impl State for Update {
    fn next(self: Box<Self>) -> Box<dyn State> {
        let Self {
            endpoint_config,
            last_position,
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
        } = *self;

        debug!("updating");

        if last_position == new_position {
            Box::new(Ready {
                endpoint_config,
                position: new_position,
                store_tx,
                store_rx,
                not_ready: Default::default(),
                pending,
                executing: Default::default(),
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
                            store_tx,
                            store_rx,
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
                debug!("moving to transfer");
                Box::new(Transfer {
                    endpoint_config,
                    store_tx,
                    store_rx,
                    operator,
                    ack_rx,
                    requests_rx,
                    outgoing_tx,
                    outgoing,
                    outgoing_pool,
                })
            } else {
                debug!("moving to ready");
                // TODO: handle role change here!
                // Outgoing loop should at least be paused?
                // If there are changes we might need to close the outgoing loop and start a new one.
                Box::new(Ready {
                    endpoint_config,
                    position: new_position,
                    store_tx,
                    store_rx,
                    not_ready: Default::default(),
                    pending,
                    executing: Default::default(),
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
