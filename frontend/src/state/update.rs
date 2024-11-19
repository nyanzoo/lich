use std::collections::HashMap;

use crossbeam::channel::{bounded, Receiver, Sender};
use log::{info, trace, warn};

use config::EndpointConfig;
use io::outgoing::Outgoing;
use necronomicon::{system_codec::Position, Packet, PoolImpl, SharedImpl};
use requests::{ClientResponse, PendingRequest, ProcessRequest};

use crate::{
    state::{ready::Ready, waiting_for_operator::WaitingForOperator, CHANNEL_CAPACITY},
    OperatorConnection,
};

use super::State;

pub struct Update {
    pub(crate) endpoint_config: EndpointConfig,
    pub(crate) last_position: Position<SharedImpl>,
    pub(crate) new_position: Position<SharedImpl>,

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

impl State for Update {
    fn next(self: Box<Self>) -> Box<dyn State> {
        let Update {
            endpoint_config,
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
            outgoing_pool,
        } = *self;

        if last_position == new_position {
            trace!("no change in position");
            Box::new(Ready {
                endpoint_config,
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
                outgoing_pool,
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
                trace!("creating outgoing to {:?}", outgoing_addr);

                match Outgoing::new(
                    outgoing_addr.as_str().expect("valid addr"),
                    head_outgoing_rx,
                    ack_tx,
                    outgoing_pool.clone(),
                ) {
                    Ok(outgoing) => (Some(outgoing), Some(ack_rx)),
                    Err(err) => {
                        warn!("failed to create outgoing to {:?}: {}", outgoing_addr, err);
                        return Box::new(WaitingForOperator {
                            endpoint_config,
                            operator,
                            requests_rx,
                            outgoing_pool,
                        });
                    }
                }
            } else {
                (None, None)
            };

            let (tail, tail_ack_rx) = if let Some(outgoing_addr) = tail_addr {
                let (ack_tx, ack_rx) = bounded(CHANNEL_CAPACITY);
                trace!("creating outgoing to {:?}", outgoing_addr);

                match Outgoing::new(
                    outgoing_addr.as_str().expect("valid addr"),
                    tail_outgoing_rx,
                    ack_tx,
                    outgoing_pool.clone(),
                ) {
                    Ok(outgoing) => (Some(outgoing), Some(ack_rx)),
                    Err(err) => {
                        warn!("failed to create outgoing to {:?}: {}", outgoing_addr, err);
                        return Box::new(WaitingForOperator {
                            endpoint_config,
                            operator,
                            requests_rx,
                            outgoing_pool,
                        });
                    }
                }
            } else {
                (None, None)
            };

            trace!("moving to ready");
            Box::new(Ready {
                endpoint_config,
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
                outgoing_pool,
            })
        }
    }
}
