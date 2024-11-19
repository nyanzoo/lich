use crossbeam::channel::{bounded, Receiver, Sender};
use log::{debug, trace, warn};

use config::EndpointConfig;
use io::outgoing::Outgoing;
use necronomicon::{system_codec::Position, PoolImpl, SharedImpl};
use phylactery::store::{Request, Response};
use requests::{ProcessRequest, System};

use crate::{
    operator_connection::OperatorConnection,
    state::{init::Init, ready::Ready, CHANNEL_CAPACITY},
};

use super::State;

pub struct WaitingForOperator {
    pub(crate) endpoint_config: EndpointConfig,

    pub(crate) store_tx: Sender<Request>,
    pub(crate) store_rx: Receiver<Response>,

    pub(crate) operator: OperatorConnection,

    pub(crate) requests_rx: Receiver<ProcessRequest<SharedImpl>>,

    pub(crate) outgoing_pool: PoolImpl,
}

impl State for WaitingForOperator {
    fn next(self: Box<Self>) -> Box<dyn State> {
        let Self {
            endpoint_config,
            store_tx,
            store_rx,
            operator,
            requests_rx,
            outgoing_pool,
        } = *self;

        // Get the `Report` from operator.
        let operator_rx = operator.rx();
        let Ok(packet) = operator_rx.recv() else {
            return Box::new(Init {
                store_tx,
                store_rx,
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

        debug!("moving to ready");
        Box::new(Ready {
            endpoint_config,
            position,

            store_tx,
            store_rx,
            not_ready: Default::default(),
            pending: Default::default(),
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
