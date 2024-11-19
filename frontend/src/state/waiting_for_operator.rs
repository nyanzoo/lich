use crossbeam::channel::{bounded, Receiver};
use log::{debug, trace, warn};

use config::EndpointConfig;
use io::outgoing::Outgoing;
use necronomicon::{system_codec::Position, PoolImpl, SharedImpl};
use requests::{ProcessRequest, System};

use crate::{
    operator_connection::OperatorConnection,
    state::{ready::Ready, CHANNEL_CAPACITY},
};

use super::State;

pub struct WaitingForOperator {
    pub(crate) endpoint_config: EndpointConfig,
    pub(crate) operator: OperatorConnection,

    pub(crate) requests_rx: Receiver<ProcessRequest<SharedImpl>>,

    pub(crate) outgoing_pool: PoolImpl,
}

impl State for WaitingForOperator {
    fn next(self: Box<Self>) -> Box<dyn State> {
        let WaitingForOperator {
            endpoint_config,
            operator,
            requests_rx,
            outgoing_pool,
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
            trace!("creating outgoing to {:?}", outgoing_addr);
            match Outgoing::new(
                outgoing_addr.clone().as_str().expect("valid addr"),
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

        debug!("moving to ready");
        Box::new(Ready {
            endpoint_config,
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

            outgoing_pool,
        })
    }
}
