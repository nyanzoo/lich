use crossbeam::channel::Receiver;
use log::{debug, info};

use config::EndpointConfig;
use necronomicon::{PoolImpl, SharedImpl};
use requests::ProcessRequest;

use crate::{
    operator_connection::OperatorConnection, state::waiting_for_operator::WaitingForOperator,
};

use super::State;

pub struct Init {
    pub(crate) endpoint_config: EndpointConfig,

    pub(crate) requests_rx: Receiver<ProcessRequest<SharedImpl>>,

    pub(crate) outgoing_pool: PoolImpl,
}

impl Init {
    pub fn init(
        endpoint_config: EndpointConfig,
        requests_rx: Receiver<ProcessRequest<SharedImpl>>,
        outgoing_pool: PoolImpl,
    ) -> Box<dyn State> {
        info!("starting state machine");
        Box::new(Init {
            endpoint_config,
            requests_rx,
            outgoing_pool,
        }) as _
    }
}

impl State for Init {
    fn next(self: Box<Self>) -> Box<dyn State> {
        debug!("initialized and moving to waiting for operator");

        let Init {
            endpoint_config,
            requests_rx,
            outgoing_pool,
        } = *self;

        let operator = OperatorConnection::connect(
            endpoint_config.operator_addr.clone(),
            endpoint_config.port,
        );

        Box::new(WaitingForOperator {
            endpoint_config,
            operator,
            requests_rx,
            outgoing_pool,
        })
    }
}
