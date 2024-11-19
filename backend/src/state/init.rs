use crossbeam::channel::{Receiver, Sender};
use log::debug;

use config::EndpointConfig;
use necronomicon::{PoolImpl, SharedImpl};
use phylactery::store::{Request, Response};
use requests::ProcessRequest;

use crate::{
    operator_connection::OperatorConnection, state::waiting_for_operator::WaitingForOperator,
};

use super::State;

pub struct Init {
    pub(crate) endpoint_config: EndpointConfig,
    pub(crate) store_tx: Sender<Request>,
    pub(crate) store_rx: Receiver<Response>,
    pub(crate) requests_rx: Receiver<ProcessRequest<SharedImpl>>,
    pub(crate) outgoing_pool: PoolImpl,
}

impl Init {
    pub fn init(
        endpoint_config: EndpointConfig,
        store_tx: Sender<Request>,
        store_rx: Receiver<Response>,
        requests_rx: Receiver<ProcessRequest<SharedImpl>>,
        outgoing_pool: PoolImpl,
    ) -> Box<dyn State> {
        Box::new(Self {
            endpoint_config,
            store_tx,
            store_rx,
            requests_rx,
            outgoing_pool,
        }) as _
    }
}

impl State for Init {
    fn next(self: Box<Self>) -> Box<dyn State> {
        let Self {
            endpoint_config,
            store_tx,
            store_rx,
            requests_rx,
            outgoing_pool,
        } = *self;

        debug!("initialized and moving to waiting for operator");

        // TODO: should check the config map value for the version.
        // also should check the version of the store.
        let version = 1; // store.version();

        let operator = OperatorConnection::connect(
            endpoint_config.operator_addr.clone(),
            endpoint_config.port,
            version,
        );

        Box::new(WaitingForOperator {
            endpoint_config,
            store_tx,
            store_rx,
            operator,
            requests_rx,
            outgoing_pool,
        })
    }
}
