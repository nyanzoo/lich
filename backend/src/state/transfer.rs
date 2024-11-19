use crossbeam::channel::{Receiver, Sender};
use log::{debug, info};

use config::EndpointConfig;
use io::outgoing::Outgoing;
use necronomicon::{system_codec::Position, ByteStr, Packet, Pool, PoolImpl, SharedImpl};
use phylactery::store::{Request, Response};
use requests::{ClientResponse, ProcessRequest};

use crate::{operator_connection::OperatorConnection, state::ready::Ready, BufferOwner};

use super::State;

pub struct Transfer {
    pub(crate) endpoint_config: EndpointConfig,

    pub(crate) store_tx: Sender<Request>,
    pub(crate) store_rx: Receiver<Response>,

    pub(crate) operator: OperatorConnection,

    pub(crate) ack_rx: Option<Receiver<ClientResponse<SharedImpl>>>,
    pub(crate) requests_rx: Receiver<ProcessRequest<SharedImpl>>,
    pub(crate) outgoing_tx: Sender<Packet<SharedImpl>>,

    pub(crate) outgoing: Option<Outgoing>,
    pub(crate) outgoing_pool: PoolImpl,
}

impl State for Transfer {
    fn next(self: Box<Self>) -> Box<dyn State> {
        let Self {
            endpoint_config,
            store_tx,
            store_rx,
            operator,
            ack_rx,
            requests_rx,
            outgoing_tx,
            outgoing,
            outgoing_pool,
        } = *self;

        debug!("transfering store and moving to ready");
        // TODO: handle transfer change here!
        if let Some(outgoing) = outgoing {
            info!("transfering store to {:?}", outgoing.addr);
            // TODO: if we don't want to nuke the store we will need to send versions!
            // Also we can skip asking the store and just recurse through the store folder
            // and send all the files to the new store. May need to be smart and send limited amount at a time.
            // for transfer in store.deconstruct_iter(outgoing_pool.clone()) {
            //     outgoing_tx
            //         .send(SystemPacket::Transfer(transfer).into())
            //         .expect("send");
            // }

            let mut owned = outgoing_pool.acquire("state", BufferOwner::Position);

            Box::new(Ready {
                endpoint_config,
                position: Position::Middle {
                    next: ByteStr::from_owned(&outgoing.addr, &mut owned).expect("valid addr"),
                },
                store_tx,
                store_rx,
                not_ready: Default::default(),
                pending: Default::default(),
                executing: Default::default(),
                operator,
                ack_rx,
                requests_rx,
                outgoing_tx,
                outgoing: Some(outgoing),
                outgoing_pool,
            })
        } else {
            Box::new(Ready {
                endpoint_config,
                position: Position::Tail { candidate: None },
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
}
