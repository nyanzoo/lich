use std::thread;

use crossbeam::channel::bounded;
use log::{info, trace};

use config::BackendConfig;
use io::incoming::Incoming;
use necronomicon::PoolImpl;

use crate::{state::Init, store::Store};

mod error;
mod state;
mod store;

const CONFIG: &str = "/etc/lich/lich.toml";

// INCOMING:
// Incoming will need to have tx+rx for requests and acks.
// Incoming will read in a request and send it over to state for processing.
//
// STATE:
// If state is tail then it will commit the change to the store, perform any actions and send back ack to incoming
// If state is head/mid then it will need to store the change in pending and send to next node via Outgoing. It will need to get an
// ack back from next node and then commit that change and send ack back to Incoming.
// If state is candidate then it will need to read out transaction log and send to next node via Outgoing.
//
// OUTGOING:
// Outgoing will need to have tx+rx for requests and acks. It sends out operator msgs to operator. The rest to next node.
// Need to read out responses and send acks back to STATE.
fn main() {
    env_logger::init();

    info!("starting lich(backend) version 0.0.1");
    let contents = std::fs::read_to_string(CONFIG).expect("read config");
    let config = toml::from_str::<BackendConfig>(&contents).expect("valid config");

    let incoming_pool = PoolImpl::new(
        config.incoming_pool.block_size,
        config.incoming_pool.capacity,
    );
    // We only need a pool temporarily for the store to initialize.
    let store = Store::new(config.store, incoming_pool.clone()).expect("store");

    let (requests_tx, requests_rx) = bounded(1024);

    trace!("starting incoming");
    _ = thread::Builder::new()
        .name("incoming".to_string())
        .spawn(move || {
            Incoming::new(config.endpoints.port, requests_tx).run(incoming_pool);
        });

    let outgoing_pool = PoolImpl::new(
        config.outgoing_pool.block_size,
        config.outgoing_pool.capacity,
    );
    let mut state = Init::init(config.endpoints, store, requests_rx, outgoing_pool);

    trace!("starting state");
    loop {
        state = state.next();
    }
}
