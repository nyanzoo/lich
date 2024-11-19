use std::{path::PathBuf, thread};

use crossbeam::channel::bounded;
use log::{info, trace};

use config::BackendConfig;
use io::incoming::Incoming;
use logger::init_logger;
use necronomicon::PoolImpl;
use phylactery::store::Store;

use crate::state::init::Init;

mod error;
mod operator_connection;
mod state;
// mod store;

#[cfg(feature = "dhat_heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const CONFIG: &str = "/etc/lich/lich.toml";

#[derive(Clone, Copy, Debug)]
enum BufferOwner {
    Join,
    OperatorFullDecode,
    Position,
}

impl necronomicon::BufferOwner for BufferOwner {
    fn why(&self) -> &'static str {
        match self {
            BufferOwner::Join => "join",
            BufferOwner::OperatorFullDecode => "operator full decode",
            BufferOwner::Position => "position",
        }
    }
}

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
    #[cfg(feature = "dhat_heap")]
    let profiler = profiler();
    #[cfg(feature = "dhat_heap")]
    let now = std::time::Instant::now();

    init_logger!();

    info!("starting lich(backend) version 0.0.1");
    let contents = std::fs::read_to_string(CONFIG).expect("read config");
    let config = toml::from_str::<BackendConfig>(&contents).expect("valid config");

    let incoming_pool = PoolImpl::new(
        config.incoming_pool.block_size.to_bytes() as usize,
        config.incoming_pool.capacity,
    );
    // We only need a pool temporarily for the store to initialize.
    let (request_tx, request_rx) = bounded(1024);
    let (response_tx, response_rx) = bounded(1024);
    let configs = config
        .stores
        .iter()
        .cloned()
        .map(|mut config| {
            #[cfg(test)]
            let hostname = "test";
            #[cfg(not(test))]
            let hostname = std::env::var("HOSTNAME").expect("hostname");
            let mut path = PathBuf::from(config.dir);
            path.push(hostname);
            config.dir = path.to_str().expect("valid path").to_owned();
            config
        })
        .collect::<Vec<_>>();
    let store = Store::new(configs, request_rx, response_tx, incoming_pool.clone()).expect("store");
    trace!("store starting");
    _ = thread::Builder::new()
        .name("store".to_string())
        .spawn(move || store.run());

    let (requests_tx, requests_rx) = bounded(1024);

    trace!("starting incoming");
    _ = thread::Builder::new()
        .name("incoming".to_string())
        .spawn(move || {
            Incoming::new(config.endpoints.port, requests_tx).run(incoming_pool);
        });

    let outgoing_pool = PoolImpl::new(
        config.outgoing_pool.block_size.to_bytes() as usize,
        config.outgoing_pool.capacity,
    );
    let mut state = Init::init(
        config.endpoints,
        request_tx,
        response_rx,
        requests_rx,
        outgoing_pool,
    );

    #[cfg(feature = "dhat_heap")]
    let _ = std::thread::spawn(move || loop {
        let elapsed = now.elapsed();
        log::trace!("elapsed: {}", elapsed.as_secs());
        if elapsed.as_secs() > 100 {
            log::error!("collecting dhat profile");
            drop(profiler);
            return;
        }
    });

    trace!("starting state");
    loop {
        state = state.next();
    }
}

#[cfg(feature = "dhat_heap")]
fn profiler() -> dhat::Profiler {
    let hostname = std::env::var("HOSTNAME").expect("hostname");
    let path = format!("/opt/lich/dhat/{}", hostname);
    std::fs::create_dir_all(&path).expect("create dhat dir");
    let file_name = format!("{path}/backend.json");
    let profiler = dhat::Profiler::builder().file_name(file_name).build();
    profiler
}
