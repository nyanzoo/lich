use std::thread;

use crossbeam::channel::bounded;
use log::info;

use config::FrontendConfig;
use io::incoming::Incoming;
use logger::init_logger;
use necronomicon::PoolImpl;

use crate::state::{Init, State};

mod state;

const CONFIG: &str = "/etc/lich/lich.toml";

enum BufferOwner {
    Join,
    OperatorFullDecode,
}

impl necronomicon::BufferOwner for BufferOwner {
    fn why(&self) -> &'static str {
        match self {
            BufferOwner::Join => "join",
            BufferOwner::OperatorFullDecode => "operator full decode",
        }
    }
}

fn main() {
    init_logger!();

    info!("starting lich(frontend) version 0.0.1");
    let contents = std::fs::read_to_string(CONFIG).expect("read config");
    let config = toml::from_str::<FrontendConfig>(&contents).expect("valid config");

    let (requests_tx, requests_rx) = bounded(1024);

    let incoming_pool = PoolImpl::new(
        config.incoming_pool.block_size,
        config.incoming_pool.capacity,
    );
    _ = thread::Builder::new()
        .name("incoming".to_string())
        .spawn(move || {
            Incoming::new(config.endpoints.port, requests_tx).run(incoming_pool);
        });

    let outgoing_pool = PoolImpl::new(
        config.outgoing_pool.block_size,
        config.outgoing_pool.capacity,
    );
    let mut state: Box<dyn State> = Init::init(config.endpoints, requests_rx, outgoing_pool) as _;

    loop {
        state = state.next();
    }
}
