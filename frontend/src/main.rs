use std::{panic, process, thread};

use crossbeam::channel::bounded;
use log::info;

use config::FrontendConfig;
use io::incoming::Incoming;
use logger::init_logger;
use necronomicon::PoolImpl;

use crate::state::{init::Init, State};

mod operator_connection;
pub(crate) use operator_connection::OperatorConnection;
mod state;

const CONFIG: &str = "/etc/lich/lich.toml";

#[derive(Clone, Copy, Debug)]
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
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        process::exit(1);
    }));

    init_logger!();

    info!("starting lich(frontend) version 0.0.1");
    let contents = std::fs::read_to_string(CONFIG).expect("read config");
    let config = toml::from_str::<FrontendConfig>(&contents).expect("valid config");

    let (requests_tx, requests_rx) = bounded(1024);

    let incoming_pool = PoolImpl::new(
        config.incoming_pool.block_size.to_bytes() as usize,
        config.incoming_pool.capacity,
    );
    _ = thread::Builder::new()
        .name("incoming".to_string())
        .spawn(move || {
            Incoming::new(config.endpoints.port, requests_tx).run(incoming_pool);
        });

    let outgoing_pool = PoolImpl::new(
        config.outgoing_pool.block_size.to_bytes() as usize,
        config.outgoing_pool.capacity,
    );
    let mut state: Box<dyn State> = Init::init(config.endpoints, requests_rx, outgoing_pool) as _;

    loop {
        state = state.next();
    }
}
