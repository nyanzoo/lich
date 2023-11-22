use std::thread;

use crossbeam::channel::bounded;
use log::info;

use config::FrontendConfig;
use io::incoming::Incoming;

use crate::state::{Init, State};

mod state;

const CONFIG: &str = "/etc/lich/lich.toml";

fn main() {
    env_logger::init();

    info!("starting lich(frontend) version 0.0.1");
    let contents = std::fs::read_to_string(CONFIG).expect("read config");
    let config = toml::from_str::<FrontendConfig>(&contents).expect("valid config");

    let (requests_tx, requests_rx) = bounded(1024);

    _ = thread::Builder::new()
        .name("incoming".to_string())
        .spawn(move || {
            Incoming::new(config.endpoints.port, requests_tx).run();
        });

    let mut state: Box<dyn State> = Init::init(config.endpoints, requests_rx) as _;

    loop {
        state = state.next();
    }
}
