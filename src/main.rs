mod common;
mod config;
mod error;

#[cfg(feature = "controller")]
mod controller;

#[cfg(feature = "operator")]
mod operator;

#[cfg(feature = "backend")]
mod backend;

#[cfg(feature = "frontend")]
mod frontend;

#[cfg(any(feature = "backend", feature = "frontend", feature = "operator"))]
const CONFIG: &str = "/etc/lich/lich.toml";

#[cfg(feature = "controller")]
fn controller() {}

#[cfg(feature = "operator")]
fn operator() {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

    use log::{debug, error, info, trace};
    use rayon::ThreadPoolBuilder;

    use necronomicon::full_decode;

    use crate::{
        common::{reqres::System, session::Session, stream::TcpListener},
        config::OperatorConfig,
        operator::Cluster,
    };

    pretty_env_logger::init();

    info!("starting lich(operator) version 0.0.1");
    let contents = std::fs::read_to_string(CONFIG).expect("read config");
    let config = toml::from_str::<OperatorConfig>(&contents).expect("valid config");

    debug!("starting operator on port {}", config.port);
    let listener = TcpListener::bind(
        &[
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.port),
            SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), config.port),
        ][..],
    )
    .expect("listener");
    let pool = ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .expect("thread pool");

    let cluster = Cluster::default();

    trace!("listening");
    for stream in listener.incoming() {
        trace!("incoming");
        let cluster = cluster.clone();
        pool.spawn(move || match stream {
            Ok(stream) => {
                let session = Session::new(stream, 5);
                info!("new session {:?}", session);
                let mut read_session = session.clone();
                loop {
                    match full_decode(&mut read_session) {
                        Ok(packet) => {
                            let request: System = packet.into();

                            match request {
                                System::Join(join) => {
                                    info!("join: {:?}", join);
                                    if let Err(err) = cluster.add(session.clone(), join) {
                                        error!("cluster.add: {}", err);
                                        continue;
                                    }
                                }
                                System::ReportAck(ack) => {
                                    info!("report ack: {:?}", ack);
                                    // TODO: remove session if ack is not ok!
                                }
                                _ => {
                                    error!(
                                        "expected join/report_ack request but got {:?}",
                                        request
                                    );
                                    continue;
                                }
                            }
                        }
                        Err(err) => {
                            error!("full_decode: {}", err);
                            break;
                        }
                    }
                }
            }
            Err(err) => error!("listener.accept: {}", err),
        });
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
#[cfg(feature = "backend")]
fn backend() {
    use std::thread;

    use crossbeam::channel::bounded;
    use log::info;

    use crate::{
        backend::{incoming::Incoming, state::Init, store::Store},
        config::BackendConfig,
    };

    pretty_env_logger::init();

    info!("starting lich(backend) version 0.0.1");
    let contents = std::fs::read_to_string(CONFIG).expect("read config");
    let config = toml::from_str::<BackendConfig>(&contents).expect("valid config");

    let store = Store::new(&config.store).expect("store");

    let (requests_tx, requests_rx) = bounded(1024);

    _ = thread::Builder::new()
        .name("incoming".to_string())
        .spawn(move || {
            Incoming::new(config.endpoints.port, requests_tx).run();
        });

    let mut state = Init::init(config.endpoints, store, requests_rx);

    loop {
        state = state.next();
    }
}

#[cfg(feature = "frontend")]
fn frontend() {
    use std::thread;

    use crossbeam::channel::bounded;
    use log::info;

    use crate::{
        config::FrontendConfig,
        frontend::{
            incoming::Incoming,
            state::{Init, State},
        },
    };

    pretty_env_logger::init();

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

fn main() {
    #[cfg(feature = "controller")]
    controller();

    #[cfg(feature = "backend")]
    backend();

    #[cfg(feature = "operator")]
    operator();

    #[cfg(feature = "frontend")]
    frontend();

    log::info!("exiting");
}
