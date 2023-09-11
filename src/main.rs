// mod acks;
mod config;
mod error;

#[cfg(feature = "backend")]
mod incoming;

#[cfg(feature = "operator")]
mod operator;

mod outgoing;
mod reqres;
mod session;

#[cfg(feature = "backend")]
mod state;

mod store;
mod util;

#[cfg(feature = "operator")]
fn main() {
    use std::net::{IpAddr, Ipv6Addr, SocketAddr, TcpListener};

    use log::{error, info, trace};
    use rayon::ThreadPoolBuilder;

    use necronomicon::full_decode;

    use crate::{operator::Cluster, reqres::System, session::Session};

    pretty_env_logger::init();

    info!("starting lich(operator) version 0.0.1");
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 10000))
        .expect("listener");
    let pool = ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .expect("thread pool");

    let cluster = Cluster::default();

    for stream in listener.incoming() {
        pool.install(|| match stream {
            Ok(stream) => {
                let session = Session::new(stream, 5);
                let mut read_session = session.clone();
                loop {
                    match full_decode(&mut read_session) {
                        Ok(packet) => {
                            let request: System = packet.into();

                            match request {
                                System::Join(join) => {
                                    trace!("join: {:?}", join);
                                    if let Err(err) = cluster.add(session.clone(), join) {
                                        error!("cluster.add: {}", err);
                                        continue;
                                    }
                                }
                                System::ReportAck(ack) => {
                                    trace!("report ack: {:?}", ack);
                                    // TODO: remove session if ack is not ok!
                                }
                                _ => {
                                    error!("expected join request but got {:?}", request);
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
fn main() {
    use std::thread;

    use crossbeam::channel::bounded;
    use log::info;

    use crate::{config::BackendConfig, incoming::Incoming, state::State, store::Store};

    pretty_env_logger::init();

    // let (pusher, popper) =
    //     ring_buffer(buffer::InMemBuffer::new(util::gigabytes(1)), Version::V1).expect("dequeue");
    info!("starting lich(backend) version 0.0.1");
    let contents = std::fs::read_to_string("./lich/tests/backend.toml").expect("read config");
    let config = toml::from_str::<BackendConfig>(&contents).expect("valid config");

    let store = Store::new(&config.store).expect("store");

    let (requests_tx, requests_rx) = bounded(1024);

    _ = thread::spawn(|| {
        Incoming::new(9999, requests_tx).run();
    });

    let mut state = State::new(config.endpoints, store, requests_rx);

    loop {
        state = state.next();
    }
}
