use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use log::{debug, error, info, trace};
use rayon::ThreadPoolBuilder;

use config::OperatorConfig;
use net::{session::Session, stream::TcpListener};
use requests::System;
use necronomicon::full_decode;

use crate::operator::Cluster;

mod chain;
mod error;
mod operator;

const CONFIG: &str = "/etc/lich/lich.toml";

fn main() {
    env_logger::init();

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
                let (mut read_session, session_writer) = session.split();
                loop {
                    match full_decode(&mut read_session) {
                        Ok(packet) => {
                            let request: System = packet.into();

                            match request {
                                System::Join(join) => {
                                    info!("join: {:?}", join);
                                    if let Err(err) = cluster.add(session_writer.clone(), join) {
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
