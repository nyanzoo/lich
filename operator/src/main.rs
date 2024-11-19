use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use log::{debug, error, info, trace};
use rayon::ThreadPoolBuilder;

use config::OperatorConfig;
use io::{decode_packet_on_reader_and, wait_for_join};
use logger::init_logger;
use necronomicon::{PoolImpl, SharedImpl};
use net::{session::Session, stream::TcpListener};
use requests::System;

use crate::operator::Cluster;

mod chain;
mod error;
mod operator;

const CONFIG: &str = "/etc/lich/lich.toml";

fn main() {
    init_logger!();

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
    let thread_pool = ThreadPoolBuilder::new()
        .num_threads(4)
        .thread_name(|x| format!("operator-{}", x))
        .build()
        .expect("thread pool");

    let cluster = Cluster::default();

    trace!("listening");
    for stream in listener.incoming() {
        trace!("incoming");
        let cluster = cluster.clone();
        match stream {
            Ok(stream) => {
                let session = Session::new(stream, 5);
                info!("new session {:?}", session);
                let (mut read_session, session_writer) = session.split();
                let pool = PoolImpl::new(1024, 1024);

                match wait_for_join(&mut read_session, &pool) {
                    Ok(join) => {
                        info!("join: {:?}", join);
                        if let Err(err) = cluster.add(session_writer, join) {
                            error!("cluster.add: {}", err);
                            continue;
                        }
                        thread_pool.spawn(move || {
                            decode_packet_on_reader_and(&mut read_session, &pool, |packet| {
                                let request: System<SharedImpl> = packet.into();

                                match request {
                                    System::ReportAck(ack) => {
                                        info!("report ack: {:?}", ack);
                                    }
                                    _ => {
                                        error!("expected report_ack request but got {:?}", request);
                                        return false;
                                    }
                                }

                                true
                            });
                        });
                    }
                    Err(err) => error!("wait_for_join: {}", err),
                }
            }
            Err(err) => error!("listener.accept: {}", err),
        }
    }
}
