use std::{
    io::{Read, Write},
    mem::size_of,
    net::{IpAddr, Ipv6Addr, SocketAddr, TcpListener, TcpStream},
    thread::JoinHandle,
    time::Duration,
};

use incoming::Incoming;
use log::{error, info};

use necronomicon::{Ack, Header};
use phylactery::{
    buffer,
    dequeue::{self, dequeue},
    entry::Version,
    ring_buffer::ring_buffer,
};

mod acks;
mod incoming;
mod outgoing;
mod state;
mod util;

pub const LICH_DIR: &str = "./lich/";

/// # Description
/// This is for running the data flow of a node in chain replication.
/// It has one thread for receiving data, one thread for processing data, and one thread for sending data.
///
fn data_loop() -> ! {
    let (pusher, popper) =
        ring_buffer(buffer::InMemBuffer::new(util::gigabytes(1)), Version::V1).expect("dequeue");

    let incoming = Incoming::new(9999, pusher);
    let in_handle = incoming.run();

    let connection = TcpStream::connect("tbd").expect("out client");

    let process_handle = std::thread::spawn(move || {
        // Note: this might be a lot of copying of data.
        // once for insert into the dequeue
        let mut buf = vec![0; 1024];
        // TODO: need to update interface of these to take a `Writer` to allow direct writes from the buffer.
        while let Ok(pop) = popper.pop(&mut buf) {}
    });

    let out_handle = std::thread::spawn(move || {});

    in_handle.join().expect("in_handle");
    process_handle.join().expect("process_handle");
    out_handle.join().expect("out_handle");
}

fn operator_loop() {
    todo!("operator_loop")
}

fn main() {
    pretty_env_logger::init();

    operator_loop();

    data_loop();
}
