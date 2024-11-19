use std::{
    io::{BufReader, Write},
    net::Shutdown,
    thread::JoinHandle,
    time::Duration,
};

use crossbeam::channel::{bounded, Receiver, Select, Sender};
use log::{debug, trace, warn};
use uuid::Uuid;

use necronomicon::{
    full_decode,
    system_codec::{Join, Role},
    ByteStr, Encode, Pool, PoolImpl, SharedImpl,
};
use net::stream::{RetryConsistent, TcpStream};
use requests::System;

use crate::BufferOwner;

const CHANNEL_CAPACITY: usize = 1024;

pub struct OperatorConnection {
    _read: JoinHandle<()>,
    _write: JoinHandle<()>,
    kill_tx: Sender<()>,
    operator_tx: Sender<System<SharedImpl>>,
    operator_rx: Receiver<System<SharedImpl>>,
}

impl Drop for OperatorConnection {
    fn drop(&mut self) {
        self.kill_tx.send(()).expect("kill operator");
    }
}

impl OperatorConnection {
    pub(crate) fn connect(addr: String, our_port: u16) -> Self {
        let (operator_tx, state_rx) = bounded(CHANNEL_CAPACITY);
        let (state_tx, operator_rx) = bounded(CHANNEL_CAPACITY);
        let (kill_tx, kill_rx) = bounded(CHANNEL_CAPACITY);

        trace!("connecting to operator at {:?}", addr);
        let mut operator = TcpStream::retryable_connect(
            addr,
            RetryConsistent::new(Duration::from_millis(500), None),
        )
        .expect("connect");
        trace!("connected to operator {:?}", operator);
        let mut operator_read = BufReader::new(operator.clone());
        let mut operator_write = operator.clone();

        let pool = PoolImpl::new(1024, 1024);
        let read_pool = pool.clone();

        let read = std::thread::spawn(move || {
            let mut owned = read_pool.acquire("frontend decode", BufferOwner::OperatorFullDecode);
            let packet = full_decode(&mut operator_read, &mut owned, None).expect("decode");

            let System::JoinAck(ack) = System::from(packet.clone()) else {
                panic!("expected join ack but got {:?}", packet);
            };

            debug!("got join ack: {:?}", ack);

            // Get the `Report` from operator.
            let report = loop {
                let mut owned =
                    read_pool.acquire("frontend decode", BufferOwner::OperatorFullDecode);
                match full_decode(&mut operator_read, &mut owned, None) {
                    Ok(packet) => {
                        let operator_msg = System::from(packet);

                        if let System::Report(report) = operator_msg.clone() {
                            report.clone().ack().encode(&mut operator).expect("encode");
                            operator.flush().expect("flush");
                            break operator_msg;
                        } else {
                            warn!("expected report but got {:?}", operator_msg);
                        }
                    }
                    Err(err) => {
                        panic!("err: {}", err);
                    }
                }
            };

            debug!("got report: {:?}", report);
            state_tx.send(report).expect("send report");

            loop {
                let mut owned =
                    read_pool.acquire("frontend decode", BufferOwner::OperatorFullDecode);
                match full_decode(&mut operator_read, &mut owned, None) {
                    Ok(packet) => {
                        let operator_msg = System::from(packet);

                        state_tx.send(operator_msg).expect("send");
                    }
                    Err(err) => {
                        panic!("err: {}", err);
                    }
                }
            }
        });

        let write = std::thread::spawn(move || {
            let fqdn = std::process::Command::new("hostname")
                .arg("-f")
                .output()
                .expect("hostname fqdn")
                .stdout;
            let fqdn = String::from_utf8_lossy(&fqdn).trim().to_string();

            debug!("got fqdn: {}", fqdn);

            let mut owned = pool.acquire("frontend join", BufferOwner::Join);
            // TODO:
            // We will likely pick to use the same port for each BE node.
            // But we need a way to identify each node.
            // We can use a uuid for this.
            let join = Join::new(
                1,
                Uuid::new_v4().as_u128(),
                Role::Frontend(
                    ByteStr::from_owned(format!("{}:{}", fqdn, our_port), &mut owned)
                        .expect("owned"),
                ),
                0,
                false,
            );

            debug!("sending join to operator: {:?}", join);
            join.encode(&mut operator_write).expect("encode");
            operator_write.flush().expect("flush");

            loop {
                let mut sel = Select::new();
                let state_rx_id = sel.recv(&state_rx);
                let kill_rx_id = sel.recv(&kill_rx);

                let oper = sel.select();
                match oper.index() {
                    i if i == state_rx_id => {
                        let system: System<SharedImpl> = oper.recv(&state_rx).expect("recv");
                        trace!("got system packet: {:?}", system);
                        system.encode(&mut operator_write).expect("encode");
                        operator_write.flush().expect("flush");
                    }
                    i if i == kill_rx_id => {
                        debug!("write got kill signal");
                        let _ = operator_write.shutdown(Shutdown::Both);
                        break;
                    }
                    _ => {
                        panic!("unknown index");
                    }
                }
            }
        });

        Self {
            _read: read,
            _write: write,
            kill_tx,
            operator_tx,
            operator_rx,
        }
    }

    pub(crate) fn tx(&self) -> Sender<System<SharedImpl>> {
        self.operator_tx.clone()
    }

    pub(crate) fn rx(&self) -> Receiver<System<SharedImpl>> {
        self.operator_rx.clone()
    }
}
