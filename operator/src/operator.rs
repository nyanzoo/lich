use std::{io::Write, sync::Arc};

use log::{debug, error, info, trace};
use necronomicon::{
    system_codec::{Join, Position, Report, Role},
    ByteStr, Encode, SharedImpl, Uuid, CHAIN_NOT_READY,
};
use net::session::SessionWriter;

use parking_lot::Mutex;

use crate::{
    chain::{Backend, ChainRole, ConnectionState, Frontend},
    error::Error,
};

// TODO: need to keep track of chain node positions.
// bceause if operator dies then we can recover the chain.
// also if entire chain dies then we can recover the chain.

#[derive(Clone, Default)]
pub(crate) struct Cluster(Arc<Mutex<ClusterInner>>);

#[derive(Default)]
struct ClusterInner {
    backends: Vec<Backend>,
    frontend: Option<Frontend>,
    observers: Vec<SessionWriter>,
}

impl ClusterInner {
    fn head(&self) -> Option<&Backend> {
        self.backends.iter().find(|backend| {
            backend.role == ChainRole::Head || backend.role == ChainRole::HeadAndTail
        })
    }

    fn tail(&self) -> Option<&Backend> {
        self.backends.iter().find(|backend| {
            backend.role == ChainRole::Tail || backend.role == ChainRole::HeadAndTail
        })
    }

    fn tail_mut(&mut self) -> Option<&mut Backend> {
        self.backends.iter_mut().find(|backend| {
            backend.role == ChainRole::Tail || backend.role == ChainRole::HeadAndTail
        })
    }

    // TODO(rojang):
    // check if needed?
    #[allow(dead_code)]
    fn existing_backend(&mut self, addr: &ByteStr<SharedImpl>) -> Option<(usize, &Backend)> {
        if let Some((pos, backend)) = self
            .backends
            .iter_mut()
            .enumerate()
            .find(|(_, backend)| backend.addr == *addr)
        {
            Some((pos, backend))
        } else {
            None
        }
    }

    fn add_backend(
        &mut self,
        mut session: SessionWriter,
        join: Join<SharedImpl>,
    ) -> Result<(), Error> {
        trace!("adding backend: {join:?}");
        let id = join.header().uuid;

        join.clone().ack().encode(&mut session)?;
        let addr = join.addr().expect("addr").to_owned();

        // We need to check the following:
        // - if we don't have the backend and have never seen it, then add as new tail.
        // - if we have the backend and don't have it in chain, then add it as candidate.
        // - if we have the backend, have it in chain and the successor is lost, then promote it to tail.
        //
        // Other cases should not happen.
        //
        //   // Update chain
        // TODO:
        // - need to determine if there is a candidate or more.
        // - if there is a candidate then we need to send the candidate the transaction log.

        if let Some(pos) = self
            .backends
            .iter()
            .position(|backend| backend.addr == addr)
        {
            let successor_role = self.backends.get(pos + 1).map(|backend| backend.role);
            let backend_role = self
                .backends
                .get(pos)
                .map(|backend| backend.role)
                .expect("backend role");
            if backend_role == ChainRole::Resigner {
                // put backend at end of chain!
                let mut backend = self.backends.remove(pos);
                info!("backend {backend:?} rejoining as candidate");
                backend.role = ChainRole::Candidate;
                self.backends.push(backend);
            } else if backend_role == ChainRole::Candidate {
                // Need to promote to tail and set tail to replica.
                // Current tail is now head or replica.
                let mut backend = self.backends.remove(pos);
                if let Some(tail) = self.tail_mut() {
                    if tail.role == ChainRole::HeadAndTail {
                        info!("setting current tail {tail:?} to head");
                        tail.role = ChainRole::Head;
                    } else {
                        info!("setting current tail {tail:?} to replica");
                        tail.role = ChainRole::Middle;
                    }
                    info!("setting candidate {backend:?} to tail");
                    backend.role = ChainRole::Tail;
                } else {
                    info!("no tail found so setting candidate {backend:?} to head and tail");
                    backend.role = ChainRole::HeadAndTail;
                }

                // put backend at end of chain!
                self.backends.push(backend);
            } else if let Some(backend) = self.backends.get_mut(pos) {
                let new_connection_state = join.successor_lost().into();
                if backend.successor_connection == new_connection_state {
                    error!("backend {backend:?} rejoining with same successor connection state, this is probably a bug");
                } else {
                    assert_eq!(new_connection_state, ConnectionState::Disconnected);

                    if let Some(role) = successor_role {
                        info!("backend {backend:?} rejoining as {role:?} and setting successor connection state to disconnected");

                        // Make sure we set head and tail correctly.
                        if (backend.role == ChainRole::Head && role == ChainRole::Tail)
                            || (backend.role == ChainRole::Tail && role == ChainRole::Head)
                        {
                            backend.role = ChainRole::HeadAndTail;
                        } else {
                            backend.role = role;
                        }
                    }

                    if let Some(b) = self.backends.get_mut(pos + 1) {
                        b.role = ChainRole::Resigner;
                    }
                }
            } else {
                unimplemented!("backend rejoining with different addr?");
                // info!("backend {backend:?} rejoining as candidate");
                // backend.role = ChainRole::Candidate;

                // // put backend at end of chain!
                // self.backends.remove(pos);
                // self.backends.push(backend.clone());
            }
        } else {
            self.backends.iter_mut().for_each(|backend| {
                backend.role = ChainRole::Middle;
            });
            self.backends.push(Backend {
                addr,
                role: ChainRole::Tail,
                session,
                successor_connection: join.successor_lost().into(),
            });
            if let Some(head) = self.backends.first_mut() {
                if head.role == ChainRole::Tail {
                    head.role = ChainRole::HeadAndTail;
                } else {
                    head.role = ChainRole::Head;
                }
            }
        }

        let head_addr = self
            .backends
            .iter()
            .find(|b| b.role == ChainRole::Head || b.role == ChainRole::HeadAndTail)
            .map(|backend| backend.addr.clone());

        let tail_addr = self
            .backends
            .iter()
            .find(|b| b.role == ChainRole::Tail || b.role == ChainRole::HeadAndTail)
            .map(|backend| backend.addr.clone());

        // Send reports
        if let Some(frontend) = self.frontend.as_mut() {
            let report = Report::new(
                1,
                id,
                Position::Frontend {
                    head: head_addr,
                    tail: tail_addr,
                },
            );

            debug!("sending report to {:?}: {:?}", frontend.addr, report);
            report.encode(&mut frontend.session)?;
            frontend.session.flush()?;
        }

        self.send_backend_reports(id)?;

        self.send_observer_reports(id)?;

        Ok(())
    }

    fn add_frontend(
        &mut self,
        mut session: SessionWriter,
        join: Join<SharedImpl>,
    ) -> Result<(), Error> {
        trace!("adding frontend: {join:?}");
        let id = join.header().uuid;
        let addr = join.addr().expect("addr").to_owned();
        if let Some(head) = self.head() {
            if let Some(tail) = self.tail() {
                let report = Report::new(
                    1,
                    join.header().uuid,
                    Position::Frontend {
                        head: Some(head.addr.clone()),
                        tail: Some(tail.addr.clone()),
                    },
                );
                join.ack().encode(&mut session)?;
                debug!("sending report to {:?}: {:?}", addr, report);
                report.encode(&mut session)?;
            } else {
                let report = Report::new(
                    1,
                    id,
                    Position::Frontend {
                        head: Some(head.addr.clone()),
                        tail: None,
                    },
                );
                join.ack().encode(&mut session)?;
                debug!("sending report to frontend: {:?}", report);
                report.encode(&mut session)?;
            }
        } else {
            join.nack(CHAIN_NOT_READY, None).encode(&mut session)?;
        }
        session.flush()?;
        self.frontend = Some(Frontend { addr, session });

        self.send_observer_reports(id)?;

        Ok(())
    }

    fn add_observer(&mut self, session: SessionWriter) {
        trace!("adding observer: {session:?}");
        self.observers.push(session);
    }

    fn send_backend_reports(&mut self, id: Uuid) -> Result<(), Error> {
        // reverse order so the previous node is always the successor!
        let mut prev = None;
        let mut candidate = None;
        for backend in &mut self.backends.iter_mut().rev() {
            let report = match backend.role {
                ChainRole::Candidate => {
                    candidate = Some(backend.addr.clone());
                    Report::new(1, id, Position::Candidate)
                }
                ChainRole::Head => Report::new(
                    1,
                    id,
                    Position::Head {
                        next: prev.take().expect("successor"),
                    },
                ),
                ChainRole::HeadAndTail => {
                    // No need to set prev as this is the last
                    Report::new(
                        1,
                        id,
                        Position::Tail {
                            candidate: candidate.clone(),
                        },
                    )
                }
                ChainRole::Middle => {
                    let next = prev.replace(backend.addr.clone());
                    Report::new(
                        1,
                        id,
                        Position::Middle {
                            next: next.expect("successor"),
                        },
                    )
                }
                ChainRole::Resigner => {
                    trace!("skipping resigner {backend:?}");
                    continue;
                }
                ChainRole::Tail => {
                    prev = Some(backend.addr.clone());
                    // TODO: check if we have a candidate and start store transfer!
                    Report::new(
                        1,
                        id,
                        Position::Tail {
                            candidate: candidate.clone(),
                        },
                    )
                }
            };
            debug!("sending report to {:?}: {:?}", backend.addr, report);
            report.encode(&mut backend.session)?;
            backend.session.flush()?;
        }

        Ok(())
    }

    // TODO: remove bad sessions
    fn send_observer_reports(&mut self, id: Uuid) -> Result<(), Error> {
        for session in &mut self.observers {
            let mut chain = Vec::new();
            for backend in &self.backends {
                chain.push(Role::Backend(backend.addr.clone()));
            }
            if let Some(frontend) = &self.frontend {
                chain.push(Role::Frontend(frontend.addr.clone()));
            }

            let report = Report::new(1, id, Position::Observer { chain });

            report.encode(session)?;

            session.flush()?;
        }

        Ok(())
    }
}

impl Cluster {
    /// Add a new backend or frontend to the cluster.
    /// Happens when a node first comes online and joins the cluster or
    /// when store tranfer completes.
    /// We then send a report to *all* nodes in the cluster.
    pub(crate) fn add(&self, session: SessionWriter, join: Join<SharedImpl>) -> Result<(), Error> {
        debug!("adding node: {join:?}");
        let mut cluster = self.0.lock();
        match join.clone().role() {
            Role::Backend(_) => cluster.add_backend(session, join),
            Role::Frontend(_) => cluster.add_frontend(session, join),
            Role::Observer => {
                cluster.add_observer(session);
                cluster.send_observer_reports(join.header().uuid)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use necronomicon::{
        byte_str,
        system_codec::{Join, JoinAck, Position, Report, Role},
        Packet, Response, SystemPacket, CHAIN_NOT_READY,
    };
    use net::{session::Session, stream::TcpStream};

    use super::Cluster;

    #[test]
    fn add_backend() {
        let cluster = Cluster::default();

        let head_stream = TcpStream::connect("head:666".to_owned()).unwrap();
        let (_, head) = Session::new(head_stream.clone(), 100).split();

        let join = Join::new(1, 1, Role::Backend(byte_str(b"head")), 1, false);

        cluster.add(head, join).expect("add head");

        head_stream.verify_writes(&[
            Packet::System(SystemPacket::JoinAck(JoinAck::new(Response::success(), 1))),
            Packet::System(SystemPacket::Report(Report::new(
                1,
                1,
                Position::Tail { candidate: None },
            ))),
        ]);

        let middle_stream = TcpStream::connect("middle:666".to_owned()).unwrap();
        let (_, middle) = Session::new(middle_stream.clone(), 100).split();

        let join = Join::new(1, 2, Role::Backend(byte_str(b"middle")), 1, false);

        cluster.add(middle, join).expect("add middle");

        middle_stream.verify_writes(&[
            Packet::System(SystemPacket::JoinAck(JoinAck::new(Response::success(), 2))),
            Packet::System(SystemPacket::Report(Report::new(
                1,
                2,
                Position::Tail { candidate: None },
            ))),
        ]);

        head_stream.verify_writes(&[Packet::System(SystemPacket::Report(Report::new(
            1,
            2,
            Position::Head {
                next: byte_str(b"middle"),
            },
        )))]);

        let tail_stream = TcpStream::connect("tail:666".to_owned()).unwrap();
        let (_, tail) = Session::new(tail_stream.clone(), 100).split();

        let join = Join::new(1, 3, Role::Backend(byte_str(b"tail")), 1, false);

        cluster.add(tail, join).expect("add tail");

        tail_stream.verify_writes(&[
            Packet::System(SystemPacket::JoinAck(JoinAck::new(Response::success(), 3))),
            Packet::System(SystemPacket::Report(Report::new(
                1,
                3,
                Position::Tail { candidate: None },
            ))),
        ]);

        middle_stream.verify_writes(&[Packet::System(SystemPacket::Report(Report::new(
            1,
            3,
            Position::Middle {
                next: byte_str(b"tail"),
            },
        )))]);

        head_stream.verify_writes(&[Packet::System(SystemPacket::Report(Report::new(
            1,
            3,
            Position::Head {
                next: byte_str(b"middle"),
            },
        )))]);

        assert_eq!(cluster.0.lock().head().unwrap().addr, byte_str(b"head"));
        assert_eq!(cluster.0.lock().tail().unwrap().addr, byte_str(b"tail"));
    }

    #[test]
    fn add_frontend() {
        let cluster = Cluster::default();

        let frontend_stream = TcpStream::connect("frontend".to_owned()).unwrap();
        let (_, frontend) = Session::new(frontend_stream.clone(), 100).split();

        let join = Join::new(1, 1, Role::Frontend(byte_str(b"frontend")), 1, false);

        cluster.add(frontend, join).expect("add frontend");

        frontend_stream.verify_writes(&[Packet::System(SystemPacket::JoinAck(JoinAck::new(
            Response::fail(CHAIN_NOT_READY, None),
            1,
        )))]);

        let head_tail_stream = TcpStream::connect("head_tail".to_owned()).unwrap();
        let (_, head_tail) = Session::new(head_tail_stream.clone(), 100).split();

        let join = Join::new(1, 2, Role::Backend(byte_str(b"head_tail")), 1, false);

        cluster.add(head_tail, join).expect("add head_tail");

        head_tail_stream.verify_writes(&[
            Packet::System(SystemPacket::JoinAck(JoinAck::new(Response::success(), 2))),
            Packet::System(SystemPacket::Report(Report::new(
                1,
                2,
                Position::Tail { candidate: None },
            ))),
        ]);

        // Frontend gets report when BE joins.
        frontend_stream.verify_writes(&[Packet::System(SystemPacket::Report(Report::new(
            1,
            2,
            Position::Frontend {
                head: Some(byte_str(b"head_tail")),
                tail: Some(byte_str(b"head_tail")),
            },
        )))]);

        // New frontend gets report when it joins.
        let frontend_stream = TcpStream::connect("new_frontend".to_owned()).unwrap();
        let (_, frontend) = Session::new(frontend_stream.clone(), 100).split();
        let join = Join::new(1, 3, Role::Frontend(byte_str(b"new_frontend")), 1, false);
        cluster.add(frontend, join).expect("add frontend");
        frontend_stream.verify_writes(&[
            Packet::System(SystemPacket::JoinAck(JoinAck::new(Response::success(), 3))),
            Packet::System(SystemPacket::Report(Report::new(
                1,
                3,
                Position::Frontend {
                    head: Some(byte_str(b"head_tail")),
                    tail: Some(byte_str(b"head_tail")),
                },
            ))),
        ]);
    }

    #[ignore = "re-enable when store transfer is implemented"]
    #[test]
    fn store_transfer() {
        let cluster = Cluster::default();

        let head_stream = TcpStream::connect("head".to_owned()).unwrap();
        let (_, head) = Session::new(head_stream.clone(), 100).split();

        let join = Join::new(1, 1, Role::Backend(byte_str(b"head")), 1, false);

        cluster.add(head, join).expect("add head");

        head_stream.verify_writes(&[
            Packet::System(SystemPacket::JoinAck(JoinAck::new(Response::success(), 1))),
            Packet::System(SystemPacket::Report(Report::new(
                1,
                1,
                Position::Tail { candidate: None },
            ))),
        ]);

        // Create a tail stream and drop it.
        {
            let tail_stream = TcpStream::connect("tail".to_owned()).unwrap();
            let (_, tail) = Session::new(tail_stream.clone(), 100).split();

            let join = Join::new(1, 2, Role::Backend(byte_str(b"tail")), 1, false);

            cluster.add(tail, join).expect("add tail");

            tail_stream.verify_writes(&[
                Packet::System(SystemPacket::JoinAck(JoinAck::new(Response::success(), 2))),
                Packet::System(SystemPacket::Report(Report::new(
                    1,
                    2,
                    Position::Tail { candidate: None },
                ))),
            ]);

            head_stream.verify_writes(&[Packet::System(SystemPacket::Report(Report::new(
                1,
                2,
                Position::Head {
                    next: byte_str(b"tail"),
                },
            )))]);

            assert_eq!(cluster.0.lock().head().unwrap().addr, byte_str(b"head"));
            assert_eq!(cluster.0.lock().tail().unwrap().addr, byte_str(b"tail"));
        }

        let (_, head) = Session::new(head_stream.clone(), 100).split();
        // Head should notice that tail is gone and re-join as candidate to become tail.
        let join = Join::new(1, 3, Role::Backend(byte_str(b"head")), 1, true);

        cluster.add(head, join).expect("add head again");

        head_stream.verify_writes(&[
            Packet::System(SystemPacket::JoinAck(JoinAck::new(Response::success(), 3))),
            Packet::System(SystemPacket::Report(Report::new(
                1,
                3,
                Position::Tail { candidate: None },
            ))),
        ]);

        // Rejoin the tail and check that it is now a candidate. Head should become Tail.

        let tail_stream = TcpStream::connect("tail".to_owned()).unwrap();
        let (_, tail) = Session::new(tail_stream.clone(), 100).split();

        let join = Join::new(1, 4, Role::Backend(byte_str(b"tail")), 1, false);

        cluster.add(tail, join).expect("add tail");

        tail_stream.verify_writes(&[
            Packet::System(SystemPacket::JoinAck(JoinAck::new(Response::success(), 4))),
            Packet::System(SystemPacket::Report(Report::new(1, 4, Position::Candidate))),
        ]);

        head_stream.verify_writes(&[Packet::System(SystemPacket::Report(Report::new(
            1,
            4,
            Position::Tail {
                candidate: Some(byte_str(b"tail")),
            },
        )))]);

        assert_eq!(cluster.0.lock().head().unwrap().addr, byte_str(b"head"));
        assert_eq!(cluster.0.lock().tail().unwrap().addr, byte_str(b"head"));

        let join = Join::new(1, 5, Role::Backend(byte_str(b"tail")), 1, false);

        let (_, tail) = Session::new(tail_stream.clone(), 100).split();
        cluster.add(tail, join).expect("add tail");

        tail_stream.verify_writes(&[
            Packet::System(SystemPacket::JoinAck(JoinAck::new(Response::success(), 5))),
            Packet::System(SystemPacket::Report(Report::new(
                1,
                5,
                Position::Tail { candidate: None },
            ))),
        ]);

        head_stream.verify_writes(&[Packet::System(SystemPacket::Report(Report::new(
            1,
            5,
            Position::Head {
                next: byte_str(b"tail"),
            },
        )))]);
    }

    // #[test]
    // fn add_observer() {
    //     todo!("add observer")
    // }

    // #[test]
    // fn cluster_restart() {
    //     todo!("cluster restart")
    // }
}
