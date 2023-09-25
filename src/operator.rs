use std::{io::Write, sync::Arc};

use log::{debug, info};
use necronomicon::{
    system_codec::{Join, Position, Report, Role, Transfer},
    Encode, Header, Kind, CHAIN_NOT_READY,
};

use parking_lot::Mutex;

use crate::{error::Error, session::Session};

// TODO: need to keep track of chain node positions.
// bceause if operator dies then we can recover the chain.
// also if entire chain dies then we can recover the chain.

#[derive(Clone, Default)]
pub(crate) struct Cluster(Arc<Mutex<ClusterInner>>);

#[derive(Clone, Default)]
struct ClusterInner {
    backends: Vec<Backend>,
    frontend: Option<Frontend>,
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
        self.backends
            .iter_mut()
            .find(|backend| backend.role == ChainRole::Tail)
    }

    fn existing_backend(&mut self, addr: &str) -> Option<Backend> {
        if let Some((pos, _)) = self
            .backends
            .iter_mut()
            .enumerate()
            .find(|(_, backend)| backend.addr == *addr)
        {
            Some(self.backends.remove(pos))
        } else {
            None
        }
    }

    fn add_backend(&mut self, session: &mut Session, join: Join) -> Result<(), Error> {
        let id = join.header().uuid();

        join.clone().ack().encode(session)?;

        // Update chain
        // TODO:
        // - need to determine if there is a candidate or more.
        // - if there is a candidate then we need to send the candidate the transaction log.
        if let Some(mut backend) = self.existing_backend(&join.addr()) {
            // Need to promote to tail and set tail to replica.
            if backend.role == ChainRole::Candidate {
                // Current tail is now head or replica.
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
            } else {
                info!("backend {backend:?} rejoining as candidate");
                backend.role = ChainRole::Candidate;
            }
            // put backend at end of chain!
            self.backends.push(backend);
        } else {
            self.backends.iter_mut().for_each(|backend| {
                backend.role = ChainRole::Middle;
            });
            self.backends.push(Backend {
                addr: join.addr().to_owned(),
                role: ChainRole::Tail,
                session: session.clone(),
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
            .find(|b| b.role == ChainRole::Head)
            .map(|backend| backend.addr.clone());

        let tail_addr = self
            .backends
            .iter()
            .find(|b| b.role == ChainRole::Tail)
            .map(|backend| backend.addr.clone());

        // Send reports
        if let Some(frontend) = self.frontend.as_mut() {
            let report = Report::new(
                Header::new(Kind::Report, 1, id),
                Position::Frontend {
                    head: head_addr,
                    tail: tail_addr,
                },
            );

            report.encode(&mut frontend.session)?;
        }

        // reverse order so the previous node is always the successor!
        let mut prev = None;
        let mut candidate = None;
        for backend in &mut self.backends.iter_mut().rev() {
            let report = match backend.role {
                ChainRole::Candidate => {
                    candidate = Some(backend.addr.clone());
                    Report::new(Header::new(Kind::Report, 1, id), Position::Candidate)
                }
                ChainRole::Head => Report::new(
                    Header::new(Kind::Report, 1, id),
                    Position::Head {
                        next: prev.take().expect("successor"),
                    },
                ),
                ChainRole::HeadAndTail => {
                    // No need to set prev as this is the last
                    Report::new(
                        Header::new(Kind::Report, 1, id),
                        Position::Tail {
                            candidate: candidate.clone(),
                        },
                    )
                }
                ChainRole::Middle => {
                    let next = prev.replace(backend.addr.clone());
                    Report::new(
                        Header::new(Kind::Report, 1, id),
                        Position::Middle {
                            next: next.expect("successor"),
                        },
                    )
                }
                ChainRole::Tail => {
                    prev = Some(backend.addr.clone());
                    // TODO: check if we have a candidate and start store transfer!
                    Report::new(
                        Header::new(Kind::Report, 1, id),
                        Position::Tail {
                            candidate: candidate.clone(),
                        },
                    )
                }
            };
            debug!("sending report to backend: {:?}", report);
            report.encode(&mut backend.session)?;
        }

        if let Some(candidate) = candidate {
            Transfer::new(Header::new(Kind::Transfer, 1, id), candidate).encode(session)?;
        }

        Ok(())
    }

    fn add_frontend(&mut self, session: &mut Session, join: Join) -> Result<(), Error> {
        let addr = join.addr().to_owned();
        if let Some(head) = self.head() {
            if let Some(tail) = self.tail() {
                let report = Report::new(
                    Header::new(Kind::Report, 1, join.header().uuid()),
                    Position::Frontend {
                        head: Some(head.addr.clone()),
                        tail: Some(tail.addr.clone()),
                    },
                );
                join.ack().encode(session)?;
                debug!("sending report to frontend: {:?}", report);
                report.encode(session)?;
            } else {
                let report = Report::new(
                    Header::new(Kind::Report, 1, join.header().uuid()),
                    Position::Frontend {
                        head: Some(head.addr.clone()),
                        tail: None,
                    },
                );
                join.ack().encode(session)?;
                debug!("sending report to frontend: {:?}", report);
                report.encode(session)?;
            }
        } else {
            join.nack(CHAIN_NOT_READY).encode(session)?;
            return Ok(());
        }
        session.flush()?;
        self.frontend = Some(Frontend {
            addr,
            session: session.clone(),
        });

        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChainRole {
    Candidate,
    Head,
    HeadAndTail,
    Middle,
    Tail,
}

#[derive(Clone, Debug)]
struct Backend {
    addr: String,
    role: ChainRole,
    session: Session,
}

#[derive(Clone, Debug)]
struct Frontend {
    addr: String,
    session: Session,
}

impl Cluster {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Mutex::new(ClusterInner {
            backends: Vec::new(),
            frontend: None,
        })))
    }

    /// Add a new backend or frontend to the cluster.
    /// Happens when a node first comes online and joins the cluster or
    /// when store tranfer completes.
    /// We then send a report to *all* nodes in the cluster.
    pub(crate) fn add(&self, mut session: Session, join: Join) -> Result<(), Error> {
        let mut cluster = self.0.lock();
        match join.clone().role() {
            Role::Backend(_) => cluster.add_backend(&mut session, join),
            Role::Frontend(_) => cluster.add_frontend(&mut session, join),
        }
    }
}

#[cfg(all(test, feature = "operator"))]
mod tests {
    use necronomicon::{
        system_codec::{Join, JoinAck, Position, Report, Role},
        Header, Kind, Packet, CHAIN_NOT_READY, SUCCESS,
    };

    use crate::{session::Session, stream::TcpStream};

    use super::Cluster;

    #[test]
    fn test_add_backend() {
        let cluster = Cluster::new();

        let head_stream = TcpStream::connect("head").unwrap();
        let head = Session::new(head_stream.clone(), 100);

        let join = Join::new(
            Header::new(Kind::Join, 1, 1),
            Role::Backend("head".to_owned()),
        );

        cluster.add(head.clone(), join).expect("add head");

        head_stream.verify_writes(&[
            Packet::JoinAck(JoinAck::new(Header::new(Kind::JoinAck, 1, 1), SUCCESS)),
            Packet::Report(Report::new(
                Header::new(Kind::Report, 1, 1),
                Position::Tail { candidate: None },
            )),
        ]);

        let middle_stream = TcpStream::connect("middle").unwrap();
        let middle = Session::new(middle_stream.clone(), 100);

        let join = Join::new(
            Header::new(Kind::Join, 1, 2),
            Role::Backend("middle".to_owned()),
        );

        cluster.add(middle.clone(), join).expect("add middle");

        middle_stream.verify_writes(&[
            Packet::JoinAck(JoinAck::new(Header::new(Kind::JoinAck, 1, 2), SUCCESS)),
            Packet::Report(Report::new(
                Header::new(Kind::Report, 1, 2),
                Position::Tail { candidate: None },
            )),
        ]);

        head_stream.verify_writes(&[Packet::Report(Report::new(
            Header::new(Kind::Report, 1, 2),
            Position::Head {
                next: "middle".to_owned(),
            },
        ))]);

        let tail_stream = TcpStream::connect("tail").unwrap();
        let tail = Session::new(tail_stream.clone(), 100);

        let join = Join::new(
            Header::new(Kind::Join, 1, 3),
            Role::Backend("tail".to_owned()),
        );

        cluster.add(tail.clone(), join).expect("add tail");

        tail_stream.verify_writes(&[
            Packet::JoinAck(JoinAck::new(Header::new(Kind::JoinAck, 1, 3), SUCCESS)),
            Packet::Report(Report::new(
                Header::new(Kind::Report, 1, 3),
                Position::Tail { candidate: None },
            )),
        ]);

        middle_stream.verify_writes(&[Packet::Report(Report::new(
            Header::new(Kind::Report, 1, 3),
            Position::Middle {
                next: "tail".to_owned(),
            },
        ))]);

        head_stream.verify_writes(&[Packet::Report(Report::new(
            Header::new(Kind::Report, 1, 3),
            Position::Head {
                next: "middle".to_owned(),
            },
        ))]);

        assert_eq!(cluster.0.lock().head().unwrap().addr, "head");
        assert_eq!(cluster.0.lock().tail().unwrap().addr, "tail");
    }

    #[test]
    fn test_add_frontend() {
        let cluster = Cluster::new();

        let frontend_stream = TcpStream::connect("frontend").unwrap();
        let frontend = Session::new(frontend_stream.clone(), 100);

        let join = Join::new(
            Header::new(Kind::Join, 1, 1),
            Role::Frontend("frontend".to_owned()),
        );

        cluster.add(frontend.clone(), join).expect("add frontend");

        frontend_stream.verify_writes(&[Packet::JoinAck(JoinAck::new(
            Header::new(Kind::JoinAck, 1, 1),
            CHAIN_NOT_READY,
        ))]);

        let head_tail_stream = TcpStream::connect("head_tail").unwrap();
        let head_tail = Session::new(head_tail_stream.clone(), 100);

        let join = Join::new(
            Header::new(Kind::Join, 1, 2),
            Role::Backend("head_tail".to_owned()),
        );

        cluster.add(head_tail.clone(), join).expect("add head_tail");

        head_tail_stream.verify_writes(&[
            Packet::JoinAck(JoinAck::new(Header::new(Kind::JoinAck, 1, 2), SUCCESS)),
            Packet::Report(Report::new(
                Header::new(Kind::Report, 1, 2),
                Position::Tail { candidate: None },
            )),
        ]);

        // Frontend doesn't get report until it joins again.
        let join = Join::new(
            Header::new(Kind::Join, 1, 3),
            Role::Frontend("frontend".to_owned()),
        );

        cluster.add(frontend.clone(), join).expect("add frontend");
        frontend_stream.verify_writes(&[
            Packet::JoinAck(JoinAck::new(Header::new(Kind::JoinAck, 1, 3), SUCCESS)),
            Packet::Report(Report::new(
                Header::new(Kind::Report, 1, 3),
                Position::Frontend {
                    head: Some("head_tail".to_owned()),
                    tail: Some("head_tail".to_owned()),
                },
            )),
        ]);
    }
}
