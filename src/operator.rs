use std::{io::Write, sync::Arc};

use log::trace;
use necronomicon::{
    system_codec::{Join, Position, Report, Role, Transfer},
    Encode, Header, Kind, CHAIN_NOT_READY,
};

use parking_lot::Mutex;

use crate::{error::Error, session::Session};

pub(crate) struct Cluster(Arc<Mutex<ClusterInner>>);

struct ClusterInner {
    backends: Vec<Backend>,
    frontend: Option<Frontend>,
}

impl ClusterInner {
    fn head(&self) -> Option<&Backend> {
        self.backends
            .iter()
            .find(|backend| backend.role == ChainRole::Head)
    }

    fn tail(&self) -> Option<&Backend> {
        self.backends
            .iter()
            .find(|backend| backend.role == ChainRole::Tail)
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
        if let Some(mut backend) = self.existing_backend(join.address()) {
            // Need to promote to tail and set tail to replica.
            if backend.role == ChainRole::Candidate {
                // Current tail is now head or replica.
                if let Some(tail) = self.tail_mut() {
                    if tail.role == ChainRole::HeadAndTail {
                        trace!("setting current tail {tail:?} to head");
                        tail.role = ChainRole::Head;
                    } else {
                        trace!("setting current tail {tail:?} to replica");
                        tail.role = ChainRole::Middle;
                    }
                    trace!("setting candidate {backend:?} to tail");
                    backend.role = ChainRole::Tail;
                } else {
                    trace!("no tail found so setting candidate {backend:?} to head and tail");
                    backend.role = ChainRole::HeadAndTail;
                }
            } else {
                trace!("backend {backend:?} rejoining as candidate");
                backend.role = ChainRole::Candidate;
            }
            // put backend at end of chain!
            self.backends.push(backend);
        } else {
            self.backends.iter_mut().for_each(|backend| {
                backend.role = ChainRole::Middle;
            });
            self.backends.push(Backend {
                addr: join.address().to_owned(),
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
            report.encode(&mut backend.session)?;
        }

        if let Some(candidate) = candidate {
            Transfer::new(Header::new(Kind::Transfer, 1, id), candidate).encode(session)?;
        }

        Ok(())
    }

    fn add_frontend(&mut self, session: &mut Session, join: Join) -> Result<(), Error> {
        let addr = join.address().to_owned();

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
                report.encode(session)?;
            }
        } else {
            join.nack(CHAIN_NOT_READY).encode(session)?;
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

#[derive(Debug)]
struct Backend {
    addr: String,
    role: ChainRole,
    session: Session,
}

struct Frontend {
    addr: String,
    session: Session,
}

impl Default for Cluster {
    fn default() -> Self {
        Self::new()
    }
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
